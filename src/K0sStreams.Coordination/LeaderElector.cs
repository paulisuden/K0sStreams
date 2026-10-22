using K0sStreams.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Coordination;

/// <summary>
/// Elección de líder con el Lease de Kubernetes (fase 4). Cada <see cref="CoordinationOptions.RetryPeriod"/>, el líder
/// renueva el Lease y un seguidor lo mira. Si nadie lo renovó durante <see cref="CoordinationOptions.LeaseDuration"/>, el
/// seguidor lo toma con la época siguiente, se pone al día con <see cref="ILeaderPromotion"/> y recién entonces pasa a
/// ser líder.
/// </summary>
internal sealed partial class LeaderElector(
    ILeaseStore store,
    LeaseClusterState state,
    ILeaderPromotion promotion,
    IOptions<CoordinationOptions> options,
    TimeProvider time,
    ILogger<LeaderElector> logger) : BackgroundService
{
    private readonly object _gate = new();

    // Mientras este broker es titular: el Lease tal como lo escribió la última vez, y cuándo.
    private LeaseRecord? _held;
    private long _heldAt;

    // Mientras se pone al día para liderar: cancela la promoción si se pierde el Lease.
    private CancellationTokenSource? _promoting;

    // Siendo seguidor: la última versión del Lease que vio y cuándo la vio por primera vez. El vencimiento se mide con el
    // reloj local y no con la hora escrita en el Lease, porque los relojes de los pods pueden no coincidir.
    private string? _observedVersion;
    private long _observedAt;

    /// <summary>La promoción en curso, o la última. Completa si nunca hubo una.</summary>
    internal Task PendingPromotion { get; private set; } = Task.CompletedTask;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.RetryPeriod, time);
        try
        {
            do
            {
                await TickAsync(stoppingToken).ConfigureAwait(false);
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // El pod se está apagando.
        }
    }

    /// <summary>
    /// Frena el bucle y suelta el Lease en vez de dejarlo vencer: el próximo líder sube enseguida y no LeaseDuration
    /// después. Va acá y no al final del bucle porque, si el apagado llega antes de que el bucle arranque, el bucle no
    /// corre nunca.
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
        await ReleaseAsync(cancellationToken).ConfigureAwait(false);
        await PendingPromotion.ConfigureAwait(false);
    }

    /// <summary>Un ciclo de la elección. Los tests lo llaman directo, sin esperar al timer.</summary>
    internal async Task TickAsync(CancellationToken ct = default)
    {
        LeaseRecord? held;
        lock (_gate)
        {
            held = _held;
        }

        if (held is null)
        {
            await ObserveAsync(ct).ConfigureAwait(false);
        }
        else
        {
            await RenewAsync(held, ct).ConfigureAwait(false);
        }
    }

    private async Task ObserveAsync(CancellationToken ct)
    {
        try
        {
            // Sin Lease es el primer arranque del cluster: quien lo crea lidera la época 1.
            LeaseRecord? lease = await store.GetAsync(ct).ConfigureAwait(false);
            LeaseRecord? acquired = lease is null
                ? await store.TryCreateAsync(state.NodeId, transitions: 1, ct).ConfigureAwait(false)
                : await TryTakeAsync(lease, ct).ConfigureAwait(false);

            if (acquired is not null)
            {
                StartPromotion(acquired);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Sin el API server no se puede elegir líder: se reintenta en el próximo ciclo.
            LogObserveFailed(logger, ex);
        }
    }

    /// <summary>Sigue al titular del Lease y, si está libre o vencido, intenta tomarlo con la época siguiente.</summary>
    private async Task<LeaseRecord?> TryTakeAsync(LeaseRecord lease, CancellationToken ct)
    {
        string? holder = string.IsNullOrEmpty(lease.HolderIdentity) ? null : lease.HolderIdentity;
        bool mine = holder == state.NodeId;
        state.Follow(lease.Transitions, mine ? null : holder);

        if (lease.Version != _observedVersion)
        {
            _observedVersion = lease.Version;
            _observedAt = time.GetTimestamp();
        }

        // A nombre propio quiere decir que este broker lo tenía antes de reiniciarse: no hace falta esperar a que venza.
        bool free = holder is null || mine || time.GetElapsedTime(_observedAt) >= options.Value.LeaseDuration;
        return free
            ? await store.TryReplaceAsync(lease, state.NodeId, lease.Transitions + 1, ct).ConfigureAwait(false)
            : null;
    }

    private void StartPromotion(LeaseRecord acquired)
    {
        var promoting = new CancellationTokenSource();
        lock (_gate)
        {
            _held = acquired;
            _heldAt = time.GetTimestamp();
            _promoting = promoting;
        }

        LogAcquired(logger, acquired.Transitions);

        // En segundo plano: ponerse al día puede tardar más que LeaseDuration y, mientras tanto, el bucle tiene que
        // seguir renovando el Lease.
        PendingPromotion = PromoteAsync(acquired.Transitions, promoting);
    }

    private async Task PromoteAsync(long epoch, CancellationTokenSource promoting)
    {
        try
        {
            await promotion.PromoteAsync(epoch, promoting.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (promoting.IsCancellationRequested)
        {
            // Perdió el Lease mientras se ponía al día: otro broker puede estar liderando una época nueva.
            LogPromotionCancelled(logger, epoch);
            return;
        }
        catch (Exception ex)
        {
            // Normalmente es que algún broker ya está en una época mayor. Al liberar el Lease, el próximo que lo tome
            // usa una época más, así que los intentos terminan superándola.
            LogPromotionFailed(logger, ex, epoch);
            await ReleaseAsync().ConfigureAwait(false);
            return;
        }

        lock (_gate)
        {
            // Si justo perdió el Lease, LoseLease ya sacó esta promoción de _promoting: no se vuelve líder.
            if (_promoting != promoting)
            {
                return;
            }

            _promoting = null;
            state.BecomeLeader(epoch);
        }

        promoting.Dispose();
        LogLeading(logger, epoch);
    }

    private async Task RenewAsync(LeaseRecord held, CancellationToken ct)
    {
        LeaseRecord? renewed;
        try
        {
            renewed = await store.TryReplaceAsync(held, state.NodeId, held.Transitions, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Sin respuesta no se sabe si el Lease sigue siendo propio: aguanta hasta RenewDeadline.
            if (time.GetElapsedTime(_heldAt) < options.Value.RenewDeadline)
            {
                LogRenewFailed(logger, ex);
                return;
            }

            LogRenewDeadlinePassed(logger, ex, held.Transitions);
            LoseLease();
            return;
        }

        if (renewed is null)
        {
            // Otro broker escribió el Lease desde la última renovación: ya no es de este broker.
            LogLeaseTaken(logger, held.Transitions);
            LoseLease();
            return;
        }

        lock (_gate)
        {
            // Si mientras tanto se liberó (falló la promoción), no se lo vuelve a anotar como propio.
            if (_held == held)
            {
                _held = renewed;
                _heldAt = time.GetTimestamp();
            }
        }
    }

    /// <summary>Deja de ser líder ya mismo y cancela la promoción en curso, si hay una.</summary>
    private void LoseLease()
    {
        CancellationTokenSource? promoting;
        lock (_gate)
        {
            _held = null;
            promoting = _promoting;
            _promoting = null;
            state.StepDown();
        }

        // Fuera del lock: cancelar ejecuta en este hilo lo que la promoción haya registrado para ese caso.
        promoting?.Cancel();
        promoting?.Dispose();
    }

    /// <summary>Deja de ser líder y libera el Lease, para que otro broker lo tome sin esperar a que venza.</summary>
    private async Task ReleaseAsync(CancellationToken ct = default)
    {
        LeaseRecord? held;
        lock (_gate)
        {
            held = _held;
        }

        LoseLease();
        if (held is null)
        {
            return;
        }

        try
        {
            await store.TryReplaceAsync(held, holder: null, held.Transitions, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Si no se pudo liberar, vence solo.
            LogReleaseFailed(logger, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "No se pudo leer o tomar el Lease; se reintenta en el próximo ciclo.")]
    private static partial void LogObserveFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "Lease tomado para la época {Epoch}; poniéndose al día antes de liderar.")]
    private static partial void LogAcquired(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Líder de la época {Epoch}.")]
    private static partial void LogLeading(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Se perdió el Lease mientras se ponía al día para la época {Epoch}.")]
    private static partial void LogPromotionCancelled(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Error, Message = "No se pudo liderar la época {Epoch}; se libera el Lease.")]
    private static partial void LogPromotionFailed(ILogger logger, Exception exception, long epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No se pudo renovar el Lease; se reintenta en el próximo ciclo.")]
    private static partial void LogRenewFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Sin poder renovar el Lease durante el plazo; deja de liderar la época {Epoch}.")]
    private static partial void LogRenewDeadlinePassed(ILogger logger, Exception exception, long epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Otro broker tomó el Lease; deja de liderar la época {Epoch}.")]
    private static partial void LogLeaseTaken(ILogger logger, long epoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No se pudo liberar el Lease; vence solo.")]
    private static partial void LogReleaseFailed(ILogger logger, Exception exception);
}
