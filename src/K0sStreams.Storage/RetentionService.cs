using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace K0sStreams.Storage;

/// <summary>
/// Le pide al log aplicar la retención cada <see cref="RetentionOptions.CheckInterval"/>.
/// </summary>
/// <remarks>
/// Está afuera de <see cref="SegmentedLog"/> a propósito: el log no arranca tareas por su cuenta,
/// así el apagado lo maneja el host con su <see cref="CancellationToken"/> y el pod puede terminar
/// limpio. El reloj viene por <see cref="TimeProvider"/>, que es lo que permite testearlo.
/// </remarks>
internal sealed partial class RetentionService : BackgroundService
{
    private readonly SegmentedLog _log;
    private readonly RetentionOptions _retention;
    private readonly TimeProvider _time;
    private readonly ILogger<RetentionService> _logger;

    public RetentionService(
        SegmentedLog log,
        IOptions<RetentionOptions> retention,
        TimeProvider time,
        ILogger<RetentionService> logger)
    {
        ArgumentNullException.ThrowIfNull(retention);

        _log = log;
        _retention = retention.Value;
        _time = time;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_retention.MaxAge is null && _retention.MaxBytes is null)
        {
            // Sin ningún límite no hay nada que vigilar, y un timer que no sirve para nada igual
            // despierta al proceso cada cinco minutos.
            return;
        }

        using var timer = new PeriodicTimer(_retention.CheckInterval, _time);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                Apply(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Apagado normal del pod: el host canceló mientras se esperaba el próximo tick.
        }
    }

    /// <summary>
    /// Una pasada. Un problema de disco no puede tirar el broker: los datos siguen enteros, lo
    /// único que no pasó es la limpieza, y la próxima pasada lo vuelve a intentar.
    /// </summary>
    private void Apply(CancellationToken ct)
    {
        try
        {
            _log.ApplyRetention(_time.GetUtcNow(), ct);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            LogRetentionFailed(_logger, ex);
        }
    }

    [LoggerMessage(
        Level = LogLevel.Error,
        Message = "Retention could not delete old segments this round. The log is intact; the next round will try again.")]
    private static partial void LogRetentionFailed(ILogger logger, Exception exception);
}
