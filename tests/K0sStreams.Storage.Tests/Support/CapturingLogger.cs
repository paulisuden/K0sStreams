using Microsoft.Extensions.Logging;

namespace K0sStreams.Storage.Tests.Support;

/// <summary>
/// Un <see cref="ILogger{T}"/> que en lugar de escribir guarda lo que le mandan, para poder afirmar
/// sobre los mensajes. Se implementa a mano porque <c>ILogger</c> es una interfaz: no hace falta
/// ningún paquete, igual que con los fakes de <c>Contracts.Fakes</c>.
/// </summary>
internal sealed class CapturingLogger : ILogger<SegmentedLog>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    /// <summary>No se usan ámbitos, así que no hay nada que abrir ni cerrar.</summary>
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    /// <summary>Todo habilitado: un test que filtre niveles dejaría de ver lo que quiere probar.</summary>
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);

        // Con la excepción pegada al mensaje, igual que la imprime la consola: el detalle del daño
        // (archivo, byte y motivo) viaja en la excepción, no en el texto de la plantilla.
        string mensaje = exception is null
            ? formatter(state, exception)
            : $"{formatter(state, exception)} {exception.Message}";

        // Los appends pueden venir de varios hilos (iteración 6), así que la lista va bajo candado.
        lock (_entries)
        {
            _entries.Add((logLevel, mensaje));
        }
    }

    /// <summary>Los mensajes de ese nivel, uno por línea, para poder buscar un texto adentro.</summary>
    public string Texto(LogLevel level)
    {
        lock (_entries)
        {
            return string.Join('\n', _entries.Where(entry => entry.Level == level).Select(entry => entry.Message));
        }
    }

    /// <summary>Cuántos mensajes se registraron en ese nivel.</summary>
    public int Cuantos(LogLevel level)
    {
        lock (_entries)
        {
            return _entries.Count(entry => entry.Level == level);
        }
    }
}
