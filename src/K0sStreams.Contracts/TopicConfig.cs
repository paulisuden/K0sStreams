using System.Text.RegularExpressions;

namespace K0sStreams.Contracts;

/// <param name="Name">Nombre del tópico. Se usa como nombre de carpeta en disco.</param>
/// <param name="Partitions">Cantidad de particiones (1 en la PoC).</param>
/// <param name="MaxRetries">Reintentos después del primer intento antes de ir a la DLQ.</param>
/// <param name="RetryBackoff">Espera base entre reintentos; crece de forma exponencial.</param>
public sealed partial record TopicConfig(string Name, int Partitions, int MaxRetries, TimeSpan RetryBackoff)
{
    public const int MaxPartitions = 64;

    /// <summary>Minúsculas, dígitos, punto, guion y guion bajo; hasta 100 caracteres. Vale para tópicos y colas.</summary>
    public static bool IsValidName(string? name) => name is not null && NamePattern().IsMatch(name);

    /// <summary>Errores de validación por campo; vacío si la configuración es válida.</summary>
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (!IsValidName(Name))
        {
            errors[nameof(Name)] = ["Usar minúsculas, dígitos, '.', '-' o '_' (1 a 100 caracteres)."];
        }

        if (Partitions is < 1 or > MaxPartitions)
        {
            errors[nameof(Partitions)] = [$"Debe estar entre 1 y {MaxPartitions}."];
        }

        if (MaxRetries < 0)
        {
            errors[nameof(MaxRetries)] = ["No puede ser negativo."];
        }

        if (RetryBackoff < TimeSpan.Zero)
        {
            errors[nameof(RetryBackoff)] = ["No puede ser negativo."];
        }

        return errors;
    }

    [GeneratedRegex("^[a-z0-9][a-z0-9._-]{0,99}$")]
    private static partial Regex NamePattern();
}
