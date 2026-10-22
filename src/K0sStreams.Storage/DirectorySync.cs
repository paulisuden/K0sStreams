using System.Runtime.InteropServices;
using System.Text;

namespace K0sStreams.Storage;

/// <summary>
/// <c>fsync</c> de una carpeta. Sincronizar un archivo recién creado no alcanza: su entrada de
/// directorio puede seguir en memoria, y tras un corte de luz el contenido estaría a salvo pero
/// el archivo no existiría. .NET no expone esta operación, así que se llama a <c>libc</c>.
/// </summary>
internal static class DirectorySync
{
    private const int ReadOnly = 0;            // O_RDONLY
    private const int DirectoryOnly = 0x10000; // O_DIRECTORY, solo en Linux

    /// <summary>Sincroniza la carpeta. En Windows no hace nada: no tiene equivalente.</summary>
    public static void Flush(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        int flags = OperatingSystem.IsLinux() ? ReadOnly | DirectoryOnly : ReadOnly;
        int descriptor = Open(ToNativePath(path), flags);
        if (descriptor < 0)
        {
            throw new IOException($"No se pudo abrir la carpeta '{path}' para sincronizarla.", Marshal.GetLastPInvokeError());
        }

        try
        {
            if (Fsync(descriptor) != 0)
            {
                throw new IOException($"No se pudo sincronizar la carpeta '{path}'.", Marshal.GetLastPInvokeError());
            }
        }
        finally
        {
            // El resultado de close() no es accionable: el fsync ya ocurrió o ya falló.
            _ = Close(descriptor);
        }
    }

    /// <summary>Ruta como bytes UTF-8 terminados en cero, que es lo que recibe libc.</summary>
    private static byte[] ToNativePath(string path)
    {
        var bytes = new byte[Encoding.UTF8.GetByteCount(path) + 1];
        Encoding.UTF8.GetBytes(path, bytes);
        return bytes;
    }

    // La ruta va como bytes y no como string: en Linux las rutas son bytes, y además evita
    // el marshaling de strings, que CA2101 marca como riesgoso.
    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int Open(byte[] path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int Fsync(int descriptor);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int Close(int descriptor);
}
