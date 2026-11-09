namespace K0sStreams.Storage;

// El estado en memoria de un tópico. Sigue siendo una clase anidada y privada: nadie fuera de
// SegmentedLog tiene por qué saber que existe.
public sealed partial class SegmentedLog
{
    /// <summary>Estado de un tópico: sus segmentos en orden y hasta dónde está confirmado.</summary>
    private sealed class TopicLog
    {
        public TopicLog(string dataDir, string topic, int partition = 0)
        {
            Topic = topic;
            Partition = partition;
            DirectoryPath = Path.Combine(dataDir, topic);
            CheckpointFile = Path.Combine(DirectoryPath, CheckpointFileName);
            LogStartFile = Path.Combine(DirectoryPath, LogStartFileName);
            LogStartTempFile = Path.Combine(DirectoryPath, LogStartTempFileName);
        }

        /// <summary>Nombre del tópico. Se usa para que los mensajes de log digan de cuál hablan.</summary>
        public string Topic { get; }

        /// <summary>En este alcance hay un solo log por tópico; la partición es siempre 0.</summary>
        public int Partition { get; }

        public Lock Gate { get; } = new();

        /// <summary>Quien agrupa los <c>fsync</c> de este tópico y sabe hasta dónde llega el disco.</summary>
        public GroupCommitWriter Writer { get; } = new();

        public string DirectoryPath { get; }

        /// <summary>Archivo donde sobrevive el high watermark entre reinicios.</summary>
        public string CheckpointFile { get; }

        /// <summary>Archivo donde sobrevive <see cref="LogStart"/> entre reinicios.</summary>
        public string LogStartFile { get; }

        /// <summary>Donde se arma <see cref="LogStartFile"/> antes de renombrarlo.</summary>
        public string LogStartTempFile { get; }

        /// <summary>
        /// Primer offset que el log todavía puede devolver. Es 0 hasta que la retención borra el
        /// primer segmento; de ahí en adelante, el offset base del segmento más viejo que queda.
        /// </summary>
        public long LogStart { get; set; }

        /// <summary>Segmentos ordenados por offset base. El último es donde se escribe.</summary>
        public List<Segment> Segments { get; } = [];

        /// <summary>Último offset confirmado por quórum. Se refleja en <see cref="CheckpointFile"/>.</summary>
        public long HighWatermark { get; set; } = -1;

        /// <summary>Si ya se leyeron los segmentos para reconstruir el índice.</summary>
        public bool Recovered { get; set; }

        /// <summary>
        /// Offset que le toca al próximo registro. Sin segmentos es <see cref="LogStart"/>, que en un
        /// log al que nunca le borraron nada vale 0.
        /// </summary>
        public long NextOffset => Segments.Count == 0 ? LogStart : Segments[^1].NextOffset;

        public Segment Add(long baseOffset)
        {
            var segment = new Segment(DirectoryPath, baseOffset);
            Segments.Add(segment);
            return segment;
        }

        /// <summary>Índice del segmento que contiene ese offset, o -1 si no está en ninguno.</summary>
        public int IndexOf(long offset)
        {
            for (int i = Segments.Count - 1; i >= 0; i--)
            {
                if (Segments[i].Contains(offset))
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
