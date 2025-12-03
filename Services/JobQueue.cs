using System.Threading.Channels;

namespace FileMoverWeb.Services
{
    public record MoveRequest(string JobId, string SourcePath, string DestObjectPath);

    public sealed class JobQueue
    {
        private readonly Channel<MoveRequest> _ch = Channel.CreateUnbounded<MoveRequest>();
        public ValueTask EnqueueAsync(MoveRequest req, CancellationToken ct = default)
            => _ch.Writer.WriteAsync(req, ct);
        public IAsyncEnumerable<MoveRequest> DequeueAllAsync(CancellationToken ct)
            => _ch.Reader.ReadAllAsync(ct);
    }
}
