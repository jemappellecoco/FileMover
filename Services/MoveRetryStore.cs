using System.Collections.Concurrent;

namespace FileMoverWeb.Services
{
    public interface IMoveRetryStore
    {
        int IncrementFail(int historyId, int statusCode, string? errorMessage);
        void Clear(int historyId);

        bool TryGet(int historyId, out MoveRetryInfo info);
    }

    public sealed class MoveRetryInfo
    {
        public int FailCount { get; set; }
        public int LastStatusCode { get; set; }
        public string? LastError { get; set; }
    }

    public sealed class MoveRetryStore : IMoveRetryStore
    {
        private readonly ConcurrentDictionary<int, MoveRetryInfo> _map = new();

        public int IncrementFail(int historyId, int statusCode, string? errorMessage)
        {
            var info = _map.AddOrUpdate(
                historyId,
                _ => new MoveRetryInfo
                {
                    FailCount = 1,
                    LastStatusCode = statusCode,
                    LastError = errorMessage
                },
                (_, current) =>
                {
                    current.FailCount++;
                    current.LastStatusCode = statusCode;
                    current.LastError = errorMessage;
                    return current;
                });

            return info.FailCount;
        }

        public void Clear(int historyId)
        {
            _map.TryRemove(historyId, out _);
        }

        public bool TryGet(int historyId, out MoveRetryInfo info)
            => _map.TryGetValue(historyId, out info!);
    }
}
