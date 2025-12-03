using System.Collections.Concurrent;

public interface ICancelStore
{
    bool ShouldCancel(int historyId);
    void Cancel(int historyId);
    void Clear(int historyId);
}

public class CancelStore : ICancelStore
{
    private readonly ConcurrentDictionary<int, bool> _canceled = new();

    public bool ShouldCancel(int historyId)
        => _canceled.ContainsKey(historyId);

    public void Cancel(int historyId)
        => _canceled[historyId] = true;

    public void Clear(int historyId)
        => _canceled.TryRemove(historyId, out _);
}
