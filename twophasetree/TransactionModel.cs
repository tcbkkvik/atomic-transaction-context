namespace transaction.twophasetree;

public record TransId(string Id)
{
    public override string ToString() => "TID:" + Id;
}

public class TransIdGenerator(string site)
{
    private int _sequence;
    private int _prevSecond;

    public TransId Create()
    {
        var dt = DateTime.UtcNow;
        if (dt.Second != _prevSecond)
        {
            _prevSecond = dt.Second;
            _sequence = 0;
        }
        var ts = dt.ToString("yyyyMMdd_HHmmss");
        return new TransId($"{ts}_{_sequence++}_{site}");
    }
}

public enum TrDecision
{
    None,
    Commit,
    Rollback,
}

public record PartResult(bool CanCommit, string? Message)
{
    public override string ToString() => $"(Success:{CanCommit}, Message:'{Message}')";
    public TrDecision ToDecision() => CanCommit ? TrDecision.Commit : TrDecision.Rollback;
}

public interface IReceive<T>
{
    T? Poll();
    void Listen(Action<T> action);
}

public class SimpleChannel<T> : IReceive<T>
{
    private T? _value;
    private Action<T>? _action;
    public void Push(T value)
    {
        _value = value;
        _action?.Invoke(value);
    }
    public T? Poll() => _value;
    public void Listen(Action<T> action)
    {
        _action = action;
        if (_value is not null) _action?.Invoke(_value);
    }
}

public class TrChannels(TransId transId)
{
    public TransId TransId = transId;
    public SimpleChannel<PartResult> _result = new();
    public SimpleChannel<double> _progress = new();
    public SimpleChannel<TrDecision> _decision = new();
}
