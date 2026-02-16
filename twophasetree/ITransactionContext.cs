namespace transaction.twophasetree;

public interface ITransactionContext //child side dialog
{
    TransId GetTransId();
    void SendProgress(double percent);
    void SendResult(bool success, string? message = null);
    IReceive<TrDecision> Decision();
    ITransactionContext Branch();
}

public interface ITransactionParticipant //parent side dialog
{
    IReceive<double> Progress { get; }
    IReceive<PartResult> Result { get; }
    void Decided(TrDecision decision);
}

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
    // Pause,
    // Continue,
}

public record PartResult(bool Success, string? Message)
{
    public override string ToString()
    {
        return $"(Success:{Success}, Message:'{Message}')";
    }
    public TrDecision ToDecision() => Success
        ? TrDecision.Commit : TrDecision.Rollback;
}

public interface IReceive<T>
{
    T? Poll();
    void Listen(Action<T> action);
}

// public interface ISend<T>
// {
//     void Push(T value);
// }

public class Channel<T> : IReceive<T> //, ISend<T>
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
