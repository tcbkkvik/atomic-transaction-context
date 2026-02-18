namespace transaction.twophasetree;

public class TransactionContext : ITransactionContext
{
    readonly TrChannels _parentCh;
    readonly TrChannels _internalCh;
    readonly List<TrChannels> _children;

    public TransactionContext(TrChannels parentCh)
    {
        _parentCh = parentCh;
        _internalCh = new(parentCh.TransId);
        _children = [_internalCh];
        _parentCh._decision.Listen(decision =>
            _children.ForEach(br => br._decision.Push(decision))
        );
    }

    public TransId GetTransId() => _parentCh.TransId;

    public void Progress(double percent)
    {
        _internalCh._progress.Push(percent);
        SignalProgress();
    }

    private void SignalProgress() => _parentCh._progress.Push(_children.Average(br => br._progress.Poll()));

    public void Ready(bool readyToCommit, string? message = null)
    {
        var result = new PartResult(readyToCommit, message);
        _internalCh._result.Push(result);
        SignalResult(result);
    }

    private void SignalResult(PartResult res)
    {
        if (!res.CanCommit || _children.All(ch => ch._result.Poll()?.CanCommit ?? false))
        {
            if (res.CanCommit) Progress(100);
            _parentCh._result.Push(res);
        }
    }

    public IReceive<TrDecision> Decision() => _internalCh._decision;

    public ITransactionContext Branch()
    {
        var ch = new TrChannels(GetTransId());
        ch._progress.Listen(pct => SignalProgress());
        ch._result.Listen(SignalResult);
        _children.Add(ch);
        return new TransactionContext(ch);
    }

    public static ITransactionContext InitiateDefaultController(TransId transId, Action<string> Log)
    {
        var ch = new TrChannels(transId);
        ch._progress.Listen(pct =>
        {
            Log($"{pct:0.}%");
        });
        ch._result.Listen(receive =>
        {
            Log($"Accumulated result from participants: {receive}");
            ch._decision.Push(receive.ToDecision());
        });
        return new TransactionContext(ch);
    }
}