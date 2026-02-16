namespace transaction.twophasetree;

class TrChannels(TransId transId)
{
    public TransId TransId = transId;
    public Channel<PartResult> _resultChannel = new();
    public Channel<double> _progressChannel = new();
    public Channel<TrDecision> _decisionChannel = new();
    public ITransactionContext InstantiateContext()
        => new TransactionContext(this);
}

class TransactionParticipant(TrChannels ch)
    : ITransactionParticipant
{
    public IReceive<double> Progress => ch._progressChannel;
    public IReceive<PartResult> Result => ch._resultChannel;
    public void Decided(TrDecision decision)
    {
        if (decision != TrDecision.None)
            ch._decisionChannel.Push(decision);
    }
}

class TransactionContext : ITransactionContext
{
    private readonly TransId _transId;
    private readonly TrChannels _parent;
    private readonly Channel<TrDecision> _decision = new();
    private readonly List<ITransactionParticipant> _branches = [];
    private PartResult? _localResult;
    private double _localProgress;

    public TransactionContext(TrChannels ch)
    {
        _parent = ch;
        _transId = ch.TransId;
        _parent._decisionChannel.Listen(decision =>
        {
            _decision.Push(decision);
            _branches.ForEach(p => p.Decided(decision));
        });
    }

    public TransId GetTransId() => _transId;

    public void SendProgress(double percent)
    {
        _localProgress = percent;
        TransmitProgress();
    }
    public void SendResult(bool success, string? message = null)
    {
        if (success) SendProgress(100);
        TransmitResult(_localResult = new PartResult(success, message));
    }
    private void TransmitProgress()
    {
        var sum = _branches.Sum(p => p.Progress.Poll());
        _parent._progressChannel.Push((_localProgress + sum) / (1.0 + _branches.Count));
    }
    private void TransmitResult(PartResult result) //up tree
    {
        static bool Good(PartResult? r) => r?.Success ?? false;
        if ((Good(_localResult)
                && _branches.All(ctx => Good(ctx.Result.Poll()))
            ) || !result.Success)
        {
            _parent._resultChannel.Push(result);
        }
    }
    public IReceive<TrDecision> Decision() => _decision;
    public ITransactionContext Branch()
    {
        var cm = new TrChannels(_transId);
        var tp = new TransactionParticipant(cm);
        tp.Result.Listen(receive => TransmitResult(receive));
        tp.Progress.Listen(_ => TransmitProgress());
        _branches.Add(tp);
        return cm.InstantiateContext();
    }
}
