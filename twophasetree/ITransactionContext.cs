namespace transaction.twophasetree;

public interface ITransactionContext
{
    TransId GetTransId();
    void Progress(double percent);
    void Ready(bool readyToCommit, string? message = null);
    IReceive<TrDecision> Decision();
    ITransactionContext Branch();
}
