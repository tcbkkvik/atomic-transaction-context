namespace transaction.twophasetree;

public class Demo
{
    private class AParticipantNode(int initialValue)
    {
        private static int _nodeIndex;
        private readonly string _prefix = $"{" ".PadLeft(++_nodeIndex * 20)}Node{_nodeIndex} ";
        private void Log(string msg) => Console.WriteLine(_prefix + msg);

        private int _value = initialValue;
        private readonly Dictionary<TransId, int> _transactionDict = []; //TID-versioned values

        /*ACID:
           Atomic: Either fully updated on commit or not applied at all on rollback.
                -> Two-phase commit ensures atomicity across 
                    distributed services; the main feature of this demo.
           Consistent: Value only updated (visible outside transaction)
                 if transaction commits, otherwise unchanged.
                -> Mutually consistent state across distributed services on/after commit.
           Isolated: Value is isolated per transaction (no interference) until Decision is made.
                -> TransId=>Value mapping ensures isolation.
           Durable: Once committed, the value is updated and will not be lost.
                -> Value in _transactionDict should already be in reliable storage 
                    BEFORE responding ready-to-commit to TransactionCoordinator. 
                    In this demo, we assume in-memory is durable for simplicity.
        */

        private static int Process(int input) => input * 2;

        public void SimpleUpdate(int newValue)
        {
            _value = newValue;
            Log($"Non-transactional update: {_value})->{newValue}");
        }

        public Task Update(ITransactionContext ctx, int newValue)
        {
            var tid = ctx.GetTransId();
            if (_transactionDict.ContainsKey(tid))
            {
                Log($"{tid} exists, idempotent: ignoring update");
                return Task.CompletedTask;
            }
            Log($"Starting update({tid}, value:{_value})->f({newValue})");
            ctx.Decision().Listen(d =>
            {
                if (d == TrDecision.None) return;
                if (d == TrDecision.Commit && _transactionDict.TryGetValue(tid, out var trxValue))
                {
                    _value = trxValue;
                }
                _transactionDict.Remove(tid);
                Log($"Done: {d} -> {_value}");
            });
            _transactionDict[tid] = _value;
            return Task.Run(async () =>
            {
                for (double pct = 1; pct < 100; pct += Random.Shared.Next(18,44))
                {
                    await Task.Delay(Random.Shared.Next(0, 9));
                    if (ctx.Decision().Poll() == TrDecision.Rollback)
                    {
                        Log("Rollback... stopping update");
                        return;
                    }
                    Log($"{pct:0.}%");
                    ctx.Progress(pct);
                }
                var processedValue = Process(newValue);
                _transactionDict[tid] = processedValue;
                Log("Ready to commit " + processedValue);
                ctx.Ready(true);
            });
        }
    }

    private static readonly TransIdGenerator TidGenerator = new("Demo");

    public static void SimulateDistributedTransaction()
    {
        var controller = TransactionContext
            .InitiateDefaultController(TidGenerator.Create(), Console.WriteLine);

        var s1 = new AParticipantNode(200);
        var s2 = new AParticipantNode(5);

        var task1 = s1.Update(controller.Branch(), 450);
        var task2 = s2.Update(controller.Branch(), 7);

        Task.WaitAll(task1, task2);
        controller.Ready(true);
    }
}