namespace transaction.twophasetree;

public class Demo
{
    private static int _nodeIndex;

    private class AParticipantNode
    {
        private readonly int _myNodeId = ++_nodeIndex;
        private readonly string _prefix;
        private readonly string _prefixLeftA;
        private readonly string _prefixRightA;
        private int _state;

        private void Log(string msg) => Console.WriteLine(_prefix + msg);
        private void LogL(string msg) => Console.WriteLine(_prefixLeftA + msg);
        private void LogR(string msg) => Console.WriteLine(_prefixRightA + msg);

        public AParticipantNode(int initialState)
        {
            _state = initialState;
            var hSpace = new string('-', _myNodeId * 15);
            var hLine = new string('-', _myNodeId * 15);
            _prefix = $"    {hSpace}      Node{_nodeIndex} ";
            _prefixLeftA = $"    <-{hLine}    Node{_nodeIndex} ";
            _prefixRightA = $"    {hLine}->    Node{_nodeIndex} ";
        }

        public Task Update(ITransactionContext ctx, int newState)
        {
            LogR($"Starting update({ctx.GetTransId()}, value:{_state})->f({newState})");
            ctx.Decision().Listen(d =>
            {
                if (d == TrDecision.Commit)
                    _state = newState;
                LogR($"Done: {d} -> {_state}");
            });
            return Task.Run(async () =>
            {
                for (double pct = 1; pct < 100; pct += Random.Shared.Next(18, 44))
                {
                    await Task.Delay(Random.Shared.Next(0, 9));
                    if (ctx.Decision().Poll() == TrDecision.Rollback)
                    {
                        Log("Rollback... stopping update");
                        return;
                    }
                    LogL($"{pct:0.}%");
                    ctx.Progress(pct);
                }
                LogL("Ready to commit " + newState);
                ctx.Ready(true);
            });
        }
    }

    /*ACID:
        Atomic(coordinator): Either fully updated on commit or not applied at all on rollback.
            -> Two-phase commit ensures atomicity across 
                distributed services; the main feature of this demo.
        Consistent(local): Value only updated (visible outside transaction)
                if transaction commits, otherwise unchanged.
            -> Mutually consistent state across distributed services on/after commit.
        Isolated(local): Value is isolated per transaction (no interference) until Decision is made.
            -> TransId=>Value mapping ensures isolation.
        Durable(local): Once committed, the value is updated and will not be lost.
            -> Value in _transactionDict should already be in reliable storage 
                BEFORE responding ready-to-commit to TransactionCoordinator. 
                In this demo, we assume in-memory is durable for simplicity.
    */

    interface IDbOperation { }
    interface IDbResponse
    {
        bool Success();
        string? Message();
        object ResponseData();
    }
    interface IDbRecordSet
    {
        IDbRecordSet ForkChangeSet();

        /// <summary>
        /// Process an operation on this record set.
        /// If ctx is provided, the implementation..
        ///  * Should validate and save result, or fail (C+D in ACID: Consistency, Durability)
        ///  * Can use ctx.Progress(%) to signal progress.
        ///  * Can use ctx.Decision().Poll() to check for early abort. (e.g. if any participant voted no)
        /// </summary>
        /// <param name="ctx">Transaction Context</param>
        /// <param name="op">Operation</param>
        /// <returns>Response from the database operation</returns>
        IDbResponse ProcessOp(ITransactionContext? ctx, IDbOperation op);

        void CommitChangeSet(IDbRecordSet changedSet);
    }

    class GenericDbNode(IDbRecordSet database)
    {
        private readonly Dictionary<TransId, IDbRecordSet> _pendingChanges = [];

        public IDbResponse Operation(ITransactionContext? ctx, IDbOperation op)
        {
            if (ctx == null)
            {   //Non-transactional; Directly on main database
                return database.ProcessOp(null, op);
            }
            //Transactional; 
            // * Operation is on changeSet     => Isolation (I in ACID)
            // * TwoPhaseCommit (Ready, Decision) => Atomic (A in ACID)
            var changeSet = _pendingChanges
                .TryGetValue(ctx.GetTransId(), out var existingChangeSet)
                ? existingChangeSet
                : _pendingChanges[ctx.GetTransId()] = database.ForkChangeSet();

            var response = changeSet.ProcessOp(ctx, op);
            ctx.Ready(response.Success(), response.Message());
            ctx.Decision().Listen(vote =>
            {
                if (vote == TrDecision.Commit)
                    database.CommitChangeSet(changeSet);
                _pendingChanges.Remove(ctx.GetTransId());
            });
            return response;
        }
    }

    class SqlDbNode
    {
        //todo?
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