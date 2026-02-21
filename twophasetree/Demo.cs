using Microsoft.Data.Sqlite;
using Dapper;

namespace transaction.twophasetree;

public class Demo
{
    private static int _nodeIndex;

    private class AParticipantNode
    {
        private readonly int _myNodeId = ++_nodeIndex;
        private readonly string _prefix_Space;
        private readonly string _prefixLeft_A;
        private readonly string _prefixRightA;
        private int _state;

        private void Log(string msg) => Console.WriteLine(_prefix_Space + msg);
        private void LogL(string msg) => Console.WriteLine(_prefixLeft_A + msg);
        private void LogR(string msg) => Console.WriteLine(_prefixRightA + msg);

        public AParticipantNode(int initialState)
        {
            _state = initialState;
            var hSpce = new string(' ', _myNodeId * 15);
            var hLine = new string('-', _myNodeId * 15);
            _prefix_Space = $"         {hSpce}  Node{_nodeIndex} ";
            _prefixLeft_A = $"      <<-{hLine}  Node{_nodeIndex} ";
            _prefixRightA = $"      {hLine}->>  Node{_nodeIndex} ";
        }

        public Task Update(ITransactionContext ctx, int newState)
        {
            LogR($"Starting update({ctx.GetTransId()}, value:{_state})->f({newState})");
            return Task.Run(async () =>
            {
                //Phase One: Do the work to prepare for the update, and report progress.
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
                ctx.Decision().Listen(d =>
                {
                    //Phase Two: Wait for the decision from the coordinator, and commit or rollback accordingly.
                    if (d == TrDecision.Commit)
                        _state = newState;
                    LogR($"Done: {d} -> {_state}");
                });
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
        IDbResponse Operation(ITransactionContext? ctx, IDbOperation op);

        void CommitChangeSet(IDbRecordSet changedSet);
    }

    class GenericDatabase(IDbRecordSet database)
    {
        private readonly Dictionary<TransId, IDbRecordSet> _pendingChanges = [];

        public IDbResponse Operation(ITransactionContext? ctx, IDbOperation op)
        {
            if (ctx == null)
            {   //Non-transactional; Directly on main database
                return database.Operation(null, op);
            }
            //Transactional; 
            //  - Operation is on changeSet     => Isolation (I in ACID)
            //  - TwoPhaseCommit (Ready, Decision) => Atomic (A in ACID)
            var changeSet = _pendingChanges
                .TryGetValue(ctx.GetTransId(), out var existingChangeSet)
                ? existingChangeSet
                : _pendingChanges[ctx.GetTransId()] = database.ForkChangeSet();

            //Phase One: Execute the operation on a branched changeSet, but do not commit to main database yet.
            var response = changeSet.Operation(ctx, op);

            ctx.Ready(response.Success(), response.Message());
            ctx.Decision().Listen(vote =>
            {
                //Phase Two: On commit, commit the changeSet to main database; on rollback, discard the changeSet.
                if (vote == TrDecision.Commit)
                {
                    database.CommitChangeSet(changeSet);
                }
                _pendingChanges.Remove(ctx.GetTransId());
            });
            return response;
        }
    }

    class SqlDbNode
    {
        // public class MockResponse : IDbResponse
        // {
        //     public string? Message() => "mockResp";
        //     public object ResponseData() => new { Data = "mockData" };
        //     public bool Success() => true;
        // }
        protected readonly SqliteConnection conn = new("Data Source=:memory:");
        protected readonly Dictionary<TransId, SqliteTransaction> _pending = [];
        public SqlDbNode()
        {
            conn.Open();
            var tr = conn.BeginTransaction();
            int a = conn.Execute("CREATE TABLE Users (Id INTEGER PRIMARY KEY, Name TEXT)");
            int b = conn.Execute("INSERT INTO Users (Name) VALUES (@Name)", new { Name = "Alice" });
            if (b != 1)
                throw new Exception("Setup failed");
            tr.Commit();
            tr.Dispose();

        }
        protected SqliteTransaction GetOrCreateTransaction(TransId tid)
            => _pending.TryGetValue(tid, out var transaction)
                ? transaction : (_pending[tid] = conn.BeginTransaction());
    }

    class SqlDbNode_v1 : SqlDbNode
    {
        public object? Op(ITransactionContext ctx)
        {
            //Phase One: Execute the SQL statement in a transaction, but do not commit yet.
            var trx = GetOrCreateTransaction(ctx.GetTransId());
            var response = conn.Query("SELECT * FROM Users", transaction: trx)
                .ToList();
            foreach (var row in response)
            {
                Console.WriteLine($"From select: Id={row.Id}, Name={row.Name}");
            }
            ctx.Ready(response != null);
            ctx.Decision().Listen(vote =>
            {
                if (!_pending.TryGetValue(ctx.GetTransId(), out var transaction))
                    return; //outdated decision, transaction already removed.
                //Phase Two: On commit, commit the transaction; on rollback, roll it back.
                if (vote == TrDecision.Commit)
                    transaction.Commit();
                else
                    transaction.Rollback();
                _pending.Remove(ctx.GetTransId());
                transaction.Dispose();
            });
            return response;
        }
    }

    class SqlDbNode_v2 : SqlDbNode
    {
        // Phase One
        public object? Op(TransId tid, string sql, object? param = null)
        {
            return conn.Query("SELECT * FROM Users", transaction: GetOrCreateTransaction(tid))
                .ToList();
            // return new MockResponse();
        }

        // Phase Two
        public void Decided(TransId tid, TrDecision vote)
        {
            if (_pending.TryGetValue(tid, out var transaction))
            {
                if (vote == TrDecision.Commit) transaction.Commit();
                else transaction.Rollback();
                _pending.Remove(tid);
                transaction.Dispose();
            }
        }
    }

    class SimulateDatabaseCluster
    {
        class DbProxy(string name)
        {
            private (bool Success, object result) RemoteOp(string localTID, object op)
            {
                bool success = Random.Shared.NextDouble()
                    > 0.2; //Simulate 80% success rate
                var msg = $"  Executed ({(success ? "Success" : "Failure")})"
                    + $" on {name},{localTID}: {op}";
                Console.WriteLine(msg);
                return (success, msg);
            }
            private void RemoteDecided(string localTID, TrDecision vote) => Console.WriteLine($"    {vote} -> {name},{localTID}");
            private readonly Dictionary<TransId, string> _transactionIdMap = [];
            

            private string ToLocalTrId(TransId tid) => _transactionIdMap.TryGetValue(tid, out var localTid)
                    ? localTid : (_transactionIdMap[tid] = $"{tid.Id}_{name}");
            //Public Proxy methods; maps global=>specific TID before real(remote) DB call.
            public (bool Success, object result) Op(TransId tid, object op)
                => RemoteOp(ToLocalTrId(tid), op);
            public void Decide(TransId tid, TrDecision vote)
                => RemoteDecided(ToLocalTrId(tid), vote);
        }

        readonly DbProxy _sqlServer = new("SqlServer");
        readonly DbProxy _partnerDb = new("PartnerDb");
        readonly SqlDbNode_v1 sqlDbNode_V1 = new();
        readonly SqlDbNode_v2 sqlDbNode_V2 = new();

        public void CrossSiteDb2PCExample()
        {
            Console.WriteLine("\nCrossSiteDatabaseOp");
            //Shared transaction ID across multiple services/databases.
            TransId sharedTransId = TidGenerator.Create();

            //Phase 1
            var (success1, _) = _partnerDb.Op(sharedTransId, new { Op = "Update", Table = "Users", Id = 123, NewAlias = "Alice1" });
            var (success2, _) = _sqlServer.Op(sharedTransId, new { Op = "Insert", Table = "AuditLog", UserId = 123, Action = "NameChange", Timestamp = DateTime.UtcNow });
            var coordinator = TransactionContext.InitiateDefaultController(sharedTransId, Console.WriteLine);
            object? resultV1 = sqlDbNode_V1.Op(coordinator);
            object? resultV2 = sqlDbNode_V2.Op(sharedTransId, "UPDATE Users SET Alias='Alice1' WHERE Id=123");
            object? resultV3 = sqlDbNode_V2.Op(sharedTransId, "");

            //Phase 2
            var allSucceeded = success1
                && success2
                && resultV1 != null
                && resultV2 != null
                ;
            var vote = allSucceeded ? TrDecision.Commit : TrDecision.Rollback;
            Console.WriteLine($"Overall decision: {vote}");

            _partnerDb.Decide(sharedTransId, vote);
            _sqlServer.Decide(sharedTransId, vote);
            coordinator.Ready(vote == TrDecision.Commit);
            sqlDbNode_V2.Decided(sharedTransId, vote);
        }
    }

    private static readonly TransIdGenerator TidGenerator = new("Demo");

    public static void SimulateDistributedTransaction()
    {
        SimulateDatabaseCluster cluster = new();
        for (int i = 0; i < 4; i++)
            cluster.CrossSiteDb2PCExample();

        Console.WriteLine("\nInitiateDefaultController");
        ITransactionContext controller = TransactionContext
            .InitiateDefaultController(TidGenerator.Create(), Console.WriteLine);

        var s1 = new AParticipantNode(200);
        var s2 = new AParticipantNode(5);

        var task1 = s1.Update(controller.Branch(), 450);
        var task2 = s2.Update(controller.Branch(), 7);

        Task.WaitAll(task1, task2);
        controller.Ready(true);
    }
}