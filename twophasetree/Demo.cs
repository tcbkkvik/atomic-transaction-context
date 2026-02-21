using Microsoft.Data.Sqlite;
using Dapper;
using System.Collections.Concurrent;

namespace transaction.twophasetree;

public partial class Demo
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

    class SqlDbNode
    {
        protected readonly SqliteConnection conn = new("Data Source=:memory:");
        protected readonly ConcurrentDictionary<TransId, SqliteTransaction> _pending = [];
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
            => _pending.GetOrAdd(tid, _ => conn.BeginTransaction());
    }

    class SqlDbNode_FullContext : SqlDbNode
    {
        public object? Op(ITransactionContext ctx)
        {
            var response = conn.Query("SELECT * FROM Users",
                transaction: GetOrCreateTransaction(ctx.GetTransId())).ToList();
            ctx.Ready(response != null);
            ctx.Decision().Listen(vote =>
            {
                if (_pending.TryGetValue(ctx.GetTransId(), out var transaction))
                {
                    if (vote == TrDecision.Commit) transaction.Commit();
                    else transaction.Rollback();
                    _pending.TryRemove(ctx.GetTransId(), out var _);
                    transaction.Dispose();
                }
            });
            return response;
        }
    }

    class SqlDbNode_TidOnly : SqlDbNode
    {
        public object? Op(TransId tid, string sql, object? param = null)
        {
            return conn.Query("SELECT * FROM Users",
                transaction: GetOrCreateTransaction(tid)).ToList();
        }
        public void Decided(TransId tid, TrDecision vote)
        {
            if (_pending.TryGetValue(tid, out var transaction))
            {
                if (vote == TrDecision.Commit) transaction.Commit();
                else transaction.Rollback();
                _pending.TryRemove(tid, out var _);
                transaction.Dispose();
            }
        }
    }

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

        private readonly ConcurrentDictionary<TransId, string> _transactionIdMap = [];
        private string ToLocalTrId(TransId tid) => _transactionIdMap.GetOrAdd(tid, _ => $"{tid.Id}_{name}");

        //Public Proxy methods; maps global=>specific TID before real(remote) DB call.
        public (bool Success, object result) Op(TransId tid, object op)
            => RemoteOp(ToLocalTrId(tid), op);
        public void Decide(TransId tid, TrDecision vote)
            => RemoteDecided(ToLocalTrId(tid), vote);
    }

    class SimulateDatabaseCluster
    {
        readonly DbProxy _sqlServer = new("SqlServer");
        readonly DbProxy _partnerDb = new("PartnerDb");
        readonly SqlDbNode_FullContext _sqlDbNode_ctx = new();
        readonly SqlDbNode_TidOnly _sqlDbNode_tid = new();

        public void CrossSiteDb2PCExample()
        {
            Console.WriteLine("\nCrossSiteDatabaseOp");
            //Shared transaction ID across multiple services/databases.
            TransId sharedTransId = TidGenerator.Create();

            //Phase 1
            var (success1, _) = _partnerDb.Op(sharedTransId, new { Op = "Update", Table = "Users", Id = 123, NewAlias = "Alice1" });
            var (success2, _) = _sqlServer.Op(sharedTransId, new { Op = "Insert", Table = "AuditLog", UserId = 123, Action = "NameChange", Timestamp = DateTime.UtcNow });
            var coordinator = TransactionContext.InitiateDefaultController(sharedTransId, Console.WriteLine);
            object? resultV1 = _sqlDbNode_ctx.Op(coordinator);
            object? resultV2 = _sqlDbNode_tid.Op(sharedTransId, "UPDATE Users SET Alias='Alice1' WHERE Id=123");
            object? resultV3 = _sqlDbNode_tid.Op(sharedTransId, "");

            //Phase 2
            var allSucceeded = success1 && success2
                && resultV1 != null && resultV2 != null;
            var vote = allSucceeded ? TrDecision.Commit : TrDecision.Rollback;
            Console.WriteLine($"Overall decision: {vote}");

            _partnerDb.Decide(sharedTransId, vote);
            _sqlServer.Decide(sharedTransId, vote);
            coordinator.Ready(vote == TrDecision.Commit);
            _sqlDbNode_tid.Decided(sharedTransId, vote);
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

        //Phase 1
        var task1 = s1.Update(controller.Branch(), 450);
        var task2 = s2.Update(controller.Branch(), 7);

        //Phase 2
        Task.WaitAll(task1, task2);
        controller.Ready(true);
    }
}