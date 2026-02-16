namespace transaction.twophasetree;

public class Demo
{
    private static readonly TransIdGenerator _transIdGen = new("Demo");

    class SomeApiService(int value)
    {
        private static int _nodeIndex;
        private readonly string _prefix = $"{" ".PadLeft(++_nodeIndex * 20)}Node{_nodeIndex} ";
        private void Log(string msg) => Console.WriteLine(_prefix + msg);
        private readonly Dictionary<TransId, int> _transactionValues = [];

        public Task Update(ITransactionContext ctx, int newValue)
        {
            var tid = ctx.GetTransId();
            if (!_transactionValues.TryAdd(tid, newValue))
            {
                Log($"{tid} exists, idempotent: ignoring update");
                return Task.CompletedTask;
            }
            Log($"Starting update({tid}, value:{value})->{newValue}");
            ctx.Decision().Listen(d =>
            {
                if (d == TrDecision.None)
                    return;
                if (d == TrDecision.Commit) value = newValue;
                _transactionValues.Remove(tid);
                Log($"Done: {d} -> {value}");
            });

            return Task.Run(async () =>
            {
                for (double pct = 1; pct < 100; pct += 14)
                {
                    await Task.Delay(Random.Shared.Next(1, 9));
                    if (ctx.Decision().Poll() == TrDecision.Rollback)
                    {
                        Log("Aborting");
                        return;
                    }
                    Log($"{pct:0.}%");
                    ctx.SendProgress(pct);
                }
                Log("Ready to commit");
                ctx.SendResult(true);
            });
        }
    }

    public static void TestTransIdGenerator()
    {
        for (int i = 0; i < 4; i++)
        {
            System.Threading.Thread.Sleep(300);
            var tid = _transIdGen.Create();
            Console.WriteLine(tid);
        }
    }

    public static void Run()
    {
        TestTransIdGenerator();
        var ch = new TrChannels(_transIdGen.Create());
        var ctx = ch.InstantiateContext();

        var part = new TransactionParticipant(ch);
        part.Progress.Listen(progress => { Console.WriteLine($"{progress:0.}%"); });
        part.Result.Listen(receive =>
        {
            Console.WriteLine($"\n Root <- {receive}");
            part.Decided(receive.ToDecision());
        });

        var s1 = new SomeApiService(200);
        var s2 = new SomeApiService(5);
        var task1 = s1.Update(ctx.Branch(), 450);
        var task2 = s2.Update(ctx.Branch(), 7);
        Task.WaitAll(task1, task2);

        ctx.SendResult(true);
    }
}