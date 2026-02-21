using System.Collections.Concurrent;

namespace transaction.twophasetree;

public partial class Demo
{
    public interface IDbOperation { }

    public interface IDbResponse
    {
        bool Success();
        string? Message();
        object ResponseData();
    }

    public interface IDbRecordSet
    {
        IDbRecordSet ForkChangeSet();

        /// <summary>
        /// Process an operation on this record set.
        /// If ctx is provided, the implementation should:
        ///  - Validate     => Consistency (C in ACID)
        ///  - Save result  => Durability  (D in ACID)
        /// </summary>
        /// <param name="ctx">Transaction Context</param>
        /// <param name="op">Operation</param>
        /// <returns>Response from the database operation</returns>
        IDbResponse Operation(ITransactionContext? ctx, IDbOperation op);

        void CommitChangeSet(IDbRecordSet changedSet);
    }

    public class GenericDatabaseParticipant(IDbRecordSet database)
    {
        private readonly ConcurrentDictionary<TransId, IDbRecordSet> _pendingChanges = [];

        public IDbResponse Operation(ITransactionContext? ctx, IDbOperation op)
        {
            if (ctx == null)
                return database.Operation(null, op);

            // - Operates on changeSet  => Isolation (I in ACID)
            // - ctx.Ready,ctx.Decision => Atomic    (A in ACID)
            var changeSet = _pendingChanges.GetOrAdd(ctx.GetTransId(), _ => database.ForkChangeSet());
            var response = changeSet.Operation(ctx, op);
            ctx.Ready(response.Success(), response.Message());
            ctx.Decision().Listen(vote =>
            {
                if (vote == TrDecision.Commit)
                    database.CommitChangeSet(changeSet);
                _pendingChanges.TryRemove(ctx.GetTransId(), out var _);
            });
            return response;
        }
    }
}