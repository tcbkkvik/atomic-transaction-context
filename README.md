
## Minimum Two Phase Commit protocol (2PC)

### 2PC TransactionContext
Minimum child->parent 2PC messaging interface
```csharp
public interface ITransactionContext
{
    TransId GetTransId();
    void Progress(double percent);
    void Ready(bool readyToCommit, string? message = null);
    IReceive<TrDecision> Decision();
    ITransactionContext Branch();
}
```
### 2PC tree
```mermaid 
flowchart
    T(2PC Coordinator
      T)
    T --> A
    T --> B
    B --> C
    B --> D
```

```mermaid
sequenceDiagram
    T->>+A: Start
    T->>+B: Start
    B->>+C: Start
    B->>+D: Start
      A-->>T: Progress%
      C-->>B: Progress%
      D-->>B: Progress%
      B-->>T: Progress%
      A->>-T: Ready
      C->>-B: Ready
      D->>-B: Ready
      B->>-T: Ready
    T->>+A: Commit
    T->>+B: Commit
    B->>+C: Commit
    B->>+D: Commit
```
