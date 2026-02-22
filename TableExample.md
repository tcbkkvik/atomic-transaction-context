### Application - distributed tables
---
#### Initial state - Account table

 Partition A
 | id | name  | balance | tid|
 |----|:------|--------:|----|
 | 2  | B     |  350.00 |  - |
 | 3  | C     | 9800.75 |  - |

 Partition B
 | id | name  | balance | tid|
 |:---|:------|--------:|----|
 | 1  | A     | 1200.50 |  - |

---
#### Transaction T1 Update on ChangeSet (Isolated)
   * B      balance  -50.00 =  300.00
   * A      balance -200.50 = 1000.00

 Partition A 
 | id | name  | balance | tid |
 |:---|:------|--------:|-----|
 | 2  | B     |  300.00 |  T1 - Ready|

 Partition B
 | id | name  | balance | tid |
 |:---|:------|--------:|-----|
 | 1  | A     | 1000.00 |  T1 - Ready|

---
#### TwoPhaseCommit T1
All Ready => Can Commit

 Partition A
 | id | name  | balance | tid|
 |:---|:------|--------:|----|
 | 2  | B     |  300.00 | T1 - Committed|
 | 3  | C     | 9800.75 |  - |

 Partition B
 | id | name  | balance | tid|
 |:---|:------|--------:|----|
 | 1  | A     | 1000.00 | T1 - Committed|
---