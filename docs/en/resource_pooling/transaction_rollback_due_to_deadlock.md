# Transaction Rollback Due to Deadlock

## Symptom

When a deadlock occurs, involved threads enter a state of mutual lock waiting and holding. openGauss features an automatic deadlock detection mechanism. If a transaction waits for a lock longer than the configured threshold, the database identifies the deadlock and terminates one of the blocking transactions with an error. The aborted transaction is automatically rolled back, ensuring data consistency.

**Typical Scenario:**

```shell
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)
```

1.  **Session 1**: Begins a transaction and updates `id = 1` in table `t1` (without committing).

``` shell
openGauss=# begin;
BEGIN
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)

openGauss=# update t1 set id = 4 where id = 1;
UPDATE 1
```

2.  **Session 2**: Begins a transaction and updates `id = 2` in table `t1` (without committing).

``` shell
openGauss=# begin;
BEGIN

openGauss=# update t1 set id = 4 where id = 2;
UPDATE 1
```

3.  **Session 1**: Attempts to update `id = 2`. The session hangs, waiting for Session 2.

```shell
openGauss=#  update t1 set id = 4 where id = 2;
```

4.  **Session 2**: Attempts to update `id = 1`. After a short wait, it throws a deadlock error and rolls back.

```shell
openGauss=# update t1 set id = 4 where id = 1;
ERROR:  deadlock detected
DETAIL:  Process 281437031608256 waits for ShareLock on transaction 5288732; blocked by process 281437054742464.
Process 281437054742464 waits for ShareLock on transaction 5288733; blocked by process 281437031608256.
HINT:  See server log for query details.
```

## Troubleshooting Method

When a deadlock occurs, the openGauss deadlock detection mechanism waits for a period and then checks for deadlocks. If one is identified, the system raises an error for one of the deadlocked threads. The error appears as follows:

```shell
openGauss=# update t1 set id = 4 where id = 1;
ERROR:  deadlock detected
DETAIL:  Process 281437031608256 waits for ShareLock on transaction 5288732; blocked by process 281437054742464.
Process 281437054742464 waits for ShareLock on transaction 5288733; blocked by process 281437031608256.
HINT:  See server log for query details.
```

## Root Cause

A deadlock is a cyclic dependency where two or more transactions hold locks that the others need to proceed. Since all involved threads are mutually blocked, the system cannot resolve the state without external intervention.


## Solution

openGauss resolves deadlocks automatically via its built-in detector. The system cancels the youngest or least costly transaction to break the cycle. You can tune the detection sensitivity using the `deadlock_timeout` parameter. For details, see [Lock Management](../database_reference/lock_management.md).
