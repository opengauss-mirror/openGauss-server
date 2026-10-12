# Transaction Rollback Due to Lock Timeout

## Symptom

If a transaction encounters a lock timeout in openGauss, the transaction is rolled back. Any modifications already performed within the transaction will not take effect, thereby ensuring data consistency.

The following example illustrates a typical lock timeout scenario within a transaction:

Prepare the following table for testing:

```shell
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)
```

First, create **Session 1**, begin a transaction, and execute an `UPDATE` to modify a tuple in table `t1`, but do not commit.

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

Next, create **Session 2**, begin a transaction, and execute an `UPDATE` targeting the same tuple. The session will hang. If Session 1 neither commits nor rolls back, Session 2 will remain blocked until a lock timeout occurs.

```shell

openGauss=# begin;
BEGIN

openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)

openGauss=#  update t1 set id = 4 where id = 1;
ERROR:  Lock wait timeout: thread 281429480878016 on node dn_6001_6002_6003 waiting for ShareLock on transaction 528
DETAIL:  blocked by hold lock thread 281429251764160, statement <update t1 set id = 4 where id = 1;>, hold lockmode

```

Roll back Session 1 and requery table `t1`. The error confirms that the modifications within the rolled-back transaction did not affect the table, thus preserving data consistency.

```shell
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)
```

## Troubleshooting Method

openGauss provides the following views for troubleshooting lock timeout issues:

If threads are still waiting for locks, you can query `pg_thread_wait_status` to check the database wait time, waiting sessions, and lock information IDs. 
As shown below, the fourth row corresponds to the wait event for Session 2.

```shell
openGauss=# select * from pg_thread_wait_status ;
     node_name     | db_name  |      thread_name       |     query_id      |       tid       |    sessionid    |  lwtid  | psessionid | tlevel | smpid | wait_status | wait_event | locktag | lockmode | block_sessionid | global_sessionid
-------------------+----------+------------------------+-------------------+-----------------+-----------------+---------+------------+--------+-------+-------------+------------+---------+----------+-----------------+------------------
 dn_6001_6002_6003 |          | dn_6001                |                 0 | 281430024040384 | 281430024040384 |  964304 |            |      0 |     0 | none        | none       |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   | 24206847998934166 | 281429372809152 | 281429372809152 | 1412474 |            |      0 |     0 | none        | none       |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   |                 0 | 281429251764160 | 281429251764160 | 1262406 |            |      0 |     0 | wait cmd    | wait cmd   |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   |                 0 | 281429480878016 | 281429480878016 | 1142447 |            |      0 |     0 | wait cmd    | wait cmd   |         |          |                 | 0:0#0
```

Additionally, you can use `pg_locks` to identify lock holders, which helps determine why a lock is held or whether it has been released. 
Based on the `pid`, you can identify the locks currently held by Session 1. In this case, Session 1 holds a page-level write lock on page 0 of the table with `oid=90118`, where 90118 corresponds to table `t1`.

```shell
openGauss=# select oid, * from pg_class where relname = 't1';
  oid  | relname | relnamespace | reltype | reloftype | relowner | relam | relfilenode | reltablespace | relpages | reltuples | relallvisible | reltoastrelid | reltoastidxid | reldeltarelid | reldeltaidx | relcudescrelid | relcudescidx | relhasindex | relisshar
ed | relpersistence | relkind | relnatts | relchecks | relhasoids | relhaspkey | relhasrules | relhastriggers | relhassubclass | relcmprs | relhasclusterkey | relrowmovement | parttype | relfrozenxid | relacl |                 reloptions                  | relr
eplident | relfrozenxid64 | relbucket | relbucketkey | relminmxid
-------+---------+--------------+---------+-----------+----------+-------+-------------+---------------+----------+-----------+---------------+---------------+---------------+---------------+-------------+----------------+--------------+-------------+----------
---+----------------+---------+----------+-----------+------------+------------+-------------+----------------+----------------+----------+------------------+----------------+----------+--------------+--------+---------------------------------------------+-----
---------+----------------+-----------+--------------+------------
 90118 | t1      |         2200 |   90120 |         0 |       10 |     0 |        4982 |             0 |        0 |         0 |             0 |         90121 |             0 |             0 |           0 |              0 |            0 | f           | f
   | p              | r       |        2 |         0 | f          | f          | f           | f              | f              | 1        | f                | f              | n        | 5276482      |        | {orientation=row,compression=no,segment=on} | d
         |        5276482 |         2 |              |          2
(1 row)

```

```shell
openGauss=# select * from pg_locks ;
   locktype    | database | relation | page | tuple | bucket | virtualxid | transactionid | classid | objid | objsubid | virtualtransaction |       pid       |    sessionid    |       mode       | granted | fastpath |      locktag       | global_sessionid
---------------+----------+----------+------+-------+--------+------------+---------------+---------+-------+----------+--------------------+-----------------+-----------------+------------------+---------+----------+--------------------+------------------
 relation      |    15208 |    12191 |      |       |        |            |               |         |       |          | 78/68              | 281429372809152 | 281429372809152 | AccessShareLock  | t       | t        | 3b68:2f9f:0:0:0:0  | 0:0#0
 virtualxid    |          |          |      |       |        | 78/68      |               |         |       |          | 78/68              | 281429372809152 | 281429372809152 | ExclusiveLock    | t       | t        | 4e:44:0:0:0:7      | 0:0#0
 relation      |    15208 |    90118 |      |       |        |            |               |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | AccessShareLock  | t       | t        | 3b68:16006:0:0:0:0 | 0:0#0
 relation      |    15208 |    90118 |      |       |        |            |               |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | RowExclusiveLock | t       | t        | 3b68:16006:0:0:0:0 | 0:0#0
 virtualxid    |          |          |      |       |        | 76/5677    |               |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | ExclusiveLock    | t       | t        | 4c:162d:0:0:0:7    | 0:0#0
 relation      |    15208 |     2685 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a7d:0:0:0:0   | 0:0#0
 relation      |    15208 |     2684 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a7c:0:0:0:0   | 0:0#0
 relation      |    15208 |     2702 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a8e:0:0:0:0   | 0:0#0
 relation      |    15208 |     2701 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a8d:0:0:0:0   | 0:0#0
 relation      |    15208 |     2700 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a8c:0:0:0:0   | 0:0#0
 relation      |    15208 |     2699 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a8b:0:0:0:0   | 0:0#0
 relation      |    15208 |     9981 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:26fd:0:0:0:0  | 0:0#0
 relation      |    15208 |     2663 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a67:0:0:0:0   | 0:0#0
 relation      |    15208 |     2662 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a66:0:0:0:0   | 0:0#0
 relation      |    15208 |     2615 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a37:0:0:0:0   | 0:0#0
 relation      |    15208 |     2620 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:a3c:0:0:0:0   | 0:0#0
 relation      |    15208 |     1259 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:4eb:0:0:0:0   | 0:0#0
 relation      |    15208 |    90118 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | AccessShareLock  | t       | t        | 3b68:16006:0:0:0:0 | 0:0#0
 relation      |    15208 |    90118 |      |       |        |            |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | RowExclusiveLock | t       | t        | 3b68:16006:0:0:0:0 | 0:0#0
 virtualxid    |          |          |      |       |        | 77/70      |               |         |       |          | 77/70              | 281429251764160 | 281429251764160 | ExclusiveLock    | t       | t        | 4d:46:0:0:0:7      | 0:0#0
 transactionid |          |          |      |       |        |            |       5282602 |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | ExclusiveLock    | t       | f        | 509b2a:0:0:0:0:6   | 0:0#0
 tuple         |    15208 |    90118 |    0 |     1 |      0 |            |               |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | ExclusiveLock    | t       | f        | 3b68:16006:0:1:0:5 | 0:0#0
 transactionid |          |          |      |       |        |            |       5282601 |         |       |          | 77/70              | 281429251764160 | 281429251764160 | ExclusiveLock    | t       | f        | 509b29:0:0:0:0:6   | 0:0#0
 transactionid |
```

```shell
 tuple         |    15208 |    90118 |    0 |     1 |      0 |            |               |         |       |          | 76/5677            | 281429480878016 | 281429480878016 | ExclusiveLock    | t       | f        | 3b68:16006:0:1:0:5 | 0:0#0
```

## Root Cause

If a transaction encounters a lock timeout in openGauss, the transaction is rolled back. Any modifications already performed within the transaction will not take effect, thereby ensuring data consistency.

## Solution

Generally, lock timeout issues are related to specific business scenarios. It is recommended to first use the views mentioned above to identify the lock-holding threads, and then analyze the specific scenario—for example, checking for long-running transactions performing write operations or concurrent business threads modifying the same data, which leads to mutual blocking.

Additionally, in **resource pooling mode**, since multiple nodes share the same data, lock conflicts may arise from read/write operations on the same page across different nodes. Special attention should be paid to concurrent modification behaviors targeting the same page across nodes. In high-concurrency scenarios, lock timeouts may also occur.