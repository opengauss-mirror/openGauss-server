# 因锁超时导致事务回滚的问题

## 一、问题现象

openGauss如果事务内遇到锁超时问题，会导致该事务回滚，事务内的已有修改不会生效，保证数据一致性。
下面以典型的事务内锁超时场景为例：
事先准备如下表用于实验：

```shell
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)
```

首先创建session1开启事务，并且执行update修改t1表的元组，但是不提交。

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

然后创建session2开启事务，同样执行update修改同一元组，会发现会话卡住无法继续，如果session1不提交或回滚的话，session2会一直卡住直到锁超时。

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

回滚session1，并重新查询t1表。报错事务内的修改不会对t1表生效，保证数据的一致性。

```shell
openGauss=# select * from t1;
 id | value
----+-------
  1 | 111
  2 | 222
  3 | 333
(3 rows)
```

## 二、定位方法

openGauss提供了如下视图用于定位锁超时问题：

如果还存在某些线程在等锁的问题，可以通过pg_thread_wait_status查看数据库的等待时间，以及等锁会话，等锁信息id等信息。
通过tid可以看出，第四行对应session2的等待事件。

```shell
openGauss=# select * from pg_thread_wait_status ;
     node_name     | db_name  |      thread_name       |     query_id      |       tid       |    sessionid    |  lwtid  | psessionid | tlevel | smpid | wait_status | wait_event | locktag | lockmode | block_sessionid | global_sessionid
-------------------+----------+------------------------+-------------------+-----------------+-----------------+---------+------------+--------+-------+-------------+------------+---------+----------+-----------------+------------------
 dn_6001_6002_6003 |          | dn_6001                |                 0 | 281430024040384 | 281430024040384 |  964304 |            |      0 |     0 | none        | none       |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   | 24206847998934166 | 281429372809152 | 281429372809152 | 1412474 |            |      0 |     0 | none        | none       |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   |                 0 | 281429251764160 | 281429251764160 | 1262406 |            |      0 |     0 | wait cmd    | wait cmd   |         |          |                 | 0:0#0
 dn_6001_6002_6003 | postgres | gsql                   |                 0 | 281429480878016 | 281429480878016 | 1142447 |            |      0 |     0 | wait cmd    | wait cmd   |         |          |                 | 0:0#0
```

此外，可以使用pg_locks查找数据库的持锁信息，用于定位持锁原因或锁是否以被释放。
通过pid可以看出session1当前的持锁情况，其中session1持有了oid=90118表的page 0写锁，其中90118即对应表t1。

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

## 三、问题根因

openGauss如果事务内遇到锁超时问题，会导致该事务回滚，事务内的已有修改不会生效，保证数据一致性。

## 四、解决方案

一般来说，锁超时问题与实际的业务场景有关。建议首先基于以上视图查找持锁线程，再结合具体场景分析，比如是否有长事务存在写操作或业务线程并发修改相同数据导致互相阻塞等行为。
此外，资源池化模式由于多个节点使用同一份数据，因此不同节点对同一页面的读写操作会存在锁冲突，同样需要注意是否存在不同节点对同一页面并发修改的行为，在高并发场景下也可能存在锁超时现象。
