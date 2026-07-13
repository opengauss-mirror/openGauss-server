# 因死锁导致事务回滚的问题

## 一、问题现象

死锁发生后，死锁相关的线程由于互相等锁，且互相占锁的状态。openGauss拥有死锁检测功能，当业务等锁超过一段时间后，会自动检测是否存在死锁问题。如果识别出死锁问题，数据库会对其中一个死锁线程报错处理。报错事务会自动回滚，事务内的已有修改不会生效，保证数据一致性。
下面以典型的死锁场景为例：
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

首先创建session1开启事务，并且执行update修改t1表中id=1的元组，但是不提交。

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

然后创建session2开启事务，并且执行update修改t1表中id=2的元组，但是不提交。

``` shell
openGauss=# begin;
BEGIN

openGauss=# update t1 set id = 4 where id = 2;
UPDATE 1
```

然后session1执行update修改t1表中id=2的元组，会话会卡住。

```shell
openGauss=#  update t1 set id = 4 where id = 2;
```

然后session2执行update修改t1表中id=1的元组，卡住一会儿后，会报错并事务回滚。

```shell
openGauss=# update t1 set id = 4 where id = 1;
ERROR:  deadlock detected
DETAIL:  Process 281437031608256 waits for ShareLock on transaction 5288732; blocked by process 281437054742464.
Process 281437054742464 waits for ShareLock on transaction 5288733; blocked by process 281437031608256.
HINT:  See server log for query details.
```

## 二、定位方法

死锁发生时，openGauss的死锁检测机制会等锁一段时间后，检查是否存在死锁问题，如果存在，会对其中一个死锁线程报错处理。报错如下图所示：

```shell
openGauss=# update t1 set id = 4 where id = 1;
ERROR:  deadlock detected
DETAIL:  Process 281437031608256 waits for ShareLock on transaction 5288732; blocked by process 281437054742464.
Process 281437054742464 waits for ShareLock on transaction 5288733; blocked by process 281437031608256.
HINT:  See server log for query details.
```

## 三、问题根因

死锁发生后，死锁相关的线程由于互相等锁，且互相占锁，除非介入外力，否则没有办法自行解除死锁状态。

## 四、解决方案

openGauss拥有死锁检测功能，当业务等锁超过一段时间后，会自动检测是否存在死锁问题。如果识别出死锁问题，数据库会对其中一个死锁线程报错处理。openGauss使用参数deadlock_timeout控制死锁检测时间，具体参数说明可以参考[锁管理](../database_reference/lock_management.md)。
