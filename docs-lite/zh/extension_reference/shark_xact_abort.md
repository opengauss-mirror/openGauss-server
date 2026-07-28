# XACT_ABORT

## d\_format\_behavior\_compat\_options<a name="section203671436822"></a>

**功能描述**

用于控制SQL语句发生错误时，是整个事务回滚，还是单条SQL回滚。设置成on时事务中的SQL语句发生错误，整个事务会立即终止并回滚；设置成off时事务中的单条SQL语句发生错误，仅回滚单条SQL，事务继续执行。

**注意事项**

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。

- 新增支持XACT_ABORT语法。

**语法格式**

```
set xact_abort { on | off };
```

**示例**

```sql
openGauss=# create table t1(id int primary key, name VARCHAR(100));
CREATE TABLE
openGauss=# insert into t1 values(1, 'zhangsan');
INSERT 0 1
openGauss=# set xact_abort to off;
SET
openGauss=# begin;
BEGIN
openGauss=# insert into t1 values(1, 'zhangsan');
ERROR:  duplicate key value violates unique constraint "t1_pkey"
DETAIL:  Key (id)=(1) already exists.
openGauss=# insert into t1 values(2, 'lisi');
INSERT 0 1
openGauss=# end;
COMMIT
openGauss=# select * from t1;
 id |   name
----+----------
  1 | zhangsan
  2 | lisi
(2 rows)

openGauss=# delete from t1 where id=2;
DELETE 1
openGauss=# select * from t1;
 id |   name
----+----------
  1 | zhangsan
(1 row)

openGauss=# set xact_abort on;
SET
openGauss=# begin;
BEGIN
openGauss=# insert into t1 values(1, 'zhangsan');
ERROR:  duplicate key value violates unique constraint "t1_pkey"
DETAIL:  Key (id)=(1) already exists.
openGauss=# insert into t1 values(2, 'lisi');
ERROR:  current transaction is aborted, commands ignored until end of transaction block, firstChar[Q]
openGauss=# end;
ROLLBACK
openGauss=# select * from t1;
 id |   name
----+----------
  1 | zhangsan
(1 row)

```
