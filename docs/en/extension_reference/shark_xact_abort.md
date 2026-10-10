# XACT_ABORT

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:28.487Z pushedAt=2026-09-21T08:32:39.890Z -->

## d_format_behavior_compat_options<a name="section203671436822"></a>

**Description**

Controls whether an error in an SQL statement causes the entire transaction to roll back or only the single SQL statement to roll back. When set to on, an error in an SQL statement within a transaction immediately terminates and rolls back the entire transaction. When set to off, an error in a single SQL statement within a transaction rolls back only that statement, and the transaction continues execution.

**Precautions**

- This section includes only the syntax newly added by shark. The original openGauss syntax has not been deleted or modified.

- Added support for the XACT_ABORT syntax.

**Syntax Format**

```
set xact_abort { on | off };
```

**Example**

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