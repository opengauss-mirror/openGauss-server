# GUC参数说明

## d\_format\_behavior\_compat\_options<a name="section203671436822"></a>

**取值范围**：字符串

**默认值**：''

**参数说明**：参数值为逗号间隔的字符串，仅允许合法字符串设定，不合法情况下，启动后报error。同样，设置时候，如果新值非法，则报error并且不修改老值。目前可选参数有：

- enable_sbr_identifier：是否允许使用 [] 包裹标识符(含数据类型)。开启以后内核原有的数组相关语法会被禁用。

```
openGauss=# set d_format_behavior_compat_options = 'enable_sbr_identifier';
SET
openGauss=# create table t1(id [int]);
CREATE TABLE
openGauss=# create table[array](a1 int);
CREATE TABLE
openGauss=# select ARRAY[1,2,3];
ERROR:  syntax error at or near "[1,2,3]"
```

- enable_table_hint_identifier：是否允许table_hint当做标识符，用于列名、变量名等。开启后允许下述hint用作标识符。
涉及的hint有：NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

```
openGauss=# set d_format_behavior_compat_options = 'enable_table_hint_identifier';
SET
openGauss=# create table testhint(nowait int);
CREATE TABLE
openGauss=# insert into testhint (nowait) values(1);
INSERT 0 1
openGauss=# select max(nowait) from testhint;
 max
-----
   1
(1 row)

openGauss=# set d_format_behavior_compat_options = '';
SET
openGauss=# create table testhint(nowait int);
ERROR:  syntax error at or near "("
LINE 1: create table testhint(nowait int);
                             ^
```

- enable_abs：是否允许@作为取绝对值操作符使用。openGauss支持在D库下以@object方式声明变量，开启后@为取绝对值操作符。

```
openGauss=# create table test(@a int, b int);
CREATE TABLE
openGauss=# insert into test values (-1,-2);
INSERT 0 1
openGauss=# select @a from test;
 @a
----
 -1
(1 row)

openGauss=# set d_format_behavior_compat_options = 'enable_abs';
SET
openGauss=# select @b as abs_b from test;
 abs_b
-------
     2
(1 row)
```

- default_collation：默认字符序开关。不设置此配置时，在未显式指定字符类型字段的字符集或字符序且表级字符序也为空时，字段为default字符序；设置此配置时，字符类型字段的字符序当表级字符序不为空时继承表级字符序，为空时设置为数据库编码对应的默认字符序。

```
openGauss=# SET d_format_behavior_compat_options = '';
SET
openGauss=# CREATE TABLE t1 (name varchar(50));
CREATE TABLE
openGauss=# \d+ t1
                                 Table "public.t1"
 Column |         Type          | Modifiers | Storage  | Stats target | Description 
--------+-----------------------+-----------+----------+--------------+-------------
 name   | character varying(50) |           | extended |              | 
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# SET d_format_behavior_compat_options = 'default_collation';
SET
openGauss=# CREATE TABLE t2 (name varchar(50));
CREATE TABLE
openGauss=# \d+ t2
                                                   Table "public.t2"
 Column |         Type          |                   Modifiers                   | Storage  |
 Stats target | Description 
--------+-----------------------+-----------------------------------------------+----------+
--------------+-------------
 name   | character varying(50) | character set UTF8 collate utf8mb4_general_ci | extended |
              | 
Has OIDs: no
Options: orientation=row, compression=no, collate=1537
Character Set: UTF8
Collate: utf8mb4_general_ci

```

- disable_target_alias：是否允许使用等号别名语法。开启以后内核对等号处理保持原来语法。

```
openGauss=# SET d_format_behavior_compat_options = '';
SET
openGauss=# select a = 1;
 a
---
 1
(1 row)
openGauss=# SET d_format_behavior_compat_options = 'disable_target_alias';
SET
openGauss=# select a = 1;
ERROR:  column "a" does not exist
LINE 1: select a = 1;
               ^
```

## ANSI_NULLS<a name="section203671436823"></a>

**取值范围**：on/off

**默认值**：on

**参数说明**：用于控制NULL值与非NULL值比较时的表现。如果设置为on，那么NULL值与NULL或者非NULL值做等于或者不等于比较结果都是NULL。如果设置成off，NULL值与NULL值做等于比较结果为true，NULL值与非NULL值做等于比较时结果为false。

```
openGauss=# set ANSI_NULLS on;
SET
openGauss=# select NULL = NULL;
 ?column?
----------

(1 row)

openGauss=# select 1 = NULL;
 ?column?
----------

(1 row)

openGauss=# select NULL <> NULL;
 ?column?
----------

(1 row)

openGauss=# select 1 <> NULL;
 ?column?
----------

(1 row)

openGauss=# set ANSI_NULLS off;
SET
openGauss=# select NULL = NULL;
 ?column?
----------
 t
(1 row)

openGauss=# select 1 = NULL;
 ?column?
----------
 f
(1 row)

openGauss=# select NULL <> NULL;
 ?column?
----------
 f
(1 row)

openGauss=# select 1 <> NULL;
 ?column?
----------
 t
(1 row)
```

## IDENTITY_INSERT<a name="section203671436824"></a>

**取值范围**：on/off

**默认值**：off

**参数说明**：用于控制在INSERT语句中是否能通过显示指定具有identity属性的列名插入用户提供的值。

```
openGauss=# show identity_insert;
 identity_insert 
-----------------
 off
(1 row)

openGauss=# create table t_identity_0013(id int identity, name varchar(10));
NOTICE:  CREATE TABLE will create implicit sequence "t_identity_0013_id_seq_identity" for serial column "t_identity_0013.id"
CREATE TABLE
openGauss=# insert into t_identity_0013(name) values('zhangsan');
INSERT 0 1
openGauss=# insert into t_identity_0013(id, name) values(100, 'wangwu');
ERROR:  Cannot insert identity column "id"
LINE 1: insert into t_identity_0013(id, name) values(100, 'wangwu');
                                    ^
openGauss=# set identity_insert=on;
SET
openGauss=# insert into t_identity_0013(id, name) values(100, 'wangwu');
INSERT 0 1
openGauss=# select * from t_identity_0013;
 id  |   name   
-----+----------
   1 | zhangsan
 100 | wangwu
(2 rows)

```

## enable_special_operator

**取值范围**：on/off

**默认值**：off

**参数说明**：用于控制是否将#号优先解释成异或操作符，设置成off时优先解释成临时表符号或普通标识符。

```sql
openGauss=# CREATE TABLE #111 (ID INT);
CREATE TABLE
openGauss=# INSERT INTO #111 VALUES(1);
INSERT 0 1
openGauss=# set enable_special_operator = true;
SET
openGauss=# select * from #111;
ERROR:  syntax error at or near "#"
LINE 1: select * from #111;
                      ^
openGauss=#
openGauss=# set enable_special_operator = off;
SET
openGauss=#
openGauss=# select * from #111;
 id
----
  1
(1 row)

openGauss=#
```

## xact_abort

**取值范围**：on/off

**默认值**：off

**参数说明**：用于控制SQL语句发生错误时，是整个事务回滚，还是单条SQL回滚。设置成on时事务中的SQL语句发生错误，整个事务会立即终止并回滚；设置成off时事务中的单条SQL语句发生错误，仅回滚单条SQL，事务继续执行。

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

