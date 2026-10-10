# GUC Parameter Description

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:13.373Z pushedAt=2026-09-21T03:38:32.510Z -->

## d\_format\_behavior\_compat\_options<a name="section203671436822"></a>

**Value Range**: String

**Default Value**: ''

**Parameter Description**: The parameter value is a comma-separated string. Only valid strings are allowed. If an invalid value is set, an error is reported after startup. Similarly, when setting the parameter, if the new value is invalid, an error is reported and the old value is not modified. Currently available options are:

- enable_sbr_identifier: Whether to allow using [] to enclose identifiers (including data types). After this option is enabled, the original array-related syntax of the kernel will be disabled.

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

- enable_table_hint_identifier: Whether to allow table_hint to be used as an identifier, such as column names and variable names. When enabled, the following hints are allowed to be used as identifiers.
The hints involved are: NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, NOEXPAND.

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

- enable_abs: Whether to allow @ to be used as an absolute value operator. openGauss supports declaring variables in the form of @object in the D database. When enabled, @ serves as the absolute value operator.

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

- default_collation: Default collation switch. When this configuration is not set, if the character set or collation of a character type field is not explicitly specified and the table-level collation is also empty, the field uses the default collation. When this configuration is set, the collation of a character type field inherits the table-level collation if it is not empty, and is set to the default collation corresponding to the database encoding if the table-level collation is empty.

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

## ANSI_NULLS<a name="section203671436823"></a>

**Value Range**: on/off

**Default Value**: on

**Parameter Description**: Used to control the behavior when a NULL value is compared with a non-NULL value. If set to on, the result of an equal or not-equal comparison between a NULL value and a NULL or non-NULL value is NULL. If set to off, the result of an equal comparison between a NULL value and a NULL value is true, and the result of an equal comparison between a NULL value and a non-NULL value is false.

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

**Value Range**: on/off

**Default Value**: off

**Parameter Description**: Used to control whether user-provided values can be inserted by explicitly specifying column names with the identity attribute in INSERT statements.

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

**Value Range**: on/off

**Default Value**: off

**Parameter Description**: Used to control whether the # sign is preferentially interpreted as an XOR operator. When set to off, it is preferentially interpreted as a temporary table symbol or a regular identifier.
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

**Value Range**: on/off

**Default Value**: off

**Parameter Description**: Used to control whether the entire transaction is rolled back or only a single SQL statement is rolled back when an SQL statement error occurs. When set to on, if an SQL statement error occurs in a transaction, the entire transaction is immediately terminated and rolled back. When set to off, if a single SQL statement error occurs in a transaction, only that single SQL statement is rolled back, and the transaction continues to execute.

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