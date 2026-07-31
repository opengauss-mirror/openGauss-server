# Hint for Degrading Some Errors to Warnings<a name="EN-US_TOPIC_0245374572"></a>

## Function<a name="en-us_topic_0237121537_section290819468377"></a>

Some errors can be degraded to warnings when INSERT and UPDATE statements are executed, without affecting the statement execution.

This hint does not support column-store tables and cannot take effect in column-store tables.

>[!WARNING]CAUTION
>
>Different from other hints, this hint affects only the processing mode when the executor encounters some errors and does not affect the execution plan.

When this hint is used, errors are degraded in the following scenarios:

- **The non-null constraint is violated.**

  If the executed SQL statement violates the non-null constraint of the table, you can use this hint to degrade errors to warnings and use one of the following strategies based on the value of the GUC parameter **sql_ignore_strategy**:
  
  - If **sql_ignore_strategy** is set to **ignore_null**, the INSERT or UPDATE operations on rows that violate non-null constraints are ignored and remaining data operations are performed.
  
  - If **sql_ignore_strategy** is set to **overwrite_null**, the null value that violates the constraint is overwritten by the default value of the target type, and the remaining data operations are performed.
  
      >[!NOTE]NOTE
      >
      >For details about the GUC parameter **sql_ignore_strategy**, see [sql_ignore_strategy](https://docs.opengauss.org/en/docs/latest-lite/database_reference/miscellaneous_parameters.html).

- **The unique constraint is violated.**

  If the executed SQL statement violates the unique constraint of a table, you can use this hint to degrade errors to warnings, ignore the INSERT/UPDATE operation on the row that violates the constraint, and continue to perform the remaining data operations.
  
- **The partitioned table cannot match a valid partition.**

  When INSERT or UPDATE is performed on a partitioned table, if a row of data does not match a valid partition of the table, you can use this hint to degrade errors to warnings, ignore the row, and continue to perform operations on the remaining data.
  
- **Failed to convert the updated or inserted value to the target column type.**

  During the execution of the INSERT or UPDATE statement, if the new value does not match the type of the target column, you can use this hint to degrade errors to warnings and continue the execution based on the new value type and the target column type:
  
  - When the new value type and column type are both numeric:
  
      If the new value is within the range of the column type, insert or update the value directly. If the new value is beyond the range of the column type, replace the value with the maximum or minimum value of the column type.
   
  - When the new value type and column type are both character strings:
  
      If the length of the new value is within the range specified by the column type, insert or update the value directly. If the length of the new value is beyond the range specified by the column type, the first n characters of the column type are retained.
   
  - When the new value type cannot be converted to the column type:
  
      Insert or update the default value of a column type.

## Syntax<a name="en-us_topic_0237121537_section17380317104213"></a>

```
ignore_error
```

## Examples<a name="en-us_topic_0237121537_section1127715590585"></a>

To use the ignore_error hints, you need to create a database named **db_ignore** in B-compatible mode.

```
create database db_ignore dbcompatibility 'B';
\c db_ignore
```

- **Ignore the non-null constraint.**

```
db_ignore=# create table t_not_null(num int not null);
CREATE TABLE
-- The ignore strategy is used.
db_ignore=# set sql_ignore_strategy = 'ignore_null';
SET
db_ignore=# insert /*+ ignore_error */ into t_not_null values(null), (1);
WARNING:  null value in column "num" violates not-null constraint
DETAIL:  Failing row contains (null).
INSERT 0 1
db_ignore=# select * from t_not_null ;
 num 
-----
   1
(1 row)

db_ignore=# update /*+ ignore_error */ t_not_null set num = null where num = 1;
WARNING:  null value in column "num" violates not-null constraint
DETAIL:  Failing row contains (null).
UPDATE 0
db_ignore=# select * from t_not_null ;
 num 
-----
   1
(1 row)


-- The overwrite strategy is used.
db_ignore=# delete from t_not_null;
db_ignore=# set sql_ignore_strategy = 'overwrite_null';
SET
db_ignore=# insert /*+ ignore_error */ into t_not_null values(null), (1);
WARNING:  null value in column "num" violates not-null constraint
DETAIL:  Failing row contains (null).
INSERT 0 2
db_ignore=# select * from t_not_null ;
 num 
-----
   0
   1
(2 rows)

db_ignore=# update /*+ ignore_error */ t_not_null set num = null where num = 1;
WARNING:  null value in column "num" violates not-null constraint
DETAIL:  Failing row contains (null).
UPDATE 1
db_ignore=# select * from t_not_null ;
 num 
-----
   0
   0
(2 rows)
```

- **Ignore the unique constraint.**

```
db_ignore=# create table t_unique(num int unique);
NOTICE:  CREATE TABLE / UNIQUE will create implicit index "t_unique_num_key" for table "t_unique"
CREATE TABLE
db_ignore=# insert into t_unique values(1);
INSERT 0 1
db_ignore=# insert /*+ ignore_error */ into t_unique values(1),(2);
WARNING:  duplicate key value violates unique constraint in table "t_unique"
INSERT 0 1
db_ignore=# select * from t_unique;
 num 
-----
   1
   2
(2 rows)

db_ignore=# update /*+ ignore_error */ t_unique set num = 1 where num = 2;
WARNING:  duplicate key value violates unique constraint in table "t_unique"
UPDATE 0
db_ignore=# select * from t_unique ;
 num 
-----
   1
   2
(2 rows)
```

- **Ignore the partitioned table that cannot match a valid partition.**

```
db_ignore=# CREATE TABLE t_ignore
db_ignore-# (
db_ignore(#     col1 integer NOT NULL,
db_ignore(#     col2 character varying(60)
db_ignore(# ) WITH(segment = on) PARTITION BY RANGE (col1)
db_ignore-# (
db_ignore(#     PARTITION P1 VALUES LESS THAN(5000),
db_ignore(#     PARTITION P2 VALUES LESS THAN(10000),
db_ignore(#     PARTITION P3 VALUES LESS THAN(15000)
db_ignore(# );
CREATE TABLE
db_ignore=# insert /*+ ignore_error */ into t_ignore values(20000);
WARNING:  inserted partition key does not map to any table partition
INSERT 0 0
db_ignore=# select * from t_ignore ;
 col1 | col2 
------+------
(0 rows)

db_ignore=# insert into t_ignore values(3000);
INSERT 0 1
db_ignore=# select * from t_ignore ;
 col1 | col2 
------+------
 3000 | 
(1 row)
db_ignore=# update /*+ ignore_error */ t_ignore set col1 = 20000 where col1 = 3000;
WARNING:  fail to update partitioned table "t_ignore".new tuple does not map to any table partition.
UPDATE 0
db_ignore=# select * from t_ignore ;
 col1 | col2 
------+------
 3000 | 
(1 row)

```

- **Failed to convert the updated or inserted value to the target column type.**

```
-- When the new value type and column type are both numeric:
db_ignore=# create table t_tinyint(num tinyint);
CREATE TABLE
db_ignore=# insert /*+ ignore_error */ into t_tinyint values(10000);
WARNING:  tinyint out of range
CONTEXT:  referenced column: num
INSERT 0 1
db_ignore=# select * from t_tinyint;
 num 
-----
 255
(1 row)

-- When the new value type and column type are both character strings:
db_ignore=# create table t_varchar5(content varchar(5));
CREATE TABLE
db_ignore=# insert /*+ ignore_error */ into t_varchar5 values('abcdefghi');
WARNING:  value too long for type character varying(5)
CONTEXT:  referenced column: content
INSERT 0 1
db_ignore=# select * from t_varchar5 ;
 content 
---------
 abcde
(1 row)
```
