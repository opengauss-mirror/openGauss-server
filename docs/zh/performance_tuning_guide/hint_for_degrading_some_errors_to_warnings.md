# 将部分Error降级为Warning的Hint

## 功能描述<a name="zh-cn_topic_0237121537_section290819468377"></a>

指定执行INSERT、UPDATE语句时可将部分Error降级为Warning，且不影响语句执行完成的hint。

该hint不支持列存表，无法在列存表中生效。

>[!WARNING]注意 
>与其他hint不同，此hint仅影响执行器遇到部分Error时的处理方式，不会对执行计划有任何影响。

使用该hint时，Error会被降级的场景有：

- **违反非空约束时**

  若执行的SQL语句违反了表的非空约束，使用此hint可将Error降级为Warning，并根据GUC参数sql_ignore_strategy的值采用以下策略的一种继续执行：
  
  - sql_ignore_strategy为ignore_null时，忽略违反非空约束的行的INSERT/UPDATE操作，并继续执行剩余数据操作。
  
  - sql_ignore_strategy为overwrite_null时，将违反约束的null值覆写为目标类型的默认值，并继续执行剩余数据操作。
  
      >[!NOTE]说明
    >GUC参数sql_ignore_strategy相关信息请参考[sql_ignore_strategy](../database_reference/miscellaneous_parameters.md)。

- **违反唯一约束时**

  若执行的SQL语句违反了表的唯一约束，使用此hint可将Error降级为Warning，忽略违反约束的行的INSERT/UPDATE操作，并继续执行剩余数据操作。
  
- **分区表无法匹配到合法分区时**

  在对分区表进行INSERT/UPDATE操作时，若某行数据无法匹配到表格的合法分区，使用此hint可将Error降级为Warning，忽略该行操作，并继续执行剩余数据操作。
  
- **更新/插入值向目标列类型转换失败时**

  执行INSERT/UPDATE语句时，若发现新值与目标列类型不匹配，使用此hint可将Error降级为Warning，并根据新值与目标列的具体类型采取以下策略的一种继续执行：
  
  - 当新值类型与列类型同为数值类型时：
  
      若新值在列类型的范围内，则直接进行插入/更新；若新值在列类型范围外，则以列类型的最大/最小值替代。
   
  - 当新值类型与列类型同为字符串类型时：
  
      若新值长度在列类型限定范围内，则以直接进行插入/更新；若新值长度在列类型的限定范围外，则保留列类型长度限制的前n个字符。
   
  - 若遇到新值类型与列类型不可转换时：
  
      插入/更新列类型的默认值。

## 语法格式<a name="zh-cn_topic_0237121537_section17380317104213"></a>

```
ignore_error
```

## 示例<a name="zh-cn_topic_0237121537_section1127715590585"></a>

为使用ignore_error hint，需要创建B兼容模式的数据库，名称为db_ignore。

```
create database db_ignore dbcompatibility 'B';
\c db_ignore
```

- **忽略非空约束**

```
db_ignore=# create table t_not_null(num int not null);
CREATE TABLE
-- 采用忽略策略
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


-- 采用覆写策略
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

- **忽略唯一约束**

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

- **忽略分区表无法匹配到合法分区**

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

- **更新/插入值向目标列类型转换失败**

```
-- 当新值类型与列类型同为数值类型
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

-- 当新值类型与列类型同为字符类型时
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
