# CREATE TABLE

## 功能描述<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_s0867185fef0f4a228532d432b598cb26"></a>

在当前数据库中创建一个新的空白表，该表由命令执行者所有。

## 注意事项<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_sb04dbf08cbd848649163edbff21254a1"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。
- 新增支持 `AS expr [PERSISTED]` 生成列语法。
- 新增支持`opt_clustered`语法。
- 建表语句中，针对UNIQUE和PRIMARY KEY约束，支持通过WITH给出选项，对应index_parameters子句，新增支持的选项包括：

```EBNF
FILLFACTOR = fillfactor
| PAD_INDEX = { ON | OFF }
| IGNORE_DUP_KEY = { ON | OFF }
| STATISTICS_NORECOMPUTE = { ON | OFF }
| STATISTICS_INCREMENTAL = { ON | OFF }
| ALLOW_ROW_LOCKS = { ON | OFF }
| ALLOW_PAGE_LOCKS = { ON | OFF }
| OPTIMIZE_FOR_SEQUENTIAL_KEY = { ON | OFF }
| XML_COMPRESSION = { ON | OFF }
| COMPRESSION_DELAY = { 0 | delay [ MINUTES | MINUTE ] }
| DATA_COMPRESSION = { NONE | ROW | PAGE | COLUMNSTORE | COLUMNSTORE_ARCHIVE }
```

其中FILLFACTOR选项的取值fillfactor为[1, 100]的整数，实际含义同A库（A库的取值范围为[10, 100]的整数），因此当D库中fillfactor的取值范围为[1, 10)，不报错，将打印notice信息，并将fillfactor的取值设置为A库的最小值10;
COMPRESSION_DELAY选项的取值delay为[0, 10080]的整数;
除FILLFACTOR选项含有实际功能，同A库，其余参数均无实际功能，仅语法支持。
- 建表语句中，针对UNIQUE和PRIMARY KEY约束，支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
- 建表语句新增支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
- 建表语句新增支持TEXTIMAGE_ON { filegroup | "default" } 选项，无实际作用，仅语法支持。
- filegroup为任意字符串，支持通过[]包裹。
- 如果同时指定ON filegroup子句和TEXTIMAGE_ON filegroup子句，ON filegroup子句应位于前面，否则会出现语法报错。
- ON/TEXTIMAGE_ON filegroup子句无法和ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP }子句同时存在。
- 支持通过特殊前缀(`#`和`##`)的表名分别创建本地临时表和全局临时表。
默认将`#`, `##`识别为标识符的一部分(通过会话级布尔参数`enable_special_operator`切换)而非操作符，因此若只作为操作符使用则需要打开该参数，若同时作为表名以及操作符使用，则关闭该参数并将操作符与操作数用空格分开。

## 语法格式<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_sc7a49d08f8ac43189f0e7b1c74f877eb"></a>

创建表。

```EBNF
CREATE [ [ GLOBAL | LOCAL ] [ TEMPORARY | TEMP ] | UNLOGGED ] TABLE [ IF NOT EXISTS ] table_name 
    ({ column_name data_type [ CHARACTER SET | CHARSET charset ] [ compress_mode ] [ COLLATE collation ] [ column_constraint [ ... ] ]
        | table_constraint
        | LIKE source_table [ like_option [...] ] }
        [, ... ])
    [ AUTO_INCREMENT [ = ] value ]
    [ [DEFAULT] CHARACTER SET | CHARSET [ = ] default_charset ] [ [DEFAULT] COLLATE [ = ] default_collation ]
    [ WITH ( {storage_parameter = value} [, ... ] ) ]
    [ [ ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } ] | [ ON filegroup ] | [ TEXTIMAGE_ON filegroup ] ]
    [ COMPRESS | NOCOMPRESS ]
    [ TABLESPACE tablespace_name ]
    [ COMMENT {=| } 'text' ];
```

- 其中列约束column\_constraint为：

    ```EBNF
    [ CONSTRAINT constraint_name ]
    { NOT NULL |
      NULL |
      CHECK ( expression ) |
      DEFAULT default_expr |
      IDENTITY [ ( seed, increment ) ] |
      GENERATED ALWAYS AS ( generation_expr ) [STORED] |
      AS ( generation_expr ) [PERSISTED] |
      AUTO_INCREMENT |
      ON UPDATE update_expr |
      UNIQUE [KEY] index_parameters [ ON filegroup ] |
      ENCRYPTED WITH ( COLUMN_ENCRYPTION_KEY = column_encryption_key, ENCRYPTION_TYPE = encryption_type_value ) |
      PRIMARY KEY index_parameters [ ON filegroup ] |
      REFERENCES reftable [ ( refcolumn ) ] [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
          [ ON DELETE action ] [ ON UPDATE action ] }
    [ ENABLE [VALIDATE | NOVALIDATE] | DISABLE [VALIDATE | NOVALIDATE] ]
    [ DEFERRABLE | NOT DEFERRABLE | INITIALLY DEFERRED | INITIALLY IMMEDIATE ]
    [ COMMENT {=| } 'text' ]
    ```

- 其中表约束table\_constraint为：

    ```EBNF
    [ CONSTRAINT [ constraint_name ] ]
    { CHECK ( expression ) |
      UNIQUE [ opt_clustered ] ( { { column_name [ ( length ) ] | ( expression ) } [ ASC | DESC ] } [, ... ] ) index_parameters [ VISIBLE | INVISIBLE ] [ ON filegroup ] |
      PRIMARY KEY [ opt_clustered ] ( { column_name [ ASC | DESC ] } [, ... ] ) index_parameters [ VISIBLE | INVISIBLE ] [ ON filegroup ] |
      FOREIGN KEY [ index_name ] ( column_name [, ... ] ) REFERENCES reftable [ (refcolumn [, ... ] ) ]
          [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ] [ ON DELETE action ] [ ON UPDATE action ] |
      PARTIAL CLUSTER KEY ( column_name [, ... ] ) }
    [ DEFERRABLE | NOT DEFERRABLE | INITIALLY DEFERRED | INITIALLY IMMEDIATE ]
    [ COMMENT {=| } 'text' ]
    ```

- 其中索引参数index\_parameters为：

    ```EBNF
    [ WITH ( {storage_parameter = value} [, ... ] ) ]
    [ USING INDEX TABLESPACE tablespace_name ]
    ```

## 参数说明

- **IDENTITY \[ \( seed, increment \) \]**

    - 该语法为列添加identity属性，序列值递增，`seed`指定起始值，`increment`指定步长。
    - 一张表只能定义一列（包括generated as identity）。

- **AS \( generation\_expr \) \[PERSISTED\]**

    该子句为兼容D库的语法，将字段创建为生成列，生成列的值在写入（插入或更新）数据时由generation\_expr计算得到，PERSISTED表示像普通列一样存储生成列的值。

    >[!NOTE]说明
    >
    >- PERSISTED关键字可省略，与不省略PERSISTED语义相同。
    >- 兼容D库的生成列无需指定列类型，由表达式计算类型得到列的类型。
    >- 兼容D库的生成列在删除生成列依赖的普通列时报错，必须先删除生成列，才能删除生成列依赖的普通列。

- **opt\_clustered**

    参数内容为CLUSTERED/NONCLUSTERED，兼容D库的语法，指定创建聚合/非聚合索引。仅语法作用，没有实际功能。

- **WITH \( \{ storage\_parameter = value \} \[, ... \] \)**

    这个子句为表或索引指定一个可选的存储参数。用于表的WITH子句还可以包含OIDS=FALSE表示不分配OID。

    针对UNIQUE和PRIMARY KEY约束，新增支持的storage\_parameter选项包括：

    - FILLFACTOR

        int类型，填充因子，实际的含义和功能同A库。

        取值范围：[1, 100]的整数，A库的取值范围为[10, 100]的整数，因此当D库中fillfactor的取值范围为[1, 10)，不报错，将打印notice信息，并将fillfactor的取值设置为A库的最小值10。

    - PAD_INDEX

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - IGNORE_DUP_KEY

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - STATISTICS_NORECOMPUTE

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - STATISTICS_INCREMENTAL

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - ALLOW_ROW_LOCKS

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - ALLOW_PAGE_LOCKS

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - OPTIMIZE_FOR_SEQUENTIAL_KEY

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - XML_COMPRESSION

        bool类型，无实际功能，仅语法兼容。

        取值范围：ON或者OFF。

    - COMPRESSION_DELAY

        int类型，单位MINUTES或者MINUTE，可选，无实际功能，仅语法兼容。

        取值范围：0 | delay [ MINUTES | MINUTE ]，其中delay为[0, 10080]的整数。

    - DATA_COMPRESSION

        string类型，无实际功能，仅语法兼容。

        取值范围：NONE | ROW | PAGE | COLUMNSTORE | COLUMNSTORE_ARCHIVE。

- **filegroup**

    - 建表语句中，针对UNIQUE和PRIMARY KEY约束，支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
    - 建表语句新增支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
    - 建表语句新增支持TEXTIMAGE_ON { filegroup | "default" } 选项，无实际作用，仅语法支持。
    - filegroup为任意字符串，支持通过[]包裹。
    - 如果同时指定ON filegroup子句和TEXTIMAGE_ON filegroup子句，ON filegroup子句应位于前面，否则会出现语法报错。
    - ON/TEXTIMAGE_ON filegroup子句无法和ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP }子句同时存在。

- **ASC | DESC**

    - table_constraint中，针对PRIMARY KEY和UNIQUE约束支持使用{ column_name [ ASC | DESC ] }语法, 为主键和唯一键提供升序或降序约束。

## 生成列示例

```sql
opengauss=# CREATE TABLE Products(
opengauss(#     QtyAvailable smallint,
opengauss(#     UnitPrice money,
opengauss(#     InventoryValue AS (QtyAvailable * UnitPrice)
opengauss(# );
NOTICE:  The virtual computed columns (non-persisted) are currently ignored and behave the same as persisted columns.
CREATE TABLE
opengauss=# ALTER TABLE Products ADD RetailValue AS (QtyAvailable * UnitPrice * 1.5) PERSISTED;
ALTER TABLE
opengauss=# \d+ Products
                                                         Table "public.products"
     Column     |   Type   |                               Modifiers                               | Storage | Stats target | Description 
----------------+----------+-----------------------------------------------------------------------+---------+--------------+-------------
 qtyavailable   | smallint |                                                                       | plain   |              | 
 unitprice      | money    |                                                                       | plain   |              | 
 inventoryvalue | money    | as ((qtyavailable * unitprice)) persisted                             | plain   |              | 
 retailvalue    | money    | as (((qtyavailable * unitprice) * (1.5)::double precision)) persisted | plain   |              | 
Has OIDs: no
Options: orientation=row, compression=no

opengauss=# ALTER TABLE Products DROP unitprice;
ERROR:  cannot drop a column used by a generated column
DETAIL:  Column "unitprice" is used by generated column "retailvalue".
opengauss=# ALTER TABLE Products DROP inventoryvalue;
ALTER TABLE
opengauss=# ALTER TABLE Products DROP retailvalue;
ALTER TABLE
opengauss=# ALTER TABLE Products DROP unitprice;
ALTER TABLE
```

## IDENTITY \[ \( seed, increment \) \] 示例
```sql
openGauss=# create extension shark;
CREATE EXTENSION
openGauss=# create table t1 (a int identity(10, 20), b int);
NOTICE:  CREATE TABLE will create implicit sequence "t1_a_seq_identity" for serial column "t1.a"
CREATE TABLE
openGauss=# \d+ t1
                              Table "public.t1"
 Column |  Type   |     Modifiers     | Storage | Stats target | Description 
--------+---------+-------------------+---------+--------------+-------------
 a      | integer | not null identity | plain   |              | 
 b      | integer |                   | plain   |              | 
Has OIDs: no
Options: orientation=row, compression=no, collate=1537
Character Set: UTF8
Collate: utf8mb4_general_ci

openGauss=# insert into t1(b) values(10);
INSERT 0 1
openGauss=# insert into t1(a, b) overriding system value values(12, 10);
INSERT 0 1
openGauss=# insert into t1 default values;
INSERT 0 1
openGauss=# select * from t1;
 a  | b  
----+----
 10 | 10
 12 | 10
 30 |   
(3 rows)

```
## WITH \( \{ storage\_parameter = value \} \[, ... \] \)示例

```sql
create table test_with_1(a int, CONSTRAINT PK_test_with_1 PRIMARY KEY(a)
WITH (PAD_INDEX = OFF, FILLFACTOR = 50, IGNORE_DUP_KEY = off, STATISTICS_NORECOMPUTE = off, STATISTICS_INCREMENTAL = off,
ALLOW_ROW_LOCKS = off, ALLOW_PAGE_LOCKS = off, OPTIMIZE_FOR_SEQUENTIAL_KEY = off, XML_COMPRESSION = off));
NOTICE:  parameter "pad_index" is currently ignored.
NOTICE:  parameter "ignore_dup_key" is currently ignored.
NOTICE:  parameter "statistics_norecompute" is currently ignored.
NOTICE:  parameter "statistics_incremental" is currently ignored.
NOTICE:  parameter "allow_row_locks" is currently ignored.
NOTICE:  parameter "allow_page_locks" is currently ignored.
NOTICE:  parameter "optimize_for_sequential_key" is currently ignored.
NOTICE:  parameter "xml_compression" is currently ignored.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "pk_test_with_1" for table "test_with_1"

create table test_with_2(a int, CONSTRAINT PK_test_with_2 PRIMARY KEY(a) with (COMPRESSION_DELAY = 0 MINUTES));
NOTICE:  parameter "compression_delay" is currently ignored.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "pk_test_with_2" for table "test_with_2"

create table test_with_3(a int, CONSTRAINT PK_test_with_3 PRIMARY KEY(a) with (COMPRESSION_DELAY = 10080 minute));
NOTICE:  parameter "compression_delay" is currently ignored.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "pk_test_with_3" for table "test_with_3"

create table test_with_4(a int, CONSTRAINT PK_test_with_4 PRIMARY KEY(a) with (data_compression = COLUMNSTORE_ARCHIVE));
NOTICE:  parameter "data_compression" is currently ignored.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "pk_test_with_4" for table "test_with_4"

create table test_with_5(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 20));
NOTICE:  parameter "pad_index" is currently ignored.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "test_with_5_pkey" for table "test_with_5"

create table test_with_6(a int, PRIMARY KEY(a) with (pad_index = on, fillfactor = 1));
NOTICE:  parameter "pad_index" is currently ignored.
NOTICE:  parameter fillfactor will be set to 10 when it is less than 10.
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "test_with_6_pkey" for table "test_with_6"

create table test_with_7(a int, UNIQUE(a) with (pad_index = on, fillfactor = 1));
NOTICE:  parameter "pad_index" is currently ignored.
NOTICE:  parameter fillfactor will be set to 10 when it is less than 10.
NOTICE:  CREATE TABLE / UNIQUE will create implicit index "test_with_7_a_key" for table "test_with_7"
```

## filegroup示例

```sql
create table t1(a int) on [primary];
create table t2(a int) on "default";
create table t3(id int) on [filegroup];
create table t4(id int) on filegroup;
create table t5(id int) on 'filegroup';
create table t6(id int) on "filegroup";
create table t7(a int) textimage_on [primary];
create table t8(a int) textimage_on "default";
create table t9(a int) on "default" textimage_on [primary];
create table t10(a int) on "default" textimage_on "default";
create table t11(a int PRIMARY KEY WITH (PAD_INDEX = OFF) ON [primary]) ON [primary];
create table t12(a int UNIQUE WITH (XML_COMPRESSION = OFF) ON [primary]) ON [primary];
create table t13(a int, CONSTRAINT PK_t11 PRIMARY KEY(a) WITH (PAD_INDEX = OFF) ON [primary]) ON [primary];
create table t14(a int, CONSTRAINT PK_t12 UNIQUE(a) WITH (XML_COMPRESSION = OFF) ON [primary]) ON [primary];
```

## ASC | DESC示例

```sql

openGauss=# create table CONSTRAINT_DESC(id int not null, v1 varchar(30), constraint PK_CONSTRAINT_DESC primary key(id DESC));
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "pk_constraint_desc" for table "constraint_desc"
CREATE TABLE
openGauss=# \d+ CONSTRAINT_DESC
                           Table "public.constraint_desc"
 Column |         Type          | Modifiers | Storage  | Stats target | Description 
--------+-----------------------+-----------+----------+--------------+-------------
 id     | integer               | not null  | plain    |              | 
 v1     | character varying(30) |           | extended |              | 
Indexes:
    "pk_constraint_desc" PRIMARY KEY, btree (id DESC) TABLESPACE pg_default
Has OIDs: no
Options: orientation=row, compression=no

```

## 使用特殊前缀创建本地和全局临时表

```sql
openGauss=# CREATE TEMPORARY TABLE #ltt1
(
    ID                        INTEGER               NOT NULL,
    NAME                      CHAR(16)              NOT NULL,
    ADDRESS                   VARCHAR(50)                   ,
    POSTCODE                  CHAR(6)
) ON COMMIT PRESERVE ROWS;
CREATE TABLE
openGauss=# CREATE GLOBAL TEMPORARY TABLE ##gtt1
(
    ID                        INTEGER               NOT NULL,
    NAME                      CHAR(16)              NOT NULL,
    ADDRESS                   VARCHAR(50)                   ,
    POSTCODE                  CHAR(6)
) ON COMMIT PRESERVE ROWS;
CREATE TABLE
```

## 相关链接<a name="section156744489391"></a>

[CREATE TABLE](https://docs.opengauss.org/zh/docs/latest/sql_reference/create_table.html)
