# ALTER TABLE

## 功能描述<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

修改表，包括修改表的定义、重命名表、重命名表中指定的列、重命名表的约束、设置表的所属模式、添加/更新多个列、打开/关闭行访问控制开关。

## 注意事项<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s8ea536d5b8ff459e9e3614e35f53bc2a"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。
- 新增支持`opt_clustered`语法。
- 修改表语句中，针对UNIQUE和PRIMARY KEY约束，支持通过WITH给出选项，对应index_parameters子句，新增支持的选项包括：

```
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

- 修改表语句中，针对UNIQUE和PRIMARY KEY约束，支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
- filegroup为任意字符串，支持通过[]包裹。

## 语法格式<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s58bdce220c9f4292ba9af919b04ad25c"></a>

- 修改表的定义。

    ```
    ALTER TABLE [ IF EXISTS ] { table_name [*] | (ONLY) table_name | (ONLY) ( table_name ) }
        action [, ... ];
    ```

    其中具体表操作action可以是以下子句之一：

    ```
    column_clause
        | ADD table_constraint [ NOT VALID ]
        | ADD table_constraint_using_index
        | VALIDATE CONSTRAINT constraint_name
        | DROP CONSTRAINT [ IF EXISTS ]  constraint_name [ RESTRICT | CASCADE ]
        | CLUSTER ON index_name
        | SET WITHOUT CLUSTER
        | SET ( {storage_parameter = value} [, ... ] )
        | RESET ( storage_parameter [, ... ] )
        | OWNER TO new_owner
        | SET TABLESPACE new_tablespace
        | SET {COMPRESS|NOCOMPRESS}
        | TO { GROUP groupname | NODE ( nodename [, ... ] ) }
        | ADD NODE ( nodename [, ... ] )
        | DELETE NODE ( nodename [, ... ] )
        | DISABLE TRIGGER [ trigger_name | ALL | USER ]
        | ENABLE TRIGGER [ trigger_name | ALL | USER ]
        | ENABLE REPLICA TRIGGER trigger_name
        | ENABLE ALWAYS TRIGGER trigger_name
        | DISABLE/ENABLE [ REPLICA | ALWAYS ] RULE
        | DISABLE ROW LEVEL SECURITY
        | ENABLE ROW LEVEL SECURITY
        | FORCE ROW LEVEL SECURITY
        | NO FORCE ROW LEVEL SECURITY
        | ENCRYPTION KEY ROTATION
        | INHERIT parents
        | NO INHERIT parents
        | OF type_name
        | NOT OF
        | REPLICA IDENTITY { DEFAULT | USING INDEX index_name | FULL | NOTHING }
        | AUTO_INCREMENT [ = ] value
        | COMMENT {=| } 'text'
        | ALTER INDEX index_name [ VISBLE | INVISIBLE ]
        | [ [ DEFAULT ] CHARACTER SET | CHARSET [ = ] default_charset ] [ [ DEFAULT ] COLLATE [ = ] default_collation ]
        | CONVERT TO CHARACTER SET | CHARSET charset | DEFAULT [ COLLATE collation ]
        | MODIFY column_name column_type ON UPDATE CURRENT_TIMESTAMP
        | IMCSTORED [ ( column_name [, ...] ) ]
        | MODIFY PARTITION partition_name IMCSTORED [ ( column_name [, ...] ) ]
        | UNIMCSTORED
        | MODIFY PARTITION partition_name UNIMCSTORED
    ```

- 其中列约束column\_constraint为：

    ```
    [ CONSTRAINT constraint_name ]
                { NOT NULL |
                    NULL |
                    CHECK ( expression ) |
                    DEFAULT default_expr  |
                    GENERATED ALWAYS AS ( generation_expr ) [STORED] |
                    AUTO_INCREMENT |
                    ON UPDATE update_expr |
                    UNIQUE [KEY] index_parameters [ ON filegroup ] |
                    PRIMARY KEY index_parameters [ ON filegroup ] |
                    ENCRYPTED WITH ( COLUMN_ENCRYPTION_KEY = column_encryption_key, ENCRYPTION_TYPE = encryption_type_value ) |
                    REFERENCES reftable [ ( refcolumn ) ] [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
                        [ ON DELETE action ] [ ON UPDATE action ] }
                        [ ENABLE [VALIDATE | NOVALIDATE] | DISABLE [VALIDATE | NOVALIDATE] ]
                        [ DEFERRABLE | NOT DEFERRABLE | INITIALLY DEFERRED | INITIALLY IMMEDIATE ] |
                    DEFAULT (expression) FOR (column_name)
    [ COMMENT 'text' ]
    ```

- 其中表约束table\_constraint为：

    ```
    [ CONSTRAINT [ constraint_name ] ]
     { CHECK ( expression ) |
       UNIQUE [ opt_clustered ] ( { { column_name [ ( length ) ] | ( expression ) } [ ASC | DESC ] } [, ... ] ) index_parameters [ VISIBLE | INVISIBLE ]
            [ ON filegroup ] |
       PRIMARY KEY [ opt_clustered ] ( { column_name [ ASC | DESC ] }[, ... ] ) index_parameters [ VISIBLE | INVISIBLE ] [ ON filegroup ] |
       PARTIAL CLUSTER KEY ( column_name [, ... ] ) |
       FOREIGN KEY [ idx_name ] ( column_name [, ... ] ) REFERENCES reftable [ ( refcolumn [, ... ] ) ]
         [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ] [ ON DELETE action ] [ ON UPDATE action ] }
        [ DEFERRABLE | NOT DEFERRABLE | INITIALLY DEFERRED | INITIALLY IMMEDIATE ]
    ```

- 其中索引参数index\_parameters为：

    ```
    [ WITH ( {storage_parameter = value} [, ... ] ) ]
        [ USING INDEX TABLESPACE tablespace_name ]
    ```

## 参数说明<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_sf4962205ddf84312a5fd888bc662e5cf"></a>

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

    - 修改表语句中，针对UNIQUE和PRIMARY KEY约束，支持ON {filegroup | "default" } 选项，无实际作用，仅语法支持。
    - filegroup为任意字符串，支持通过[]包裹。

- **DEFAULT (expression) FOR (column_name)**

    - 该语法可以为指定列添加DEFAULT约束，该约束为一个表达式。
    - 对于显式声明约束名的场景，仅做语法支持，使用该语法创建的DEFAULT约束无法通过约束名进行删除。

## opt\_clustered示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
openGauss=# CREATE TABLE alter_table_tbl1 (a INT, b INT);
openGauss=# ALTER TABLE alter_table_tbl1 ADD CONSTRAINT alter_table_tbl_a UNIQUE CLUSTERED (a);
openGauss=# ALTER TABLE alter_table_tbl1 ADD CONSTRAINT alter_table_tbl_b PRIMARY KEY NONCLUSTERED (a);
```

## WITH \( \{ storage\_parameter = value \} \[, ... \] \)示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table test1(col1 int primary key with(fillfactor = 20), col2 int);
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "test1_pkey" for table "test1"

alter table test1 add constraint unique_name unique(col2) with (fillfactor = 50, ignore_dup_key = on);
NOTICE:  parameter "ignore_dup_key" is currently ignored.
NOTICE:  ALTER TABLE / ADD UNIQUE will create implicit index "unique_name" for table "test1"

alter table test1 add column col3 int unique with (pad_index = on);
NOTICE:  parameter "pad_index" is currently ignored.
NOTICE:  ALTER TABLE / ADD UNIQUE will create implicit index "test1_col3_key" for table "test1"

create table test2(col1 int, col2 int);

alter table test2 add constraint pk_id primary key(col1) with (fillfactor = 50, allow_row_locks = off);
NOTICE:  parameter "allow_row_locks" is currently ignored.
NOTICE:  ALTER TABLE / ADD PRIMARY KEY will create implicit index "pk_id" for table "test2"

create table test3(col1 int, col2 int);

alter table test3 add column col3 int primary key with (data_compression = none);
NOTICE:  parameter "data_compression" is currently ignored.
NOTICE:  ALTER TABLE / ADD PRIMARY KEY will create implicit index "test3_pkey" for table "test3"
```

## filegroup示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table test1(col1 int primary key with(fillfactor = 20), col2 int);

alter table test1 add constraint unique_name unique(col2) with (fillfactor = 50, ignore_dup_key = on) on [primary1];

alter table test1 add column col3 int unique with (pad_index = on) on [primary2];

create table test2(col1 int, col2 int);

alter table test2 add constraint pk_id primary key(col1) with (fillfactor = 50, allow_row_locks = off) on [primar3];

create table test3(col1 int, col2 int);

alter table test3 add column col3 int primary key with (data_compression = none) on [primar4];
```

## DEFAULT (expression) FOR (column_name) 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
openGauss=# create table ADD_DEFAULT(id int, v1 varchar(20), v2 float);
CREATE TABLE
openGauss=# \d+ ADD_DEFAULT
                             Table "public.add_default"
 Column |         Type          | Modifiers | Storage  | Stats target | Description 
--------+-----------------------+-----------+----------+--------------+-------------
 id     | integer               |           | plain    |              | 
 v1     | character varying(20) |           | extended |              | 
 v2     | double precision      |           | plain    |              | 
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# alter table ADD_DEFAULT add default (mod(4, 3)) for id;
NOTICE:  DEFAULT added. The added DEFAULT can not be dropped by name
ALTER TABLE
openGauss=# \d+ ADD_DEFAULT
                                 Table "public.add_default"
 Column |         Type          |     Modifiers     | Storage  | Stats target | Description 
--------+-----------------------+-------------------+----------+--------------+-------------
 id     | integer               | default mod(4, 3) | plain    |              | 
 v1     | character varying(20) |                   | extended |              | 
 v2     | double precision      |                   | plain    |              | 
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# insert into ADD_DEFAULT(v1, v2) values('bac', 3.1);
INSERT 0 1
openGauss=# select * from ADD_DEFAULT;
 id | v1  | v2  
----+-----+-----
  1 | bac | 3.1
(1 row)

openGauss=# create table ADD_CONSTRAINT_DEFAULT(id int, v1 varchar(20), v2 timestamptz);
CREATE TABLE
openGauss=# \d+ ADD_CONSTRAINT_DEFAULT
                         Table "public.add_constraint_default"
 Column |           Type           | Modifiers | Storage  | Stats target | Description 
--------+--------------------------+-----------+----------+--------------+-------------
 id     | integer                  |           | plain    |              | 
 v1     | character varying(20)    |           | extended |              | 
 v2     | timestamp with time zone |           | plain    |              | 
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# alter table ADD_CONSTRAINT_DEFAULT add constraint ADD_SYSTEIME_DEFAULT default (pg_systimestamp()) for v2;
NOTICE:  DEFAULT added. The added DEFAULT can not be dropped by name
ALTER TABLE
test_d=# \d+ ADD_CONSTRAINT_DEFAULT
                                 Table "public.add_constraint_default"
 Column |           Type           |         Modifiers         | Storage  | Stats target | Description 
--------+--------------------------+---------------------------+----------+--------------+-------------
 id     | integer                  |                           | plain    |              | 
 v1     | character varying(20)    |                           | extended |              | 
 v2     | timestamp with time zone | default pg_systimestamp() | plain    |              | 
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# insert into ADD_CONSTRAINT_DEFAULT(id, v1) values(1, 'abc');
INSERT 0 1
openGauss=# select * from ADD_CONSTRAINT_DEFAULT;
 id | v1  |              v2               
----+-----+-------------------------------
  1 | abc | 2025-10-30 11:17:36.821797+08
(1 row)

```

## 相关链接<a name="section156744489391"></a>

[ALTER TABLE](https://docs.opengauss.org/zh/docs/latest/sql_reference/alter_table.html)
