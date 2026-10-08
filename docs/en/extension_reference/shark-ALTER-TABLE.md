# ALTER TABLE

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:44.472Z pushedAt=2026-09-21T09:39:34.773Z -->

## Function Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

Alter a table, including modifying table definitions, renaming a table, renaming specified columns in the table, renaming table constraints, setting the schema of the table, adding/updating multiple columns, and enabling/disabling the row access control switch.

## NOTE<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s8ea536d5b8ff459e9e3614e35f53bc2a"></a>

- This chapter contains only the syntax added by Shark. The original openGauss syntax has not been deleted or modified.
- The `opt_clustered` syntax is newly supported.
- In ALTER TABLE statements, for UNIQUE and PRIMARY KEY constraints, options can be specified through WITH, corresponding to the index_parameters clause. The newly supported options include:

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

For the FILLFACTOR option, the value fillfactor is an integer in the range [1, 100]. Its actual meaning is the same as that in database A (where the value range is an integer in [10, 100]). Therefore, when the value of fillfactor in database D falls within [1, 10), no error is reported; instead, a notice message is printed and the fillfactor value is set to the minimum value of database A, which is 10. For the COMPRESSION_DELAY option, the value delay is an integer in the range [0, 10080]. Except for the FILLFACTOR option, which has actual functionality and behaves the same as in database A, all other parameters have no actual functionality and are provided for syntax compatibility only.

- In ALTER TABLE statements, the ON {filegroup | "default" } option is supported for UNIQUE and PRIMARY KEY constraints. It has no actual effect and is provided for syntax support only.
- filegroup can be any string and may be enclosed in [].

## Syntax<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s58bdce220c9f4292ba9af919b04ad25c"></a>

- Alter the table definition.

    ```
    ALTER TABLE [ IF EXISTS ] { table_name [*] | (ONLY) table_name | (ONLY) ( table_name ) }
        action [, ... ];
    ```

    Where the specific table operation action can be one of the following clauses:

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

- Where the column constraint `column\_constraint` is:

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

- Where the table constraint `table\_constraint` is:

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

- Where the index parameter index_parameters is:

    ```
    [ WITH ( {storage_parameter = value} [, ... ] ) ]
        [ USING INDEX TABLESPACE tablespace_name ]
    ```

## Parameter Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_sf4962205ddf84312a5fd888bc662e5cf"></a>

- **opt\_clustered**

    The parameter value is CLUSTERED/NONCLUSTERED, compatible with the syntax of database D, specifying the creation of a clustered or non-clustered index. For syntax purposes only, with no actual functionality.

- **WITH \( \{ storage\_parameter = value \} \[, ... \] \)**

    This clause specifies an optional storage parameter for a table or index. The WITH clause used for tables can also include OIDS=FALSE to indicate that OIDs are not allocated.

    For UNIQUE and PRIMARY KEY constraints, the newly supported storage_parameter options include:

    - FILLFACTOR

        int type, fill factor. Its actual meaning and functionality are the same as those in database A.

        Value Range: an integer in [1, 100]. The value range in database A is an integer in [10, 100]. Therefore, when the fillfactor value in database D falls within [1, 10), no error is reported; instead, a notice message is printed and the fillfactor value is set to the minimum value of database A, which is 10.

    - PAD_INDEX

        Bool type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - IGNORE_DUP_KEY

        Bool type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - STATISTICS_NORECOMPUTE

        Bool type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - STATISTICS_INCREMENTAL

        Bool type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - ALLOW_ROW_LOCKS

        bool type, no actual functionality, syntax compatibility only.

        Value range: ON or OFF.

    - ALLOW_PAGE_LOCKS

        Boolean type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - OPTIMIZE_FOR_SEQUENTIAL_KEY

        Boolean type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - XML_COMPRESSION

        bool type. No actual functionality. Syntax compatibility only.

        Value range: ON or OFF.

    - COMPRESSION_DELAY

        int type, in MINUTES or MINUTE, optional. No actual functionality. Syntax compatibility only.

        Value Range: 0 | delay [ MINUTES | MINUTE ], where delay is an integer in [0, 10080].

    - DATA_COMPRESSION

        String type. No actual functionality. Syntax compatibility only.

        Value Range: NONE | ROW | PAGE | COLUMNSTORE | COLUMNSTORE_ARCHIVE.

- **filegroup**

    - In ALTER TABLE statements, the ON {filegroup | "default"} option is supported for UNIQUE and PRIMARY KEY constraints. It has no actual functionality and provides syntax support only.
    - filegroup is an arbitrary string and can be enclosed in [].

- **DEFAULT (expression) FOR (column_name)**

    - This syntax adds a DEFAULT constraint to a specified column, where the constraint is an expression.
    - For scenarios where a constraint name is explicitly declared, only syntax support is provided. A DEFAULT constraint created using this syntax cannot be dropped by its constraint name.

## opt_clustered Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
openGauss=# CREATE TABLE alter_table_tbl1 (a INT, b INT);
openGauss=# ALTER TABLE alter_table_tbl1 ADD CONSTRAINT alter_table_tbl_a UNIQUE CLUSTERED (a);
openGauss=# ALTER TABLE alter_table_tbl1 ADD CONSTRAINT alter_table_tbl_b PRIMARY KEY NONCLUSTERED (a);
```

## WITH \( \{ storage\_parameter = value \} \[, ... \] \) Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## filegroup Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table test1(col1 int primary key with(fillfactor = 20), col2 int);

alter table test1 add constraint unique_name unique(col2) with (fillfactor = 50, ignore_dup_key = on) on [primary1];

alter table test1 add column col3 int unique with (pad_index = on) on [primary2];

create table test2(col1 int, col2 int);

alter table test2 add constraint pk_id primary key(col1) with (fillfactor = 50, allow_row_locks = off) on [primar3];

create table test3(col1 int, col2 int);

alter table test3 add column col3 int primary key with (data_compression = none) on [primar4];
```

## DEFAULT (expression) FOR (column_name) Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## Related Links<a name="section156744489391"></a>

[ALTER TABLE](../sql_reference/alter_table.md)