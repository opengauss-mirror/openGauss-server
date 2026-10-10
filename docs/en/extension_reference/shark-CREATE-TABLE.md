# CREATE TABLE

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:31:37.655Z pushedAt=2026-09-24T11:15:52.847Z -->

## Description<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_s0867185fef0f4a228532d432b598cb26"></a>

A new empty table is created in the current database, and the table is owned by the user executing the command.

## Notes<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_sb04dbf08cbd848649163edbff21254a1"></a>

- This section contains only the syntax newly added by shark. The original openGauss syntax has not been removed or modified.
- The `AS expr [PERSISTED]` generated column syntax is newly supported.
- The `opt_clustered` syntax is newly supported.
- In the CREATE TABLE statement, for UNIQUE and PRIMARY KEY constraints, options can be given via WITH, corresponding to the index_parameters clause. The newly supported options include:

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

The value fillfactor of the FILLFACTOR option is an integer in the range [1, 100], with the same actual meaning as in A database (where the value range is an integer in [10, 100]). Therefore, when the value of fillfactor in D database falls within [1, 10), no error is reported, a notice message is printed, and the fillfactor value is set to the minimum value 10 of A database. The value delay of the COMPRESSION_DELAY option is an integer in the range [0, 10080]. Except for the FILLFACTOR option, which has actual functionality and behaves the same as in A database, all other parameters have no actual functionality and are provided for syntax-only compatibility.
- In the CREATE TABLE statement, the ON {filegroup | "default" } option is supported for UNIQUE and PRIMARY KEY constraints. It has no actual effect and is provided for syntax-only compatibility.
- The CREATE TABLE statement newly supports the ON {filegroup | "default" } option. It has no actual effect and is provided for syntax-only compatibility.
- The CREATE TABLE statement newly supports the TEXTIMAGE_ON { filegroup | "default" } option. It has no actual effect and is provided for syntax-only compatibility.
- filegroup is any string and can be enclosed in [].
- If both the ON filegroup clause and the TEXTIMAGE_ON filegroup clause are specified, the ON filegroup clause must precede the TEXTIMAGE_ON filegroup clause; otherwise, a syntax error occurs.
- The ON/TEXTIMAGE_ON filegroup clause cannot coexist with the ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } clause.
- Local temporary tables and global temporary tables are supported via table names with the special prefixes `#` and `##`, respectively.
By default, `#` and `##` are recognized as part of an identifier (controlled by the session-level Boolean parameter `enable_special_operator`) rather than as operators. Therefore, if they are used only as operators, this parameter must be enabled. If they are used as both table name prefixes and operators, disable this parameter and separate the operator from its operand with a space.

## Syntax<a name="zh-cn_topic_0283137629_zh-cn_topic_0237122117_zh-cn_topic_0059778169_sc7a49d08f8ac43189f0e7b1c74f877eb"></a>

Creates a table.

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

- Where the column constraint column\_constraint is:

    ```EBNF
    [ CONSTRAINT constraint_name ]
    { NOT NULL |
      NULL |
      CHECK ( expression ) |
      DEFAULT default_expr |
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

- Where the table constraint table\_constraint is:

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

- Where the index parameters index\_parameters is:

    ```EBNF
    [ WITH ( {storage_parameter = value} [, ... ] ) ]
    [ USING INDEX TABLESPACE tablespace_name ]
    ```

## Parameters

- **AS \( generation\_expr \) \[PERSISTED\]**

    This clause is a compatibility syntax for D database. It creates the column as a generated column, whose value is computed from generation\_expr when data is written (inserted or updated). PERSISTED indicates that the generated column value is stored in the same way as a regular column.

    >[!NOTE] Note
    >
    >- The PERSISTED keyword can be omitted, and the semantics remain the same as when PERSISTED is not omitted.
    >- A generated column in D database compatibility mode does not require an explicit column type; the type is derived from the expression computation result.
    >- When a regular column on which a generated column depends is dropped, an error is reported. The generated column must be dropped before the regular column it depends on can be dropped.

- **opt\_clustered**

    The parameter value is CLUSTERED/NONCLUSTERED, for compatibility with D database syntax, specifying the creation of a clustered/nonclustered index. It serves a syntax-only purpose and has no actual functionality.

- **WITH \( \{ storage\_parameter = value \} \[, ... \] \)**

    This clause specifies an optional storage parameter for a table or index. The WITH clause used for tables can also include OIDS=FALSE to indicate that no OIDs are assigned.

    For UNIQUE and PRIMARY KEY constraints, the newly supported storage_parameter options include:

    - FILLFACTOR

        int type, fillfactor. The actual meaning and functionality are the same as those in the A database.

        Value range: an integer in [1, 100]. The value range in the A database is an integer in [10, 100]. Therefore, when the value of fillfactor in the D database falls within [1, 10), no error is reported; instead, a notice message is printed, and the fillfactor value is set to the minimum value of 10 in the A database.

    - PAD_INDEX

        bool type, with no actual functionality and syntax-only compatibility.

        Value range: ON or OFF.

    - IGNORE_DUP_KEY

        bool type, no actual functionality, syntax-only compatibility.

        Value range: ON or OFF.

    - STATISTICS_NORECOMPUTE

        bool type, no actual functionality, syntax-only compatibility.

        Value range: ON or OFF.

    - STATISTICS_INCREMENTAL

        Boolean type, no actual functionality, syntax-only compatibility.

        Value range: ON or OFF.

    - ALLOW_ROW_LOCKS

        bool type, with no actual functionality and syntax-only compatibility.

        Value range: ON or OFF.

    - ALLOW_PAGE_LOCKS

        bool type, with no actual functionality and syntax-only compatibility.

        Value range: ON or OFF.

    - OPTIMIZE_FOR_SEQUENTIAL_KEY

        Bool type. No actual functionality, syntax-only compatibility.

        Value range: ON or OFF.

    - XML_COMPRESSION

        Bool type. No actual functionality, syntax-only compatibility.

        Value range: ON or OFF.

    - COMPRESSION_DELAY

        int type, unit MINUTES or MINUTE, optional, no actual functionality, syntax-only compatibility.

        Value range: 0 | delay [ MINUTES | MINUTE ], where delay is an integer in [0, 10080].

    - DATA_COMPRESSION

        String type, with no actual functionality and syntax-only compatibility.

        Value range: NONE | ROW | PAGE | COLUMNSTORE | COLUMNSTORE_ARCHIVE.

- **filegroup**

    - In the CREATE TABLE statement, the ON {filegroup | "default" } option is supported for UNIQUE and PRIMARY KEY constraints, with no actual effect and syntax-only compatibility.
    - The CREATE TABLE statement newly supports the ON {filegroup | "default" } option, with no actual effect and syntax-only compatibility.
    - The CREATE TABLE statement newly supports the TEXTIMAGE_ON { filegroup | "default" } option, which has no actual effect and is provided for syntax-only compatibility.
    - filegroup is an arbitrary string, supported via enclosing in [].
    - If both the ON filegroup clause and the TEXTIMAGE_ON filegroup clause are specified, the ON filegroup clause must precede the TEXTIMAGE_ON filegroup clause; otherwise, a syntax error occurs.
    - The ON/TEXTIMAGE_ON filegroup clause cannot coexist with the ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } clause.

- **ASC | DESC**

    - In table_constraint, the { column_name [ ASC | DESC ] } syntax is supported for PRIMARY KEY and UNIQUE constraints, providing ascending or descending ordering for primary keys and unique keys.

## Generated Column Example
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

## WITH \( \{ storage\_parameter = value \} \[, ... \] \) Example

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

## filegroup example

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

## ASC | DESC example

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

## Creating Local and Global Temporary Tables Using Special Prefixes<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
openGauss=# CREATE TEMPORARY TABLE #ltt1
(
    ID                        INTEGER               NOT NULL,
    NAME                      CHAR(16)              NOT NULL,
    ADDRESS                   VARCHAR(50)                   ,
    POSTCODE                  CHAR(6)
) ON COMMIT PRESERVE ROWS;

openGauss=# CREATE GLOBAL TEMPORARY TABLE ##gtt1
(
    ID                        INTEGER               NOT NULL,
    NAME                      CHAR(16)              NOT NULL,
    ADDRESS                   VARCHAR(50)                   ,
    POSTCODE                  CHAR(6)
) ON COMMIT PRESERVE ROWS;

```

## Related Links<a name="section156744489391"></a>

[CREATE TABLE](https://docs.opengauss.org/en/docs/latest/sql_reference/create_table.html)