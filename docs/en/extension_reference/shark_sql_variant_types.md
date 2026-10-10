# sql_variant Type

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:57.191Z pushedAt=2026-09-21T07:24:05.358Z -->

## Feature Description

openGauss supports the sql_variant type in shark. The sql_variant type can store values of non-user-defined types (except for specially noted types) while preserving the original type information, and can be used in columns, parameters, variables, and function return values.

## Notes

- The binary length of Basic Types must be <= 8000 bytes.
- This type is supported in tables, views, anonymous blocks, stored procedures, and user-defined functions.

## Restrictions

- Not supported as a partition key
- Indexes are supported, but an error will be reported if comparison between source types is not supported
- Source type information will be lost during dump, logical replication, and similar operations

## Examples

**Example 1:** Search for the sql_variant type in the system table.

```auto
\x    --Display query results in column format.
select * from pg_type where typname='sql_variant';
```

The result is displayed as follows:

```auto
-[ RECORD 1 ]--+----------------
typname        | sql_variant
typnamespace   | 16388
typowner       | 10
typlen         | -1
typbyval       | f
typtype        | b
typcategory    | U
typispreferred | f
typisdefined   | t
typdelim       | ,
typrelid       | 0
typelem        | 0
typarray       | 16636
typinput       | sql_variantin
typoutput      | sql_variantout
typreceive     | sql_variantrecv
typsend        | sql_variantsend
typmodin       | -
typmodout      | -
typanalyze     | -
typalign       | i
typstorage     | x
typnotnull     | f
typbasetype    | 0
typtypmod      | -1
typndims       | 0
typcollation   | 100
typdefaultbin  |
typdefault     |
typacl         |
```

**Example 2:** Cast a character type to the sql_variant type.

```auto
select 'aa'::char::sql_variant;
select 'circles rounds circles days'::char(20)::sql_variant;
```

```auto
sql_variant
---------------
 a
(1 row)

sql_variant
---------------
 圈圈圆圆圈圈
(1 row)
```

**Example 3:** sql_variant requires the binary length of Basic Types to be &lt;= 8000.

1. The binary length of the Basic Types is less than or equal to 8000.

```auto
select 'my'::varchar(7999)::sql_variant;
select 'your'::varchar(8000)::sql_variant;
```

The result display is:

```auto
sql_variant
---------------
 我的
(1 row)

sql_variant
---------------
 你的
(1 row)
```

2. The binary length of the basic type exceeds 8000.

```auto
select repeat('qqqqqqqq',1001)::varchar(8001)::sql_variant;
```

The result display indicates that the value is out of range, and the type conversion will fail:

```auto
ERROR:  value of basic type must be a binary length <= 8000 byte
CONTEXT:  referenced column: repeat
```