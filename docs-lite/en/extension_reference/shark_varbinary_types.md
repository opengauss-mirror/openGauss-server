# varbinary Type

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:23.965Z pushedAt=2026-09-21T08:21:48.284Z -->

## Overview

varbinary is a variable-length binary data type. The length of the input data can be 0 bytes. Binary data constants in hexadecimal format are supported.

## Precautions

- This type can be used in tables, views, anonymous blocks, stored procedures, and user-defined functions.
- When varbinary(MAX) is used, the maximum binary length that openGauss can store is supported.

## Restrictions

- Index is supported, but an error will be reported if comparison between source types is not supported.
- When using varbinary(n), n must be a positive integer.
- Not supported as a partition key.
- Supported type conversions: i — implicit conversion, a — assignment conversion, e — explicit conversion.

| Supported Types | bytea | varchar | bpchar | int2/int4/int8 | real | double | numeric | date | time | smalltime |
| ------------ | ------- | --------- | -------- | ---------------- | ------ | -------- | --------- | ------ | ------ | ----------- |
| from | a | e | e | i | a | i | a | a | a | a |
| to | a | a | a | i | Not supported | Not supported | a | a | a | a |

## Examples

**Example 1:** View type information

```
test_varbinary=# select * from pg_type where typname = 'varbinary' ;
-[ RECORD 1 ]--+-------------------
typname        | varbinary
typnamespace   | 311341
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
typarray       | 311353
typinput       | varbinaryin
typoutput      | varbinaryout
typreceive     | varbinaryrecv
typsend        | varbinarysend
typmodin       | varbinarytypmodin
typmodout      | varbinarytypmodout
typanalyze     | -
typalign       | i
typstorage     | x
typnotnull     | f
typbasetype    | 0
typtypmod      | -1
typndims       | 0
typcollation   | 0
typdefaultbin  |
typdefault     |
typacl         |
```

**Example 2:** Simple usage

```
CREATE TABLE t1 (id int, a VARBINARY(1));
CREATE TABLE t2 (id int, a VARBINARY(16));
CREATE TABLE t3 (id int, a VARBINARY(MAX));
```

**Example 3:** Type conversion

```
test_varbinary=# select 'hello world'::varbinary;
-[ RECORD 1 ]-----------------------
varbinary | 0x68656c6c6f20776f726c64

test_varbinary=# select 'hello world'::varbinary::varchar;
-[ RECORD 1 ]--------
varchar | hello world
```