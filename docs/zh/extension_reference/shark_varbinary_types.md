# varbinary类型

## 功能简述

varbinary是可变长度二进制数据。 所输入数据的长度可以是 0 字节。支持输入16进制形式的二进制数据常量。

## 注意事项

- 本类型支持在表、视图、匿名块、存储过程和自定义函数中使用。
- 使用varbinary(MAX)时可以存储openGauss能存储的最大二进制长度。

## 限制

- 支持index，但是如果源类型之间不支持比较，会报错
- 使用varbinary(n)时n要是正整数
- 不支持作为分区键
- 支持的类型转换：i-隐式转换，a-赋值转换，e-显示转换

| 支持的类型 | bytea | varchar | bpchar | int2/int4/int8 | real | double | numeric | date | time | smalltime |
| ------------ | ------- | --------- | -------- | ---------------- | ------ | -------- | --------- | ------ | ------ | ----------- |
| from | a | e | e | i | a | i | a | a | a | a |
| to | a | a | a | i | 不支持 | 不支持 | a | a | a | a |

## 示例

**示例1：** 查看类型信息

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

**示例2：** 简单使用

```
CREATE TABLE t1 (id int, a VARBINARY(1));
CREATE TABLE t2 (id int, a VARBINARY(16));
CREATE TABLE t3 (id int, a VARBINARY(MAX));
```

**示例3：** 类型转换

```
test_varbinary=# select 'hello world'::varbinary;
-[ RECORD 1 ]-----------------------
varbinary | 0x68656c6c6f20776f726c64

test_varbinary=# select 'hello world'::varbinary::varchar;
-[ RECORD 1 ]--------
varchar | hello world
```
