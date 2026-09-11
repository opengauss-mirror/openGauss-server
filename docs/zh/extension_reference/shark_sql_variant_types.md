# sql_variant类型

## 功能描述

openGauss在shark中支持sql_variant类型，sql_variant类型可以保存非用户定义类型（除特殊说明的类型）的值，并保留原类型信息，可以用在列、参数、变量和函数返回值中。

## 注意事项

- 基础类型的二进制长度必须 <= 8000字节。
- 本类型支持在表、视图、匿名块、存储过程和自定义函数中使用。

## 限制

- 不支持作为分区键
- 支持index，但是如果源类型之间不支持比较，会报错
- dump、逻辑复制等会失去源类型信息

## 示例

**前置 SQL**
```auto
-- 创建 UTF-8 编码的 D 兼容数据库
CREATE DATABASE test_sql_variant
WITH
    DBCOMPATIBILITY = 'D'
    ENCODING = 'UTF8'
    LC_COLLATE = 'C.UTF-8'
    LC_CTYPE = 'C.UTF-8'
    TEMPLATE = template0;

-- 切换到 D 兼容数据库
\c test_sql_variant

-- 安装 shark 插件
create extension shark;
```
**示例1：** 在系统表中查找sql_variant类型。

```auto
\x    --开启列式展示查询结果(设置为 on)

select * from pg_type where typname='sql_variant';
```

结果显示为：

```auto
Expanded display is on.

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

**示例2：** 字符类型强制转换为sql_variant类型。

```auto
\x    --取消列式展示查询结果(设置为 off)

select 'aa'::char::sql_variant;
select '圈圈圆圆圈圈天天'::char(20)::sql_variant;
```

```auto
Expanded display is off.

sql_variant
---------------
 a
(1 row)

sql_variant
---------------
 圈圈圆圆圈圈
(1 row)
```

**示例3：** sql_variant对基础类型二进制长度要求<=8000。

1、基础类型二进制长度小于等于8000。

```auto
select '我的'::varchar(7999)::sql_variant;
select '你的'::varchar(8000)::sql_variant;
```

结果显示为：

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

2、基础类型二进制长度大于8000。

```auto
select repeat('qqqqqqqq',1001)::varchar(8001)::sql_variant;
```

结果显示为超出范围，类型转换会失败：

```auto
ERROR:  value of basic type must be a binary length <= 8000 byte
CONTEXT:  referenced column: repeat
```
