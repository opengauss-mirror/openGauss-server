# gms_sql

## gms_sql概述

gms_sql是一个基于openGauss的插件，用于执行动态SQL，支持使用DDL和DML等。

目前支持的接口有：
OPEN_CURSOR --打开一个cursor，并返回该cursor的id。
CLOSE_CURSOR --用于关闭一个cursor。
COLUMN_VALUE --用于将fetch结果中的指定列保存到返回参数中。
DEFINE_COLUMN --用于定义返回的结果中的列结构。
EXECUTE         --用于执行PARSE函数处理的SQL语句。
FETCH_ROWS --用于载入结果集中的一行结果。
PARSE         --解析当前语句的语法。
RETURN_RESULT --用于将已执行语句的结果返回到客户端应用程序。
BIND_ARRAY --该存储过程根据传入动态SQL进行table变量进行绑定作用。
BIND_VARIABLE --该存储过程根据传入动态SQL进行变量的绑定。
DESCRIBE_COLUMNS、DESCRIBE_COLUMNS2、DESCRIBE_COLUMNS3 --该存储过程输出列的元数据信息。
DEFINE_ARRAY --定义要从给定游标中选择的集合，仅用于SELECT语句。
IS_OPEN         --该函数用于判断游标是否打开。
FETCH_ROWS --执行动态SQL返回结果数据集。
DEFINE_COLUMN   --动态SQL返回的变量名、通过COLUMN_VALUE存储过程定义返回列的值。

## gms_sql限制

- 仅支持Create extension命令方式加载插件。

## gms_sql安装

openGauss打包编译时默认已经包含了gms_sql, 可以在安装完openGauss后，直接通过create extension gms_sql;加载插件。

## gms_sql使用

### 创建Extension<a name="section21088306113"></a>

创建gms_sql Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_sql;
```

### 使用Extension<a name="section107391050141118"></a>

#### 对应接口

open_cursor函数
接口：open_cursor() RETURNS int
功能：检查游标编号数组，检查未使用的游标编号，并为该游标分配内存上下文，创建。当前仅支持打开游标个数最多为100个；
返回值：
游标编号。

is_open函数
接口：
is_open(c int) RETURNS bool
功能：检查对应的游标是否已经分配。
参数：c: 游标编号
返回值：游标是否已经分配

parse存储过程
接口：parse(c int, stmt varchar2, ver int) 
功能：解析要执行的语句。
参数：
c: 游标编号
stmt:动态SQL语句
ver:oracle对应版本。（即gms_sql.native,gms_sql.v6,gms_sql.v7（无实际意义仅仅为兼容作用））

bind_variable存储过程
接口：bind_variable(c int, name varchar2, value "any")
功能：设置绑定变量的值和类型
参数：
c: 游标编号
name:绑定的变量名
value:传入值的变量

bind_array存储过程
接口：bind_array(c int, name varchar2, value anyarray)
功能：设置集合类型的绑定变量的值和类型
参数：
c: 游标编号
name:绑定的变量名
value:保存一组值的变量

define_column存储过程
接口：define_column(c int, col int, value "any", column_size int DEFAULT -1) 
功能：定义返回结果列的类型和长度。
参数：
c: 游标编号
col：返回结果列下标
value: 与列类型相同的变量名
column_size:该列的最大长度

execute函数
接口：execute(c int) RETURNS bigint
功能：通过SPI接口，传入之前绑定的变量值，并执行查询语句。
参数：
c: 游标编号
返回值: 获取的结果行数。

fetch_rows函数
接口：fetch_rows(c int) RETURNS int
功能：通过SPI接口根据游标获取查询结果。
参数：
c: 游标编号
返回值: 读取的结果行数。

execute_and_fetch函数
接口：
execute_and_fetch(c int, exact bool DEFAULT false) RETURNS int
功能：执行execute和fetch_rows

last_row_count函数
接口：last_row_count() RETURNS int 
功能：返回fetch的行数。

column_value存储过程
接口：column_value(c int, pos int, INOUT value anyelement)
功能：将查询结果某一列的值保存到变量中。
参数：
c: 游标编号
pos: 列的下标
value: 要保存值的变量

return_result存储过程
接口：
第一种格式：
return_result(c refcursor, to_client bool DEFAULT false)
游标传入值为refcursor.
第二种格式：
return_result(c int, to_client bool DEFAULT false)
游标传入值为之前通过open_cursor打开的游标。
功能：将动态sql的查询结果返回给客户端。
参数：
c: 游标
to_client：是否返回到客户端，暂未使用。

describe_columns存储过程
接口：describe_columns(c int, INOUT col_cnt int, INOUT desc_t
 gms_sql.desc_rec[])
功能：获取查询结果的列的相关信息。
参数：
c: 游标编号
col_cnt为返回的列的数量。
desc_t为列的描述信息，每一列的信息都保存在之前定义desc_rec类型中。传入类型为desc_tab。
 
describe_columns2存储过程
接口：describe_columns2(c int, INOUT col_cnt int,INOUT desc_t 
gms_sql.desc_rec2[])
功能：与describe_columns相同
参数：
c: 游标编号
col_cnt为返回的列的数量。
desc_t为列的描述信息，传入类型为desc_tab2类型中。

describe_columns3存储过程
接口:describe_columns3(c int, INOUT col_cnt int,INOUT desc_t 
gms_sql.desc_rec3[])
describe_columns3(c int, INOUT col_cnt int,INOUT desc_t 

gms_sql.desc_rec4[])
功能：与describe_columns相同
参数：
c: 游标编号
col_cnt为返回的列的数量。
desc_t为列的描述信息，传入类型可以为desc_tab3或者desc_tab4。

close_cursor存储过程
接口：close_cursor(c int)
功能：检查游标是否打开，打开时，关闭游标，并关闭游标对应的portal,释放游标占用的内存。
参数：
c: 游标编号

debug_cursor存储过程 
接口：debug_cursor(c int)
功能：输出游标的相关信息，包括sql，绑定的变量名，变量类型，定义的列信息。
参数：
c: 游标编号

#### GMS_SQL 简单执行流程如下

1.OPEN_CURSOR

2.PARSE

3.BIND_VARIABLE

4.DEFINE_COLUMN或者 DEFINE_ARRAY

5.EXECUTE

6.FETCH_ROWS 或者 EXECUTE_AND_FETCH

7.COLUMN_VALUE

8.CLOSE_CURSOR

#### 示例

```
openGauss=# CREATE EXTENSION gms_sql;
CREATE EXTENSION
openGauss=# show gms_sql_max_open_cursor_count;
 gms_sql_max_open_cursor_count 
-------------------------------
 100
(1 row)

openGauss=# do $$
openGauss$# declare
openGauss$#   c int;
openGauss$#   strval varchar;
openGauss$#   intval int;
openGauss$#   nrows int default 30;
openGauss$# begin
openGauss$#   c := gms_sql.open_cursor();
openGauss$#   gms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)', gms_sql.v6);
openGauss$#   gms_sql.bind_variable(c, 'nrows', nrows);
openGauss$#   gms_sql.define_column(c, 1, strval);
openGauss$#   gms_sql.define_column(c, 2, intval);
openGauss$#   perform gms_sql.execute(c);
openGauss$#   while gms_sql.fetch_rows(c) > 0
openGauss$#   loop
openGauss$#     gms_sql.column_value(c, 1, strval);
openGauss$#     gms_sql.column_value(c, 2, intval);
openGauss$#     raise notice 'c1: %, c2: %', strval, intval;
openGauss$#   end loop;
openGauss$#   gms_sql.close_cursor(c);
openGauss$# end;
openGauss$# $$;
NOTICE:  c1: ahoj1, c2: 1
NOTICE:  c1: ahoj2, c2: 2
NOTICE:  c1: ahoj3, c2: 3
NOTICE:  c1: ahoj4, c2: 4
NOTICE:  c1: ahoj5, c2: 5
NOTICE:  c1: ahoj6, c2: 6
NOTICE:  c1: ahoj7, c2: 7
NOTICE:  c1: ahoj8, c2: 8
NOTICE:  c1: ahoj9, c2: 9
NOTICE:  c1: ahoj10, c2: 10
NOTICE:  c1: ahoj11, c2: 11
NOTICE:  c1: ahoj12, c2: 12
NOTICE:  c1: ahoj13, c2: 13
NOTICE:  c1: ahoj14, c2: 14
NOTICE:  c1: ahoj15, c2: 15
NOTICE:  c1: ahoj16, c2: 16
NOTICE:  c1: ahoj17, c2: 17
NOTICE:  c1: ahoj18, c2: 18
NOTICE:  c1: ahoj19, c2: 19
NOTICE:  c1: ahoj20, c2: 20
NOTICE:  c1: ahoj21, c2: 21
NOTICE:  c1: ahoj22, c2: 22
NOTICE:  c1: ahoj23, c2: 23
NOTICE:  c1: ahoj24, c2: 24
NOTICE:  c1: ahoj25, c2: 25
NOTICE:  c1: ahoj26, c2: 26
NOTICE:  c1: ahoj27, c2: 27
NOTICE:  c1: ahoj28, c2: 28
NOTICE:  c1: ahoj29, c2: 29
NOTICE:  c1: ahoj30, c2: 30
ANONYMOUS BLOCK EXECUTE
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_sql Extension的方法如下所示：

```
openGauss=# DROP Extension gms_sql [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
