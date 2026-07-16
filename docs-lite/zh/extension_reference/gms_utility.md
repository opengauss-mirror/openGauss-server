# gms_utility

## gms_utility概述

gms_utility是一个基于openGauss的插件，为用户提供了各种实用的程序和函数。目前支持以下23个接口：

- `ANALYZE_DATABASE`
- `ANALYZE_SCHEMA`
- `CANONICALIZE`
- `COMMA_TO_TABLE`
- `COMPILE_SCHEMA`
- `DB_VERSION`
- `EXEC_DDL_STATEMENT`
- `EXPAND_SQL_TEXT`
- `FORMAT_CALL_STACK`
- `FORMAT_ERROR_BACKTRACE`
- `FORMAT_ERROR_STACK`
- `GET_CPU_TIME`
- `GET_ENDIANNESS`
- `GET_HASH_VALUE`
- `GET_SQL_HASH`
- `GET_TIME`
- `NAME_RESOLVE`
- `NAME_TOKENIZE`
- `OLD_CURRENT_SCHEMA`
- `OLD_CURRENT_USER`
- `IS_BIT_SET`
- `IS_CLUSTER_DATABASE`
- `TABLE_TO_COMMA`

## gms_utility限制

仅支持`CREATE EXTENSION`命令方式加载，使用`DROP EXTENSION`命令方式卸载。

仅支持在A库环境下使用，非A库环境不建议使用该插件。

## gms_utility安装

opengGauss打包编译时默认已经包含了gms_utility，可以在安装完成后，直接通过`create extension gms_utility;`的方式加载插件。

## gms_utility使用

### 创建Extension

创建gms_utility extension可直接使用`create extension gms_utility;`命令进行创建。

### 使用Extension

#### 函数声明

- ANALYZE_DATABASE(method IN VARCHAR2, estimate_rows IN NUMBER DEFAULT NULL, estimate_percent IN NUMBER DEFAULT NULL, method_opt IN VARCHAR2 DEFAULT NULL);

  **描述**：此过程分析数据库中的所有表、索引的统计信息。

  **参数说明**：

  - `method`：取值范围为`ESTIMATE`、`COMPUTE`、`DELETE`；若为ESTIMATE，则参数`estimate_rows`或者`estimate_percent`必须有值。
  - `estimate_rows`：要估计的行数，参数需要大于等于0。
  - `estimate_percent`：要估计的百分比，有效范围在0~100。如果指定了`estimate_rows`，则忽略此参数。
  - `method_opt`：下列格式的方法选项：
    - `[FOR TABLE]`
    - `[FOR ALL [INDEXED] COLUMNS [SIZE n]]`；(目前只做语法支持)
    - `[FOR ALL INDEXES]`
  
  **使用说明**：
  
  - 参数`estimate_rows`与参数`estimate_percent`的值仅在参数`method=ESTIMATE`时做校验；
  - 参数`method=DELETE`时，不能设置参数`method_opt`的值。

- ANALYZE_SCHEMA(schema IN VARCHAR2, method IN VARCHAR2, estimate_rows IN NUMBER DEFAULT NULL, estimate_percent IN NUMBER DEFAULT NULL, method_opt IN VARCHAR2 DEFAULT NULL);

  **描述**：此过程分析指定schema中所有表、索引的统计信息。

  **参数说明**：

  - `schema`：指定要进行解析的schema的名称。

  - `method`：取值范围为`ESTIMATE`、`COMPUTE`、`DELETE`；若为ESTIMATE，则参数`estimate_rows`或者`estimate_percent`必须有值。
  - `estimate_rows`：要估计的行数，参数需要大于等于0。
  - `estimate_percent`：要估计的百分比，有效范围在0~100。如果指定了`estimate_rows`，则忽略此参数。
  - `method_opt`：下列格式的方法选项：
    - `[FOR TABLE]`
    - `[FOR ALL [INDEXED] COLUMNS [SIZE n]]`；(目前只做语法支持)
    - `[FOR ALL INDEXES]`
  
  **使用说明**：
  
  - 参数`estimate_rows`与参数`estimate_percent`的值仅在参数`method=ESTIMATE`时做校验；
  - 参数`method=DELETE`时，不能设置参数`method_opt`的值。

- CANONICALIZE(name IN VARCHAR2, canon_name OUT VARCHAR2, canon_len IN BINARY_INTEGER);

  **描述**：该存储过程规范化给定的字符串。其处理单个保留字或者关键字（例如'table'），并且去除单个标识符的空格，比如' table '变成TABLE。

  **参数说明**：

  - `name`：需要规范化的字符串。
  - `canon_name`：规范化后的输出字符串。
  - `canon_len`：要规范化的字符串的长度（以字节为单位）。

  **返回值**：

  返回值canon_name中前canon_len字节的内容。

  **使用说明**：

  - 如果入参`name`是NULL，那么输出为NULL。
  - 如果名称不是点分隔的名称，并且名称首尾都是双引号，那么去掉这两个引号。否则使用NLS_UPPER转换为大写。注意，这种情况不包括带有特殊字符（如空格）的名字，但没有用双引号包围的情况。
  - 如果name是一个点分隔的名称（dotted name，例如a."b''.c）,对于点分名称中的每个组件，如果组件以双引号开头和结尾，则不会对该组件进行转换。否则使用NLS_UPPER转换为大写，并将开始和结束双引号应用于此组件的大写形式。在这种情况下，每个规范化的组件将在输入位置连接在一起，用“.”分隔。
  - a[.b]*之后的任何其他字符都将被忽略。
  - 该接口不处理形如'A B.'的情形。

- COMMA_TO_TABLE(list IN VARCHAR2, tablen OUT INTEGER, tab OUT VARCHAR2[]);

  **描述**：该存储过程将一个由逗号分隔的名字列表转换为一个 PL/SQL 名字表。

  **参数说明**：

  - `list`：由逗号分隔的名字列表。
  - `tablen`：PL/SQL 表中的表数量。
  - `tab`：包含名字列表的 PL/SQL 表。

  **返回值**：

  返回一个PL、SQL表，其中值包含1..n，而n+1为null。

  **使用说明**：

  - 列表必须是非空的逗号分隔列表：任何不是逗号分隔列表的内容都会被拒绝。双引号内的逗号不计入。

  - 逗号分隔列表中的条目不能包含多字节字符。

  - tab 中的值是从原始列表中复制的，没有进行任何转换。

  - 如果分隔符之间的字符串长度超过 63 个字节，该过程将失败。

- COMPILE_SCHEMA(schema IN VARCHAR2, compile_all IN BOOLEAN DEFAULT TRUE, reuse_settings IN BOOLEAN DEFAULT FALSE);

  **描述**：该存储过程编译指定schema的所有存储过程、函数、包、视图。暂不支持触发器。

  **参数说明**：

  - `schema`：schema名称。
  - `compile_all`：如果为TRUE，将编译schema下的所有对象；如果为FALSE，仅编译INVALID对象。
  - `reuse_settings`：表示是否应重用对象中的会话设置，或者是否应采用当前会话的设置（当前只做语法支持）。

- DB_VERSION(version OUT VARCHAR2, compatibility OUT VARCHAR2);

  **描述**：该存储过程返回数据库的版本信息。

  **参数说明**：

  - `version`：返回数据库内部版本。
  - `compatibility`：返回值与`version`的值一致。

- EXEC_DDL_STATEMENT(parse_string IN VARCHAR2);

  **描述**：此存储过程执行parse_string中的DDL语句。

  **参数说明**：

  - `parse_string`：要执行的DDL语句。

  **使用说明**：

  - 使用时需要在表前面加上schema名称，例如`public.t1`。或者设置GUC参数`set behavior_compat_options="bind_procedure_searchpath";`。
  - 仅支持执行DDL语句，一次只支持执行单个SQL。

- EXPAND_SQL_TEXT(input_sql_text IN CLOB, output_sql_text OUT CLOB);

  **描述**：递归地将输入SQL查询中的任何视图引用替换成相应的视图子查询。

  **参数说明**：

  - `input_sql_text`：输入的sql查询文本。
  - `output_sql_text`：view-expanded查询文本。

  **使用说明**：

  - 使用时需要在要解析的表前面加上schema名称，例如`public.t1`。或者设置GUC参数`set behavior_compat_options="bind_procedure_searchpath";`。
  - 解析时会对视图及表进行权限校验。

- FORMAT_CALL_STACK RETURN VARCHAR2;

  **描述**：此函数用于格式化当前调用堆栈。该函数可以在任何存储过程或触发器上使用，以访问调用堆栈。

  **返回值**：返回调用堆栈。

- FORMAT_ERROR_BACKTRACE RETURN VARCHAR2;

  **描述**：即使子程序是从外部作用域中的异常处理程序调用的，此函数也会在引发异常的点显示调用堆栈。

  **返回值**：回溯字符串。如果当前没有处理错误，则返回 `NULL`。 

- FORMAT_ERROR_STACK RETURN VARCHAR2

  **描述**：此函数用于格式化当前错误堆栈。异常处理程序可以使用该函数查看完整的错误堆栈。

  **返回值**：返回错误堆栈。

- GET_CPU_TIME RETURN NUMBER;

  **描述**：此函数返回当前CPU处理时间的度量值，单位为百分之一秒。两次调用返回的时间差衡量的是这两点之间的CPU处理时间（而不是总运行时间）。

  **返回值**：代表从某个时刻算起的时间，单位为为百分之一秒。

- GET_ENDIANNESS RETURN NUMBER;

  **描述**：此函数用于获取数据库平台的字节序。

  **返回值**：指示数据库平台的字节序：1表示大端序，2表示小端序。

- GET_HASH_VALUE(name VARCHAR2, base NUMBER, hash_size NUMBER);

  **描述**：此函数计算给定string的hash值。

  **参数说明**：

  - `name`：要进行hash计算的字符串。
  - `base`：开始返回哈希值的基值，取值范围为`-2147483648~2147483647`。
  - `hash_size`：所需的哈希表大小，取值范围为`1~2147483647`。

  **返回值**：基于输入字符串的哈希值。

- GET_SQL_HASH(name IN VARCHAR2, hash OUT RAW, last4byte OUT NUMBER);

  **描述**：此存储过程使用MD5算法计算给定字符串的哈希值。

  **参数说明**：

  - `name`：要计算的hash值的字符串。
  - `hash`：存储16字节返回的hash值。
  - `last4byte`：存储最后四个字节的值。

  **返回值**：基于输入字符串的哈希值（最后4个字节）。MD5哈希算法计算一个16字节的哈希值存储在`hash`中，`last4byte`最后4字节。

- GET_TIME RETURN NUMBER;

  **描述**：此函数用于以百分之一秒为单位返回当前时间。该函数主要用于确定经过的时间。

  **返回值**：时间是从调用子程序的时间点算起的百分之一秒数。

- NAME_RESOLVE(name IN VARCHAR2, context IN NUMBER, schema OUT VARCHAR2, part1 OUT VARCHAR2, part2 OUT VARCHAR2, dblink OUT VARCHAR2, part1_type OUT NUMBER, object_number OUT NUMBER);

  **描述**：此过程解析给定的名称，包括同义词翻译和必要的授权检查。

  **参数说明**：

  - `name`：对象名。可以是[[a.]b.]c[@d]的形式，其中a、b、c是SQL标识符，d是dblink。不对dblink执行语法检查。如果指定了dblink，或者如果名称解析为dblink，则不会解析对象，但会填写schema、part1、part2和dblink OUT参数。a、 b和c可以是分隔标识符，并且可以包含全球化支持（NLS）字符（单字节和多字节）。
  - `context`：必须为0~10之间的整数；
    - 0：table
    - 1：PL/SQL
    - 2：sequences
    - 3：trigger
    - 4：Java Source（当前暂不支持该类型）
    - 5：Java resource（当前暂不支持该类型）
    - 6：Java class（当前暂不支持该类型）
    - 7：type
    - 8：Java shared data（当前暂不支持该类型）
    - 9：index
    - 10：若带有@dblink，则只做解析，不查找对象；否则报错
  - `schema`：对象的schema，如果名称中没有schema，则通过解析name确定schema。
  - `part1`：name的第一部分。名称的类型指定为part1_type。
  - `part2`：如果非空，则为子程序名。如果part1不是NULL，则子程序在part1指示的包内。如果part1为NULL，则该子程序是顶级子程序。
  - `dblink`：如果非空，那么数据库连接要么被指定为name的一部分，要么name是解析为数据库链接的同义词。在这种情况下，如果需要进一步的名称转换。
  - `part1_type`：part1的类型
    - 1：index
    - 2：table
    - 4：view
    - 5：synonym
    - 6：sequence
    - 7：procedure(top level)
    - 8：function(top level)
    - 9：package
    - 12：trigger
    - 13：type
  - `object_number`：对象标识符。
  
  **使用说明**：
  
  - 查找schema为当前登录用户同名schema或者public下。
  - 解析表与视图时实际可以在`context=0、2、7`时查询到信息；解析sequence实际可以在`context=2、7`时查询到信息。

- NAME_TOKENIZE(name IN VARCHAR2, a OUT VARCHAR2, b OUT VARCHAR2, c OUT VARCHAR2, dblink OUT VARCHAR2, nextpos OUT BINARY_INTEGER);

  **描述**：此过程调用解析器将给定名称解析为`a[.b[.c]][@dblink]`。去掉双引号，如果没有引号，则转换为大写。它没有进行语义分析。缺失的值保留为NULL。

  **参数说明**：

  - `name`：输入的name，由SQL标识符组成（例如，scott.foo@dblink）。
  - `a`：输出名称的第一个token。
  - `b`：输出名称的第二个token，如果存在。
  - `c`：输出名的第三个token，如果存在。
  - `dblink`：输出name的dblink。
  - `nextpos`：解析输入name后的下一个位置。

- OLD_CURRENT_SCHEMA RETURN VARCHAR2;

  **描述**：即返回当前会话的schema值。

- OLD_CURRENT_USER RETURN VARCHAR2;

  **描述**：即返回当前会话的user值。

- IS_BIT_SET(r IN RAW, n IN NUMBER) RETURN NUMBER;

  **描述**：此函数检查给定RAW值中给定位的位设置。

  **参数说明**：

  - `r`：输入的十六进制RAW值。
  - `n`：r中要校验的bit。

  **返回值**：如果原始r中的为n被设置，则返回1，否则返回0。
  
  **使用说明**：
  
  - 位从高到低编号，最低位为位号1。
  - 参数二可以使用小数作为入参时，使用四舍五入规则转换。

- IS_CLUSTER_DATABASE RETURN BOOLEAN;

  **描述**：此函数用于确定此数据库是否以RAC数据库模式运行。

  **返回值**：当前版本中此函数返回FALSE。

- TABLE_TO_COMMA(tab IN VARCHAR2[], tablen OUT BINARY_INTEGER, list OUT VARCHAR2);

  **描述**：此过程接受一个 PL/SQL 表，该表的范围是 1..n，并以第 n+1 个位置为 null 来终止。

  **参数说明**：

  - `tab`：包含名字列表的 PL/SQL 表。
  - `tablen`：PL/SQL 表中的表数量。
  - `list`：由逗号分隔的表列表。

  **返回值**：由逗号分隔的列表以及在表中找到的元素数量。

#### 函数使用

- ANALYZE_DATABASE 使用

```sql
openGauss=# call gms_utility.analyze_database('COMPUTE');
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_database('ESTIMATE', estimate_percent=>80);
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_database('ESTIMATE', estimate_rows=>10000, method_opt=>'FOR TABLE');
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_database('DELETE');
 analyze_database
------------------

(1 row)
```

- ANALYZE_SCHEMA 使用

```sql
openGauss=# call gms_utility.analyze_schema('public', 'COMPUTE');
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_schema('public', 'ESTIMATE', estimate_percent=>80);
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_schema('public', 'ESTIMATE', estimate_rows=>10000, method_opt=>'FOR TABLE');
 analyze_database
------------------

(1 row)
openGauss=# call gms_utility.analyze_schema('public', 'DELETE');
 analyze_database
------------------

(1 row)
```

- CANONICALIZE 使用

```sql
openGauss=# declare
openGauss-# canon_name varchar2(100);
openGauss-# begin
openGauss$# gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 100);
openGauss$# raise info 'canon_name: %', canon_name;
openGauss$# end;
openGauss$# /
INFO:  canon_name: "KOLL"."ROOY"."NUUOP"."A"
ANONYMOUS BLOCK EXECUTE
```

- COMMA_TO_TABLE 使用

```sql
openGauss=# declare
openGauss-#     tablen binary_integer := 0;
openGauss-#     tab varchar2[];
openGauss-#     i int;
openGauss-# list varchar2 := 'gaussdb.dept, gaussdb.emp, gaussdb.jobhist';
openGauss-# begin
openGauss$#     gms_utility.comma_to_table(list, tablen, tab);
openGauss$#     gms_output.put_line('table len: ' || tablen);
openGauss$#     for i in 1..tablen loop
openGauss$#         raise info 'tablename: %', tab(i);
openGauss$#     end loop;
openGauss$# end;
openGauss$# /
INFO:  tablename: gaussdb.dept
INFO:  tablename:  gaussdb.emp
INFO:  tablename:  gaussdb.jobhist
ANONYMOUS BLOCK EXECUTE
```

- COMPILE_SCHEMA 使用

```sql
openGauss=# call gms_utility.compile_schema('public', false);
 compile_schema
----------------

(1 row)
openGauss=# call gms_utility.compile_schema('public');
 compile_schema
----------------

(1 row)
```

- DB_VERSION 使用

```sql
openGauss=# declare
openGauss-#     version varchar2(50);
openGauss-#     compatibility varchar2(50);
openGauss-# begin
openGauss$#     gms_utility.db_version(version, compatibility);
openGauss$#     raise info 'version: %', version;
openGauss$#     raise info 'compatibility: %', compatibility;
openGauss$# end;
openGauss$# /
INFO:  version: openGauss 7.0.0-RC1
INFO:  compatibility: openGauss 7.0.0-RC1
ANONYMOUS BLOCK EXECUTE
```

- EXEC_DDL_STATEMENT 使用

```sql
openGauss=# call gms_utility.exec_ddl_statement('create table public.t_exec_ddl (c1 int, c2 text);');
 exec_ddl_statement
--------------------

(1 row)
openGauss=# call gms_utility.exec_ddl_statement('alter table public.t_exec_ddl add column c3 boolean default true');
 exec_ddl_statement
--------------------

(1 row)
openGauss=# call gms_utility.exec_ddl_statement('drop table public.t_exec_ddl');
 exec_ddl_statement
--------------------

(1 row)
```

- EXPAND_SQL_TEXT 使用

```sql
openGauss=# create table test_dx(id int);
CREATE TABLE
openGauss=# create view view1 as select * from test_dx;
CREATE VIEW
openGauss=# declare
openGauss-#     input_sql_text clob := 'select * from public.view1';
openGauss-#     output_sql_text clob;
openGauss-# begin
openGauss$#     gms_utility.expand_sql_text(input_sql_text, output_sql_text);
openGauss$#     raise info 'output_sql_text: %', output_sql_text;
openGauss$# end;
openGauss$# /
INFO:  output_sql_text: SELECT id FROM (SELECT test_dx.id FROM public.test_dx) view1
ANONYMOUS BLOCK EXECUTE
```

- FORMAT_CALL_STACK 使用

```sql
openGauss=# create or replace function t_inner
openGauss-# returns void as $$
openGauss$# begin
openGauss$#     raise info 't_inner call stack: ';
openGauss$#     raise info '%', gms_utility.format_call_stack();
openGauss$# end;
openGauss$# $$ language plpgsql;
CREATE FUNCTION

openGauss=# select t_inner();
INFO:  t_inner call stack:
CONTEXT:  referenced column: t_inner
INFO:           4    t_inner()
CONTEXT:  referenced column: t_inner
 t_inner
---------

(1 row)
```

- FORMAT_ERROR_BACKTRACE 使用

```sql
openGauss=# create or replace function t_inner(a int, b int)
openGauss-# returns int as $$
openGauss$# declare
openGauss$#     res int := 0;
openGauss$# begin
openGauss$#     res = a / b;
openGauss$#     return res;
openGauss$# exception
openGauss$#     when others then
openGauss$#     raise exception 'expected exception';
openGauss$# end;
openGauss$# $$ language plpgsql;
CREATE FUNCTION
openGauss=# create or replace function t_outter(a int, b int)
openGauss-# returns int as $$
openGauss$# declare
openGauss$#     res int := 0;
openGauss$# begin
openGauss$#     res := t_inner(a, b);
openGauss$#     return res;
openGauss$# exception
openGauss$#     when others then
openGauss$#     raise info '%', gms_utility.format_error_backtrace();
openGauss$#     return -1;
openGauss$# end;
openGauss$# $$ language plpgsql;
CREATE FUNCTION
openGauss=# select t_outter(100, 0);
INFO:  16777248: PL/pgSQL function t_outter(integer,integer) line 5 at assignment
16777248: referenced column: t_outter

CONTEXT:  referenced column: t_outter
 t_outter
----------
       -1
(1 row)
```

- FORMAT_ERROR_STACK 使用

```sql
openGauss=# create or replace function t_inner(a int, b int)
openGauss-# returns int as $$
openGauss$# declare
openGauss$#     res int := 0;
openGauss$# begin
openGauss$#     res = a / b;
openGauss$#     return res;
openGauss$# exception
openGauss$#     when others then
openGauss$#     raise exception 'expected exception';
openGauss$# end;
openGauss$# $$ language plpgsql;
CREATE FUNCTION

openGauss=# create or replace function t_outter(a int, b int)
openGauss-# returns int as $$
openGauss$# declare
openGauss$#     res int := 0;
openGauss$# begin
openGauss$#     res := t_inner(a, b);
openGauss$#     return res;
openGauss$# exception
openGauss$#     when others then
openGauss$#     raise info '%s', gms_utility.format_error_stack();
openGauss$#     return -1;
openGauss$# end;
openGauss$# $$ language plpgsql;
CREATE FUNCTION
openGauss=# select t_outter(100, 0);
INFO:  16777248: expected exception
PL/pgSQL function t_outter(integer,integer) line 5 at assignment
referenced column: t_outters
CONTEXT:  referenced column: t_outter
 t_outter
----------
       -1
(1 row)
```

- GET_CPU_TIME 使用

```sql
openGauss=# declare
openGauss-#     t1 number := 0;
openGauss-#     t2 number := 0;
openGauss-#     timeDelta number;
openGauss-#     i integer;
openGauss-#     sum integer;
openGauss-# begin
openGauss$#     t1 := gms_utility.get_cpu_time();
openGauss$#     for i in 1..1000000 loop
openGauss$#        sum := sum + i * 2 + i / 2;
openGauss$#     end loop;
openGauss$#     t2 := gms_utility.get_cpu_time();
openGauss$#     timeDelta = t2 - t1;
openGauss$#     raise info 'cpuTimeDelta: %', timeDelta;
openGauss$# end;
openGauss$# /
INFO:  cpuTimeDelta: 117
ANONYMOUS BLOCK EXECUTE
```

- GET_ENDIANNESS 使用

```sql
openGauss=# select gms_utility.get_endianness();
 get_endianness
----------------
              2
(1 row)
```

- GET_HASH_VALUE 使用

```sql
openGauss=# select gms_utility.get_hash_value('Today is a good day', 1000, 1024);
 get_hash_value
----------------
           1054
(1 row)
```

- GET_SQL_HASH 使用

```sql
openGauss=# declare
openGauss-#     hash    raw(50);
openGauss-#     l4b     number;
openGauss-# begin
openGauss$#     gms_utility.get_sql_hash('Today is a good day!', hash, l4b);
openGauss$#     raise info 'hash: %', hash;
openGauss$#     raise info 'last4byte: %', l4b;
openGauss$# end;
openGauss$# /
INFO:  hash: 834E5BE13C0240D4F1AEBB1BCE7205AF
INFO:  last4byte: 2936369870
ANONYMOUS BLOCK EXECUTE
```

- GET_TIME 使用

```sql
openGauss=# declare
openGauss-#     t1 number;
openGauss-#     t2 number;
openGauss-#     td number;
openGauss-#     sum bigint := 0;
openGauss-#     i int := 0;
openGauss-# begin
openGauss$#     t1 = gms_utility.get_time();
openGauss$#     for i in 1..1000000 loop
openGauss$#         sum := sum + i * 2 - i / 2;
openGauss$#     end loop;
openGauss$#     t2 = gms_utility.get_time();
openGauss$#
openGauss$#     td = t2 - t1;
openGauss$#     raise info 'costtime: %', td;
openGauss$# end;
openGauss$# /
INFO:  costtime: 188
ANONYMOUS BLOCK EXECUTE
```

- NAME_RESOLVE 使用

```sql
openGauss=# create table t_resolve (c1 int, c2 text);
CREATE TABLE

openGauss=# declare
openGauss-#     name varchar2 := 'public.t_resolve';
openGauss-#     context number := 0;
openGauss-#     schema  varchar2;
openGauss-#     part1   varchar2;
openGauss-#     part2   varchar2;
openGauss-#     dblink  varchar2;
openGauss-#     part1_type  number;
openGauss-#     object_number   number;
openGauss-# begin
openGauss$#     gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
openGauss$#     raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
openGauss$# end;
openGauss$# /
INFO:  schema = PUBLIC, part1 = T_RESOLVE, part2 = <NULL>, dblink = <NULL>, part1_type = 2, object_number = 254220
ANONYMOUS BLOCK EXECUTE

openGauss=# declare
openGauss-#     name varchar2 := 't_resolve';
openGauss-#     context number := 0;
openGauss-#     schema  varchar2;
openGauss-#     part1   varchar2;
openGauss-#     part2   varchar2;
openGauss-#     dblink  varchar2;
openGauss-#     part1_type  number;
openGauss-#     object_number   number;
openGauss-# begin
openGauss$#     gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
openGauss$#     raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
openGauss$# end;
openGauss$# /
INFO:  schema = PUBLIC, part1 = T_RESOLVE, part2 = <NULL>, dblink = <NULL>, part1_type = 2, object_number = 254220
ANONYMOUS BLOCK EXECUTE
```

- NAME_TOKENIZE 使用

```sql
openGauss=# declare
openGauss-#     name varchar2(50) := 'peer.lokppe.vuumee@ookeyy';
openGauss-#     a   varchar2(50);
openGauss-#     b   varchar2(50);
openGauss-#     c   varchar2(50);
openGauss-#     dblink  varchar2(50);
openGauss-#     nextpos integer;
openGauss-# begin
openGauss$#     gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
openGauss$#     raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
openGauss$# end;
openGauss$# /
INFO:  a = PEER, b = LOKPPE, c = VUUMEE, dblink = OOKEYY, nextpos = 25
ANONYMOUS BLOCK EXECUTE
```

- OLD_CURRENT_SCHEMA 使用

```sql
openGauss=# select gms_utility.old_current_schema();
 old_current_schema
--------------------
 public
(1 row)
```

- OLD_CURRENT_USER 使用

```sql
openGauss=# select gms_utility.old_current_user();
 old_current_user
------------------
 omm
(1 row)
```

- IS_BIT_SET 使用

```sql
openGauss=# declare
openGauss-# r raw(50) := '123456AF';
openGauss-# result NUMBER;
openGauss-# pos NUMBER;
openGauss-# begin
openGauss$# for pos in 1..32 loop
openGauss$#

openGauss$# result := gms_utility.is_bit_set(r, pos);
openGauss$#

openGauss$# raise info 'position = %, result = %', pos, result;
openGauss$# end loop;
openGauss$# end;
openGauss$# /
INFO:  position = 1, result = 1
INFO:  position = 2, result = 1
INFO:  position = 3, result = 1
INFO:  position = 4, result = 1
INFO:  position = 5, result = 0
INFO:  position = 6, result = 1
INFO:  position = 7, result = 0
INFO:  position = 8, result = 1
INFO:  position = 9, result = 0
INFO:  position = 10, result = 1
INFO:  position = 11, result = 1
INFO:  position = 12, result = 0
INFO:  position = 13, result = 1
INFO:  position = 14, result = 0
INFO:  position = 15, result = 1
INFO:  position = 16, result = 0
INFO:  position = 17, result = 0
INFO:  position = 18, result = 0
INFO:  position = 19, result = 1
INFO:  position = 20, result = 0
INFO:  position = 21, result = 1
INFO:  position = 22, result = 1
INFO:  position = 23, result = 0
INFO:  position = 24, result = 0
INFO:  position = 25, result = 0
INFO:  position = 26, result = 1
INFO:  position = 27, result = 0
INFO:  position = 28, result = 0
INFO:  position = 29, result = 1
INFO:  position = 30, result = 0
INFO:  position = 31, result = 0
INFO:  position = 32, result = 0
ANONYMOUS BLOCK EXECUTE
```

- IS_CLUSTER_DATABASE 使用

```sql
openGauss=# select gms_utility.is_cluster_database();
 is_cluster_database
---------------------
 f
(1 row)
```

- TABLE_TO_COMMA 使用

```sql
openGauss=# declare
openGauss-#     tab varchar2[];
openGauss-#     tablen  integer;
openGauss-#     list    varchar2;
openGauss-# begin
openGauss$#     tab(1) := 'build';
openGauss$#     tab(2) := 'test';
openGauss$#     tab(3) := 'date';
openGauss$#     gms_utility.table_to_comma(tab, tablen, list);
openGauss$#     raise info 'tablen: %, result: %', tablen, list;
openGauss$# end;
openGauss$# /
INFO:  tablen: 3, result: build,test,date
ANONYMOUS BLOCK EXECUTE
```
