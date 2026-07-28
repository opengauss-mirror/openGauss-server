# gms_assert

## gms_assert概述

gms_assert是一个基于openGauss的插件，为用户提供了验证输入值的功能。目前支持的接口有：GMS_ASSERT.NOOP、GMS_ASSERT.ENQUOTE_LITERAL、GMS_ASSERT.ENQUOTE_NAME、GMS_ASSERT.SIMPLE_SQL_NAME、GMS_ASSERT.QUALIFIED_SQL_NAME、GMS_ASSERT.SCHEMA_NAME、GMS_ASSERT.SQL_OBJECT_NAME。

## gms_assert限制

- 仅支持Create extension命令方式加载插件。

## gms_assert安装

openGauss打包编译时默认已经包含了gms_assert，可以在安装完openGauss后，直接通过`create extension gms_assert;`加载插件。

## gms_assert使用

### 创建Extension<a name="section21088306113"></a>

创建gms_assert extension可直接使用`create extension gms_assert;`命令进行创建。

```
openGauss=# CREATE Extension gms_assert;
```

### 使用Extension<a name="section107391050141118"></a>

#### 函数声明

- NOOP(`str` IN TEXT);

  **描述**：空操作函数，将输入值直接返回，不做任何验证或处理。适用于不需要验证输入值，快速返回结果的情况。

  **参数说明**：

  - `str`：指定要验证的文本。

  **返回值**：

  返回`str`。

- ENQUOTE_LITERAL(`str` IN TEXT);

  **描述**：为输入文本`str`首尾添加单引号（如果`str`首尾已有单引号则不再额外添加），并验证`str`中首尾之外的单引号是否成对出现。

  **参数说明**：

  - `str`：指定要验证并添加单引号的文本。

  **返回值**：

  返回`str`首尾添加了单引号的结果。如果`str`中的单引号没有成对出现，则抛出异常。

- ENQUOTE_NAME(`str` IN TEXT, capitalize IN BOOLEAN DEFAULT TRUE);

  **描述**：为输入文本`str`首尾添加双引号（如果`str`首尾已有双引号则不再额外添加），并验证`str`中首尾之外的双引号是否成对出现。

  **参数说明**：

  - `str`：指定要验证并添加双引号的文本。
  - `capitalize`：是否在添加双引号前将`str`转为大写。

  **返回值**：

  返回`str`首尾添加了双引号的结果。如果`str`中的双引号没有成对出现，则抛出异常。

- SIMPLE_SQL_NAME(`str` IN TEXT);

  **描述**：验证输入是否是一个简单的SQL名称，即是否首尾有双引号包裹，或以字母或下划线开头，后续仅由数字、字母和部分特殊符号（`_`、`$`、`#`）构成。`str`前后可有任意长度的空格。

  **参数说明**：

  - `str`：指定要验证的文本。

  **返回值**：

  返回`str`去除前后空格后的结果。如果`str`不是一个简单的SQL名称，则会抛出异常。

- QUALIFIED_SQL_NAME(`str` IN TEXT);

  **描述**：验证输入是否是一个合格的SQL名称。一个合格SQL名称满足以下语法构成规则，其中`simple_name`是一个简单的SQL名称。

  ```
  qualified_sql_name ::= local_qualified_name ['@' local_qualified_name ['@'simple_name]]
  local_qualified_name ::= simple_name {'.' simple_name}
  ```

  **参数说明**：

  - `str`：指定要验证的文本。

  **返回值**：

  返回`str`。如果`str`不是一个合格的SQL名称，则会抛出异常。

- SCHEMA_NAME(`str` IN TEXT);

  **描述**：验证输入是否是一个现存模式的名称，如果不是则抛出异常。

  **参数说明**：

  - `str`：指定要验证的文本。

  **返回值**：

  返回`str`。如果`str`不是一个现存模式的名称，则会抛出异常。

- SQL_OBJECT_NAME(`str` IN TEXT);

  **描述**：验证输入是否是一个现存数据库对象的名称。

  **参数说明**：

  - `str`：指定要验证的文本。

  **返回值**：

  返回`str`。如果`str`不是一个现存数据库对象的名称，则抛出异常。

  **使用说明**：

  - 函数对数据库对象名称的判断大小写不敏感。
  - 函数对数据库对象名称的判断受用户本身权限的约束。输入用户无权访问的数据库对象的名称，函数依然会抛出异常。

#### 函数使用

- noop 使用

```sql
openGauss=# SELECT gms_assert.noop(NULL);
 noop
------

(1 row)

openGauss=# SELECT gms_assert.noop(E'O\'hello');
  noop
---------
 O'hello
(1 row)

openGauss=# SELECT gms_assert.noop(4.1);
 noop
------
 4.1
(1 row)

openGauss=# SELECT gms_assert.noop('A line. ');
   noop
----------
 A line.
(1 row)
```

- enquote_literal使用

```sql
openGauss=# SELECT gms_assert.enquote_literal(NULL);
 enquote_literal
-----------------
 ''
(1 row)

openGauss=# SELECT gms_assert.enquote_literal('AbC');
 enquote_literal
-----------------
 'AbC'
(1 row)

openGauss=# SELECT gms_assert.enquote_literal('A''''bC');
 enquote_literal
-----------------
 'A''bC'
(1 row)

openGauss=# SELECT gms_assert.enquote_literal('''AbC''');
 enquote_literal
-----------------
 'AbC'
(1 row)

openGauss=# SELECT gms_assert.enquote_literal('''AbC');
ERROR:  numeric or value error
CONTEXT:  referenced column: enquote_literal
openGauss=# SELECT gms_assert.enquote_literal('A''bC');
ERROR:  numeric or value error
CONTEXT:  referenced column: enquote_literal
```

- ENQUOTE_NAME

```sql
openGauss=# SELECT gms_assert.enquote_name(NULL);
 enquote_name
--------------
 ""
(1 row)

openGauss=# SELECT gms_assert.enquote_name('Ab_c');
 enquote_name
--------------
 "AB_C"
(1 row)

openGauss=# SELECT gms_assert.enquote_name('A""b_c');
 enquote_name
--------------
 "A""B_C"
(1 row)

openGauss=# SELECT gms_assert.enquote_name('"Ab _c"');
 enquote_name
--------------
 "Ab _c"
(1 row)

openGauss=# SELECT gms_assert.enquote_name('A"ss"b_c');
ERROR:  invalid SQL name
CONTEXT:  referenced column: enquote_name
openGauss=# SELECT gms_assert.enquote_name('Ab_c', true);
 enquote_name
--------------
 "AB_C"
(1 row)

openGauss=# SELECT gms_assert.enquote_name('Ab_c', false);
 enquote_name
--------------
 "Ab_c"
(1 row)

openGauss=# SELECT gms_assert.enquote_name('"Ab_c"', true);
 enquote_name
--------------
 "Ab_c"
(1 row)
```

- SIMPLE_SQL_NAME

```sql
opengauss=# SELECT gms_assert.simple_sql_name(NULL);
ERROR:  invalid SQL name
CONTEXT:  referenced column: simple_sql_name
opengauss=# SELECT gms_assert.simple_sql_name(' a_1B$# ');
 simple_sql_name
-----------------
 a_1B$#
(1 row)

opengauss=# SELECT gms_assert.simple_sql_name(' "a_ *B" ');
 simple_sql_name
-----------------
 "a_ *B"
(1 row)

opengauss=# SELECT gms_assert.simple_sql_name('a_""B');
ERROR:  invalid SQL name
CONTEXT:  referenced column: simple_sql_name
```

- QUALIFIED_SQL_NAME

```sql
opengauss=# SELECT gms_assert.qualified_sql_name(NULL);
ERROR:  invalid qualified SQL name
CONTEXT:  referenced column: qualified_sql_name
opengauss=# SELECT gms_assert.qualified_sql_name('abc');
 qualified_sql_name
--------------------
 abc
(1 row)

opengauss=# SELECT gms_assert.qualified_sql_name('abc@"def*"@GHI');
 qualified_sql_name
--------------------
 abc@"def*"@GHI
(1 row)

opengauss=# SELECT gms_assert.qualified_sql_name('abc@"def*"@GHI.jkl');
ERROR:  invalid qualified SQL name
CONTEXT:  referenced column: qualified_sql_name
```

- SCHEMA_NAME

```sql
opengauss=# SELECT gms_assert.schema_name(NULL);
ERROR:  invalid schema
CONTEXT:  referenced column: schema_name
opengauss=# CREATE SCHEMA test;
CREATE SCHEMA
opengauss=# SELECT gms_assert.schema_name('test');
 schema_name
-------------
 test
(1 row)

opengauss=# SELECT gms_assert.schema_name('Test');
ERROR:  invalid schema
CONTEXT:  referenced column: schema_name
opengauss=# DROP SCHEMA test;
DROP SCHEMA
opengauss=# SELECT gms_assert.schema_name('test');
ERROR:  invalid schema
CONTEXT:  referenced column: schema_name
```

- SQL_OBJECT_NAME

```sql
opengauss=# SELECT gms_assert.sql_object_name(NULL);
ERROR:  invalid object name
CONTEXT:  referenced column: sql_object_name
opengauss=# CREATE TABLE tb1(col1 int);
CREATE TABLE
opengauss=# CREATE SYNONYM syn1 FOR tb1;
CREATE SYNONYM
opengauss=# SELECT gms_assert.sql_object_name('tb1');
 sql_object_name
-----------------
 tb1
(1 row)

opengauss=# SELECT gms_assert.sql_object_name('syn1');
 sql_object_name
-----------------
 syn1
(1 row)

opengauss=# SELECT gms_assert.sql_object_name('SYN1');
 sql_object_name
-----------------
 SYN1
(1 row)

opengauss=# DROP TABLE tb1;
DROP TABLE
opengauss=# SELECT gms_assert.sql_object_name('tb1');
ERROR:  invalid object name
CONTEXT:  referenced column: sql_object_name
```

### 删除Extension

在openGauss中删除gms_assert Extension的方法如下所示：

```
openGauss=# DROP extension gms_assert [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
