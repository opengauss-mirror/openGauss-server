# gms_assert

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:04.216Z pushedAt=2026-09-24T10:51:04.734Z -->

## gms_assert Overview

gms_assert is an extension based on openGauss that provides users with the ability to validate input values. The currently supported interfaces are: GMS_ASSERT.NOOP, GMS_ASSERT.ENQUOTE_LITERAL, GMS_ASSERT.ENQUOTE_NAME, GMS_ASSERT.SIMPLE_SQL_NAME, GMS_ASSERT.QUALIFIED_SQL_NAME, GMS_ASSERT.SCHEMA_NAME, and GMS_ASSERT.SQL_OBJECT_NAME.

## gms_assert Limitations

- Only the CREATE EXTENSION command is supported for loading the extension.

## Installing gms_assert

openGauss already includes gms_assert by default during packaging and compilation. After installing openGauss, you can directly load the extension by running `create extension gms_assert;`.

## gms_assert Usage

### Creating an Extension<a name="section21088306113"></a>

The gms_assert extension can be created directly using the <code>create extension gms_assert;</code> command.

```
openGauss=# CREATE Extension gms_assert;
```

### Using Extension<a name="section107391050141118"></a>

#### Function Declaration

- NOOP(`str` IN TEXT);

  **Description**: A no-operation function that returns the input value directly without any validation or processing. It is suitable for scenarios where no validation of the input value is required and a quick result is needed.

  **Parameter Description**:

  - `str`: Specifies the text to be validated.

  **Return Value**:

  Returns `str`.

- ENQUOTE_LITERAL(`str` IN TEXT);

  **Description**: Adds single quotes to the beginning and end of the input text `str` (if `str` already has single quotes at the beginning and end, no additional quotes are added), and validates whether single quotes other than those at the beginning and end of `str` appear in pairs.

  **Parameter Description**:

  - `str`: Specifies the text to be validated and enclosed in single quotation marks.

  **Return Value**:

  Returns the result of `str` with single quotation marks added at the beginning and end. If the single quotation marks in `str` do not appear in pairs, an exception is thrown.

- ENQUOTE_NAME(`str` IN TEXT, capitalize IN BOOLEAN DEFAULT TRUE);

  **Description**: Adds double quotation marks at the beginning and end of the input text `str` (if `str` already has double quotation marks at both ends, no additional marks are added), and validates whether the double quotation marks other than those at the beginning and end appear in pairs in `str`.

  **Parameter Description**:

  - `str`: Specifies the text to be validated and enclosed in double quotes.
  - `capitalize`: Whether to convert `str` to uppercase before adding double quotes.

  **Return Value**:

  Returns the result of `str` with double quotes added at both ends. If the double quotes in `str` are not paired, an exception is thrown.

- SIMPLE_SQL_NAME(`str` IN TEXT);

  **Description**: Validates whether the input is a simple SQL name, that is, whether it is enclosed in double quotes at the beginning and end, or starts with a letter or underscore, followed only by digits, letters, and certain special characters (`_`, `$`, `#`). `str` may have arbitrary whitespace before and after.

  **Parameter Description**:

  - `str`: Specifies the text to validate.

  **Return Value**:

  Returns the result of `str` with leading and trailing spaces removed. If `str` is not a simple SQL name, an exception is thrown.

- QUALIFIED_SQL_NAME(`str` IN TEXT);

  **Description**: Validates whether the input is a qualified SQL name. A qualified SQL name conforms to the following syntax composition rules, where `simple_name` is a simple SQL name.

  ```
  qualified_sql_name ::= local_qualified_name ['@' local_qualified_name ['@'simple_name]]
  local_qualified_name ::= simple_name {'.' simple_name}
  ```

  **Parameter Description**:

  - `str`: Specifies the text to be validated.

  **Return Value**:

  Returns `str`. If `str` is not a valid SQL name, an exception is thrown.

- SCHEMA_NAME(`str` IN TEXT);

  **Description**: Validates whether the input is the name of an existing schema, and throws an exception if it is not.

  **Parameter Description**:

  - `str`: Specifies the text to be validated.

  **Return Value**:

  Returns `str`. If `str` is not the name of an existing schema, an exception is thrown.

- SQL_OBJECT_NAME(`str` IN TEXT);

  **Description**: Validates whether the input is the name of an existing database object.

  **Parameter Description**:

  - `str`: Specifies the text to validate.

  **Return Value**:

  Returns `str`. If `str` is not the name of an existing database object, an exception is thrown.

  **Usage Notes**:

  - The function is case-insensitive when determining database object names.
  - The function's determination of database object names is subject to the user's own permission constraints. If a user inputs the name of a database object that they do not have permission to access, the function will still throw an exception.

#### Function Usage

- noop Usage

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

- enquote_literal Usage

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

### Deleting an Extension

To delete the gms_assert extension in openGauss, use the following method:

```
openGauss=# DROP extension gms_assert [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.