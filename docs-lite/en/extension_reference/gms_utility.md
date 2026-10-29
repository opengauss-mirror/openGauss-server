# gms_utility

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:08.605Z pushedAt=2026-09-21T02:27:06.992Z -->

## gms_utility Overview

gms_utility is an openGauss-based plugin that provides users with various practical procedures and functions. Currently, the following 23 interfaces are supported:

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

## gms_utility Limitations

Only loading via the `CREATE EXTENSION` command and unloading via the `DROP EXTENSION` command are supported.

It is only supported in the database A environment. Use of this extension in non-database A environments is not recommended.

## gms_utility Installation

openGauss already includes gms_utility by default during packaging and compilation. After installation is complete, you can directly load the extension by using <code>create extension gms_utility;</code>.

## gms_utility Usage

### Creating an Extension

To create the gms_utility extension, you can directly use the `create extension gms_utility;` command.

### Using Extension

#### Function Declaration

- ANALYZE_DATABASE(method IN VARCHAR2, estimate_rows IN NUMBER DEFAULT NULL, estimate_percent IN NUMBER DEFAULT NULL, method_opt IN VARCHAR2 DEFAULT NULL);

  **Description**: This procedure analyzes the statistics of all tables and indexes in the database.

  **Parameter Description**:

  - `method`: The value range is `ESTIMATE`, `COMPUTE`, `DELETE`. If set to ESTIMATE, either the parameter `estimate_rows` or `estimate_percent` must have a value.
  - `estimate_rows`: Number of rows to estimate. The parameter must be greater than or equal to 0.
  - `estimate_percent`: Percentage to estimate. The valid range is 0–100. If `estimate_rows` is specified, this parameter is ignored.
  - `method_opt`: Method options in the following format:
    - `[FOR TABLE]`
    - `[FOR ALL [INDEXED] COLUMNS [SIZE n]]`; (Currently only syntax support is provided)
    - `[FOR ALL INDEXES]`

  **Usage Instructions**:

  - The values of the parameter `estimate_rows` and the parameter `estimate_percent` are validated only when the parameter `method=ESTIMATE`;
  - When the parameter `method=DELETE`, the value of the parameter `method_opt` cannot be set.

- ANALYZE_SCHEMA(schema IN VARCHAR2, method IN VARCHAR2, estimate_rows IN NUMBER DEFAULT NULL, estimate_percent IN NUMBER DEFAULT NULL, method_opt IN VARCHAR2 DEFAULT NULL);

  **Description**: This procedure analyzes the statistics of all tables and indexes in the specified schema.

  **Parameter Description**:

  - `schema`: Specifies the name of the schema to be analyzed.

  - `method`: The value range is `ESTIMATE`, `COMPUTE`, or `DELETE`. If set to ESTIMATE, either the `estimate_rows` or `estimate_percent` parameter must have a value.
  - `estimate_rows`: The number of rows to estimate. This parameter must be greater than or equal to 0.
  - `estimate_percent`: Percentage to estimate. The valid range is 0 to 100. If `estimate_rows` is specified, this parameter is ignored.
  - `method_opt`: Method options in the following formats:
    - `[FOR TABLE]`
    - `[FOR ALL [INDEXED] COLUMNS [SIZE n]]`; (Currently only provides syntax support)
    - `[FOR ALL INDEXES]`

  **Usage Instructions**:

  - The values of the parameter `estimate_rows` and the parameter `estimate_percent` are validated only when the parameter `method=ESTIMATE`;
  - When the parameter `method=DELETE`, the value of the parameter `method_opt` cannot be set.

- CANONICALIZE(name IN VARCHAR2, canon_name OUT VARCHAR2, canon_len IN BINARY_INTEGER);

  **Description**: This stored procedure canonicalizes a given string. It processes a single reserved word or keyword (for example, 'table') and removes spaces from a single identifier, such that ' table ' becomes TABLE.

  **Parameter Description**:

  - `name`: The string to be canonicalized.
  - `canon_name`: The canonicalized output string.
  - `canon_len`: The length (in bytes) of the string to be canonicalized.

  **Return Value**:

  The content of the first canon_len bytes in the return value canon_name.

  **Usage Instructions**:

  - If the input parameter `name` is NULL, the output is NULL.
  - If the name is not a dot-separated name and both the beginning and end of the name are double quotation marks, remove these two quotation marks. Otherwise, use NLS_UPPER to convert it to uppercase. Note that this does not include names with special characters (such as spaces) that are not enclosed in double quotation marks.
  - If name is a dot-separated name (for example, a."b''.c"), for each component in the dot-separated name, if the component begins and ends with double quotation marks, the component will not be converted. Otherwise, use NLS_UPPER to convert it to uppercase, and apply the beginning and ending double quotation marks to the uppercase form of this component. In this case, each canonicalized component will be concatenated at the input position, separated by ".".
  - Any characters after a[.b]* are ignored.
  - This interface does not handle cases in the form of 'A B.'.

- COMMA_TO_TABLE(list IN VARCHAR2, tablen OUT INTEGER, tab OUT VARCHAR2[]);

  **Description**: This stored procedure converts a comma-separated name list into a PL/SQL name table.

  **Parameter Description**:

  - `list`: A comma-separated list of names.
  - `tablen`: The number of tables in the PL/SQL table.
  - `tab`: A PL/SQL table containing the list of names.

  **Return Value**:

  Returns a PL/SQL table where the values contain 1..n, and n+1 is null.

  **Usage Instructions**:

  - The list must be a non-empty comma-separated list: anything that is not a comma-separated list will be rejected. Commas inside double quotation marks are not counted.

  - Entries in the comma-separated list cannot contain multibyte characters.

  - The values in tab are copied from the original list without any conversion.

  - If the string between delimiters exceeds 63 bytes, the procedure will fail.

- COMPILE_SCHEMA(schema IN VARCHAR2, compile_all IN BOOLEAN DEFAULT TRUE, reuse_settings IN BOOLEAN DEFAULT FALSE);

  **Description**: This stored procedure compiles all stored procedures, functions, packages, and views in the specified schema. Triggers are not supported currently.

  **Parameter Description**:

  - `schema`: Schema name.
  - `compile_all`: If TRUE, compiles all objects in the schema; if FALSE, compiles only INVALID objects.
  - `reuse_settings`: Indicates whether to reuse the session settings from the object, or whether to adopt the settings of the current session (currently only syntax support is provided).

- DB_VERSION(version OUT VARCHAR2, compatibility OUT VARCHAR2);

  **Description**: This stored procedure returns the version information of the database.

  **Parameter Description**:

  - `version`: Returns the internal version of the database.
  - `compatibility`: The return value is the same as that of `version`.

- EXEC_DDL_STATEMENT(parse_string IN VARCHAR2);

  **Description**: This stored procedure executes the DDL statement in parse_string.

  **Parameter Description**:

  - `parse_string`: The DDL statement to be executed.

  **Usage Instructions**:

  - When using, you need to prefix the table with the schema name, for example, `public.t1`. Alternatively, set the GUC parameter `set behavior_compat_options="bind_procedure_searchpath";`.
  - Only DDL statement execution is supported, and only a single SQL can be executed at a time.

- EXPAND_SQL_TEXT(input_sql_text IN CLOB, output_sql_text OUT CLOB);

  **Description**: Recursively replaces any view references in the input SQL query with the corresponding view subquery.

  **Parameter Description**:

  - `input_sql_text`: The input SQL query text.
  - `output_sql_text`: The view-expanded query text.

  **Usage Instructions**:

  - When using this function, you need to prefix the table to be parsed with the schema name, for example, `public.t1`. Alternatively, set the GUC parameter `set behavior_compat_options="bind_procedure_searchpath";`.
  - During parsing, permission checks are performed on views and tables.

- FORMAT_CALL_STACK RETURN VARCHAR2;

  **Description**: This function formats the current call stack. It can be used in any stored procedure or trigger to access the call stack.

  **Return Value**: Returns the call stack.

- FORMAT_ERROR_BACKTRACE RETURN VARCHAR2;

  **Description**: This function displays the call stack at the point where the exception was raised, even if the subprogram is called from an exception handler in an outer scope.

  **Return Value**: A backtrace string. Returns `NULL` if no error is currently being handled. 

- FORMAT_ERROR_STACK RETURN VARCHAR2

  **Description**: This function formats the current error stack. The exception handler can use this function to view the complete error stack.

  **Return Value**: Returns the error stack.

- GET_CPU_TIME RETURN NUMBER;

  **Description**: This function returns a measurement of the current CPU processing time in hundredths of a second. The difference between the times returned by two calls measures the CPU processing time (rather than the total elapsed time) between those two points.

  **Return Value**: Represents the time measured from a certain point, in hundredths of a second.

- GET_ENDIANNESS RETURN NUMBER;

  **Description**: This function is used to obtain the endianness of the database platform.

  **Return Value**: Indicates the endianness of the database platform: 1 for big-endian, 2 for little-endian.

- GET_HASH_VALUE(name VARCHAR2, base NUMBER, hash_size NUMBER);

  **Description**: This function computes the hash value of a given string.

  **Parameter Description**:

  - `name`: The string to be hashed.
  - `base`: The base value from which the hash value starts to be returned. The value range is `-2147483648–2147483647`.
  - `hash_size`: The required hash table size. The value range is `1–2147483647`.

  **Return Value**: The hash value based on the input string.

- GET_SQL_HASH(name IN VARCHAR2, hash OUT RAW, last4byte OUT NUMBER);

  **Description**: This stored procedure uses the MD5 algorithm to compute the hash value of a given string.

  **Parameter Description**:

  - `name`: The string for which the hash value is to be calculated.
  - `hash`: Stores the 16-byte hash value returned.
  - `last4byte`: Stores the value of the last four bytes.

  **Return Value**: The hash value (last 4 bytes) based on the input string. The MD5 hash algorithm calculates a 16-byte hash value stored in `hash`, with `last4byte` containing the last 4 bytes.

- GET_TIME RETURN NUMBER;

  **Description**: This function returns the current time in hundredths of a second. It is primarily used to determine elapsed time.

  **Return Value**: The time is the number of hundredths of a second measured from the point at which the subprogram is invoked.

- NAME_RESOLVE(name IN VARCHAR2, context IN NUMBER, schema OUT VARCHAR2, part1 OUT VARCHAR2, part2 OUT VARCHAR2, dblink OUT VARCHAR2, part1_type OUT NUMBER, object_number OUT NUMBER);

  **Description**: This procedure resolves the given name, including synonym translation and necessary authorization checks.

  **Parameter Description**:

  - `name`: Object name. It can be in the form of [[a.]b.]c[@d], where a, b, and c are SQL identifiers, and d is a dblink. No syntax check is performed on the dblink. If a dblink is specified, or if the name resolves to a dblink, the object is not resolved, but the schema, part1, part2, and dblink OUT parameters are filled in. a, b, and c can be delimited identifiers and may contain Globalization Support (NLS) characters (single-byte and multi-byte).
  - `context`: Must be an integer between 0 and 10.
    - 0: table
    - 1: PL/SQL
    - 2: sequences
    - 3: trigger
    - 4: Java Source (This type is currently not supported.)
    - 5: Java resource (This type is currently not supported.)
    - 6: Java class (This type is currently not supported.)
    - 7: type
    - 8: Java shared data (this type is currently not supported)
    - 9: index
    - 10: If @dblink is included, only parsing is performed without object lookup; otherwise, an error is reported.
  - <code>schema</code>: The schema of the object. If no schema is specified in the name, the schema is determined by resolving the name.
  - `part1`: The first part of name. The type of the name is specified as part1_type.
  - `part2`: If not null, it is the subprogram name. If part1 is not NULL, the subprogram is within the package indicated by part1. If part1 is NULL, the subprogram is a top-level subprogram.
  - `dblink`: If not null, the database connection is either specified as part of name, or name is a synonym that resolves to a database link. In this case, further name translation may be required.
  - `part1_type`: The type of part1
    - 1: index
    - 2: table
    - 4: view
    - 5: synonym
    - 6: sequence
    - 7: procedure (top level)
    - 8: function (top level)
    - 9: package
    - 12: trigger
    - 13: type
  - `object_number`: Object identifier.

  **Usage Instructions**:

  - The search is performed in the schema with the same name as the currently logged-in user or under public.
  - When resolving tables and views, information can actually be queried when <code>context=0, 2, 7</code>; when resolving sequences, information can actually be queried when <code>context=2, 7</code>.

- NAME_TOKENIZE(name IN VARCHAR2, a OUT VARCHAR2, b OUT VARCHAR2, c OUT VARCHAR2, dblink OUT VARCHAR2, nextpos OUT BINARY_INTEGER);

  **Description**: This procedure calls the parser to resolve the given name into <code>a[.b[.c]][@dblink]</code>. Double quotation marks are removed, and if there are no quotation marks, the name is converted to uppercase. It does not perform semantic analysis. Missing values are left as NULL.

  **Parameter Description**:

  - `name`: Input name, which consists of SQL identifiers (for example, scott.foo@dblink).
  - `a`: First token of the output name.
  - `b`: Second token of the output name, if present.
  - `c`: Third token of the output name, if present.
  - `dblink`: The dblink of the output name.
  - `nextpos`: The next position after parsing the input name.

- OLD_CURRENT_SCHEMA RETURN VARCHAR2;

  **Description**: Returns the schema value of the current session.

- OLD_CURRENT_USER RETURN VARCHAR2;

  **Description**: Returns the user value of the current session.

- IS_BIT_SET(r IN RAW, n IN NUMBER) RETURN NUMBER;

  **Description**: This function checks the bit setting of a specified bit in a given RAW value.

  **Parameter Description**:

  - `r`: The input hexadecimal RAW value.
  - `n`: the bit in r to be verified.

  **Return Value**: returns 1 if bit n in the original r is set; otherwise, returns 0.

  **Usage Instructions**:

  - Bits are numbered from high to low, with the lowest bit being bit 1.
  - When a decimal is used as the input for the second parameter, it is converted using the rounding rule.

- IS_CLUSTER_DATABASE RETURN BOOLEAN;

  **Description**: This function determines whether the database is running in RAC database mode.

  **Return Value**: In the current version, this function returns FALSE.

- TABLE_TO_COMMA(tab IN VARCHAR2[], tablen OUT BINARY_INTEGER, list OUT VARCHAR2);

  **Description**: This stored procedure accepts a PL/SQL table whose range is 1..n and is terminated by null at the n+1 position.

  **Parameter Description**:

  - `tab`: PL/SQL table containing the list of names.
  - `tablen`: Number of tables in the PL/SQL table.
  - `list`: Comma-separated list of tables.

  **Return Value**: Comma-separated list and the number of elements found in the table.

#### Function Usage

- ANALYZE_DATABASE Usage

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

- ANALYZE_SCHEMA Usage

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

- CANONICALIZE Usage

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

- COMMA_TO_TABLE Usage

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

- COMPILE_SCHEMA Usage

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

- DB_VERSION Usage

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

- EXEC_DDL_STATEMENT Usage

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

- EXPAND_SQL_TEXT Usage

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

- FORMAT_CALL_STACK Usage

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

- FORMAT_ERROR_BACKTRACE Usage

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

- FORMAT_ERROR_STACK Usage

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

- GET_CPU_TIME Usage

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

- GET_ENDIANNESS Usage

```sql
openGauss=# select gms_utility.get_endianness();
 get_endianness
----------------
              2
(1 row)
```

- GET_HASH_VALUE Usage

```sql
openGauss=# select gms_utility.get_hash_value('Today is a good day', 1000, 1024);
 get_hash_value
----------------
           1054
(1 row)
```

- GET_SQL_HASH Usage

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

- GET_TIME Usage

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

- NAME_RESOLVE Usage

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

- NAME_TOKENIZE Usage

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

- OLD_CURRENT_SCHEMA Usage

```sql
openGauss=# select gms_utility.old_current_schema();
 old_current_schema
--------------------
 public
(1 row)
```

- OLD_CURRENT_USER Usage

```sql
openGauss=# select gms_utility.old_current_user();
 old_current_user
------------------
 omm
(1 row)
```

- IS_BIT_SET Usage

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

- IS_CLUSTER_DATABASE Usage

```sql
openGauss=# select gms_utility.is_cluster_database();
 is_cluster_database
---------------------
 f
(1 row)
```

- TABLE_TO_COMMA Usage

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