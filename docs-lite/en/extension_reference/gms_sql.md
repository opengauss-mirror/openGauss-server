# gms_sql

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:26:19.435Z pushedAt=2026-09-24T10:58:21.516Z -->

## gms_sql Overview

gms_sql is an openGauss-based plugin used for executing dynamic SQL, supporting DDL, DML, and other operations.

Currently supported interfaces include:
OPEN_CURSOR -- Opens a cursor and returns the cursor ID.
CLOSE_CURSOR -- Closes a cursor.
COLUMN_VALUE -- Saves a specified column from the fetch result into a return parameter.
DEFINE_COLUMN -- Defines the column structure in the returned result.
EXECUTE         -- Executes the SQL statement processed by the PARSE function.
FETCH_ROWS -- Loads a row from the result set.
PARSE         -- Parses the syntax of the current statement.
RETURN_RESULT -- Returns the result of an executed statement to the client app.
BIND_ARRAY -- This stored procedure binds a table variable based on the input dynamic SQL.
BIND_VARIABLE -- This stored procedure binds a variable based on the input dynamic SQL.
DESCRIBE_COLUMNS, DESCRIBE_COLUMNS2, DESCRIBE_COLUMNS3 -- These stored procedures output column metadata information.
DEFINE_ARRAY -- Defines a collection to be selected from a given cursor, used only for SELECT statements.
IS_OPEN         -- This function determines whether a cursor is open.
FETCH_ROWS -- Executes dynamic SQL and returns a result dataset.
DEFINE_COLUMN   -- Defines the variable name returned by dynamic SQL and the value of the returned column through the COLUMN_VALUE stored procedure.

## gms_sql Restrictions

- Only the Create extension command is supported for loading the plugin.

## gms_sql Installation

gms_sql is included by default during openGauss packaging and compilation. After openGauss is installed, the extension can be loaded directly by using create extension gms_sql;.

## gms_sql Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_sql extension, use the CREATE Extension command directly:

```
openGauss=# CREATE Extension gms_sql;
```

### Using Extension<a name="section107391050141118"></a>

#### Corresponding Interfaces

open_cursor Function
Interface: open_cursor() RETURNS int
Function: Checks the cursor ID array, identifies an unused cursor ID, allocates a memory context for the cursor, and creates it. Currently, a maximum of 100 cursors can be opened.
Return value:
Cursor ID.

is_open Function
Interface:
is_open(c int) RETURNS bool
Function: Checks whether the corresponding cursor has been allocated.
Parameters: c: cursor ID
Return value: Whether the cursor has been allocated

parse Stored Procedure
Interface: parse(c int, stmt varchar2, ver int)
Function: Parses the statement to be executed.
Parameters:
c: cursor ID
stmt: dynamic SQL statement
ver: corresponding Oracle version (i.e., gms_sql.native, gms_sql.v6, gms_sql.v7 (has no practical meaning and serves only for compatibility))

bind_variable Stored Procedure
Interface: bind_variable(c int, name varchar2, value "any")
Function: Sets the value and type of a bound variable.
Parameters:
c: cursor ID
name: bound variable name
value: variable that passes in the value

bind_array stored procedure
Interface: bind_array(c int, name varchar2, value anyarray)
Function: Sets the value and type of a collection-type bound variable.
Parameters
c: cursor ID
name: bound variable name
value: variable that holds a set of values

define_column stored procedure
Interface: define_column(c int, col int, value "any", column_size int DEFAULT -1)
Function: Defines the type and length of a return result column.
Parameters
c: cursor ID
col: return result column index
value: variable name with the same type as the column
column_size: maximum length of the column

execute function
Interface: execute(c int) RETURNS bigint
Function: Passes the previously bound variable values through the SPI interface and executes the query statement.
Parameters
c: cursor ID
Return value: number of result rows obtained.

fetch_rows function
Interface: fetch_rows(c int) RETURNS int
Function: Fetches query results based on the cursor through the SPI interface.
Parameters
c: cursor ID
Return value: number of result rows read.

execute_and_fetch function
Interface:
execute_and_fetch(c int, exact bool DEFAULT false) RETURNS int
Function: Executes execute and fetch_rows.

last_row_count Function
Interface: last_row_count() RETURNS int
Function: Returns the number of rows fetched.

column_value Stored Procedure
Interface: column_value(c int, pos int, INOUT value anyelement)
Function: Saves the value of a column in the query result to a variable.
Parameters
c: cursor ID
pos: column index
value: variable to store the value

return_result Stored Procedure
Interface:
First format:
return_result(c refcursor, to_client bool DEFAULT false)
The cursor input value is refcursor.
Second format:
return_result(c int, to_client bool DEFAULT false)
The cursor input value is a cursor previously opened via open_cursor.
Function: Returns the query result of the dynamic SQL to the client.
Parameters
c: cursor
to_client: Whether to return to the client. Not yet used.

describe_columns Stored Procedure
Interface: describe_columns(c int, INOUT col_cnt int, INOUT desc_t
 gms_sql.desc_rec[])
Function: Obtains column-related information of the query result.
Parameters
c: cursor ID
col_cnt: number of columns returned.
desc_t: column description. Information of each column is stored in the previously defined desc_rec type. The input type is desc_tab.

describe_columns2 Stored Procedure
Interface: describe_columns2(c int, INOUT col_cnt int, INOUT desc_t 
gms_sql.desc_rec2[])
Function: Same as describe_columns.
Parameters
c: cursor ID
col_cnt: number of columns returned.
desc_t: column description. The input type is desc_tab2.

describe_columns3 stored procedure
Interface: describe_columns3(c int, INOUT col_cnt int, INOUT desc_t 
gms_sql.desc_rec3[])
describe_columns3(c int, INOUT col_cnt int, INOUT desc_t 

gms_sql.desc_rec4[])
Function: Same as describe_columns.
Parameters
c: cursor ID
col_cnt is the returned number of columns.
desc_t is the column description, and the input type can be desc_tab3 or desc_tab4.

close_cursor stored procedure
Interface: close_cursor(c int)
Function: Checks whether the cursor is open. If open, closes the cursor and the corresponding portal, and releases the memory occupied by the cursor.
Parameters
c: cursor ID

debug_cursor stored procedure 
Interface: debug_cursor(c int)
Function: Outputs cursor-related information, including the SQL statement, bound variable names, variable types, and defined column information.
Parameters
c: cursor ID

#### Simple execution flow of GMS_SQL is as follows:

1. OPEN_CURSOR

2. PARSE

3. BIND_VARIABLE

4. DEFINE_COLUMN or DEFINE_ARRAY

5. EXECUTE

6. FETCH_ROWS or EXECUTE_AND_FETCH

7. COLUMN_VALUE

8. CLOSE_CURSOR

#### Example

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

### Deleting the Extension<a name="section1587441381220"></a>

The method for deleting the gms_sql extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_sql [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, the CASCADE keyword must be added to delete all dependent objects.