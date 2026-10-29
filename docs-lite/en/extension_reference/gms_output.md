# gms_output Overview

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:53.261Z pushedAt=2026-09-12T06:21:55.956Z -->

## gms_output Overview

gms_output is an openGauss-based plugin that provides users with the capability to write text lines into memory for later retrieval and display. The currently supported interfaces are: GMS_OUTPUT.ENABLE, GMS_OUTPUT.GET_LINE, GMS_OUTPUT.GET_LINES, GMS_OUTPUT.NEW_LINE, GMS_OUTPUT.PUT, GMS_OUTPUT.PUT_LINE, and GMS_OUTPUT.DISABLE.

## gms_output Limitations

- Only the CREATE EXTENSION command is supported for loading the plugin.
- gms_output is only supported in mode A.

## gms_output Installation

openGauss already includes gms_output by default during packaging and compilation. After installing openGauss, you can directly load the extension by running create extension gms_output;.

## gms_output Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_output extension, you can directly use the CREATE Extension command:

```
openGauss=# CREATE Extension gms_output;
```

### Using Extension<a name="section107391050141118"></a>

#### Function Declarations

- ENABLE(buffer_size IN INTEGER DEFAULT 20000)
  Description: Pre-allocates space. Before using GMS_OUTPUT, GMS_OUTPUT.ENABLE must be executed.
  Parameter Details: Sets the buffer_size for pre-allocated space. The maximum value is 1000000, the minimum value is 2000, and the default value is 20000, in bytes.
- DISABLE()
  Description: Destroys the allocated space.
- GET_LINE(line INOUT text, status INOUT INTEGER)
  Description: This function retrieves a line array from the buffer and reads one line of information, where the end-of-line marker is distinguished by '\0'.
  Parameter Details: line: used to receive the returned line information; status: if retrieval is successful, this parameter returns 0; otherwise, it returns 1.
- GET_LINES(lines INOUT text[], numlines INOUT INTEGER)
  Description: This function retrieves a line array from the buffer and reads the specified number of lines of text information.
  Parameter Details: lines: used to receive the returned line information; numlines: indicates the actual number of lines of text data retrieved.
  > Note: After GMS_OUTPUT.GET_LINE and GMS_OUTPUT.GET_LINES, the buffer is cleared and the retrieved data is empty.
- PUT(item IN VARCHAR2)
  Description: This function outputs a partial line to the buffer.
  Parameter Details: item: indicates the content to be written to the buffer.
- PUT_LINE(item IN VARCHAR2)
  Description: This function outputs a line of information to the buffer.
  Parameter Details: item: indicates the content to be written to the buffer.
- NEW_LINE()
  Description: Places a line terminator into the line buffer.

#### Function Usage

Test the enable and disable functions.

```sql
openGauss=# create schema gms_output_test;
CREATE SCHEMA
openGauss=# set search_path=gms_output_test;
SET
openGauss=# select gms_output.disable;
 disable 
---------
 
(1 row)

openGauss=# select gms_output.enable(20000);
 enable 
--------
 
(1 row)

```

Test the get_line and get_lines functions.

```sql
openGauss=# begin
openGauss$#   gms_output.enable;
openGauss$#   gms_output.put_line('This ');
openGauss$# end;
openGauss$# /
This
ANONYMOUS BLOCK EXECUTE
openGauss=# select gms_output.get_line(0,1);
  get_line
-------------
 ("This ",0)
(1 row)

openGauss=# begin
openGauss$#  gms_output.enable(100);
openGauss$#  gms_output.PUT_LINE('{131231321312313},{dhsfsdjfsdf}');
openGauss$#  gms_output.PUT_LINE('{Good or random text},{dhsfsdjfsdf}');
openGauss$# end;
openGauss$# /
WARNING:  Limit increased to 2000 bytes.
CONTEXT:  SQL statement "CALL gms_output.enable(100)"
PL/pgSQL function inline_code_block line 2 at PERFORM
{131231321312313},{dhsfsdjfsdf}
{好还是打发士大夫},{dhsfsdjfsdf}
ANONYMOUS BLOCK EXECUTE
openGauss=# select  gms_output.get_lines('{lines}',3);
                                    get_lines
----------------------------------------------------------------------------------
 ("{""{131231321312313},{dhsfsdjfsdf}"",""{Good or random text},{dhsfsdjfsdf}""}",2)
(1 row)

```

Test put and put_line functions

```sql
openGauss=# begin
openGauss$#  gms_output.enable(4000);
openGauss$#  gms_output.put('123');
openGauss$#  gms_output.put_line('YYY');
openGauss$# end;
openGauss$# /
123
YYY
ANONYMOUS BLOCK EXECUTE
```

Test new_line function

```sql
openGauss=# begin
openGauss$#  gms_output.put('44');
openGauss$#  gms_output.new_line();
openGauss$# end;
openGauss$# /
44
ANONYMOUS BLOCK EXECUTE
```

### Deleting the Extension<a name="section1587441381220"></a>

The method for deleting the gms_output extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_output [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.