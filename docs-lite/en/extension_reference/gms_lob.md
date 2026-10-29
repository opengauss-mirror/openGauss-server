# gms_lob

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:58.598Z pushedAt=2026-09-12T06:05:02.803Z -->

## gms_lob Overview

gms_lob is a plugin based on openGauss, providing users with read and write operations on LOB objects, including BLOB, CLOB, and BFILE. The currently supported interfaces are: GMS_LOB.GETLENGTH, GMS_LOB.READ, GMS_LOB.WRITE, GMS_LOB.APPEND, GMS_LOB.CREATETEMPORARY, GMS_LOB.FREETEMPORARY, GMS_LOB.OPEN, GMS_LOB.CLOSE, GMS_LOB.ISOPEN, GMS_LOB.BFILEOPEN, GMS_LOB.BFILECLOSE, GMS_LOB.BFILEREAD, GMS_LOB.FILEOPEN, and GMS_LOB.FILECLOSE.

## gms_lob Limitations

- Only the Create extension command is supported for loading the plugin.

## gms_lob Installation

### Prerequisites

openGauss has been installed.

### Loading gms_lob

Gms_lob is implemented as an extension to provide related interface support, so the extension must be loaded before use.
Command line: create extension gms_lob;

## gms_lob Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_lob extension, use the CREATE Extension command directly:

```
openGauss=# CREATE Extension gms_lob;
```

### Using Extension<a name="section107391050141118"></a>

#### **Package Constants**

| Constant        | Type             | Value                  | Description                                          |
| --------------- | ---------------- | ---------------------- | ---------------------------------------------------- |
| `CALL`          | `INTEGER`        | `12`                   | Creates a temporary LOB with a lifetime of the current call |
| `FILE_READONLY` | `BINARY_INTEGER` | `0`                    | Opens a BFILE in read-only mode                      |
| `LOB_READONLY`  | `BINARY_INTEGER` | `0`                    | Opens a LOB in read-only mode                        |
| `LOB_READWRITE` | `BINARY_INTEGER` | `1`                    | Opens a LOB in read-write mode                       |
| `LOBMAXSIZE`    | `NUMERIC`        | `18446744073709551615` | Maximum number of bytes for a LOB                    |
| `SESSION`       | `INTEGER`        | `10`                   | Creates a temporary LOB with a lifetime of the current session |

#### Function Declaration

##### **gms_lob.getlength**

getlength obtains the length of a LOB value.

```
GMS_LOB.GETLENGTH (
   lob_loc    IN  BLOB/CLOB) 
  RETURN INTEGER;
```

getlength obtains the size of the file associated with the BFILE object.

```
GMS_LOB.GETLENGTH (
   bfileobj     IN  bfile) 
  RETURN INTEGER;
```

##### **gms_lob.read**

Reads data from a LOB starting at a specified offset.

```
GMS_LOB.READ (
   lob_loc   IN             BLOB,
   amount    INOUT          INTEGER, // read length
   offset    IN             INTEGER, // offset
   buffer    OUT            RAW);

GMS_LOB.READ (
   lob_loc   IN             CLOB,
   amount    INOUT          INTEGER,
   offset    IN             INTEGER,
   buffer    OUT            VARCHAR2); 
```

Reads data from the file associated with the BFILE starting at a specified offset.

```
GMS_LOB.READ (
   bfileobj  IN             bfile,
   amount    INOUT          INTEGER, // Read length.
   offset    IN             INTEGER, // Offset.
   buffer    OUT            RAW);
```

##### **gms_lob.bfileread**

Reads data from the specified offset of a BFILE-associated file.

```
GMS_LOB.BFILEREAD (
   bfileobj  IN             bfile,
   amount    INOUT          INTEGER, // Read length.
   offset    IN             INTEGER) // Offset.
  RETURNS RAW;
```

##### **gms_lob.write**

Writes data to a specified offset in the LOB.

```
GMS_LOB.WRITE (
   lob_loc  INOUT          BLOB,
   amount   IN             NUMERIC,//Read length, rounded down.
   offset   IN             NUMERIC,//Offset, rounded down.
   buffer   IN             RAW);

GMS_LOB.WRITE (
   lob_loc  INOUT          CHARACTER,
   amount   IN             NUMERIC,
   offset   IN             NUMERIC,
   buffer   IN             VARCHAR2); 
```

##### **gms_lob.append**

Appends the content of the source LOB to the destination LOB.

```
GMS_LOB.APPEND (
   dest_lob INOUT          BLOB, 
   src_lob  IN             BLOB); 

GMS_LOB.APPEND (
   dest_lob INOUT          CLOB, 
   src_lob  IN             CLOB);
```

##### **gms_lob.createtemporary**

Creates a temporary BLOB or CLOB and its corresponding index in the user's default temporary tablespace, and sets lob_loc to a varlena pointer.

```
GMS_LOB.CREATETEMPORARY (
   lob_loc INOUT         BLOB/CLOB,
   cache   IN            BOOLEAN, --Specifies whether to read the LOB into the buffer (not effective).
   dur     IN            PLS_INTEGER := GMS_LOB.SESSION);--Specifies when to clear the temporary LOB (10/SESSION: at the end of the session; 12/CALL: at the end of the call) (not effective).
```

##### **gms_lob.freetemporary**

Releases a temporary BLOB or CLOB in the default temporary tablespace and sets lob_loc to a null varlena.

```
GMS_LOB.FREETEMPORARY (
   lob_loc  INOUT     BLOB/CLOB); 
```

##### **gms_lob.open**

The OPEN function is used to open a LOB in a specified mode. Valid modes include read-only and read/write.

```
GMS_LOB.OPEN (
   lob_loc   INOUT BLOB/CLOB,
   open_mode IN            BINARY_INTEGER); --Integer 0 / 1: read-only / read/write, corresponding to gms_lob.LOB_READONLY / gms_lob.LOB_READWRITE
```

##### **gms_lob.bfileopen**

The BFILEOPEN function is used to open a BFILE object association file in the specified mode. Currently, only file reading is allowed.

```
GMS_LOB.BFILEOPEN (
   bfileobj  IN             bfile,
   mode      IN             INTEGER) --Currently, the only valid value is 0 (read-only). Other values cause an error.
  RETURNS pg_catalog.bfile;
```

##### **gms_lob.fileopen**

The FILEOPEN stored procedure is used to open a BFILE object association file in the specified mode. Currently, only file reading is allowed.

```
GMS_LOB.FILEOPEN (
   bfileobj  INOUT          bfile,
   mode      IN             INTEGER); --Currently, the only valid value is 0 (read-only). Other values cause an error.
```

##### **gms_lob.close**

The CLOSE function is used for closing a previously opened LOB.

```
GMS_LOB.CLOSE (
    lob_loc    INOUT  BLOB/CLOB); 
```

##### **gms_lob.bfileclose**

The BFILECLOSE function is used for closing the file associated with the BFILE object.

```
GMS_LOB.BFILECLOSE (
   bfileobj  IN             bfile);
```

##### **gms_lob.fileclose**

The FILECLOSE stored procedure is used to close the file associated with a BFILE object.

```
GMS_LOB.FILEOPEN (
   bfileobj  IN             bfile);
```

##### **gms_lob.isopen**

The isopen function is used for determining whether a LOB is open.

```
GMS_LOB.ISOPEN (
   lob_loc IN BLOB/CLOB) 
  RETURN INTEGER; -- 1 is open/ 0 is close
```

#### Function Usage

Simultaneous use of functions in the GMS_LOB package

```sql
create table tbl_testlob(id int, c_lob clob, b_lob blob);
insert into tbl_testlob values(1, 'clob', cast_to_raw('blob'));
insert into tbl_testlob values(2, 'Chinese clobobject test', cast_to_raw('Chinese blobobject test'));
create or replace function func_clob() returns void 
AS $$
DECLARE
    v_clob1 clob;
    v_clob2 clob;
    v_clob3 clob;
    len1 int;
    len3 int;
BEGIN
    select c_lob into v_clob1 from tbl_testlob where id = 1;
    gms_lob.open(v_clob1, gms_lob.LOB_READWRITE);
    gms_lob.append(v_clob1, ' test');
    len1 := gms_lob.getlength(v_clob1);
    gms_output.put_line('clob2:' || v_clob2);
    gms_lob.read(v_clob1, len1, 1, v_clob2);
    gms_output.put_line('clob1:' || v_clob1);
    gms_output.put_line('clob2:' || v_clob2);

    select c_lob into v_clob3 from tbl_testlob where id = 2;
    len3 := gms_lob.getlength(v_clob3);

    gms_output.put_line('clob3:' || v_clob3);
    --The open function is not called. The default permission is read and write.
    gms_lob.write(v_clob3, len1, len3, v_clob1);
    gms_output.put_line('clob3:' || v_clob3);
    
    gms_lob.close(v_clob1);
    gms_lob.freetemporary(v_clob2);
END;
$$LANGUAGE plpgsql;
create or replace function func_blob() returns void 
AS $$
DECLARE
    v_blob1 blob;
    v_blob2 blob;
    v_blob3 blob;
    len1 int;
    len3 int;
BEGIN
    select b_lob into v_blob1 from tbl_testlob where id = 1;
    gms_lob.open(v_blob1, gms_lob.LOB_READWRITE);

    len1 := gms_lob.getlength(v_blob1);
    gms_output.put_line('blob1:' || v_blob1::text);
    gms_output.put_line('blob2:' || v_blob2::text);
    gms_lob.read(v_blob1, len1, 1, v_blob2);
    gms_output.put_line('blob1:' || v_blob1::text);
    gms_output.put_line('blob2:' || v_blob2::text);

    select b_lob into v_blob3 from tbl_testlob where id = 2;
    len3 := gms_lob.getlength(v_blob3);
    --Do not call the open function. The default permission is read and write.
    gms_output.put_line('blob3:' || v_blob3::text);
    gms_lob.write(v_blob3, len1, len3, v_blob1);
    gms_output.put_line('blob3:' || v_blob3::text);
    
    gms_lob.close(v_blob1);
    gms_lob.freetemporary(v_blob2);
END;
$$LANGUAGE plpgsql;
select func_clob();
clob2:
clob1:clob test
clob2:clob test
clob3:中文clobobject测试
clob3:中文clobobject测clob test
 func_clob
-----------
 
(1 row)

select func_blob();
blob1:626C6F62
blob2:
blob1:626C6F62
blob2:626C6F62
blob3:E4B8ADE69687626C6F626F626A656374E6B58BE8AF95
blob3:E4B8ADE69687626C6F626F626A656374E6B58BE8AF626C6F62
 func_blob 
-----------
 
(1 row)
```

Test the open/close/createtemporary/freetemporary/isopen functions.

```sql
--(1) Open an invalid LOB.
DECLARE
    v_clob clob;
BEGIN
    gms_lob.open(v_clob, gms_lob.LOB_READWRITE);
    
    gms_lob.close(v_clob);
END;
/
ERROR:  invalid LOB object specified
CONTEXT:  SQL statement "CALL gms_lob.open(v_clob,gms_lob.LOB_READWRITE)"
PL/pgSQL function inline_code_block line 3 at SQL statement
DECLARE
    v_clob clob;
BEGIN
    gms_lob.createtemporary(v_clob, false);
    gms_lob.open(v_clob, gms_lob.LOB_READWRITE);
    gms_output.put_line('isopen: ' || gms_lob.isopen(v_clob));
    gms_lob.close(v_clob);
    gms_output.put_line('isopen: ' || gms_lob.isopen(v_clob));
    gms_lob.freetemporary(v_clob);
END;
/
isopen: 1
isopen: 0

DECLARE
    v_clob CLOB;
    v_char VARCHAR2(100);
BEGIN
    v_char := 'Chinese people';
    gms_lob.createtemporary(v_clob,TRUE,12);
    gms_lob.append(v_clob,v_char);
    gms_output.put_line(v_clob||' character length:'||gms_lob.getlength(v_clob));
    gms_lob.freetemporary(v_clob);
    gms_output.put_line('Output after release: '||v_clob);
END;
/
Chinese中国人 字符长度：10
 释放后再输出：

```

Test read/write/append functions

```sql
declare
c1 clob :='abcdefgh';
amount INTEGER :=3;
off_set INTEGER :=1;
var_buf varchar2(10);
begin
gms_lob.read(c1, amount, off_set, var_buf);
gms_output.put_line('clob read: ' || var_buf::text);
end;
/
clob read: abc

declare
c1 clob :='11111111';
amount INTEGER :=3;
off_set INTEGER :=1;
c2 clob :='222';
begin
gms_lob.write(c1, amount, off_set, c2);
gms_output.put_line(c1::text);
end;
/
22211111

declare
c1 clob :='11111111';
c2 clob :='222';
begin
gms_lob.append(c1, c2);
gms_output.put_line(c1::text);
end;
/
11111111222
```

Test bfileopen/bfileclose/bfileread functions

```
create extension gms_lob;
create extension gms_output;
CREATE or REPLACE DIRECTORY bfile_test_dir AS '/tmp';
create table falt_bfile (id number, bfile_name bfile);
insert into falt_bfile values(1, bfilename('bfile_test_dir','regress_bfile.txt'));
copy (select * from falt_bfile) to '/tmp/regress_bfile.txt';
select gms_output.enable;
 enable 
--------
 
(1 row)

DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    my_bfile = gms_lob.bfileopen(my_bfile, 0);
    amount := gms_lob.getlength(my_bfile);
    buff = gms_lob.bfileread(my_bfile, amount, f_offset);
    gms_lob.bfileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/
1 bfilename('bfile_test_dir', 'regress_bfile.txt')
```

Test fileopen/fileclose/read functions

```
DECLARE
    buff raw(2000);
    my_bfile bfile;
    amount integer;
    f_offset integer := 1;
BEGIN
    my_bfile := bfilename('bfile_test_dir','regress_bfile.txt');
    gms_lob.fileopen(my_bfile, 0);
    amount := gms_lob.getlength(my_bfile);
    gms_lob.read(my_bfile, amount, f_offset, buff);
    gms_lob.fileclose(my_bfile);
    gms_output.put_line(CONVERT_FROM(decode(buff,'hex'), 'SQL_ASCII'));
END;
/
1 bfilename('bfile_test_dir', 'regress_bfile.txt')
```

### Deleting the Extension<a name="section1587441381220"></a>

The method for deleting the gms_output extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_lob [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.