# gms_compress

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:04.722Z pushedAt=2026-09-12T05:51:25.850Z -->

## gms_compress Overview

gms_compress is a plugin based on openGauss that provides users with the capability to write text lines into memory for later retrieval and display. The currently supported interfaces are: GMS_COMPRESS.LZ_COMPRESS_OPEN, GMS_COMPRESS.LZ_COMPRESS_ADD, GMS_COMPRESS.LZ_COMPRESS_CLOSE, GMS_COMPRESS.LZ_UNCOMPRESS_OPEN, GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT, GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE, GMS_COMPRESS.ISOPEN, GMS_COMPRESS.LZ_COMPRESS, and GMS_COMPRESS.LZ_UNCOMPRESS.

The zlib library interface version used is 1.2.8, and the corresponding compression resources and performance should be referenced against the standards of that zlib version.

## gms_compress Limitations

- Only supports loading the extension via the `create extension` command.
- Only supports using a maximum of 5 compression/decompression handles.
- Only supports compression of row and blob types.
- The data stored per handle cannot exceed 1 GB.

## gms_compress Installation

gms_compress is included by default when openGauss is packaged and compiled. After openGauss is installed, you can load the extension directly by running <code>create extension gms_compress;</code>.

## gms_compress Usage

### Creating an Extension<a name="section21088305113"></a>

To create the gms_compress extension, you can directly use the <code>create extension</code> command:

```
openGauss=# create extension gms_compress;
```

### Using Extension<a name="section107391010141118"></a>

#### Function Declaration

- LZ_COMPRESS_OPEN (
   dst       IN OUT BLOB,
   quality   IN INTEGER DEFAULT 6)
 RETURN INTEGER;

  Description: This function initializes a segmented context for maintaining compression state and data.

  Parameter Details: dst is an input/output parameter, used to pass in data for storing compression; quality is an input parameter, defining the compression level with a value range of [1, 9] and a default value of 6. The function returns a handle.
- LZ_COMPRESS_ADD (
   handle IN             INTEGER, 
   dst    IN OUT   BLOB, 
   src    IN             RAW); 

  Description: This procedure adds a segment of compressed data to the corresponding handle.

  Parameter Details: dst is an Input/Output Parameter that has no meaning in the current openGauss and is provided only for syntax compatibility; src is an Input Parameter for the original data to be compressed; handle is the handle opened through LZ_COMPRESS_OPEN.
- LZ_COMPRESS_CLOSE (
   handle IN             INTEGER, 
   dst    OUT  BLOB); 

  Description: This procedure closes and completes the piecewise compression operation.

  Parameter Details: dst is an output parameter, which receives the address for storing compressed data; handle is the handle opened via LZ_COMPRESS_OPEN.
- LZ_UNCOMPRESS_OPEN(
   src  IN  BLOB)
  RETURN INTEGER;

  Description: This function initializes a segment context for maintaining decompression state and data.

  Parameter Details: src is an input parameter, which passes in the already compressed data to be decompressed, and returns a handle.
- LZ_UNCOMPRESS_EXTRACT(
   handle  IN          INTEGER, 
   dst     OUT   RAW); 

  Description: This procedure extracts all decompressed data from the handle.

  Parameter Details: dst is an output parameter, passing in the address for storing decompressed data; handle is the handle opened via LZ_UNCOMPRESS_OPEN.
- LZ_UNCOMPRESS_CLOSE(
   handle  IN   INTEGER); 

  Description: This procedure closes and completes segmented decompression.

  Parameter Details: handle is the handle opened via LZ_UNCOMPRESS_OPEN.
- ISOPEN(
   handle in INTEGER) 
 RETURN BOOLEAN;

  Description: This function checks whether the handle of the segmented compression/decompression context is open or closed.

  Parameter Details: handle is the handle opened via LZ_COMPRESS_OPEN or LZ_UNCOMPRESS_OPEN. Returns true if open, otherwise returns false.
- LZ_COMPRESS (
  src       IN           BLOB,
  quality   IN           INTEGER DEFAULT 6) 
 RETURN BLOB;

  LZ_COMPRESS (
   src       IN           RAW,
   quality   IN           INTEGER DEFAULT 6) 
 RETURN RAW;

  LZ_COMPRESS (
  src      IN            BLOB, 
  dst      IN OUT  BLOB, 
  quality  IN            INTEGER DEFAULT 6);

  Description: These functions and procedures compress data using the Lempel-Ziv compression algorithm.

  Parameter Details: For functions, the compressed data is returned as the return value. For stored procedures, dst is an Input/Output Parameter, which passes in the address for storing the compressed data. quality is an input parameter that defines the compression level, with a value range of [1, 9] and a default value of 6.
- LZ_UNCOMPRESS(
   src  IN  RAW)
  RETURN RAW;

  LZ_UNCOMPRESS(
   src  IN  BLOB)
  RETURN BLOB;

  LZ_UNCOMPRESS(
   src  IN  BLOB,
   dst  IN OUT  BLOB); 

  Description: These functions and procedures accept a RAW or BLOB compressed string as input, verify whether it is a valid compressed value, decompress it using the Lempel-Ziv compression algorithm, and return the uncompressed RAW or BLOB result.

  Parameter Details: For functions, the decompressed data is returned as the return value. For stored procedures, dst is an input/output parameter that receives the address for storing the decompressed data.

#### Function Usage

[!NOTE] Note
    Because the compression result is affected by the zlib interface, the compression results may not be completely consistent due to reasons such as zlib version differences.

Test the lz_compress and lz_uncompress functions

```sql
openGauss=# create schema gms_compress_test;
CREATE SCHEMA
openGauss=# set search_path=gms_compress_test;
SET
openGauss=# select GMS_COMPRESS.LZ_COMPRESS('123'::raw);
                 lz_compress                  
----------------------------------------------
 1F8B080000000000000363540600CC52A5FA02000000
(1 row)

openGauss=# select GMS_COMPRESS.LZ_UNCOMPRESS(GMS_COMPRESS.LZ_COMPRESS('123'::raw));
 lz_uncompress 
---------------
 0123
(1 row)
```

Test the stored procedure

```sql
openGauss=# DECLARE
openGauss$#  content BLOB;
openGauss$#  v_handle int;
openGauss$#  src raw;
openGauss$# BEGIN
openGauss$# content := '123';
openGauss$# v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
openGauss$# src := '123';
openGauss$# GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,src);
openGauss$# GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
openGauss$#  RAISE NOTICE 'content=%', content;
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
NOTICE:  content=1F8B080000000000000363540600CC52A5FA02000000
```

```sql
openGauss=# DECLARE
openGauss$#  content BLOB;
openGauss$#  v_handle int;
openGauss$#  v_raw raw;
openGauss$# BEGIN
openGauss$# content := '123';
openGauss$#  content := GMS_COMPRESS.LZ_COMPRESS(content);
openGauss$# v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
openGauss$#  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
openGauss$# GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
openGauss$#  RAISE NOTICE 'content=%', content;
openGauss$#  RAISE NOTICE 'v_raw=%', v_raw;
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
NOTICE:  content=1F8B080000000000000363540600CC52A5FA02000000
NOTICE:  v_raw=0123
```

```sql
openGauss=# DECLARE
openGauss$#   content BLOB;
openGauss$#   v_handle int;
openGauss$#   v_bool boolean;
openGauss$# BEGIN
openGauss$#  content := '123';
openGauss$#   v_bool := false;
openGauss$#  v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
openGauss$#   v_bool := GMS_COMPRESS.ISOPEN(v_handle);
openGauss$#   RAISE NOTICE 'v_bool=%', v_bool;
openGauss$#  GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
openGauss$#   v_bool := GMS_COMPRESS.ISOPEN(v_handle);
openGauss$#   RAISE NOTICE 'v_bool=%', v_bool;
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
NOTICE:  v_bool=t
NOTICE:  v_bool=f
```

### Deleting the Extension<a name="section1587444381220"></a>

The method for deleting the gms_compress extension in openGauss is as follows:

```
openGauss=# DROP extension gms_compress [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, the CASCADE keyword must be added to drop all dependent objects.