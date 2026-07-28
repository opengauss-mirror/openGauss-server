# gms_lob

## gms_lob概述

gms_lob是一个基于openGauss的插件，为用户提供了对LOB对象，包括BLOB，CLOB和BFILE进行读写操作等功能。目前支持的接口有：GMS_LOB.GETLENGTH、GMS_LOB.READ、GMS_LOB.WRITE、GMS_LOB.APPEND、GMS_LOB.CREATETEMPORARY、GMS_LOB.FREETEMPORARY、GMS_LOB.OPEN、GMS_LOB.CLOSE、GMS_LOB.ISOPEN、GMS_LOB.BFILEOPEN、GMS_LOB.BFILECLOSE、GMS_LOB.BFILEREAD、GMS_LOB.FILEOPEN、GMS_LOB.FILECLOSE。

## gms_lob限制

- 仅支持Create extension命令方式加载插件。

## gms_lob安装

### 前提条件

已经安装openGauss

### 加载gms_lob

Gms_lob作为插件来实现的相关接口支持，所以使用前需要加载插件。
命令行: create extension gms_lob;

## gms_lob使用

### 创建Extension<a name="section21088306113"></a>

创建gms_lob Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_lob;
```

### 使用Extension<a name="section107391050141118"></a>

#### **包常量**

| Constant        | Type             | Value                  | Description                        |
| --------------- | ---------------- | ---------------------- | ---------------------------------- |
| `CALL`          | `INTEGER`        | `12`                   | 创建生命周期为当前调用的临时LOB    |
| `FILE_READONLY` | `BINARY_INTEGER` | `0`                    | 用只读方式打开BFILE                |
| `LOB_READONLY`  | `BINARY_INTEGER` | `0`                    | 用只读方式打开LOB                  |
| `LOB_READWRITE` | `BINARY_INTEGER` | `1`                    | 用读-写方式打开LOB                 |
| `LOBMAXSIZE`    | `NUMERIC`        | `18446744073709551615` | LOB的最大字节数                    |
| `SESSION`       | `INTEGER`        | `10`                   | 创建生命周期为当前session的临时LOB |

#### 函数声明

##### **gms_lob.getlength**

getlength获取LOB值的长度

```
GMS_LOB.GETLENGTH (
   lob_loc    IN  BLOB/CLOB) 
  RETURN INTEGER;
```

getlength获取bfile对象关联的文件大小

```
GMS_LOB.GETLENGTH (
   bfileobj     IN  bfile) 
  RETURN INTEGER;
```

##### **gms_lob.read**

从LOB中指定偏移量开始 读取数据

```
GMS_LOB.READ (
   lob_loc   IN             BLOB,
   amount    INOUT          INTEGER, // 读取长度
   offset    IN             INTEGER, // 偏移量
   buffer    OUT            RAW);

GMS_LOB.READ (
   lob_loc   IN             CLOB,
   amount    INOUT          INTEGER,
   offset    IN             INTEGER,
   buffer    OUT            VARCHAR2); 
```

从BFILE关联文件的指定偏移量开始 读取数据

```
GMS_LOB.READ (
   bfileobj  IN             bfile,
   amount    INOUT          INTEGER, // 读取长度
   offset    IN             INTEGER, // 偏移量
   buffer    OUT            RAW);
```

##### **gms_lob.bfileread**

从BFILE关联文件的指定偏移量开始 读取数据

```
GMS_LOB.BFILEREAD (
   bfileobj  IN             bfile,
   amount    INOUT          INTEGER, // 读取长度
   offset    IN             INTEGER) // 偏移量
  RETURNS RAW;
```

##### **gms_lob.write**

将数据写入LOB中指定的偏移量

```
GMS_LOB.WRITE (
   lob_loc  INOUT          BLOB,
   amount   IN             NUMERIC,//读取长度，向下取整
   offset   IN             NUMERIC,//偏移量，向下取整
   buffer   IN             RAW);

GMS_LOB.WRITE (
   lob_loc  INOUT          CHARACTER,
   amount   IN             NUMERIC,
   offset   IN             NUMERIC,
   buffer   IN             VARCHAR2); 
```

##### **gms_lob.append**

将源 LOB 的内容追加到目标 LOB

```
GMS_LOB.APPEND (
   dest_lob INOUT          BLOB, 
   src_lob  IN             BLOB); 

GMS_LOB.APPEND (
   dest_lob INOUT          CLOB, 
   src_lob  IN             CLOB);
```

##### **gms_lob.createtemporary**

在用户的默认临时表空间中创建临时 BLOB 或 CLOB 及其相应的索引，将lob_loc置为varlena指针

```
GMS_LOB.CREATETEMPORARY (
   lob_loc INOUT         BLOB/CLOB,
   cache   IN            BOOLEAN, --指定是否将LOB读取到缓冲区（不生效）
   dur     IN            PLS_INTEGER := GMS_LOB.SESSION);--指定何时清除临时LOB（10/ SESSION：会话结束时；12/ CALL：调用结束时）(不生效）
```

##### **gms_lob.freetemporary**

释放默认临时表空间中的临时 BLOB 或 CLOB，将lob_loc置为空值varlena

```
GMS_LOB.FREETEMPORARY (
   lob_loc  INOUT     BLOB/CLOB); 
```

##### **gms_lob.open**

OPEN函数用于以指定的模式打开LOB，有效模式包括只读和读/写。

```
GMS_LOB.OPEN (
   lob_loc   INOUT BLOB/CLOB,
   open_mode IN            BINARY_INTEGER); --取整0 / 1 只读/读写 gms_lob.LOB_READONLY/gms_lob.LOB_READWRITE
```

##### **gms_lob.bfileopen**

BFILEOPEN函数用于以指定的模式打开BFILE对象关联文件，目前只允许读文件。

```
GMS_LOB.BFILEOPEN (
   bfileobj  IN             bfile,
   mode      IN             INTEGER) --目前取值只有 0 只读，其他值报错。
  RETURNS pg_catalog.bfile;
```

##### **gms_lob.fileopen**

FILEOPEN存储过程用于以指定的模式打开BFILE对象关联文件，目前只允许读文件。

```
GMS_LOB.FILEOPEN (
   bfileobj  INOUT          bfile,
   mode      IN             INTEGER); --目前取值只有 0 只读，其他值报错。
```

##### **gms_lob.close**

CLOSE 函数用于关闭先前打开的 LOB。

```
GMS_LOB.CLOSE (
    lob_loc    INOUT  BLOB/CLOB); 
```

##### **gms_lob.bfileclose**

BFILECLOSE函数用于关闭BFILE对象关联文件。

```
GMS_LOB.BFILECLOSE (
   bfileobj  IN             bfile);
```

##### **gms_lob.fileclose**

FILECLOSE存储过程用于关闭BFILE对象关联文件。

```
GMS_LOB.FILEOPEN (
   bfileobj  IN             bfile);
```

##### **gms_lob.isopen**

isopen函数用于判断LOB是否打开

```
GMS_LOB.ISOPEN (
   lob_loc IN BLOB/CLOB) 
  RETURN INTEGER; -- 1 is open/ 0 is close
```

#### 函数使用

Gms_lob包中函数同时使用

```sql
create table tbl_testlob(id int, c_lob clob, b_lob blob);
insert into tbl_testlob values(1, 'clob', cast_to_raw('blob'));
insert into tbl_testlob values(2, '中文clobobject测试', cast_to_raw('中文blobobject测试'));
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
    --不调用open函数。默认权限为读写
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
    --不调用open函数。默认权限为读写
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

测试Open/close/createtemporary/freetemporary/isopen函数

```sql
--（1）打开无效的lob
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
    v_char := 'Chinese中国人';
    gms_lob.createtemporary(v_clob,TRUE,12);
    gms_lob.append(v_clob,v_char);
    gms_output.put_line(v_clob||' 字符长度：'||gms_lob.getlength(v_clob));
    gms_lob.freetemporary(v_clob);
    gms_output.put_line(' 释放后再输出：'||v_clob);
END;
/
Chinese中国人 字符长度：10
 释放后再输出：

```

测试read/write/append函数

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

测试bfileopen/bfileclose/bfileread函数

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

测试fileopen/fileclose/read函数

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

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_output Extension的方法如下所示：

```
openGauss=# DROP Extension gms_lob [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
