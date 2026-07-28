# gms_compress

## gms_compress概述

gms_compress是一个基于openGauss的插件，为用户提供了将文本行写入内存、供以后提取和显示的功能。目前支持的接口有：GMS_COMPRESS.LZ_COMPRESS_OPEN、GMS_COMPRESS.LZ_COMPRESS_ADD、GMS_COMPRESS.LZ_COMPRESS_CLOSE、GMS_COMPRESS.LZ_UNCOMPRESS_OPEN、GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT、GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE、GMS_COMPRESS.ISOPEN、GMS_COMPRESS.LZ_COMPRESS、GMS_COMPRESS.LZ_UNCOMPRESS。

zlib库接口使用版本为1.2.8，对应的压缩使用的资源、性能参考对应zlib版本的标准。

## gms_compress限制

- 仅支持`create extension`命令方式加载插件。
- 仅支持最大使用5个（未）压缩句柄。
- 仅支持压缩row和blob类型。
- 每个句柄存储的数据不能超过1GB。

## gms_compress安装

openGauss打包编译时默认已经包含了gms_compress，可以在安装完openGauss后，直接通过`create extension gms_compress;`加载插件。

## gms_compress使用

### 创建Extension<a name="section21088305113"></a>

创建gms_compress Extension可直接使用`create extension`命令进行创建：

```
openGauss=# create extension gms_compress;
```

### 使用Extension<a name="section107391010141118"></a>

#### 函数声明

- LZ_COMPRESS_OPEN (
   dst       IN OUT BLOB, 
   quality   IN INTEGER DEFAULT 6) 
 RETURN INTEGER;

  描述：此函数初始化一个分段上下文，用于维护压缩状态和数据。
  
  参数详解：dst为出入参，传入用于存储压缩的数据，quality为入参，定义压缩级别，取值范围为[1~9]，默认值为6，返回句柄。
- LZ_COMPRESS_ADD (
   handle IN             INTEGER, 
   dst    IN OUT   BLOB, 
   src    IN             RAW); 
  
  描述：此过程为对应句柄添加一段压缩数据。
  
  参数详解：dst入参在当前openGauss中无意义，仅作语法兼容，src为入参，待压缩的原始数据，handle为通过LZ_COMPRESS_OPEN打开的句柄。
- LZ_COMPRESS_CLOSE (
   handle IN             INTEGER, 
   dst    OUT  BLOB); 
  
  描述：此过程关闭并完成分段压缩操作。
  
  参数详解：dst为出参，传入用于存放压缩数据的地址，handle为通过LZ_COMPRESS_OPEN打开的句柄。
- LZ_UNCOMPRESS_OPEN(
   src  IN  BLOB)
  RETURN INTEGER;
  
  描述：此函数初始化一个分段上下文，用于维护解压缩状态和数据。
  
  参数详解：src为入参，传入已经压缩的数据，待解压，返回句柄。
- LZ_UNCOMPRESS_EXTRACT(
   handle  IN          INTEGER, 
   dst     OUT   RAW); 
  
  描述：此过程提取出句柄中所有解压后的数据。
  
  参数详解：dst为出参，传入用于存储解压后的数据地址，handle为通过LZ_UNCOMPRESS_OPEN打开的句柄。
- LZ_UNCOMPRESS_CLOSE(
   handle  IN   INTEGER); 
  
  描述：此过程关闭并完成分段解压缩。
  
  参数详解：handle为通过LZ_UNCOMPRESS_OPEN打开的句柄。
- ISOPEN(
   handle in INTEGER) 
 RETURN BOOLEAN;
  
  描述：此函数检查分段（未）压缩上下文的句柄是打开还是关闭。
  
  参数详解：handle为通过LZ_COMPRESS_OPEN或LZ_UNCOMPRESS_OPEN打开的句柄，打开返回true，否则返回false。
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
  
  描述：这些函数和过程使用Lempel-Ziv压缩算法压缩数据。
  
  参数详解：对于函数，压缩后的数据作为返回值返回，对于存储过程，dst为出入参，传入用于存储压缩后的数据地址，quality为入参，定义压缩级别，取值范围为[1~9]，默认值为6。
- LZ_UNCOMPRESS(
   src  IN  RAW)
  RETURN RAW;
  
  LZ_UNCOMPRESS(
   src  IN  BLOB)
  RETURN BLOB;
  
  LZ_UNCOMPRESS(
   src  IN  BLOB,
   dst  IN OUT  BLOB); 
  
  描述：这些函数和过程接受RAW、BLOB压缩字符串作为输入，验证其是否为有效的压缩值，使用Lempel-Ziv压缩算法对其进行解压缩，并返回未压缩的RAW或BLOB结果。
  
  参数详解：对于函数，解压后的数据作为返回值返回，对于存储过程，dst为出入参，传入用于存储解压后的数据地址。

#### 函数使用

[!NOTE]说明
    由于压缩结果受zlib接口影响，可能由于zlib版本差异等原因，导致压缩结果不完全一致

测试lz_compress、lz_uncompress函数

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

测试存储过程

```sql
openGauss=# DECLARE
openGauss$#  content BLOB;
openGauss$#  v_handle int;
openGauss$#  src raw;
openGauss$# BEGIN
openGauss$# content := '123';
openGauss$#    v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
openGauss$#    src := '123';
openGauss$#    GMS_COMPRESS.LZ_COMPRESS_ADD(v_handle,content,src);
openGauss$#    GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
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
openGauss$#   content := '123';
openGauss$#  content := GMS_COMPRESS.LZ_COMPRESS(content);
openGauss$#    v_handle := GMS_COMPRESS.LZ_UNCOMPRESS_OPEN(content);
openGauss$#  GMS_COMPRESS.LZ_UNCOMPRESS_EXTRACT(v_handle, v_raw);
openGauss$#    GMS_COMPRESS.LZ_UNCOMPRESS_CLOSE(v_handle);
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
openGauss$#   content := '123';
openGauss$#   v_bool := false;
openGauss$#    v_handle := GMS_COMPRESS.LZ_COMPRESS_OPEN(content);
openGauss$#   v_bool := GMS_COMPRESS.ISOPEN(v_handle);
openGauss$#   RAISE NOTICE 'v_bool=%', v_bool;
openGauss$#    GMS_COMPRESS.LZ_COMPRESS_CLOSE(v_handle,content);
openGauss$#   v_bool := GMS_COMPRESS.ISOPEN(v_handle);
openGauss$#   RAISE NOTICE 'v_bool=%', v_bool;
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
NOTICE:  v_bool=t
NOTICE:  v_bool=f
```

### 删除Extension<a name="section1587444381220"></a>

在openGauss中删除gms_compress Extension的方法如下所示：

```
openGauss=# DROP extension gms_compress [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
