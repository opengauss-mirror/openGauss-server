[TOC]



## 使用案例帮助文档

### 帮助信息

```
Usage: gs_filedump [-abcdfhikuxy] [-r relfilenode] [-T reltoastrelid] [-R startblock [endblock]] [-D attrlist] [-S blocksize] [-s segsize] [-n segnumber] file
```

`-h` 显示帮助

`-abdfkxyv` 调试时使用，一般情况用不到 

`-s` 和 `-S` 强制块，段大小，一般情况用不到

`-i`  显示解析条目的详细信息 

`-o` 不转储已删除的条目信息

`-c` 用于解析控制文件

`-m` 用于解析映射文件 `pg_filenode.map`

`-u` 用于解析`ustore`存储引擎的表（不加`-u`参数默认解析`astore`存储引擎的表）

`-t` 转储Toast文件（支持`astore` 和`ustore`存储引擎）

`-R` 指定要解析的block范围，(以0开始) 

`-D` 解析的表的列类型，以逗号分割，支持的类型

```
Supported types:
        bigint bigserial bool char charN date float float4 float8 int
        json macaddr name numeric oid real serial smallint smallserial text
        time timestamp timestamptz timetz uuid varchar varcharN xid xml
      ~ ignores all attributes left in a tuple
```

**段页式存储表解析参数：**

当指定解析段页式存储的表时，文件必须指定为 **[文件目录/1]**

`-r` 指定要解析的表的`[relfilenode]`

`-T` 指定要解析的表对应的pg_toast表的`[relfilenode]`，(解析段页式存储表时，不再支持`-t`参数)



### 几个使用案例

#### astore存储引擎

##### 查看表结构，表类型，表数据

```sql
-- 建表，造数
openGauss=# create table tbl_001(id int, tmsp timestamp, text text, primary key(id));
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "tbl_001_pkey" for table "tbl_001"
CREATE TABLE

openGauss=# insert into tbl_001
select generate_series as id,
(current_timestamp - POWER(random() * 100, (random() * 10 % 3)))::TIMESTAMP as timestamp,
md5(random()) as text
from  generate_series(1,5);
INSERT 0 5

openGauss=# \d+ tbl_001;
                                  Table "public.tbl_001"
 Column |            Type             | Modifiers | Storage  | Stats target | Description 
--------+-----------------------------+-----------+----------+--------------+-------------
 id     | integer                     | not null  | plain    |              | 
 tmsp   | timestamp without time zone |           | plain    |              | 
 text   | text                        |           | extended |              | 
Indexes:
    "tbl_001_pkey" PRIMARY KEY, btree (id) TABLESPACE pg_default
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# select * from tbl_001;
 id |            tmsp            |               text               
----+----------------------------+----------------------------------
  1 | 2014-09-08 08:12:03.888841 | 8897f71deb93ec6299f6742294b0de5d
  2 | 2001-06-29 11:12:43.078154 | d475a36d25c3a6056679dd3a902f3a7a
  3 | 2007-09-03 16:12:38.970582 | 7a2cd1d21dd2ad17b68ccad762bd0836
  4 | 2025-07-31 09:44:47.962485 | 6e2d5eb3ae7bbdba1291e0dbe9b5f990
  5 | 2025-06-21 19:48:45.16557  | 0d8f76f7a4e22024b5d98d93c4b062ab
(5 rows)

-- 删除一条数据
openGauss=# delete from tbl_001 where id=3;
DELETE 1
openGauss=# select * from tbl_001;
 id |            tmsp            |               text               
----+----------------------------+----------------------------------
  1 | 2014-09-08 08:12:03.888841 | 8897f71deb93ec6299f6742294b0de5d
  2 | 2001-06-29 11:12:43.078154 | d475a36d25c3a6056679dd3a902f3a7a
  4 | 2025-07-31 09:44:47.962485 | 6e2d5eb3ae7bbdba1291e0dbe9b5f990
  5 | 2025-06-21 19:48:45.16557  | 0d8f76f7a4e22024b5d98d93c4b062ab
(4 rows)

openGauss=# vacuum tbl_001;
VACUUM
```



##### 查看表的`relfilenode`

```sql
openGauss=# select oid, relname, relfilenode, reltoastrelid from pg_class where relname='tbl_001';
  oid   | relname | relfilenode | reltoastrelid 
--------+---------+-------------+---------------
 106577 | tbl_001 |      106577 |        106580
(1 row)

openGauss=# select pg_relation_filepath('tbl_001');
 pg_relation_filepath 
----------------------
 base/15938/106577
(1 row)

-- $PGDATA/base/15938/106577 即为存储表tbl_001的物理文件
```

##### 使用`gs_filedump`转储表数据

**不加`-o`参数，默认会显示已删除的条目**

```bash
[omm@cmnode1 gs_filedump]$ gs_filedump -D int,timestamp,text $PGDATA/base/15938/106577

*******************************************************************
* PostgreSQL File/Block Formatted Dump Utility
*
* File: /app/opengauss/cluster/dn1/base/15938/106577
* Options used: -D int,timestamp,text
*******************************************************************

Block    0 ********************************************************
<Header> -----
 Block Offset: 0x00000000         Offsets: Lower      60 (0x003c)
 Block: Size 8192  Version    6            Upper    7792 (0x1e70)
 LSN:  logid      0 recoff 0x03120268      Special  8192 (0x2000)
 Items:    5                      Free Space: 7732
 Checksum: 0x9710  Prune XID: 0x00021865  Flags: 0x0040 (CHECKSUM_FNV1A)
 Length (including item array): 44

<Data> -----
 Item   1 -- Length:   73  Offset: 8112 (0x1fb0)  Flags: NORMAL
COPY: 1 2014-09-08 08:12:03.888841      8897f71deb93ec6299f6742294b0de5d
 Item   2 -- Length:   73  Offset: 8032 (0x1f60)  Flags: NORMAL
COPY: 2 2001-06-29 11:12:43.078154      d475a36d25c3a6056679dd3a902f3a7a
 Item   3 -- Length:   73  Offset: 7952 (0x1f10)  Flags: NORMAL
COPY: 3 2007-09-03 16:12:38.970582      7a2cd1d21dd2ad17b68ccad762bd0836
 Item   4 -- Length:   73  Offset: 7872 (0x1ec0)  Flags: NORMAL
COPY: 4 2025-07-31 09:44:47.962485      6e2d5eb3ae7bbdba1291e0dbe9b5f990
 Item   5 -- Length:   73  Offset: 7792 (0x1e70)  Flags: NORMAL
COPY: 5 2025-06-21 19:48:45.16557       0d8f76f7a4e22024b5d98d93c4b062ab


*** End of File Encountered. Last Block Read: 0 ***
```



**加上`-o`参数，将不显示已删除的条目**

```bash
[omm@cmnode1 ~]$ ./gs_filedump -D int,timestamp,text $PGDATA/base/15938/106577

*******************************************************************
* PostgreSQL File/Block Formatted Dump Utility
*
* File: /app/opengauss/cluster/dn1/base/15938/106577
* Options used: -D int,timestamp,text
*******************************************************************

Block    0 ********************************************************
<Header> -----
 Block Offset: 0x00000000         Offsets: Lower      60 (0x003c)
 Block: Size 8192  Version    6            Upper    7792 (0x1e70)
 LSN:  logid      0 recoff 0x03120268      Special  8192 (0x2000)
 Items:    5                      Free Space: 7732
 Checksum: 0x9710  Prune XID: 0x00021865  Flags: 0x0040 (CHECKSUM_FNV1A)
 Length (including item array): 44

<Data> -----
 Item   1 -- Length:   73  Offset: 8112 (0x1fb0)  Flags: NORMAL
COPY: 1 2014-09-08 08:12:03.888841      8897f71deb93ec6299f6742294b0de5d
 Item   2 -- Length:   73  Offset: 8032 (0x1f60)  Flags: NORMAL
COPY: 2 2001-06-29 11:12:43.078154      d475a36d25c3a6056679dd3a902f3a7a
 Item   3 -- Length:   73  Offset: 7952 (0x1f10)  Flags: NORMAL
COPY: 3 2007-09-03 16:12:38.970582      7a2cd1d21dd2ad17b68ccad762bd0836
 Item   4 -- Length:   73  Offset: 7872 (0x1ec0)  Flags: NORMAL
COPY: 4 2025-07-31 09:44:47.962485      6e2d5eb3ae7bbdba1291e0dbe9b5f990
 Item   5 -- Length:   73  Offset: 7792 (0x1e70)  Flags: NORMAL
COPY: 5 2025-06-21 19:48:45.16557       0d8f76f7a4e22024b5d98d93c4b062ab


*** End of File Encountered. Last Block Read: 0 ***
[omm@cmnode1 ~]$ gs_filedump -o -D int,timestamp,text $PGDATA/base/15938/106577

*******************************************************************
* PostgreSQL File/Block Formatted Dump Utility
*
* File: /app/opengauss/cluster/dn1/base/15938/106577
* Options used: -o -D int,timestamp,text
*******************************************************************

Block    0 ********************************************************
<Header> -----
 Block Offset: 0x00000000         Offsets: Lower      60 (0x003c)
 Block: Size 8192  Version    6            Upper    7792 (0x1e70)
 LSN:  logid      0 recoff 0x03120268      Special  8192 (0x2000)
 Items:    5                      Free Space: 7732
 Checksum: 0x9710  Prune XID: 0x00021865  Flags: 0x0040 (CHECKSUM_FNV1A)
 Length (including item array): 44

<Data> -----
 Item   1 -- Length:   73  Offset: 8112 (0x1fb0)  Flags: NORMAL
COPY: 1 2014-09-08 08:12:03.888841      8897f71deb93ec6299f6742294b0de5d
 Item   2 -- Length:   73  Offset: 8032 (0x1f60)  Flags: NORMAL
COPY: 2 2001-06-29 11:12:43.078154      d475a36d25c3a6056679dd3a902f3a7a
 Item   3 -- Length:   73  Offset: 7952 (0x1f10)  Flags: NORMAL
tuple was removed by transaction #137317
 Item   4 -- Length:   73  Offset: 7872 (0x1ec0)  Flags: NORMAL
COPY: 4 2025-07-31 09:44:47.962485      6e2d5eb3ae7bbdba1291e0dbe9b5f990
 Item   5 -- Length:   73  Offset: 7792 (0x1e70)  Flags: NORMAL
COPY: 5 2025-06-21 19:48:45.16557       0d8f76f7a4e22024b5d98d93c4b062ab


*** End of File Encountered. Last Block Read: 0 ***
```

**如果只需要解析前两列信息，可用`~`符号忽略之后的列类型**

```bash
[omm@cmnode1 ~]$ gs_filedump -o -D int,timestamp,~ $PGDATA/base/15938/106577 |grep -i copy
COPY: 1 2014-09-08 08:12:03.888841
COPY: 2 2001-06-29 11:12:43.078154
COPY: 4 2025-07-31 09:44:47.962485
COPY: 5 2025-06-21 19:48:45.16557
```

> 注意： ~ 符号只能存在-D参数的最后，不能存在在中间，例如 `-D int,~,text`  是不被允许的

##### 其他

```bash
# 如果需要显示条目详细信息可加-i参数
gs_filedump -i -D int,timestamp,text $PGDATA/base/15743/237606
# 如果需要转储toast文件，可加-t参数
gs_filedump -t -D int,timestamp,text $PGDATA/base/15743/237606

```

#### ustore存储引擎

```bash
# 使用方式通astore,只需要添加一个-u参数指定存储引擎类型

[omm@cmnode1 ~]$ gs_filedump -u -D int,timestamp,text $PGDATA/base/15938/106591

# 加上`-o`参数，将不显示已删除的条目
gs_filedump -u -o -D int,timestamp,text $PGDATA/base/15938/106591
# 如果只需要解析前两列信息，可用`~`符号忽略之后的列类型
gs_filedump -u -D int,timestamp,~ $PGDATA/base/15938/106591
# 如果需要显示条目详细信息可加-i参数
gs_filedump -u -i -D int,timestamp,text $PGDATA/base/15938/106591
# 如果需要转储toast文件，可加-t参数
gs_filedump -u -t -D int,timestamp,text $PGDATA/base/15938/106591
```



#### 段页式存储引擎

```bash
openGauss=# \d+ stbl_001;
                                 Table "public.stbl_001"
 Column |            Type             | Modifiers | Storage  | Stats target | Description 
--------+-----------------------------+-----------+----------+--------------+-------------
 id     | integer                     |           | plain    |              | 
 tmsp   | timestamp without time zone |           | plain    |              | 
 text   | text                        |           | extended |              | 
Has OIDs: no
Options: orientation=row, segment=on, compression=no

openGauss=# select oid, relname, relfilenode, reltoastrelid from pg_class where relname='stbl_001';
  oid   | relname  | relfilenode | reltoastrelid 
--------+----------+-------------+---------------
 106594 | stbl_001 |        4169 |        106597
(1 row)

openGauss=# select oid, relname, relfilenode, reltoastrelid from pg_class where oid = 106597;
  oid   |     relname     | relfilenode | reltoastrelid 
--------+-----------------+-------------+---------------
 106597 | pg_toast_106594 |        4170 |             0
(1 row)

openGauss=# select pg_relation_filepath('stbl_001');
 pg_relation_filepath 
----------------------
 base/15938/4169
(1 row)
```

 

因为段页式存储引擎的表都存在1-5号文件，不单独存储在独立的文件中，所有转储段页式存储表，需要指定1号文件的路径，和要转储表的`relfilenode`

```bash
# -r 指定表relfilenode
[omm@cmnode1 ~]$ gs_filedump -r 4169 -D int,timestamp,text $PGDATA/base/15938/1 | grep -i copy
```



段页式存储，如果需要转储toast类型字段，需要用`-T`参数指定`pg_toast`对应的`relfilenode`

```bash
[omm@cmnode1 ~]$ gs_filedump -r 4169 -T 4170 -D int,timestamp,text $PGDATA/base/15938/1 | grep -i copy
```



### 在无法登录数据库时查询表映射信息

以上都是能登录数据库时，可以方便的查出表对应二进制文件和`relfilenode`等相关信息，如果数据库无法登录时，该怎么获取相关信息？

**以tbl_001表为例**

#### 1、系统表pg_class的`oid`为1259，首先通过映射文件找到pg_class对应的物理文件。

```bash
[omm@cmnode1 dn1]$ cd $PGDATA
[omm@cmnode1 dn1]$ find . -name pg_filenode.map
./global/pg_filenode.map
./base/1/pg_filenode.map
./base/15933/pg_filenode.map
./base/15938/pg_filenode.map

[omm@cmnode1 dn1]$ gs_filedump -m ./base/1/pg_filenode.map |grep 1259
OID: 1259       Filenode: 15419

# 这里查询到系统表pg_class对应的物理文件为15419
```

#### 2、先通过pg_class文件查询表pg_class的相关信息

```bash
[omm@cmnode1 dn1]$ gs_filedump  -io -D  name,oid,oid,oid,oid,oid,oid,oid,float8,float8,int,oid,oid,oid,oid,oid,oid,bool,bool,char,char,smallint,smallint,bool,bool,bool,bool,bool,char,bool,bool,char,int,text,text,~ ./base/15938/15419  |grep -v "tbl_001_" | grep -i -B 6 " tbl_001"
  XMIN: 125897  XMAX: 0  CID|XVAC: 7  OID: 106577
  Block Id: 6  linp Index: 23   Attributes: 40   Size: 32
  infomask: 0x290b (HASNULL|HASVARWIDTH|HASOID|XMAX_LOCK_ONLY|XMIN_COMMITTED|XMAX_INVALID|UPDATED|HEAP_ONLY) 
  t_bits: [0]: 0xff [1]: 0xff [2]: 0xff [3]: 0xff 
          [4]: 0x9d 

COPY: tbl_001   2200    106579  0       10      0       106577  0       1       5       0       106580  0       0       0       0       0       t       f       p       r       3       0137315   \N      \0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0L\0\0\0orientation=row\0H\0\0\0compression=no\0\0
```

>  通过以上信息可获得tbl_001的`oid`为 106577
>
>  `relnamespace`为：2200， `relfilenode`为：106577， `reltoastrelid`为： 106580
>
>  `reloptions`为：orientation=row，compression=no

#### 3、以同样的方式获取系统表pg_attribute和系统表pg_type的oid，系统表pg_namespace的relfilenode

```
[omm@cmnode1 dn1]$ gs_filedump  -io -D  name,oid,oid,oid,oid,oid,oid,oid,float8,float8,int,oid,oid,oid,oid,oid,oid,bool,bool,char,char,smallint,smallint,bool,bool,bool,bool,bool,char,bool,bool,char,int,text,text,~ ./base/15743/15339 |grep -iE -B 10 "pg_attribute|pg_type|pg_namespace"
```

> 获得 pg_type对应`oid`为1247, pg_attribute对应`oid`为：1249,
>
> `pg_namespace`对应`oid`为：2615， `relfilenode`为：15535
>
> 注意：不同数据库这里获取的`oid`值可能不同

#### 4、获取表pg_type和表pg_attribute对应的物理文件

```bash
[omm@cmnode1 dn1]$ gs_filedump -m ./base/1/pg_filenode.map |grep -E "1247|1249|2615"
OID: 1249       Filenode: 15393
OID: 1247       Filenode: 15297

# pg_attribute:
[omm@cmnode1 dn1]$ find . -name 15393
./base/1/15393
./base/15933/15393
./base/15938/15393
# pg_type
[omm@cmnode1 dn1]$ find . -name 15297
./base/1/15297
./base/15933/15297
./base/15938/15297
```

#### 5、通过表pg_attribute获取表tbl_001的字段名称及类型oid

```bash
[omm@cmnode1 dn1]$ gs_filedump -o -D oid,name,oid,int,smallint,smallint,~ ./base/15938/15393 |grep -i "copy: 106577"
COPY: 106577    id      23      -1      4       1
COPY: 106577    tmsp    1114    -1      8       2
COPY: 106577    text    25      -1      -1      3
COPY: 106577    ctid    27      0       6       -1
COPY: 106577    xmin    28      0       8       -3
COPY: 106577    cmin    29      0       4       -4
COPY: 106577    xmax    28      0       8       -5
COPY: 106577    cmax    29      0       4       -6
COPY: 106577    tableoid        26      0       4       -7
COPY: 106577    xc_node_id      23      0       4       -8
```

排查掉最后一列为负数的，即为表tbl_001的列信息

```bash
COPY: 106577    id      23      -1      4       1
COPY: 106577    tmsp    1114    -1      8       2
COPY: 106577    text    25      -1      -1      3
```

#### 6、通过系统表pg_type查询`oid`为：[23, 1114,  25] 的具体类型

>  注意，此处的`id, tmsp, text `为创建表的表名，非表类型。

```bash
[omm@cmnode1 dn1]$ gs_filedump -i -D name,~ ./base/15938/15297 |grep -EA 5 'OID: 23$|OID: 1114$|OID: 25$' | grep -E 'OID|COPY' | grep -v infomask | awk '{print $NF}' |xargs -n2
23 int4
25 text
1114 timestamp
```

与第5步的信息组合，可获取表结构为：

```bash
tbl_001:
id    int4
tmsp  timestamp
text  text

oid: 106577 relnamespace为：2200， relfilenode为：106577， reltoastrelid为： 106580
```



### 脚本查询

鉴于查询表结构较为复杂，现把上述步骤整理为脚本 gs_desc，便于用户使用

#### 帮助信息

```bash
[omm@cmnode1 ~]$ gs_desc -h
usage: gs_desc [-h] [-s SEARCHPATH] [-n NAMESPACE [NAMESPACE ...]] -t
               TABLENAME [TABLENAME ...]

Process some integers.

optional arguments:
  -h, --help            show this help message and exit
  -s SEARCHPATH, --searchpath SEARCHPATH
                        Specify the search path
  -n NAMESPACE [NAMESPACE ...], --namespace NAMESPACE [NAMESPACE ...]
                        Specify the namespace(s)
  -t TABLENAME [TABLENAME ...], --tablename TABLENAME [TABLENAME ...]
                        Specify the tablename(s)
```



#### 举例

```bash
[omm@cmnode1 ~]$ gs_desc -t tbl_001 utbl_001 utbl_002
**************************************************
*
*        Namespaces: None, Tables: ['tbl_001', 'utbl_001', 'utbl_002']
*
**************************************************
        Table "public.tbl_001"
Column Name    | Type
---------------+--------
id             | int4
tmsp           | timestamp
text           | text

OID: 106577, Relname.Relfilenode: 106577, Toast.Relfilenode: 106580
Suggest Query Type: 
     -o -D int,timestamp,text
Location of Binary file : 
   /app/opengauss/cluster/dn1/base/15938/106577

Options: orientation=row, compression=no

        Table "public.utbl_001"
Column Name    | Type
---------------+--------
id             | int4
tmsp           | timestamp
text           | text

OID: 106585, Relname.Relfilenode: 106591, Toast.Relfilenode: 106592
Suggest Query Type: 
    -u -o -D int,timestamp,text
Location of Binary file : 
   /app/opengauss/cluster/dn1/base/15938/106591

Options: orientation=row, type=ustore, compression=no

@@@@@@@@@@
Not found table(s): ['utbl_002']
@@@@@@@@@@
```



### 转储数据检查

```bash
# 计算通过gs_filedump工具导出表的MD5值
[omm@cmnode1 ~]$ gs_filedump -o -D int,timestamp,text /app/opengauss/cluster/dn1/base/15938/106577 |grep -i copy | sed 's/COPY: //g' |md5sum
56223536bcb06106bd10389e1cd7fef6  -

# 计算通过gsql的copy命令获取表数据的MD5值
[omm@cmnode1 ~]$ gsql -c "copy tbl_001 to stdout" | md5sum
56223536bcb06106bd10389e1cd7fef6  -
```

对比两种方式获取的数据的md5值是一致的，可以判断导出表数据无误。