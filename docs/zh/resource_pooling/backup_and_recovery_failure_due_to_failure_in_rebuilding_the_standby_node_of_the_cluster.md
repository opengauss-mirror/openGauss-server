# 因重建集群备机失败导致备份恢复失败的问题

## 一、问题表现  

行存压缩表，备份恢复后，重建集群备机失败:  

1. 连接数据库并建立普通表并插入数据:  
`drop table if exists t_alter_rowcompress_0059 cascade;`  
`create table t_alter_rowcompress_0059(columnone,
integer,columntwo char(50), columnthree varchar(50), columnfour integer,
columnfive char(50), columnsix varchar(50), columnseven char(50),
columneight char(50), columnnine varchar(50), columnten varchar(50),
columneleven char(50), columntwelve char(50),
columnthirteen varchar(50), columnfourteen char(50),
columnfifteem varchar(50));`  
`insert into t_alter_rowcompress_0059 values(generate_series(0, 1000),
'test',substring(md5(random()::text), 1, 16),
2, substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16),
substring(md5(random()::text), 1, 16));`  
2. 修改普通表为压缩表:  
`alter table t_alter_rowcompress_0059 set (compresstype=2, compress_chunk_size=1024, compress_prealloc_chunks=6, compress_byte_convert=true, compress_diff_convert=true, compress_level=6); checkpoint;`
3. 新建备份目录并初始化:  
`gs_probackup init -B /home/zcz_ct/f_aler_rowcompress_0059`
4. 在备份路径内初始化一个新的备份实例:  
`gs_probackup add-instance -B /home/zcz_ct/f_aler_rowcompress_0059 -D /usr1/zczct_install/omm/openGauss/dn1 --instance=pro1`
5. 进行全量备份:  
`gs_probackup backup -B /home/zcz_ct/f_aler_rowcompress_0059 -b full --stream --instance=pro1 --skip-block-validation --no-validate -p 24512 -d postgres`
6. 删除表，停止数据库的主节点:  
`drop table if exists t_alter_rowcompress_0059;`
7. 执行restore恢复流程:  
`gs_probackup restore -B /home/zcz_ct/f_aler_rowcompress_0059 --instance=pro1 --incremental-mode=checksum -D /usr1/zczct_install/omm/openGauss/dn1;`
8. 启动数据库主节点，在备节点执行重建备机:  
`gs_ctl build -D /usr1/zczct_install/omm/openGauss/dn1`  

## 二、定位方法

首先通过备机恢复的打印可以分析出，在读取页面的某个block时出现了错误，最后导致整个页面的checksum计算错误校验不通过而失败。如下所示:  

```shell
    [2024-08-08 16:24:29.120][501509][dn_6001_6002 6003][gs rewind]: path: base/16384/47443_compress, type: O, action: 1 Begin fetching files
    [2024-08-08 16:25:29.427][501509][dn 6001 6002 6003][gs rewind]: unexpected result while fetching remote files: ERROR: can not read actual block 0, error code: 18446744073709551613,CONTEXT: referenced column: jsongs rewind receive FATAL, it will exit
    [2024-08-08 16:25:29.428][501509][dn_6001 6002 6003][gs_ctl]: inc build failed.
    [2024-08-08 16:25:29.428][501509][dn 6001 6002 6003][gs ctl]: Get repl auth mode is and repl uuid is
    [2024-08-08 16:25:29.434][501509][dn 6001 6002 6003][gs rewind]: last build uncompleted, change to full build.gs rewind receive FATAL, it will exit
    [2024-08-08 16:25:29.434][501509][dn 6001 6002 6003][gs ctl]: inc build failed.
    [2024-08-08 16:25:29.435][501509][dn 6001 6002 6003][gs ctl]: Get repl auth mode is and repl uuid is
    [2024-08-08 16:25:29.441][501509][dn_6001_6002_6003][gs_rewind]: last build uncompleted, change to full build.gs rewind receive FATAL, it will exit.palled plinq ou :[110s6]
    [2024-08-08 16:25:29.441][501509][dn 6001 6002 6003][gs ctl]: inc build failed, change to full build.
    [2024-08-08 16:25:29.441][501509][dn_6001_6002 6003][gs_ctl]: set gaussdb state file when auto build build:db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(FULL_BUILD).
    [2024-08-08 16:25:29.442][501509][dn 6001 6002 6003][gs ctl]: Get repl auth mode is and repl uuid is
    [2024-08-08 16:25:29.447][501509][dn 6001 6002 6003][gs ctl]: connected to server success, build started.
    [2024-08-08 16:25:29.521][501509][dn 6001 6002 6003][gs ctl]: clear old target dir success
    [2024-08-08 16:25:29.521][501509][dn 6001 6002 6003][gs ctl]: create build tag file success
    [2024-08-08 16:25:29.521][501509][dn_6001_60026003][gs_ctl]: create build tag file again successcr
    [2024-08-08 16:25:29.521][501509][dn 6001 6002 6003][gs ctl]: get system identifier success
    [2024-08-08 16:25:29.521][501509][dn 6001 6002 6003][gs ctl]: create backup label success
    [2024-08-08 16:25:29.590][501509][dn_6001_6002 6003][gs_ctl]: xlog start point: 0/24004690
    [2024-08-08 16:25:29.590][501509][dn 6001 6002 6003][gs_ctl]: begin build tablespace listae
    [2024-08-08 16:25:29.590][501509][dn 6001 6002 6003][gs ctl]: finish build tablespace list
    [2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: begin get xlog by xlogstreamar
    [2024-08-08 16:25:29.590][501509][dn 6001 6002 6003][gs ctl]: starting background WAL receiver
    [2024-08-08 16:25:29.590][501509][dn 6001 6002 6003][gs_ctl]: starting walreceiver
    [2024-08-08 16:25:29.592][501509][dn 6001 6002 6003][gs_ctl]: begin receive tar filese
    [2024-08-08 16:25:29.598][501509][dn 6001 6002 6003][gs ctl]: check identify system success
    [2024-08-08 16:25:29.598][501509][dn 6001 6002 6003][gs ctl]: send START_REPLICATION 0/24000000 success
    Begin Receiving files
    Progress: [=== ==] 100% (148468/148468KB). (1/1)tablespaces. Receive files
    Finish Receiving files
    [2024-08-08 16:26:29.858][501509][dn_6001_6002_6003][gs_ctl]: could not get WAL end position from server: FATAL: checksum error
```  

因为重建备机的过程不涉及对表的修改，由此可以推断出是压缩表在备份过程中损坏了；通过向上分析，先使用pagehack工具分析备份恢复的文件。发现恢复后的文件已经损坏：  

```shell
-rW- 1 zcz ct zcz ct 4 Aug 30 15:24 PG VERSION
$ pagehack -f 16445_compressa
free(): double free detected in tcache 2
Aborted (core dumped)
$ pagehack -f 16448_compress
free(): double free detected in tcache 2
Aborted (core dumped)
```  

通过GDB跟踪备份恢复过程的写文件操作：  

```shell
    Breakpoint 5, CfsReadCompressedPage (dst=0xffffffffb208 "", destLen=8192, extent offset blkno=0, cfsReadStruct=0xffffffffb180, blockNum=0) at ofs_tools.cpp:70
    70 in ofs tools.cpp
    (qdb)p cfsExtentAddress
    $2 = (CfsExtentAddress *) 0xfffff7fd000c
    (gdb)p *cfsExtentAddress
    $3 = {checksum = 1050652, nchunks = 8 '\b', allocated_chunks = 8 '\b', Chunknos = 0xfffff7fd0012}
    (gdb)p *cfsExtentAddress.chunknos
    $4 = 1
    (gdb)p *cfsExtentAddress.chunknos+1
    $5 = 2
    (gdb) p *cfsExtentAddress.(chunknos+1)
    A syntax error in expression, near (chunknos+1)
    (gdb) p *(cfsExtentAddress.chunknos )+1
    $6 = 2
    (gdb) p *(cfsExtentAddress.chunknos+7)
    $9 = 8
    (gdb) p *(cfsExtentAddress.chunknos+8 )
    $10 = 0
```  

通过跟踪restore阶段的gdb发现是在向新的文件里写数据时；计算压缩页面的size错误；而导致的checksum计算值不同；通过分析CalRealWriteSize；发现在处理压缩与非压缩页面时都是返回一个固定值导致的；  

## 三、问题根因

备份恢复过程中，重写压缩文件的流程出错；因为压缩标记的原因，导致部分数据块返回的size大小不正确；也导致后面重建备机过程中的校验checksum不正确；  

## 四、解决方案

需要在GetSizeOfCprsHeadData函数对每个页面的block进行准确的计算即可；所以删除掉针对带COMP_ASIGNMENT标记页面的特殊处理；还是通过计算来向新的页面写入正确数据，就可以保证checksum值的准确性；
