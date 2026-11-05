# Backup and Recovery Failure Due to Failure in Rebuilding the Standby Node of the Cluster

## Symptom

For row-store compressed tables, rebuilding the standby node of the cluster fails after performing a backup and restore operation.

1. Connect to the database, create a regular table, and insert data:

```sql
DROP TABLE IF EXISTS t_alter_rowcompress_0059 CASCADE;
CREATE TABLE t_alter_rowcompress_0059 (
    columnone INTEGER,
    columntwo CHAR(50),
    columnthree VARCHAR(50),
    columnfour INTEGER,
    columnfive CHAR(50),
    columnsix VARCHAR(50),
    columnseven CHAR(50),
    columneight CHAR(50),
    columnnine VARCHAR(50),
    columnten VARCHAR(50),
    columneleven CHAR(50),
    columntwelve CHAR(50),
    columnthirteen VARCHAR(50),
    columnfourteen CHAR(50),
    columnfifteen VARCHAR(50)
);
INSERT INTO t_alter_rowcompress_0059 VALUES (
    generate_series(0, 1000),
    'test',
    substring(md5(random()::text), 1, 16),
    2,
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16),
    substring(md5(random()::text), 1, 16)
);
```

2. Alter the regular table to a compressed table:

```sql
ALTER TABLE t_alter_rowcompress_0059 SET (
    compresstype = 2,
    compress_chunk_size = 1024,
    compress_prealloc_chunks = 6,
    compress_byte_convert = true,
    compress_diff_convert = true,
    compress_level = 6
);
CHECKPOINT;
```

3. Create a backup directory and initialize it:

```bash
gs_probackup init -B /home/zcz_ct/f_aler_rowcompress_0059
```

4. Initialize a new backup instance within the backup path:

```bash
gs_probackup add-instance -B /home/zcz_ct/f_aler_rowcompress_0059 \
-D /usr1/zczct_install/omm/openGauss/dn1 \
--instance=pro1
```

5. Perform a full backup:

```bash
gs_probackup backup -B /home/zcz_ct/f_aler_rowcompress_0059 \
-b full \
--stream \
--instance=pro1 \
--skip-block-validation \
--no-validate \
-p 24512 \
-d postgres
```

6. Drop the table and stop the primary database node:

```sql
DROP TABLE IF EXISTS t_alter_rowcompress_0059;
```

7. Execute the restore process:

```bash
gs_probackup restore -B /home/zcz_ct/f_aler_rowcompress_0059 \
--instance=pro1 \
--incremental-mode=checksum \
-D /usr1/zczct_install/omm/openGauss/dn1;
```

8. Start the primary database node and rebuild the standby node:

```bash
gs_ctl build -D /usr1/zczct_install/omm/openGauss/dn1
```

## Troubleshooting Method

Analysis of the standby node rebuild logs indicates an error occurred while reading a specific block of a page. This ultimately led to a checksum verification failure, causing the operation to abort, as shown below:

```shell
[2024-08-08 16:24:29.120][501509][dn_6001_6002_6003][gs_rewind]: path: base/16384/47443_compress, type: O, action: 1 Begin fetching files
[2024-08-08 16:25:29.427][501509][dn_6001_6002_6003][gs_rewind]: unexpected result while fetching remote files: ERROR: can not read actual block 0, error code: 18446744073709551613,CONTEXT: referenced column: jsongs rewind receive FATAL, it will exit
[2024-08-08 16:25:29.428][501509][dn_6001_6002_6003][gs_ctl]: inc build failed.
[2024-08-08 16:25:29.428][501509][dn_6001_6002_6003][gs_ctl]: Get repl auth mode is and repl uuid is
[2024-08-08 16:25:29.434][501509][dn_6001_6002_6003][gs_rewind]: last build uncompleted, change to full build.
[2024-08-08 16:25:29.434][501509][dn_6001_6002_6003][gs_ctl]: inc build failed.
[2024-08-08 16:25:29.435][501509][dn_6001_6002_6003][gs_ctl]: Get repl auth mode is and repl uuid is
[2024-08-08 16:25:29.441][501509][dn_6001_6002_6003][gs_rewind]: last build uncompleted, change to full build.
[2024-08-08 16:25:29.441][501509][dn_6001_6002_6003][gs_ctl]: inc build failed, change to full build.
[2024-08-08 16:25:29.441][501509][dn_6001_6002_6003][gs_ctl]: set gaussdb state file when auto build build:db state(BUILDING_STATE), server mode(STANDBY_MODE), build mode(FULL_BUILD).
[2024-08-08 16:25:29.442][501509][dn_6001_6002_6003][gs_ctl]: Get repl auth mode is and repl uuid is
[2024-08-08 16:25:29.447][501509][dn_6001_6002_6003][gs_ctl]: connected to server success, build started.
[2024-08-08 16:25:29.521][501509][dn_6001_6002_6003][gs_ctl]: clear old target dir success
[2024-08-08 16:25:29.521][501509][dn_6001_6002_6003][gs_ctl]: create build tag file success
[2024-08-08 16:25:29.521][501509][dn_6001_6002_6003][gs_ctl]: create build tag file again success
[2024-08-08 16:25:29.521][501509][dn_6001_6002_6003][gs_ctl]: get system identifier success
[2024-08-08 16:25:29.521][501509][dn_6001_6002_6003][gs_ctl]: create backup label success
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: xlog start point: 0/24004690
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: begin build tablespace list
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: finish build tablespace list
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: begin get xlog by xlogstream
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: starting background WAL receiver
[2024-08-08 16:25:29.590][501509][dn_6001_6002_6003][gs_ctl]: starting walreceiver
[2024-08-08 16:25:29.592][501509][dn_6001_6002_6003][gs_ctl]: begin receive tar files
[2024-08-08 16:25:29.598][501509][dn_6001_6002_6003][gs_ctl]: check identify system success
[2024-08-08 16:25:29.598][501509][dn_6001_6002_6003][gs_ctl]: send START_REPLICATION 0/24000000 success
Begin Receiving files
Progress: [======] 100% (148468/148468KB). (1/1)tablespaces. Receive files
Finish Receiving files
[2024-08-08 16:26:29.858][501509][dn_6001_6002_6003][gs_ctl]: could not get WAL end position from server: FATAL: checksum error
```

Since the standby rebuild process does not modify table structures, it can be inferred that the compressed table was corrupted during the backup phase. Further analysis using the `pagehack` utility confirmed that the restored files were indeed corrupted:

```shell
-rw- 1 zcz_ct zcz_ct 4 Aug 30 15:24 PG_VERSION
$ pagehack -f 16445_compress
free(): double free detected in tcache 2
Aborted (core dumped)
$ pagehack -f 16448_compress
free(): double free detected in tcache 2
Aborted (core dumped)
```

Tracking the file write operations during the restore phase via GDB revealed the following:

```gdb
Breakpoint 5, CfsReadCompressedPage (dst=0xffffffffb208 "", destLen=8192, extent_offset blkno=0, cfsReadStruct=0xffffffffb180, blockNum=0) at cfs_tools.cpp:70
70 in cfs_tools.cpp
(gdb) p cfsExtentAddress
$2 = (CfsExtentAddress *) 0xfffff7fd000c
(gdb) p *cfsExtentAddress
$3 = {checksum = 1050652, nchunks = 8 '\b', allocated_chunks = 8 '\b', chunknos = 0xfffff7fd0012}
(gdb) p *cfsExtentAddress.chunknos
$4 = 1
(gdb) p *(cfsExtentAddress.chunknos+1)
$6 = 2
(gdb) p *(cfsExtentAddress.chunknos+7)
$9 = 8
(gdb) p *(cfsExtentAddress.chunknos+8)
$10 = 0
```

GDB tracking during the restore phase indicated that while writing data to the new file, the calculated size of the compressed page was incorrect. This resulted in a mismatched checksum. Analysis of the `CalRealWriteSize` function revealed that it returned a fixed value regardless of whether it was processing compressed or non-compressed pages.

## Root Cause

An error occurred in the process of rewriting compressed files during backup and restore. Due to compression markers, certain data blocks returned an incorrect size. This caused checksum verification failures during the subsequent standby node rebuild process.

## Solution

It is necessary to accurately calculate the block size for each page within the `GetSizeOfCprsHeadData` function. Removing the special handling logic for pages marked with `COMP_ASSIGNMENT` and relying on dynamic calculation ensures that correct data is written to the new pages, thereby guaranteeing the accuracy of the checksum value.
