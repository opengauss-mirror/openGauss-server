# Transaction Rollback Due to Other Errors

## Symptom

If an error occurs within a transaction, the transaction is automatically rolled back. Any modifications made within the transaction will not take effect, thereby ensuring data consistency. Errors within a transaction can arise from various causes, such as syntax errors, session timeouts due to prolonged inactivity, or operations on non-existent tables.

## Troubleshooting Method

Errors within a transaction can stem from numerous causes and require specific analysis. You can identify the root cause through two primary channels:
1.  **Client Error Messages**: Analyze the error output returned directly to the client.
2.  **Database Logs**: Check the database logs located in the `pg_log` directory for detailed error information.

The following example demonstrates a transaction rollback caused by querying a non-existent table.

**Client Output:**

```shell
tpccdb=# create table t1 (id int);
CREATE TABLE
tpccdb=# begin;
BEGIN
tpccdb=# select * from t2;
ERROR:  relation "t2" does not exist on node1
LINE 1: select * from t2;
                      ^
tpccdb=# select * from t1;
ERROR:  current transaction is aborted, commands ignored until end of transaction block, firstChar[Q]
tpccdb=# rollback;
ROLLBACK
```

**Corresponding Database Log (`pg_log`):**

```shell
2024-10-08 11:27:32.498 zhoucong_dev tpccdb [local] 281426503981504 0[0:0#0]  0 [BACKEND] LOG:  statement: select * from t2;
2024-10-08 11:27:32.509 zhoucong_dev tpccdb [local] 281426503981504 0[0:0#0]  281474976710792 [BACKEND] ERROR:  relation "t2" does not exist on node1 at character 15
2024-10-08 11:27:32.509 zhoucong_dev tpccdb [local] 281426503981504 0[0:0#0]  281474976710792 [BACKEND] STATEMENT:  select * from t2;
2024-10-08 11:27:32.509 zhoucong_dev tpccdb [local] 281426503981504 0[0:0#0]  281474976710792 [BACKEND] BACKTRACELOG:  tid[397046]'s backtrace:
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0x10b04bc]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z9errfinishiz+0x334) [0x10a4740]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z15parserOpenTableP10ParseStatePK8RangeVaribbb+0x5ec) [0xd0f0e0]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z18addRangeTableEntryP10ParseStateP8RangeVarP5Aliasbbbbb+0x180) [0xd0f52c]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0xcc92b4]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z23transformFromClauseItemP10ParseStateP4NodePP13RangeTblEntryPiS5_S6_PP4Listbbbb+0xcc) [0xccb56c]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z19transformFromClauseP10ParseStateP4Listbbb+0xbc) [0xcc81bc]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0xc325e4]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z13transformStmtP10ParseStateP4Nodebb+0x24c) [0xc2b784]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z21transformTopLevelStmtP10ParseStateP4Nodebb+0x224) [0xc2a700]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z13parse_analyzeP4NodePKcPjibbP10ParseState+0x184) [0xc29c44]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z22pg_analyze_and_rewriteP4NodePKcPjiP10ParseState+0x7c) [0x1999d68]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0x199df74]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z12PostgresMainiPPcPKcS2_+0x44c4) [0x19b0264]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0x18c96a8]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb(_Z17GaussDbThreadMainIL15knl_thread_role1EEiP14knl_thread_arg+0x704) [0x18de780]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0x18d8ff0]
       /home/zhoucong_dev/work/openGauss-server-list/openGauss-server/dest/bin/gaussdb() [0x2594230]
       /lib64/libpthread.so.0(+0x88cc) [0xfffbe92988cc]
       /lib64/libc.so.6(+0xd954c) [0xfffbe91d954c]
       Use addr2line to get pretty function name and line
```

## Root Cause

Errors within a transaction can arise from various causes, such as syntax errors, session timeouts due to prolonged inactivity, or operations on non-existent tables. To determine the specific cause, analyze the error messages returned to the client or review the detailed logs in the `pg_log` directory.

## Solution

If an error occurs within a transaction, the system automatically rolls back the transaction. Any modifications made within the transaction are discarded and do not take effect, thereby preserving data consistency.
