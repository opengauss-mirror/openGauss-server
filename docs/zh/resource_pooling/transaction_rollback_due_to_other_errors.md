# 因其他报错导致事务回滚的问题

## 一、问题现象

如果事务内报错，则导致事务自动回滚，事务内的修改不会生效，保证数据一致性。事务内报错的原因可能很多，比如语法错误、会话长时间未执行操作导致超时、操作不存在的表等等。

## 二、定位方法

事务内报错的原因可能很多，需要具体分析，一方面可以通过客户端返回的报错信息识别报错原因。另一方面可以查看pg_log目录下的数据库日志里是否存在报错信息识别报错原因。
下面是一个因在事务内查找不存在的表而报错的例子。
客户端输出如下：

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

对应时间段的pg_log报错如下：

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

## 三、问题根因

事务内报错的原因可能很多，比如语法错误、会话长时间未执行操作导致超时、操作不存在的表等等。事务内报错的原因可能很多，需要具体分析，一方面可以通过客户端返回的报错信息识别报错原因，另一方面可以查看pg_log目录下的数据库日志里是否存在报错信息识别报错原因。

## 四、解决方案

如果事务内报错，则导致事务自动回滚，事务内的修改不会生效，保证数据一致性。
