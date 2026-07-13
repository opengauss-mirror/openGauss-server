# 因数据库启动首轮Reform失败导致节点退出的问题

## 一、问题现象

在资源池化场景中，当一个稳定集群出现异常，例如有节点重启、节点被踢出集群、节点加入集群等，DMS需要处理这一情况，将集群从不稳定状态转变为稳定状态，这一过程在资源池化架构下称之为reform。其中，某一数据库节点启动时触发的reform称为该节点的first reform（或启动轮reform）。在reform期间，如果出现新的节点状态变化，例如有节点在reform期间起停，则会导致本轮reform失败，DMS会基于当前的集群状态重新组织新的reform，用于将集群变为新的稳定状态。在目前DMS的设计中，数据库节点的启动轮reform对于该节点是不允许失败的，如果某个节点的启动轮reform失败，DMS会促使该节点退出，然后由CM重新拉起该节点，并由DMS触发新的一轮reform，将集群变为新的稳定状态。

## 二、定位方法

以三节点集群为例，记为0节点、1节点、2节点，其中0节点为主节点，执行`kill -9 xxx`强行停止0节点，在0节点状态为Starting时，执行`kill -9 xxx`强行停止1节点或2节点。会发现0节点再次重启。

查看相应时间段的pg_log日志（目录：$GAUSSLOG/pg_log/dn_600X），可以发现如下告警信息`WARNING:  [SS reform][db sync wait] Node:0 first-round reform failed, shutdown now`，数据库节点发现first-round reform失败后，就会向postmaster线程发送SIGTERM信号，然后走数据库退出流程。

```
2024-09-19 17:14:12.592 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 01000  0 [BACKEND] WARNING:  [SS reform][db sync wait] Node:0 first-round reform failed, shutdown now
2024-09-19 17:14:12.592 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  received fast shutdown request
2024-09-19 17:14:12.592 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  send to startup shutdown request
```

```
2024-09-19 17:14:16.557 66ebeb64.6536 [unknown] 281429379690368 dn_6001 0 dn_6001_6002_6003 00000  0 [DBL_WRT] LOG:  Double write exit
2024-09-19 17:14:16.557 66ebeb64.6536 [unknown] 281429379690368 dn_6001 0 dn_6001_6002_6003 00000  0 [DBL_WRT] LOG:  Double write exit
2024-09-19 17:14:16.558 66ebeb64.6536 [unknown] 281429379690368 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  database system is shut down
2024-09-19 17:14:16.568  [postmaster][reaper][281466178961424] LOG: checkpoint thread exit and nowait for sharestorage
2024-09-19 17:14:16.568 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  All obsArch and obsBarrier exit.
2024-09-19 17:14:16.581 66ebeae8.6534 [unknown] 281449383309184 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  auditor thread exit, id : 0
2024-09-19 17:14:16.581 66ebeae8.6534 [unknown] 281449383309184 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  auditor shutting down
2024-09-19 17:14:16.595 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  All Audit threads exit.
2024-09-19 17:14:16.595 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  All obsArch and obsBarrier exit.
2024-09-19 17:14:16.595 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 DB010  0 [REDO] LOG:  page workers all exit or not open parallel redo
2024-09-19 17:14:16.595 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  all build walsenders exited
2024-09-19 17:14:16.595 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  [SS] notify dms auxiliary thread exit
2024-09-19 17:14:16.597 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  [SS] dms auxiliary thread exit
2024-09-19 17:14:16.597 66ebeab1.1 [unknown] 281466178961424 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  DMS uninit worker threads, DRC, errdesc and DL
2024-09-19 17:14:37.776 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: Shutting down MOT Engine
2024-09-19 17:14:37.776 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: All background tasks stopped
2024-09-19 17:14:37.776 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Destroying the Checkpoint Manager
2024-09-19 17:14:37.778 [MOT] <TID:3224506/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker0 - Exiting
2024-09-19 17:14:37.778 [MOT] <TID:3224510/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker1 - Exiting
2024-09-19 17:14:37.778 [MOT] <TID:3224520/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker2 - Exiting
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Destroying the Recovery Manager
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Destroying the Redo Log Handler
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: All tables cleared
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: Key surrogate manager destroyed
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: Table manager destroyed
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <System>             Shutdown: Session manager destroyed
2024-09-19 17:14:37.782 [MOT] <TID:3140211/04092> <SID:-----/-----> [INFO]     <Memory>             Kernel allocation MM Layer Pre-Shutdown Report report:
  NUMA Interleaved memory usage: Current = 1 MB, Peak = 1 MB
```

查看相应时间段的DMS日志（目录：$GAUSSLOG/pg_log/DMS/run），DMS频繁报错2节点通信异常，最后打印日志`INFO>[mes]: disconnect node 2.`，可以看出是由于2节点断开连接，本轮reform失败，由于0节点是first-round reform，因此0节点退出。

```
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|INFO>[mes] mes_close_send_pipe_nolock priority=4, inst_id=2, channel_id=31 [mes_tcp.c:420]
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|INFO>[mes] mes_close_send_pipe priority=4, inst_id=2, channel_id=31 [mes_tcp.c:405]
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|INFO>[mes] mes_close_pipe:inst_id=2,channel_id=31, prio=4, recv pipe closed [mes_tcp.c:429]
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|ERROR>[mes] epoll_ctl event Del failed [fd=667],errno=9. [mes_recv.c:220]
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|INFO>[mes] mes_close_recv_pipe_nolock priority=5, inst_id=2, channel_id=31 [mes_tcp.c:198]
UTC+8 2024-09-19 17:14:37.353|DMS|3140211|INFO>[mes] mes_close_recv_pipe priority=5, inst_id=2, channel_id=31 [mes_tcp.c:183]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_send_pipe priority=5, inst_id=2, channel_id=31 [mes_tcp.c:405]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_pipe:inst_id=2,channel_id=31, prio=5, recv pipe closed [mes_tcp.c:429]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|ERROR>[mes] epoll_ctl event Del failed [fd=668],errno=9. [mes_recv.c:220]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_recv_pipe_nolock priority=6, inst_id=2, channel_id=31 [mes_tcp.c:198]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_recv_pipe priority=6, inst_id=2, channel_id=31 [mes_tcp.c:183]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_send_pipe priority=6, inst_id=2, channel_id=31 [mes_tcp.c:405]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes] mes_close_pipe:inst_id=2,channel_id=31, prio=6, recv pipe closed [mes_tcp.c:429]
UTC+8 2024-09-19 17:14:37.354|DMS|3140211|INFO>[mes]: disconnect node 2. [mes_func.c:1954]
UTC+8 2024-09-19 17:14:37.358|DMS|3140211|INFO>[mes] mes_uninit success [mes_func.c:1504]
UTC+8 2024-09-19 17:14:42.459|DMS|3452914|INFO>[mes] start timer, sleep_time:100000 [cm_timer.c:107]
UTC+8 2024-09-19 17:14:42.459|DMS|3452914|INFO>[LOG] file '/usr1/ertao/openGauss/cluster/log/gaussdb/ertao/pg_log/DMS/run/dms.rlog' is added [cm_log.c:899]
UTC+8 2024-09-19 17:14:42.459|DMS|3452914|INFO>[LOG] file '/usr1/ertao/openGauss/cluster/log/gaussdb/ertao/pg_log/DMS/debug/dms.dlog' is added [cm_log.c:899]
UTC+8 2024-09-19 17:14:42.459|DMS|3452914|INFO>[DMS] dms_init start [dms_process.c:1387]
UTC+8 2024-09-19 17:14:42.459|DMS|3452914|INFO>[DMS] dms_set_global_dms start [dms_process.c:1339]
```

0节点退出后，cm会检测出0节点退出，自动拉起0节点，集群最后会恢复稳定状态。

```
[xxxxxxxx@xxxxxxxxx111 dn_6001]$ cm_ctl query -Cv
[  CMServer State   ]

node            instance state
--------------------------------
1  xxxxxxxxx111 1        Standby
2  xxxxxxxxx135 2        Standby
3  xxxxxxxxx137 3        Primary


[ Defined Resource State ]

node            res_name instance  state
------------------------------------------
1  xxxxxxxxx111 dms_res  6001      OnLine
2  xxxxxxxxx135 dms_res  6002      OnLine
3  xxxxxxxxx137 dms_res  6003      OnLine
1  xxxxxxxxx111 dss      20001     OnLine
2  xxxxxxxxx135 dss      20002     OnLine
3  xxxxxxxxx137 dss      20003     OnLine

[   Cluster State   ]

cluster_state   : Normal
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[  Datanode State   ]

node            instance state            | node            instance state            | node            instance state
------------------------------------------------------------------------------------------------------------------------------------
1  xxxxxxxxx111 6001     P Primary Normal | 2  xxxxxxxxx135 6002     S Standby Normal | 3  xxxxxxxxx137 6003     S Standby Normal
```

## 三、问题根因

在目前DMS的设计中，数据库节点的启动轮reform对于该节点是不允许失败的，如果某节点的启动轮reform失败，DMS会控制该节点退出，然后由CM重新拉起该节点，并由DMS触发新的一轮reform，将集群变为新的稳定状态。在此过程中，用户不需要手动介入，只需要等待reform结束，即`cm_ctl query -Cv`查询结果，各节点状态都为`Normal`。

## 四、解决方案

该问题为reform特殊场景，不需要手动接入，只需要等待reform结束即可。
