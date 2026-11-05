# Node Exit Due to Failure of the First Round of Reform During Database Startup

## Symptom

In a resource pool scenario, when a stable cluster encounters anomalies such as node restarts, node evictions, or node joins, DMS must handle the situation to transition the cluster from an unstable state to a stable state; this process is called **Reform** in the resource pool architecture. The Reform triggered by a specific database node starting up is referred to as that node's **first-round Reform** (or startup-round Reform).

If a new node state change occurs during Reform—for example, if a node starts or stops during the Reform period—the current round of Reform will fail. DMS will then organize a new Reform based on the current cluster state to transition the cluster to a new stable state.

Under the current DMS design, the first-round Reform for a database node is **not allowed to fail** for that node. If the first-round Reform of a node fails, DMS forces that node to exit. CM will then restart the node, and DMS will trigger a new round of Reform to transition the cluster to a new stable state.

## Troubleshooting Method

Taking a three-node cluster as an example, denoted as Node 0, Node 1, and Node 2, where Node 0 is the primary node. Execute `kill -9 xxx` to forcibly stop Node 0. While Node 0 is in the `Starting` state, execute `kill -9 xxx` to forcibly stop Node 1 or Node 2. You will observe that Node 0 restarts again.

Check the `pg_log` logs for the corresponding timeframe (directory: `$GAUSSLOG/pg_log/dn_600X`). You will find the following warning message: `WARNING:  [SS reform][db sync wait] Node:0 first-round reform failed, shutdown now`. Upon detecting the failure of the first-round Reform, the database node sends a `SIGTERM` signal to the postmaster thread and proceeds with the database shutdown process.

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

Check the DMS logs for the corresponding timeframe (directory: `$GAUSSLOG/pg_log/DMS/run`). DMS frequently reports communication exceptions with Node 2. Finally, the log `INFO>[mes]: disconnect node 2.` is printed. It can be seen that the disconnection of Node 2 caused the current round of Reform to fail. Since Node 0 was undergoing its first-round Reform, Node 0 exited.

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

After Node 0 exits, CM will detect the exit of Node 0 and automatically restart Node 0. The cluster will eventually recover to a stable state.

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

## Root Cause

Under the current DMS design, the first-round Reform is not permitted to fail for a database node. If the first-round Reform of a node fails, DMS controls that node to exit. CM will then restart the node, and DMS will trigger a new round of Reform to transition the cluster to a new stable state. During this process, no manual intervention is required from the user; one only needs to wait for the Reform to complete. That is, when checking the results of `cm_ctl query -Cv`, if the state of each node is `Normal`, the process is complete.

## Solution

This issue pertains to a special Reform scenario. No manual intervention is required; simply wait for the Reform to complete.
