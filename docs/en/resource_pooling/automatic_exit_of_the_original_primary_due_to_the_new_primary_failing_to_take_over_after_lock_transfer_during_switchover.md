# Automatic Exit of the Original Primary Due to the New Primary Failing to Take Over After Lock Transfer

## Symptom

In a resource pool deployment scenario, the Distributed Management Subsystem (DMS) determines the cluster primary node by acquiring the reform lock from the Cluster Manager (CM). During a switchover operation, the reform lock is transferred from the original primary node to the new primary node. If the cluster state changes after the lock transfer—for example, if the new primary node fails and exits—the original primary node will automatically restart.

## Troubleshooting Method

Inspect the `pg_log` logs for the relevant timeframe (directory: `$GAUSSLOG/pg_log/dn_600X`). You will observe that the original primary node initiated a restart sequence.

```
024-10-09 15:07:59.659 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  auditor process 0 started, pid=281417482809248
2024-10-09 15:07:59.662 67062bcf.10000 [unknown] 281417460002720 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  reaper backend started.
2024-10-09 15:07:59.663 67062bcf.2945 [unknown] 281417482809248 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  audit_process_cxt_init enter
2024-10-09 15:07:59.663 67062bcf.2945 [unknown] 281417482809248 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  audit_process_cxt_init pipe init successfully for pipe : 0 file descriptor: 40
2024-10-09 15:07:59.678 67062bcf.2945 [unknown] 281417482809248 dn_6001 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  audit_process_cxt_init success
2024-10-09 15:07:59.788 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <System>             Startup: Loading configuration from /usr3/zhoucong/openGauss/cluster/dn1/mot.conf
2024-10-09 15:07:59.833 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Configuration>      Configuring total memory for relative memory values to: 61440 MB
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Configuration>      Loading max_connections from envelope into MOTEngine: 2000
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Configuration>      Adjusted maximum number of threads to 2004
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Configuration>      Configuring synchronous redo-log handler
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Memory>             Global Memory Limit is configured to: 0 MB --> 49152 MB
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Memory>             Local Memory Limit is configured to: 0 MB --> 9216 MB
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Memory>             Session Memory Limit is configured to: 0 KB --> 0 KB (maximum 2000 sessions)
2024-10-09 15:07:59.834 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <Memory>             Configured automatic chunk allocation policy to 'CHUNK-INTERLEAVED' on 4 nodes NUMA machine in PRODUCTION mode
2024-10-09 15:07:59.854 [MOT] <TID:2970744/-----> <SID:-----/-----> [WARNING]  <System>             NUMA policy in the system is set to automatic. (/proc/sys/kernel/numa_balancing is 1)
2024-10-09 15:07:59.854 [MOT] <TID:2970744/-----> <SID:-----/-----> [INFO]     <System>             Startup: Affinity initialized - numaNodes = 4, coresPerCpu = 24, sessionAffinityMode = 1, taskAffinityMode = 1
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <System>             Startup: All core services Initialized successfully
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <RedoLog>            Using synchronous redo log handler
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <Logger>             Using external logger
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <System>             Startup: Redo-log handler initialized successfully
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointControlFile: init - a previous checkpoint was not found
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <Recovery>           MTLSRecoveryManager: Initialized successfully
2024-10-09 15:07:59.875 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <System>             Startup: Recovery manager initialized successfully
2024-10-09 15:07:59.875 [MOT] <TID:2972938/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker0 - Starting
2024-10-09 15:07:59.875 [MOT] <TID:2972938/00000> <SID:00000/00000> [INFO]     <GC>                 GC PARAMS:
2024-10-09 15:07:59.875 [MOT] <TID:2972938/00000> <SID:00000/00000> [INFO]     <GC>                 QUEUE0:DELETE_QUEUE SizeLimit = 1KB, SizeLimitHigh = 8192KB, Count = 500
2024-10-09 15:07:59.875 [MOT] <TID:2972938/00000> <SID:00000/00000> [INFO]     <GC>                 QUEUE1:VERSION_QUEUE SizeLimit = 100KB, SizeLimitHigh = 8192KB, Count = 1000
2024-10-09 15:07:59.875 [MOT] <TID:2972938/00000> <SID:00000/00000> [INFO]     <GC>                 QUEUE2:UPDATE_COLUMN_QUEUE SizeLimit = 0KB, SizeLimitHigh = 8192KB, Count = 100000
2024-10-09 15:07:59.875 [MOT] <TID:2972938/00000> <SID:00000/00000> [INFO]     <GC>                 QUEUE3:GENERIC_QUEUE SizeLimit = 100KB, SizeLimitHigh = 8192KB, Count = 1000
2024-10-09 15:07:59.876 [MOT] <TID:2972939/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker1 - Starting
2024-10-09 15:07:59.877 [MOT] <TID:2972941/-----> <SID:-----/-----> [INFO]     <Checkpoint>         CheckpointWorker2 - Starting
2024-10-09 15:07:59.878 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <System>             Startup: Checkpoint manager initialized successfully
2024-10-09 15:07:59.878 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <System>             Startup: MOT Engine initialization finished successfully
2024-10-09 15:07:59.878 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <FDW>                Switching to External snapshot manager
2024-10-09 15:07:59.878 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 01000  0 [BACKEND] WARNING:  Incremental Checkpoint is enabled, cannot create and operate on MOT tables (MOT does not support incremental checkpoint)
2024-10-09 15:07:59.878 [MOT] <TID:2970744/02000> <SID:-----/-----> [INFO]     <JitExec>            MOT JIT execution is disabled by user configuration
2024-10-09 15:07:59.881 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  [SS reform][db] Node:0 starts, found cluster PRIMARY:0
2024-10-09 15:07:59.881 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 00000  0 [DMS] LOG:  [SS reform] set dss server status as primary start.
2024-10-09 15:08:02.056 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 00000  0 [DMS] LOG:  [SS reform] set dss server status as primary: success
2024-10-09 15:08:02.061 67062bcc.1 [unknown] 281457309450256 [unknown] 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  Dynamically loading the DMS library, version:
```

Inspect the DMS logs for the relevant timeframe (directory: `$GAUSSLOG/pg_log/DMS/run`). The following error messages indicate that after the switchover lock transfer completed, the switchover operation failed, triggering the automatic exit of the original primary node.

```
UTC+8 2024-10-09 15:07:52.019|DMS|3753680|ERROR>[DMS REFORM]local reformer version(1, 781801668325620) is not same as share reformer version(0, 781783729655967) andshare switchover version(1, 781783739740808) [dms_reform_health.c:103]
UTC+8 2024-10-09 15:07:52.019|DMS|3753680|ERROR>[DMS REFORM]reformer abort during reform [dms_reform_health.c:252]
UTC+8 2024-10-09 15:07:52.019|DMS|3753680|ERROR>[DMS REFORM] node exit, reform fail during switchover, old primary node need restart after reformer lock transfered, demote id:0, reformer id:1 [dms_reform_health.c:302]
UTC+8 2024-10-09 15:07:52.019|DMS|3753677|INFO>[DMS REFORM]dms_reform_fail enter [dms_reform_proc.c:1374]
```

At this stage, the cluster state appears as follows:

```shell
[  Datanode State   ]

node            node_ip         instance                                         state            | node            node_ip         instance                                         state            | node            node_ip         instance                                         state
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss111 xx.xx.xx.111    6001 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 P Down    Starting | 2  openGauss135 xx.xx.xx.135    6002 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 S Standby Starting | 3  openGauss137 xx.xx.xx.137    6003 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 S Standby Starting
```

After the original primary node exits, CM restarts it as a standby node. DMS then reorganizes the reform process, allowing the database to automatically recover to a stable state.

```shell
[xxxxxxxx@openGauss135 ~]$ cm_ctl query -Cvipd
[  Datanode State   ]

node            node_ip         instance                                         state            | node            node_ip         instance                                         state            | node            node_ip         instance                                         state
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss111 xx.xx.xx.111    6001 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 P Standby Normal | 2  openGauss135 xx.xx.xx.135    6002 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 S Primary Normal | 3  openGauss137 xx.xx.xx.137    6003 12703  /usr3/xxxxxxxx/openGauss/cluster/dn1 S Standby Normal
```

## Root Cause

The current DMS design dictates that in a failover scenario, the original primary node does not participate in the ongoing reform process (even if it has restarted). Instead, it waits until the failover completes before reorganizing the reform to integrate the original primary node back into the cluster. This behavior prevents the original primary node from writing logs based on an outdated checkpoint after restarting during a failover, which could corrupt the log stream. Specifically, in scenarios where the new primary fails to promote after a switchover lock transfer, DMS forces the original primary node to exit automatically. CM subsequently restarts this node to ensure cluster consistency.

## Solution

This recovery process is fully automated by DMS and requires no manual intervention from the user. Simply wait for the reform process to complete.