# Cluster Restart Timeout Due to Forced Shutdown with Excessive Redo Logs

## Symptom

The openGauss cluster startup process times out and exits with the following error message:

```shell
cm_ctl: start cluster.
cm_ctl: start nodeid: 1
cm_ctl: start nodeid: 2
cm_ctl: start nodeid: 3
.................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................................
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

## Troubleshooting Method

Inspect the cluster state; all nodes are observed to be in the `Standby Starting` state. In a resource pool deployment, verify whether the Datanode (DN) has started by examining the `DBstart.log` and `dms_control.log` files within the `$PGDATA` directory. The `dms_control.log` contains the following entries:

```shell
Starting dn...
start dn in /usr1/dbuser/install/dss_home success.
```

Inspect the logs located at `$GAUSSLOG/pg_log/dn_xxx`. The presence of the following information indicates that the current node is actively performing Redo replay:

```
2024-10-11 17:46:12.560 6708f1c0.6311 [unknown] 281458809548704 dn_6001 0 dn_6001_6002_6003 DB010  0 [REDO] LOG:  [REDO_LOG_TRACE]ProcessPendingRecords:replayedLsn:30602196424, blockcnt:268435455, WorkerCount:4, readEndLSN:30603794696
2024-10-11 17:46:54.131 6708f1c0.6311 [unknown] 281458809548704 dn_6001 0 dn_6001_6002_6003 DB010  0 [REDO] LOG:  [REDO_LOG_TRACE]ProcessPendingRecords:replayedLsn:30602196424, blockcnt:402653183, WorkerCount:4, readEndLSN:30603794696
2024-10-11 17:47:08.195 [unknown] [unknown] localhost 281459307032480 0[0:0#0]  0 [DMS] WARNING:  [SS reform] wait recovery to apply done for 600000000 us.
2024-10-11 17:47:34.682 6708f1c0.6311 [unknown] 281458809548704 dn_6001 0 dn_6001_6002_6003 DB010  0 [REDO] LOG:  [REDO_LOG_TRACE]ProcessPendingRecords:replayedLsn:30602196424, blockcnt:536870911, WorkerCount:4, readEndLSN:30603794696
2024-10-11 17:47:39.673 6708f1c0.6311 [unknown] 281458809548704 dn_6001 0 dn_6001_6002_6003 DB010  0 [REDO] LOG:  RedoSpeedDiag: 151146675 us, redoBytes:136,preEndPtr:30602196424, readPtr:30602196424, endPtr:30602196560, speed:0 KB/s, totalTime:151146675
2024-10-11 17:47:45.312 6708f1c0.6319 [unknown] 281458464436128 dn_6001 0 dn_6001_6002_6003 00000  0 [DBL_WRT] LOG:  [single flush] [first version] Reset DW file: file_head[dwn 147, start 0], writer pos is 0
```

This indicates that the current node is currently replaying WAL (Write-Ahead Logging) data. In this scenario, simply wait for the cluster to recover naturally.

## Root Cause

The volume of Redo logs is excessive. The current node is undergoing a lengthy log replay process during startup.

## Solution

Wait for an appropriate duration and monitor the `$GAUSSLOG/pg_log/dn_xxx` logs to confirm that the replay process has completed. Subsequently, execute the `cm_ctl query -Cvidpw` command to verify that the cluster has returned to a normal operational state.
