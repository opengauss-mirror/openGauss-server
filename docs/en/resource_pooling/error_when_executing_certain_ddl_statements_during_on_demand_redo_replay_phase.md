# Error When Executing Certain DDL Statements During On-Demand Redo Replay Phase

## Symptom

During the on-demand redo replay phase, executing certain DDL statements results in an error, reporting the following error message:

```shell
openGauss=# create database tpcc1;
ERROR:  only support INSERT/UPDATE/DELETE/SELECT/SET/SHOW/CALL, and ALTER/CREATE/DROP on Table/Index/View/Procedure/Schema, and ALTER on Database during SS on-demand recovery, command 752
```

## Troubleshooting Method

Query the `ondemand_recovery_status` view to confirm that the current node is in the on-demand replay state. Verify that the `in_ondemand_recovery` field returns `true` and the `ondemand_recovery_status` field shows `ONDEMAND_RECOVERY_REDO`.

```shell
openGauss=# select * from ondemand_recovery_status();
-[ RECORD 1 ]---------------+-----------------------
primary_checkpoint_redo_lsn | 0/0
realtime_build_replayed_lsn | B/24C39FA0
hashmap_used_blocks         | 9928223
hashmap_total_blocks        | 70640909
trxn_queue_blocks           | 0
seg_queue_blocks            | 0
in_ondemand_recovery        | t
ondemand_recovery_status    | ONDEMAND_RECOVERY_REDO
realtime_build_status       | DISABLED
recovery_pause_status       | NOT PAUSE
record_item_num             | 57001
record_item_mbytes          | 648
```

## Root Cause

The primary node is currently in the on-demand redo replay phase and supports a limited set of DDL operations (see [Feature Enhancements](ultimate_rto_on_demand_playback.md) for details). This behavior is expected.

## Solution

Wait for the on-demand redo replay phase to complete. After this phase ends, there are no restrictions on SQL execution. To monitor the progress of the on-demand redo replay phase, query the `hashmap_used_blocks` field in the `ondemand_recovery_status` view. When the value of `hashmap_used_blocks` reaches `0`, the system performs a full checkpoint and exits the on-demand replay mode. At this point, the `in_ondemand_recovery` field becomes `false` and the `ondemand_recovery_status` field changes to `normal`.
