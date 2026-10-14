# Replay Mode Changes to Extreme RTO Due to Failure During On-Demand Replay Phase

## Symptom

The database is configured with on-demand replay. However, after a primary node failure, the database switches to Extreme RTO replay mode instead.

## Troubleshooting Method

Check the database logs located in `$GAUSSLOG/pg_log/dn_xxxx`. The logs indicate that the previous on-demand replay cycle did not complete successfully, prompting the system to select Extreme RTO replay for the current recovery.

```shell
2024-10-15 17:14:06.807 670e3259.5407 [unknown] 281329567313824 dn_6003 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  [SS reform][On-demand] do not allow replay in ondemand recovery if last ondemand recovery crash, replayed in extreme rto recovery mode
```

At this stage, querying the control file may show the `Cluster status` as `in on-demand build` or `in on-demand redo`. This represents residual state from the previous on-demand replay cycle. The actual recovery status should be determined based on the log records. This residual information will be refreshed to `normal` after the current recovery cycle concludes.

```shell
[chendong@openGauss137 dn_6003]$ pg_controldata +data

pg_control data

...

reformer data (last page id 64)

Reform control version number:        1
Stable instances list:                6
Primary instance ID:                  2
Recovery instance ID:                 0
Cluster status:                       in on-demand redo
Cluster run mode:                     primary cluster
```

## Root Cause

This behavior aligns with the constraints of the on-demand replay feature. If a failure occurs during the on-demand replay recovery process (specifically during the `in on-demand build` or `in on-demand redo` phases) and the replay does not complete successfully, the system will automatically switch to Extreme RTO replay mode upon the next recovery attempt when on-demand replay is configured.

## Solution

Wait for the current replay cycle to complete. The `Cluster status` in the control file will recover to `normal`. Subsequently, if another failure occurs and the recovering node is configured for on-demand replay, the system will resume using the on-demand replay mode for recovery.

```shell
[chendong@openGauss137 dn_6003]$ pg_controldata +data

pg_control data

...

reformer data (last page id 64)

Reform control version number:        1
Stable instances list:                6
Primary instance ID:                  2
Recovery instance ID:                 0
Cluster status:                       normal
Cluster run mode:                     primary cluster
```
