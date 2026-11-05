# Cluster Startup Failure Due to Insufficient Semaphores

## Symptom

When starting or restarting an openGauss resource pool deployment cluster, the following error message is reported:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal. However, some nodes in the `Datanode State` display the status `Down Manually stopped`, and the overall `cluster_state` is reported as `Degraded`.

## Troubleshooting Method

Log in to the affected node machine and navigate to the `$PGDATA` directory. Inspect the `DBstart.log` file for the following error message:

```shell
2024-10-14 14:57:29.056 670cc0cb.1 [unknown] 281461820162064 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error does *not* mean that you have run out of disk space. It occurs when either the system limit for the maximum number of semaphore sets (SEMMNI), or the system wide maximum number of semaphores (SEMMNS), would be exceeded. You need to raise the respective kernel parameter. Alternatively, reduce openGauss's consumption of semaphores by reducing its `max_connections` parameter.

    The openGauss documentation contains more information about configuring your system for openGauss.
```

The aforementioned logs indicate that the issue is caused by insufficient system semaphores.

## Root Cause

Insufficient system semaphores prevent the database from meeting the minimum semaphore requirements, leading to cluster startup or restart failure. For specific kernel semaphore parameter requirements, refer to Table 2 (Operating System Parameters) in [gs_checkos](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/gs_checkos.md).

## Solution

Verify the current system semaphore configuration using the `cat /proc/sys/kernel/sem` command. Modify any parameters that do not meet the minimum requirements, then restart the cluster.
