# Cluster Startup Failure Due to Insufficient Memory

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
2024-10-14 11:27:29.056 670c8992.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error usually means that the openGauss's request for a shared memory segment exceeded available memory or swap space, or exceeded your kernel's SHMALL parameter.  You can either reduce the request size or reconfigure the kernel with larger SHMALL.  To reduce the request size (currently xxx bytes), reduce openGauss's shared memory usage, perhaps by reducing shared_buffers.
    The openGauss documentation contains more information about shared memory configuration.
```

The aforementioned logs indicate that the database failed to allocate the configured shared memory size, resulting in an abnormal state.

## Root Cause

Insufficient system memory prevents the database from reserving the required memory during startup, leading to cluster startup or restart failure. For specific memory requirements, refer to Table 1 (Hardware Environment Requirements) in [Preparing the Hardware and Software Installation Environment](https://atomgit.com/opengauss/docs/blob/master/docs/en/getting_started/preparing_for_installation.md).

## Solution

Resolve this issue by performing one of the following actions, followed by a cluster restart:

*   Free up system memory to meet the environmental requirements.
*   Modify relevant database configuration parameters to reduce memory consumption.
*   In high-concurrency scenarios, it is recommended to deploy on machines with larger memory capacity or utilize workload management to limit system concurrency.
