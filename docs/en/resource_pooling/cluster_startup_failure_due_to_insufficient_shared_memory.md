# Cluster Startup Failure Due to Insufficient Shared Memory

## Symptom

1. When starting or restarting an openGauss resource pool deployment cluster, the following error message is reported:

    ```shell
    cm_ctl: start cluster failed in (601)s!

    HINT: Maybe the cluster is continually being started in the background.
    You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
    ```

    The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal. However, some nodes in the `Datanode State` display the status `Down Manually stopped`, and the overall `cluster_state` is reported as `Degraded`.

2. Additionally, the following error message may be present:

    ```shell
    dss failed to startup.
    ```

    This indicates that the DSS service failed to launch.

## Troubleshooting Method

**For Symptom 1:**

Log in to the affected node machine, navigate to the `$PGDATA` directory, and inspect the `DBstart.log` file. The following error message will be present:

```shell
2024-10-14 16:16:29.056 670cd359.1 [unknown] 281461820163632 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error usually means that the openGauss's request for a shared memory segment exceeded your kernel's SHMMAX parameter.  You can either reduce the request size or reconfigure the kernel with larger SHMMAX.  To reduce the request size (currently xxx bytes), reduce openGauss's shared memory usage, perhaps by reducing shared_buffers.

If the request size is already small, it's possible that it is less than your kernel's SHMMIN parameter, in which case raising the request size or reconfiguring SHMMIN is called for.

    The openGauss documentation contains more information about configuring your system for openGauss.
```

The aforementioned logs indicate that Symptom 1 is caused by insufficient system shared memory.

**For Symptom 2:**

Log in to the affected node machine, navigate to the `$DSS_HOME/log/run` directory, and inspect the most recent `dssinstance.rlog` file. The following error messages will be present:

```shell
UTC+8 2024-11-01 09:38:05.249|dssserver|761014|ERROR>DSS instance failed to initialize shared memory! [dss_instance.c:260]
UTC+8 2024-11-01 09:38:05.249|dssserver|761014|ERROR>DSS instance failed to initialized! [dss_instance.c:383]

UTC+8 2024-11-01 09:38:05.250|dssserver|761014|ERROR>dss failed to startup. [dssserver.c:381]
```

The aforementioned logs clearly indicate that Symptom 2 is caused by insufficient system shared memory, resulting in DSS startup failure.

However, if the deployment is in a developer environment, this issue may also arise due to a duplicate `INST_ID` value in the `dss_inst.ini` configuration file conflicting with other developer environments on the same machine.

## Root Cause

Insufficient system shared memory prevents the database from meeting the minimum shared memory requirements, leading to cluster startup or restart failure. For specific shared memory size requirements, refer to Table 2 (Operating System Parameters) in [gs_checkos](https://gitcode.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/gs_checkos.md).

## Solution

Apply the following solutions based on the specific scenario:

1. **Standard Deployment:**
    *   Verify the current system shared memory configuration using the `cat /proc/sys/kernel/shmall` and `cat /proc/sys/kernel/shmmax` commands.
    *   If the parameters do not meet the minimum requirements, modify the `/etc/sysctl.conf` file accordingly. Execute the `sysctl -p` command to apply the changes, then restart the cluster.
    *   If the parameters meet the requirements, use the `ipcs -m` command to identify other processes consuming high amounts of shared memory. If these processes are no longer required, release the occupied shared memory using the `ipcrm -m shmid` or `ipcrm -a` commands.

2. **Developer Environment Deployment:**
    *   Modify the `INST_ID` parameter within the `dss_inst.ini` configuration file and attempt to restart the service.
    *   If the startup still fails after modifying `INST_ID`, it confirms that the issue is due to insufficient shared memory. Resolve this by following the standard deployment solution outlined above.

> [!WARNING]
> Exercise caution when executing the `ipcrm -a` command as the `root` user, as it may adversely affect processes belonging to other users.
