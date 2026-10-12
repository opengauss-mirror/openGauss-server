# Cluster Startup Failure Due to Missing Files or Directories

## Symptom

When starting or restarting an openGauss resource pool deployment cluster, the following error message is reported:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal. However, some nodes in the `Datanode State` display the status `Down Manually stopped`, and the overall `cluster_state` is reported as `Degraded`.

## Troubleshooting Method

1. Log in to the affected node machine and navigate to the `$GAUSSLOG/cm/cm_agent` directory. Inspect the most recent `cm_agent` log file for the following error messages:

    ```shell
    2024-10-10 09:26:02.270 tid=2015951  LOG: gaussdb state file "/.../.../dn1/gaussdb.state" is not exist, could not get the build infomation: No such file or directory

    2024-10-10 09:26:02.746 tid=2015996 DiskUsageCheck ERROR: [GetDiskUsageForPath][line:908] GetDiskUsageForPath /.../.../dn1 disk usage failed! errno:2 err:No such file or directory.

    2024-10-10 09:26:02.880 tid=2015948  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).
    2024-10-10 09:26:02.880 tid=2015948  ERROR: failed to connect to datanode:/.../...//dn1
    ```

    The absolute path `/.../.../dn1` corresponds to `$PGDATA`. The aforementioned logs indicate that the `$PGDATA` directory is missing.

2. Execute the `cd $PGDATA` command. The following response confirms that the `$PGDATA` directory does not exist:

    ```shell
    [  ~]$ cd $PGDATA
    -bash: cd: /.../.../dn1: No such file or directory
    ```

3. If no explicit errors are found in the `cm_agent` logs but you can access the `$PGDATA` directory, inspect the `DBstart.log` file. The following error messages will be present:

    ```shell
    2024-10-10 09:55:35.343 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

    2024-10-10 09:55:35.543 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

    2024-10-10 09:55:35.743 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

    2024-10-10 09:55:35.743 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
    ```

    These database startup logs explicitly indicate that the `pg_hba.conf` file is missing.

## Root Cause

The absence of essential system configuration files or directories prevents the cluster from starting or restarting.

## Solution

If any nodes in the cluster remain in a normal operational state, copy the missing directories or files from a healthy node to the affected node. If no healthy nodes exist, uninstall the cluster and perform a fresh installation followed by a restart.
