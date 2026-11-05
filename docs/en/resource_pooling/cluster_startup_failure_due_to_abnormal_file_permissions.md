# Cluster Startup Failure Due to Abnormal File Permissions

## Symptom

The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout.

Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are in the `Down` state.

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. The following error messages will be present:

    ```shell
    2024-10-09 11:07:59.018 tid=313687  LOG: open gaussdb state file "/.../.../dn1/gaussdb.state" failed, could not get the build information: Permission denied.

    2024-10-09 11:07:58.966 tid=313678  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).

    2024-10-09 11:07:58.966 tid=313678  ERROR: failed to connect to datanode:/.../.../dn1
    ```

    The absolute path `/.../.../dn1` corresponds to `$PGDATA`. The aforementioned logs indicate that the `$PGDATA` directory has insufficient permissions.

2. Execute `cm_ctl query -Cvipd`. The output shows that all nodes in the `CMServer State` are normal. However, within the `Datanode State`, nodes display the status `Down Manually stopped`, and the overall `cluster_state` is reported as `Unavailable`.

3. Inspect the permissions of the `$PGDATA` directory. It is observed that the database runtime user lacks the necessary privileges to access the `dn1` directory:

    ```shell
    drwx------. 15 root  root  8.0K Oct  9 11:06 dn1
    ```

## Root Cause

The database runtime user lacks the required read and write permissions for the `pg_hba.conf` file.

## Solution

Log in to the affected machine as the `root` user. Modify the ownership or permissions of the `$PGDATA` directory using `chown` or `chmod` to grant access to the database runtime user. Subsequently, restart the cluster.
