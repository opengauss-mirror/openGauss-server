# Cluster Startup Failure Due to Abnormal Disk Array Permissions

## Symptom

The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are in the `Down` state.

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. The following error message will be present:

    ```shell
    2024-10-09 10:03:34.593 tid=82953 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/xxx failed, errno 13.
    ```

    `errno 13` indicates insufficient permissions. Execute `ll /dev/xxx` and observe that the ownership is set to `root disk`.

2. Inspect the permissions of other disks. Verify that the runtime user lacks access privileges to the shared storage devices:

    ```shell
    ll /dev | grep $USER
    # Returns empty result
    ```

3. Log in as the `root` user and modify the permissions of the inaccessible disks. Subsequently, start or restart the database successfully. Using the `omm` user as an example:

    ```shell
    # Review the user's XML configuration file to identify the disks utilized by shared storage.
    # It is recommended to use `lsscsi -is` to view device identifiers and determine the corresponding disk path.

    chown -R omm:omm /dev/xxx

    # If permissions are abnormal across multiple nodes, repeat this modification on each affected node.
    ```

4. Execute `cm_ctl query -Cvipd` to verify that the cluster status has returned to normal. If anomalies persist, restart the cluster using `cm_ctl stop` followed by `cm_ctl start`, then query the status again to confirm recovery.

## Root Cause

The database runtime user lacks the necessary read and write permissions to access the underlying disk array devices.

## Solution

Log in to the affected machine as the `root` user. Utilize `chmod` or `chown` to grant the required disk permissions to the database runtime user. Restart the cluster to apply the changes.
