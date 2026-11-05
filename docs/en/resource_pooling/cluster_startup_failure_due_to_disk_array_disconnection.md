# Cluster Startup Failure Due to Disk Array Disconnection

## Symptom

The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are in the `Down` state.

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. The following error messages will be present:

    ```shell
    2024-10-08 19:57:53.495 tid=3790142 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/xxx failed, errno 2.

    2024-10-08 19:57:53.495 tid=3790142 VotingDisk FATAL: Init voting disk failed!
    ```

    `errno 2` indicates that the disk device cannot be found.

2. Execute `lsscsi -is` to inspect device identifiers. Observe that the corresponding disk device is missing from the list.

## Root Cause

The disconnection of the disk array prevents the resource pool deployment cluster from starting or restarting.

## Solution

Perform the following steps to resolve the issue:

**As the `root` user:**

1. Inspect the disk array connection status:

    ```bash
    iscsiadm -m node
    ```

2. Terminate all disk array sessions:

    ```bash
    iscsiadm -m node --logoutall=all
    ```

3. Establish a new connection to the disk array:

    ```bash
    iscsiadm -m node -p xx.xx.xx.xxx -l
    ```

4. Rescan the SCSI bus to detect the devices:

    ```bash
    rescan-scsi-bus.sh
    ```

    At this stage, executing `lsscsi -is` should reveal that the corresponding disk device is now visible.

**As the database runtime user:**

5. Start the cluster and verify its status:

    ```bash
    cm_ctl start
    cm_ctl query -Cvipd
    ```

> [!WARNING]
> + The logout command affects all disk arrays attached to the node.
> + Replace `xx.xx.xx.xxx` in the login command with the actual IP address discovered in step 1.
> + After reconnecting to the disk array, ensure that the permissions of the reattached devices are modified to grant access to the database runtime user using `chmod` or `chown`.
