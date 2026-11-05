# Cluster Unable to Start Due to Lack of Permissions for the Voting Disk

## Symptom

1. The cluster fails to start normally. Executing the `cm_ctl query -Cvipdw` command reveals that a specific node remains in the `Down` state:

    ```shell
    [  Datanode State   ]

    node            node_ip         instance                                              state
    ---------------------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Down    Unknown
    2  openGauss135 NodeIP_2    6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
    3  openGauss137 NodeIP_3    6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
    ```

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and open the current `cm_agent` log file.

2. Locate the following error entries in the log:

    ```
    2024-10-09 10:24:19.659 tid=695385 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/disk/by-id/scsi-36382028100ed96ac46c91b0d0000005b failed, errno 13.
    2024-10-09 10:24:19.659 tid=695385 VotingDisk FATAL: Init Voting disk failed!
    ```

    The presence of `VotingDisk` error records indicates an issue with the voting disk. Notably, `errno 13` signifies `Permission denied`, a common error occurring when attempting to access a resource without sufficient privileges.

3. Inspect the XML configuration file used during installation. Search for the `votingDiskPath` keyword to identify the configured voting disk. Subsequently, execute the `lsscsi -is` and `ll /dev` commands to verify the permissions and ownership of the corresponding voting disk device. If the ownership does not match the current database runtime user, the startup failure is attributed to altered voting disk permissions.

## Root Cause

The voting disk is configured during the initial installation of a resource pool deployment. A dedicated portion of shared storage is allocated as the voting disk for CM-based disk health detection. Each node's `cm_agent` periodically writes heartbeats to this disk. The `cm_server` reads these heartbeats to monitor node health. If the `cm_server` fails to read a heartbeat within the specified threshold, the node is deemed abnormal. Consequently, if a node lacks the necessary permissions to access the voting disk, it cannot write heartbeats, leading to a startup failure.

## Solution

Ensure the voting disk configuration is correct during installation. Verify that the ownership of the voting disk belongs to the database runtime user. If the ownership has been modified, restore it immediately using the `chown` command.
