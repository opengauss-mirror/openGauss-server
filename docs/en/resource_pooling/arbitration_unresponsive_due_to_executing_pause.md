# Arbitration Unresponsive Due to Executing Pause

## Symptom

1. The cluster fails to start, or failover cannot proceed (no primary node can be elected). Execute `cm_ctl query -Cvipdw`. Under the `Cluster State` section, the `pausing` status displays `Yes`, as shown below:

    ```shell
    [   Cluster State   ]

    cluster_state   : Normal
    redistributing  : No
    balanced        : Yes
    current_az      : AZ_ALL
    pausing         : Yes
    ```

## Troubleshooting Method

1. If the cluster fails to start or failover cannot proceed normally, execute `cm_ctl query -Cvipdw`. Verify whether the `pausing` status under `Cluster State` is `Yes`.

2. Inspect the CM `cm_client` logs. Search for the keyword `pause` to determine whether the `cm_ctl pause` command was previously executed.

    ```shell
    2024-10-08 16:15:33.189 tid=929054  DEBUG1: ip: "20.20.20.111", cmd: "cm_ctl pause".
    ```

## Root Cause

The cluster is currently in the paused state and does not respond to arbitration.

## Solution

Execute the `cm_ctl resume` command to restore the cluster.


