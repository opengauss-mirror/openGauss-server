# Primary Node Change Caused by Cluster Restart

## Symptom

After stopping and restarting the cluster using CM, the primary node role has changed to a different node.

## Troubleshooting Method

Since a primary change indicates that arbitration occurred within the cluster, inspect the `cm_server` logs to determine the cause of the failover.

1.  Navigate to the `$GAUSSLOG/cm/cm_server` directory and inspect the `key-event` logs for the `kick out` keyword. Verify whether the original primary node was evicted and subsequently rejoined the cluster. Example logs:

    ```shell
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 09:33:23.992 tid=1279030 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
    ```

2.  Record the timestamp of the eviction. Then, inspect the `cm_server` logs within the same directory to identify the reason for the arbitration. In the example below, the original primary was evicted because a resource instance was manually stopped or reported a timeout:

    ```shell
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: last(16) is different from current(17), result is 0.
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: node(1) stat (unavail).
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
    ```

Common causes for node eviction include `disk heartbeat timeout`, network flaps (`disconnect with`), or other resource instance failures.

3.  Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the logs corresponding to the same timestamp to pinpoint the specific fault. In this case, the failure was due to the inability to connect to the database instance during that window:

    ```shell
    2024-10-10 09:30:42.574 tid=1277198  ERROR: failed to connect to datanode:/usr2/test_0925_install/omm/openGauss/dn1
    ```

## Root Cause

A primary node change following a cluster restart via CM is considered an abnormal event. Typically, this occurs because the original primary experienced a transient failure during the restart process, triggering cluster arbitration. Once the underlying issue resolved, the node successfully rejoined the cluster as a standby. Detailed log analysis is required to determine the exact trigger for the original primary's failure.

## Solution

Restore the cluster to its original topology by running the `cm_ctl switchover -a` command.
