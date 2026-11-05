# Node Eviction Due to Report Timeout

## Symptom

During the operation of an openGauss resource pool cluster, the `cm_agent` on a specific node fails to report its status to the `cm_server` primary node due to a NIC failure. This triggers a `report timeout` error, leading to the node being evicted from the cluster and causing an abnormal cluster state.

## Troubleshooting Method

Follow this procedure to diagnose the issue:

1.  Use the `cm_ctl query -Cvipdw` command to observe the cluster status. In this example, Node 2 is in the `Down Unknown` state, indicating that the node has failed and gone offline.
2.  Log in to the faulty node. Navigate to the `$GAUSSLOG/pg_log` directory and inspect the most recent run log. You will find records similar to the following:

    ```shell
    2024-09-26 20:11:48.859 66f54e2d.5266 postgres 140030261589760 cm_agent 0 dn_6001_6002_6003 57P01  0 [BACKEND] FATAL:  terminating connection due to administrator command
    ```

    This log indicates that the database received a shutdown command from the `cm_agent`. Therefore, inspect the `cm_agent` logs to determine why the shutdown signal was issued.
3.  Navigate to the `$GAUSSHOME/cm/cm_agent` directory on the faulty node and inspect the logs around the timestamp of the failure. You will find entries similar to the following:

    ```shell
    2024-09-26 20:11:48.734 tid=9986  LOG: nic not running, immediate shutdown nodes.
    2024-09-26 20:11:48.734 tid=9986  LOG: immediate_shutdown_nodes, 2 resource will be stopped.
    2024-09-26 20:11:48.846 tid=9987  LOG: 394 cm_agent connect to cm_server start.
    ```

    The explicit `nic error` message confirms that the `CM` component detected a NIC failure.
4.  On the **CM Server Primary** node, navigate to the `$GAUSSHOME/cm/cm_server` directory and inspect the `key_event` logs. You will find records similar to the following:

    ```shell
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(2) kick out.]
    ```

   This confirms the eviction of the faulty node. Inspecting the `cm_server` logs around this timestamp reveals the specific reason:
 
    ```shell
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: Network timeout:6
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: Network base_time:2024-09-26 20:11:49
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-09-26 20:11:44|2024-09-26 20:11:48|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 20:11:43|1970-01-01 08:00:00|2024-09-26 20:11:42|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 20:11:48|2024-09-26 20:11:44|1970-01-01 08:00:00|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: last(39) is different from current(40), result is 0.
    2024-09-26 20:11:49.677 tid=5045 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(2) kick out.]
    2024-09-26 20:11:49.677 tid=5045 MaxClusterAb LOG: recv node(2) agent report res status msg timeout.
    ```

    These logs confirm that the root cause of the eviction is a `report timeout`, which was ultimately triggered by a network fault on the faulty node leading to an unexpected restart.

## Root Cause

Under normal circumstances, the `cm_agent` periodically reports status messages to the `cm_server`. If the `cm_server` fails to detect successful status reports within the threshold, it proactively evicts the node. Simultaneously, the `cm_agent` detects the NIC failure and initiates an immediate shutdown of the database resources.

## Solution

This issue is caused by a bidirectional network failure. Check the NIC status on the faulty node and restore network connectivity to resolve the issue.