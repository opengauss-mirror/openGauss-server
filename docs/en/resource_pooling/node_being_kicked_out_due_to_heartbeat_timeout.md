# Node Eviction Due to Heartbeat Timeout

## Symptom

During the operation of an openGauss resource pool cluster, a node may be evicted from the cluster.

Checking the cluster status using `cm_ctl query -Cvipdw` reports the following symptoms:

1. A specific node shows the database status as `P Down    Unknown` and the `CM Server` status as `Down`.
2. All databases in the cluster appear to be `Normal`, but logs indicate that a node has restarted.

## Troubleshooting Method

For **Symptom 1**, use the `cm_ctl query -Cvipdw` command directly to observe the cluster status as an auxiliary diagnostic method.

For **Symptom 2**, navigate to the `$GAUSSLOG/pg_log` directory on each node to inspect the logs and verify whether a restart has occurred.

Once the above symptoms are confirmed, follow these steps to determine if the node eviction was caused by a heartbeat timeout:

1. Taking **Node 1** as an example of the faulty node, navigate to the `$GAUSSLOG/cm/cm_server` directory on the **CM Server Primary** node. Locate the most recent `key_event` log file. You will find records similar to the following:

    ```shell
    tid=3847747 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    tid=3847747 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
    ```

    These logs confirm that Node 1 was evicted and subsequently rejoined the cluster. The next step is to determine why Node 1 was evicted.

2. Still within the `$GAUSSLOG/cm/cm_server` directory on the **CM Server Primary** node, locate the logs corresponding to the eviction timestamp (you can search directly for the keyword `kick out`). Look for records similar to the following:

    ```shell
    tid=3847747 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    tid=3847747 MaxClusterAb LOG: Network timeout:6
    tid=3847747 MaxClusterAb LOG: Network base_time:2024-10-11 09:52:44
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-10-11 09:52:35|2024-10-11 09:52:35|
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |2024-10-11 09:52:34|1970-01-01 08:00:00|2024-10-11 09:52:43|
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |2024-10-11 09:52:33|2024-10-11 09:52:43|1970-01-01 08:00:00|
    ```

    As shown above, the bidirectional heartbeat matrix indicates a link timeout exceeding the threshold of `6s` between two nodes. Therefore, the eviction was caused by a heartbeat link timeout.

## Root Cause

Node evictions caused by heartbeat timeouts are invariably related to network issues, specifically network flaps or Network Interface Card (NIC) restarts.

To investigate such issues, first check `/var/log/messages` or network-specific logs for NIC up/down events or packet loss during the incident timeframe.

If no conclusive evidence is found in the system logs, enable `DEBUG` logging for the `CM` components in the same environment. After the issue recurs, navigate to the `$GAUSSLOG/cm/cm_agent` directory on the evicted node. Locate the logs corresponding to the timestamp, filter for the `RHB` keyword, and inspect for packet transmission or reception failures caused by `TCP` connection anomalies. This information serves as strong evidence to continue troubleshooting the network layer.

