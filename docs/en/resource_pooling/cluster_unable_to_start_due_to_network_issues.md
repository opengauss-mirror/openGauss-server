# Cluster Startup Failure Due to Network Issues

## Symptom

During the startup process of an openGauss resource pool deployment cluster, network issues may cause one or more nodes to fail to start. The root cause is that network connectivity problems prevent the `CM Server` primary node from delivering commands to the `CM Agent` nodes, resulting in the database not receiving the startup instruction. This document uses a firewall-enabled scenario as an example to detail the troubleshooting steps and results.

## Troubleshooting Method

Using the firewall-enabled scenario as an example, after executing `cm_ctl start` to launch the cluster, the second node experiences a failure and cannot be successfully started. Querying via `cm_ctl query -Cvipdw` reveals the state as `Down Starting`. The troubleshooting procedure is as follows:

1. Navigate to the `$GAUSSLOG/pg_log` directory. The absence of any `Gaussdb` startup logs indicates that the startup failure occurred at the CM arbitration layer.

2. Navigate to the `$GAUSSHOME/cm/cm_agent` directory. Use the `zgrep` command to filter for the `restart` keyword in the most recent log files. If no recent `restart` records are found, it can be concluded that the `CM Agent` did not initiate any startup action.

3. Execute the `cm_ctl query -Cvipdw` command to identify the `CM Server` primary node. Navigate to the `$GAUSSHOME/cm/cm_server` directory on that primary node and inspect the most recent logs for the following information:

    ```shell
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: Network base_time:2024-09-26 17:59:33
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-09-26 17:59:32|2024-09-26 17:59:32|
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|1970-01-01 08:00:00|1970-01-01 08:00:00|
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 17:59:30|2024-09-26 17:59:30|1970-01-01 08:00:00|
    ```

    The second line of the aforementioned logs represents the heartbeat message records received by the faulty node. It can be observed that other nodes receive heartbeats from this node, but the faulty node does not receive heartbeats from any other nodes.

4. Furthermore, navigate to the `$GAUSSHOME/cm/cm_agent` directory on a healthy (non-faulty) node. Inspect the most recent logs for the following record:

    ```shell
    2024-09-26 18:59:48.432 tid=1008255 RHB ERROR: [mes] send pipe to dst_inst[1] priority[0] is not ready.
    ```

    This log indicates that the underlying connection between this node and the faulty node cannot be established. The root cause is the inability to establish TCP communication, indicating a link-layer fault.

5. Based on the above analysis, investigate the link-layer cause. Use firewall commands to check the firewall status on the faulty node:

    ```shell
    ● firewalld.service - firewalld - dynamic firewall daemon
    Loaded: loaded (/usr/lib/systemd/system/firewalld.service; enabled; vendor preset: enabled)
    Active: active (dead) since Thu 2024-09-26 16:50:19 CST; 10s ago
        Docs: man:firewalld(1)
    Process: 176150 ExecStart=/usr/sbin/firewalld --nofork --nopid $FIREWALLD_ARGS (code=exited, status=0/SUCCESS)
    Main PID: 176150 (code=exited, status=0/SUCCESS)
    ```

    The output reveals that the root cause of the failed link establishment is the enabled firewall on the faulty node, which blocks the unidirectional traffic from other nodes to this node.

## Root Cause

When the `CM Agent` on a node detects that the local runtime conditions are met, the `cm_ctl start` command triggers the startup of that node, allowing it to join the cluster. Therefore, diagnosing this type of issue requires confirming the following two points:

1. Whether the `CM Agent` has explicit startup behavior.
2. Whether there is explicit successful connection establishment or interruption between the `CM Server` primary node and the other `CM Agent` nodes.

Using the above methodology, the vast majority of cluster node startup failures caused by network issues can be identified.

## Solution

For this type of issue, if it is caused by a system network fault, restore the network configuration and manually restart the faulty node.
