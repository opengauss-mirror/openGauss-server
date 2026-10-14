# Resource Pool Environment Operation Failure Due to Network Flap

## Symptom

Assume a resource pool cluster where the interconnect network between the primary node and other nodes experiences a sudden network flap. The typical symptom is a sudden business interruption in the resource pool cluster, immediately followed by the primary node being evicted. After a standby node promotes to primary, the old primary repeatedly attempts to rejoin the cluster but is subsequently evicted again. Each time the old primary attempts to join, it causes a brief interruption to ongoing business operations.

## Troubleshooting Method

Inspect the logs on the **CM Server Primary** node located in `$GAUSSLOG/cm/cm_server`. Search for the keyword `kick out`:

```shell
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: recv node(1) agent report res status msg timeout.
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
```

The logs indicate that the old primary was evicted because the `cm_agent` on that node failed to report its status to the CM Server within the threshold. This confirms a network connectivity issue between the old primary and the CM Server primary.

## Root Cause

The resource pool architecture relies on three distinct networks: the **service network**, the **storage network**, and the **interconnect network**. Currently, CM monitors both the storage network and the interconnect network. Any abnormality detected on these networks triggers an exception report and results in the eviction of the faulty node.

In contrast, if an anomaly occurs on the **service network**, it leads to a large number of connection timeouts for business applications, but the database processes themselves generally remain unaffected. In this case, the interconnect network flap triggered the eviction logic.

## Solution

Once the underlying network fault is resolved, the database cluster typically recovers automatically. If the node fails to restart automatically after reaching the maximum retry limit, the administrator can manually restart the specific node using the `cm_ctl start -n x` command.
