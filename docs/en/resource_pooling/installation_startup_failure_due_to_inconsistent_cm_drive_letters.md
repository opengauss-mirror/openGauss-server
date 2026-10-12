# c

## Symptom

During the OM installation of a resource pool environment, the cluster fails to start during the startup phase.

## Troubleshooting Method

Check the node heartbeat status using the `cm_ctl show` command. The output is as follows:

```shell
[  Network Connect State  ]

Network timeout:       180s
Current CMServer time: 2025-03-15 10:24:27
Network stat('Y' means connected, otherwise 'N'):
|  \  |  Y  |  Y  |
|  Y  |  \  |  Y  |
|  Y  |  Y  |  \  |


[  Node Disk HB State  ]

Node disk hb timeout:    200s
Current CMServer time: 2025-03-15 10:24:28
Node disk hb stat('Y' means connected, otherwise 'N'):
|  N  |  N  |  Y  |
```

If the disk heartbeat status of any node is `N`, verify whether the voting disk paths configured for CM are consistent across all nodes using the following command:

```shell
cm_ctl list --param --server | grep voting_disk_path
```

- If the output displays WWNs or SCSI IDs, compare them directly for consistency.
- If the output displays formats such as `/dev/sda` or symbolic links, retrieve the corresponding WWN or SCSI ID using the following command:

```shell
lsscsi -is | grep sda(obtained disk name)
```

If inconsistencies are detected on any node, reconfigure the parameters.

## Root Cause

Different nodes are using different physical disks for CM operations. This inconsistency causes disk heartbeat read/write anomalies on certain nodes, leading to their eviction from the cluster by CM.

## Solution

Manually modify the CM parameters. If symbolic links are used, adjust their targets to ensure that the disk identifiers are consistent across all nodes.
