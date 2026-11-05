# Cluster Unable to Start Due to Not Meeting the Majority Principle

## Symptom

When starting a single node in an openGauss resource pool deployment cluster, the node appears to start successfully. However, executing `cm_ctl query -Cvipdw` reveals that the cluster remains unavailable and the started node is only in the `Standby` state. The output resembles the following:

```shell
[  CMServer State   ]

node            node_ip         instance                                                state
-----------------------------------------------------------------------------------------------
1  openGauss111 NodeIP_1    1    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby
2  openGauss135 NodeIP_2    2    /usr2/test_0925_install/omm/openGauss/cm/cm_server Down
3  openGauss137 NodeIP_3    3    /usr2/test_0925_install/omm/openGauss/cm/cm_server Down

cm_ctl: can't connect to cm_server.
Maybe cm_server is not running, or timeout expired. Please try again.
```

After a single node is started, the cluster remains unavailable, and the only online node is a standby node.

## Troubleshooting Method

Execute `cm_ctl query -Cvipdw` to inspect the cluster status. Observe whether the number of online nodes satisfies the majority principle. Specifically, verify whether the following condition holds true:

`(Number of Online Nodes) + 1 > (Total Number of Nodes) / 2`

If this condition is not met, the cluster cannot function normally.

## Root Cause

The number of online nodes in the cluster is insufficient to form a quorum, thereby violating the majority principle required for cluster availability.

## Solution

Ensure that the number of online nodes meets the majority requirement. Once the quorum is restored, the cluster will be able to start and operate normally.
