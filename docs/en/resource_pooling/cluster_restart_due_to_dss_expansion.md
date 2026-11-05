# Cluster Restart Due to DSS Expansion

## Symptom

When performing DSS volume expansion, the cluster may enter a persistent `Starting` state. This behavior manifests as follows:

```shell
dsscmd adv -g log0 -v /dev/disk/by-id/wwn-0x6382028100ed96ac0cefc8020000001a
Succeed to add volume online, vg_name is log0, volume path is /dev/disk/by-id/wwn-0x6382028100ed96ac0cefc8020000001a.

[  Datanode State   ]

node            node_ip         instance                                              state
---------------------------------------------------------------------------------------------------------
1  openGauss111 NodeIP_1    6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Standby Starting
2  openGauss135 NodeIP_2    6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
3  openGauss137 NodeIP_3    6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Down    Starting
```

## Root Cause

During the DSS expansion phase, the newly added storage volumes are considered unregistered by the Cluster Manager (CM). Consequently, CM detects instances associated with these unregistered resources and initiates a re-registration process for them. This process causes the cluster nodes to stall in the `Starting` state.

## Solution

1. If the cluster remains stuck in the `Starting` state, restarting the cluster can expedite the registration of the unregistered instances.
2. Avoid performing DSS expansion operations while business workloads are active, as this procedure will inevitably interrupt services.
