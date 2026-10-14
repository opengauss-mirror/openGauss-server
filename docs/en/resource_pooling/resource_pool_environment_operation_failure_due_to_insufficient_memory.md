# Resource Pool Environment Operation Failure Due to Insufficient Memory

## Symptom

During the operation of an openGauss resource pool cluster, the primary node's services suddenly interrupt. Using `cm_ctl query` reveals that a standby node has automatically promoted itself to primary.

```shell
[ctt_ltt@openGauss171 dn_6002]$ cm_ctl query -Cvipd
[  CMServer State   ]

node            node_ip         instance                                      state
-------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  1    /data/ctt_ltt/omm/openGauss/cm/cm_server Down
2  openGauss171 xx.xx.xx.xx  2    /data/ctt_ltt/omm/openGauss/cm/cm_server Primary


[ Defined Resource State ]

node            node_ip         res_name instance  state
----------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  dms_res  6001      Deleted
2  openGauss171 xx.xx.xx.xx  dms_res  6002      OnLine
1  openGauss169 xx.xx.xx.xx  dss      20001     Deleted
2  openGauss171 xx.xx.xx.xx  dss      20002     OnLine

[   Cluster State   ]

cluster_state   : Unavailable
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[  Datanode State   ]

node            node_ip         instance                                    state            | node            node_ip         instance                                    state
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  6001 3500   /data/ctt_ltt/omm/openGauss/dn1 P Down    Unknown | 2  openGauss171 xx.xx.xx.xx  6002 3500   /data/ctt_ltt/omm/openGauss/dn1 S Standby Promoting
```

After a brief recovery period, the promotion completes:

```shell
[ctt_ltt@openGauss171 ~]$ cm_ctl query -Cvipd
[  CMServer State   ]

node            node_ip         instance                                      state
-------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  1    /data/ctt_ltt/omm/openGauss/cm/cm_server Down
2  openGauss171 xx.xx.xx.xx  2    /data/ctt_ltt/omm/openGauss/cm/cm_server Primary


[ Defined Resource State ]

node            node_ip         res_name instance  state
----------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  dms_res  6001      Deleted
2  openGauss171 xx.xx.xx.xx  dms_res  6002      OnLine
1  openGauss169 xx.xx.xx.xx  dss      20001     Deleted
2  openGauss171 xx.xx.xx.xx  dss      20002     OnLine

[   Cluster State   ]

cluster_state   : Degraded
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[  Datanode State   ]

node            node_ip         instance                                    state            | node            node_ip         instance                                    state
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  6001 3500   /data/ctt_ltt/omm/openGauss/dn1 P Down    Unknown | 2  openGauss171 xx.xx.xx.xx  6002 3500   /data/ctt_ltt/omm/openGauss/dn1 S Primary Normal
```

## Troubleshooting Method

1.  **Inspect cm_server logs on the promoted node** (directory: `$GAUSSLOG/cm/cm_server`). The logs indicate that the old primary was evicted due to a resource status report timeout:

```shell
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: recv node(1) agent report res status msg timeout.
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
```

2.  **Inspect cm_agent logs on the promoted node** (directory: `$GAUSSLOG/cm/cm_agent`). The logs show that the new primary executed an IOFence operation against the faulty node to isolate resources:

```shell
2024-10-17 10:38:39.460 tid=64601 ProcessCmsMsg LOG: [UnregOneResInst]: cmd:(/data/ctt_ltt/omm/openGauss/gauss/app/bin/dss_contrl.sh -unreg 0 /data/ctt_ltt/omm/openGauss/dss_home /data/ctt_ltt/omm/openGauss/dn1) is executing.
```

3.  **Inspect the OS kernel logs on the faulty node**. After the node restarts and recovers, use the `dmesg -T` command. The output reveals signs of an Out-Of-Memory (OOM) event, confirming that memory exhaustion on the old primary caused the report timeout and subsequent failover:

```shell
[root@openGauss169 log]# dmesg -T
[Thu Oct 17 10:48:13 2024] Booting Linux on physical CPU 0x0000080000 [0x481fd010]
[Thu Oct 17 10:48:13 2024] Linux version 4.19.90-2003.4.0.0036.oe1.aarch64 (abuild@obs-worker-003) (gcc version 7.3.0 (GCC)) #1 SMP Mon Mar 23 19:06:43 UTC 2020
[Thu Oct 17 10:48:13 2024] efi: Getting EFI parameters from FDT:
[Thu Oct 17 10:48:13 2024] efi: EFI v2.70 by EDK II
[Thu Oct 17 10:48:13 2024] efi:  SMBIOS 3.0=0x2f760000  ACPI 2.0=0x2f7f0000  MEMATTR=0x2956b018  ESRT=0x295e2798  MEMRESERVE=0x2956ce18
[Thu Oct 17 10:48:13 2024] esrt: Reserving ESRT space from 0x00000000295e2798 to 0x00000000295e27d0.
[Thu Oct 17 10:48:13 2024] kexec_core: Reserving 256MB of low memory at 1792MB for crashkernel (System low RAM: 2047MB)
[Thu Oct 17 10:48:13 2024] crashkernel reserved: 0x000020bfc0000000 - 0x000020c000000000 (1024 MB)
[Thu Oct 17 10:48:13 2024] cma: Failed to reserve 512 MiB
```

## Root Cause

The resource pool architecture supports comprehensive resource monitoring. When a node encounters an exception—such as heartbeat timeouts, process failures, or network disconnections—it is evicted from the cluster. Additionally, the cluster performs IOFencing on the faulty node to prevent data corruption on shared storage. In this case, memory exhaustion on the primary node prevented it from reporting its status to the CM Server, triggering the eviction and failover mechanism.

## Solution

It is recommended to implement memory usage monitoring in production environments. Configure alerts to trigger when memory usage exceeds a predefined threshold. Proactive monitoring helps prevent node evictions and business interruptions caused by memory exhaustion.
