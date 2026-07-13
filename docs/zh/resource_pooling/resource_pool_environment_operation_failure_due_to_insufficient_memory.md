# 因内存不足导致资源池化环境运行失败的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，主机业务突然中断。使用`cm_ctl query`查看集群状态，发现备机自动升主。

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

经过短暂的恢复后，备机升主完成。

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

## 二、定位方法

检查升主节点的cm_server日志，目录为`$GAUSSLOG/cm/cm_server`，故障时间由于旧主机资源状态上报超时，出现kick out。

```shell
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: recv node(1) agent report res status msg timeout.
2024-10-17 10:38:38.913 tid=65598 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
```

检查升主节点cm_agent日志，目录为`$GAUSSLOG/cm/cm_agent`，发现升主节点对故障节点执行了IOFence，进行资源隔离。

```shell
2024-10-17 10:38:39.460 tid=64601 ProcessCmsMsg LOG: [UnregOneResInst]: cmd:(/data/ctt_ltt/omm/openGauss/gauss/app/bin/dss_contrl.sh -unreg 0 /data/ctt_ltt/omm/openGauss/dss_home /data/ctt_ltt/omm/openGauss/dn1) is executing.
```

故障节点重启恢复后，使用dmesg命令检查操作系统，发现存在内存满现象，因此可以认定是旧主机内存耗尽，导致资源状态上报超时，被新节点认定异常，触发了failover。

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

## 三、问题根因

资源池化支持对各类资源进行监控，当某个节点遇到异常时，会由于心跳超时/进程状态异常/网络断连等各种故障原因被踢出集群，同时也会对故障节点进行IOFence，避免共享磁盘的数据被异常损坏。

## 四、解决方案

对于该问题，建议在生产环境增加对内存使用情况的监控，超过指定阈值及时进行报警，避免出现内存耗尽导致节点踢出，业务中断的问题。
