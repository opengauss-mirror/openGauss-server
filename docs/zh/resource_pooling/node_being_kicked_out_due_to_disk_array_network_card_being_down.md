# 因磁阵网卡down导致节点被踢出的问题

## 一、问题现象

本文以一主一备的资源池化集群为例，模拟磁阵网卡down的场景，以此说明资源池化集群在该场景的故障处理逻辑。<br/>
复现流程如下，以down主节点磁阵网卡为例：<br/>
查看磁阵网卡ip。

```shell
[root@openGaussnode1 ~]# cat /etc/init.d/after.local
iscsiadm -m node -p xx.xx.xx.xx -l
[root@openGaussnode1 ~]# iscsiadm -m node
xx.xx.xx.xx:3260,1iqn.2006-08.com.huawei:oceanstor:21008446fe44c4af::20000:xx.xx.xx.x
```

查看对应ip的网卡名。

```shell
[root@openGaussnode1 ~]# ifconfig
enp131s0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST> mtu 1500
inet 8.92.0.node1 netmask xx.xx.xx.xx broadcast xx.xx.xx.xxx
inet6 fe80::d3e6:fdfb:45be:8c17 prefixlen 64 scopeid 0x20
ether ac:b3:b5:1f:5f:ce txqueuelen 1000 (Ethernet)
RX packets 547543331 bytes 2689392112232 (2.4 TiB)
RX errors 0 dropped 0 overruns 0 frame 0
TX packets 533210687 bytes 51386691742 (47.8 GiB)
TX errors 0 dropped 0 overruns 0 carrier 0 collisions 0
```

执行网卡down操作。

```shell
ifconfig enp131s0 down
```

等待集群稳定后，查看集群状态，此时主节点被踢出，备节点升主。

```shell
[ctt_ltt@openGaussnode1 ~]$ cm_ctl query -Cvipd
[ CMServer State ]

node node_ip instance state
1 openGaussnode1 172.168.90.node1 1 /data/ctt_ltt/omm/openGauss/cm/cm_server Standby
2 openGaussnode2 172.168.90.node2 2 /data/ctt_ltt/omm/openGauss/cm/cm_server Primary

[ Defined Resource State ]

node node_ip res_name instance state
1 openGaussnode1 172.168.90.node1 dms_res 6001 Deleted
2 openGaussnode2 172.168.90.node2 dms_res 6002 OnLine
1 openGaussnode1 172.168.90.node1 dss 20001 Deleted
2 openGaussnode2 172.168.90.node2 dss 20002 OnLine

[ Cluster State ]

cluster_state : Degraded
redistributing : No
balanced : No
current_az : AZ_ALL

[ Datanode State ]

node node_ip instance state | node node_ip instance state
1 openGaussnode1 172.168.90.node1 6001 3500 /data/ctt_ltt/omm/openGauss/dn1 P Down Manually stopped | 2 openGaussnode2 172.168.90.node2 6002 3500 /data/ctt_ltt/omm/openGauss/dn1 S Primary Normal
```

## 二、定位方法

查看cm_server的key event日志，日志所在路径：`$GAUSSLOG/cm/cm_server/key_event-xxx.log`，可以发现11:46:33.189主节点被踢出。

```
[ctt_ltt@openGaussnode2 cm_server]$ vim key_event-2024-10-21_102457-current.log
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-21 11:47:28.398 tid=459338 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
```

查看对应时间段的cm_server日志，日志所在路径：`$GAUSSLOG/cm/cm_server/cm_server-xxx.log`，可以发现原主节点`res inst manual stop or report timeout`，即为被主动停止或响应超时。

```
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb LOG: node(1) stat (unavail).
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb LOG: cms will set key(/ctt_ltt/CM/CMServer/Cluster) value([{"version":"2", "nodes": [{"id":2}]}]) to ddb.
2024-10-21 11:46:33.836 tid=459328 AGENT_WORKER LOG: isreg status list, version:34.
2024-10-21 11:46:33.836 tid=459328 AGENT_WORKER LOG: res(dms_res) isreg list: 6001:not_support, 6002:not_support,
2024-10-21 11:46:33.836 tid=459328 AGENT_WORKER LOG: res(dss) isreg list: 20001:reg, 20002:reg,
2024-10-21 11:46:33.843 tid=459328 AGENT_WORKER LOG: [CLIENT] release lock success. key=/ctt_ltt/CM/LockOwner/dms_res/dms_reformer_lock, value=6001.
```

查看原主节点被踢出前的cm_agent日志，日志所在路径`$GAUSSLOG/cm/cm_agent/cm_agent-xxx.log`，检索报错信息，可以发现原主节点始终无法获取磁阵中的数据，可能是磁阵故障或网卡故障所致。

```
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: res(dss) inst(20001) has been restart (2) times, restart more than (5) time will manually stop.
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: [StartResourceCheck], it takes 96 s.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: Write data
Î^Ug to dev /dev/disk/by-id/scsi-368446fe10044c4afb4cdf44e000000dd failed, dataLen 512 size -1 offset 20971520 errno 5.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: [SetVotingDiskData] update data to disk failed.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: update voting disk status failed, expiredTime=135, diskTimeout=200
2024-10-21 11:46:25.node1 tid=145440 ERROR: failed to connect to datanode:/data/ctt_ltt/omm/openGauss/dn1
2024-10-21 11:46:25.node1 tid=145440 ERROR: connection return errmsg : could not receive data from server, error: Connection reset by peer, remote datanode: (null)
```

查看系统信息，其中`enp131s0: [NIC]Netdev is dow`即为网卡down的日志信息，可以通过网卡名识别出是磁阵网卡down。

```shell
[root@openGaussnode1 ~]# grep " hinic" /var/log/messages
Oct 21 11:44:09 openGaussnode1 kernel: [246531.145454] hinic 0000:83:00.0 enp131s0: [NIC]Netdev is down
Oct 21 11:47:11 openGaussnode1 kernel: [246713.532202] hinic 0000:83:00.0 enp131s0: [NIC]Finally num_qps: 16, num_rss: 16
Oct 21 11:47:12 openGaussnode1 kernel: [246713.622446] hinic 0000:83:00.0 enp131s0: [NIC]Netdev is up
```

查看网卡状态，从`Link detected: no`可知该网卡连接断开。

```shell
[root@openGaussnode1 ~]# ethtool enp131s0
Settings for enp131s0:
Supported ports: [ FIBRE ]
Supported link modes: 10000baseKR/Full
25000baseCR/Full
25000baseKR/Full
Supported pause frame use: Symmetric
Supports auto-negotiation: No
Supported FEC modes: Not reported
Advertised link modes: 10000baseKR/Full
25000baseCR/Full
25000baseKR/Full
Advertised pause frame use: Symmetric
Advertised auto-negotiation: No
Advertised FEC modes: Not reported
Speed: 10000Mb/s
Duplex: Full
Port: FIBRE
PHYAD: 0
Transceiver: internal
Auto-negotiation: off
Current message level: 0x00000005 (5)
drv link
Link detected: no
```

## 三、问题根因

本问题的复现场景是由于手动down磁阵网卡导致对应节点的cm长时间无法读写磁阵中保存的投票信息等数据，进而导致被集群踢出。

## 四、解决方案

磁阵网卡down场景可以执行`ifconfig enp131s0 up`启动网卡，然后查看网卡状态`Link detected: yes`即为连接正常。然后执行`cm_ctl start`启动故障节点即可。

```shell
[root@openGaussnode1 ~]# ethtool enp131s0
Settings for enp131s0:
Supported ports: [ FIBRE ]
Supported link modes: 10000baseKR/Full
25000baseCR/Full
25000baseKR/Full
Supported pause frame use: Symmetric
Supports auto-negotiation: No
Supported FEC modes: Not reported
Advertised link modes: 10000baseKR/Full
25000baseCR/Full
25000baseKR/Full
Advertised pause frame use: Symmetric
Advertised auto-negotiation: No
Advertised FEC modes: Not reported
Speed: 10000Mb/s
Duplex: Full
Port: FIBRE
PHYAD: 0
Transceiver: internal
Auto-negotiation: off
Current message level: 0x00000005 (5)
drv link
Link detected: yes
```

如果是由于网卡故障或磁阵故障导致cm和dss无法正常访问磁阵，需要联系系统管理员先排除故障再执行以上操作。
