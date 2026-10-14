# Node Eviction Due to Storage Array NIC Failure

## Symptom

This section uses a one-primary-one-standby resource pool cluster as an example to simulate a storage array NIC failure scenario, illustrating the cluster fault handling logic under these conditions.<br/>
**Reproduction Steps:**<br/>
Identify the storage array NIC IP address:

```shell
[root@openGaussnode1 ~]# cat /etc/init.d/after.local
iscsiadm -m node -p xx.xx.xx.xx -l
[root@openGaussnode1 ~]# iscsiadm -m node
xx.xx.xx.xx:3260,1 iqn.2006-08.com.huawei:oceanstor:21008446fe44c4af::20000:xx.xx.xx.x
```

Identify the corresponding NIC device name for the IP address:

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

Bring the NIC down:

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

## Troubleshooting Method

**Check CM Server Key Event Logs**:
Navigate to `$GAUSSLOG/cm/cm_server/key_event-xxx.log` on the **CM Server Primary** node. Search for the keyword `kick out` to identify the timestamp of the node eviction.

```
[ctt_ltt@openGaussnode2 cm_server]$ vim key_event-2024-10-21_102457-current.log
2024-10-21 11:46:33.189 tid=459338 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-21 11:47:28.398 tid=459338 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
```

**Analyze CM Server Logs**:
Check `$GAUSSLOG/cm/cm_server/cm_server-xxx.log` around the eviction timestamp. Look for `res inst manual stop or report timeout`, indicating the resource instance was stopped or became unresponsive.

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

**Inspect CM Agent Logs on the Evicted Node**:
Check `$GAUSSLOG/cm/cm_agent/cm_agent-xxx.log` on the evicted node. Look for registration failures. Persistent `reg failed` messages for DSS indicate an inability to access the storage array.

```
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: res(dss) inst(20001) has been restart (2) times, restart more than (5) time will manually stop.
2024-10-21 11:46:24.657 tid=145445 StartAndStop LOG: [StartResourceCheck], it takes 96 s.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: Write data
Î^Ug to dev /dev/disk/by-id/scsi-368446fe10044c4afb4cdf44e000000dd failed, dataLen 512 size -1 offset 20971520 errno 5.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: [SetVotingDiskData] update data to disk failed.
2024-10-21 11:46:24.675 tid=145449 VotingDisk ERROR: update voting disk status failed, expiredTime=135, diskTimeout=200
2024-10-21 11:46:25.node1 tid=145440 ERROR: failed to connect to datanode:/data/ctt_ltt/omm/openGauss/dn1
2024-10-21 11:46:25.node1 tid=145440 ERROR: connection return errmsg : could not receive data from server, error: Connection reset by peer, remote

**Verify System Kernel Logs**:
Check `/var/log/messages` for NIC state changes. The message `[NIC]Netdev is down` confirms the NIC failure.

```shell
[root@openGaussnode1 ~]# grep " hinic" /var/log/messages
Oct 21 11:44:09 openGaussnode1 kernel: [246531.145454] hinic 0000:83:00.0 enp131s0: [NIC]Netdev is down
Oct 21 11:47:11 openGaussnode1 kernel: [246713.532202] hinic 0000:83:00.0 enp131s0: [NIC]Finally num_qps: 16, num_rss: 16
Oct 21 11:47:12 openGaussnode1 kernel: [246713.622446] hinic 0000:83:00.0 enp131s0: [NIC]Netdev is up
```

**Check NIC Status**:
Use `ethtool` to verify the link status. `Link detected: no` confirms the physical layer disconnection.

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

## Root Cause

Manually bringing down the storage array NIC prevented the node's Cluster Manager (CM) from reading or writing critical data (such as voting disk information) stored on the shared storage for an extended period. Consequently, the node failed the disk heartbeat checks and was evicted from the cluster to maintain overall cluster integrity.

## Solution

Bring the NIC back up using the `ifconfig enp131s0 up` command. Use `ethtool` to ensure `Link detected: yes`. Then start the evicted node using `cm_ctl start`.

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

If the failure is due to physical NIC damage or a storage array hardware fault preventing CM and DSS from accessing the storage, contact your system administrator to resolve the hardware issue before performing the recovery steps above.
