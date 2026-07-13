# 因CM不满足多数派原则导致集群无法使用的问题

## 一、问题现象

对于节点个数大于或等于3的集群来说。如果利用`cm_ctl stop -n 2...N`命令停止多节点，只留有单节点存活那么集群将无法正常使用，且会在如下两种状态反复切换：

状态一：

```shell
[  CMServer State   ]

node            node_ip         instance                                                 state
------------------------------------------------------------------------------------------------
1  openGauss151 NodeIp_1     1    /data/ss_development_install_1/install/cm/cm_server Primary
2  openGauss153 NodeIp_2     2    /data/ss_development_install_1/install/cm/cm_server Down
3  openGauss155 NodeIp_3     3    /data/ss_development_install_1/install/cm/cm_server Down


[ Defined Resource State ]

node            node_ip         res_name instance  state  
----------------------------------------------------------
1  openGauss151 NodeIp_1    dms_res  6001      OnLine 
2  openGauss153 NodeIp_2    dms_res  6002      Deleted
3  openGauss155 NodeIp_3    dms_res  6003      Deleted
1  openGauss151 NodeIp_1    dss      20001     OnLine 
2  openGauss153 NodeIp_2    dss      20002     Deleted
3  openGauss155 NodeIp_3    dss      20003     Deleted

[   Cluster State   ]

cluster_state   : Degraded
redistributing  : No
balanced        : Yes
current_az      : AZ_ALL

[  Datanode State   ]

node            node_ip         instance                                                   state
--------------------------------------------------------------------------------------------------------------
1  openGauss151 NodeIp_1     6001 22290  /data/ss_development_install_1/install/data/dn P Primary Normal
2  openGauss153 NodeIp_2     6002 22290  /data/ss_development_install_1/install/data/dn S Down    Unknown
3  openGauss155 NodeIp_3     6003 22290  /data/ss_development_install_1/install/data/dn S Down    Unknown
```

状态二：

```shell
[  CMServer State   ]

node            node_ip         instance                                                 state
------------------------------------------------------------------------------------------------
1  openGauss151 NodeIp_1     1    /data/ss_development_install_1/install/cm/cm_server Standby
2  openGauss153 NodeIp_2     2    /data/ss_development_install_1/install/cm/cm_server Down
3  openGauss155 NodeIp_3     3    /data/ss_development_install_1/install/cm/cm_server Down

cm_ctl: can't connect to cm_server.
Maybe cm_server is not running, or timeout expired. Please try again.
```

## 二、根因分析

出现上述状态是由于CMS主节点是基于抢锁机制来选主的，与此同时CMS还有另外一套检测机制，如果是大于等于3节点的集群环境，若发现连接CMS主节点的`cm_agent`进程小于等于1那么会强制将当前的CMS主降级为备（这种设计本质上是防止脑裂场景），但是抢锁线程发现当前集群不存在CMS主节点，又会主动去抢锁，以至于集群会在两种状态切换。总体来说，当前集群不可用。

## 三、 解决方案

出现该情况，并没有直接的解决方案，若想集群恢复，只能利用`cm_ctl start -n 1...N`命令启动其他节点，以保证集群不再是单节点运行。
