# Cluster Unusable Due to CM Not Meeting the Majority Principle

## Symptom

In clusters with three or more nodes, executing the `cm_ctl stop -n 2...N` command to terminate multiple nodes—leaving only a single node alive—renders the cluster unusable. The cluster will oscillate repeatedly between the following two states:

**State 1:**

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

**State 2:**

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

## Root Cause

The oscillation between the aforementioned states occurs due to the CM primary election mechanism, which relies on lock preemption. Concurrently, CM implements a protective detection mechanism: in a cluster with three or more nodes, if the number of `cm_agent` processes connected to the CM primary node is less than or equal to 1, the current CM primary is forcibly demoted to standby. This design inherently prevents split-brain scenarios. However, the lock preemption thread, detecting the absence of a CM primary node in the cluster, will proactively attempt to acquire the lock. This interaction causes the cluster to fluctuate continuously between the two states described above, rendering the cluster effectively unusable.

## Solution

There is no direct recovery method for this specific scenario. To restore cluster availability, execute the `cm_ctl start -n 1...N` command to start the remaining nodes, ensuring the cluster is no longer operating as a single node.
