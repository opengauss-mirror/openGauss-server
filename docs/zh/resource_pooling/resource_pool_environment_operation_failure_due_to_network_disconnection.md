# 因节点间网络闪断导致资源池化环境运行失败的问题

## 一、问题现象

假设资源池化集群中，主机和其它节点的节点间网络，突然出现了网络闪断的情况。具体表现为资源池化集群突然出现业务中断，随即主机被踢出集群，备机升主之后，频繁出现旧主节点加入集群，又被踢出集群的现象。每次旧主节点加入集群的过程，都会导致业务出现短暂中断。

## 二、定位方法

检查cm_server主节点的日志，目录为`$GAUSSLOG/cm/cm_server`，搜索kick out关键字如下：

```shell
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: recv node(1) agent report res status msg timeout.
2024-10-18 16:32:03.475 tid=470394 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
```

通过以上日志可以看出，旧主节点被踢出是因为对应节点的cm_agent上报状态超时，可以说明旧主节点与cm_server主节点的网络连接存在问题。

## 三、问题根因

资源池化环境存在3层网络：业务网络、存储网络和节点间网络。当前CM能够监控存储网络和节点间网络，对于以上网络发生异常时，都会上报异常状态，并踢出故障节点。当业务网络发生异常时，业务会因为无法连接出现大量超时，数据库本身运行无影响。

## 四、解决方案

对于因为网络故障导致的资源池化集群异常，解决网络故障后，数据库集群一般可以恢复。如果因为达到了最大重试次数没有自动拉起，管理员使用`cm_ctl start -n x`命令拉起对应节点即可。
