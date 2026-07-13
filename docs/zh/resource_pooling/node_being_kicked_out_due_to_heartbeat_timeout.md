# 因心跳超时导致节点被踢出的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，可能出现某个集群被踢出的情况。

当使用`cm_ctl query -Cvipdw`查询集群状态，可能出现以下情况：

1. 存在某个节点数据库状态为`P Down    Unknown`，`CM Server`状态为`Down`。
2. 存在集群各数据库状态为`Normal`，但是某个节点发生过重启行为。

## 二、定位方法

针对于上述第一种情况，可直接通过`cm_ctl query -Cvipdw`查询集群状态，进行辅助判断。

针对于第二种情况，需要进入各个节点的`$GAUSSLOG/pg_log`下，排查日志记录，查看是否存在重启行为。

当上述表现满足时，可以通过如下步骤，排查是否因心跳超时原因导致节点异常、节点重启：

1. 以异常节点为1节点为例，进入`CM Server`主节点的`$GAUSSLOG/cm/cm_server`目录下，寻找最近时间点的`key_event`日志，可发现如下记录：

    ```shell
    tid=3847747 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    tid=3847747 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
    ```

    说明1节点存在踢出行为，随后又加入集群，需要确定1节点为何被踢出。
2. 相应的，进入`CM Server`主节点的`$GAUSSLOG/cm/cm_server`目录下，寻找该时间点所对应的日志（可以直接搜寻`kick out`关键字），有以下记录：

    ```shell
    tid=3847747 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    tid=3847747 MaxClusterAb LOG: Network timeout:6
    tid=3847747 MaxClusterAb LOG: Network base_time:2024-10-11 09:52:44
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-10-11 09:52:35|2024-10-11 09:52:35|
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |2024-10-11 09:52:34|1970-01-01 08:00:00|2024-10-11 09:52:43|
    tid=3847747 MaxClusterAb LOG: [RHB] hb infos: |2024-10-11 09:52:33|2024-10-11 09:52:43|1970-01-01 08:00:00|
    ```

    如上所示，在双向心跳矩阵中，存在两节点间链接超时，超过阈值`6s`。因此踢出原因为心跳链接超时。

## 三、问题根因

对于因心跳超时导致的踢出的行为均和网络有关，尤其是网络闪断或网卡重启行为。

对于该类问题，首先应该在`/var/log/message`或网络日志中查找问题时间段是否有网卡启停或者网络丢包行为。

如果无上述可靠信息，可在同环境下开启`CM`组件的`DEBUG`日志功能。再次出现该问题后，进入被踢出节点`$GAUSSLOG/cm/cm_agent`目录下，寻找对应时间点的日志，过滤`RHB`关键字，查看是否存在因`TCP`链接异常所导致的包发送或接收失败行为，可佐证继续排查网络问题。
