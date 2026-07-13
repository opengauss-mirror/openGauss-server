# 因集群重启导致主节点变更的问题

## 一、问题现象

利用CM关闭集群再重新启动以后，发现主节点发生了变更。

## 二、定位方法

主节点发生了更改，一定是集群发生了仲裁，故需要看`cm_server`日志（CM）去查看发生仲裁的原因：

1. 进入`$GAUSSLOG/cm/cm_server`下的`key-event`日志，查看`kick out`关键字，并且确认是否是原主被kick out又加回集群，日志如下：

    ```shell
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 09:33:23.992 tid=1279030 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) join in cluster.]
    ```

2. 记录该次原主被kick out的时间，进入同级目录下的cm_server日志，观察该次原主发生仲裁的原因，以如下日志为例：

    ```shell
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: last(16) is different from current(17), result is 0.
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: node(1) stat (unavail).
    2024-10-10 09:30:40.042 tid=1279030 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
    ```

可以看到，node(1)即原主节点被踢出的原因是某个资源实例有故障导致的，当然导致节点被踢出的原因有很多种，例如，`disk heartbeat timeout`心跳超时，`disconnect with`节点间断连等。

3. 进入`$GAUSSLOG/cm/cm_agent`日志，查看同一时间的日志，去具体判断故障的原因，以该次为例是属于该时间段内无法连到数据库导致，日志如下：

    ```shell
    2024-10-10 09:30:42.574 tid=1277198  ERROR: failed to connect to datanode:/usr2/test_0925_install/omm/openGauss/dn1
    ```

## 三、问题根因

通过CM重启集群，如果发生了主节点更改的状况，一定是属于异常情况，通常认为是在这个过程中，集群发生了仲裁，原主节点发生了短暂的故障，而后故障恢复又以备机的角色被重新加回集群，需要通过具体日志排查，何种原因导致的原主节点故障。

## 四、解决方案

利用`cm_ctl switchover -a`将集群恢复到原始状态。
