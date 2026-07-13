# 因执行pause导致仲裁不响应的问题

## 一、问题现象

1. 集群无法启动，或failover无法进行（无法选出主节点）。使用`cm_ctl query -Cvipdw`命令查询，Cluster State下pausing状态为Yes，类似于如下显示：

    ```shell
    [   Cluster State   ]

    cluster_state   : Normal
    redistributing  : No
    balanced        : Yes
    current_az      : AZ_ALL
    pausing         : Yes
    ```

## 二、定位方法

1. 当遇到集群无法启动或者failover无法正常进行的情况下，使用`cm_ctl query -Cvipdw`命令查询，Cluster State下pausing状态为Yes。

2. 查询CM的cm_client日志，搜索pause关键字，观察是否执行过`cm_ctl pause`命令。

    ```shell
    2024-10-08 16:15:33.189 tid=929054  DEBUG1: ip: "20.20.20.111", cmd: "cm_ctl pause".
    ```

## 三、问题根源

当前集群处于pause状态，集群不响应仲裁。

## 四、解决方案

使用cm_ctl resume命令恢复集群。
