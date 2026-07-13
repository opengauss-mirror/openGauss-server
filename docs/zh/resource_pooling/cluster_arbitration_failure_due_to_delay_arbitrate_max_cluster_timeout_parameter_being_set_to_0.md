# 因delay_arbitrate_max_cluster_timeout参数设置为0导致集群仲裁失效问题

## 一、问题现象

该问题没有明显的表现，一般来说需要我们通过日志定位，反向筛查集群的仲裁是否失效，一般表现为当我们发现某节点出现明显的问题但是进入`$GAUSSLOG/cm/cm_server`下的`key-event`日志，查看`kick out`关键字，如果无法找到有问题的节点被`kick out`，此时我们怀疑集群的仲裁已经失效。

## 二、定位方法

1. 确认仲裁线程是否失效，一般来说如果仲裁线程正常运行会定时在`$GAUSSLOG/cm/cm_server`下的`cm_server`日志打印如下字样的矩阵：

    ```shell
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: Network base_time:2024-12-20 02:29:09
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-12-20 02:29:08|2024-12-20 02:29:08|
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |2024-12-20 02:29:08|1970-01-01 08:00:00|2024-12-20 02:29:08|
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |2024-12-20 02:29:09|2024-12-20 02:29:09|1970-01-01 08:00:00|
    ```

2. 如果长时间未发现上述字样的矩阵，则仲裁线程可以断定为已经失效，一般失效都是由`delay_arbitrate_max_cluster_timeout`引起的，在确认是否为参数`delay_arbitrate_max_cluster_timeout`引起的之前，我们首先排除仲裁线程是否已经退出，可以在`$GAUSSLOG/cm/cm_server`下的`cm_server`日志搜索关键字`MaxNodeClusterArbitrateMain will exit`,若存在此关键字且后续不再出现`MaxNodeClusterArbitrateMain will start`关键字，则认为仲裁线程已经退出且没有再启动，不过一般不会出现`cm_server`进程还在，仲裁线程单独退出的情况，排除以后，我们可以用如下命令查询该参数的值：

    ```shell
    cm_ctl list --param --server | grep 'delay_arbitrate_max_cluster_timeout'
    ```

    若`delay_arbitrate_max_cluster_timeout`为0则说明仲裁失效。

## 三、根因分析

`delay_arbitrate_max_cluster_timeout`表示延迟仲裁最大集群的时间。其取值范围为0-1000，默认值为300，而0表示不仲裁。

## 四、 解决方案

利用如下命令打开集群仲裁时间，延迟时间可根据需要选择大小：

```shell
cm_ctl set --param --server -k 'delay_arbitrate_max_cluster_timeout=300'
```
