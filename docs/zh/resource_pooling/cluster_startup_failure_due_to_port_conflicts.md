# 因端口冲突导致集群启动失败的问题

## 一、问题现象

以资源池化一主一备环境为例，介绍因端口冲突导致启动或重启失败的问题。

主节点 IP 为 aaa.aaa.aaa.aaa，备节点 IP 为 bbb.bbb.bbb.bbb，端口号均为 xxx。

1. 若openGauss资源池化集群启动或重启成功，但使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中某个节点的`state`为`Down`。

2. 若openGauss资源池化集群启动或重启失败，有如下报错信息：

    ```shell
    cm_ctl: start cluster failed in (601)s!
    
    HINT: Maybe the cluster is continually being started in the background.

    You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
    ```

    报告启动集群失败，10min 超时。

## 二、定位方法

对于问题现象1:

1. 登录故障节点机器，进入`$GAUSSLOG/cm/cm_agent`目录下，寻找该节点最近时间点的`cm_agent`日志，发现如下报错信息：

    ```shell
    2024-09-29 16:52:33.198 tid=3554807 StartAndStop WARNING: port:xxx already in use.
    ```

   从上述日志可以确认，端口 xxx 已被使用。

2. 查看`$GAUSSLOG/cm/cm_ctl`日志，发现如下信息：

    ```shell
    2024-09-29 16:37:21.420 tid=3554057  DEBUG1: begin to create ssl connection

    2024-09-29 16:37:21.437 tid=3554057  DEBUG1: connect to cmserver success, remotehost is bbb.bbb.bbb.bbb:xxx.
    ```

   从上述日志可以确认，连接 cmserver 成功，连接的 ip 为 bbb.bbb.bbb.bbb，连接的端口号为 xxx，同时可以发现日志内没有主节点 cmsever 连接成功的信息。

通过上述方法，可以确认问题现象1的集群`CMServer State`中某个节点的`state`为`Down`是由该节点 CM 端口冲突导致的。

对于问题现象2：

在`cm_ctl start`回显报错信息后，可能会有以下几种情况：

1. 使用`cm_ctl query -Cvipd`查询集群状态后，发现直接报错，无法显示查询信息，这说明所有节点机器的 CM 端口均有冲突。

2. 首先使用`cm_ctl query -Cvipd`查询集群状态后，可以观测到`CMServer State`中节点状态均为`Normal`，但是`Datanode State`均不为`Normal`或某个节点数据库状态不为`Normal`。

    然后登录故障节点机器，进入`$GAUSSLOG/cm/cm_ctl`目录下，寻找该节点最近时间点的`cm_ctl1`日志，发现如下报错信息：

    ```shell
    DEBUG1: starting exceeds 2 mins, instance status:g_cluster_start_status=3,g_az_start_status=5, g_node_start_status=5, g_instance_start_status=5
    ```

    这说明部分节点或所有节点机器的数据库端口有冲突。

## 三、问题根因

在各个组件启动时，需要使用特定端口进行 socket 通信，如果所有节点 cm 端口都被占用，在集群启动或重启过程中无法成功建立连接，导致 cm 拉起失败，查询集群状态会报错；如果部分节点 cm 端口被占用，集群会启动或重启成功，但对应节点的 cmserver 状态为`Down`，集群数据库状态为`Normal`；如果所有或部分节点数据库端口被占用，在集群启动或重启过程中无法成功建立连接，导致集群启动或重启失败，集群各节点数据库状态均不为`Normal`，但 cmserver 状态均为`Normal`。因此在启动或重启数据库过程中，请保证端口独立，不与其他进程混用、不使用默认系统端口。

## 四、解决方案

对于该问题，请在启动或重启完成后（无论成功与否）使用`lsof -i:端口号`或其他指令寻找占用端口的进程，杀掉该进程避免启动或重启失败。杀掉后等一会时间再次查询集群状态，若仍未正常，重启集群即可。
