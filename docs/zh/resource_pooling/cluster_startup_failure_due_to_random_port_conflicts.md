# 因随机端口冲突导致集群启动失败的问题

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

且进入`$DSS_HOME/log/run/dss_instance.log`文件中，可以看到如下报错：
    ```shell
        [mes]:Start tcp lsnr failed. Host_name: xxx.xxx.xxx.xxx, inst_id:0, port:xxxx, os error:98.
    ```
提示端口冲突，创建`TCP`连接失败。

## 二、定位方法

首先可以根据问题现象中的日志文件，确定是否存在因端口冲突导致的`TCP`建立失败的问题，其次可以根据冲突端口号，利用`lsof`指令查看是否存在其他进程正在使用该端口。

如果无其他进程使用该端口，则可利用`cat /proc/sys/net/ipv4/ip_local_port_range`命令查看端口范围，根据安装的`xml`配置文件查看安装的端口是否在该端口范围内。

如果在该端口范围内，可认为是池化内组件随机端口冲突。

## 三、问题根因

对于部分组件的使用端口，会有两种指定形式：

1. `xml`安装时指定端口，各个组件线程的使用端口会基于指定端口偏移区间进行占用；
2. 对于部分工作线程，会基于`/proc/sys/net/ipv4/ip_local_port_range`的空闲端口范围随机选择端口占用；

因此可能会出现两种形式选择同一个端口进行占用，发生端口冲突，导致建立`TCP`连接失败。

## 四、解决方案

对于该问题，只需保证`xml`安装的端口不在`/proc/sys/net/ipv4/ip_local_port_range`的空闲端口范围内，便可解决该冲突，需要在用户在安装的时候进行查证。
