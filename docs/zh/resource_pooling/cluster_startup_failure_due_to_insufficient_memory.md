# 因内存不足问题导致启动或重启集群失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为正常，`Datanode State`中有的节点的状态为`Down  Manually stopped`, `cluster_state`为`Degraded`。

## 二、定位方法

登录故障节点机器，进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下几种报错信息：

```shell
2024-10-14 11:27:29.056 670c8992.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error usually means that the openGauss's request for a shaared memory segment exceeded available memory or swap space, or exceeded your kernel's SHMALL parameter.  You can

either reduce the request the request size or reconfigure the kernel with larger SHMALL.  To reduce the request size (currently xxx bytes), reduce openGauss's shared memory usage, perhaps by reducing shared_buffers.
    The openGauss documentation contains more information about shared memory configuration.
```

通过上述报错信息可以看出是提示不能申请配置设置里的共享内存大小从而导致数据库状态异常。

## 三、问题根因

系统内存不足，导致数据库启动需要预留的内存不被支持，从而导致集群启动或重启失败。具体内存要求参考[准备软硬件安装环境](../getting_started/preparing_for_installation.md)中的表1硬件环境要求。

## 四、解决方案

针对该问题的解决方案为，清理内存至符合环境要求的大小或修改相关数据库配置参数大小，然后重启集群即可。通常在高并发场景下出现的内存不足情况，此时建议使用大内存的机器或使用负载管理限制系统的并发。
