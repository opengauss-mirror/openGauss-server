# 因信号量不足问题导致启动或重启集群失败的问题

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

登录故障节点机器，进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下报错信息：

```shell
2024-10-14 14:57:29.056 670cc0cb.1 [unknown] 281461820162064 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error does *not* means that you have run out of disk space. It occurs when either the system limit for the maximum number of semaphore sets (SEMMNI), or the system wide maximum number of semaphores (SEMMNS), wouled be exceeded.  You need to raise the respective kernel parameter.  Alternatively,reduce openGauss's consumption of semaphores by reducing its max_connections parameter.

    The openGauss documentation contains more information about configuring your system for openGauss.
```

通过上述报错信息可以看出该问题是系统信号量不足导致的。

## 三、问题根因

系统信号量不足，导致不满足数据库的最低信号量要求，从而导致集群的启动或重启失败。具体内核信号量参数要求参考[gs_checkos](../tool_and_commandreference/gs_checkos.md)中的表2操作系统参数。

## 四、解决方案

针对该问题的解决方案为，使用`cat /proc/sys/kernel/sem`命令查看系统的参数设置是否符合最低要求，修改不符合最低要求的参数值，然后重启集群即可。
