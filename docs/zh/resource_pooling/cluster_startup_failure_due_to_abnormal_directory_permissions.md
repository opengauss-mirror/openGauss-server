# 因目录权限异常导致集群启动失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为`Down`。

## 二、定位方法

1. 进入`$GAUSSLOG/cm/cm_agent`目录下，寻找该节点最近时间点的`cm_agent`日志，发现如下报错信息：

   ```shell
   2024-10-09 11:07:59.018 tid=313687  LOG: open gaussdb state file "/.../.../dn1/gaussdb.state" failed, could not get the build infomation: Permission denied.

   2024-10-09 11:07:58.966 tid=313678  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).

   2024-10-09 11:07:58.966 tid=313678  ERROR: failed to connect to datanode:/.../.../dn1
   ```

   绝对路径`/.../.../dn1`就是`$PGDATA`，上述报错信息表明问题是`$PGDATA`目录的权限不足。

2. 使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点正常，`Datanode State`中有节点的状态为`Down Manually stopped`, `cluster_state`为`Unavailable`。
   
3. 查看`$PGDATA`目录的权限，发现用户没有`dn1`目录的权限。

   ```shell
   drwx------. 15 root  root  8.0K Oct  9 11:06 dn1
   ```

## 三、问题根因

用户对`$PGDATA`目录没有权限。

## 四、解决方案

需要以 root 用户登录对应机器，使用`chmod`或`chown`修改`$PGDATA`目录权限为子用户，再重启集群即可。
