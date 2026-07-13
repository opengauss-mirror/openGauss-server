# 因磁阵权限异常导致集群启动失败的问题

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
   2024-10-09 10:03:34.593 tid=82953 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/xxx failed, errno 13.
   ```

   errno 13 表示权限不足。

   使用`ll /dev/xxx`命令查到的权限为`root disk`。

2. 查看其他磁盘的权限，发现用户没有了共享存储使用的盘的权限。

   ```shell
   ll /dev | grep $USER 返回的结果为空
   ```

3. 在 root 用户下，修改没有权限的磁盘权限，数据库正常或重启成功，以 omm 用户为例。

   ```shell
   查看用户的 xml 文件，获取共享存储使用的磁盘。（建议使用 lsscsi -is 查看设备标识符，确定对应的盘符。）

   chown -R omm:omm /dev/xxx

   如果多个节点权限异常，每个节点都需要修改。
   ```

4. 使用`cm_ctl query -Cvipd`命令查询集群状态，显示状态正常。若还存在异常，使用`cm_ctl stop`和`cm_ctl start`命令重启集群后再次查询，显示状态正常。

## 三、问题根因

用户对磁盘没有权限。

## 四、解决方案

需要以 root 用户登录对应机器，使用`chmod`或`chown`修改对应磁盘权限为子用户，再重启集群即可。
