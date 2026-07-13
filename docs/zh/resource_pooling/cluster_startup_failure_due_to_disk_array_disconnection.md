# 因磁阵断连导致集群启动失败的问题

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
   2024-10-08 19:57:53.495 tid=3790142 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/xxx failed, errno 2.

   2024-10-08 19:57:53.495 tid=3790142 VotingDisk FATAL: Init voting disk failed!
   ```

   这里的 errno 2 表示找不到磁盘。

2. 使用`lsscsi -is`查看设备标识符，发现没有对应的盘符。

## 三、问题根因

磁阵断链导致无法启动或重启资源池化集群。

## 四、解决方案

对于该问题，有解决方案如下：

在 root 用户下执行：

1. 查看磁阵连接

   ```shell
   iscsiadm -m node
   ```

2. 断开磁阵连接

   ```shell
   iscsiadm -m node --logoutall=all
   ```

3. 登录磁阵连接

   ```shell
   iscsiadm -m node -p xx.xx.xx.xxx -l
   ```

4. 重新扫描盘符连接

   ```shell
   rescan-scsi-bus.sh
   ```

   此时使用`lsscsi -is`查看设备标识符，可以发现有了对应的盘符。

在子用户下执行：

5. 使用`cm_ctl start`命令启动集群后再使用`cm_ctl query -Cvipd`查询，显示集群状态正常。

>[!WARNING]注意
>
>+ 使用断开磁阵连接命令会影响节点上所有的磁阵。
>
>+ 登录磁阵连接命令中 -p 后的 iP 对应查到的 ip。
>
>+ 重新登录磁阵连接后，还需要再修改磁阵的权限为子用户，可以使用`chmod`或`chown`进行修改。
