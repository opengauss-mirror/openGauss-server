# 因CM盘符不一致导致安装启动失败的问题

## 一、问题现象

在资源池化om安装过程中，集群安装在启动阶段失败。

## 二、定位方法

可以通过cm_ctl show命令查看节点心跳状态，打印状态如下：

```shell
[  Network Connect State  ]

Network timeout:       180s
Current CMServer time: 2025-03-15 10:24:27
Network stat('Y' means connected, otherwise 'N'):
|  \  |  Y  |  Y  |
|  Y  |  \  |  Y  |
|  Y  |  Y  |  \  |


[  Node Disk HB State  ]

Node disk hb timeout:    200s
Current CMServer time: 2025-03-15 10:24:28
Node disk hb stat('Y' means connected, otherwise 'N'):
|  N  |  N  |  Y  |
```

如果发现存在节点的磁盘写心跳状态为N，可以查看所有节点的cm的投票盘对应盘符是否一致。命令如下：

```shell
cm_ctl list --param --server | grep voting_disk_path
```

如果显示的是wwn或者scsi编号，直接进行对比是否一致。
如果显示的是/dev/sda或者软链接等类似的格式，要通过以下命令查看对应的wwn或者scsi标号。

```shell
lsscsi -is | grep sda(获取到的磁盘名)
```

如果查看有节点的不一致，则需要重新配置。

## 三、问题根因

各个节点的cm使用了不同的磁盘，导致部分节点读写磁盘心跳异常，从而被cm踢出集群。

## 四、解决方案

手动修改cm参数，如果是软链接手动修改软链接的指向，确保各个节点盘符一致。
