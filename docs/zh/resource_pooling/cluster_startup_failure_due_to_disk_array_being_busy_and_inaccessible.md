# 因磁阵繁忙不可访问导致集群启动失败的问题

## 一、问题现象

在openGauss资源池化集群启动过程中，集群中某个节点启动失败，节点Manually stopped。

## 二、定位方法

查看`/var/log/messages`操作系统日志，可以在日志中观测到类似如下记录：

```shell
Mar 7 11:36:24 host59 systemd-udevd[3348439]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:24 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:27 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:27 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:31 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:36 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:38 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
```

报告无法访问磁盘。

## 三、问题根因

磁阵无法响应，数据库无法正常读写。

## 四、解决方案

减少磁阵的IO压力，或者增加磁阵的IO带宽，检查磁阵是否故障。
