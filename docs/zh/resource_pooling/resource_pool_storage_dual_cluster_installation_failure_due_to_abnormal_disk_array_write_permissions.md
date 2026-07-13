# 因磁阵写入权限异常导致资源池化存储双集群安装失败的问题

## 一、问题现象

在资源池化存储双集群安装过程中，集群安装失败。

## 二、定位方法

安装备集群过程中，屏幕上会打印：

```shell
dss failed to startup
```

在`$DSS_HOME/log/run`目录下，寻找最近时间点的`dssserver.rlog`，可以在日志中观测到类似如下记录：

```shell
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Failed to lock vg:/dev/xxx. [dss_diskgroup.c:301]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>DSS instance failed to load vg:data ctrl! [dss_diskgroup.c:301]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>DSS instance failed to initialized! [dss_instance.c:465]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>dss failed to startup. [dssserver.c:282]
```

报告无法访问磁盘。

## 三、问题根因

双集群同步的lun在安装过程中没有写入权限。

## 四、解决方案

在DORADO管控平台上将同步pair分裂，取消从端资源保护。
