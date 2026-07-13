# 因DSS组件perctrl无权限导致数据库启动失败的问题

## 一、问题现象

在openGauss资源池化集群启动过程中，集群中某个节点启动失败，原因为DSS组件启动失败，常出现在服务器重启后。

## 二、定位方法

在`$DSS_HOME/log/run`目录下，寻找最近时间点的dsscmd.rlog，可以在日志中观测到类似如下记录：

```shell
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>[PERCTRL]rgister failed, result:255, Failed to invoke the SCSI interface, OS errno:17, OS error msg:File exists. [ddes_perctrl_api.c:284]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Scsi3 register failed, rk 1, dev /dev/disk/by-id/wwn-0x6382028100c0772b315389ae0000004c, errno 17. [cm_iofence.c:48]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Failed to register, rk: 0, dev: /dev/disk/by-id/wwn-0x6382028100c0772b315389ae0000004c, ret: -1. [dss_io_fence.c:249]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|INFO>free g_vgs_info. [dss_diskgroup.c:223]
```

报告DSS因无法register磁盘，从而导致该DSS组件无法启动，数据库拉起失败。

## 三、问题根因

CAP_SYS_RAWIO 是 Linux 中的一种能力（capability），它允许进程直接进行原始 I/O 操作。具体来说，这个能力使得进程能够执行以下操作：

1. 直接访问硬件设备<br> 
允许进程直接访问硬件设备，而不需要通过标准的文件系统接口。这对于需要直接与硬件交互的应用程序（如设备驱动程序或某些网络应用程序）非常重要。
2. 进行低级别的 I/O 操作<br>
允许进程执行低级别的 I/O 操作，例如直接读写设备寄存器或内存映射 I/O。这对于需要高性能或低延迟的应用程序（如实时系统）是必需的。

dss perctrl操作磁盘需要 CAP_SYS_RAWIO 能力，但是当前用户没有该权限，因此需要为 dss perctrl 进程添加该权限。

## 四、解决方案

```shell
sudo -i setcap CAP_SYS_RAWIO+ep 绝对路径/perctrl(通常在$GAUSSHOME/bin目录下)
```
