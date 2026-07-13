# 因DSS启动加载共享盘失败导致数据库安装失败的问题

## 一、 问题现象

资源池化环境安装过程中若出现`dss load vg`失败会出现如下报错信息：

```shell
[GAUSS-51252] : Failed to start the DSS server. Please check the dss logs.
```

## 二、 定位方法

使用`cd`命令进入`$DSS_HOME/log/run`目录下，查看对应的`DSS server`日志，有以下日志信息：

```shell
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Scsi3 caw failed, addr 1032192, dev /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx, errno 0. [cm_dlock.c:327]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Scsi3 caw failed, addr 1032192, dev /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx, errno 0. [cm_dlock.c:327]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Failed to lock vg, entry_path /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx. [dss_diskgroup.c:938]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Failed to lock vg:/dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx. [dss_diskgroup.c:583]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>DSS instance failed to load vg:data ctrl! [dss_diskgroup.c:356]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>DSS instance failed to load vg:data! [dss_diskgroup.c:387]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>[RECOVERY]ABORT INFO: Failed to get vg info when instance start. [dss_instance.c:661]
```

从日志里可以清楚地看到`DSS instance failed to load vg`，`DSS`加载共享盘失败。查看安装时的配置文件，检查其盘符的命名方式。

## 三、 问题根因

在`Linux`系统中，`/dev`是设备文件的存储目录。`/dev/xxx`这种形式的设备文件是传统的设备命名方式，`xxx`通常是设备的基本名称。它的缺点是，<strong>设备名称可能会根据系统中设备的添加、移除或者启动顺序等因素而改变</strong>。例如，如果你在系统中添加了一个新的磁盘，原来的`sda`设备可能会变成`sdb`，这会导致基于设备名称的配置（如在`/etc/fstab`文件中挂载磁盘分区）出现问题，因为设备名与实际设备的映射关系发生了改变。

而`/dev/disk/by-id`是一种更稳定的设备文件目录结构。这个目录下的设备文件是通过设备的唯一标识符来命名的，这些标识符通常是由设备的制造商、型号、序列号等信息生成的。该命名方式提供了更高的设备命名稳定性，并便于设备的识别与管理。<strong>无论设备的添加顺序如何改变，或者在系统重启后，只要设备本身的物理特性（如序列号等）不变，设备文件的名称就不会改变</strong>。这使得在系统管理和配置文件中使用设备文件路径时更加可靠，减少了因设备名称变化而导致的错误。通过设备的唯一标识符命名，管理员可以很容易地识别设备的具体信息，如制造商、型号等。这对于设备的维护、故障排查以及在多个相似设备中区分不同的设备非常有用。

归根到底是在配置文件`cluster_config.xml`中，各个机器的盘符不一致导致的，即：配置文件中使用的`/dev/xxx`传统设备命名方式。

## 四、 解决方案  

首先，检查在安装环境时配置的`cluster_config.xml`文件，该文件中的盘符`/dev/xxx`若不能保证各个机器的盘符一致，建议使用统一的`/dev/disk/by-id`编码。

而后，使用`gs_uninstall --delete-data`命令删除安装数据，重新进行预安装和安装过程即可。
