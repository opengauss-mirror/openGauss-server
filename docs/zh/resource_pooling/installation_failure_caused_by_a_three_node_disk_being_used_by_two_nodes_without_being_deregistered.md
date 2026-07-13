
# 因三节点盘未解注册被两节点使用导致安装失败的问题

## 问题现象

资源池化环境安装过程中若出现磁盘未解注册被使用，会出现如下报错信息：

```shell
The disk is not registered correctly, unable to access a specific disk location.
```

## 定位方法

可使用命令辅助判断，观察磁盘是否出现在命令的输出列表中：

1. 使用`lsblk`命令可以列出系统中的块设备（包括磁盘、分区等）及其挂载点信息。如果磁盘已解注册，它将不会出现在输出列表中。

    ```shell
    NAME   MAJ:MIN   RM  SIZE  RO TYPE MOUNTPOINT
     sda      8:0    0   50G   0    disk 
     ├─sda1   8:1    0   49G   0    part /
     └─sda2   8:2    0    1G   0    part [SWAP]
    ```

2. 使用`fdisk -l`命令可以列出系统中的磁盘和分区信息。同样，如果磁盘已解注册，它将不会在输出中显示。

    ```shell
    Disk /dev/sda: 50 GiB, 53687091200 bytes, 104857600 sectors
    Disk model: QEMU HARDDISK
    Units: sectors of 1 * 512 = 512 bytes
    Sector size (logical/physical): 512 bytes / 512 bytes
    I/O size (minimum/optimal): 512 bytes / 512 bytes
    Disklabel type: gpt
    Disk identifier: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX

    Device      Start      End       Sectors   Size      Type
    /dev/sda1   2048    102399999   102397952  49G    Linux filesystem
    /dev/sda2 102400000 104855551   2455552    1.2G   Linux swap
    ```

## 问题根因

导致该问题的原因较多：

1. 用户权限问题：如果当前用户没有足够的权限来解注册磁盘，操作可能会失败。通常，需要管理员权限才能执行磁盘解注册等高级操作。
2. 硬件问题：如果磁盘本身或连接磁盘的硬件（如电缆、控制器等）出现故障，可能会导致解注册失败。例如，磁盘可能出现坏道、控制器故障或电缆松动等问题。

## 解决方案

可使用`dsscmd unreghl`命令对旧的盘进行解注册，之后重新进行安装即可。

```shell
dsscmd unreghl [-t type] [-D DSS_HOME]
```
