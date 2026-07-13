
# 因磁盘满导致安装失败的问题

## 问题现象

若openGauss资源池化环境在搭建过程中，由于磁盘满而导致安装失败会有如下报错信息：

```shell
No Space Left on Device.
```

## 定位方法

可定期使用`df -h`命令监控磁盘容量变化。

```shell
df -h
Filesystem                  Size  Used Avail Use% Mounted on
devtmpfs                    127G     0  127G   0% /dev
tmpfs                       128G  1.7M  127G   1% /dev/shm
tmpfs                       128G   87M  127G   1% /run
tmpfs                       128G     0  128G   0% /sys/fs/cgroup
/dev/mapper/openeuler-root   49G   39G  8.4G  83% /
tmpfs                       128G  2.4M  127G   1% /tmp
/dev/sda2                   976M  108M  801M  12% /boot
/dev/sda1                   200M  5.8M  195M   3% /boot/efi
/dev/mapper/openeuler-home  1.1T  699G  293G  71% /home
tmpfs                        26G     0   26G   0% /run/user/1093
tmpfs                        26G     0   26G   0% /run/user/0
/dev/nvme0n1                3.0T  1.2T  1.8T  42% /usr1
/dev/nvme1n1                3.0T  186G  2.8T   7% /usr2
/dev/nvme2n1                3.0T   26G  2.9T   1% /usr3
/dev/nvme3n1                3.0T  202G  2.8T   7% /usr4
```

## 问题根因

导致该问题的原因如下：

1. 磁盘满，无空间存储安装文件：没有足够的空间来存放安装文件，导致安装无法进行。
2. 磁盘满，无空间存储数据文件：数据库安装过程中会创建一些初始的数据文件，用于存储数据库的系统表、元数据等。如果磁盘已满，就无法创建这些数据文件。
3. 磁盘满，无空间存储临时文件：数据库安装过程中会生成一些临时文件，用于存储中间结果、压缩包等。如果磁盘已满，就无法创建这些临时文件。

## 解决方法

对于该问题，有如下几种解决措施：

1. 若环境支持扩容，适当增大磁盘容量。例如，额外挂载或者替换比现有磁盘容量更大的磁盘。

2. 外部监控磁盘使用情况，定时进行磁盘清理操作。安装环境前使用`df -h`命令，查看磁盘使用情况，并对一些无用文件进行清理。
