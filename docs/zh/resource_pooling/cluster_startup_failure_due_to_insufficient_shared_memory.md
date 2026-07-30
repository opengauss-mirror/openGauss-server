# 因共享内存不足导致集群启动失败的问题

## 一、问题现象

1. 若openGauss资源池化集群启动或重启失败，有如下报错信息：

    ```shell
    cm_ctl: start cluster failed in (601)s!

    HINT: Maybe the cluster is continually being started in the background.

    You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
    ```

    报告启动集群失败，10min 超时。

    使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为正常，`Datanode State`中有的节点的状态为`Down  Manually stopped`, `cluster_state`为`Degraded`。

2. 另外还可能有如下报错信息：

    ```shell
    dss failed to startup.
    ```

    `dss`启动失败。

## 二、定位方法

对于问题现象1:

登录故障节点机器，进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下报错信息：

```shell
2024-10-14 16:16:29.056 670cd359.1 [unknown] 281461820163632 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] HINT:  This error usually means that the openGauss's request for a shared memory segment exceeded your kernel's SHMMAX parameter.  You can either reduce the request size or reconfigure the kernel with larger SHMMAX.  To reduce the request size (currently xxx bytes), reduce openGauss's shared memory usage, perhaps by reducing shared_buffers.

If the request size is already small, it's possible that it is less than your kernel's SHMMIN parameter, in which case raising the request size or reconfiguring SHMMIN is called for.

    The openGauss documentation contains more information about configuring your system for openGauss.
```

通过上述报错信息可以看出问题现象1是系统共享内存不足导致的。

对于问题现象2：

登录故障节点机器，进去`$DSS_HOME/log/run`目录下，查看最近的`dssinstance.rlog`日志，发现如下报错信息：

```shell
UTC+8 2024-11-01 09:38:05.249|dssserver|761014|ERROR>DSS instance failed to initialize shared memory! [dss_instance.c:260]
UTC+8 2024-11-01 09:38:05.249|dssserver|761014|ERROR>DSS instance failed to initialized! [dss_instance.c:383]

UTC+8 2024-11-01 09:38:05.250|dssserver|761014|ERROR>dss failed to startup. [dssserver.c:381]
```

通过上述报错信息可以明显看出问题现象2是系统共享内存不足导致的`dss`启动失败。

但是，若为开发者环境部署方式，则该问题也有可能是由于`dss_inst.ini`配置文件中的`INST_ID`与机器内其他开发者环境的`INST_ID`重复而导致。

## 三、问题根因

系统共享内存不足，导致不满足数据库的共享内存大小的最低要求，从而导致集群的启动或重启失败。具体共享内存大小要求参考[gs_checkos](https://docs.opengauss.org/zh/docs/latest/tool_and_commandreference/gs_checkos.html)中的表2操作系统参数。

## 四、解决方案

针对该问题的解决方案为，使用`cat /proc/sys/kernel/shmall`，`cat /proc/sys/kernel/shmmax`命令查看系统的参数设置是否符合最低要求。若不符合，进入`/etc/sysctl.conf`文件修改不符合最低要求的参数值，然后执行`sysctl -p`命令使修改的配置生效，最后重启集群即可；若符合，使用`ipcs -m`命令查看机器上其他占用高的进程，若高占用的进程已不需要，可以使用`ipcrm -m shmid`或`ipcrm -a`命令清理被占用的共享内存。

若为开发者环境部署方式，还需修改`dss_inst.ini`配置文件中的`INST_ID`，重新进行启动尝试，若还是启动失败，说明共享内存不足，参考上述的解决方案。

>[!WARNING]注意
>
>+ 在 root 用户下慎用 ipcrm -a 命令，会影响其他用户进程。
