# 因端口冲突导致DMS初始化失败的问题

## 一、问题现象

在openGauss资源池化集群启动过程中，集群中某个节点启动失败，原因为DMS组件启动失败，且在数据库`pg_log`日志中，显示报错信息为：

```shell
2024-09-23 21:18:32.540 66f16aa6.1 [unknown] 281467464384528 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] FATAL: failed to initialize dms, errno: 309, reason: Failed to bind socket for xxx.xxx.xxx.xx:xxx, error code 98
2024-09-23 21:18:32.541 66f16aa6.10000 [unknown] 281461363814304 dn_6002 0 dn_6001_6002_6003 00000 0 [BACKEND] LOG: [Alarm Module]alarm checker shutting down...
```

报告DMS因无法在指定IP与端口绑定socket，从而导致该DMS组件无法初始化，数据库拉起失败。

## 二、定位方法

对于该类型的启动失败原因定位，可按照如下步骤所示：

1. 在命令行下发`cm_ctl start`命令后，使用`cm_ctl query -Cvipdw`确定集群状态，可观测到集群内某个节点状态不为`Normal`，为`Down`状态，其他节点为`Normal`状态，此时证明该节点拉起失败。
2. 之后登录到故障节点机器，进入`$GAUSSHOME/cm/cm_agent`目录下，寻找该节点最近时间点的`cm_agent`日志，利用`zgrep`指令在该文件日志中过滤`restart`关键字，可以得到重启五次失败的日志，如下所示：

    ```shell
    2024-09-23 21:18:20.139 tid=261655 StartAndStop LOG: res(dss) inst(20002) has been restart (1) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:24.374 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (3) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:28.753 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (4) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:33.106 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (5) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:34.383 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) is offline, but restart times (5) >= limit (5), can't do restart again, will do manually stop.
    ```

    从上述日志可以确认，`dms_res`组件重启五次，最终超过五次最大重启次数，从而导致数据库拉起失败。
3. 随后进入`$GAUSSHOME/pg_log`目录下，寻找最近时间点的启动日志，可以在数据库日志中观测到如下记录：

    ```shell
    2024-09-23 21:18:32.540 66f16aa6.1 [unknown] 281467464384528 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] FATAL: failed to initialize dms, errno: 309, reason: Failed to bind socket for xxx.xxx.xxx.xx:xxx, error code 98
    2024-09-23 21:18:32.541 66f16aa6.10000 [unknown] 281461363814304 dn_6002 0 dn_6001_6002_6003 00000 0 [BACKEND] LOG: [Alarm Module]alarm checker shutting down...
    ```

    上述日志中详细描述了DMS组件启动失败的原因，为端口占用导致的启动失败。

通过上述方法，可以确定本次的该节点拉起失败的原因为IP端口冲突。

## 三、问题根因

在各个组件启动时，需要使用特定端口进行socket通信，如果端口被占用，在拉起过程中无法通过建立连接，导致拉起失败。因此在使用和安装部署数据库过程中，请保证端口独立，不与其他进程混用、不使用默认系统端口。

## 四、解决方案

对于该问题，可以有两种解决方式：

1. 在安装部署时，配置的端口请避免使用系统默认端口，避免和系统任务产生冲突。
2. 如果发生端口冲突，请在启动失败后使用`lsof -i:端口`或其他指令寻找占用端口的进程，杀掉该进程避免启动失败。
