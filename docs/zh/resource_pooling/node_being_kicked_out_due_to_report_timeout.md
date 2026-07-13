# 因报告超时导致节点被踢出的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，集群内的某个节点的`CM Agent`会因网卡故障无法向`CM Server`主节点上报状态，发生`report timeout`超时错误，进而被踢出，导致集群状态异常。

## 二、定位方法

该问题定位流程如所示：

1. 使用`cm_ctl query -Cvipdw`，可观测某节点发生故障。这里以第二个节点为例，可查询二节点状态为`Down Unknown`状态，说明该节点故障下线。
2. 登录该节点机器，到`$GAUSSLOG/pg_log`目录下，寻找最后一次运行日志文件，可以找到如下记录：

    ```shell
    2024-09-26 20:11:48.859 66f54e2d.5266 postgres 140030261589760 cm_agent 0 dn_6001_6002_6003 57P01  0 [BACKEND] FATAL:  terminating connection due to administrator command
    ```

    说明数据库收到了来自于`CM Agent`的关闭操作，因此需要查看`CM Agent`日志确定为何种要向数据库发送`shutdown`命令。
3. 进入`$GAUSSHOME/cm/cm_agent`目录中，寻找该时间点附近的日志，可以得到以下日志信息：

    ```shell
    2024-09-26 20:11:48.734 tid=9986  LOG: nic not running, immediate shutdown nodes.
    2024-09-26 20:11:48.734 tid=9986  LOG: immediate_shutdown_nodes, 2 resource will be stopped.
    2024-09-26 20:11:48.846 tid=9987  LOG: 394 cm_agent connect to cm_server start.
    ```

    报错详细为`nic error`，可确定为`CM`组件检测到网卡故障。
4. 同样的，可以进入`CM server`主节点的`$GAUSSHOME/cm/cm_server`目录下，可以在`key_event`日志文件中，找到如下记录：

    ```shell
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(2) kick out.]
    ```

    可以看到故障节点被踢出。相应的，查看该节点的`cm_server`日志，可以看到踢出原因：

    ```shell
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: Network timeout:6
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: Network base_time:2024-09-26 20:11:49
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-09-26 20:11:44|2024-09-26 20:11:48|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 20:11:43|1970-01-01 08:00:00|2024-09-26 20:11:42|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 20:11:48|2024-09-26 20:11:44|1970-01-01 08:00:00|
    2024-09-26 20:11:49.676 tid=5045 MaxClusterAb LOG: last(39) is different from current(40), result is 0.
    2024-09-26 20:11:49.677 tid=5045 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(2) kick out.]
    2024-09-26 20:11:49.677 tid=5045 MaxClusterAb LOG: recv node(2) agent report res status msg timeout.
    ```

    通过该日志进一步确定踢出故障节点的原因为`report timeout`，本质为故障节点网络故障导致的意外重启。

## 三、问题根因

在正常流程中,`CM Agent`会固定向`CM Server`上报消息，如果两方检测不到消息上报状态成功，`CM Server`会主动踢出该节点，`CM Agent`会主动关闭数据库。

## 四、解决方案

对于该问题，为双向网络故障，可直接检查网卡状态，后续恢复网络状态即可。
