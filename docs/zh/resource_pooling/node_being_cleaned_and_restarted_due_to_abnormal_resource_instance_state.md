# 因资源实例状态异常导致节点被clean重启的问题

## 一、问题现象

1. 如果观察到某资源实例（比如`dss`，`dms`等）突然`OffLine`，然后间隔一段时间后，查询集群状态发现节点已经被踢出，数据库状态进入`Manually stopped`状态，如下所示：

    ```shell
    [ Defined Resource State ]

    node            node_ip         res_name instance  state  
    ----------------------------------------------------------
    1  openGauss111 NodeIP_1    dms_res  6001      OnLine 
    2  openGauss135 NodeIP_2    dms_res  6002      OnLine 
    3  openGauss137 NodeIP_3    dms_res  6003      OnLine 
    1  openGauss111 NodeIP_1    dss      20001     OffLine
    2  openGauss135 NodeIP_2    dss      20002     OnLine 
    3  openGauss137 NodeIP_3    dss      20003     OnLine 



    [ Defined Resource State ]

    node            node_ip         res_name instance  state  
    ----------------------------------------------------------
    1  openGauss111 NodeIP_1    dms_res  6001      OffLine
    2  openGauss135 NodeIP_2    dms_res  6002      OnLine 
    3  openGauss137 NodeIP_3    dms_res  6003      OnLine 
    1  openGauss111 NodeIP_1    dss      20001     OnLine
    2  openGauss135 NodeIP_2    dss      20002     OnLine 
    3  openGauss137 NodeIP_3    dss      20003     OnLine 



    cm_ctl query -Cvipdw

    [  CMServer State   ]

    node            node_ip         instance                                                state
    -----------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    1    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby
    2  openGauss135 NodeIP_2    2    /usr2/test_0925_install/omm/openGauss/cm/cm_server Primary
    3  openGauss137 NodeIP_3    3    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby


    [ Defined Resource State ]

    node            node_ip         res_name instance  state  
    ----------------------------------------------------------
    1  openGauss111 NodeIP_1    dms_res  6001      Deleted
    2  openGauss135 NodeIP_2    dms_res  6002      OnLine 
    3  openGauss137 NodeIP_3    dms_res  6003      OnLine 
    1  openGauss111 NodeIP_1    dss      20001     Deleted
    2  openGauss135 NodeIP_2    dss      20002     OnLine 
    3  openGauss137 NodeIP_3    dss      20003     OnLine 

    [   Cluster State   ]

    cluster_state   : Degraded
    redistributing  : No
    balanced        : No
    current_az      : AZ_ALL

    [  Datanode State   ]

    node            node_ip         instance                                              state
    ---------------------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Down    Manually stopped
    2  openGauss135 NodeIP_2    6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Primary Normal
    3  openGauss137 NodeIP_3    6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Normal
    ```

## 二、定位方法

1. 首先进入cm_server主节点的`$GAUSSLOG/cm/cm_server`，打开key-event日志，搜索`kick out`关键字，找到最后一次该节点被踢出的记录，并记录该次踢出时间。

    ```shell
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    ```

2. 通过上一步记录的时间，寻找该节点被踢出的原因。

    ```shell
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: node(1) stat (unavail).
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
    ```

    如果观察到'res inst manual stop or report timeout'字样的描述则说明是有资源实例不正常所导致的节点被踢出。

3. 搜索`restart`关键字，观察有没有类似如下字样的日志：

    ```shell
    2024-10-10 15:43:42.442 tid=2498489 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
    2024-10-10 15:43:42.442 tid=2498489 StartAndStop LOG: res(dss) inst(20001) has been restart (4) times, restart more than (5) time will manually stop.
    2024-10-10 15:43:43.792 tid=2498489 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
    2024-10-10 15:43:43.792 tid=2498489 StartAndStop LOG: res(dss) inst(20001) has been restart (5) times, restart more than (5) time will manually stop.
    2024-10-10 15:43:45.117 tid=2498489 StartAndStop LOG: res(dss) inst(20001) is offline, but restart times (5) >= limit (5), can't do restart again, will do manually stop.
    ```

    或者如下字样：

    ```shell
    2024-10-09 11:31:57.208 tid=1143779 StartAndStop LOG: res(dms_res) inst(6001) has been restart (2) times, restart more than (5) time will manually stop.
    ```

    则证明CM所监控的资源实例出现故障导致节点被踢出集群。

4. 需要通过对应资源实例的日志对故障进行进一步排查。

## 三、根因分析

通常情况下，cm_agent在重启周期30s内，重启资源实例超过5次都失败，则认为资源实例状态异常，此时会调用脚本将资源实例clean掉，并将该节点踢出集群。

## 四、解决方案

1. 首先排查`$GAUSSHOEM/bin`下的`dss_conctrl.sh`和`dms_conctrl.sh`有无异常。

2. 进一步观察`$DSS_HOME`下的`dss`日志或`$GAUSSLOG`下的`pg_log`日志，进一步排查资源实例故障的原因。
