# 因不满足多数派原则导致集群无法启动的问题

## 一、问题现象

1. 启动单节点时，节点启动以后，利用cm_ctl query -Cvipdw查询集群状态，集群显示类似如下状态：

    ```shell
    [  CMServer State   ]

    node            node_ip         instance                                                state
    -----------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    1    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby     
    2  openGauss135 NodeIP_2    2    /usr2/test_0925_install/omm/openGauss/cm/cm_server Down             
    3  openGauss137 NodeIP_3    3    /usr2/test_0925_install/omm/openGauss/cm/cm_server Down              

    cm_ctl: can't connect to cm_server.             
    Maybe cm_server is not running, or timeout expired. Please try again. 
    ```

    单一节点启动后，集群不可用，且启动节点仅为备节点。

## 二、定位方法

`cm_ctl query -Cvipdw`查询集群状态，观察集群在线节点是否满足多数派的要求（在线节点+1 > 总节点数/2），如果不满足多数派的要求，集群无法正常使用。

## 三、问题根因

集群在线节点数不满足多数派的要求。

## 四、解决方案

保证在线节点数量满足多数派即可。
