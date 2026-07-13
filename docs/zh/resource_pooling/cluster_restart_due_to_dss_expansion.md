# 因拓容DSS导致集群重启的问题

## 一、问题现象

1. 在利用dss扩容的时候，会出现一种现象，可能会导致集群处于Starting，类似于如下现象：

    ```shell
    dsscmd adv -g log0 -v /dev/disk/by-id/wwn-0x6382028100ed96ac0cefc8020000001a
    Succeed to add volume online, vg_name is log0, volume path is /dev/disk/by-id/wwn-0x6382028100ed96ac0cefc8020000001a.

    [  Datanode State   ]

    node            node_ip         instance                                              state
    ---------------------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Standby Starting
    2  openGauss135 NodeIP_2    6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
    3  openGauss137 NodeIP_3    6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Down    Starting
    ```

## 二、问题根因

在dss扩容阶段，由于新扩容进来的盘对于CM来说是未注册的，故而导致CM检测到有部分实例是未注册的，CM会对未注册的实例重新进行注册，故而集群中节点卡在Starting状态。

## 三、解决方案

1. 若集群始终卡在Starting，可以尝试重启集群使得未注册实例快速注册上。

2. 尽量不要在有业务的情况下，进行dss拓容操作，会导致业务中断。
