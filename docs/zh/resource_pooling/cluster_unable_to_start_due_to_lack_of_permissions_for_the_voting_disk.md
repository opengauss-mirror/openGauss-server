# 因Voting盘无权限导致集群无法启动的问题

## 一、问题现象

1. 正常启动集群始终无法启动，利用`cm_ctl query -Cvipdw`命令查询以后发现某节点无法启动：

    ```shell
    [  Datanode State   ]

    node            node_ip         instance                                              state
    ---------------------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1    6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Down    Unknown
    2  openGauss135 NodeIP_2    6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
    3  openGauss137 NodeIP_3    6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Starting
    ```

## 二、定位方法

1. 首先进入$GAUSSLOG/cm/cm_agent之下打开当前的cm_agent日志。

2. 查看日志若发现如下字样的日志记录：

    ```
    2024-10-09 10:24:19.659 tid=695385 VotingDisk ERROR: [InitVotingDiskHandler] open disk /dev/disk/by-id/scsi-36382028100ed96ac46c91b0d0000005b failed, errno 13.
    2024-10-09 10:24:19.659 tid=695385 VotingDisk FATAL: Init Voting disk failed!
    ```

    关于VotingDisk的错误记录，则认为是Voting盘有问题，值得注意的是errno 13 通常表示 `Permission denied`，即权限被拒绝。这是一个常见的错误，通常在尝试访问或操作一个没有足够权限的文件或资源时发生。

3. 进入xml文件查看，搜索`votingDiskPath`关键字，查看Voting盘配置的是哪个盘，随后利用lsscsi -is命令以及 ll /dev 命令查看Voting盘对应的权限以及属主，若不为当前用户则认为是Voting盘权限被更改引起的该节点无法启动。

## 三、问题根因

Voting盘是安装时候配置的，在资源池化下，从共享盘中规划一块空间作为投票盘，给CM用于磁盘检测。各节点CMA定时向投票盘写入心跳，CMS获取投票盘中的磁盘心跳，若规定时间内CMS没读上来心跳，则会判定该节点异常，若某节点对Voting盘没有权限，则无法写入心跳。

## 四、解决方案

在安装时确认好投票盘的配置，在使用过程中Voting盘的属主为当前用户的，若被修改，利用chown命令及时改回来。
