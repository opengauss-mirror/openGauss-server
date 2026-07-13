# 因om_monitor定时任务被清理导致节点Down的问题

## 一、问题现象

1. 若观察到某节点突然掉线即节点状态为Down，但是cm_server却仍然在线，类似于如下显示：

    ```shell

    [  CMServer State   ]

    node            node_ip         instance                                                state
    -----------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1   1    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby
    2  openGauss135 NodeIP_2   2    /usr2/test_0925_install/omm/openGauss/cm/cm_server Standby
    3  openGauss137 NodeIP_3   3    /usr2/test_0925_install/omm/openGauss/cm/cm_server Primary


    [ Defined Resource State ]

    node            node_ip         res_name instance  state  
    ----------------------------------------------------------
    1  openGauss111 NodeIP_1   dms_res  6001      Deleted
    2  openGauss135 NodeIP_2   dms_res  6002      OnLine 
    3  openGauss137 NodeIP_3   dms_res  6003      OnLine 
    1  openGauss111 NodeIP_1   dss      20001     Deleted
    2  openGauss135 NodeIP_2   dss      20002     OnLine 
    3  openGauss137 NodeIP_3   dss      20003     OnLine 


    [   Cluster State   ]

    cluster_state   : Degraded
    redistributing  : No
    balanced        : No
    current_az      : AZ_ALL


    [  Datanode State   ]

    node            node_ip         instance                                              state
    ---------------------------------------------------------------------------------------------------------
    1  openGauss111 NodeIP_1   6001 20128  /usr2/test_0925_install/omm/openGauss/dn1 P Down    Unknown
    2  openGauss135 NodeIP_2   6002 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Standby Normal
    3  openGauss137 NodeIP_3   6003 20128  /usr2/test_0925_install/omm/openGauss/dn1 S Primary Normal
    ```

## 二、定位方法

1. 进入$GAUSSLOG/cm/cm_agent日志，观察cm_agent日志是否有新生成的日志，若没有进入下一步。

2. `ps ux`观察进程，观察om_monitor以及cm_agent是否都在，若都不在，进入下一步。

3. 利用 `crontab -l` 查看定时任务，观察是否存在如下有om_monitor字样的定时任务。

    ```shell
    */1 * * * * source /etc/profile;(if [ -f ~/.profile ];then source ~/.profile;fi);source ~/.bashrc;source /home/test_0925/envfile;nohup /usr2/test_0925_install/omm/openGauss/gauss/app/bin/om_monitor -L /usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925/cm/om_monitor >>/dev/null 2>&1 &
    ```

    若没有或者被注释掉了，则重新将定时任务开启。

## 三、问题根因

安装openGauss数据库时，会设置系统定时任务，每间隔1min拉起om_monitor进程。om_monitor为常驻进程，负责拉起cma进程，cma进程负责拉起dn，dss，cms进程。如果om_monitor不在并且定时任务也被清理掉了，必然会导致节点无法拉起。

## 四、解决方案

1. `crontab -e`命令编辑 crontab 文件。

2. 输入步骤1的命令后，出现如下显示，添加定时任务。

    ```shell
    */1 * * * * source /etc/profile;(if [ -f ~/.profile ];then source ~/.profile;fi);source ~/.bashrc;source /home/test_0925/envfile;nohup /usr2/test_0925_install/omm/openGauss/gauss/app/bin/om_monitor -L /usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925/cm/om_monitor >>/dev/null 2>&1 &
    ```

    注意：将上述路径中的'/usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925'替换为自己的$GAUSSLOG路径。

3. 重新拉起集群。
