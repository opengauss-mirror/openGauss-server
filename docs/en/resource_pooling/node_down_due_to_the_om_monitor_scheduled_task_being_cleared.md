# Node Down Due to Cleared om_monitor Scheduled Task

## Symptom

A node suddenly goes offline (state: `Down`), while the `cm_server` remains online. The output resembles the following:

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

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and check whether new logs are being generated. If no new logs appear, proceed to the next step.
2. Use the `ps ux` command to check whether the `om_monitor` and `cm_agent` processes are running. If neither process exists, proceed to the next step.
3. Use the `crontab -l` command to inspect the scheduled tasks. Verify whether a task containing `om_monitor` exists, similar to the following:

    ```shell
    */1 * * * * source /etc/profile;(if [ -f ~/.profile ];then source ~/.profile;fi);source ~/.bashrc;source /home/test_0925/envfile;nohup /usr2/test_0925_install/omm/openGauss/gauss/app/bin/om_monitor -L /usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925/cm/om_monitor >>/dev/null 2>&1 &
    ```

    If the task is missing or commented out, the cleared scheduled task is the cause of the failure.

## Root Cause

During openGauss installation, a system scheduled task is configured to start the `om_monitor` process every minute. `om_monitor` is a resident daemon responsible for starting the `cm_agent` process, which in turn starts the DN, DSS, and CMS processes. If `om_monitor` terminates and its scheduled task is removed, the node cannot restart automatically, leading to a `Down` state.

## Solution

1. Edit the crontab file using the `crontab -e` command.
2. Add the following scheduled task. 

    ```shell
    */1 * * * * source /etc/profile;(if [ -f ~/.profile ];then source ~/.profile;fi);source ~/.bashrc;source /home/test_0925/envfile;nohup /usr2/test_0925_install/omm/openGauss/gauss/app/bin/om_monitor -L /usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925/cm/om_monitor >>/dev/null 2>&1 &
    ```
    
    Note: Replace the path `/usr2/test_0925_install/omm/openGauss/log/gaussdb/test_0925` with your actual `$GAUSSLOG` path.

3. Restart the cluster using `cm_ctl start`.