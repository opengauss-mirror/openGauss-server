# Node Clean Restart Due to Abnormal Resource Instance State

## Symptom

1. A resource instance (such as `dss` or `dms`) suddenly goes `OffLine`, and subsequent cluster status queries show the node has been evicted with the database entering the `Manually stopped` state. Sample outputs are as follows:

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

## Troubleshooting Method

1. Navigate to the `$GAUSSLOG/cm/cm_server` directory on the **CMServer Primary** node. Open the `key-event` logs and search for the keyword `kick out` to locate the most recent eviction record for the affected node. Record the timestamp of this eviction.

    ```shell
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    ```

2. Using the timestamp recorded in the previous step, inspect the surrounding logs to determine the root cause of the eviction.

    ```shell
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: [KeyEvent: KEY_EVENT_RES_ARBITRATE] [Instance: 0] [Details: node(1) kick out.]
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: node(1) stat (unavail).
    2024-10-10 15:43:46.137 tid=3356184 MaxClusterAb LOG: kick out result: node(1) res inst manual stop or report timeout.
    ```

    If the log contains the phrase `res inst manual stop or report timeout`, it confirms that the node was evicted due to an abnormal resource instance state.

3. Search for the keyword `restart` and look for log entries similar to the following:

    ```shell
    2024-10-10 15:43:42.442 tid=2498489 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
    2024-10-10 15:43:42.442 tid=2498489 StartAndStop LOG: res(dss) inst(20001) has been restart (4) times, restart more than (5) time will manually stop.
    2024-10-10 15:43:43.792 tid=2498489 StartAndStop LOG: cur inst(20001) isreg stat=(4), and reg failed, restart failed.
    2024-10-10 15:43:43.792 tid=2498489 StartAndStop LOG: res(dss) inst(20001) has been restart (5) times, restart more than (5) time will manually stop.
    2024-10-10 15:43:45.117 tid=2498489 StartAndStop LOG: res(dss) inst(20001) is offline, but restart times (5) >= limit (5), can't do restart again, will do manually stop.
    ```

    Or:

    ```shell
    2024-10-09 11:31:57.208 tid=1143779 StartAndStop LOG: res(dms_res) inst(6001) has been restart (2) times, restart more than (5) time will manually stop.
    ```

    These logs prove that the monitored resource instance failed, triggering the node eviction.

4. Further investigation into the specific resource instance logs is required to pinpoint the exact failure.

## Root Cause

Typically, if the `cm_agent` fails to restart a resource instance more than **5 times** within a **30-second restart cycle**, the system identifies the resource instance as abnormal. Consequently, CM invokes a script to perform a clean operation on the resource instance and evicts the node from the cluster.

## Solution

1. First, verify the integrity of the `dss_conctrl.sh` and `dms_conctrl.sh` scripts located in the `$GAUSSHOEM/bin` directory.
2. Further inspect the `dss` logs under `$DSS_HOME` or the database logs (`pg_log`) under `$GAUSSLOG` to identify the underlying cause of the resource instance failure.
