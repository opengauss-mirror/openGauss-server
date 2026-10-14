# DSS Server Startup Failure Due to Port Conflict

## Symptom

During the startup of an openGauss resource pool cluster, a specific node fails to start because the DSS component fails to launch. The `$DSS_HOME/log/run/dssinstance.rlog` contains the following error messages:

```shell
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] start_one_receiver start receiver:priority=8 , id=0. [mes_recv.c:92]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] start sender monitor finish [mes_recv.c:281]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes] failed to create lsnr sockets for listener type 0 [cs_listener.c:299]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes]:Start tcp lsnr failed. Host_name: xx.xx.xx.xx, inst_id:0, port:8080, os error:17. [mes_tcp.c:875]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes]mes_init failed. [mes_func.c:1114]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] mes_uninit start [mes_func.c:1453]
UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] mes_stop_heartbeat_thread end [mes_func.c:1441]
UTC+8 2024-10-09 15:36:56.889|dssserver|141732|INFO>[mes] work thread closed, tid:281465243017248, close:0 [mes_queue.c:965]
```

The logs indicate that DSS failed to bind a socket to the specified IP address and port, preventing the DSS component from initializing and causing the database startup to fail.

## Troubleshooting Method

To diagnose this startup failure, follow these steps:

1. After executing `cm_ctl start`, run `cm_ctl query -Cvipdw` to check the cluster status. Observe that one node is in the `Down` state while others are `Normal`, indicating a node startup failure.
2. Log in to the faulty node. Navigate to the `$GAUSSHOME/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. Use `zgrep` to filter for the keyword `restart`. You will observe logs similar to the following, showing repeated restart attempts:

    ```shell
    2024-09-23 21:18:24.374 tid=261655 StartAndStop LOG: res(dss) inst(6002) has been restart (3) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:28.753 tid=261655 StartAndStop LOG: res(dss) inst(6002) has been restart (4) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:33.106 tid=261655 StartAndStop LOG: res(dss) inst(6002) has been restart (5) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:34.383 tid=261655 StartAndStop LOG: res(dss) inst(6002) is offline, but restart times (5) >= limit (5), can't do restart again, will do manually stop.
    ```

    These logs confirm that the `dss` component exceeded the maximum restart limit of five attempts.
3. Navigate to the `$DSS_HOME/log/run` directory and inspect the most recent startup log. Locate the following records:

    ```shell
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] start_one_receiver start receiver:priority=8 , id=0. [mes_recv.c:92]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] start sender monitor finish [mes_recv.c:281]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes] failed to create lsnr sockets for listener type 0 [cs_listener.c:299]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes]:Start tcp lsnr failed. Host_name: xx.xx.xx.xx, inst_id:0, port:8080, os error:17. [mes_tcp.c:875]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|ERROR>[mes]mes_init failed. [mes_func.c:1114]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] mes_uninit start [mes_func.c:1453]
    UTC+8 2024-10-09 15:36:56.888|dssserver|141705|INFO>[mes] mes_stop_heartbeat_thread end [mes_func.c:1441]
    UTC+8 2024-10-09 15:36:56.889|dssserver|141732|INFO>[mes] work thread closed, tid:281465243017248, close:0 [mes_queue.c:965]
    ```

    The logs explicitly state that the DSS component failed to start due to port occupation.

Using the aforementioned steps, you can determine that the node startup failure is caused by an IP port conflict.

## Root Cause

Components require specific ports to establish socket communication during startup. If the target port is already occupied by another process, the connection cannot be established, leading to a startup failure. Therefore, ensure that assigned ports are exclusive to the database and do not overlap with system services or other applications.

## Solution

There are three approaches to resolve this issue:

1. **Pre-deployment Configuration**: When configuring ports during installation, avoid using default system ports to prevent conflicts with OS services.
2. **Post-failure Remediation**: If a conflict occurs, identify the occupying process after startup failure using commands such as `lsof -i:<port>`. Terminate the conflicting process to allow the database to start successfully.
3. **Configuration Adjustment**: If adjusting the conflicting process is not feasible, modify the port used in the `DSS_NODES_LIST` parameter within the `$DSS_HOME/cfg/dss_inst.ini` file on all nodes.
