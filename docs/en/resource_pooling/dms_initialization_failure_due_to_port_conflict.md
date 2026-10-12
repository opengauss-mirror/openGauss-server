# DMS Initialization Failure Due to Port Conflict

## Symptom

During the startup of an openGauss resource pool cluster, a specific node fails to start because the DMS component fails to initialize. The database `pg_log` contains the following error message:

```shell
2024-09-23 21:18:32.540 66f16aa6.1 [unknown] 281467464384528 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] FATAL: failed to initialize dms, errno: 309, reason: Failed to bind socket for xxx.xxx.xxx.xx:xxx, error code 98
2024-09-23 21:18:32.541 66f16aa6.10000 [unknown] 281461363814304 dn_6002 0 dn_6001_6002_6003 00000 0 [BACKEND] LOG: [Alarm Module]alarm checker shutting down...
```

The logs indicate that DMS failed to bind a socket to the specified IP address and port, preventing the DMS component from initializing and causing the database startup to fail.

## Troubleshooting Method

To diagnose this startup failure, follow these steps:

1. After executing `cm_ctl start`, run `cm_ctl query -Cvipdw` to check the cluster status. Observe that one node is in the `Down` state while others are `Normal`, indicating a node startup failure.
2. Log in to the faulty node. Navigate to the `$GAUSSHOME/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. Use `zgrep` to filter for the keyword `restart`. You will observe logs similar to the following, showing repeated restart attempts:

    ```shell
    2024-09-23 21:18:20.139 tid=261655 StartAndStop LOG: res(dss) inst(20002) has been restart (1) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:24.374 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (3) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:28.753 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (4) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:33.106 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) has been restart (5) times, restart more than (5) time will manually stop.
    2024-09-23 21:18:34.383 tid=261655 StartAndStop LOG: res(dms_res) inst(6002) is offline, but restart times (5) >= limit (5), can't do restart again, will do manually stop.
    ```

    These logs confirm that the `dms_res` component exceeded the maximum restart limit of five attempts.
3. Navigate to the `$GAUSSHOME/pg_log` directory and inspect the most recent startup log. Locate the following records:

    ```shell
    2024-09-23 21:18:32.540 66f16aa6.1 [unknown] 281467464384528 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] FATAL: failed to initialize dms, errno: 309, reason: Failed to bind socket for xxx.xxx.xxx.xx:xxx, error code 98
    2024-09-23 21:18:32.541 66f16aa6.10000 [unknown] 281461363814304 dn_6002 0 dn_6001_6002_6003 00000 0 [BACKEND] LOG: [Alarm Module]alarm checker shutting down...
    ```

    The logs explicitly state that the DMS component failed to start due to port occupation.

Using the steps above, you can determine that the node startup failure is caused by an IP port conflict.

## Root Cause

Components require specific ports to establish socket communication during startup. If the target port is already occupied by another process, the connection cannot be established, leading to a startup failure. Therefore, ensure that assigned ports are exclusive to the database and do not overlap with system services or other applications.

## Solution

There are two approaches to resolve this issue:

1. **Pre-deployment Configuration**: When configuring ports during installation, avoid using default system ports to prevent conflicts with OS services.
2. **Post-failure Remediation**: If a conflict occurs, identify the occupying process after startup failure using commands such as `lsof -i:<port>`. Terminate the conflicting process to allow the database to start successfully.
