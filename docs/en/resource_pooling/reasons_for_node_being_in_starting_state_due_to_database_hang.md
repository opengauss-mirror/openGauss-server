# Node Being in Starting State Due to Database Hang

## Symptom

During the cluster running, a node is observed to be in the `Starting` state while all other resource instances appear normal. Further log analysis is required to determine the root cause.

## Troubleshooting Method

1.  Use the `ps ux` command to check whether the database process (gaussdb process) exists. If the process exists but the DN remains faulty, the database may be experiencing a hang (phony dead state).

2.  Navigate to the `$GAUSSLOG/cm/cm_agent` directory and search for the keyword `phony dead`. Logs similar to the following indicate that phony dead detection has been triggered. 

    ```shell
    2024-10-10 16:03:54.868 tid=3762978  LOG: has found 1 times for instance(dn_6001) phony dead check.
    2024-10-10 16:03:55.019 tid=3762975  ERROR: failed to connect to datanode:/usr1/ertao/openGauss/cluster/dn1
    2024-10-10 16:03:55.019 tid=3762975  ERROR: connection return errmsg : wait 20.20.20.79:36964 timeout expired
    ```

    If the phony dead count exceeds 5, the database process will be terminated.

3.  Inspect the `cm_agent` logs to determine the trigger. Check for `failed to connect to datanode` (connection failure) or `sqlcommand failed` (SQL execution failure during health checks). Once a phony dead state is confirmed, investigate the database logs for the underlying cause.

## Root Cause

1.  **cm_agent Phony Dead Detection Mechanism**:
    The `cm_agent` performs periodic health checks on the database. It reads the process status from `/proc/%d/status` to check for states `T` (stopped), `Z` (zombie), or `D` (uninterruptible sleep).
    *   State `D` is not reported as phony dead because it typically requires a server reboot.
    *   State `Z` is transient and handled by the OS.
    If the process is not in the aforementioned states, `cm_agent` attempts to connect to the database and execute a test SQL statement. A connection failure or SQL execution failure is interpreted as a database hang, triggering the phony dead logic.
2.  **cm_server Handling Mechanism**:
  Upon receiving a phony dead report from `cm_agent`, the `cm_server` tallies the occurrences. Once the count reaches or exceeds `phony_dead_effective_time` (default: 5 times), the `cm_server` instructs the `cm_agent` to forcefully kill the database process. After termination, the `cm_server` enforces a cooldown period defined by `instance_phony_dead_restart_interval` (default: 21600 seconds) before allowing further checks on that instance.
