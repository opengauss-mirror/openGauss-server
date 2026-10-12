# Node Entering ReadOnly State Due to Insufficient Local Disk Space

## Symptom

During the operation of an openGauss resource pool cluster, if the `ss_enable_check_sys_disk_usage` parameter is enabled, the following symptoms may occur:

1.  When querying the cluster status using `cm_ctl query -Cvipdw`, one node shows a state of `ReadOnly`, while other nodes remain in the `Normal` state.
2.  Write transactions on the primary node fail with the following error:

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

These symptoms indicate that a node has entered the `ReadOnly` state. If the primary node enters this state, write operations are blocked.

## Troubleshooting Method

Follow this procedure to diagnose the issue:

1.  Identify write transaction failures on the primary node. The application or client (e.g., `gsql`) will return the error:

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

    Inspect the database logs in the `$GAUSSLOG/pg_log` directory on the primary node for entries similar to the following:

    ```shell
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] ERROR:  cannot execute INSERT in a read-only transaction
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] CONTEXT:  SQL statement "INSERT INTO user_logs (user_data)
    ```

    This confirms that the primary node is in a read-only state.

2.  Verify the cluster status using `cm_ctl query -Cvipdw`. Confirm that one node is in the `ReadOnly` state. Note that read operations on standby nodes are not affected.

3.  On the **CM Server Primary** node, navigate to the `$GAUSSLOG/cm/cm_server` directory. Inspect the logs around the time of the failure for entries similar to the following:

    ```shell
    2024-10-09 02:39:39.063 tid=1286737 StorageDetect LOG: [PreAlarmForNodeThreshold] [logDisk usage] Pre Alarm threshold reached, node=1, log_disk_usage=93.
    2024-10-09 02:39:39.064 tid=1286737 StorageDetect LOG: [PreAlarmForNodeThreshold] [dataDisk usage] Pre Alarm threshold reached, instanceId=6001,disk_usage=93, shared_disk_usage_for_data=1, shared_disk_usage_for_log=1.
    2024-10-09 02:39:39.064 tid=1286737 StorageDetect LOG: [ReadOnlyActDoNoting] instance 6001 is transaction read only, disk_usage:93,shared_disk_usage_for_data:1, shared_disk_usage_for_log:1, read_only_threshold:85
    ```

    These logs indicate that the `CM Server` detected excessive local disk usage and triggered the `ReadOnly` protection mechanism.

4.  On the affected node, use the `df -h` command to verify disk usage. In this example, the `$PGDATA` directory resides on `/dev/nvme0n1` mounted at `/usr1`:

    ```shell
    Filesystem                  Size  Used Avail Use% Mounted on
    devtmpfs                    383G     0  383G   0% /dev
    tmpfs                       383G  1.1M  383G   1% /dev/shm
    tmpfs                       383G   75M  383G   1% /run
    tmpfs                       383G     0  383G   0% /sys/fs/cgroup
    /dev/mapper/openeuler-root   49G   31G   17G  65% /
    tmpfs                       383G  1.0M  383G   1% /tmp
    /dev/sda2                   976M  108M  801M  12% /boot
    /dev/sda1                   200M  5.8M  195M   3% /boot/efi
    /dev/mapper/openeuler-home  3.6T  818G  2.6T  24% /home
    tmpfs                        77G     0   77G   0% /run/user/1243
    tmpfs                        77G     0   77G   0% /run/user/0
    /dev/nvme0n1                3.0T  2.8T  183G  94% /usr1
    ```

    The 94% usage rate exceeds the default threshold, causing the node to enter the `ReadOnly` state.

## Root Cause

To ensure write availability, each node must maintain local disk usage below the default threshold of `85%`. When the available local disk space drops below `15%`, the system automatically transitions the node to the `ReadOnly` state to prevent data corruption. If the primary node is affected, write operations are suspended.

## Solution

Apply one of the following solutions:

1.  **Free up disk space**: Clean up unnecessary files from the local disk to reduce usage below the threshold.
2.  **Disable disk usage monitoring**: Turn off the local disk capacity check to prevent the node from entering the `ReadOnly` state. Use the following commands:

    ```shell
    cm_ctl set --param --server -k ss_enable_check_sys_disk_usage=0
    cm_ctl reload --param --server
    ```

3.  **Adjust the threshold**: Increase the capacity check threshold to accommodate higher disk usage. Replace `xx` with the desired percentage value:

    ```shell
    cm_ctl set --param --server -k datastorage_threshold_value_check=xx
    cm_ctl reload --param --server
    ```

    After adjusting the threshold, the node will automatically return to the `Normal` state once the condition is met.