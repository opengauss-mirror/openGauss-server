# Troubleshooting and Analysis of Slow Checkpoint During Database Shutdown or Switchover

## Symptom

When stopping a specific DN node or performing a primary-standby switchover, the database remains in the checkpoint phase for an extended period.

## Common Causes and Analysis Methods

Whether stopping the database or performing a primary-standby switchover, the checkpoint triggered in both cases is a **shutdown-type checkpoint**. A shutdown-type checkpoint differs from checkpoints during normal operation—it requires a **full checkpoint** (whereas checkpoints during normal operation are typically incremental checkpoints).

1. **Full Checkpoint**: At this stage, all dirty pages in memory must be flushed to disk. The following message in the run log confirms this: `will do full checkpoint, need flush xx pages`. In openGauss 3.0.0 and later versions, the actual page flushing is performed by the pagewriter threads; the checkpoint thread, during the shutdown phase, only needs to wait for the pagewriter threads to finish flushing all dirty pages. When more than one minute has passed without completion, the run log will print: `incremental checkpoint mode, shuting down, wait for dirty page flush, remain num`. At this point, you can check whether the `remain num` value (the number of remaining dirty pages) is continuously decreasing to determine whether the dirty page flush process is stuck.

2. **System I/O Issues**: If the dirty page count decreases slowly, try investigating whether there is a system I/O bottleneck. Use `iostat -xdk 1` to print system I/O information every second. Pay attention to `rkB/s`, `wkB/s`, and `%util` to see whether disk I/O utilization is too high or the write rate is too low, ruling out any disk hardware performance issues. Note that during this phase, it is generally impossible to connect to the database because the system is in the `PM_SHUTDOWN` phase.

3. **Monitoring Pagewriter During Normal Operation**: During normal operation, you can query the following view to check the current pagewriter flush efficiency and the number of remaining dirty pages:

    ```shell
    openGauss=# select * from local_pagewriter_stat();
         node_name     | pgwr_actual_flush_total_num | pgwr_last_flush_num | remain_dirty_page_num | queue_head_page_rec_lsn | queue_rec_lsn |     current_xlog_insert_lsn | ckpt_redo_point
    -------------------+-----------------------------+---------------------+-----------------------+-------------------------+---------------+-------------------------+-----------------
     dn_6001_6002_6003 |                     3008152 |                   1 |                     0 | 0/0                     | 12/13BC8F98   | 12/13BC9228              | 12/13BC8F98
    (1 row)

    openGauss=#
    ```

4. **Tuning Pagewriter Performance**: If the number of dirty pages to be flushed is excessively high, it indicates that the pagewriter flush rate during normal operation is insufficient. In such cases, try adjusting the following GUC parameters to see if performance improves:

    | Parameter Name | Tuning Recommendation |
    | :--- | :--- |
    | `pagewriter_sleep` | Can be appropriately decreased to accelerate page flushing |
    | `max_io_capacity` | Sets the per-second I/O upper limit for batch page flushing by background writer threads; can be appropriately increased |
    | `max_redo_log_size` | Expected log volume between the recovery point and the latest log; can be appropriately decreased |

    For more details, refer to [Optimizing Database Parameters](../performance_tuning_guide/optimizing_database_parameters.md).

5. **Resource Pooling Storage**: In a resource pooling cluster architecture, since the underlying storage is connected to a disk array, the operating system alone may not reveal the status of the array. In this case, log in to the disk array management interface to check for any abnormal alarms, or monitor the I/O performance of the LUNs in use on the array.

6. **Disk I/O Scheduler**: The disk I/O scheduling method also affects dirty page flush performance. Currently, SSDs are commonly used on servers. For SSDs, it is recommended to use the **NOOP** (also called **NONE**) scheduling policy. You can check and modify it as follows:

    ```shell
    # cat /sys/block/$DEVICE-NAME/queue/scheduler
    [mq-deadline] kyber bfq none
    # The value in brackets `[]` is the currently active disk I/O scheduler.

    # echo none > /sys/block/$DEVICE-NAME/queue/scheduler
    # Use echo to modify.
    ```

## Information Collection

1. **Identifying Where It Is Stuck**: For checkpoint stalls during database shutdown, the first step is to determine which phase it is stuck in. Apart from the keyword messages in the run log mentioned above, the fastest method is to print the database process stack. You can use `gstack <PID_of_gaussdb_process>` or refer to the [Built-in Stack Tool](https://atomgit.com/opengauss/docs/blob/master/docs/en/characteristic_description/built_in_stack_tool.md).

2. **Database Run Logs**: Collect the logs under `$GAUSSLOG/pg_log` from the time of the incident. It is recommended to go back far enough to include the logs of the last successful checkpoint during normal operation.

3. **OS I/O Status**: Redirect the output of `iostat -xdk 1` to a temporary file. It is recommended to collect continuously for 10 to 30 seconds.
