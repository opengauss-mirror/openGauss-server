# 停库或倒换过程中做checkpoint慢的问题定位和分析

## 一、问题现象

在执行stop数据库的某一个DN节点或者执行主备倒换时，数据库长时间处于checkpoint阶段。

## 二、常见原因及分析手段

无论是执行stop数据库或者执行主备倒换，此时触发的checkpoint都属于shutdown类型的checkpoint。shutdown类型的checkpoint与正常运行态的checkpoint不同，是需要做全量checkpoint的（平时运行时的checkpoint一般属于增量checkpoint）。

1. 全量checkpoint，此时需要将内存中所有的脏页刷盘，在运行日志中有如下打印可以确认`will do full checkpoint, need flush xx pages`。在openGauss 3.0.0以后的版本中，刷脏页的操作均由pagewriter线程完成，checkpoint线程在shutdown阶段仅仅只是需要等待pagewriter线程把所有的脏页刷盘。当超过1分钟还是没有刷完时，运行日志中会打印如下信息`incremental checkpoint mode, shuting down, wait for dirty page flush, remain num`。此时可以通过`remain num`后面的剩余脏页数量是否在持续减少来判断当前刷脏页的流程是否卡住。
2. 如果脏页变化的数量比较慢，可以尝试排查下是否是系统IO问题，通过`iostat -xdk 1`每秒打印一次当前系统的IO信息，可以关注`rkB/s`、`wkB/s`和`%util`，看磁盘IO利用率是否过高，或者写速率是否过低，排除下是否使磁盘的硬件性能问题。一般该阶段是无法在连接到数据库中的，因为当前处于PM_SHUTDOWN阶段。
3. 正常运行状态下可以通过如下视图查看当前pagewriter的刷脏效率，以及当前剩余脏页的数量。

    ```shell
    openGauss=# select * from local_pagewriter_stat();
         node_name     | pgwr_actual_flush_total_num | pgwr_last_flush_num | remain_dirty_page_num | queue_head_page_rec_lsn | queue_rec_lsn |     current_xlog_insert_lsn | ckpt_
    redo_point
    -------------------+-----------------------------+---------------------+-----------------------+-------------------------+---------------    +-------------------------+------
    -----------
     dn_6001_6002_6003 |                     3008152 |                   1 |                     0 | 0/0                     | 12/13BC8F98   | 12/    13BC9228             | 12/13
    BC8F98
    (1 row)
    
    openGauss=#
    ```

4. 如果观察到此时需要刷的脏页过多，说明在正常运行时，pagewriter刷脏的速度不够快。遇到这种情况可以尝试调整如下GUC参数看有否优化：

    |参数名|调整建议|
    |---|---|
    |pagewriter_sleep|可以适当调小，加快页面刷盘|
    |max_io_capacity|设置后端写线程批量刷页每秒的IO上限，可以适当调大|
    |max_redo_log_size|恢复点到当前最新日志之间日志量的期望值，可以适当调小|

    更多可参考[数据库I/O参数调优](../performance_tuning_guide/optimizing_database_parameters.md)。

5. 资源池化集群架构下由于底层存储对接的是磁阵，单从操作系统这一侧可能无法看到磁阵上的状态。这时可以登录到磁阵的管理界面中，看当前磁阵是否有异常告警，或者在磁阵中监控使用的LUN的IO性能。
6. 磁盘IO的调度方式也会影响刷脏的性能，当前在服务器上经常使用的是固态硬盘，对于固态硬盘，建议使用NOOP（也叫NONE）的调度策略。可参考如下方式查看和修改：

    ```shell
    # cat /sys/block/$DEVICE-NAME/queue/scheduler
    [mq-deadline] kyber bfq none
    []中即为当前使用的磁盘IO调度模式。
    # echo none > /sys/block/$DEVICE-NAME/queue/scheduler
    使用echo修改即可。
    ```

## 三、收集相关信息

1. 对于停库时checkpoint卡住的问题，一般先要确认卡在哪个阶段，除了前面提到的一些运行日志中的关键字信息外，最快的方式是通过打印数据库进程的堆栈来确认，打印数据库进程堆栈的方法可以使用`gstack <gaussdb进程的PID>`或者参考[内置stack工具](https://docs.opengauss.org/zh/docs/latest/characteristic_description/built_in_stack_tool.html)。
2. 收集数据库运行日志：需要收集发生时刻的`$GAUSSLOG/pg_log`下面的日志，建议往前收集到包含最后一次正常运行时checkpoint执行的日志。
3. 操作系统当前IO状态，通过`iostat -xdk 1`重定向到某个临时文件中，建议收集持续10~30秒的。
