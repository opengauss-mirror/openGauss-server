# 案例：调整I/O相关参数降低日志膨胀率

- 调整参数前的参数值：
  - pagewriter_sleep=2000ms
  - bgwriter_delay=2000ms
  - max_io_capacity=500MB
- 调整参数后的参数值：
  - pagewriter_sleep=100ms
  - bgwriter_delay=1s
  - max_io_capacity=300MB

> [!NOTE]说明
>
> - 将max_io_capacity调整小是因为，IO不会利用到之前参数的最大值，调整该值，是为了限制后端写线程IO的占用上限。
> - 当日志量达到一定量时，日志才会触发回收，该值的计算方式是**：**wal_keep_segments + checkpoint_segments \* 2 + 1 ，假设 checkpoint_segments 设置128，wal_keep_segments 设置128，日志量就是 (128 + 128 \* 2 + 1) * 16MB = 6GB。
> - 调整参数前，tpcc导数阶段，不同的数据量xlog有不同程度的膨胀，基本会导致GB级别的日志膨胀，主要是因为脏页未刷盘，recovery点不能推进，日志不能及时回收。调整参数后，日志膨胀明显降低。
> - 以2000仓为例，调整参数前，导数阶段，日志膨胀10GB，调整参数后，日志基本没有膨胀，维持在设置的参数计算出的xlog最低量的范围内。
