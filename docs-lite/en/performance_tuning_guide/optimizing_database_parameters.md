# Optimizing Database Parameters<a name="EN-US_TOPIC_0289900754"></a>

To ensure high performance of the database, it is recommended that you set the database system parameters \(GUC parameters\) based on the hardware resources and actual services. This section describes GUC parameters that affect the database performance. For details about how to set GUC parameters, see the  _Administrator Guide_.

## Optimizing Database Memory Parameters<a name="EN-US_TOPIC_0289900947"></a>

The performance of complex query statements strongly depends on the configuration parameters of the database memory. The database memory parameters include the control parameters for logical memory management and parameters determining whether execution operators are spilled to disks.

### Parameter for Logical Memory Management<a name="en-us_topic_0283136881_en-us_topic_0237121495_en-us_topic_0073253552_en-us_topic_0062863366_section6641095815423"></a>

**max\_process\_memory**  is a parameter used for logical memory management. It specifies the maximum available memory on each database node. Set this parameter by referring to  [max\_process\_memory](https://docs.opengauss.org/en/docs/latest-lite/database_reference/memory.html#en-us_topic_0283136786_en-us_topic_0237124699_en-us_topic_0059777577_sbebcee7acf2042dc8824982f22a2b4a8).

Use the following formula to calculate the available memory for job execution:

**max\_process\_memory**  – Shared memory \(including  **shared\_buffers**\) –  **cstore\_buffers**

Therefore, the memory available to job execution depends on  **shared\_buffers**  and  **cstore\_buffers**.

Views for logical memory management are provided to display the used memory and peak information in each database block. You can connect to a database node and run  **pg\_total\_memory\_detail** to query information about the memory usage on this database node.

When the specified physical memory is insufficient,  **work\_mem**  determines whether to write additional operator calculation data into temporary tables based on query characteristics and concurrency. This reduces performance by five to 10 times and prolongs the query response time from seconds to minutes.

- For complex serial queries, each query requires five to ten associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/10.
- For simple serial queries, each query requires two to five associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/5.
- For concurrent queries, set  **work\_mem**  using the following formula:  **work\_mem**  =  **work\_mem**  for serial queries/Number of concurrent SQL statements.

### Parameter Determining Whether to Spill Execution Operators to Disks<a name="en-us_topic_0283136881_en-us_topic_0237121495_en-us_topic_0073253552_en-us_topic_0062863366_section14594953151011"></a>

**work\_mem**  sets the used memory threshold. Execution operators that can be spilled to disks will be written when the used memory exceeds the threshold. Such execution operators include Hash\(VecHashJoin\), Agg\(VecAgg\), Sort\(VecSort\), Material\(VecMaterial\), SetOp\(VecSetOp\), and WindowAgg\(VecWindowAgg\). They can be vectorized or non-vectorized. This parameter ensures concurrent throughput and the performance of a single query job. Therefore, you need to optimize the parameter based on the output of  **Explain Performance**.

## Optimizing Database I/O Parameter<a name="EN-US_TOPIC_0000001149231237"></a>

### I/O Parameters<a name="section181599115402"></a>

- **pagewriter\_sleep**: controls the page flushing frequency of the backend write process pagewriter in incremental checkpoint mode. When the ratio of dirty pages to the value of  **shared\_buffers**  reaches the value of  **dirty\_page\_percent\_max**, the number of dirty pages in each batch is calculated based on the value of  **max\_io\_capacity**. The pagewriter thread is used to push the recovery point. If the pagewriter thread is set to a large value, the recovery point is pushed slowly, the system breaks down and starts for a long time, and Xlogs are stacked.

    To reduce the RTO and log bloat, you need to decrease the value of  **pagewriter\_sleep**  to accelerate disk flushing, promote the recovery point, and promote log recycling.

- **bgwriter\_delay**: controls the page flushing frequency of the backend writer process bgwriter in incremental checkpoint mode. When the ratio of the number of idle buffer pages to the value of  **shared\_buffers**  is less than the value of  **candidate\_buf\_percent\_target**, the number of dirty pages in each batch is calculated based on the value of  **max\_io\_capacity**. The bgwriter thread flushes obsolete pages to disks to accelerate the slot occupation speed during service execution. If the time is too long, the performance will be affected.

    To improve service performance, set  **bgwriter\_delay**  to a smaller value.

- **max\_io\_capacity**: specifies the I/O upper limit per second for the backend write processes \(pagewriter and bgwriter\) to flush pages in batches. Set this parameter based on the service scenario and disk I/O capability. If the RTO is short or the data volume is much larger than the shared memory, and the service access data volume is random, the value of this parameter cannot be too small. A small parameter value reduces the number of pages flushed by the backend write process. If a large number of pages are eliminated due to service triggering, the services are affected.

    **max\_io\_capacity**  must be set based on the optimal random write I/O capability.
