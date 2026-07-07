# Optimizing Database Parameters<a name="EN-US_TOPIC_0245374529"></a>

To ensure high performance of the database, it is recommended that you set the database system parameters \(GUC parameters\) based on the hardware resources and actual services. This section describes GUC parameters that affect the database performance. For details about how to set GUC parameters, see the  _Administrator Guide_.

## Optimizing Database Memory Parameters<a name="EN-US_TOPIC_0245374530"></a>

The performance of complex query statements strongly depends on the configuration parameters of the database memory. The database memory parameters include the control parameters for logical memory management and parameters determining whether execution operators are spilled to disks.

### Parameter for Logical Memory Management<a name="en-us_topic_0237121495_en-us_topic_0073253552_en-us_topic_0062863366_section6641095815423"></a>

**max\_process\_memory**  is a parameter used for logical memory management. It specifies the maximum available memory on each database node. Set this parameter by referring to  [max\_process\_memory](memory.md).

Use the following formula to calculate the available memory for job execution:

**max\_process\_memory**  – Shared memory \(including  **shared\_buffers**\) –  **cstore\_buffers**

Therefore, the memory available to job execution depends on  **shared\_buffers**  and  **cstore\_buffers**.

Views for logical memory management are provided to display the used memory and peak information in each database block. You can connect to a database node and run  **pg\_total\_memory\_detail**  to query information about the memory usage on this database node. Alternatively, you can connect to the primary node of the database and run  **pgxc\_total\_memory\_detail**  to query information about the memory usage on all the database nodes.

When the specified physical memory is insufficient,  **work\_mem**  determines whether to write additional operator calculation data into temporary tables based on query characteristics and concurrency. This reduces performance by five to 10 times and prolongs the query response time from seconds to minutes.

- For complex serial queries, each query requires five to ten associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/10.
- For simple serial queries, each query requires two to five associated operations. Set  **work\_mem**  using the following formula:  **work\_mem**  = 50% of the memory/5.
- For concurrent queries, set  **work\_mem**  using the following formula:  **work\_mem**  =  **work\_mem**  for serial queries/Number of concurrent SQL statements.

### Parameter Determining Whether to Spill Execution Operators to Disks<a name="en-us_topic_0237121495_en-us_topic_0073253552_en-us_topic_0062863366_section14594953151011"></a>

**work\_mem**  sets the used memory threshold. Execution operators that can be spilled to disks will be written when the used memory exceeds the threshold. Such execution operators include Hash\(VecHashJoin\), Agg\(VecAgg\), Sort\(VecSort\), Material\(VecMaterial\), SetOp\(VecSetOp\), and WindowAgg\(VecWindowAgg\). They can be vectorized or non-vectorized. This parameter ensures concurrent throughput and the performance of a single query job. Therefore, you need to optimize the parameter based on the output of  **Explain Performance**.

## Optimizing Concurrent Queue Parameters<a name="EN-US_TOPIC_0245374531"></a>

You can globally or locally control concurrent queues.

### Global Concurrent Queues<a name="en-us_topic_0237121496_en-us_topic_0073253553_en-us_topic_0062863367_section20895258152731"></a>

In a global concurrent queue,  **max\_active\_statements**  controls the number of concurrent jobs on the primary node of the database. All common users' jobs are controlled, regardless of their complexity. When the number of concurrent jobs reaches the specified threshold, the rest of the jobs wait in a queue. Administrators' jobs are not under such control.

Set this parameter based on system capacities, such as memory and I/O usage. In a resource pool associated with common users, if the jobs of different priorities occupy different portions, they will be queued by priority first. Then, the jobs of the same priority are queued. Jobs in the queue of highest priority will be woken up first.

>[!NOTE]NOTE   
>
>- In a high transactional concurrency scenario, you are advised to set  **max\_active\_statements**  to  **-1**, indicating that global concurrency is not limited.  
>- In an analytical query scenario, set  **max\_active\_statements**  to the number of CPU cores divided by the number of database nodes. Generally, its value ranges from 5 to 8.  

### Local Concurrent Queues<a name="en-us_topic_0237121496_en-us_topic_0073253553_en-us_topic_0062863367_section43125250152853"></a>

You can locally control the number of concurrent jobs within the same resource pool on the primary node of the database. The number of concurrent complex jobs are controlled based on their cost.

**parctl\_min\_cost**  is used to determine whether a job is complex.
