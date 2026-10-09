# Configuring Operator Memory Borrowing

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:49:27.826Z -->

The operator memory borrowing feature of openGauss is implemented based on the Lingqu memory borrowing capability. It allows certain operators to dynamically borrow idle memory resources from remote nodes during query execution. By expanding the work_mem available to operators, this feature effectively improves the execution efficiency of memory-intensive operations.

## Applicable Scenarios and Limitations

Environment requirements: The environment must support and be configured with the Lingqu memory borrowing capability. Additionally, remote nodes configured in the cluster must have free memory resources.

- Applicable Scenarios
    - Large analytical queries: When processing large-scale datasets, substantial memory space is required for Sort/HashJoin/Agg operations. Insufficient memory triggers extensive spilling to disk, causing significant performance degradation.
    - Insufficient local memory: When the local node has tight memory resources but other nodes in the cluster have free memory.

- Operators that support dynamic memory borrowing:
    - HashAgg
    - HashJoin
    - Sort
    - Sonic HashAgg / Vector HashAgg
    - Sonic HashJoin / Vector HashJoin

## Impact of Cluster Resources on Operator Memory Borrowing

Operator memory borrowing is a solution that leverages idle remote memory to accelerate memory-intensive operators. When this feature is enabled, if remote memory is idle and the query itself is memory-intensive, significant performance improvement can be achieved. However, if the remote memory is busy and cannot be lent, the system may fall back to local-memory-only mode, resulting in no noticeable performance improvement compared to when the feature is disabled.

After the feature is enabled, the memory available to operators is constrained by both work_mem and borrow_work_mem. For details about work_mem, refer to the SMP description. The upper limit of memory borrowing for a single operator is restricted by borrow_work_mem and the available remote memory. In the worst-case scenario, the system falls back to local-memory-only mode.

When properly configured, operator memory borrowing can deliver the following benefits:

1. Reduce or even eliminate spill-to-disk operations caused by insufficient memory
2. Improve the execution efficiency of memory-intensive operators by 30% (typical TPCH 1T scenario)
3. Improve overall resource utilization in mixed-load clusters

## Configuration and Usage

To enable the operator memory borrowing feature, perform the following configurations:

1. Modify the configuration file (postgresql.conf) to set the maximum available memory borrowing size. This parameter requires an openGauss restart to take effect.

    ```
    # Total available borrowed memory for this openGauss instance, shared by operator memory borrowing and other features (such as HTAP borrowing).
    max_rack_memory = 64GB
    ```

2. Configure the borrowable memory for operators

    ```
    set borrow_work_mem = 16GB
    ```

## Usage Restrictions and Precautions

1. Remote node load: The cluster must have sufficient idle resources.
2. The restrictions on borrow_work_mem are largely the same as those on work_mem. The actual upper limit of memory that may be consumed equals SMP concurrency *operator* (work_mem + borrow_work_mem).
3. Resource contention: A large number of concurrent borrowing operations across the cluster may lead to resource contention. Performance is affected by the performance of the memory borrowing interface.

## Reliability

In operator memory borrowing scenarios, if memory release fails while the process is still running, residual memory remains within the process and cannot be reclaimed by the MXE background mechanism. Therefore, a fault handling mechanism is required to manage this memory. By enabling enable_rack_memory_cleaner, a memory cleanup thread is started to collect and uniformly release the residual memory.

### enable_rack_memory_cleaner

**Parameter description**: Controls the background thread for UBSE cluster status query and residual memory cleanup in Lingqu scenarios. In UBSE failure scenarios, the operator memory borrowing feature uses this thread to detect the failure, stops memory borrowing, and falls back to the native execution flow to ensure normal statement execution. Additionally, in failure scenarios, the release of borrowed memory also fails. When this thread is enabled, it collects memory that failed to be released and performs a unified release after the UBSE functionality becomes available again.

This parameter is of the SIGHUP type. For the setting method, see the corresponding method in [Table 1](https://docs.opengauss.org/en/docs/latest/database_administration_guide/reset_parameters.html#en_topic_0283137176_en_topic_0237121562_en_topic_0059777490_t91a6f212010f4503b24d7943aed6d846).

**Value range**: Boolean

- `on`/`true` enables the query cleanup thread.
- `off`/`false` indicates that the query cleanup thread is not enabled.

**Default value**: `off`/`false`.

**Setting suggestion:**
In non-Lingqu scenarios, do not enable this parameter to avoid thread conflicts with database performance.