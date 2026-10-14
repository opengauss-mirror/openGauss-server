# Resource Pooling High Availability System Configuration

## Principle Overview

The failure recovery mechanism in a resource pooling architecture differs fundamentally from traditional primary-standby deployments. Because resource pooling utilizes shared storage, standby nodes do not need to receive or replay Write-Ahead Logs (WAL) from the primary node. Upon a failure, the standby node initiates recovery starting from the redo point recorded in the primary's control file and continues until it replays the last log entry of the primary. Consequently, the RTO in a resource pooling deployment is directly proportional to the volume of logs that must be replayed—specifically, the delta between the latest log location on the primary and the redo point. To guarantee RTO objectives, it is critical to monitor the REDO log gap. The following SQL utilities can be used for this purpose.

    ```
    // Predefine the following custom function
    CREATE OR REPLACE FUNCTION hex_to_int(hexval varchar) RETURNS BIGINT AS $$
    DECLARE
        result bigint;
    BEGIN
        EXECUTE 'SELECT x' || quote_literal(hexval) || '::bigint' INTO result;
        RETURN result;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE STRICT;
    
    create view query_redo_gap as select b.node_name,
        b.pgwr_actual_flush_total_num,
        b.pgwr_last_flush_num,
        b.remain_dirty_page_num,
        b.queue_head_page_rec_lsn,
        b.queue_rec_lsn,
        b.current_xlog_insert_lsn,
        b.ckpt_redo_point,
        (hex_to_int(substring(b.current_xlog_insert_lsn, 1, b.curr_pos - 1)) * 4 * 1024 * 1024 * 1024 +
        hex_to_int(substring(b.current_xlog_insert_lsn, b.curr_pos + 1))) as curr_redo,
        (hex_to_int(substring(b.ckpt_redo_point, 1, b.redo_pos - 1)) * 4 * 1024 * 1024 * 1024 +
        hex_to_int(substring(b.ckpt_redo_point, b.redo_pos + 1))) as ckpt_redo,
        (curr_redo - ckpt_redo) / 1024 / 1024 as "REDO GAP(MB)"
        from (select a.*,
            position('/' IN current_xlog_insert_lsn) as curr_pos,
            position('/' IN ckpt_redo_point) as redo_pos
            from local_pagewriter_stat() a) b;
    
    // Query the log replay volume. The REDO GAP field indicates the amount of logs that must be replayed if a failure occurs. In the example below, if the primary fails at this moment, the cluster would need to replay 247 MB of logs to recover.
    openGauss=# select * from query_redo_gap;
     node_name | pgwr_actual_flush_total_num | pgwr_last_flush_num | remain_dirty_page_num | queue_head_page_rec_lsn | queue_rec_lsn | current_xlog_in
    kpt_redo  |  REDO GAP(MB)
    -----------+-----------------------------+---------------------+-----------------------+-------------------------+---------------+----------------
    ----------+----------------
     node0     |                    32280595 |                1360 |                 49473 | A/500F0248              | A/5F6800A8    | A/5F830648
    292833864 | 247.2509765625
    ```
A larger REDO GAP implies more logs to replay during recovery, resulting in a longer RTO. If the REDO GAP continues to grow, it indicates that the primary node's page flushing speed cannot keep pace with dirty page generation. Consider optimizing disk I/O throughput or reducing business workload pressure. To improve flushing efficiency, you can adjust the following parameters:
*   Decrease `pagewrite_sleep`.
*   Increase `pagewriter_thread_num`.
*   Decrease `incremental_checkpoint_timeout`.

For more details, see [Background Writer](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_reference/background_writer.md) and [Checkpoints](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_reference/checkpoints.md).

## Recovery Mode Selection

The resource pooling architecture supports five recovery modes: Serial Recovery, Parallel Recovery, Ultra-low RTO Recovery, Ultra-low RTO On-Demand Recovery, and Ultra-low RTO On-Demand Real-time Build. The relationship between these modes is summarized below:

| Recovery Mode                      | Activation Parameters                                                                                                                               | Speed  | Memory Footprint | Use Case                                                                                                                                 |
| :--------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :----- | :-------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| **Serial Recovery**                 | `recovery_max_workers = 1`                                                                                                                         | Slow   | Low             | Not recommended.                                                                                                                          |
| **Parallel Recovery**               | `recovery_max_workers > 1`<br>`recovery_parse_workers = 1`                                                                                          | Medium | Medium          | Not recommended.                                                                                                                          |
| **Ultra-low RTO Recovery**         | `ss_enable_ondemand_recovery = off`<br>`recovery_parse_workers > 1`<br>`recovery_redo_workers >= 1`                                                 | Medium | Medium          | Recommended for general systems, especially when the REDO GAP is large or RTO requirements are not strict.                               |
| **Ultra-low RTO On-Demand Recovery** | `ss_enable_ondemand_recovery = on`<br>`ss_enable_ondemand_realtime_build = off`<br>`recovery_parse_workers > 1`<br>`recovery_redo_workers >= 1` | Fast   | High            | Recommended for RTO-sensitive systems; requires keeping the REDO GAP relatively small.                                                    |
| **Ultra-low RTO On-Demand Real-time Build** | `ss_enable_ondemand_recovery = on`<br>`ss_enable_ondemand_realtime_build = on`<br>`recovery_parse_workers > 1`<br>`recovery_redo_workers >= 1` | Fastest| High            | Recommended for RTO-sensitive systems; requires keeping the REDO GAP relatively small. Consumes additional CPU and I/O even during normal operation. |

*   **Serial, Parallel, Ultra-low RTO, and On-Demand Recovery** modes only activate recovery threads *after* a failure occurs. They do not consume CPU resources during normal operation. **Ultra-low RTO On-Demand Real-time Build** mode continuously builds the log replay structure in memory during normal operation. This reduces the build time required immediately after a failure, thereby accelerating RTO, but it incurs a constant CPU overhead.

*   **Ultra-low RTO On-Demand Recovery** and **Ultra-low RTO On-Demand Real-time Build** store replay structures in memory, requiring substantial memory allocation. These features pre-allocate memory during standby startup. They are not recommended for environments with limited memory. If insufficient memory triggers frequent hashmap cleanup cycles, the total recovery time may actually exceed that of standard Ultra-low RTO Recovery.
