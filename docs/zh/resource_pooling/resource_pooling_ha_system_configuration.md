# 资源池化高可用系统配置

## 原理介绍

资源池化架构的故障恢复机制与传统的主备架构存在区别：由于资源池化下底层采用共享存储，因此备机无需接收来自主机的预写式日志，也无需对以上日志进行重演。当故障发生后，备机根据主机控制文件中的redo点开始恢复，直到恢复到主机的最后一条日志为止。因此，资源池化架构下的RTO时间与该阶段需要回放的日志量有关，即主机最新日志位置 - redo点的日志位置。为了保证资源池化部署下的RTO时间，需要额外关注REDO日志量，可以使用如下SQL进行查询。

    ```
    // 预置如下自定义函数
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
    
    // 使用如下SQL查询回放日志量，其中的REDO GAP 字段即表示系统发生故障后需要回放的日志量。如下所示当前时刻如果主机如果发生故障，集群恢复需要回放247MB日志
    openGauss=# select * from query_redo_gap;
     node_name | pgwr_actual_flush_total_num | pgwr_last_flush_num | remain_dirty_page_num | queue_head_page_rec_lsn | queue_rec_lsn | current_xlog_in
    kpt_redo  |  REDO GAP(MB)
    -----------+-----------------------------+---------------------+-----------------------+-------------------------+---------------+----------------
    ----------+----------------
     node0     |                    32280595 |                1360 |                 49473 | A/500F0248              | A/5F6800A8    | A/5F830648
    292833864 | 247.2509765625
    ```
当查询到的REDO GAP越大，系统故障后需要回放的日志越多，RTO时间越长。如果发现系统的REDO GAP不断增加，则说明主机当前刷脏速度跟不上脏页生成速度，需要考虑提高刷盘能力或降低业务压力。提高刷盘能力可以尝试降低pagewrite_sleep间隙、提高pagewriter_thread_num数量、降低incremental_checkpoint_timeout间隔等方式。(详见[后端写线程](../database_reference/background_writer.md)和[检查点](../database_reference/checkpoints.md))

## 回放方式选择

池化架构当前有串行回放、并行回放、极致RTO回放、极致RTO按需回放、极致RTO按需回放实时构建可以选择，对应关系如下：

| 回放模式                | 开启参数                                                     | 回放速度 | 对内存消耗 | 适用场景                                                     |
| ----------------------- | ------------------------------------------------------------ | -------- | ---------- | ------------------------------------------------------------ |
| 串行回放                | recovery_max_workers = 1                                     | 慢       | 低         | 不推荐使用                                                   |
| 并行回放                | recovery_max_workers > 1, recovery_parse_workers = 1         | 较慢     | 适中       | 不推荐适用                                                   |
| 极致RTO回放             | ss_enable_ondemand_recovery = off, recovery_parse_workers > 1, recovery_redo_workers >= 1 | 适中     | 适中       | 在一般系统中使用，当REDO GAP较大或RTO不敏感场景下推荐使用    |
| 极致RTO按需回放         | ss_enable_ondemand_recovery = on, ss_enable_ondemand_realtime_build = off, recovery_parse_workers > 1, recovery_redo_workers >= 1 | 较快     | 较高       | 在RTO敏感系统使用，需要控制REDO GAP不能过大                  |
| 极致RTO按需回放实时构建 | ss_enable_ondemand_recovery = on, ss_enable_ondemand_realtime_build = on, recovery_parse_workers > 1, recovery_redo_workers >= 1 | 最快     | 较高       | 在RTO敏感系统使用，需要控制REDO GAP不能过大，且备机未发生故障时也会消耗CPU和IO |

串行回放、并行回放、极致RTO回放、极致RTO按需回放都是故障后才会启动的回放，非故障状态下不会启动回放线程，不消耗CPU资源；极致RTO按需回放实时构建在非故障状态下也会启动回放线程，实时构建当前的日志，降低故障发生后的日志构建时间，加速RTO，因此会消耗一定的CPU资源。

此外，极致RTO按需回放、极致RTO按需回放实时构建会将需要回放的内容存放在内存中，需要消耗大量的内存资源，开启该特性后在备机启动阶段就会预占内存，不推荐在内存较小的环境上使用。如果由于内存不足导致回放期间频繁触发hashmap清理，回放总耗时甚至会比使用极致RTO回放更高。
