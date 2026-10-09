-- ============================================================
-- WDR report - global key activity summary (version number: 93161)
-- add function and view: dbe_perf.get_global_key_activity,
-- dbe_perf.global_key_activity (SQL function, no OID required)
-- snapshot tables are maintained by the maindb script only: the WDR
-- snapshot machinery connects to the initial database exclusively,
-- so no snapshot operations in this script.
-- ============================================================

CREATE OR REPLACE FUNCTION dbe_perf.get_global_key_activity()
RETURNS TABLE(activity_type text, activity_name text, total_count bigint, total_wait_us bigint,
    total_size_bytes bigint
) AS $$
WITH
    stat_agg AS (
        SELECT
            SUM(blks_hit)::bigint AS blks_hit, SUM(blks_read)::bigint AS blks_read,
            SUM(xact_commit)::bigint AS xact_commit, SUM(xact_rollback)::bigint AS xact_rollback,
            SUM(deadlocks)::bigint AS deadlocks, SUM(tup_returned)::bigint AS tup_returned,
            SUM(tup_fetched)::bigint AS tup_fetched, SUM(tup_inserted)::bigint AS tup_inserted,
            SUM(tup_updated)::bigint AS tup_updated, SUM(tup_deleted)::bigint AS tup_deleted,
            SUM(temp_files)::bigint AS temp_files, SUM(temp_bytes)::bigint AS temp_bytes
        FROM dbe_perf.GLOBAL_STAT_DATABASE
    ),
    bgw_agg AS (
        SELECT
            SUM(buffers_alloc)::bigint AS buffers_alloc, SUM(buffers_checkpoint)::bigint AS buffers_checkpoint,
            SUM(buffers_clean)::bigint AS buffers_clean, SUM(buffers_backend)::bigint AS buffers_backend,
            SUM(checkpoints_timed)::bigint AS checkpoints_timed, SUM(checkpoints_req)::bigint AS checkpoints_req,
            SUM(checkpoint_write_time)::bigint AS checkpoint_write_time,
            SUM(checkpoint_sync_time)::bigint AS checkpoint_sync_time
        FROM dbe_perf.GLOBAL_BGWRITER_STAT
    ),
    wait_lock AS (
        SELECT
            SUM(wait)::bigint AS wait_lock_wait, SUM(total_wait_time)::bigint AS wait_lock_total_wait,
            SUM(failed_wait)::bigint AS wait_lock_failed
        FROM dbe_perf.GLOBAL_WAIT_EVENTS WHERE type = 'LOCK_EVENT'
    ),
    wait_lwlock AS (
        SELECT
            SUM(wait)::bigint AS wait_lwlock_wait, SUM(total_wait_time)::bigint AS wait_lwlock_total_wait,
            SUM(failed_wait)::bigint AS wait_lwlock_failed
        FROM dbe_perf.GLOBAL_WAIT_EVENTS WHERE type = 'LWLOCK_EVENT'
    ),
    file_io_agg AS (
        SELECT SUM(phyrds)::bigint AS phyrds, SUM(phywrts)::bigint AS phywrts,
            SUM(phyblkrd)::bigint AS phyblkrd, SUM(phyblkwrt)::bigint AS phyblkwrt,
            SUM(readtim)::bigint AS readtim, SUM(writetim)::bigint AS writetim
        FROM dbe_perf.GLOBAL_FILE_IOSTAT
    ),
    redo_agg AS (
        SELECT SUM(phywrts)::bigint AS redo_phywrts,  SUM(phyblkwrt)::bigint AS redo_phyblkwrt,
            SUM(writetim)::bigint AS redo_writetim
        FROM dbe_perf.GLOBAL_FILE_REDO_IOSTAT
    )
-- 1. Buffer Activity
SELECT 'Buffer'::text AS activity_type, 'Logical Read'::text AS activity_name, blks_hit AS total_count,
      NULL::bigint AS total_wait_us, NULL::bigint AS total_size_bytes FROM stat_agg
UNION ALL
SELECT 'Buffer'::text, 'Physical Read'::text, blks_read, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Buffer'::text, 'Buffer Allocation'::text, buffers_alloc, NULL, NULL FROM bgw_agg
UNION ALL
SELECT 'Buffer'::text, 'Checkpoint Write Buffer'::text, buffers_checkpoint, NULL, NULL FROM bgw_agg
UNION ALL
SELECT 'Buffer'::text, 'Backend Write Buffer'::text, buffers_clean, NULL, NULL FROM bgw_agg
UNION ALL
SELECT 'Buffer'::text, 'Backend Direct Write Buffer'::text, buffers_backend, NULL, NULL FROM bgw_agg
-- 2. Transaction Activity
UNION ALL
SELECT 'Transaction'::text, 'Transaction Commit'::text, xact_commit, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Transaction'::text, 'Transaction Rollback'::text, xact_rollback, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Transaction'::text, 'Deadlock Count'::text, deadlocks, NULL, NULL FROM stat_agg
-- 3. Lock Activity
UNION ALL
SELECT 'Lock'::text, 'Heavyweight Lock Wait'::text, wait_lock_wait, wait_lock_total_wait, NULL FROM wait_lock
UNION ALL
SELECT 'Lock'::text, 'Lightweight Lock Wait'::text, wait_lwlock_wait, wait_lwlock_total_wait, NULL FROM wait_lwlock
UNION ALL
SELECT 'Lock'::text, 'Lock Wait Failed'::text,
      COALESCE(wait_lock_failed, 0) + COALESCE(wait_lwlock_failed, 0) AS total_count,
      NULL, NULL FROM (SELECT 1) dummy LEFT JOIN wait_lock ON true LEFT JOIN wait_lwlock ON true
-- 4. WAL Activity
UNION ALL
SELECT 'WAL'::text, 'WAL Write Count'::text, redo_phywrts, NULL, NULL FROM redo_agg
UNION ALL
SELECT 'WAL'::text, 'WAL Write Data Size'::text, NULL, NULL, (redo_phyblkwrt * 8192)::bigint FROM redo_agg
UNION ALL
SELECT 'WAL'::text, 'WAL Write Elapse'::text, NULL, redo_writetim, NULL FROM redo_agg
UNION ALL
SELECT 'WAL'::text, 'Checkpoint Count'::text,
      (checkpoints_timed + checkpoints_req)::bigint, NULL, NULL FROM bgw_agg
UNION ALL
SELECT 'WAL'::text, 'Checkpoint Write Elapse'::text, NULL,
      (checkpoint_write_time * 1000)::bigint, NULL FROM bgw_agg
UNION ALL
SELECT 'WAL'::text, 'Checkpoint Sync Elapse'::text, NULL,
      (checkpoint_sync_time * 1000)::bigint, NULL FROM bgw_agg
-- 5. SMGR Activity
UNION ALL
SELECT 'SMGR'::text, 'Data File Read Count'::text, phyrds, NULL, NULL FROM file_io_agg
UNION ALL
SELECT 'SMGR'::text, 'Data File Write Count'::text, phywrts, NULL, NULL FROM file_io_agg
UNION ALL
SELECT 'SMGR'::text, 'Data Block Read Count'::text, phyblkrd, NULL, NULL FROM file_io_agg
UNION ALL
SELECT 'SMGR'::text, 'Data Block Write Count'::text, phyblkwrt, NULL, NULL FROM file_io_agg
UNION ALL
SELECT 'SMGR'::text, 'Data File Read Elapse'::text, NULL, readtim, NULL FROM file_io_agg
UNION ALL
SELECT 'SMGR'::text, 'Physical Write Elapse'::text, NULL, writetim, NULL FROM file_io_agg
-- 6. Executor Activity
UNION ALL
SELECT 'Executor'::text, 'Returned Rows'::text, tup_returned, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Fetched Rows'::text, tup_fetched, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Inserted Rows'::text, tup_inserted, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Updated Rows'::text, tup_updated, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Deleted Rows'::text, tup_deleted, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Temporary File Count'::text, temp_files, NULL, NULL FROM stat_agg
UNION ALL
SELECT 'Executor'::text, 'Temp File Size'::text, NULL, NULL, temp_bytes FROM stat_agg
$$ LANGUAGE SQL STABLE;

-- node_name labels instance-level aggregates (cluster-wide sums), not per-node rows
CREATE OR REPLACE VIEW dbe_perf.global_key_activity AS SELECT node_name, t.* FROM dbe_perf.node_name, dbe_perf.get_global_key_activity() t;
GRANT SELECT ON dbe_perf.global_key_activity TO PUBLIC;
