-- thread IO statistics: builtin function, system view and dbe_perf views
-- structure check of the 23 columns exposed by pg_thread_io_stat
SELECT * FROM pg_catalog.pg_thread_io_stat() LIMIT 0;
SELECT * FROM gs_thread_io_stat LIMIT 0;
SELECT * FROM dbe_perf.thread_io_stat LIMIT 0;
SELECT * FROM dbe_perf.global_thread_io_stat LIMIT 0;
-- generate IO activity on the session worker role
CREATE TABLE thread_io_stat_t(a int, b text);
INSERT INTO thread_io_stat_t SELECT g, repeat('x', 64) FROM generate_series(1, 2000) g;
CHECKPOINT;
VACUUM thread_io_stat_t;
-- the worker role must report IO counters after the workload above
SELECT count(*) > 0 AS has_worker FROM gs_thread_io_stat WHERE role_name = 'Worker';
SELECT COALESCE(bool_and(num_reads >= 0 AND num_writes >= 0 AND bytes_read >= 0 AND bytes_written >= 0), true) AS counters_valid FROM gs_thread_io_stat;
SELECT count(*) > 0 AS has_global_rows FROM dbe_perf.global_thread_io_stat;
DROP TABLE thread_io_stat_t;
