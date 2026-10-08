-- WDR key activity: dbe_perf function and global view
SELECT * FROM dbe_perf.get_global_key_activity() LIMIT 0;
SELECT * FROM dbe_perf.global_key_activity LIMIT 0;
-- run a small workload so that counters move
CREATE TABLE key_activity_t(a int);
INSERT INTO key_activity_t SELECT g FROM generate_series(1, 1000) g;
-- the aggregate always produces activity rows of the known types
SELECT count(*) > 0 AS has_activities FROM dbe_perf.global_key_activity WHERE activity_type IN ('Buffer','Transaction','Lock','WAL','SMGR','Executor');
SELECT count(*) > 0 AS has_commit FROM dbe_perf.global_key_activity WHERE activity_name = 'Transaction Commit' AND COALESCE(total_count, 0) >= 1;
SELECT COALESCE(bool_and(total_count IS NULL OR total_count >= 0), true) AS counters_valid FROM dbe_perf.global_key_activity;
DROP TABLE key_activity_t;
