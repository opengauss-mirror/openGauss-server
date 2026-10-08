-- ============================================================
-- WDR report - global key activity summary rollback (version: 93161)
-- ============================================================

-- delete added functions and views
DROP VIEW IF EXISTS dbe_perf.global_key_activity CASCADE;
DROP FUNCTION IF EXISTS dbe_perf.get_global_key_activity() CASCADE;

-- delete added snapshot tables
DROP TABLE IF EXISTS snapshot.snap_global_key_activity;
