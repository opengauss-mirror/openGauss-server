--rollback TABLE
DROP INDEX IF EXISTS pg_toast.pg_toast_4885_index;
DROP TYPE IF EXISTS pg_toast.pg_toast_4885;
DROP TABLE IF EXISTS pg_toast.pg_toast_4885;
DROP INDEX IF EXISTS pg_statistic_history_tab_statype_attnum_index;
DROP INDEX IF EXISTS pg_statistic_history_current_analyzetime_relid_index;
DROP TYPE IF EXISTS pg_catalog.pg_statistic_history;
DROP TABLE IF EXISTS pg_catalog.pg_statistic_history;

DROP INDEX IF EXISTS pg_statistic_lock_index;
DROP TYPE IF EXISTS pg_catalog.pg_statistic_lock;
DROP TABLE IF EXISTS pg_catalog.pg_statistic_lock;
