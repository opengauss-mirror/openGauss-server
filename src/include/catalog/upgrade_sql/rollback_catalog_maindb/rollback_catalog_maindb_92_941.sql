--rollback TABLE
DROP INDEX IF EXISTS pg_proc_ext_proc_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_proc_ext;
DROP TABLE IF EXISTS pg_catalog.pg_proc_ext;