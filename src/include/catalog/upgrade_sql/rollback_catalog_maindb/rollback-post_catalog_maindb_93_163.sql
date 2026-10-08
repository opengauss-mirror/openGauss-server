/*------ Rollback dbe_perf OS disk views: os_disk_io_info ------*/
DROP VIEW IF EXISTS DBE_PERF.global_os_disk_io_info CASCADE;
DROP VIEW IF EXISTS DBE_PERF.os_disk_io_info CASCADE;

/*------ rollback builtin functions for dbe_perf OS disk view ------*/
DROP FUNCTION IF EXISTS pg_catalog.pg_os_disk_io_info() CASCADE;
