/*------ Rollback dbe_perf OS net dev views: os_net_dev_info ------*/
DROP VIEW IF EXISTS DBE_PERF.global_os_net_dev_info CASCADE;
DROP VIEW IF EXISTS DBE_PERF.os_net_dev_info CASCADE;

/*------ rollback builtin functions for dbe_perf OS net dev views ------*/
DROP FUNCTION IF EXISTS pg_catalog.pg_os_net_dev_ext() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_os_net_dev_info() CASCADE;
