DROP FUNCTION IF EXISTS pg_catalog.gs_get_hba_conf() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2873;

CREATE FUNCTION pg_catalog.gs_get_hba_conf()
RETURNS record LANGUAGE INTERNAL VOLATILE STRICT as 'gs_get_hba_conf';
comment on function pg_catalog.gs_get_hba_conf() is 'config: information about pg_hba conf file';