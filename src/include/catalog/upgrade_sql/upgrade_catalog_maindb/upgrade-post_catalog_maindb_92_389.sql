--step1 add new sys function gs_deployment()
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5998;
CREATE FUNCTION pg_catalog.gs_deployment() RETURNS text LANGUAGE INTERNAL IMMUTABLE as 'gs_deployment';
