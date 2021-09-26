DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_in_recovery() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9138;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hadr_in_recovery
(  OUT is_in_recovery boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_hadr_in_recovery';