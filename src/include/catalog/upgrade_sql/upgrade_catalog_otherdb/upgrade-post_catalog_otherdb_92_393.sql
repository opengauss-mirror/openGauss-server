DROP FUNCTION IF EXISTS pg_catalog.gs_current_xlog_insert_end_location() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2860;
CREATE OR REPLACE FUNCTION pg_catalog.gs_current_xlog_insert_end_location()
RETURNS text LANGUAGE INTERNAL STRICT as 'gs_current_xlog_insert_end_location';
