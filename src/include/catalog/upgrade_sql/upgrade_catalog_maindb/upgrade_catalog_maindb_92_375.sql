-- ----------------------------------------------------------------
-- upgrade pg_pooler_status
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8, OUT used_count int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3955;
CREATE FUNCTION pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8, OUT used_count int8, OUT idx int8, OUT streamid int8) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'pg_stat_get_pooler_status';
