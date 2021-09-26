DROP FUNCTION IF EXISTS pg_catalog.gs_get_global_barriers_status() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9034;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_global_barriers_status
(  OUT slot_name text,
   OUT global_barrier_id text,
   OUT global_achive_barrier_id text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_global_barriers_status';