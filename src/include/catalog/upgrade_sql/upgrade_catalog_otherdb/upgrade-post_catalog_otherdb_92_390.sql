DROP FUNCTION IF EXISTS pg_catalog.gs_set_obs_delete_location_with_slotname(cstring, cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9035;
CREATE OR REPLACE FUNCTION pg_catalog.gs_set_obs_delete_location_with_slotname
(  cstring,
   cstring)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_set_obs_delete_location_with_slotname';
