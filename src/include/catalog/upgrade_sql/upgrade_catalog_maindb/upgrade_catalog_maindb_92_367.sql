/*------ add sys fuction gs_is_recycle_object ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4895;
CREATE FUNCTION pg_catalog.gs_is_recycle_object(int4, int4, name, OUT gs_is_recycle_object bool) 
RETURNS bool LANGUAGE INTERNAL as 'gs_is_recycle_object';