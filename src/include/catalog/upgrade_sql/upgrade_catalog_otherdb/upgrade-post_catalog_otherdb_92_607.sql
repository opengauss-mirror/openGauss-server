/*------ add sys fuction gs_is_recycle_obj ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4896;
CREATE OR REPLACE FUNCTION pg_catalog.gs_is_recycle_obj(IN classid Oid, IN objid Oid, IN objname name, OUT output_result bool) 
RETURNS bool LANGUAGE INTERNAL STABLE as 'gs_is_recycle_obj';
