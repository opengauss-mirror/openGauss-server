/*------ add sys fuction gs_stat_undo ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_undo();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4434;
CREATE FUNCTION pg_catalog.gs_stat_undo(OUT curr_used_zone_count int4, OUT top_used_zones text, OUT curr_used_undo_size int4,
OUT undo_threshold int4, OUT oldest_xid_in_undo oid, OUT oldest_xmin oid, OUT total_undo_chain_len oid, OUT max_undo_chain_len oid,
OUT create_undo_file_count int4, OUT discard_undo_file_count int4)
RETURNS record LANGUAGE INTERNAL as 'gs_stat_undo';