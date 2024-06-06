/*------ add sys fuction gs_stat_undo ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_undo();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4434;
CREATE FUNCTION pg_catalog.gs_stat_undo(
    OUT curr_used_zone_count int4, 
    OUT top_used_zones text, 
    OUT curr_used_undo_size int4,
    OUT undo_threshold int4, 
    OUT global_recycle_xid xid, 
    OUT oldest_xmin xid, 
    OUT total_undo_chain_len int8, 
    OUT max_undo_chain_len int8,
    OUT create_undo_file_count int4, 
    OUT discard_undo_file_count int4)
RETURNS SETOF record 
LANGUAGE INTERNAL STABLE NOT SHIPPABLE ROWS 1 as 'gs_stat_undo';