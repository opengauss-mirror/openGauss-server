/*------ add sys fuction gs_undo_translot_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, int4, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT text, OUT oid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT text, OUT oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4541;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_slot(IN zone_id int4, IN read_memory boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT slot_states oid)
RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 1 as 'gs_undo_translot_dump_slot';

/*------ add sys fuction gs_undo_translot_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT text, OUT oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4438;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_xid(IN zone_id xid, IN read_memory boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT slot_states oid)
RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 1 as 'gs_undo_translot_dump_xid';

/*------ add sys fuction gs_stat_undo ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_undo();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4434;
CREATE FUNCTION pg_catalog.gs_stat_undo(OUT curr_used_zone_count int4, OUT top_used_zones text, OUT curr_used_undo_size int4,
OUT undo_threshold int4, OUT oldest_xid_in_undo oid, OUT oldest_xmin oid, OUT total_undo_chain_len oid, OUT max_undo_chain_len oid,
OUT create_undo_file_count int4, OUT discard_undo_file_count int4)
RETURNS record LANGUAGE INTERNAL as 'gs_stat_undo';
