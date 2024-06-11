DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, int4, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT slot_ptr text, OUT gs_undo_translot oid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT slot_ptr text, OUT gs_undo_translot oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4541;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot_dump_slot';

DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT slot_ptr text, OUT gs_undo_translot oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4438;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot_dump_xid';
