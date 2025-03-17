DROP FUNCTION IF EXISTS pg_catalog.to_char(unknown, unknown) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_history_memory_detail() cascade;
DROP FUNCTION IF EXISTS gs_is_dw_io_blocked() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_zone(int4, boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_spaces(int4, boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_slot(int4, boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_record(bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(int4, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;