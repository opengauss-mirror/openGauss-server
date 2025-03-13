DROP FUNCTION IF EXISTS pg_catalog.to_char(unknown, unknown) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9756; 
CREATE FUNCTION pg_catalog.to_char ( 
unknown,
unknown
) RETURNS text LANGUAGE INTERNAL STABLE SHIPPABLE STRICT as 'unknown_to_char2';

COMMENT ON FUNCTION pg_catalog.to_char(unknown, unknown) is 'unknown,unknown to char for compatibility';

-- DROP: gs_get_parallel_decode_status()
DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text) CASCADE;
-- CREATE: gs_get_parallel_decode_status
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9377;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text, OUT reader_lsn text, OUT working_txn_cnt int8, OUT working_txn_memory int8)
 RETURNS SETOF RECORD
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE
AS $function$gs_get_parallel_decode_status$function$;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_history_memory_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5257;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_history_memory_detail(
memlog_filename cstring,
OUT memory_info text) RETURNS SETOF TEXT LANGUAGE INTERNAL IMMUTABLE NOT FENCED NOT SHIPPABLE ROWS 100 as 'gs_get_history_memory_detail';

DROP FUNCTION IF EXISTS gs_is_dw_io_blocked() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4772;
CREATE OR REPLACE FUNCTION pg_catalog.gs_is_dw_io_blocked(OUT boolean)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1000
AS $function$gs_is_dw_io_blocked$function$;

/*------ add sys fuction gs_undo_meta_dump_zone ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_zone(int4, boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4433;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_zone(zone_id int4, read_memory boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 1 as 'gs_undo_meta_dump_zone';

/*------ add sys fuction gs_undo_meta_dump_spaces ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_spaces(int4, boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4432;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_spaces(zone_id int4, read_memory boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 1 as 'gs_undo_meta_dump_spaces';

/*------ add sys fuction gs_undo_meta_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_slot(int4, boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4437;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_slot(zone_id int4, read_memory boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 1 as 'gs_undo_meta_dump_slot';

/*------ add sys fuction gs_undo_dump_record ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_record(bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4539;
CREATE FUNCTION pg_catalog.gs_undo_dump_record(undoptr bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 1 as 'gs_undo_dump_record';

/*------ add sys fuction gs_undo_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4540;
CREATE FUNCTION pg_catalog.gs_undo_dump_xid(undoptr xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE NOT FENCED NOT SHIPPABLE ROWS 1 as 'gs_undo_dump_xid';

DROP FUNCTION IF EXISTS pg_catalog.array_count(anyarray, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_repair_urq(oid, OUT result text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_meta(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_record(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_translot(text, int8, int8, boolean, OUT zone_id int8, OUT detail text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_urq(oid, text, int8, text, OUT error_code text, OUT detail text) CASCADE;