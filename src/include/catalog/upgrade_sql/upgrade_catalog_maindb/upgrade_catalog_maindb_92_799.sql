DO $upgrade$
BEGIN
IF working_version_num() < 92608 then
/*------ add sys fuction gs_undo_meta_dump_zone ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_zone(int4, boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4433;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_zone(int4, boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_meta_dump_zone';

/*------ add sys fuction gs_undo_meta_dump_spaces ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_spaces(int4, boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4432;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_spaces(int4, boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_meta_dump_spaces';

/*------ add sys fuction gs_undo_meta_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_slot(int4, boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4437;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_slot(int4, boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_meta_dump_slot';

/*------ add sys fuction gs_undo_translot_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, int4, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4541;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot_dump_slot';

/*------ add sys fuction gs_undo_translot_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4438;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT gs_undo_translot oid)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot_dump_xid';

/*------ add sys fuction gs_undo_dump_record ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_record(bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4539;
CREATE FUNCTION pg_catalog.gs_undo_dump_record(bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text)
RETURNS record LANGUAGE INTERNAL as 'gs_undo_dump_record';

/*------ add sys fuction gs_undo_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4540;
CREATE FUNCTION pg_catalog.gs_undo_dump_xid(xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text)
RETURNS record LANGUAGE INTERNAL as 'gs_undo_dump_xid';

/*------ add sys fuction gs_undo_dump_parsepage_mv ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_parsepage_mv(text, bigint, text, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4542;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_dump_parsepage_mv(relpath text, blkno bigint, reltype text, rmem boolean, OUT output text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_undo_dump_parsepage_mv$function$;
comment on function PG_CATALOG.gs_undo_dump_parsepage_mv(relpath text, blkno bigint, reltype text, rmem boolean) is 'parse uheap data page and undo to output file based on given filepath';
END IF;
END $upgrade$;
