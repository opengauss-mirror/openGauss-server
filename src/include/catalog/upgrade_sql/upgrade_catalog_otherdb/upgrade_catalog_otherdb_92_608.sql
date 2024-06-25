/*------ add sys fuction gs_undo_meta_dump_zone ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_zone(int4, boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4433;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_zone(zone_id int4, read_memory boolean, OUT zone_id oid, OUT persist_type oid, OUT insert text, OUT discard text, OUT forcediscard text, OUT lsn text)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 as 'gs_undo_meta_dump_zone';

/*------ add sys fuction gs_undo_meta_dump_spaces ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_spaces(int4, boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4432;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_spaces(zone_id int4, read_memory boolean, OUT zone_id oid, OUT undorecord_space_tail text, OUT undorecord_space_head text, OUT undorecord_space_lsn text, OUT undoslot_space_tail text, OUT undoslot_space_head text, OUT undoreslot_space_lsn text)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 as 'gs_undo_meta_dump_spaces';

/*------ add sys fuction gs_undo_meta_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_slot(int4, boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4437;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_meta_dump_slot(zone_id int4, read_memory boolean, OUT zone_id oid, OUT allocate text, OUT recycle text, OUT frozen_xid text, OUT global_frozen_xid text, OUT recycle_xid text, OUT global_recycle_xid text)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 as 'gs_undo_meta_dump_slot';

/*------ add sys fuction gs_undo_translot_dump_slot ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4541;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_slot(zone_id int4, read_memory boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT slot_states oid)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 as 'gs_undo_translot_dump_slot';

/*------ add sys fuction gs_undo_translot_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4438;
CREATE OR REPLACE FUNCTION pg_catalog.gs_undo_translot_dump_xid(zone_id xid, read_memory boolean, OUT zone_id oid, OUT slot_xid text, OUT start_undoptr text, OUT end_undoptr text, OUT lsn text, OUT slot_states oid)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 as 'gs_undo_translot_dump_xid';

/*------ add sys fuction gs_undo_dump_record ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_record(bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4539;
CREATE FUNCTION pg_catalog.gs_undo_dump_record(undoptr bigint, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text)
RETURNS record LANGUAGE INTERNAL as 'gs_undo_dump_record';

/*------ add sys fuction gs_undo_dump_xid ------*/
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
OUT reloid text, OUT relfilenode text, OUT utype text, OUT blkprev text, OUT blockno text, OUT uoffset text,
OUT prevurp text, OUT payloadlen text, OUT oldxactid text, OUT partitionoid text, OUT tablespace text, OUT alreadyread_bytes text, OUT prev_undorec_len text, OUT td_id text, OUT reserved text, OUT flag text, OUT flag2 text, OUT t_hoff text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4540;
CREATE FUNCTION pg_catalog.gs_undo_dump_xid(undoptr xid, OUT undoptr oid, OUT xactid oid, OUT cid text,
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

DROP FUNCTION IF EXISTS pg_catalog.gs_catalog_attribute_records(oid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8010;
CREATE OR REPLACE FUNCTION pg_catalog.gs_catalog_attribute_records(IN relid oid, OUT attrelid oid, OUT attname name, OUT atttypid oid, OUT attstattarget integer, OUT attlen smallint, OUT attnum smallint, OUT attndims integer, OUT attcacheoff integer, OUT atttypmod integer, OUT attbyval boolean, OUT attstorage "char", OUT attalign "char", OUT attnotnull boolean, OUT atthasdef boolean, OUT attisdropped boolean, OUT attislocal boolean, OUT attcmprmode tinyint, OUT attinhcount integer, OUT attcollation oid, OUT attacl aclitem[], OUT attoptions text[], OUT attfdwoptions text[], OUT attinitdefval bytea, OUT attkvtype tinyint) RETURNS SETOF RECORD STRICT STABLE ROWS 1000 LANGUAGE INTERNAL AS 'gs_catalog_attribute_records';DROP FUNCTION IF EXISTS pg_catalog.gs_verify_urq(oid, text, int8, text, OUT verify_code text, OUT detail text) CASCADE;
comment on function pg_catalog.gs_catalog_attribute_records(IN relid oid, OUT attrelid oid, OUT attname name, OUT atttypid oid, OUT attstattarget integer, OUT attlen smallint, OUT attnum smallint, OUT attndims integer, OUT attcacheoff integer, OUT atttypmod integer, OUT attbyval boolean, OUT attstorage "char", OUT attalign "char", OUT attnotnull boolean, OUT atthasdef boolean, OUT attisdropped boolean, OUT attislocal boolean, OUT attcmprmode tinyint, OUT attinhcount integer, OUT attcollation oid, OUT attacl aclitem[], OUT attoptions text[], OUT attfdwoptions text[], OUT attinitdefval bytea, OUT attkvtype tinyint) is 'attribute description for catalog relation';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9155;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_urq
(
	reloid oid,
	type text,
	blkno int8,
    vtype text,
	OUT verify_code text,
    OUT detail text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE rows 1 as 'gs_verify_urq';
comment on function pg_catalog.gs_verify_urq() is 'Verify the ubtree recycle queue';
 
DROP FUNCTION IF EXISTS pg_catalog.gs_repair_urq(oid, OUT result text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9156;
CREATE OR REPLACE FUNCTION pg_catalog.gs_repair_urq
(
	reloid oid,
    OUT result text
) RETURNS text LANGUAGE INTERNAL STABLE strict as 'gs_repair_urq';
comment on function pg_catalog.gs_repair_urq() is 'Reinitialize recycle queue of ubtree index ';

DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_record(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4543;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_undo_record(type text, start_idx int8, end_idx int8, location boolean, OUT zone_id oid, OUT detail text)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 strict as 'gs_verify_undo_record';
comment on function pg_catalog.gs_verify_undo_record(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) is 'verify undo record';
 
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_translot(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_translot(text, int8, int8, boolean, OUT zone_id int8, OUT detail text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4544;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_undo_translot(type text, start_idx int8, end_idx int8, location boolean, OUT zone_id int8, OUT detail text)
RETURNS SETOF record LANGUAGE INTERNAL rows 1 strict as 'gs_verify_undo_translot';
comment on function pg_catalog.gs_verify_undo_translot(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) is 'verify undo transaction slot';
 
DROP FUNCTION IF EXISTS pg_catalog.gs_verify_undo_meta(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4545;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_undo_meta(type text, start_idx int8, end_idx int8, location boolean, OUT zone_id oid, OUT detail text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 strict as 'gs_verify_undo_meta';
comment on function  pg_catalog.gs_verify_undo_meta(text, int8, int8, boolean, OUT zone_id oid, OUT detail text) is 'verify undo meta';
