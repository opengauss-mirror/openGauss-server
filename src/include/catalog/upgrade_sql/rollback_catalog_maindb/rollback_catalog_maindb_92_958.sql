DROP FUNCTION IF EXISTS pg_catalog.ondemand_recovery_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6991;
CREATE FUNCTION pg_catalog.ondemand_recovery_status(
    out primary_checkpoint_redo_lsn text,
    out realtime_build_replayed_lsn text,
    out hashmap_used_blocks         oid,
    out hashmap_total_blocks        oid,
    out trxn_queue_blocks           oid,
    out seg_queue_blocks            oid,
    out in_ondemand_recovery        boolean,
    out ondemand_recovery_status    text,
    out realtime_build_status       text,
    out recovery_pause_status       text,
    out record_item_num             oid,
    out record_item_mbytes          oid
)
RETURNS SETOF record LANGUAGE INTERNAL as 'ondemand_recovery_status' stable;

DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4130;
CREATE FUNCTION pg_catalog.pg_buffercache_pages
(
    OUT bufferid integer,
	OUT relfilenode oid,
	OUT bucketid integer,
	OUT storage_type bigint,
	OUT reltablespace oid,
	OUT reldatabase oid,
	OUT relforknumber integer,
	OUT relblocknumber oid,
	OUT isdirty boolean,
	OUT isvalid boolean,
	OUT usage_count smallint,
	OUT pinning_backends integer
)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$pg_buffercache_pages$function$;

