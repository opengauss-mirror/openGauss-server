DROP FUNCTION IF EXISTS pg_catalog.ondemand_recovery_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6991;
CREATE OR REPLACE FUNCTION pg_catalog.ondemand_recovery_status(
    OUT primary_checkpoint_redo_lsn text,
    OUT realtime_build_replayed_lsn text,
    OUT hashmap_used_blocks oid,
    OUT hashmap_total_blocks oid,
    OUT trxn_queue_blocks oid,
    OUT seg_queue_blocks oid,
    OUT in_ondemand_recovery boolean,
    OUT ondemand_recovery_status text,
    OUT realtime_build_status text,
    OUT recovery_pause_status text,
    OUT record_item_num oid,
    OUT record_item_mbytes oid
) RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE COST 10 ROWS 10
AS $function$get_ondemand_recovery_status$function$;

DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4130;
CREATE OR REPLACE FUNCTION pg_catalog.pg_buffercache_pages
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
	OUT pinning_backends integer,
	OUT segfileno integer,
	OUT segblockno oid,
	OUT aio_in_process boolean
)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$pg_buffercache_pages$function$;

