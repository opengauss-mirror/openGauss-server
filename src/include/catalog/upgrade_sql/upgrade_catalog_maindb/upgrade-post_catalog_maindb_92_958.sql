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

-- create query_node_reform_info_from_dms
DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info_from_dms() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2869;
CREATE FUNCTION pg_catalog.query_node_reform_info_from_dms
(
    int4,
    out name text,
    out description text
)
RETURNS SETOF record
LANGUAGE INTERNAL
STRICT NOT FENCED NOT SHIPPABLE ROWS 1000
AS $function$query_node_reform_info_from_dms$function$;

-- create query_all_drc_info
DROP FUNCTION IF EXISTS pg_catalog.query_all_drc_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2870;
CREATE FUNCTION pg_catalog.query_all_drc_info
(
    int4,
    out RESOURCE_ID text,
    out MASTER_ID int4,
    out COPY_INSTS int8,
    out CLAIMED_OWNER int4,
    out LOCK_MODE int4,
    out LAST_EDP int4,
    out TYPE int4,
    out IN_RECOVERY char,
    out COPY_PROMOTE int4,
    out PART_ID int4,
    out EDP_MAP int8,
    out LSN int8,
    out LEN int4,
    out RECOVERY_SKIP int4,
    out RECYCLING char,
    out CONVERTING_INST_ID int4,
    out CONVERTING_CURR_MODE int4,
    out CONVERTING_REQ_MODE int4
)
RETURNS SETOF record
LANGUAGE INTERNAL
STRICT NOT FENCED NOT SHIPPABLE ROWS 1000
AS $function$query_all_drc_info$function$;

comment on function pg_catalog.query_node_reform_info_from_dms(int4) is 'query node reform info from dms';
comment on function pg_catalog.query_all_drc_info(int4) is 'query all drc info';
