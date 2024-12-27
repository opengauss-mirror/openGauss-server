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

-- drop reform info functions
DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info_from_dms() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2869;
CREATE FUNCTION pg_catalog.query_node_reform_info_from_dms
(
    int4,
    out name text,
    out description text
)
RETURNS SETOF record LANGUAGE INTERNAL as 'query_node_reform_info_from_dms';

-- drop drc info functions
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
RETURNS SETOF record LANGUAGE INTERNAL as 'query_all_drc_info';
