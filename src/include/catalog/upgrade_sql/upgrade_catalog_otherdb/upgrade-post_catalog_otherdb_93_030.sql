DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2867;
CREATE OR REPLACE FUNCTION pg_catalog.query_node_reform_info
(
    OUT reform_node_id          integer,
    OUT reform_type             text,
    OUT reform_start_time       timestamp with time zone,
    OUT reform_end_time         timestamp with time zone,
    OUT is_reform_success       boolean,
    OUT redo_start_time         timestamp with time zone,
    OUT redo_end_time           timestamp with time zone,
    OUT xlog_total_bytes        int8,
    OUT hashmap_construct_time  timestamp with time zone,
    OUT action                  text
)
 RETURNS SETOF record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE ROWS 64
AS $function$query_node_reform_info$function$;
comment on function pg_catalog.query_node_reform_info
(
    OUT reform_node_id          integer,
    OUT reform_type             text,
    OUT reform_start_time       timestamp with time zone,
    OUT reform_end_time         timestamp with time zone,
    OUT is_reform_success       boolean,
    OUT redo_start_time         timestamp with time zone,
    OUT redo_end_time           timestamp with time zone,
    OUT xlog_total_bytes        int8,
    OUT hashmap_construct_time  timestamp with time zone,
    OUT action                  text
) is 'query node reform information';

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
