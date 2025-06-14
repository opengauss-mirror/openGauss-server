SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 3240, 3241, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_sql_limit
(
   limit_id int8 NOCOMPRESS,
   limit_name name NOCOMPRESS,
   is_valid bool NOCOMPRESS,
   work_node int1 NOCOMPRESS,
   max_concurrency int8 NOCOMPRESS,
   start_time timestamptz NOCOMPRESS,
   end_time timestamptz NOCOMPRESS,
   limit_type text NOCOMPRESS,
   databases name[] NOCOMPRESS,
   users name[] NOCOMPRESS,
   limit_opt text[] NOCOMPRESS
) WITHOUT OIDS TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 3242;

CREATE INDEX IF NOT EXISTS gs_sql_limit_id_index ON pg_catalog.gs_sql_limit USING btree (limit_id int8_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8231;

CREATE OR REPLACE FUNCTION pg_catalog.gs_create_sql_limit
(
    limit_name name,
    limit_type text,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
)
RETURNS int8 NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_create_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8232;

CREATE OR REPLACE FUNCTION pg_catalog.gs_update_sql_limit
(
    limit_id int8,
    limit_name name,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
)
RETURNS boolean NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_update_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8233;

CREATE OR REPLACE FUNCTION pg_catalog.gs_select_sql_limit
(
    IN limit_id int8,
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$gs_select_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8234;

CREATE OR REPLACE FUNCTION pg_catalog.gs_select_sql_limit
(
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$gs_select_sql_limit_all$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8235;

CREATE OR REPLACE FUNCTION pg_catalog.gs_delete_sql_limit
(
    limit_id int8
)
RETURNS boolean NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_delete_sql_limit$function$;

DROP FUNCTION IF EXISTS pg_catalog.realtime_build_queue_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6993;
CREATE OR REPLACE FUNCTION pg_catalog.realtime_build_queue_status(
    OUT batch_num oid,
    OUT readline_queue_size oid,
    OUT trxn_manager_queue_size oid,
    OUT trxn_worker_queue_size oid,
    OUT segworker_queue_size oid,
    OUT batchredo_queue_size1 oid,
    OUT batchredo_queue_size2 oid,
    OUT batchredo_queue_size3 oid,
    OUT batchredo_queue_size4 oid,
    OUT redomanager_queue_size1 oid,
    OUT redomanager_queue_size2 oid,
    OUT redomanager_queue_size3 oid,
    OUT redomanager_queue_size4 oid,
    OUT hashmap_manager_queue_size1 oid,
    OUT hashmap_manager_queue_size2 oid,
    OUT hashmap_manager_queue_size3 oid,
    OUT hashmap_manager_queue_size4 oid
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$get_realtime_build_queue_status$function$;;

DROP FUNCTION IF EXISTS dbe_perf.get_statement_history(IN in_time timestamp with time zone) CASCADE;
CREATE OR REPLACE FUNCTION dbe_perf.get_statement_history(IN in_time timestamp with time zone)
RETURNS TABLE (
    db_name name,
    schema_name name,
    origin_node integer,
    user_name name,
    application_name text,
    client_addr text,
    client_port integer,
    unique_query_id bigint,
    debug_query_id bigint,
    query text,
    start_time timestamp with time zone,
    finish_time timestamp with time zone,
    slow_sql_threshold bigint,
    transaction_id bigint,
    thread_id bigint,
    session_id bigint,
    n_soft_parse bigint,
    n_hard_parse bigint,
    query_plan text,
    n_returned_rows bigint,
    n_tuples_fetched bigint,
    n_tuples_returned bigint,
    n_tuples_inserted bigint,
    n_tuples_updated bigint,
    n_tuples_deleted bigint,
    n_blocks_fetched bigint,
    n_blocks_hit bigint,
    db_time bigint,
    cpu_time bigint,
    execution_time bigint,
    parse_time bigint,
    plan_time bigint,
    rewrite_time bigint,
    pl_execution_time bigint,
    pl_compilation_time bigint,
    data_io_time bigint,
    net_send_info text,
    net_recv_info text,
    net_stream_send_info text,
    net_stream_recv_info text,
    lock_count bigint,
    lock_time bigint,
    lock_wait_count bigint,
    lock_wait_time bigint,
    lock_max_count bigint,
    lwlock_count bigint,
    lwlock_wait_count bigint,
    lwlock_time bigint,
    lwlock_wait_time bigint,
    details bytea,
    is_slow_sql boolean,
    trace_id text,
    advise text,
    net_send_time bigint,
    srt1_q bigint,
    srt2_simple_query bigint,
    srt3_analyze_rewrite bigint,
    srt4_plan_query bigint,
    srt5_light_query bigint,
    srt6_p bigint,
    srt7_b bigint,
    srt8_e bigint,
    srt9_d bigint,
    srt10_s bigint,
    srt11_c bigint,
    srt12_u bigint,
    srt13_before_query bigint,
    srt14_after_query bigint,
    rtt_unknown bigint,
    parent_query_id bigint,
    net_trans_time bigint
) AS $$
DECLARE
    node_role text;
BEGIN
    SELECT local_role INTO node_role FROM pg_stat_get_stream_replications() LIMIT 1;
    
    IF node_role = 'Primary' OR node_role = 'Normal' THEN
        RETURN QUERY SELECT * FROM dbe_perf.statement_history where finish_time >= in_time and is_slow_sql = 't';
    ELSIF node_role = 'Standby' OR node_role = 'Cascade Standby' THEN
        RETURN QUERY SELECT * FROM dbe_perf.standby_statement_history(true, in_time, now());
    ELSE
        RAISE EXCEPTION 'unknown node role: %', node_role;
    END IF;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;
