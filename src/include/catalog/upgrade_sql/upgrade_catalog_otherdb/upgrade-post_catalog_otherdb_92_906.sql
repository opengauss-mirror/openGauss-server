do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select extname from pg_extension where extname='dolphin' and extversion = '2.0')
    LOOP
        if ans = true then
            ALTER EXTENSION dolphin UPDATE TO '2.0.1';
        end if;
    exit;
    END LOOP;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.ss_txnstatus_cache_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8889;
CREATE FUNCTION pg_catalog.ss_txnstatus_cache_stat(
    OUT vcache_gets bigint,
    OUT hcache_gets bigint,
    OUT nio_gets bigint,
    OUT avg_hcache_gettime_us float8,
    OUT avg_nio_gettime_us float8,
    OUT cache_hit_rate float8,
    OUT hcache_eviction bigint,
    OUT avg_eviction_refcnt float8
) 
RETURNS SETOF record 
LANGUAGE internal STABLE NOT FENCED NOT SHIPPABLE ROWS 100 
AS 'ss_txnstatus_cache_stat';

-- those type of view will recreate.
-- openGauss=# select table_schema, table_name,column_name from information_schema.columns where column_name = 'db_time';
--  table_schema |          table_name           | column_name
-- --------------+-------------------------------+-------------
--  pg_catalog   | gs_wlm_session_query_info_all | db_time  -- table
--  pg_catalog   | gs_wlm_session_info_all       | db_time  -- view
--  dbe_perf     | statement                     | db_time  -- view
--  pg_catalog   | statement_history             | db_time  -- unlogged table
--  dbe_perf     | statement_history             | db_time  -- view
--  dbe_perf     | summary_statement             | db_time  -- view
--  dbe_perf     | gs_slow_query_info            | db_time  -- view
--  dbe_perf     | gs_slow_query_history         | db_time  -- view
--  dbe_perf     | global_slow_query_history     | db_time  -- view
--  dbe_perf     | global_slow_query_info        | db_time  -- view
-- those proc will recreated.
-- openGauss=# select proname, pronamespace  from pg_proc where proargnames @> array['db_time'];
--              proname              | pronamespace
-- ----------------------------------+--------------
--  get_instr_unique_sql             |           11
--  pg_stat_get_wlm_session_info     |           11
--  standby_statement_history        |         4988
--  standby_statement_history        |         4988
--  get_global_full_sql_by_timestamp |         4988
--  get_global_slow_sql_by_timestamp |         4988

/* We process history related tables、views and function now. */
DROP VIEW  IF EXISTS DBE_PERF.statement CASCADE;
DROP VIEW IF EXISTS DBE_PERF.summary_statement CASCADE;
DROP VIEW IF EXISTS DBE_PERF.statement_history CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.get_instr_unique_sql() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5702;
CREATE FUNCTION pg_catalog.get_instr_unique_sql
(
    OUT node_name name,
    OUT node_id integer,
    OUT user_name name,
    OUT user_id oid,
    OUT unique_sql_id bigint,
    OUT query text,
    OUT n_calls bigint,
    OUT min_elapse_time bigint,
    OUT max_elapse_time bigint,
    OUT total_elapse_time bigint,
    OUT n_returned_rows bigint,
    OUT n_tuples_fetched bigint,
    OUT n_tuples_returned bigint,
    OUT n_tuples_inserted bigint,
    OUT n_tuples_updated bigint,
    OUT n_tuples_deleted bigint,
    OUT n_blocks_fetched bigint,
    OUT n_blocks_hit bigint,
    OUT n_soft_parse bigint,
    OUT n_hard_parse bigint,
    OUT db_time bigint,
    OUT cpu_time bigint,
    OUT execution_time bigint,
    OUT parse_time bigint,
    OUT plan_time bigint,
    OUT rewrite_time bigint,
    OUT pl_execution_time bigint,
    OUT pl_compilation_time bigint,
    OUT data_io_time bigint,
    OUT net_send_info text,
    Out net_recv_info text,
    OUT net_stream_send_info text,
    OUT net_stream_recv_info text,
    OUT last_updated timestamp with time zone,
    OUT sort_count bigint,
    OUT sort_time bigint,
    OUT sort_mem_used bigint,
    OUT sort_spill_count bigint,
    OUT sort_spill_size bigint,
    OUT hash_count bigint,
    OUT hash_time bigint,
    OUT hash_mem_used bigint,
    OUT hash_spill_count bigint,
    OUT hash_spill_size bigint,
    OUT net_send_time bigint,
    OUT srt1_q bigint,
    OUT srt2_simple_query bigint,
    OUT srt3_analyze_rewrite bigint,
    OUT srt4_plan_query bigint,
    OUT srt5_light_query bigint,
    OUT srt6_p bigint,
    OUT srt7_b bigint,
    OUT srt8_e bigint,
    OUT srt9_d bigint,
    OUT srt10_s bigint,
    OUT srt11_c bigint,
    OUT srt12_u bigint,
    OUT srt13_before_query bigint,
    OUT srt14_after_query bigint,
    OUT rtt_unknown bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_unique_sql';

DROP INDEX IF EXISTS pg_catalog.statement_history_time_idx;
DROP TABLE IF EXISTS pg_catalog.statement_history cascade;

CREATE unlogged table  IF NOT EXISTS pg_catalog.statement_history(
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
    rtt_unknown bigint
);
REVOKE ALL on table pg_catalog.statement_history FROM public;
create index pg_catalog.statement_history_time_idx on pg_catalog.statement_history USING btree (start_time, is_slow_sql);

DROP FUNCTION IF EXISTS dbe_perf.standby_statement_history(boolean);
DROP FUNCTION IF EXISTS dbe_perf.standby_statement_history(boolean, timestamp with time zone[]);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3118;
CREATE FUNCTION dbe_perf.standby_statement_history(
    IN  only_slow boolean,
    OUT db_name name,
    OUT schema_name name,
    OUT origin_node integer,
    OUT user_name name,
    OUT application_name text,
    OUT client_addr text,
    OUT client_port integer,
    OUT unique_query_id bigint,
    OUT debug_query_id bigint,
    OUT query text,
    OUT start_time timestamp with time zone,
    OUT finish_time timestamp with time zone,
    OUT slow_sql_threshold bigint,
    OUT transaction_id bigint,
    OUT thread_id bigint,
    OUT session_id bigint,
    OUT n_soft_parse bigint,
    OUT n_hard_parse bigint,
    OUT query_plan text,
    OUT n_returned_rows bigint,
    OUT n_tuples_fetched bigint,
    OUT n_tuples_returned bigint,
    OUT n_tuples_inserted bigint,
    OUT n_tuples_updated bigint,
    OUT n_tuples_deleted bigint,
    OUT n_blocks_fetched bigint,
    OUT n_blocks_hit bigint,
    OUT db_time bigint,
    OUT cpu_time bigint,
    OUT execution_time bigint,
    OUT parse_time bigint,
    OUT plan_time bigint,
    OUT rewrite_time bigint,
    OUT pl_execution_time bigint,
    OUT pl_compilation_time bigint,
    OUT data_io_time bigint,
    OUT net_send_info text,
    OUT net_recv_info text,
    OUT net_stream_send_info text,
    OUT net_stream_recv_info text,
    OUT lock_count bigint,
    OUT lock_time bigint,
    OUT lock_wait_count bigint,
    OUT lock_wait_time bigint,
    OUT lock_max_count bigint,
    OUT lwlock_count bigint,
    OUT lwlock_wait_count bigint,
    OUT lwlock_time bigint,
    OUT lwlock_wait_time bigint,
    OUT details bytea,
    OUT is_slow_sql boolean,
    OUT trace_id text,
    OUT advise text,
    OUT net_send_time bigint,
    OUT srt1_q bigint,
    OUT srt2_simple_query bigint,
    OUT srt3_analyze_rewrite bigint,
    OUT srt4_plan_query bigint,
    OUT srt5_light_query bigint,
    OUT srt6_p bigint,
    OUT srt7_b bigint,
    OUT srt8_e bigint,
    OUT srt9_d bigint,
    OUT srt10_s bigint,
    OUT srt11_c bigint,
    OUT srt12_u bigint,
    OUT srt13_before_query bigint,
    OUT srt14_after_query bigint,
    OUT rtt_unknown bigint)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
LANGUAGE internal AS $function$standby_statement_history_1v$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3119;
CREATE FUNCTION dbe_perf.standby_statement_history(
    IN  only_slow boolean,
    VARIADIC finish_time timestamp with time zone[],
    OUT db_name name,
    OUT schema_name name,
    OUT origin_node integer,
    OUT user_name name,
    OUT application_name text,
    OUT client_addr text,
    OUT client_port integer,
    OUT unique_query_id bigint,
    OUT debug_query_id bigint,
    OUT query text,
    OUT start_time timestamp with time zone,
    OUT finish_time timestamp with time zone,
    OUT slow_sql_threshold bigint,
    OUT transaction_id bigint,
    OUT thread_id bigint,
    OUT session_id bigint,
    OUT n_soft_parse bigint,
    OUT n_hard_parse bigint,
    OUT query_plan text,
    OUT n_returned_rows bigint,
    OUT n_tuples_fetched bigint,
    OUT n_tuples_returned bigint,
    OUT n_tuples_inserted bigint,
    OUT n_tuples_updated bigint,
    OUT n_tuples_deleted bigint,
    OUT n_blocks_fetched bigint,
    OUT n_blocks_hit bigint,
    OUT db_time bigint,
    OUT cpu_time bigint,
    OUT execution_time bigint,
    OUT parse_time bigint,
    OUT plan_time bigint,
    OUT rewrite_time bigint,
    OUT pl_execution_time bigint,
    OUT pl_compilation_time bigint,
    OUT data_io_time bigint,
    OUT net_send_info text,
    OUT net_recv_info text,
    OUT net_stream_send_info text,
    OUT net_stream_recv_info text,
    OUT lock_count bigint,
    OUT lock_time bigint,
    OUT lock_wait_count bigint,
    OUT lock_wait_time bigint,
    OUT lock_max_count bigint,
    OUT lwlock_count bigint,
    OUT lwlock_wait_count bigint,
    OUT lwlock_time bigint,
    OUT lwlock_wait_time bigint,
    OUT details bytea,
    OUT is_slow_sql boolean,
    OUT net_send_time bigint,
    OUT srt1_q bigint,
    OUT srt2_simple_query bigint,
    OUT srt3_analyze_rewrite bigint,
    OUT srt4_plan_query bigint,
    OUT srt5_light_query bigint,
    OUT srt6_p bigint,
    OUT srt7_b bigint,
    OUT srt8_e bigint,
    OUT srt9_d bigint,
    OUT srt10_s bigint,
    OUT srt11_c bigint,
    OUT srt12_u bigint,
    OUT srt13_before_query bigint,
    OUT srt14_after_query bigint,
    OUT rtt_unknown bigint)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
LANGUAGE internal AS $function$standby_statement_history$function$;

CREATE VIEW DBE_PERF.statement AS
  SELECT * FROM get_instr_unique_sql();

DROP FUNCTION IF EXISTS dbe_perf.get_summary_statement() cascade;
CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statement()
RETURNS setof dbe_perf.statement
AS $$
DECLARE
  row_data dbe_perf.statement%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statement';
        FOR row_data IN EXECUTE(query_str) LOOP
          return next row_data;
       END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.summary_statement AS
  SELECT * FROM DBE_PERF.get_summary_statement();

CREATE VIEW DBE_PERF.statement_history AS
  select * from pg_catalog.statement_history;

DROP FUNCTION IF EXISTS dbe_perf.get_global_full_sql_by_timestamp(timestamp with time zone, timestamp with time zone);
CREATE OR REPLACE FUNCTION dbe_perf.get_global_full_sql_by_timestamp
  (in start_timestamp timestamp with time zone,
   in end_timestamp timestamp with time zone,
   OUT node_name name,
   OUT db_name name,
   OUT schema_name name,
   OUT origin_node integer,
   OUT user_name name,
   OUT application_name text,
   OUT client_addr text,
   OUT client_port integer,
   OUT unique_query_id bigint,
   OUT debug_query_id bigint,
   OUT query text,
   OUT start_time timestamp with time zone,
   OUT finish_time timestamp with time zone,
   OUT slow_sql_threshold bigint,
   OUT transaction_id bigint,
   OUT thread_id bigint,
   OUT session_id bigint,
   OUT n_soft_parse bigint,
   OUT n_hard_parse bigint,
   OUT query_plan text,
   OUT n_returned_rows bigint,
   OUT n_tuples_fetched bigint,
   OUT n_tuples_returned bigint,
   OUT n_tuples_inserted bigint,
   OUT n_tuples_updated bigint,
   OUT n_tuples_deleted bigint,
   OUT n_blocks_fetched bigint,
   OUT n_blocks_hit bigint,
   OUT db_time bigint,
   OUT cpu_time bigint,
   OUT execution_time bigint,
   OUT parse_time bigint,
   OUT plan_time bigint,
   OUT rewrite_time bigint,
   OUT pl_execution_time bigint,
   OUT pl_compilation_time bigint,
   OUT data_io_time bigint,
   OUT net_send_info text,
   OUT net_recv_info text,
   OUT net_stream_send_info text,
   OUT net_stream_recv_info text,
   OUT lock_count bigint,
   OUT lock_time bigint,
   OUT lock_wait_count bigint,
   OUT lock_wait_time bigint,
   OUT lock_max_count bigint,
   OUT lwlock_count bigint,
   OUT lwlock_wait_count bigint,
   OUT lwlock_time bigint,
   OUT lwlock_wait_time bigint,
   OUT details bytea,
   OUT is_slow_sql bool,
   OUT trace_id text,
   OUT advise text,
   OUT net_send_time bigint,
   OUT srt1_q bigint,
   OUT srt2_simple_query bigint,
   OUT srt3_analyze_rewrite bigint,
   OUT srt4_plan_query bigint,
   OUT srt5_light_query bigint,
   OUT srt6_p bigint,
   OUT srt7_b bigint,
   OUT srt8_e bigint,
   OUT srt9_d bigint,
   OUT srt10_s bigint,
   OUT srt11_c bigint,
   OUT srt12_u bigint,
   OUT srt13_before_query bigint,
   OUT srt14_after_query bigint,
   OUT rtt_unknown bigint
   )
 RETURNS setof record
 AS $$
 DECLARE
  row_data pg_catalog.statement_history%rowtype;
  row_name record;
  query_str text;
  -- node name
  query_str_nodes text;
  BEGIN
    -- Get all node names(CN + master DN)
   query_str_nodes := 'select * from dbe_perf.node_name';
   FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statement_history where start_time >= ''' ||$1|| ''' and start_time <= ''' || $2 || '''';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          db_name := row_data.db_name;
          schema_name := row_data.schema_name;
          origin_node := row_data.origin_node;
          user_name := row_data.user_name;
          application_name := row_data.application_name;
          client_addr := row_data.client_addr;
          client_port := row_data.client_port;
          unique_query_id := row_data.unique_query_id;
          debug_query_id := row_data.debug_query_id;
          query := row_data.query;
          start_time := row_data.start_time;
          finish_time := row_data.finish_time;
          slow_sql_threshold := row_data.slow_sql_threshold;
          transaction_id := row_data.transaction_id;
          thread_id := row_data.thread_id;
          session_id := row_data.session_id;
          n_soft_parse := row_data.n_soft_parse;
          n_hard_parse := row_data.n_hard_parse;
          query_plan := row_data.query_plan;
          n_returned_rows := row_data.n_returned_rows;
          n_tuples_fetched := row_data.n_tuples_fetched;
          n_tuples_returned := row_data.n_tuples_returned;
          n_tuples_inserted := row_data.n_tuples_inserted;
          n_tuples_updated := row_data.n_tuples_updated;
          n_tuples_deleted := row_data.n_tuples_deleted;
          n_blocks_fetched := row_data.n_blocks_fetched;
          n_blocks_hit := row_data.n_blocks_hit;
          db_time := row_data.db_time;
          cpu_time := row_data.cpu_time;
          execution_time := row_data.execution_time;
          parse_time := row_data.parse_time;
          plan_time := row_data.plan_time;
          rewrite_time := row_data.rewrite_time;
          pl_execution_time := row_data.pl_execution_time;
          pl_compilation_time := row_data.pl_compilation_time;
          data_io_time := row_data.data_io_time;
          net_send_info := row_data.net_send_info;
          net_recv_info := row_data.net_recv_info;
          net_stream_send_info := row_data.net_stream_send_info;
          net_stream_recv_info := row_data.net_stream_recv_info;
          lock_count := row_data.lock_count;
          lock_time := row_data.lock_time;
          lock_wait_count := row_data.lock_wait_count;
          lock_wait_time := row_data.lock_wait_time;
          lock_max_count := row_data.lock_max_count;
          lwlock_count := row_data.lwlock_count;
          lwlock_wait_count := row_data.lwlock_wait_count;
          lwlock_time := row_data.lwlock_time;
          lwlock_wait_time := row_data.lwlock_wait_time;
          details := row_data.details;
          is_slow_sql := row_data.is_slow_sql;
          trace_id := row_data.trace_id;
          advise := row_data.advise;
          net_send_time =row_data.net_send_time;
          srt1_q := row_data.srt1_q;
          srt2_simple_query := row_data.srt2_simple_query;
          srt3_analyze_rewrite := row_data.srt3_analyze_rewrite;
          srt4_plan_query := row_data.srt4_plan_query;
          srt5_light_query := row_data.srt5_light_query;
          srt6_p := row_data.srt6_p;
          srt7_b := row_data.srt7_b;
          srt8_e := row_data.srt8_e;
          srt9_d := row_data.srt9_d;
          srt10_s := row_data.srt10_s;
          srt11_c := row_data.srt11_c;
          srt12_u := row_data.srt12_u;
          srt13_before_query := row_data.srt13_before_query;
          srt14_after_query := row_data.srt14_after_query;
          rtt_unknown := row_data.rtt_unknown;
          return next;
       END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS dbe_perf.get_global_slow_sql_by_timestamp(timestamp with time zone, timestamp with time zone);
CREATE OR REPLACE FUNCTION DBE_PERF.get_global_slow_sql_by_timestamp
  (in start_timestamp timestamp with time zone,
   in end_timestamp timestamp with time zone,
   OUT node_name name,
   OUT db_name name,
   OUT schema_name name,
   OUT origin_node integer,
   OUT user_name name,
   OUT application_name text,
   OUT client_addr text,
   OUT client_port integer,
   OUT unique_query_id bigint,
   OUT debug_query_id bigint,
   OUT query text,
   OUT start_time timestamp with time zone,
   OUT finish_time timestamp with time zone,
   OUT slow_sql_threshold bigint,
   OUT transaction_id bigint,
   OUT thread_id bigint,
   OUT session_id bigint,
   OUT n_soft_parse bigint,
   OUT n_hard_parse bigint,
   OUT query_plan text,
   OUT n_returned_rows bigint,
   OUT n_tuples_fetched bigint,
   OUT n_tuples_returned bigint,
   OUT n_tuples_inserted bigint,
   OUT n_tuples_updated bigint,
   OUT n_tuples_deleted bigint,
   OUT n_blocks_fetched bigint,
   OUT n_blocks_hit bigint,
   OUT db_time bigint,
   OUT cpu_time bigint,
   OUT execution_time bigint,
   OUT parse_time bigint,
   OUT plan_time bigint,
   OUT rewrite_time bigint,
   OUT pl_execution_time bigint,
   OUT pl_compilation_time bigint,
   OUT data_io_time bigint,
   OUT net_send_info text,
   OUT net_recv_info text,
   OUT net_stream_send_info text,
   OUT net_stream_recv_info text,
   OUT lock_count bigint,
   OUT lock_time bigint,
   OUT lock_wait_count bigint,
   OUT lock_wait_time bigint,
   OUT lock_max_count bigint,
   OUT lwlock_count bigint,
   OUT lwlock_wait_count bigint,
   OUT lwlock_time bigint,
   OUT lwlock_wait_time bigint,
   OUT details bytea,
   OUT is_slow_sql bool,
   OUT trace_id text,
   OUT advise text,
   OUT net_send_time bigint,
   OUT srt1_q bigint,
   OUT srt2_simple_query bigint,
   OUT srt3_analyze_rewrite bigint,
   OUT srt4_plan_query bigint,
   OUT srt5_light_query bigint,
   OUT srt6_p bigint,
   OUT srt7_b bigint,
   OUT srt8_e bigint,
   OUT srt9_d bigint,
   OUT srt10_s bigint,
   OUT srt11_c bigint,
   OUT srt12_u bigint,
   OUT srt13_before_query bigint,
   OUT srt14_after_query bigint,
   OUT rtt_unknown bigint)
 RETURNS setof record
 AS $$
 DECLARE
  row_data pg_catalog.statement_history%rowtype;
  row_name record;
  query_str text;
  -- node name
  query_str_nodes text;
  BEGIN
    -- Get all node names(CN + master DN)
   query_str_nodes := 'select * from dbe_perf.node_name';
   FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.statement_history where start_time >= ''' ||$1|| ''' and start_time <= ''' || $2 || ''' and is_slow_sql = true ';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          db_name := row_data.db_name;
          schema_name := row_data.schema_name;
          origin_node := row_data.origin_node;
          user_name := row_data.user_name;
          application_name := row_data.application_name;
          client_addr := row_data.client_addr;
          client_port := row_data.client_port;
          unique_query_id := row_data.unique_query_id;
          debug_query_id := row_data.debug_query_id;
          query := row_data.query;
          start_time := row_data.start_time;
          finish_time := row_data.finish_time;
          slow_sql_threshold := row_data.slow_sql_threshold;
          transaction_id := row_data.transaction_id;
          thread_id := row_data.thread_id;
          session_id := row_data.session_id;
          n_soft_parse := row_data.n_soft_parse;
          n_hard_parse := row_data.n_hard_parse;
          query_plan := row_data.query_plan;
          n_returned_rows := row_data.n_returned_rows;
          n_tuples_fetched := row_data.n_tuples_fetched;
          n_tuples_returned := row_data.n_tuples_returned;
          n_tuples_inserted := row_data.n_tuples_inserted;
          n_tuples_updated := row_data.n_tuples_updated;
          n_tuples_deleted := row_data.n_tuples_deleted;
          n_blocks_fetched := row_data.n_blocks_fetched;
          n_blocks_hit := row_data.n_blocks_hit;
          db_time := row_data.db_time;
          cpu_time := row_data.cpu_time;
          execution_time := row_data.execution_time;
          parse_time := row_data.parse_time;
          plan_time := row_data.plan_time;
          rewrite_time := row_data.rewrite_time;
          pl_execution_time := row_data.pl_execution_time;
          pl_compilation_time := row_data.pl_compilation_time;
          data_io_time := row_data.data_io_time;
          net_send_info := row_data.net_send_info;
          net_recv_info := row_data.net_recv_info;
          net_stream_send_info := row_data.net_stream_send_info;
          net_stream_recv_info := row_data.net_stream_recv_info;
          lock_count := row_data.lock_count;
          lock_time := row_data.lock_time;
          lock_wait_count := row_data.lock_wait_count;
          lock_wait_time := row_data.lock_wait_time;
          lock_max_count := row_data.lock_max_count;
          lwlock_count := row_data.lwlock_count;
          lwlock_wait_count := row_data.lwlock_wait_count;
          lwlock_time := row_data.lwlock_time;
          lwlock_wait_time := row_data.lwlock_wait_time;
          details := row_data.details;
          is_slow_sql := row_data.is_slow_sql;
          trace_id := row_data.trace_id;
          advise := row_data.advise;
          net_send_time =row_data.net_send_time;
          srt1_q := row_data.srt1_q;
          srt2_simple_query := row_data.srt2_simple_query;
          srt3_analyze_rewrite := row_data.srt3_analyze_rewrite;
          srt4_plan_query := row_data.srt4_plan_query;
          srt5_light_query := row_data.srt5_light_query;
          srt6_p := row_data.srt6_p;
          srt7_b := row_data.srt7_b;
          srt8_e := row_data.srt8_e;
          srt9_d := row_data.srt9_d;
          srt10_s := row_data.srt10_s;
          srt11_c := row_data.srt11_c;
          srt12_u := row_data.srt12_u;
          srt13_before_query := row_data.srt13_before_query;
          srt14_after_query := row_data.srt14_after_query;
          rtt_unknown := row_data.rtt_unknown;
          return next;
       END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

/* we process wlm releates.*/
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info_all CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.create_wlm_session_info(IN flag int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wlm_session_info(OID) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5002;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_wlm_session_info
(OID,
 OUT datid oid,
 OUT dbname text,
 OUT schemaname text,
 OUT nodename text,
 OUT username text,
 OUT application_name text,
 OUT client_addr inet,
 OUT client_hostname text,
 OUT client_port integer,
 OUT query_band text,
 OUT block_time bigint,
 OUT start_time timestamp with time zone,
 OUT finish_time timestamp with time zone,
 OUT duration bigint,
 OUT estimate_total_time bigint,
 OUT status text,
 OUT abort_info text,
 OUT resource_pool text,
 OUT control_group text,
 OUT estimate_memory integer,
 OUT min_peak_memory integer,
 OUT max_peak_memory integer,
 OUT average_peak_memory integer,
 OUT memory_skew_percent integer,
 OUT spill_info text,
 OUT min_spill_size integer,
 OUT max_spill_size integer,
 OUT average_spill_size integer,
 OUT spill_skew_percent integer,
 OUT min_dn_time bigint,
 OUT max_dn_time bigint,
 OUT average_dn_time bigint,
 OUT dntime_skew_percent integer,
 OUT min_cpu_time bigint,
 OUT max_cpu_time bigint,
 OUT total_cpu_time bigint,
 OUT cpu_skew_percent integer,
 OUT min_peak_iops integer,
 OUT max_peak_iops integer,
 OUT average_peak_iops integer,
 OUT iops_skew_percent integer,
 OUT warning text,
 OUT queryid bigint,
 OUT query text,
 OUT query_plan text,
 OUT node_group text,
 OUT cpu_top1_node_name text,
 OUT cpu_top2_node_name text,
 OUT cpu_top3_node_name text,
 OUT cpu_top4_node_name text,
 OUT cpu_top5_node_name text,
 OUT mem_top1_node_name text,
 OUT mem_top2_node_name text,
 OUT mem_top3_node_name text,
 OUT mem_top4_node_name text,
 OUT mem_top5_node_name text,
 OUT cpu_top1_value bigint,
 OUT cpu_top2_value bigint,
 OUT cpu_top3_value bigint,
 OUT cpu_top4_value bigint,
 OUT cpu_top5_value bigint,
 OUT mem_top1_value bigint,
 OUT mem_top2_value bigint,
 OUT mem_top3_value bigint,
 OUT mem_top4_value bigint,
 OUT mem_top5_value bigint,
 OUT top_mem_dn text,
 OUT top_cpu_dn text,
 OUT n_returned_rows bigint,
 OUT n_tuples_fetched bigint,
 OUT n_tuples_returned bigint,
 OUT n_tuples_inserted bigint,
 OUT n_tuples_updated bigint,
 OUT n_tuples_deleted bigint,
 OUT n_blocks_fetched bigint,
 OUT n_blocks_hit bigint,
 OUT db_time bigint,
 OUT cpu_time bigint,
 OUT execution_time bigint,
 OUT parse_time bigint,
 OUT plan_time bigint,
 OUT rewrite_time bigint,
 OUT pl_execution_time bigint,
 OUT pl_compilation_time bigint,
 OUT net_send_time bigint,
 OUT data_io_time bigint,
 OUT is_slow_query bigint,
 OUT srt1_q bigint,
 OUT srt2_simple_query bigint,
 OUT srt3_analyze_rewrite bigint,
 OUT srt4_plan_query bigint,
 OUT srt5_light_query bigint,
 OUT srt6_p bigint,
 OUT srt7_b bigint,
 OUT srt8_e bigint,
 OUT srt9_d bigint,
 OUT srt10_s bigint,
 OUT srt11_c bigint,
 OUT srt12_u bigint,
 OUT srt13_before_query bigint,
 OUT srt14_after_query bigint,
 OUT rtt_unknown bigint
 ) RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_wlm_session_info';


CREATE VIEW pg_catalog.gs_wlm_session_info_all AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(0);

--process wlm_session info
CREATE VIEW pg_catalog.gs_wlm_session_info AS
SELECT
        S.datid,
        S.dbname,
        S.schemaname,
        S.nodename,
        S.username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        S.query_band,
        S.block_time,
        S.start_time,
        S.finish_time,
        S.duration,
        S.estimate_total_time,
        S.status,
        S.abort_info,
        S.resource_pool,
        S.control_group,
        S.estimate_memory,
        S.min_peak_memory,
        S.max_peak_memory,
        S.average_peak_memory,
        S.memory_skew_percent,
        S.spill_info,
        S.min_spill_size,
        S.max_spill_size,
        S.average_spill_size,
        S.spill_skew_percent,
        S.min_dn_time,
        S.max_dn_time,
        S.average_dn_time,
        S.dntime_skew_percent,
        S.min_cpu_time,
        S.max_cpu_time,
        S.total_cpu_time,
        S.cpu_skew_percent,
        S.min_peak_iops,
        S.max_peak_iops,
        S.average_peak_iops,
        S.iops_skew_percent,
        S.warning,
        S.queryid,
        S.query,
        S.query_plan,
        S.node_group,
        S.cpu_top1_node_name,
        S.cpu_top2_node_name,
        S.cpu_top3_node_name,
        S.cpu_top4_node_name,
        S.cpu_top5_node_name,
        S.mem_top1_node_name,
        S.mem_top2_node_name,
        S.mem_top3_node_name,
        S.mem_top4_node_name,
        S.mem_top5_node_name,
        S.cpu_top1_value,
        S.cpu_top2_value,
        S.cpu_top3_value,
        S.cpu_top4_value,
        S.cpu_top5_value,
        S.mem_top1_value,
        S.mem_top2_value,
        S.mem_top3_value,
        S.mem_top4_value,
        S.mem_top5_value,
        S.top_mem_dn,
        S.top_cpu_dn
FROM gs_wlm_session_query_info_all S;

CREATE VIEW pg_catalog.gs_wlm_session_history AS
SELECT
        S.datid,
        S.dbname,
        S.schemaname,
        S.nodename,
        S.username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        S.query_band,
        S.block_time,
        S.start_time,
        S.finish_time,
        S.duration,
        S.estimate_total_time,
        S.status,
        S.abort_info,
        S.resource_pool,
        S.control_group,
        S.estimate_memory,
        S.min_peak_memory,
        S.max_peak_memory,
        S.average_peak_memory,
        S.memory_skew_percent,
        S.spill_info,
        S.min_spill_size,
        S.max_spill_size,
        S.average_spill_size,
        S.spill_skew_percent,
        S.min_dn_time,
        S.max_dn_time,
        S.average_dn_time,
        S.dntime_skew_percent,
        S.min_cpu_time,
        S.max_cpu_time,
        S.total_cpu_time,
        S.cpu_skew_percent,
        S.min_peak_iops,
        S.max_peak_iops,
        S.average_peak_iops,
        S.iops_skew_percent,
        S.warning,
        S.queryid,
        S.query,
        S.query_plan,
        S.node_group,
        S.cpu_top1_node_name,
        S.cpu_top2_node_name,
        S.cpu_top3_node_name,
        S.cpu_top4_node_name,
        S.cpu_top5_node_name,
        S.mem_top1_node_name,
        S.mem_top2_node_name,
        S.mem_top3_node_name,
        S.mem_top4_node_name,
        S.mem_top5_node_name,
        S.cpu_top1_value,
        S.cpu_top2_value,
        S.cpu_top3_value,
        S.cpu_top4_value,
        S.cpu_top5_value,
        S.mem_top1_value,
        S.mem_top2_value,
        S.mem_top3_value,
        S.mem_top4_value,
        S.mem_top5_value,
        S.top_mem_dn,
        S.top_cpu_dn
FROM pg_catalog.gs_wlm_session_info_all S;

CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_session_info(IN flag int)
RETURNS int
AS $$
DECLARE
	query_str text;
	record_cnt int;
	BEGIN
		record_cnt := 0;

		query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(1)';

		IF flag > 0 THEN
			EXECUTE 'INSERT INTO pg_catalog.gs_wlm_session_query_info_all ' || query_str;
		ELSE
			EXECUTE query_str;
		END IF;

		RETURN record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;

DROP FUNCTION IF EXISTS dbe_perf.global_slow_query_info() cascade;
DROP FUNCTION IF EXISTS dbe_perf.global_slow_query_history() cascade;
DROP VIEW IF EXISTS dbe_perf.gs_slow_query_info cascade;
DROP VIEW IF EXISTS dbe_perf.gs_slow_query_history cascade;

CREATE OR REPLACE VIEW dbe_perf.gs_slow_query_info AS
SELECT
		S.dbname,
		S.schemaname,
		S.nodename,
		S.username,
		S.queryid,
		S.query,
		S.start_time,
		S.finish_time,
		S.duration,
		S.query_plan,
		S.n_returned_rows,
		S.n_tuples_fetched,
		S.n_tuples_returned,
		S.n_tuples_inserted,
		S.n_tuples_updated,
		S.n_tuples_deleted,
		S.n_blocks_fetched,
		S.n_blocks_hit,
		S.db_time,
		S.cpu_time,
		S.execution_time,
		S.parse_time,
		S.plan_time,
		S.rewrite_time,
		S.pl_execution_time,
		S.pl_compilation_time,
		S.net_send_time,
		S.data_io_time,
		S.srt1_q,
	    S.srt2_simple_query,
	    S.srt3_analyze_rewrite,
	    S.srt4_plan_query,
	    S.srt5_light_query,
	    S.srt6_p,
	    S.srt7_b,
	    S.srt8_e,
	    S.srt9_d,
	    S.srt10_s,
	    S.srt11_c,
	    S.srt12_u,
	    S.srt13_before_query,
	    S.srt14_after_query,
	    S.rtt_unknown
FROM gs_wlm_session_query_info_all S where S.is_slow_query = 1;

CREATE OR REPLACE VIEW dbe_perf.gs_slow_query_history AS
SELECT
		S.dbname,
		S.schemaname,
		S.nodename,
		S.username,
		S.queryid,
		S.query,
		S.start_time,
		S.finish_time,
		S.duration,
		S.query_plan,
		S.n_returned_rows,
		S.n_tuples_fetched,
		S.n_tuples_returned,
		S.n_tuples_inserted,
		S.n_tuples_updated,
		S.n_tuples_deleted,
		S.n_blocks_fetched,
		S.n_blocks_hit,
		S.db_time,
		S.cpu_time,
		S.execution_time,
		S.parse_time,
		S.plan_time,
		S.rewrite_time,
		S.pl_execution_time,
		S.pl_compilation_time,
		S.net_send_time,
		S.data_io_time,
        S.srt1_q,
	    S.srt2_simple_query,
	    S.srt3_analyze_rewrite,
	    S.srt4_plan_query,
	    S.srt5_light_query,
	    S.srt6_p,
	    S.srt7_b,
	    S.srt8_e,
	    S.srt9_d,
	    S.srt10_s,
	    S.srt11_c,
	    S.srt12_u,
	    S.srt13_before_query,
	    S.srt14_after_query,
	    S.rtt_unknown
FROM pg_catalog.pg_stat_get_wlm_session_info(0) S where S.is_slow_query = 1;

CREATE OR REPLACE FUNCTION dbe_perf.global_slow_query_history
RETURNS setof dbe_perf.gs_slow_query_history
AS $$
DECLARE
		row_data dbe_perf.gs_slow_query_history%rowtype;
		row_name record;
		query_str text;
		query_str_nodes text;
		BEGIN
				--Get all the node names
				query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
				FOR row_name IN EXECUTE(query_str_nodes) LOOP
						query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM dbe_perf.gs_slow_query_history''';
						FOR row_data IN EXECUTE(query_str) LOOP
								return next row_data;
						END LOOP;
				END LOOP;
				return;
		END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION dbe_perf.global_slow_query_info()
RETURNS setof dbe_perf.gs_slow_query_info
AS $$
DECLARE
		row_data dbe_perf.gs_slow_query_info%rowtype;
		row_name record;
		query_str text;
		query_str_nodes text;
		BEGIN
				--Get all the node names
				query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
				FOR row_name IN EXECUTE(query_str_nodes) LOOP
						query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM dbe_perf.gs_slow_query_info''';
						FOR row_data IN EXECUTE(query_str) LOOP
								return next row_data;
						END LOOP;
				END LOOP;
				return;
		END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_slow_query_history AS
SELECT * FROM DBE_PERF.global_slow_query_history();

CREATE OR REPLACE VIEW DBE_PERF.global_slow_query_info AS
SELECT * FROM DBE_PERF.global_slow_query_info();

GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_query_info_all TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_history TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info_all TO PUBLIC;
GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;
GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
GRANT SELECT ON TABLE DBE_PERF.global_slow_query_history TO PUBLIC;
GRANT SELECT ON TABLE DBE_PERF.global_slow_query_info TO PUBLIC;

do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select extname from pg_extension where extname='dolphin' and extversion = '2.0.1')
    LOOP
        if ans = true then
            ALTER EXTENSION dolphin UPDATE TO '2.0';
        end if;
    exit;
    END LOOP;
END$$;