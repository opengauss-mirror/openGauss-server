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
 ) RETURNS setof record LANGUAGE INTERNAL STABLE ROWS 100 NOT FENCED as 'pg_stat_get_wlm_session_info';


CREATE OR REPLACE VIEW DBE_PERF.global_slow_query_info AS
SELECT * FROM DBE_PERF.global_slow_query_info();


CREATE OR REPLACE VIEW dbe_perf.statement_complex_history AS
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
FROM pg_catalog.pg_stat_get_wlm_session_info(0) S;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statement_complex_history()
RETURNS setof dbe_perf.statement_complex_history
AS $$
DECLARE
  row_data dbe_perf.statement_complex_history%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.statement_complex_history';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_statement_complex_history AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history();

CREATE OR REPLACE VIEW DBE_PERF.statement_complex_history_table AS
  SELECT * FROM gs_wlm_session_info;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statement_complex_history_table()
RETURNS setof dbe_perf.statement_complex_history_table
AS $$
DECLARE
  row_data dbe_perf.statement_complex_history_table%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statement_complex_history_table';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_statement_complex_history_table AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history_table();


DROP FUNCTION IF EXISTS dbe_perf.standby_statement_history(boolean, timestamp with time zone[]);
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
    OUT rtt_unknown bigint,
    OUT parent_query_id bigint,
    OUT net_trans_time bigint)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
LANGUAGE internal AS $function$standby_statement_history$function$;