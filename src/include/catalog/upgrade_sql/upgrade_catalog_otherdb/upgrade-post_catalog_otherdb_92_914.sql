DO $DO$
DECLARE
ans boolean;
BEGIN
select case when count(*) = 1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
IF ans = true then
DROP FUNCTION IF EXISTS DBE_PERF.get_global_full_sql_by_timestamp();
DROP FUNCTION IF EXISTS DBE_PERF.get_global_slow_sql_by_timestamp();
DROP VIEW IF EXISTS DBE_PERF.statement_history;
END IF;
END $DO$;

DROP INDEX IF EXISTS pg_catalog.statement_history_time_idx;
DROP TABLE IF EXISTS pg_catalog.statement_history cascade;

CREATE unlogged table IF NOT EXISTS pg_catalog.statement_history(
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
    parent_query_id bigint
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
    OUT rtt_unknown bigint,
    OUT parent_query_id bigint)
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
    OUT rtt_unknown bigint,
    OUT parent_query_id bigint)
    RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
LANGUAGE internal AS $function$standby_statement_history$function$;

DO $DO$
DECLARE
ans boolean;
    username text;
    querystr text;
BEGIN
select case when count(*) = 1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
IF ans = true then
CREATE VIEW DBE_PERF.statement_history AS
select * from pg_catalog.statement_history;
CREATE OR REPLACE FUNCTION DBE_PERF.get_global_full_sql_by_timestamp
            ( IN start_timestamp timestamp with time zone,
            IN end_timestamp timestamp with time zone,
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
            OUT lwlock_time bigint ,
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
            OUT rtt_unknown bigint
            )
            returns setof record
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
                    IF row_data.parent_query_id = 0 then
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
END IF;
END LOOP;
END LOOP;
            return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_slow_sql_by_timestamp
            ( IN start_timestamp timestamp with time zone,
            IN end_timestamp timestamp with time zone,
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
            OUT lwlock_time bigint ,
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
            OUT rtt_unknown bigint
            )
            returns setof record
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
                query_str := 'SELECT * FROM DBE_PERF.statement_history where start_time >= ''' ||$1|| ''' and start_time <= ''' || $2 || ''' and is_slow_sql = true';
FOR row_data IN EXECUTE(query_str) LOOP
                   IF row_data.parent_query_id = 0 THEN
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
END IF;
END LOOP;
END LOOP;
            return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

SELECT SESSION_USER INTO username;
IF EXISTS(SELECT oid from pg_catalog.pg_class where relname='statement_history') then
            querystr := 'REVOKE SELECT on table dbe_perf.statement_history from public;';
EXECUTE IMMEDIATE querystr;
querystr := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(username) || ';';
EXECUTE IMMEDIATE querystr;
querystr := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(username) || ';';
EXECUTE IMMEDIATE querystr;
GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
END IF;
END IF;
END $DO$;

do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = true then
            ALTER EXTENSION dolphin UPDATE TO '3.0';
        end if;
        exit;
    END LOOP;
END$$;