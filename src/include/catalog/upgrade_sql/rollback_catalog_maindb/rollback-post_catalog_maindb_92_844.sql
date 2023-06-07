--------------------------------------------------------------
-- delete pg_catalog.pg_collation 
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.Delete_pg_collation_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_collation where collname in ('utf8mb4_general_ci', 'utf8mb4_unicode_ci', 'utf8mb4_bin', 'binary', 'utf8_general_ci', 'utf8_unicode_ci', 'utf8_bin');
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';
 
SELECT pg_catalog.Delete_pg_collation_temp();
DROP FUNCTION pg_catalog.Delete_pg_collation_temp();

DROP INDEX IF EXISTS pg_catalog.pg_collation_enc_def_index;DROP FUNCTION IF EXISTS pg_catalog.gs_validate_ext_listen_ip() cascade;DO $DO$
DECLARE
    ans boolean;
    check_version boolean;
BEGIN
        SELECT case WHEN count(*)=1 THEN true ELSE false END AS ans FROM (SELECT nspname FROM pg_namespace WHERE nspname='dbe_perf' LIMIT 1) INTO ans;
        IF ans = true THEN
            DROP VIEW IF EXISTS DBE_PERF.statement_history cascade;
            DROP FUNCTION IF EXISTS DBE_PERF.standby_statement_history(boolean);
            DROP FUNCTION IF EXISTS DBE_PERF.standby_statement_history(boolean, timestamp with time zone[]);

            SELECT case WHEN (working_version_num() >= 92606 AND working_version_num() < 90656) OR (working_version_num() >= 92827) THEN true ELSE false END INTO check_version;
            IF check_version = true THEN
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3118;
                CREATE OR REPLACE FUNCTION dbe_perf.standby_statement_history(
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
                    OUT trace_id text)
                RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
                LANGUAGE internal AS $function$standby_statement_history_1v$function$;

                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3119;
                CREATE OR REPLACE FUNCTION dbe_perf.standby_statement_history(
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
                    OUT trace_id text)
                RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
                LANGUAGE internal AS $function$standby_statement_history$function$;
            END IF;

            check_version = false;
            SELECT case WHEN (working_version_num() >= 92301 AND working_version_num() < 90420) THEN true ELSE false END INTO check_version;
            IF check_version = true THEN
                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3118;
                CREATE OR REPLACE FUNCTION dbe_perf.standby_statement_history(
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
                    OUT is_slow_sql boolean)
                RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
                LANGUAGE internal AS $function$standby_statement_history_1v$function$;

                SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3119;
                CREATE OR REPLACE FUNCTION dbe_perf.standby_statement_history(
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
                    OUT is_slow_sql boolean)
                RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 10000
                LANGUAGE internal AS $function$standby_statement_history$function$;
            END IF;
        END IF;
END$DO$;

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
    trace_id text
);
REVOKE ALL on table pg_catalog.statement_history FROM public;
create index pg_catalog.statement_history_time_idx on pg_catalog.statement_history USING btree (start_time, is_slow_sql);

DO $DO$
DECLARE
    ans boolean;
    username text;
    querystr text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    IF ans = true then
        CREATE VIEW DBE_PERF.statement_history AS select * from pg_catalog.statement_history;

        SELECT SESSION_USER INTO username;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='statement_history') THEN
            querystr := 'REVOKE SELECT on table dbe_perf.statement_history FROM public;';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
        END IF;
    END IF;
END$DO$;

DO $DO$
DECLARE
    ans boolean;
    username text;
    querystr text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    IF ans = true THEN
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_full_sql_by_timestamp(timestamp with time zone, timestamp with time zone) cascade;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_slow_sql_by_timestamp(timestamp with time zone, timestamp with time zone) cascade;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_full_sql_by_timestamp
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
        OUT is_slow_sql boolean,
        OUT trace_id text)
        RETURNS setof record
        AS $$
        DECLARE
        row_data pg_catalog.statement_history%rowtype;
        row_name record;
        query_str text;
        query_str_nodes text;
        BEGIN
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
                    return next;
                END LOOP;
            END LOOP;
            return;
        END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

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
        OUT is_slow_sql boolean,
        OUT trace_id text)
        RETURNS setof record
        AS $$
        DECLARE
        row_data pg_catalog.statement_history%rowtype;
        row_name record;
        query_str text;
        query_str_nodes text;
        BEGIN
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
                    return next;
                END LOOP;
            END LOOP;
            return;
        END; $$
        LANGUAGE 'plpgsql' NOT FENCED;
    END IF;
END$DO$;

