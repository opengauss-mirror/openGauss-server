
DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='statement_history') THEN
        query_str := 'REVOKE SELECT on table dbe_perf.statement_history FROM public;';
        EXECUTE IMMEDIATE query_str;
        query_str := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        query_str := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
    END IF;
END;
/


DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;

    IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='track_memory_context_detail') THEN
      query_str := 'REVOKE INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.track_memory_context_detail FROM ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE query_str;
      REVOKE SELECT ON TABLE dbe_perf.track_memory_context_detail FROM PUBLIC;
    END IF;
END;
/

DO $DO$
DECLARE
  ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_full_sql_by_timestamp() cascade;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_slow_sql_by_timestamp() cascade;
        DROP VIEW IF EXISTS DBE_PERF.statement_history cascade;
    end if;
END$DO$;

DROP INDEX IF EXISTS pg_catalog.statement_history_time_idx;
DROP TABLE IF EXISTS pg_catalog.statement_history;

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
    details text
);
REVOKE ALL on table pg_catalog.statement_history FROM public;
create index pg_catalog.statement_history_time_idx on pg_catalog.statement_history USING btree (start_time);

DO $DO$
DECLARE
  ans boolean;
  username text;
  querystr text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        CREATE VIEW DBE_PERF.statement_history AS
            select * from pg_catalog.statement_history;

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
           OUT details text)
         RETURNS setof record
         AS $$
         DECLARE
          row_data pg_catalog.statement_history%rowtype;
          query_str text;
          -- node name
          node_names name[];
          each_node_name name;
          BEGIN
            -- Get all node names(CN + master DN)
            node_names := ARRAY(SELECT pgxc_node.node_name FROM pgxc_node WHERE (node_type = 'C' or node_type = 'D') AND nodeis_active = true);
            FOREACH each_node_name IN ARRAY node_names
            LOOP
                query_str := 'EXECUTE DIRECT ON (' || each_node_name || ') ''SELECT * FROM DBE_PERF.statement_history where start_time >= ''''' ||$1|| ''''' and start_time <= ''''' || $2 || '''''''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    node_name := each_node_name;
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
           OUT details text)
         RETURNS setof record
         AS $$
         DECLARE
          row_data pg_catalog.statement_history%rowtype;
          row_name record;
          query_str text;
          -- node name
          node_names name[];
          each_node_name name;
          BEGIN
            -- Get all node names(CN + master DN)
            node_names := ARRAY(SELECT pgxc_node.node_name FROM pgxc_node WHERE (node_type = 'C' or node_type = 'D') AND nodeis_active = true);
            FOREACH each_node_name IN ARRAY node_names
            LOOP
                query_str := 'EXECUTE DIRECT ON (' || each_node_name || ') ''SELECT * FROM DBE_PERF.statement_history where start_time >= ''''' ||$1|| ''''' and start_time <= ''''' || $2 || ''''' and (extract(epoch from (finish_time - start_time))  * 1000000) >= slow_sql_threshold ''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    node_name := each_node_name;
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
                    return next;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        DROP FUNCTION IF EXISTS pg_catalog.statement_detail_decode() CASCADE;
        set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5732;
        CREATE OR REPLACE FUNCTION pg_catalog.statement_detail_decode
        (  IN text,
           IN text,
           IN boolean)
        RETURNS text LANGUAGE INTERNAL NOT FENCED as 'statement_detail_decode';

        SELECT SESSION_USER INTO username;
        querystr := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(username) || ';';
        EXECUTE IMMEDIATE querystr;
        querystr := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(username) || ';';
        EXECUTE IMMEDIATE querystr;
        GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
    end if;
END$DO$;

CREATE OR REPLACE PROCEDURE proc_hotkeyview()
AS
    BEGIN
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='global_stat_hotkeys_info') THEN
            REVOKE SELECT ON TABLE global_stat_hotkeys_info FROM PUBLIC;
        END IF;
    END;
/

CALL proc_hotkeyview();
DROP PROCEDURE IF EXISTS proc_hotkeyview;

CREATE OR REPLACE PROCEDURE proc_matview()
AS
    BEGIN
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matview') THEN
            REVOKE SELECT ON TABLE pg_catalog.gs_matview FROM PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matview_dependency') THEN
            REVOKE SELECT ON TABLE pg_catalog.gs_matview_dependency FROM PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matviews') THEN
            REVOKE SELECT ON TABLE pg_catalog.gs_matviews FROM PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='track_memory_context_detail') THEN
            REVOKE SELECT ON TABLE dbe_perf.track_memory_context_detail FROM PUBLIC;
        END IF;
    END;
/

CALL proc_matview();
DROP PROCEDURE IF EXISTS proc_matview;

create or replace function table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    dist_type text;
    num int :=0;
    sqltmp text;
    BEGIN
        sqltmp := 'select count(*) from pg_attribute join pg_class on pg_attribute.attrelid = pg_class.oid where pg_class.relname =' || quote_literal(table_name) ||' and pg_attribute.attname = ' ||quote_literal($2);
        EXECUTE immediate sqltmp into num;
        if num = 0 then
            return;
        end if;
        -- make sure not to affect the logic for non-range/list distribution tables
        EXECUTE immediate 'select a.pclocatortype from (pgxc_class a join pg_class b on a.pcrelid = b.oid join pg_namespace c on c.oid = b.relnamespace)
                            where b.relname = quote_ident(:1) and c.nspname in (select unnest(current_schemas(false)))' into dist_type using table_name;
        if dist_type <> 'G' and dist_type <> 'L' then
            dist_type = 'H'; -- dist type used to be hardcoded as 'H'
        end if;

        if row_num = 0 then
            EXECUTE 'select count(1) from ' || $1 into tolal_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''' || dist_type || ''') as seqNum from ' || $1 ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, count(1) as num
                           from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''' || dist_type || ''') as seqNum from ' || $1 ||
                            ' limit ' || row_num ||') group by seqNum order by num DESC';
        end if;

        if tolal_num = 0 then
            seqNum = 0;
            Num = 0;
            Ratio = ROUND(0, 3) || '%';
            return;
        end if;

        for row_data in EXECUTE execute_query loop
            seqNum = row_data.seqNum;
            Num = row_data.num;
            Ratio = ROUND(row_data.num / tolal_num * 100, 3) || '%';
            RETURN next;
        end loop;
    END;
$$LANGUAGE plpgsql NOT FENCED;
drop function if exists report_application_error(text,integer);

create or replace function table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    dist_type text;
    BEGIN
        -- make sure not to affect the logic for non-range/list distribution tables
        EXECUTE immediate 'select a.pclocatortype from (pgxc_class a join pg_class b on a.pcrelid = b.oid join pg_namespace c on c.oid = b.relnamespace)
                            where b.relname = quote_ident(:1) and c.nspname in (select unnest(current_schemas(false)))' into dist_type using table_name;
        if dist_type <> 'G' and dist_type <> 'L' then
            dist_type = 'H'; -- dist type used to be hardcoded as 'H'
        end if;

        if row_num = 0 then
            EXECUTE 'select count(1) from ' || $1 into tolal_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''' || dist_type || ''') as seqNum from ' || $1 ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''' || dist_type || ''') as seqNum from ' || $1 ||
                            ' limit ' || row_num ||') group by seqNum order by num DESC';
        end if;

        if tolal_num = 0 then
            seqNum = 0;
            Num = 0;
			Ratio = ROUND(0, 3) || '%';
            return;
        end if;

        for row_data in EXECUTE execute_query loop
            seqNum = row_data.seqNum;
            Num = row_data.num;
            Ratio = ROUND(row_data.num / tolal_num * 100, 3) || '%';
            RETURN next;
        end loop;
    END;
$$LANGUAGE plpgsql NOT FENCED;


create or replace function pg_catalog.pgxc_cgroup_map_ng_conf(IN ngname text)
returns bool
AS $$
declare
    query_string text;
    flag boolean;
begin
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN ngname, IN special;
    IF flag = true THEN
        return false;
    ELSE
        query_string := 'execute direct on ALL ''SELECT * from pg_catalog.gs_cgroup_map_ng_conf(''''' || ngname || ''''')''';
        execute query_string;
    END IF;
    return true;
end;
$$language plpgsql NOT FENCED;

DO $$
DECLARE
ans boolean;
user_name text;
query_str text;
BEGIN
    select case when count(*)=1 then true else false end as ans from
        (select * from pg_class where relname = 'db_users' and relnamespace=11 limit 1) into ans;
    if ans = true then
        GRANT SELECT on pg_catalog.db_users TO public;

        SELECT SESSION_USER INTO user_name;
        query_str := 'REVOKE ALL on pg_catalog.db_users FROM ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
    end if;
END $$;

DO $DO$
DECLARE
    ans boolean;
    user_name text;
    global_query_str text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_os_runtime
          (OUT node_name name, OUT id integer, OUT name text, OUT value numeric, OUT comments text, OUT cumulative boolean)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.os_runtime%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.os_runtime''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                id := row_data.id;
                name := row_data.name;
                value := row_data.value;
                comments := row_data.comments;
                cumulative := row_data.cumulative;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_os_threads()
        RETURNS setof DBE_PERF.os_threads
        AS $$
        DECLARE
          row_data DBE_PERF.os_threads%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.os_threads''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_instance_time
          (OUT node_name name, OUT stat_id integer, OUT stat_name text, OUT value bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.instance_time%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all CN DN node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.instance_time''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                stat_id := row_data.stat_id;
                stat_name := row_data.stat_name;
                value := row_data.value;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_stat
          (OUT node_name name, OUT sessid text, OUT statid integer, OUT statname text, OUT statunit text, OUT value bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_stat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.session_stat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                sessid := row_data.sessid;
                statid := row_data.statid;
                statname := row_data.statname;
                statunit := row_data.statunit;
                value := row_data.value;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_time
          (OUT node_name name, OUT sessid text, OUT stat_id integer, OUT stat_name text, OUT value bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_time%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.session_time''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                sessid := row_data.sessid;
                stat_id := row_data.stat_id;
                stat_name := row_data.stat_name;
                value := row_data.value;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_memory
          (OUT node_name name, OUT sessid text, OUT init_mem integer, OUT used_mem integer, OUT peak_mem integer)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_memory%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.session_memory''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                sessid := row_data.sessid;
                init_mem := row_data.init_mem;
                used_mem := row_data.used_mem;
                peak_mem := row_data.peak_mem;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_memory_detail
          (OUT node_name name, OUT sessid text, OUT sesstype text, OUT contextname text, OUT level smallint,
           OUT parent text, OUT totalsize bigint, OUT freesize bigint, OUT usedsize bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_memory_detail%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.session_memory_detail''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                sessid := row_data.sessid;
                sesstype := row_data.sesstype;
                contextname := row_data.contextname;
                level := row_data.level;
                parent := row_data.parent;
                totalsize := row_data.totalsize;
                freesize := row_data.freesize;
                usedsize := row_data.usedsize;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_thread_wait_status()
        RETURNS setof DBE_PERF.thread_wait_status
        AS $$
        DECLARE
          row_data DBE_PERF.thread_wait_status%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn dn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.thread_wait_status''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_operator_history_table()
        RETURNS setof DBE_PERF.operator_history_table
        AS $$
        DECLARE
          row_data DBE_PERF.operator_history_table%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the CN node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.operator_history_table ''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_operator_history()
        RETURNS setof DBE_PERF.operator_history
        AS $$
        DECLARE
          row_data DBE_PERF.operator_history%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.operator_history''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_operator_runtime()
        RETURNS setof DBE_PERF.operator_runtime
        AS $$
        DECLARE
          row_data DBE_PERF.operator_runtime%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.operator_runtime''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statement_complex_history()
        RETURNS setof DBE_PERF.statement_complex_history
        AS $$
        DECLARE
          row_data DBE_PERF.statement_complex_history%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement_complex_history''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statement_complex_runtime()
        RETURNS setof DBE_PERF.statement_complex_runtime
        AS $$
        DECLARE
          row_data DBE_PERF.statement_complex_runtime%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.statement_complex_runtime''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_shared_memory_detail
          (OUT node_name name, OUT contextname text, OUT level smallint, OUT parent text,
           OUT totalsize bigint, OUT freesize bigint, OUT usedsize bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_name record;
          row_data DBE_PERF.shared_memory_detail%rowtype;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.shared_memory_detail''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    node_name := row_name.node_name;
                    contextname := row_data.contextname;
                    level := row_data.level;
                    parent := row_data.parent;
                    totalsize := row_data.totalsize;
                    freesize := row_data.freesize;
                    usedsize := row_data.usedsize;
                    RETURN NEXT;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_all_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_all_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_all_indexes
          (OUT schemaname name, OUT toastrelschemaname name, OUT toastrelname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_blks_read AS idx_blks_read,
                T.idx_blks_hit AS idx_blks_hit
              FROM DBE_PERF.statio_all_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_all_sequences
          (OUT node_name name, OUT relid oid, OUT schemaname name,
           OUT relname name, OUT blks_read bigint, OUT blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_all_sequences''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                blks_read := row_data.blks_read;
                blks_hit := row_data.blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_all_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT heap_blks_read bigint,
           OUT heap_blks_hit bigint, OUT idx_blks_read bigint, OUT idx_blks_hit bigint, OUT toast_blks_read bigint,
           OUT toast_blks_hit bigint, OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_all_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_all_tables
          (OUT schemaname name, OUT relname name, OUT toastrelschemaname name, OUT toastrelname name, OUT heap_blks_read bigint,
           OUT heap_blks_hit bigint, OUT idx_blks_read bigint, OUT idx_blks_hit bigint, OUT toast_blks_read bigint,
           OUT toast_blks_hit bigint, OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                SELECT
                    C.relname AS relname,
                    C.schemaname AS schemaname,
                    O.relname AS toastrelname,
                    N.nspname AS toastrelschemaname,
                    C.heap_blks_read AS heap_blks_read,
                    C.heap_blks_hit AS heap_blks_hit,
                    C.idx_blks_read AS idx_blks_read,
                    C.idx_blks_hit AS idx_blks_hit,
                    C.toast_blks_read AS toast_blks_read,
                    C.toast_blks_hit AS toast_blks_hit,
                    C.tidx_blks_read AS tidx_blks_read,
                    C.tidx_blks_hit AS tidx_blks_hit
                FROM DBE_PERF.statio_all_tables C
                    LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
                    LEFT JOIN pg_namespace N ON O.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_sys_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_sys_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_sys_indexes
          (OUT schemaname name, OUT toastrelschemaname name, OUT toastrelname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_blks_read AS idx_blks_read,
                T.idx_blks_hit AS idx_blks_hit
              FROM DBE_PERF.statio_sys_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_sys_sequences
          (OUT node_name name, OUT relid oid, OUT schemaname name,
           OUT relname name, OUT blks_read bigint, OUT blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_sys_sequences''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                blks_read := row_data.blks_read;
                blks_hit := row_data.blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_sys_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT heap_blks_read bigint,
           OUT heap_blks_hit bigint, OUT idx_blks_read bigint, OUT idx_blks_hit bigint, OUT toast_blks_read bigint,
           OUT toast_blks_hit bigint, OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_sys_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_sys_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT heap_blks_read bigint, OUT heap_blks_hit bigint,
           OUT idx_blks_read bigint, OUT idx_blks_hit bigint,
           OUT toast_blks_read bigint, OUT toast_blks_hit bigint,
           OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                  C.schemaname AS schemaname,
                  C.relname AS relname,
                  O.relname AS toastrelname,
                  N.nspname AS toastrelschemaname,
                  C.heap_blks_read AS heap_blks_read,
                  C.heap_blks_hit AS heap_blks_hit,
                  C.idx_blks_read AS idx_blks_read,
                  C.idx_blks_hit AS idx_blks_hit,
                  C.toast_blks_read AS toast_blks_read,
                  C.toast_blks_hit AS toast_blks_hit,
                  C.tidx_blks_read AS tidx_blks_read,
                  C.tidx_blks_hit AS tidx_blks_hit
              FROM DBE_PERF.statio_sys_tables C
                  LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
                  LEFT JOIN pg_namespace N ON O.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_user_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_user_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_user_indexes
          (OUT schemaname name, OUT toastrelschemaname name, OUT toastrelname name,
           OUT relname name, OUT indexrelname name, OUT idx_blks_read numeric, OUT idx_blks_hit numeric)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_blks_read AS idx_blks_read,
                T.idx_blks_hit AS idx_blks_hit
              FROM DBE_PERF.statio_user_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_user_sequences
          (OUT node_name name, OUT relid oid, OUT schemaname name,
           OUT relname name, OUT blks_read bigint, OUT blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_user_sequences''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                blks_read := row_data.blks_read;
                blks_hit := row_data.blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statio_user_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT heap_blks_read bigint,
           OUT heap_blks_hit bigint, OUT idx_blks_read bigint, OUT idx_blks_hit bigint, OUT toast_blks_read bigint,
           OUT toast_blks_hit bigint, OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statio_user_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statio_user_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT heap_blks_read bigint, OUT heap_blks_hit bigint,
           OUT idx_blks_read bigint, OUT idx_blks_hit bigint,
           OUT toast_blks_read bigint, OUT toast_blks_hit bigint,
           OUT tidx_blks_read bigint, OUT tidx_blks_hit bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                C.schemaname AS schemaname,
                C.relname AS relname,
                O.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                C.heap_blks_read AS heap_blks_read,
                C.heap_blks_hit AS heap_blks_hit,
                C.idx_blks_read AS idx_blks_read,
                C.idx_blks_hit AS idx_blks_hit,
                C.toast_blks_read AS toast_blks_read,
                C.toast_blks_hit AS toast_blks_hit,
                C.tidx_blks_read AS tidx_blks_read,
                C.tidx_blks_hit AS tidx_blks_hit
              FROM DBE_PERF.statio_user_tables C
                LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
                LEFT JOIN pg_namespace N ON O.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                heap_blks_read := row_data.heap_blks_read;
                heap_blks_hit := row_data.heap_blks_hit;
                idx_blks_read := row_data.idx_blks_read;
                idx_blks_hit := row_data.idx_blks_hit;
                toast_blks_read := row_data.toast_blks_read;
                toast_blks_hit := row_data.toast_blks_hit;
                tidx_blks_read := row_data.tidx_blks_read;
                tidx_blks_hit := row_data.tidx_blks_hit;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_dn_stat_all_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint,
           OUT n_tup_ins bigint, OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_all_tables''';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        node_name := row_name.node_name;
                        relid := row_data.relid;
                        schemaname := row_data.schemaname;
                        relname := row_data.relname;
                        seq_scan := row_data.seq_scan;
                        seq_tup_read := row_data.seq_tup_read;
                        idx_scan := row_data.idx_scan;
                        idx_tup_fetch := row_data.idx_tup_fetch;
                        n_tup_ins := row_data.n_tup_ins;
                        n_tup_upd := row_data.n_tup_upd;
                        n_tup_del := row_data.n_tup_del;
                        n_tup_hot_upd := row_data.n_tup_hot_upd;
                        n_live_tup := row_data.n_live_tup;
                        n_dead_tup := row_data.n_dead_tup;
                        last_vacuum := row_data.last_vacuum;
                        last_autovacuum := row_data.last_autovacuum;
                        last_analyze := row_data.last_analyze;
                        last_autoanalyze := row_data.last_autoanalyze;
                        vacuum_count := row_data.vacuum_count;
                        autovacuum_count := row_data.autovacuum_count;
                        analyze_count := row_data.analyze_count;
                        autoanalyze_count := row_data.autoanalyze_count;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_dn_stat_all_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint,
           OUT n_tup_ins bigint, OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                        SELECT
                            T.schemaname AS schemaname,
                            T.relname AS relname,
                            C.relname AS toastrelname,
                            N.nspname AS toastrelschemaname,
                            T.seq_scan AS seq_scan,
                            T.seq_tup_read AS seq_tup_read,
                            T.idx_scan AS idx_scan,
                            T.idx_tup_fetch AS idx_tup_fetch,
                            T.n_tup_ins AS n_tup_ins,
                            T.n_tup_upd AS n_tup_upd,
                            T.n_tup_del AS n_tup_del,
                            T.n_tup_hot_upd AS n_tup_hot_upd,
                            T.n_live_tup AS n_live_tup,
                            T.n_dead_tup AS n_dead_tup,
                            T.last_vacuum AS last_vacuum,
                            T.last_autovacuum AS last_autovacuum,
                            T.last_analyze AS last_analyze,
                            T.last_autoanalyze AS last_autoanalyze,
                            T.vacuum_count AS vacuum_count,
                            T.autovacuum_count AS autovacuum_count,
                            T.analyze_count AS analyze_count,
                            T.autoanalyze_count AS autoanalyze_count
                        FROM DBE_PERF.stat_all_tables T
                            LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                            LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        schemaname := row_data.schemaname;
                        IF row_data.toastrelname IS NULL THEN
                            relname := row_data.relname;
                        ELSE
                            relname := NULL;
                        END IF;
                        toastrelschemaname := row_data.toastrelschemaname;
                        toastrelname := row_data.toastrelname;
                        seq_scan := row_data.seq_scan;
                        seq_tup_read := row_data.seq_tup_read;
                        idx_scan := row_data.idx_scan;
                        idx_tup_fetch := row_data.idx_tup_fetch;
                        n_tup_ins := row_data.n_tup_ins;
                        n_tup_upd := row_data.n_tup_upd;
                        n_tup_del := row_data.n_tup_del;
                        n_tup_hot_upd := row_data.n_tup_hot_upd;
                        n_live_tup := row_data.n_live_tup;
                        n_dead_tup := row_data.n_dead_tup;
                        last_vacuum := row_data.last_vacuum;
                        last_autovacuum := row_data.last_autovacuum;
                        last_analyze := row_data.last_analyze;
                        last_autoanalyze := row_data.last_autoanalyze;
                        vacuum_count := row_data.vacuum_count;
                        autovacuum_count := row_data.autovacuum_count;
                        analyze_count := row_data.analyze_count;
                        autoanalyze_count := row_data.autoanalyze_count;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_cn_stat_all_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint,
           OUT n_tup_ins bigint, OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                        SELECT
                            T.schemaname AS schemaname,
                            T.relname AS relname,
                            C.relname AS toastrelname,
                            N.nspname AS toastrelschemaname,
                            T.seq_scan AS seq_scan,
                            T.seq_tup_read AS seq_tup_read,
                            T.idx_scan AS idx_scan,
                            T.idx_tup_fetch AS idx_tup_fetch,
                            T.n_tup_ins AS n_tup_ins,
                            T.n_tup_upd AS n_tup_upd,
                            T.n_tup_del AS n_tup_del,
                            T.n_tup_hot_upd AS n_tup_hot_upd,
                            T.n_live_tup AS n_live_tup,
                            T.n_dead_tup AS n_dead_tup,
                            T.last_vacuum AS last_vacuum,
                            T.last_autovacuum AS last_autovacuum,
                            T.last_analyze AS last_analyze,
                            T.last_autoanalyze AS last_autoanalyze,
                            T.vacuum_count AS vacuum_count,
                            T.autovacuum_count AS autovacuum_count,
                            T.analyze_count AS analyze_count,
                            T.autoanalyze_count AS autoanalyze_count
                        FROM DBE_PERF.stat_all_tables T
                            LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                            LEFT JOIN pg_namespace N ON C.relnamespace = N.oid
                        WHERE T.schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        schemaname := row_data.schemaname;
                        IF row_data.toastrelname IS NULL THEN
                            relname := row_data.relname;
                        ELSE
                            relname := NULL;
                        END IF;
                        toastrelschemaname := row_data.toastrelschemaname;
                        toastrelname := row_data.toastrelname;
                        seq_scan := row_data.seq_scan;
                        seq_tup_read := row_data.seq_tup_read;
                        idx_scan := row_data.idx_scan;
                        idx_tup_fetch := row_data.idx_tup_fetch;
                        n_tup_ins := row_data.n_tup_ins;
                        n_tup_upd := row_data.n_tup_upd;
                        n_tup_del := row_data.n_tup_del;
                        n_tup_hot_upd := row_data.n_tup_hot_upd;
                        n_live_tup := row_data.n_live_tup;
                        n_dead_tup := row_data.n_dead_tup;
                        last_vacuum := row_data.last_vacuum;
                        last_autovacuum := row_data.last_autovacuum;
                        last_analyze := row_data.last_analyze;
                        last_autoanalyze := row_data.last_autoanalyze;
                        vacuum_count := row_data.vacuum_count;
                        autovacuum_count := row_data.autovacuum_count;
                        analyze_count := row_data.analyze_count;
                        autoanalyze_count := row_data.autoanalyze_count;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_all_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
           OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_all_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_all_indexes
          (OUT schemaname name, OUT relname name, OUT indexrelname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_scan AS idx_scan,
                T.idx_tup_read AS idx_tup_read,
                T.idx_tup_fetch AS idx_tup_fetch
              FROM DBE_PERF.stat_all_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_sys_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
           OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_sys_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                n_live_tup := row_data.n_live_tup;
                n_dead_tup := row_data.n_dead_tup;
                last_vacuum := row_data.last_vacuum;
                last_autovacuum := row_data.last_autovacuum;
                last_analyze := row_data.last_analyze;
                last_autoanalyze := row_data.last_autoanalyze;
                vacuum_count := row_data.vacuum_count;
                autovacuum_count := row_data.autovacuum_count;
                analyze_count := row_data.analyze_count;
                autoanalyze_count := row_data.autoanalyze_count;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_sys_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint,
           OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.schemaname AS schemaname,
                T.relname AS relname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.seq_scan AS seq_scan,
                T.seq_tup_read AS seq_tup_read,
                T.idx_scan AS idx_scan,
                T.idx_tup_fetch AS idx_tup_fetch,
                T.n_tup_ins AS n_tup_ins,
                T.n_tup_upd AS n_tup_upd,
                T.n_tup_del AS n_tup_del,
                T.n_tup_hot_upd AS n_tup_hot_upd,
                T.n_live_tup AS n_live_tup,
                T.n_dead_tup AS n_dead_tup,
                T.last_vacuum AS last_vacuum,
                T.last_autovacuum AS last_autovacuum,
                T.last_analyze AS last_analyze,
                T.last_autoanalyze AS last_autoanalyze,
                T.vacuum_count AS vacuum_count,
                T.autovacuum_count AS autovacuum_count,
                T.analyze_count AS analyze_count,
                T.autoanalyze_count AS autoanalyze_count
              FROM DBE_PERF.stat_sys_tables T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                n_live_tup := row_data.n_live_tup;
                n_dead_tup := row_data.n_dead_tup;
                last_vacuum := row_data.last_vacuum;
                last_autovacuum := row_data.last_autovacuum;
                last_analyze := row_data.last_analyze;
                last_autoanalyze := row_data.last_autoanalyze;
                vacuum_count := row_data.vacuum_count;
                autovacuum_count := row_data.autovacuum_count;
                analyze_count := row_data.analyze_count;
                autoanalyze_count := row_data.autoanalyze_count;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_sys_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
           OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_sys_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_sys_indexes
          (OUT schemaname name, OUT relname name, OUT indexrelname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_scan AS idx_scan,
                T.idx_tup_read AS idx_tup_read,
                T.idx_tup_fetch AS idx_tup_fetch
              FROM DBE_PERF.stat_sys_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_user_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
           OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''D'')';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_user_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                n_live_tup := row_data.n_live_tup;
                n_dead_tup := row_data.n_dead_tup;
                last_vacuum := row_data.last_vacuum;
                last_autovacuum := row_data.last_autovacuum;
                last_analyze := row_data.last_analyze;
                last_autoanalyze := row_data.last_autoanalyze;
                vacuum_count := row_data.vacuum_count;
                autovacuum_count := row_data.autovacuum_count;
                analyze_count := row_data.analyze_count;
                autoanalyze_count := row_data.autoanalyze_count;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_user_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint,
           OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''D'')';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.schemaname AS schemaname,
                T.relname AS relname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.seq_scan AS seq_scan,
                T.seq_tup_read AS seq_tup_read,
                T.idx_scan AS idx_scan,
                T.idx_tup_fetch AS idx_tup_fetch,
                T.n_tup_ins AS n_tup_ins,
                T.n_tup_upd AS n_tup_upd,
                T.n_tup_del AS n_tup_del,
                T.n_tup_hot_upd AS n_tup_hot_upd,
                T.n_live_tup AS n_live_tup,
                T.n_dead_tup AS n_dead_tup,
                T.last_vacuum AS last_vacuum,
                T.last_autovacuum AS last_autovacuum,
                T.last_analyze AS last_analyze,
                T.last_autoanalyze AS last_autoanalyze,
                T.vacuum_count AS vacuum_count,
                T.autovacuum_count AS autovacuum_count,
                T.analyze_count AS analyze_count,
                T.autoanalyze_count AS autoanalyze_count
              FROM DBE_PERF.stat_user_tables T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                n_live_tup := row_data.n_live_tup;
                n_dead_tup := row_data.n_dead_tup;
                last_vacuum := row_data.last_vacuum;
                last_autovacuum := row_data.last_autovacuum;
                last_analyze := row_data.last_analyze;
                last_autoanalyze := row_data.last_autoanalyze;
                vacuum_count := row_data.vacuum_count;
                autovacuum_count := row_data.autovacuum_count;
                analyze_count := row_data.analyze_count;
                autoanalyze_count := row_data.autoanalyze_count;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_user_indexes
          (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
           OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_user_indexes''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                indexrelid := row_data.indexrelid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                indexrelname := row_data.indexrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_user_indexes
          (OUT schemaname name, OUT relname name, OUT indexrelname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.indexrelname AS indexrelname,
                T.idx_scan AS idx_scan,
                T.idx_tup_read AS idx_tup_read,
                T.idx_tup_fetch AS idx_tup_fetch
              FROM DBE_PERF.stat_user_indexes T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                    indexrelname := row_data.indexrelname;
                ELSE
                    relname := NULL;
                    indexrelname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                idx_scan := row_data.idx_scan;
                idx_tup_read := row_data.idx_tup_read;
                idx_tup_fetch := row_data.idx_tup_fetch;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_database
          (OUT node_name name, OUT datid oid, OUT datname name, OUT numbackends integer, OUT xact_commit bigint,
           OUT xact_rollback bigint, OUT blks_read bigint, OUT blks_hit bigint, OUT tup_returned bigint, OUT tup_fetched bigint,
           OUT tup_inserted bigint, OUT tup_updated bigint, OUT tup_deleted bigint, OUT conflicts bigint, OUT temp_files bigint,
           OUT temp_bytes bigint, OUT deadlocks bigint, OUT blk_read_time double precision, OUT blk_write_time double precision,
           OUT stats_reset timestamp with time zone)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_database%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_database''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                datid := row_data.datid;
                datname := row_data.datname;
                numbackends := row_data.numbackends;
                xact_commit := row_data.xact_commit;
                xact_rollback := row_data.xact_rollback;
                blks_read := row_data.blks_read;
                blks_hit := row_data.blks_hit;
                tup_returned := row_data.tup_returned;
                tup_fetched := row_data.tup_fetched;
                tup_inserted := row_data.tup_inserted;
                tup_updated := row_data.tup_updated;
                tup_deleted := row_data.tup_deleted;
                conflicts := row_data.conflicts;
                temp_files := row_data.temp_files;
                temp_bytes := row_data.temp_bytes;
                deadlocks := row_data.deadlocks;
                blk_read_time := row_data.blk_read_time;
                blk_write_time := row_data.blk_write_time;
                stats_reset := row_data.stats_reset;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_database_conflicts
          (OUT node_name name, OUT datid oid, OUT datname name, OUT confl_tablespace bigint,
           OUT confl_lock bigint, OUT confl_snapshot bigint, OUT confl_bufferpin bigint, OUT confl_deadlock bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_database_conflicts%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_database_conflicts''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                datid := row_data.datid;
                datname := row_data.datname;
                confl_tablespace := row_data.confl_tablespace;
                confl_lock := row_data.confl_lock;
                confl_snapshot := row_data.confl_snapshot;
                confl_bufferpin := row_data.confl_bufferpin;
                confl_deadlock := row_data.confl_deadlock;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_xact_sys_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
           OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_xact_sys_tables%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_xact_sys_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_xact_sys_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint,
           OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.seq_scan AS seq_scan,
                T.seq_tup_read AS seq_tup_read,
                T.idx_scan AS idx_scan,
                T.idx_tup_fetch AS idx_tup_fetch,
                T.n_tup_ins AS n_tup_ins,
                T.n_tup_upd AS n_tup_upd,
                T.n_tup_del AS n_tup_del,
                T.n_tup_hot_upd AS n_tup_hot_upd
              FROM DBE_PERF.stat_xact_sys_tables T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_xact_user_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
           OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_xact_user_tables%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''D'')';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_xact_user_tables''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                relid := row_data.relid;
                schemaname := row_data.schemaname;
                relname := row_data.relname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_xact_user_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint,
           OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''D'')';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
              SELECT
                T.relname AS relname,
                T.schemaname AS schemaname,
                C.relname AS toastrelname,
                N.nspname AS toastrelschemaname,
                T.seq_scan AS seq_scan,
                T.seq_tup_read AS seq_tup_read,
                T.idx_scan AS idx_scan,
                T.idx_tup_fetch AS idx_tup_fetch,
                T.n_tup_ins AS n_tup_ins,
                T.n_tup_upd AS n_tup_upd,
                T.n_tup_del AS n_tup_del,
                T.n_tup_hot_upd AS n_tup_hot_upd
              FROM DBE_PERF.stat_xact_user_tables T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
              FOR row_data IN EXECUTE(query_str) LOOP
                schemaname := row_data.schemaname;
                IF row_data.toastrelname IS NULL THEN
                    relname := row_data.relname;
                ELSE
                    relname := NULL;
                END IF;
                toastrelschemaname := row_data.toastrelschemaname;
                toastrelname := row_data.toastrelname;
                seq_scan := row_data.seq_scan;
                seq_tup_read := row_data.seq_tup_read;
                idx_scan := row_data.idx_scan;
                idx_tup_fetch := row_data.idx_tup_fetch;
                n_tup_ins := row_data.n_tup_ins;
                n_tup_upd := row_data.n_tup_upd;
                n_tup_del := row_data.n_tup_del;
                n_tup_hot_upd := row_data.n_tup_hot_upd;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_user_functions
          (OUT node_name name, OUT funcid oid, OUT schemaname name, OUT funcname name, OUT calls bigint,
           OUT total_time double precision, OUT self_time double precision)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_user_functions%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_user_functions''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                funcid := row_data.funcid;
                schemaname := row_data.schemaname;
                funcname := row_data.funcname;
                calls := row_data.calls;
                total_time := row_data.total_time;
                self_time := row_data.self_time;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_xact_user_functions
          (OUT node_name name, OUT funcid oid, OUT schemaname name, OUT funcname name, OUT calls bigint,
           OUT total_time double precision, OUT self_time double precision)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_xact_user_functions%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_xact_user_functions''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                funcid := row_data.funcid;
                schemaname := row_data.schemaname;
                funcname := row_data.funcname;
                calls := row_data.calls;
                total_time := row_data.total_time;
                self_time := row_data.self_time;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_bad_block
          (OUT node_name TEXT, OUT databaseid INT4, OUT tablespaceid INT4, OUT relfilenode INT4, OUT forknum INT4,
           OUT error_count INT4, OUT first_time timestamp with time zone, OUT last_time timestamp with time zone)
        RETURNS setof record
        AS $$
        DECLARE
          row_name record;
          each_node_out record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_bad_block''';
              FOR each_node_out IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                databaseid := each_node_out.databaseid;
                tablespaceid := each_node_out.tablespaceid;
                relfilenode := each_node_out.relfilenode;
                forknum := each_node_out.forknum;
                error_count := each_node_out.error_count;
                first_time := each_node_out.first_time;
                last_time := each_node_out.last_time;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_file_redo_iostat
          (OUT node_name name, OUT phywrts bigint, OUT phyblkwrt bigint, OUT writetim bigint,
           OUT avgiotim bigint, OUT lstiotim bigint, OUT miniotim bigint, OUT maxiowtm bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.file_redo_iostat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.file_redo_iostat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                phywrts := row_data.phywrts;
                phyblkwrt := row_data.phyblkwrt;
                writetim := row_data.writetim;
                avgiotim := row_data.avgiotim;
                lstiotim := row_data.lstiotim;
                miniotim := row_data.miniotim;
                maxiowtm := row_data.maxiowtm;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_rel_iostat
          (OUT node_name name, OUT phyrds bigint,
          OUT phywrts bigint, OUT phyblkrd bigint, OUT phyblkwrt bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.local_rel_iostat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.local_rel_iostat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                phyrds := row_data.phyrds;
                phywrts := row_data.phywrts;
                phyblkrd := row_data.phyblkrd;
                phyblkwrt := row_data.phyblkwrt;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_file_iostat
          (OUT node_name name, OUT filenum oid, OUT dbid oid, OUT spcid oid, OUT phyrds bigint,
           OUT phywrts bigint, OUT phyblkrd bigint, OUT phyblkwrt bigint, OUT readtim bigint,
           OUT writetim bigint, OUT avgiotim bigint, OUT lstiotim bigint, OUT miniotim bigint, OUT maxiowtm bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.file_iostat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.file_iostat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                filenum := row_data.filenum;
                dbid := row_data.dbid;
                spcid := row_data.spcid;
                phyrds := row_data.phyrds;
                phywrts := row_data.phywrts;
                phyblkrd := row_data.phyblkrd;
                phyblkwrt := row_data.phyblkwrt;
                readtim := row_data.readtim;
                writetim := row_data.writetim;
                avgiotim := row_data.avgiotim;
                lstiotim := row_data.lstiotim;
                miniotim := row_data.miniotim;
                maxiowtm := row_data.maxiowtm;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_locks
          (OUT node_name name,
           OUT locktype text,
           OUT database oid,
           OUT relation oid,
           OUT page integer,
           OUT tuple smallint,
           OUT virtualxid text,
           OUT transactionid xid,
           OUT classid oid,
           OUT objid oid,
           OUT objsubid smallint,
           OUT virtualtransaction text,
           OUT pid bigint,
           OUT mode text,
           OUT granted boolean,
           OUT fastpath boolean)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.locks%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.locks''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                locktype := row_data.locktype;
                database := row_data.database;
                relation := row_data.relation;
                page := row_data.page;
                tuple := row_data.tuple;
                virtualxid := row_data.virtualxid;
                transactionid := row_data.classid;
                objid := row_data.objid;
                objsubid := row_data.objsubid;
                virtualtransaction := row_data.virtualtransaction;
                pid := row_data.pid;
                mode := row_data.mode;
                granted := row_data.granted;
                fastpath := row_data.fastpath;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_replication_slots
          (OUT node_name name,
           OUT slot_name text,
           OUT plugin text,
           OUT slot_type text,
           OUT datoid oid,
           OUT database name,
           OUT active boolean,
           OUT x_min xid,
           OUT catalog_xmin xid,
           OUT restart_lsn text,
           OUT dummy_standby boolean)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.replication_slots%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.replication_slots''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                slot_name := row_data.slot_name;
                plugin := row_data.plugin;
                slot_type := row_data.slot_type;
                datoid := row_data.datoid;
                database := row_data.database;
                active := row_data.active;
                x_min := row_data.xmin;
                catalog_xmin := row_data.catalog_xmin;
                restart_lsn := row_data.restart_lsn;
                dummy_standby := row_data.dummy_standby;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_bgwriter_stat
          (OUT node_name name,
           OUT checkpoints_timed bigint,
           OUT checkpoints_req bigint,
           OUT checkpoint_write_time double precision,
           OUT checkpoint_sync_time double precision,
           OUT buffers_checkpoint bigint,
           OUT buffers_clean bigint,
           OUT maxwritten_clean bigint,
           OUT buffers_backend bigint,
           OUT buffers_backend_fsync bigint,
           OUT buffers_alloc bigint,
           OUT stats_reset timestamp with time zone)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.bgwriter_stat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.bgwriter_stat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                checkpoints_timed := row_data.checkpoints_timed;
                checkpoints_req := row_data.checkpoints_req;
                checkpoint_write_time := row_data.checkpoint_write_time;
                checkpoint_sync_time := row_data.checkpoint_sync_time;
                buffers_checkpoint := row_data.buffers_checkpoint;
                buffers_clean := row_data.buffers_clean;
                maxwritten_clean := row_data.maxwritten_clean;
                buffers_backend := row_data.buffers_backend;
                buffers_backend_fsync := row_data.buffers_backend_fsync;
                buffers_alloc := row_data.buffers_alloc;
                stats_reset := row_data.stats_reset;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_replication_stat
          (OUT node_name name,
           OUT pid bigint,
           OUT usesysid oid,
           OUT usename name,
           OUT application_name text,
           OUT client_addr inet,
           OUT client_hostname text,
           OUT client_port integer,
           OUT backend_start timestamp with time zone,
           OUT state text,
           OUT sender_sent_location text,
           OUT receiver_write_location text,
           OUT receiver_flush_location text,
           OUT receiver_replay_location text,
           OUT sync_priority integer,
           OUT sync_state text)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.replication_stat%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.replication_stat''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                pid := row_data.pid;
                usesysid := row_data.usesysid;
                usename := row_data.usename;
                client_addr := row_data.client_addr;
                client_hostname := row_data.client_hostname;
                client_port := row_data.client_port;
                state := row_data.state;
                sender_sent_location := row_data.sender_sent_location;
                receiver_write_location := row_data.receiver_write_location;
                receiver_flush_location := row_data.receiver_flush_location;
                receiver_replay_location := row_data.receiver_replay_location;
                sync_priority := row_data.sync_priority;
                sync_state := row_data.sync_state;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_transactions_running_xacts()
        RETURNS setof DBE_PERF.transactions_running_xacts
        AS $$
        DECLARE
          row_data DBE_PERF.transactions_running_xacts%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn dn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.transactions_running_xacts''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_transactions_running_xacts()
        RETURNS setof DBE_PERF.transactions_running_xacts
        AS $$
        DECLARE
          row_data DBE_PERF.transactions_running_xacts%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.transactions_running_xacts''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_transactions_prepared_xacts()
        RETURNS setof DBE_PERF.transactions_prepared_xacts
        AS $$
        DECLARE
          row_data DBE_PERF.transactions_prepared_xacts%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn dn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.transactions_prepared_xacts''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_transactions_prepared_xacts()
        RETURNS setof DBE_PERF.transactions_prepared_xacts
        AS $$
        DECLARE
          row_data DBE_PERF.transactions_prepared_xacts%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type=''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM DBE_PERF.transactions_prepared_xacts''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statement()
        RETURNS setof DBE_PERF.statement
        AS $$
        DECLARE
          row_data DBE_PERF.statement%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement''';
                FOR row_data IN EXECUTE(query_str) LOOP
                  return next row_data;
               END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statement_count()
        RETURNS setof DBE_PERF.statement_count
        AS $$
        DECLARE
          row_data DBE_PERF.statement_count%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement_count''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_config_settings
          (out node_name text,
           out name text,
           out setting text,
           out unit text,
           out category text,
           out short_desc text,
           out extra_desc text,
           out context text,
           out vartype text,
           out source text,
           out min_val text,
           out max_val text,
           out enumvals text[],
           out boot_val text,
           out reset_val text,
           out sourcefile text,
           out sourceline integer)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.config_settings%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.config_settings''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                name := row_data.name;
                setting := row_data.setting;
                unit := row_data.unit;
                category := row_data.category;
                short_desc := row_data.short_desc;
                extra_desc := row_data.extra_desc;
                context := row_data.context;
                vartype := row_data.vartype;
                source := row_data.source;
                min_val := row_data.min_val;
                max_val := row_data.max_val;
                enumvals := row_data.enumvals;
                boot_val := row_data.boot_val;
                reset_val := row_data.reset_val;
                sourcefile := row_data.sourcefile;
                sourceline := row_data.sourceline;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_wait_events()
        RETURNS setof DBE_PERF.wait_events
        AS $$
        DECLARE
          row_data DBE_PERF.wait_events%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.wait_events''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_user_login()
        RETURNS SETOF DBE_PERF.user_login
        AS $$
        DECLARE
          ROW_DATA DBE_PERF.user_login%ROWTYPE;
          ROW_NAME RECORD;
          QUERY_STR TEXT;
          QUERY_STR_NODES TEXT;
          BEGIN
            QUERY_STR_NODES := 'SELECT NODE_NAME FROM PGXC_NODE WHERE NODE_TYPE=''C'' AND nodeis_active = true';
            FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
              QUERY_STR := 'EXECUTE DIRECT ON (' || ROW_NAME.NODE_NAME || ') ''SELECT * FROM DBE_PERF.user_login''';
              FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
                RETURN NEXT ROW_DATA;
              END LOOP;
            END LOOP;
            RETURN;
          END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_record_reset_time(OUT node_name text, OUT reset_time timestamp with time zone)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select * from get_node_stat_reset_time()''';
              FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                reset_time := row_data.get_node_stat_reset_time;
                return next;
              END LOOP;
            END LOOP;
            RETURN;
          END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.global_threadpool_status()
        RETURNS SETOF DBE_PERF.local_threadpool_status
        AS $$
        DECLARE
          ROW_DATA DBE_PERF.local_threadpool_status%ROWTYPE;
          ROW_NAME RECORD;
          QUERY_STR TEXT;
          QUERY_STR_NODES TEXT;
        BEGIN
          QUERY_STR_NODES := 'SELECT NODE_NAME FROM PGXC_NODE WHERE NODE_TYPE IN (''C'', ''D'') AND nodeis_active=true';
          FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
            QUERY_STR := 'EXECUTE DIRECT ON (' || ROW_NAME.NODE_NAME || ') ''SELECT * FROM DBE_PERF.local_threadpool_status''';
            FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
              RETURN NEXT ROW_DATA;
            END LOOP;
          END LOOP;
          RETURN;
        END; $$
        LANGUAGE 'plpgsql';

        DROP FUNCTION IF EXISTS DBE_PERF.get_global_active_session() CASCADE;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_xact_all_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
           OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.stat_xact_all_tables%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_xact_all_tables''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    node_name := row_name.node_name;
                    relid := row_data.relid;
                    schemaname := row_data.schemaname;
                    relname := row_data.relname;
                    seq_scan := row_data.seq_scan;
                    seq_tup_read := row_data.seq_tup_read;
                    idx_scan := row_data.idx_scan;
                    idx_tup_fetch := row_data.idx_tup_fetch;
                    n_tup_ins := row_data.n_tup_ins;
                    n_tup_upd := row_data.n_tup_upd;
                    n_tup_del := row_data.n_tup_del;
                    n_tup_hot_upd := row_data.n_tup_hot_upd;
                    return next;
                END LOOP;
            END LOOP;
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type = ''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.stat_xact_all_tables where schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    node_name := row_name.node_name;
                    relid := row_data.relid;
                    schemaname := row_data.schemaname;
                    relname := row_data.relname;
                    seq_scan := row_data.seq_scan;
                    seq_tup_read := row_data.seq_tup_read;
                    idx_scan := row_data.idx_scan;
                    idx_tup_fetch := row_data.idx_tup_fetch;
                    n_tup_ins := row_data.n_tup_ins;
                    n_tup_upd := row_data.n_tup_upd;
                    n_tup_del := row_data.n_tup_del;
                    n_tup_hot_upd := row_data.n_tup_hot_upd;
                    return next;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_xact_all_tables
          (OUT schemaname name, OUT relname name,
           OUT toastrelschemaname name, OUT toastrelname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint,
           OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
           OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_data record;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all the node names
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                SELECT
                    T.schemaname AS schemaname,
                    T.relname AS relname,
                    N.nspname AS toastrelschemaname,
                    C.relname AS toastrelname,
                    T.seq_scan AS seq_scan,
                    T.seq_tup_read AS seq_tup_read,
                    T.idx_scan AS idx_scan,
                    T.idx_tup_fetch AS idx_tup_fetch,
                    T.n_tup_ins AS n_tup_ins,
                    T.n_tup_upd AS n_tup_upd,
                    T.n_tup_del AS n_tup_del,
                    T.n_tup_hot_upd AS n_tup_hot_upd
                FROM DBE_PERF.stat_xact_all_tables T
                    LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                    LEFT JOIN pg_namespace N ON C.relnamespace = N.oid''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    schemaname := row_data.schemaname;
                    IF row_data.toastrelname IS NULL THEN
                        relname := row_data.relname;
                    ELSE
                        relname := NULL;
                    END IF;
                    toastrelschemaname := row_data.toastrelschemaname;
                    toastrelname := row_data.toastrelname;
                    seq_scan := row_data.seq_scan;
                    seq_tup_read := row_data.seq_tup_read;
                    idx_scan := row_data.idx_scan;
                    idx_tup_fetch := row_data.idx_tup_fetch;
                    n_tup_ins := row_data.n_tup_ins;
                    n_tup_upd := row_data.n_tup_upd;
                    n_tup_del := row_data.n_tup_del;
                    n_tup_hot_upd := row_data.n_tup_hot_upd;
                    return next;
                END LOOP;
            END LOOP;
            query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type = ''C'' AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                SELECT
                    T.relname AS relname,
                    T.schemaname AS schemaname,
                    C.relname AS toastrelname,
                    N.nspname AS toastrelschemaname,
                    T.seq_scan AS seq_scan,
                    T.seq_tup_read AS seq_tup_read,
                    T.idx_scan AS idx_scan,
                    T.idx_tup_fetch AS idx_tup_fetch,
                    T.n_tup_ins AS n_tup_ins,
                    T.n_tup_upd AS n_tup_upd,
                    T.n_tup_del AS n_tup_del,
                    T.n_tup_hot_upd AS n_tup_hot_upd
                FROM DBE_PERF.stat_xact_all_tables T
                LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                LEFT JOIN pg_namespace N ON C.relnamespace = N.oid
                WHERE T.schemaname = ''''pg_catalog'''' OR T.schemaname =''''pg_toast'''' ''';
                FOR row_data IN EXECUTE(query_str) LOOP
                    schemaname := row_data.schemaname;
                    IF row_data.toastrelname IS NULL THEN
                        relname := row_data.relname;
                    ELSE
                        relname := NULL;
                    END IF;
                    toastrelschemaname := row_data.toastrelschemaname;
                    toastrelname := row_data.toastrelname;
                    seq_scan := row_data.seq_scan;
                    seq_tup_read := row_data.seq_tup_read;
                    idx_scan := row_data.idx_scan;
                    idx_tup_fetch := row_data.idx_tup_fetch;
                    n_tup_ins := row_data.n_tup_ins;
                    n_tup_upd := row_data.n_tup_upd;
                    n_tup_del := row_data.n_tup_del;
                    n_tup_hot_upd := row_data.n_tup_hot_upd;
                    return next;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_stat_activity
          (out coorname text, out datid oid, out datname text, out pid bigint,
           out usesysid oid, out usename text, out application_name text, out client_addr inet,
           out client_hostname text, out client_port integer, out backend_start timestamptz,
           out xact_start timestamptz, out query_start timestamptz, out state_change timestamptz,
           out waiting boolean, out enqueue text, out state text, out resource_pool name,
           out query_id bigint, out query text)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_stat_activity%rowtype;
          coor_name record;
          fet_active text;
          fetch_coor text;
          BEGIN
            --Get all cn node names
            fetch_coor := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR coor_name IN EXECUTE(fetch_coor) LOOP
              coorname :=  coor_name.node_name;
              fet_active := 'EXECUTE DIRECT ON (' || coor_name.node_name || ') ''SELECT * FROM DBE_PERF.session_stat_activity''';
              FOR row_data IN EXECUTE(fet_active) LOOP
                coorname := coorname;
                datid :=row_data.datid;
                datname := row_data.datname;
                pid := row_data.pid;
                usesysid :=row_data.usesysid;
                usename := row_data.usename;
                application_name := row_data.application_name;
                client_addr := row_data.client_addr;
                client_hostname :=row_data.client_hostname;
                client_port :=row_data.client_port;
                backend_start := row_data.backend_start;
                xact_start := row_data.xact_start;
                query_start := row_data.query_start;
                state_change := row_data.state_change;
                waiting := row_data.waiting;
                enqueue := row_data.enqueue;
                state := row_data.state;
                resource_pool :=row_data.resource_pool;
                query_id :=row_data.query_id;
                query := row_data.query;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_cn_stat_all_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint,
           OUT n_tup_ins bigint, OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                        SELECT * FROM DBE_PERF.stat_all_tables WHERE schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        node_name := row_name.node_name;
                        relid := row_data.relid;
                        schemaname := row_data.schemaname;
                        relname := row_data.relname;
                        seq_scan := row_data.seq_scan;
                        seq_tup_read := row_data.seq_tup_read;
                        idx_scan := row_data.idx_scan;
                        idx_tup_fetch := row_data.idx_tup_fetch;
                        n_tup_ins := row_data.n_tup_ins;
                        n_tup_upd := row_data.n_tup_upd;
                        n_tup_del := row_data.n_tup_del;
                        n_tup_hot_upd := row_data.n_tup_hot_upd;
                        n_live_tup := row_data.n_live_tup;
                        n_dead_tup := row_data.n_dead_tup;
                        last_vacuum := row_data.last_vacuum;
                        last_autovacuum := row_data.last_autovacuum;
                        last_analyze := row_data.last_analyze;
                        last_autoanalyze := row_data.last_autoanalyze;
                        vacuum_count := row_data.vacuum_count;
                        autovacuum_count := row_data.autovacuum_count;
                        analyze_count := row_data.analyze_count;
                        autoanalyze_count := row_data.autoanalyze_count;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_stat_db_cu
          (OUT node_name1 text, OUT db_name text,
           OUT mem_hit bigint, OUT hdd_sync_read bigint,
           OUT hdd_asyn_read bigint)
        RETURNS setof record
        AS $$
        DECLARE
          row_name record;
          each_node_out record;
          query_str text;
          query_str_nodes text;
          BEGIN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT  D.datname AS datname,
              pg_stat_get_db_cu_mem_hit(D.oid) AS mem_hit,
              pg_stat_get_db_cu_hdd_sync(D.oid) AS hdd_sync_read,
              pg_stat_get_db_cu_hdd_asyn(D.oid) AS hdd_asyn_read
              FROM pg_database D;''';
                FOR each_node_out IN EXECUTE(query_str) LOOP
                  node_name1 := row_name.node_name;
                  db_name := each_node_out.datname;
                  mem_hit := each_node_out.mem_hit;
                  hdd_sync_read := each_node_out.hdd_sync_read;
                  hdd_asyn_read := each_node_out.hdd_asyn_read;
                  return next;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_cn_stat_all_tables
          (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name,
           OUT seq_scan bigint, OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint,
           OUT n_tup_ins bigint, OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint, OUT n_live_tup bigint,
           OUT n_dead_tup bigint, OUT last_vacuum timestamp with time zone, OUT last_autovacuum timestamp with time zone,
           OUT last_analyze timestamp with time zone, OUT last_autoanalyze timestamp with time zone, OUT vacuum_count bigint,
           OUT autovacuum_count bigint, OUT analyze_count bigint, OUT autoanalyze_count bigint)
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''
                        SELECT * FROM DBE_PERF.stat_all_tables WHERE schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        node_name := row_name.node_name;
                        relid := row_data.relid;
                        schemaname := row_data.schemaname;
                        relname := row_data.relname;
                        seq_scan := row_data.seq_scan;
                        seq_tup_read := row_data.seq_tup_read;
                        idx_scan := row_data.idx_scan;
                        idx_tup_fetch := row_data.idx_tup_fetch;
                        n_tup_ins := row_data.n_tup_ins;
                        n_tup_upd := row_data.n_tup_upd;
                        n_tup_del := row_data.n_tup_del;
                        n_tup_hot_upd := row_data.n_tup_hot_upd;
                        n_live_tup := row_data.n_live_tup;
                        n_dead_tup := row_data.n_dead_tup;
                        last_vacuum := row_data.last_vacuum;
                        last_autovacuum := row_data.last_autovacuum;
                        last_analyze := row_data.last_analyze;
                        last_autoanalyze := row_data.last_autoanalyze;
                        vacuum_count := row_data.vacuum_count;
                        autovacuum_count := row_data.autovacuum_count;
                        analyze_count := row_data.analyze_count;
                        autoanalyze_count := row_data.autoanalyze_count;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_session_stat_activity
          (out coorname text, out datid oid, out datname text, out pid bigint,
           out usesysid oid, out usename text, out application_name text, out client_addr inet,
           out client_hostname text, out client_port integer, out backend_start timestamptz,
           out xact_start timestamptz, out query_start timestamptz, out state_change timestamptz,
           out waiting boolean, out enqueue text, out state text, out resource_pool name,
           out query_id bigint, out query text)
        RETURNS setof record
        AS $$
        DECLARE
          row_data DBE_PERF.session_stat_activity%rowtype;
          coor_name record;
          fet_active text;
          fetch_coor text;
          BEGIN
            --Get all cn node names
            fetch_coor := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
            FOR coor_name IN EXECUTE(fetch_coor) LOOP
              coorname :=  coor_name.node_name;
              fet_active := 'EXECUTE DIRECT ON (' || coor_name.node_name || ') ''SELECT * FROM DBE_PERF.session_stat_activity''';
              FOR row_data IN EXECUTE(fet_active) LOOP
                coorname := coorname;
                datid :=row_data.datid;
                datname := row_data.datname;
                pid := row_data.pid;
                usesysid :=row_data.usesysid;
                usename := row_data.usename;
                application_name := row_data.application_name;
                client_addr := row_data.client_addr;
                client_hostname :=row_data.client_hostname;
                client_port :=row_data.client_port;
                backend_start := row_data.backend_start;
                xact_start := row_data.xact_start;
                query_start := row_data.query_start;
                state_change := row_data.state_change;
                waiting := row_data.waiting;
                enqueue := row_data.enqueue;
                state := row_data.state;
                resource_pool :=row_data.resource_pool;
                query_id :=row_data.query_id;
                query := row_data.query;
                return next;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

    end if;
END $DO$;
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_statistics()
RETURNS setof record
AS $$
DECLARE
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM gs_wlm_session_statistics''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_history()
RETURNS setof record
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_session_history''';
                        FOR row_data IN EXECUTE(query_str) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_info()
RETURNS setof record
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str_nodes text;
        BEGIN
                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_session_info''';
                        FOR row_data IN EXECUTE(query_str) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_info_bytime(text, TIMESTAMP, TIMESTAMP, int) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_info_bytime(text, TIMESTAMP, TIMESTAMP, int)
RETURNS setof gs_wlm_session_info
AS $$
DECLARE
        row_data gs_wlm_session_info%rowtype;
        row_name record;
        query_str text;
        query_str_nodes text;
        query_str_cn text;
        BEGIN
        		IF $1 IN ('start_time', 'finish_time') THEN

	        	ELSE
	        		 raise WARNING 'Illegal character entered for function, colname must be start_time or finish_time';
	        		return;
	        	END IF;

                --Get all the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
                query_str_cn := 'SELECT * FROM gs_wlm_session_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_cn||''';';
                        FOR row_data IN EXECUTE(query_str) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_wlm_rebuild_user_resource_pool()
RETURNS setof gs_wlm_rebuild_user_resource_pool
AS $$
DECLARE
	row_data gs_wlm_rebuild_user_resource_pool%rowtype;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_rebuild_user_resource_pool''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_wlm_get_workload_records()
RETURNS setof record
AS $$
DECLARE
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_workload_records''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_sql_count()
RETURNS setof gs_sql_count
AS $$
DECLARE
    row_data gs_sql_count%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_sql_count''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_os_threads()
RETURNS setof pg_os_threads
AS $$
DECLARE
    row_data pg_os_threads%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_os_threads''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_running_xacts()
RETURNS setof pg_running_xacts
AS $$
DECLARE
    row_data pg_running_xacts%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the coordinator node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM pg_running_xacts''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_variable_info()
RETURNS setof pg_variable_info
AS $$
DECLARE
    row_data pg_variable_info%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM pg_variable_info''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

 CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_get_wal_senders_status(OUT nodename text, OUT source_ip text, OUT source_port integer,OUT dest_ip text, OUT dest_port integer,OUT sender_pid int, OUT local_role text,  OUT peer_role text,  OUT peer_state text,
                                 OUT state text,
                                 OUT sender_sent_location text, OUT sender_write_location text, OUT sender_flush_location text, OUT sender_replay_location text,
                                 OUT receiver_received_location text, OUT receiver_write_location text, OUT receiver_flush_location text, OUT receiver_replay_location text)

 RETURNS setof record
 AS $$
 DECLARE
     row_data record;
     row_name record;
     query_str text;
     query_str_nodes text;
     source_addr text;
     dest_addr text;
     BEGIN
         query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
         FOR row_name IN EXECUTE(query_str_nodes) LOOP
             query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_get_wal_senders()''';
             FOR row_data IN EXECUTE(query_str) LOOP
                 nodename = row_name.node_name;
                 source_addr = substring(row_data.channel from 0 for position('-->' in row_data.channel));
                 dest_addr = substring(row_data.channel from position('-->' in row_data.channel) + 3);
                 source_ip = substring(source_addr from 0 for position(':' in source_addr));
                 source_port = cast(substring(source_addr from position(':' in source_addr) + 1) as integer);
                 dest_ip = substring(dest_addr from 0 for position(':' in dest_addr));
                 dest_port = cast(substring(dest_addr from position(':' in dest_addr) + 1) as integer);
                 sender_pid = row_data.sender_pid;
                 local_role = row_data.local_role;
                 peer_role = row_data.peer_role;
                 peer_state = row_data.peer_state;
                 state = row_data.state;
                 sender_sent_location = row_data.sender_sent_location;
                 sender_write_location = row_data.sender_write_location;
                 sender_flush_location = row_data.sender_flush_location;
                 sender_replay_location = row_data.sender_replay_location;
                 receiver_received_location = row_data.receiver_received_location;
                 receiver_write_location = row_data.receiver_write_location;
                 receiver_flush_location = row_data.receiver_flush_location;
                 receiver_replay_location = row_data.receiver_replay_location;
                 return next;
             END LOOP;
         END LOOP;
         return;
     END; $$
 LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.gs_get_stat_session_cu(OUT node_name1 text, OUT mem_hit int, OUT hdd_sync_read int, OUT hdd_asyn_read int)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    each_node_out record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_session_cu()''';
            FOR each_node_out IN EXECUTE(query_str) LOOP
                node_name1 := row_name.node_name;
                mem_hit := each_node_out.mem_hit;
                hdd_sync_read := each_node_out.hdd_sync_read;
                hdd_asyn_read := each_node_out.hdd_asyn_read;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.gs_stat_reset()
RETURNS void
AS $$
DECLARE
    row_name record;
    each_node_out record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_reset()''';
            EXECUTE(query_str);
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_statistics()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM gs_wlm_operator_statistics''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_history()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_operator_history''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_info()
RETURNS setof gs_wlm_operator_info
AS $$
DECLARE
    row_data gs_wlm_operator_info%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_operator_info''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_statistics()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT * FROM gs_wlm_ec_operator_statistics''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_history()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_ec_operator_history''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_info()
RETURNS setof gs_wlm_ec_operator_info
AS $$
DECLARE
    row_data gs_wlm_ec_operator_info%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM gs_wlm_ec_operator_info''';
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_bad_block(OUT nodename TEXT, OUT databaseid INT4, OUT tablespaceid INT4, OUT relfilenode INT4, OUT forknum INT4, OUT error_count INT4, OUT first_time timestamp with time zone, OUT last_time timestamp with time zone)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    each_node_out record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_bad_block()''';
            FOR each_node_out IN EXECUTE(query_str) LOOP
                nodename := row_name.node_name;
                databaseid := each_node_out.databaseid;
                tablespaceid := each_node_out.tablespaceid;
                relfilenode := each_node_out.relfilenode;
                forknum := each_node_out.forknum;
                error_count := each_node_out.error_count;
                first_time := each_node_out.first_time;
                last_time := each_node_out.last_time;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_bad_block_clear()
RETURNS void
AS $$
DECLARE
    row_name record;
    each_node_out record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_bad_block_clear()''';
            EXECUTE(query_str);
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS  pg_catalog.pgxc_query_audit(IN starttime TIMESTAMPTZ, IN endtime TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT "type" text, OUT "result" text, OUT userid text, OUT username text, OUT database text, OUT client_conninfo text, OUT "object_name" text, OUT detail_info text, OUT node_name text, OUT thread_id text, OUT local_port text, OUT remote_port text)
 CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_query_audit(IN starttime TIMESTAMPTZ, IN endtime TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT "type" text, OUT "result" text, OUT userid text, OUT username text, OUT database text, OUT client_conninfo text, OUT "object_name" text, OUT detail_info text, OUT node_name text, OUT thread_id text, OUT local_port text, OUT remote_port text)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    row_data record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get the node names of all CNs
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_catalog.pg_query_audit(''''' || starttime || ''''',''''' || endtime || ''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                "time"   := row_data."time";
                "type"   := row_data."type";
                "result" := row_data."result";
                userid := row_data.userid;
                username := row_data.username;
                database := row_data.database;
                client_conninfo := row_data.client_conninfo;
                "object_name"   := row_data."object_name";
                detail_info := row_data.detail_info;
                node_name   := row_data.node_name;
                thread_id   := row_data.thread_id;
                local_port  := row_data.local_port;
                remote_port := row_data.remote_port;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.pgxc_stat_all_tables()
 CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_all_tables()
RETURNS setof pg_stat_all_tables
AS $$
DECLARE
	row_data pg_stat_all_tables%rowtype;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D''';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_all_tables''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type = ''C'' AND nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_all_tables where schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW pg_catalog.pgxc_get_stat_all_tables AS
        SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
    FROM pg_class p,
        (SELECT  relname, schemaname, SUM(n_tup_ins) n_tup_ins, SUM(n_tup_upd) n_tup_upd, SUM(n_tup_del) n_tup_del, SUM(n_live_tup) n_live_tup, SUM(n_dead_tup) n_dead_tup, CAST((SUM(n_dead_tup) / SUM(n_dead_tup + n_live_tup + 0.0001) * 100)
         AS NUMERIC(5,2)) dirty_page_rate FROM pgxc_stat_all_tables() GROUP BY (relname,schemaname)) s
        WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) AND p.relname NOT LIKE '%pg_cudesc%' ORDER BY dirty_page_rate DESC;


CREATE OR REPLACE FUNCTION pg_catalog.gs_get_stat_db_cu(OUT node_name1 text, OUT db_name text, OUT mem_hit bigint, OUT hdd_sync_read bigint, OUT hdd_asyn_read bigint)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    each_node_out record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT  D.datname AS datname,
            pg_stat_get_db_cu_mem_hit(D.oid) AS mem_hit,
            pg_stat_get_db_cu_hdd_sync(D.oid) AS hdd_sync_read,
            pg_stat_get_db_cu_hdd_asyn(D.oid) AS hdd_asyn_read
            FROM pg_database D;''';
            FOR each_node_out IN EXECUTE(query_str) LOOP
                node_name1 := row_name.node_name;
                db_name := each_node_out.datname;
                mem_hit := each_node_out.mem_hit;
                hdd_sync_read := each_node_out.hdd_sync_read;
                hdd_asyn_read := each_node_out.hdd_asyn_read;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_delta_info(IN rel TEXT, OUT node_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str_nodes text;
    query_str text;
    rel_info record;
    row_name record;
    row_data record;
    BEGIN
        EXECUTE format('select c.relname,n.nspname from pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) where C.oid = (select %L::regclass::oid)', rel) into rel_info;
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_catalog.pg_get_delta_info('''''||rel_info.relname||''''','''''||rel_info.nspname||''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                live_tuple := row_data.live_tuple;
                data_size := row_data.data_size;
                blockNum := row_data.blockNum;
                part_name := row_data.part_name;
                return next;
            END LOOP;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_activity_with_conninfo
(
out coorname text,
out datid oid,
out datname text,
out pid bigint,
out sessionid bigint,
out usesysid oid,
out usename text,
out application_name text,
out client_addr inet,
out client_hostname text,
out client_port integer,
out backend_start timestamptz,
out xact_start timestamptz,
out query_start timestamptz,
out state_change timestamptz,
out waiting boolean,
out enqueue text,
out state text,
out resource_pool name,
out query_id bigint,
out query text,
out connection_info text
)
RETURNS setof record
AS $$
DECLARE
    row_data pg_stat_activity%rowtype;
    coor_name record;
    fet_active text;
    fetch_coor text;
    BEGIN
        --Get all the node names
        fetch_coor := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR coor_name IN EXECUTE(fetch_coor) LOOP
            coorname :=  coor_name.node_name;
            fet_active := 'EXECUTE DIRECT ON (' || coor_name.node_name || ') ''SELECT * FROM pg_stat_activity ''';
            FOR row_data IN EXECUTE(fet_active) LOOP
        coorname := coorname;
        datid :=row_data.datid;
                datname := row_data.datname;
                pid := row_data.pid;
                sessionid := row_data.sessionid;
        usesysid :=row_data.usesysid;
                usename := row_data.usename;
                application_name := row_data.application_name;
                client_addr := row_data.client_addr;
        client_hostname :=row_data.client_hostname;
        client_port :=row_data.client_port;
                backend_start := row_data.backend_start;
                xact_start := row_data.xact_start;
                query_start := row_data.query_start;
                state_change := row_data.state_change;
                waiting := row_data.waiting;
                enqueue := row_data.enqueue;
                state := row_data.state;
        resource_pool :=row_data.resource_pool;
        query_id :=row_data.query_id;
                query := row_data.query;
        connection_info := row_data.connection_info;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_activity
(
out coorname text,
out datid oid,
out datname text,
out pid bigint,
out sessionid bigint,
out usesysid oid,
out usename text,
out application_name text,
out client_addr inet,
out client_hostname text,
out client_port integer,
out backend_start timestamptz,
out xact_start timestamptz,
out query_start timestamptz,
out state_change timestamptz,
out waiting boolean,
out enqueue text,
out state text,
out resource_pool name,
out query_id bigint,
out query text
)
RETURNS setof record
AS $$
DECLARE
    row_data pg_stat_activity%rowtype;
    coor_name record;
    fet_active text;
    fetch_coor text;
    BEGIN
        --Get all the node names
        fetch_coor := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR coor_name IN EXECUTE(fetch_coor) LOOP
            coorname :=  coor_name.node_name;
            fet_active := 'EXECUTE DIRECT ON (' || coor_name.node_name || ') ''SELECT * FROM pg_stat_activity ''';
            FOR row_data IN EXECUTE(fet_active) LOOP
        coorname := coorname;
        datid :=row_data.datid;
                datname := row_data.datname;
                pid := row_data.pid;
                sessionid := row_data.sessionid;
        usesysid :=row_data.usesysid;
                usename := row_data.usename;
                application_name := row_data.application_name;
                client_addr := row_data.client_addr;
        client_hostname :=row_data.client_hostname;
        client_port :=row_data.client_port;
                backend_start := row_data.backend_start;
                xact_start := row_data.xact_start;
                query_start := row_data.query_start;
                state_change := row_data.state_change;
                waiting := row_data.waiting;
                enqueue := row_data.enqueue;
                state := row_data.state;
        resource_pool :=row_data.resource_pool;
        query_id :=row_data.query_id;
                query := row_data.query;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

create or replace function pg_catalog.pgxc_cgroup_map_ng_conf(IN ngname text)
returns bool
AS $$
declare
    host_info record;
    query_str text;
    query_string text;
    flag boolean;
    special text := '[^A-z0-9_]';
begin
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN ngname, IN special;
    IF flag = true THEN
        return false;
    ELSE
        query_str = 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        for host_info in execute(query_str) loop
                query_string := 'execute direct on (' || host_info.node_name || ') ''SELECT * from pg_catalog.gs_cgroup_map_ng_conf(''''' || ngname || ''''')''';
                execute query_string;
        end loop;
    END IF;
    return true;
end;
$$language plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_xacts_iscommitted(IN xid int8, OUT nodename text, OUT iscommitted bool)
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name|| ') ''SELECT pgxc_is_committed('|| xid ||'::text::xid) as bcommitted''';
            FOR row_data IN EXECUTE(query_str) LOOP
                nodename = row_name.node_name;
                iscommitted = row_data.bcommitted;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_senders_catchup_time(OUT nodename text, OUT lwpid int, OUT local_role text,  OUT peer_role text,
                                                        OUT state text, OUT sender text,
                                                        OUT catchup_start TimestampTz,  OUT catchup_end TimestampTz)

RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_get_senders_catchup_time where state = ''''Catchup'''' ''';
            FOR row_data IN EXECUTE(query_str) LOOP
                nodename = row_name.node_name;
                lwpid = row_data.lwpid;
                local_role = row_data.local_role;
                peer_role = row_data.peer_role;
                state = row_data.state;
                sender = row_data."type";
                catchup_start = row_data.catchup_start;
                catchup_end = row_data.catchup_end;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_parse_clog(OUT xid int8, OUT nodename text, OUT status text)
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_parse_clog() ''';
            FOR row_data IN EXECUTE(query_str) LOOP
                xid = row_data.xid;
                nodename = row_name.node_name;
                status = row_data.status;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_get_wal_senders(OUT nodename text, OUT sender_pid int, OUT local_role text,  OUT peer_role text,  OUT peer_state text,
                                OUT state text,
                                OUT sender_sent_location text, OUT sender_write_location text, OUT sender_flush_location text, OUT sender_replay_location text,
                                OUT receiver_received_location text, OUT receiver_write_location text, OUT receiver_flush_location text, OUT receiver_replay_location text)

RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_stat_get_wal_senders() where sender_flush_location != receiver_replay_location  ''';
            FOR row_data IN EXECUTE(query_str) LOOP
                nodename = row_name.node_name;
                sender_pid = row_data.sender_pid;
                local_role = row_data.local_role;
                peer_role = row_data.peer_role;
                peer_state = row_data.peer_state;
                state = row_data.state;
                sender_sent_location = row_data.sender_sent_location;
                sender_write_location = row_data.sender_write_location;
                sender_flush_location = row_data.sender_flush_location;
                sender_replay_location = row_data.sender_replay_location;
                receiver_received_location = row_data.receiver_received_location;
                receiver_write_location = row_data.receiver_write_location;
                receiver_flush_location = row_data.receiver_flush_location;
                receiver_replay_location = row_data.receiver_replay_location;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.reload_active_coordinator()
RETURNS boolean
AS $$
DECLARE
	row_name record;
	query_str text;
	query_str_nodes text;
	query_result  boolean = false;
	return_result  bool = true;
	BEGIN
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pgxc_pool_reload()''';
			begin
				EXECUTE(query_str) into query_result;
				if query_result = 'f' then
					return_result = false;
					return return_result;
				end if;
			end;
		END LOOP;
		return return_result;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

GRANT SELECT ON pg_catalog.pgxc_get_stat_all_tables TO public;

SET search_path TO information_schema;
DROP VIEW IF EXISTS information_schema.column_privileges CASCADE;
CREATE OR REPLACE VIEW column_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(x.relname AS sql_identifier) AS table_name,
           CAST(x.attname AS sql_identifier) AS column_name,
           CAST(x.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(x.grantee, x.relowner, 'USAGE')
                  OR x.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

    FROM (
           SELECT pr_c.grantor,
                  pr_c.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_c.prtype,
                  pr_c.grantable,
                  pr_c.relowner
           FROM (SELECT oid, relname, relnamespace, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).*
                 FROM pg_class
                 WHERE relkind IN ('r', 'v', 'f')
                ) pr_c (oid, relname, relnamespace, relowner, grantor, grantee, prtype, grantable),
                pg_attribute a
           WHERE a.attrelid = pr_c.oid
                 AND a.attnum > 0
                 AND NOT a.attisdropped
           UNION
           SELECT pr_a.grantor,
                  pr_a.grantee,
                  attname,
                  relname,
                  relnamespace,
                  pr_a.prtype,
                  pr_a.grantable,
                  c.relowner
           FROM (SELECT attrelid, attname, (aclexplode(coalesce(attacl, acldefault('c', relowner)))).*
                 FROM pg_attribute a JOIN pg_class cc ON (a.attrelid = cc.oid)
                 WHERE attnum > 0
                       AND NOT attisdropped
                ) pr_a (attrelid, attname, grantor, grantee, prtype, grantable),
                pg_class c
           WHERE pr_a.attrelid = c.oid
                 AND relkind IN ('r', 'v', 'f')
         ) x,
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE x.relnamespace = nc.oid
          AND x.grantee = grantee.oid
          AND x.grantor = u_grantor.oid
          AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE OR REPLACE VIEW role_column_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           column_name,
           privilege_type,
           is_grantable
    FROM column_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

DROP VIEW IF EXISTS information_schema.routine_privileges CASCADE;
CREATE OR REPLACE VIEW routine_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS specific_catalog,
           CAST(n.nspname AS sql_identifier) AS specific_schema,
           CAST(p.proname || '_' || CAST(p.oid AS text) AS sql_identifier) AS specific_name,
           CAST(current_database() AS sql_identifier) AS routine_catalog,
           CAST(n.nspname AS sql_identifier) AS routine_schema,
           CAST(p.proname AS sql_identifier) AS routine_name,
           CAST('EXECUTE' AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(grantee.oid, p.proowner, 'USAGE')
                  OR p.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

    FROM (
            SELECT oid, proname, proowner, pronamespace, (aclexplode(coalesce(proacl, acldefault('f', proowner)))).* FROM pg_proc
         ) p (oid, proname, proowner, pronamespace, grantor, grantee, prtype, grantable),
         pg_namespace n,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE p.pronamespace = n.oid
          AND grantee.oid = p.grantee
          AND u_grantor.oid = p.grantor
          AND p.prtype IN ('EXECUTE')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE OR REPLACE VIEW role_routine_grants AS
    SELECT grantor,
           grantee,
           specific_catalog,
           specific_schema,
           specific_name,
           routine_catalog,
           routine_schema,
           routine_name,
           privilege_type,
           is_grantable
    FROM routine_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

DROP VIEW IF EXISTS information_schema.udt_privileges CASCADE;
CREATE OR REPLACE VIEW udt_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(n.nspname AS sql_identifier) AS udt_schema,
           CAST(t.typname AS sql_identifier) AS udt_name,
           CAST('TYPE USAGE' AS character_data) AS privilege_type, -- sic
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(grantee.oid, t.typowner, 'USAGE')
                  OR t.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

    FROM (
            SELECT oid, typname, typnamespace, typtype, typowner, (aclexplode(coalesce(typacl, acldefault('T', typowner)))).* FROM pg_type
         ) AS t (oid, typname, typnamespace, typtype, typowner, grantor, grantee, prtype, grantable),
         pg_namespace n,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE t.typnamespace = n.oid
          AND t.typtype = 'c'
          AND t.grantee = grantee.oid
          AND t.grantor = u_grantor.oid
          AND t.prtype IN ('USAGE')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE OR REPLACE VIEW role_udt_grants AS
    SELECT grantor,
           grantee,
           udt_catalog,
           udt_schema,
           udt_name,
           privilege_type,
           is_grantable
    FROM udt_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

DROP VIEW IF EXISTS information_schema.table_privileges CASCADE;
CREATE OR REPLACE VIEW table_privileges AS
    SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
           CAST(grantee.rolname AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(c.prtype AS character_data) AS privilege_type,
           CAST(
             CASE WHEN
                  -- object owner always has grant options
                  pg_has_role(grantee.oid, c.relowner, 'USAGE')
                  OR c.grantable
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable,
           CAST(CASE WHEN c.prtype = 'SELECT' THEN 'YES' ELSE 'NO' END AS yes_or_no) AS with_hierarchy

    FROM (
            SELECT oid, relname, relnamespace, relkind, relowner, (aclexplode(coalesce(relacl, acldefault('r', relowner)))).* FROM pg_class
         ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
         pg_namespace nc,
         pg_authid u_grantor,
         (
           SELECT oid, rolname FROM pg_authid
           UNION ALL
           SELECT 0::oid, 'PUBLIC'
         ) AS grantee (oid, rolname)

    WHERE c.relnamespace = nc.oid
          AND c.relkind IN ('r', 'v')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE OR REPLACE VIEW role_table_grants AS
    SELECT grantor,
           grantee,
           table_catalog,
           table_schema,
           table_name,
           privilege_type,
           is_grantable,
           with_hierarchy
    FROM table_privileges
    WHERE grantor IN (SELECT role_name FROM enabled_roles)
          OR grantee IN (SELECT role_name FROM enabled_roles);

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.column_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.role_column_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.routine_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.role_routine_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.udt_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.role_udt_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.table_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE information_schema.role_table_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON information_schema.column_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_column_grants TO PUBLIC;
GRANT SELECT ON information_schema.routine_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_routine_grants TO PUBLIC;
GRANT SELECT ON information_schema.udt_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_udt_grants TO PUBLIC;
GRANT SELECT ON information_schema.table_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_table_grants TO PUBLIC;

RESET search_path;

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_get_all_user_resource_info() cascade;
CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_all_user_resource_info(OUT userid Oid, OUT used_memory int, OUT total_memory int, OUT used_cpu float, OUT total_cpu int, OUT used_space bigint, OUT total_space bigint, OUT used_temp_space bigint, OUT total_temp_space bigint, OUT used_spill_space bigint, OUT total_spill_space bigint, OUT read_kbytes bigint, OUT write_kbytes bigint, OUT read_counts bigint, OUT write_counts bigint, OUT read_speed float, OUT write_speed float)
RETURNS setof record
AS $$
DECLARE
	row_data record;
	row_name record;
	query_str text;
	query_str2 text;
	BEGIN
		query_str := 'SELECT rolname FROM pg_authid';
		FOR row_name IN EXECUTE(query_str) LOOP
			query_str2 := 'SELECT * FROM pg_catalog.gs_wlm_user_resource_info(''' || row_name.rolname || ''')';
			FOR row_data IN EXECUTE(query_str2) LOOP
                                userid := row_data.userid;
                                used_memory := row_data.used_memory;
                                total_memory := row_data.total_memory;
                                used_cpu := row_data.used_cpu;
                                total_cpu := row_data.total_cpu;
                                used_space := row_data.used_space;
                                total_space := row_data.total_space;
                                used_temp_space := row_data.used_temp_space;
                                total_temp_space := row_data.total_temp_space;
                                used_spill_space := row_data.used_spill_space;
                                total_spill_space := row_data.total_spill_space;
                                read_kbytes := row_data.read_kbytes;
                                write_kbytes := row_data.write_kbytes;
                                read_counts := row_data.read_counts;
                                write_counts := row_data.write_counts;
                                read_speed := row_data.read_speed;
                                write_speed := row_data.write_speed;
				return next;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_total_user_resource_info_oid AS
    SELECT * FROM pg_catalog.gs_wlm_get_all_user_resource_info();

CREATE OR REPLACE VIEW pg_catalog.pg_total_user_resource_info AS
SELECT
    S.usename AS username,
    T.used_memory,
    T.total_memory,
    T.used_cpu,
    T.total_cpu,
    T.used_space,
    T.total_space,
    T.used_temp_space,
    T.total_temp_space,
    T.used_spill_space,
    T.total_spill_space,
    T.read_kbytes,
    T.write_kbytes,
    T.read_counts,
    T.write_counts,
    T.read_speed,
    T.write_speed
FROM pg_user AS S, pg_catalog.pg_total_user_resource_info_oid AS T
WHERE S.usesysid = T.userid;

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_session_respool() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5021;
CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_session_respool(input bigint, OUT datid oid, OUT threadid bigint, OUT sessionid bigint, OUT threadpid integer, OUT usesysid oid, OUT cgroup text, OUT session_respool name) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_session_respool';

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP VIEW IF EXISTS DBE_PERF.summary_statement cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_statement(
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
        OUT hash_spill_size bigint
    ) cascade;
    DROP VIEW IF EXISTS DBE_PERF.STATEMENT cascade;
  end if;
END$DO$;
DROP FUNCTION IF EXISTS pg_catalog.get_instr_unique_sql() cascade;

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
    OUT last_updated timestamp with time zone
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_unique_sql';

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE VIEW DBE_PERF.statement AS
      SELECT * FROM get_instr_unique_sql();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statement()
    RETURNS setof DBE_PERF.statement
    AS $$
    DECLARE
      row_data DBE_PERF.statement%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
      BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
          query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement''';
            FOR row_data IN EXECUTE(query_str) LOOP
              return next row_data;
           END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_statement AS
      SELECT * FROM DBE_PERF.get_summary_statement();

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.STATEMENT TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.summary_statement TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;

  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_summary_statement' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_summary_statement
    DROP COLUMN IF EXISTS snap_sort_count,
    DROP COLUMN IF EXISTS snap_sort_time,
    DROP COLUMN IF EXISTS snap_sort_mem_used,
    DROP COLUMN IF EXISTS snap_sort_spill_count,
    DROP COLUMN IF EXISTS snap_sort_spill_size,
    DROP COLUMN IF EXISTS snap_hash_count,
    DROP COLUMN IF EXISTS snap_hash_time,
    DROP COLUMN IF EXISTS snap_hash_mem_used,
    DROP COLUMN IF EXISTS snap_hash_spill_count,
    DROP COLUMN IF EXISTS snap_hash_spill_size;
  end if;
END$DO$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_full_sql_by_timestamp() cascade;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_slow_sql_by_timestamp() cascade;
        DROP VIEW IF EXISTS DBE_PERF.statement_history cascade;
    end if;
END$$;
DROP INDEX IF EXISTS pg_catalog.statement_history_time_idx;
DROP TABLE IF EXISTS pg_catalog.statement_history;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP VIEW IF EXISTS DBE_PERF.summary_statement cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_statement(
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
        OUT last_updated timestamp with time zone
    ) cascade;
    DROP VIEW IF EXISTS DBE_PERF.STATEMENT cascade;
  end if;
END$DO$;
DROP FUNCTION IF EXISTS pg_catalog.get_instr_unique_sql() cascade;

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
    OUT net_stream_recv_info text
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_unique_sql';

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE VIEW DBE_PERF.statement AS
      SELECT * FROM get_instr_unique_sql();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statement()
    RETURNS setof DBE_PERF.statement
    AS $$
    DECLARE
      row_data DBE_PERF.statement%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
      BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
          query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement''';
            FOR row_data IN EXECUTE(query_str) LOOP
              return next row_data;
           END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_statement AS
      SELECT * FROM DBE_PERF.get_summary_statement();

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.STATEMENT TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.summary_statement TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;

  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_summary_statement' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_summary_statement
    DROP COLUMN IF EXISTS snap_last_updated;
  end if;
END$DO$;

-- wait events
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP VIEW IF EXISTS DBE_PERF.global_wait_events cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_wait_events() cascade;
    DROP VIEW IF EXISTS DBE_PERF.wait_events cascade;
  end if;
END$DO$;
DROP FUNCTION IF EXISTS pg_catalog.get_instr_wait_event() cascade;
DROP FUNCTION IF EXISTS pg_catalog.get_instr_wait_event(IN param int4, OUT nodename text, OUT type text, OUT event text, OUT wait bigint, OUT failed_wait bigint, OUT total_wait_time bigint, OUT avg_wait_time bigint, OUT max_wait_time bigint, OUT min_wait_time bigint, OUT last_updated timestamp with time zone) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5705;
CREATE FUNCTION pg_catalog.get_instr_wait_event
(
    IN param int4,
    OUT nodename text,
    OUT type text,
    OUT event text,
    OUT wait bigint,
    OUT failed_wait bigint,
    OUT total_wait_time bigint,
    OUT avg_wait_time bigint,
    OUT max_wait_time bigint,
    OUT min_wait_time bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_wait_event';

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE VIEW DBE_PERF.wait_events AS
      SELECT * FROM get_instr_wait_event(NULL);

    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_wait_events()
    RETURNS setof DBE_PERF.wait_events
    AS $$
    DECLARE
      row_data DBE_PERF.wait_events%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
      BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
          query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.wait_events''';
          FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
          END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.global_wait_events AS
      SELECT * FROM DBE_PERF.get_global_wait_events();

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.wait_events TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_wait_events TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.wait_events TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_wait_events TO PUBLIC;

  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.statement_detail_decode() CASCADE;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_wait_events' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_global_wait_events
    DROP COLUMN IF EXISTS snap_last_updated;
  end if;
END$DO$;

DROP VIEW IF EXISTS global_stat_hotkeys_info cascade;

DROP FUNCTION IF EXISTS pg_catalog.global_stat_clean_hotkeys();
DROP FUNCTION IF EXISTS pg_catalog.global_stat_get_hotkeys_info();

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_clean_hotkeys();
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_get_hotkeys_info();

DROP FUNCTION IF EXISTS pg_catalog.gs_get_global_barrier_status() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_local_barrier_status() cascade;

DROP FUNCTION IF EXISTS pg_catalog.hypopg_create_index(text);
DROP FUNCTION IF EXISTS pg_catalog.hypopg_display_index();
DROP FUNCTION IF EXISTS pg_catalog.hypopg_drop_index(int4);
DROP FUNCTION IF EXISTS pg_catalog.hypopg_estimate_size(int4);
DROP FUNCTION IF EXISTS pg_catalog.hypopg_reset_index();

DROP FUNCTION IF EXISTS pg_catalog.plancache_status() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.prepare_statement_status() CASCADE;

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3957;
CREATE OR REPLACE FUNCTION pg_catalog.plancache_status(
    OUT nodename text,
    OUT query text,
    OUT refcount int4,
    OUT valid bool,
    OUT DatabaseID oid,
    OUT schema_name text,
    OUT params_num int4)
RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_status';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3959;
CREATE OR REPLACE FUNCTION pg_catalog.prepare_statement_status(
    OUT nodename text,
    OUT cn_sess_id int8,
    OUT cn_node_id int4,
    OUT cn_time_line int4,
    OUT statement_name text,
    OUT refcount int4,
    OUT is_shared bool)
RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_prepare_status';

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS DBE_PERF.global_plancache_status() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.local_plancache_status CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_plancache_status CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.local_prepare_statement_status CASCADE;
    DROP FUNCTION IF EXISTS  DBE_PERF.global_prepare_statement_status() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_prepare_statement_status CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.global_plancache_clean() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.local_plancache_clean CASCADE;

    CREATE VIEW DBE_PERF.local_plancache_status AS
      SELECT * FROM pg_catalog.plancache_status();

    end if;
END $DO$;


DROP AGGREGATE IF EXISTS streaming_int2_int4_sum_gather(int8);
DROP AGGREGATE IF EXISTS streaming_int8_sum_gather(numeric);
DROP AGGREGATE IF EXISTS streaming_interval_avg_gather(interval[]);
DROP AGGREGATE IF EXISTS streaming_float8_avg_gather(float8[]);
DROP AGGREGATE IF EXISTS streaming_numeric_avg_gather(numeric[]);
DROP AGGREGATE IF EXISTS streaming_int8_avg_gather(int8[]);
DROP AGGREGATE IF EXISTS gather(anyelement);
DROP FUNCTION IF EXISTS pg_catalog.gather_sfunc_dummy(anyelement, anyelement);


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;
DROP FUNCTION IF EXISTS pg_catalog.check_cont_query_schema_changed(cq_id oid);
DROP FUNCTION IF EXISTS pg_catalog.reset_cont_query_stats();
DROP FUNCTION IF EXISTS pg_catalog.reset_cont_query_stats(stream_id oid);
DROP FUNCTION IF EXISTS pg_catalog.reset_local_cont_query_stats(cq_id oid);
DROP FUNCTION IF EXISTS pg_catalog.reset_local_cont_query_stat(cq_id oid);
DROP VIEW IF EXISTS pg_catalog.stream_stats;
DROP VIEW IF EXISTS pg_catalog.cont_query_stats;
DROP FUNCTION IF EXISTS pg_catalog.get_cont_query_stats();
DROP FUNCTION IF EXISTS pg_catalog.get_local_cont_query_stats();
DROP FUNCTION IF EXISTS pg_catalog.get_local_cont_query_stat(cq_id oid);

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.streaming_cont_query_matrelid_index_local_set_unique(flag bool)
        RETURNS BOOL
        AS '$libdir/streaming', 'streaming_cont_query_matrelid_index_local_set_unique'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;
        CREATE OR REPLACE FUNCTION pg_catalog.streaming_cont_query_matrelid_index_set_unique(flag bool)
        RETURNS BOOL
        AS '$libdir/streaming', 'streaming_cont_query_matrelid_index_set_unique'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;
    end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        SELECT pg_catalog.streaming_cont_query_matrelid_index_local_set_unique(true) INTO ans;
        SELECT pg_catalog.streaming_cont_query_matrelid_index_set_unique(true) INTO ans;
    end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.streaming_cont_query_matrelid_index_set_unique(flag bool);
        DROP FUNCTION IF EXISTS pg_catalog.streaming_cont_query_matrelid_index_local_set_unique(flag bool);
    end if;
END$$;


DROP FUNCTION IF EXISTS pg_catalog.gs_set_obs_delete_location(text) cascade;


do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.global_single_flush_dw_status CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.local_single_flush_dw_stat();
        DROP FUNCTION IF EXISTS pg_catalog.remote_single_flush_dw_stat();
    end if;
END$$;


DROP FUNCTION IF EXISTS pg_catalog.gs_index_advise(cstring);


DROP AGGREGATE IF EXISTS pg_catalog.mode(order by anyelement);
DROP FUNCTION IF EXISTS pg_catalog.mode_final(internal) CASCADE;

drop function if exists pg_catalog.series(name) cascade;
drop function if exists pg_catalog.top_key(name, int8) cascade;
drop function if exists pg_catalog.top_key(name, int) cascade;
drop function if exists pg_catalog.series(anyelement) cascade;
drop function if exists pg_catalog.top_key(anyelement, int8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6006;
create or replace function pg_catalog.series(name) returns text LANGUAGE INTERNAL as 'series_internal';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6008;
create or replace function pg_catalog.top_key(name, int8) returns text LANGUAGE INTERNAL as 'top_key_internal';

DROP FUNCTION IF EXISTS pg_catalog.pg_create_physical_replication_slot_extern(name, boolean, text) cascade;

DO $$
BEGIN
    IF EXISTS(SELECT c.relname,n.nspname FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid AND c.relname='snapshot' AND n.nspname='snapshot')
    THEN
        create table IF NOT EXISTS snapshot.snap_global_operator_runtime(snapshot_id bigint, snap_queryid bigint, snap_pid bigint, snap_plan_node_id integer, snap_plan_node_name text, snap_start_time timestamp with time zone, snap_duration bigint, snap_status text, snap_query_dop integer, snap_estimated_rows bigint, snap_tuple_processed bigint, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory integer, snap_memory_skew_percent  integer, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size integer, snap_spill_skew_percent integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_warning text);

        create table IF NOT EXISTS snapshot.snap_global_operator_history(snapshot_id bigint, snap_queryid bigint, snap_pid bigint, snap_plan_node_id integer, snap_plan_node_name text, snap_start_time timestamp with time zone, snap_duration bigint, snap_query_dop integer, snap_estimated_rows bigint, snap_tuple_processed bigint, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory integer, snap_memory_skew_percent  integer, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size integer, snap_spill_skew_percent integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_warning text);

        create table IF NOT EXISTS snapshot.snap_global_operator_history_table(snapshot_id bigint, snap_queryid bigint, snap_pid bigint, snap_plan_node_id integer, snap_plan_node_name text, snap_start_time timestamp with time zone, snap_duration bigint, snap_query_dop integer, snap_estimated_rows bigint, snap_tuple_processed bigint, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory integer, snap_memory_skew_percent  integer, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size integer, snap_spill_skew_percent integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_warning text);

        create table IF NOT EXISTS snapshot.snap_global_operator_ec_runtime(snapshot_id bigint, snap_queryid bigint, snap_plan_node_id integer, snap_start_time timestamp with time zone, snap_ec_status text, snap_ec_execute_datanode text, snap_ec_dsn text, snap_ec_username text, snap_ec_query text, snap_ec_libodbc_type text, snap_ec_fetch_count bigint);

        create table IF NOT EXISTS snapshot.snap_global_operator_ec_history(snapshot_id bigint, snap_queryid bigint, snap_plan_node_id integer, snap_start_time timestamp with time zone, snap_duration bigint,snap_tuple_processed bigint, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory integer,snap_ec_status text, snap_ec_execute_datanode text, snap_ec_dsn text, snap_ec_username text, snap_ec_query text,snap_ec_libodbc_type text);

        create table IF NOT EXISTS snapshot.snap_global_operator_ec_history_table(snapshot_id bigint, snap_queryid bigint, snap_plan_node_id integer, snap_start_time timestamp with time zone, snap_duration bigint,snap_tuple_processed bigint, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory integer,snap_ec_status text, snap_ec_execute_datanode text, snap_ec_dsn text, snap_ec_username text, snap_ec_query text,snap_ec_libodbc_type text);

        create table IF NOT EXISTS snapshot.snap_global_statement_complex_runtime(snapshot_id bigint, snap_datid oid, snap_dbname name, snap_schemaname text, snap_nodename text, snap_username name, snap_application_name text, snap_client_addr inet, snap_client_hostname text, snap_client_port integer, snap_query_band text, snap_pid bigint, snap_block_time bigint, snap_start_time timestamp with time zone, snap_duration bigint, snap_estimate_total_time  bigint, snap_estimate_left_time   bigint, snap_enqueue text, snap_resource_pool name, snap_control_group text, snap_estimate_memory integer, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory  integer, snap_memory_skew_percent  integer, snap_spill_info text, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size   integer, snap_spill_skew_percent   integer, snap_min_dn_time bigint , snap_max_dn_time bigint, snap_average_dn_time bigint, snap_dntime_skew_percent  integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_min_peak_iops integer, snap_max_peak_iops integer, snap_average_peak_iops integer, snap_iops_skew_percent integer, snap_warning text , snap_queryid bigint, snap_query text, snap_query_plan text, snap_node_group text, snap_top_cpu_dn text, snap_top_mem_dn text);

        create table IF NOT EXISTS snapshot.snap_global_statement_complex_history(snapshot_id bigint, snap_datid oid, snap_dbname text, snap_schemaname text, snap_nodename text, snap_username text, snap_application_name text, snap_client_addr inet, snap_client_hostname text, snap_client_port integer, snap_query_band text, snap_block_time bigint, snap_start_time timestamp with time zone, snap_finish_time timestamp with time zone, snap_duration bigint, snap_estimate_total_time  bigint, snap_status text, snap_abort_info text, snap_resource_pool name, snap_control_group text, snap_estimate_memory integer, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory  integer, snap_memory_skew_percent  integer, snap_spill_info text, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size   integer, snap_spill_skew_percent   integer, snap_min_dn_time bigint , snap_max_dn_time bigint, snap_average_dn_time bigint, snap_dntime_skew_percent  integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_min_peak_iops integer, snap_max_peak_iops integer, snap_average_peak_iops integer, snap_iops_skew_percent integer, snap_warning text , snap_queryid bigint, snap_query text, snap_query_plan text, snap_node_group text,snap_cpu_top1_node_name text, snap_cpu_top2_node_name text, snap_cpu_top3_node_name text, snap_cpu_top4_node_name text, snap_cpu_top5_node_name text, snap_mem_top1_node_name text, snap_mem_top2_node_name text, snap_mem_top3_node_name text, snap_mem_top4_node_name text, snap_mem_top5_node_name text, snap_cpu_top1_value bigint, snap_cpu_top2_value bigint, snap_cpu_top3_value bigint, snap_cpu_top4_value bigint, snap_cpu_top5_value bigint, snap_mem_top1_value bigint, snap_mem_top2_value bigint, snap_mem_top3_value bigint, snap_mem_top4_value bigint, snap_mem_top5_value bigint, snap_top_mem_dn text, snap_top_cpu_dn text);

        create table IF NOT EXISTS snapshot.snap_global_statement_complex_history_table(snapshot_id bigint, snap_datid oid, snap_dbname text, snap_schemaname text, snap_nodename text, snap_username text, snap_application_name text, snap_client_addr inet, snap_client_hostname text, snap_client_port integer, snap_query_band text, snap_block_time bigint, snap_start_time timestamp with time zone, snap_finish_time timestamp with time zone, snap_duration bigint, snap_estimate_total_time  bigint, snap_status text, snap_abort_info text, snap_resource_pool name, snap_control_group text, snap_estimate_memory integer, snap_min_peak_memory integer, snap_max_peak_memory integer, snap_average_peak_memory  integer, snap_memory_skew_percent  integer, snap_spill_info text, snap_min_spill_size integer, snap_max_spill_size integer, snap_average_spill_size   integer, snap_spill_skew_percent   integer, snap_min_dn_time bigint , snap_max_dn_time bigint, snap_average_dn_time bigint, snap_dntime_skew_percent  integer, snap_min_cpu_time bigint, snap_max_cpu_time bigint, snap_total_cpu_time bigint, snap_cpu_skew_percent integer, snap_min_peak_iops integer, snap_max_peak_iops integer, snap_average_peak_iops integer, snap_iops_skew_percent integer, snap_warning text , snap_queryid bigint, snap_query text, snap_query_plan text, snap_node_group text,snap_cpu_top1_node_name text, snap_cpu_top2_node_name text, snap_cpu_top3_node_name text, snap_cpu_top4_node_name text, snap_cpu_top5_node_name text, snap_mem_top1_node_name text, snap_mem_top2_node_name text, snap_mem_top3_node_name text, snap_mem_top4_node_name text, snap_mem_top5_node_name text, snap_cpu_top1_value bigint, snap_cpu_top2_value bigint, snap_cpu_top3_value bigint, snap_cpu_top4_value bigint, snap_cpu_top5_value bigint, snap_mem_top1_value bigint, snap_mem_top2_value bigint, snap_mem_top3_value bigint, snap_mem_top4_value bigint, snap_mem_top5_value bigint, snap_top_mem_dn text, snap_top_cpu_dn text);

    END IF;
END $$;

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE VIEW DBE_PERF.class_vital_info AS
      SELECT
        C.oid AS relid,
        N.nspname AS schemaname,
        C.relname AS relname,
        C.relkind AS relkind
      FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i');
  end if;
END $$;


CREATE OR REPLACE VIEW pg_catalog.pg_indexes AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        I.relname AS indexname,
        T.spcname AS tablespace,
        pg_get_indexdef(I.oid) AS indexdef
    FROM pg_index X JOIN pg_class C ON (C.oid = X.indrelid)
         JOIN pg_class I ON (I.oid = X.indexrelid)
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = I.reltablespace)
    WHERE C.relkind = 'r' AND I.relkind = 'i';

DROP FUNCTION IF EXISTS pg_catalog.copy_error_log_create() CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.copy_error_log_create()
RETURNS bool
AS $$
DECLARE
    query_str_create_table text;
    query_str_create_index text;
    query_str_do_revoke text;
    BEGIN
        query_str_create_table := 'CREATE TABLE public.pgxc_copy_error_log
                            (relname varchar, begintime timestamptz, filename varchar, rownum int8, rawrecord text, detail text)
                            DISTRIBUTE BY hash(begintime)';
        EXECUTE query_str_create_table;

        query_str_create_index := 'CREATE INDEX copy_error_log_relname_idx ON public.pgxc_copy_error_log(relname)';
        EXECUTE query_str_create_index;

        query_str_do_revoke := 'REVOKE ALL on public.pgxc_copy_error_log FROM public';
        EXECUTE query_str_do_revoke;

        return true;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

REVOKE ALL on FUNCTION pg_catalog.copy_error_log_create() FROM public;

DROP VIEW IF EXISTS pg_catalog.pg_gtt_relstats cascade;
DROP VIEW IF EXISTS pg_catalog.pg_gtt_attached_pids cascade;
DROP VIEW IF EXISTS pg_catalog.pg_gtt_stats cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_get_gtt_relstats() cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_get_gtt_statistics() cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_gtt_attached_pid() cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_list_gtt_relfrozenxids() cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_partition_filepath() cascade;

drop aggregate if exists pg_catalog.median(float8);
drop aggregate if exists pg_catalog.median(interval);
DROP FUNCTION if exists pg_catalog.median_float8_finalfn(internal) cascade;
DROP FUNCTION if exists pg_catalog.median_interval_finalfn(internal) cascade;
DROP FUNCTION if exists pg_catalog.median_transfn(internal, "any") cascade;

--clob cast text
DROP CAST IF EXISTS (TEXT AS CLOB) CASCADE;
DROP CAST IF EXISTS (CLOB AS TEXT) CASCADE;

 /* text to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (TEXT) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(TEXT)
RETURNS TEXT
AS $$ select $1 $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* char to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (CHAR) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(CHAR)
RETURNS TEXT
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* varchar to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (VARCHAR) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(VARCHAR)
RETURNS TEXT
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* nvarchar2 to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (NVARCHAR2) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(NVARCHAR2)
RETURNS TEXT
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* clob to regclass */
DROP CAST IF EXISTS (clob AS regclass) CASCADE;
/* clob to bpchar */
DROP CAST IF EXISTS (clob AS bpchar) CASCADE;
/* clob to varchar */
DROP CAST IF EXISTS (clob AS varchar) CASCADE;
/* clob to nvarchar2 */
DROP CAST IF EXISTS (clob AS nvarchar2) CASCADE;
/* bpchar to clob */
DROP CAST IF EXISTS (bpchar AS clob) CASCADE;
/* varchar to clob */
DROP CAST IF EXISTS (varchar AS clob) CASCADE;
/* nvarchar2 to clob */
DROP CAST IF EXISTS (nvarchar2 AS clob) CASCADE;
/* char to clob */
DROP CAST IF EXISTS (char AS clob) CASCADE;
/* name to clob */
DROP CAST IF EXISTS (name AS clob) CASCADE;
/* clob to char */
DROP CAST IF EXISTS (clob AS char) CASCADE;
/* clob to name */
DROP CAST IF EXISTS (clob AS name) CASCADE;
/* pg_node_tree to clob */
DROP CAST IF EXISTS (pg_node_tree AS clob) CASCADE;
/* clob to timestamp */
DROP CAST IF EXISTS (clob AS timestamp) CASCADE;
/* cidr to clob*/
DROP CAST IF EXISTS (cidr AS clob) CASCADE;
/* inet to clob */
DROP CAST IF EXISTS (inet AS clob) CASCADE;
/* bool to clob */
DROP CAST IF EXISTS (bool AS clob) CASCADE;
/* xml to clob */
DROP CAST IF EXISTS (xml AS clob) CASCADE;
/* clob to xml */
DROP CAST IF EXISTS (clob AS xml) CASCADE;
/* date to clob */
DROP CAST IF EXISTS (date AS clob) CASCADE;
/* clob to date */
DROP CAST IF EXISTS (clob AS date) CASCADE;
/* int1 to clob */
DROP CAST IF EXISTS (int1 AS clob) CASCADE;
/* int2 to clob */
DROP CAST IF EXISTS (int2 AS clob) CASCADE;
/* int4 to clob */
DROP CAST IF EXISTS (int4 AS clob) CASCADE;
/* int8 to clob */
DROP CAST IF EXISTS (int8 AS clob) CASCADE;
/* float4 to clob */
DROP CAST IF EXISTS (float4 AS clob) CASCADE;
/* float8 to clob */
DROP CAST IF EXISTS (float8 AS clob) CASCADE;
/* numeric to clob */
DROP CAST IF EXISTS (numeric AS clob) CASCADE;
/* timestamptz to clob */
DROP CAST IF EXISTS (timestamptz AS clob) CASCADE;
/* timestamp to clob */
DROP CAST IF EXISTS (timestamp AS clob) CASCADE;
/* clob to int1 */
DROP CAST IF EXISTS (clob AS int1) CASCADE;
/* clob to int2*/
DROP CAST IF EXISTS (clob AS int2) CASCADE;
/* clob to int4*/
DROP CAST IF EXISTS (clob AS int4) CASCADE;
/* clob to int8*/
DROP CAST IF EXISTS (clob AS int8) CASCADE;
/* clob to float4*/
DROP CAST IF EXISTS (clob AS float4) CASCADE;
/* clob to float8*/
DROP CAST IF EXISTS (clob AS float8) CASCADE;
/* clob to numeric*/
DROP CAST IF EXISTS (clob AS numeric) CASCADE;

DO $DO$
DECLARE
    ans boolean;
    user_name text;
    global_query_str text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
      DROP VIEW IF EXISTS DBE_PERF.local_active_session CASCADE;
    end if;
END $DO$;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_current_instance_info(text, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
    row_data gs_wlm_instance_history%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    query_str_temp text;
    node_name_new text;
    node_count int;
    BEGIN

        IF $2 is null THEN
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info()';
        ELSE
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info() limit '||$2;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='||quote_literal($1);
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name_new in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                select count(*) from pgxc_node where node_name=quote_literal(node_name_new) into node_count;
                IF node_count <> 0 THEN
                    query_str := 'EXECUTE DIRECT ON ('||node_name_new||') ''' || query_str_temp ||''';';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
                    END LOOP;
                END IF;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_history_instance_info(text, TIMESTAMP, TIMESTAMP, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
       row_data gs_wlm_instance_history%rowtype;
       row_name record;
       query_str text;
       query_str_nodes text;
    query_str_temp text;
    node_name_new text;
    node_count int;
    BEGIN

        IF $4 is null THEN
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'||quote_literal($2)||' and timestamp <'||quote_literal($3);
        ELSE
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'||quote_literal($2)||' and timestamp <'||quote_literal($3)||' limit '||quote_literal($4);
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='||quote_literal($1);
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ' || quote_literal(query_str_temp) ||';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ' || quote_literal(query_str_temp) ||';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name_new in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                select count(*) from pgxc_node where node_name=quote_literal(node_name_new) into node_count;
                IF node_count <> 0 THEN
                    query_str := 'EXECUTE DIRECT ON ('||node_name_new||') ' || quote_literal(query_str_temp) ||';';
                    FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
                    END LOOP;
                END IF;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_current_instance_info(text, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
    row_data gs_wlm_instance_history%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    query_str_temp text;
    node_name text;
    BEGIN

        IF $2 is null THEN
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info()';
        ELSE
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info() limit '||$2;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='||quote_literal($1);
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table(quote_literal($1),',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_history_instance_info(text, TIMESTAMP, TIMESTAMP, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
       row_data gs_wlm_instance_history%rowtype;
       row_name record;
       query_str text;
       query_str_nodes text;
    query_str_temp text;
    node_name text;
    BEGIN

        IF $4 is null THEN
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'||quote_literal($2)||' and timestamp <'||quote_literal($3);
        ELSE
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'||quote_literal($2)||' and timestamp <'||quote_literal($3)||' limit '||quote_literal($4);
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='||quote_literal($1);
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ' || quote_literal(query_str_temp) ||';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ' || quote_literal(query_str_temp) ||';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table(quote_literal($1),',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ' || quote_literal(query_str_temp) ||';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP VIEW IF EXISTS pg_catalog.pg_total_user_resource_info CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_get_all_user_resource_info(OUT userid Oid, OUT used_memory int, OUT total_memory int, OUT used_cpu float, OUT total_cpu int, OUT used_space bigint, OUT total_space bigint, OUT used_temp_space bigint, OUT total_temp_space bigint, OUT used_spill_space bigint, OUT total_spill_space bigint, OUT read_kbytes bigint, OUT write_kbytes bigint, OUT read_counts bigint, OUT write_counts bigint, OUT read_speed float, OUT write_speed float) cascade;
CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_all_user_resource_info()
RETURNS setof record
AS $$
DECLARE
	row_data record;
	row_name record;
	query_str text;
	query_str2 text;
	BEGIN
		query_str := 'SELECT rolname FROM pg_authid';
		FOR row_name IN EXECUTE(query_str) LOOP
			query_str2 := 'SELECT * FROM pg_catalog.gs_wlm_user_resource_info(''' || row_name.rolname || ''')';
			FOR row_data IN EXECUTE(query_str2) LOOP
				return next row_data;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_total_user_resource_info_oid AS
SELECT * FROM pg_catalog.gs_wlm_get_all_user_resource_info() AS
(userid Oid,
 used_memory int,
 total_memory int,
 used_cpu float,
 total_cpu int,
 used_space bigint,
 total_space bigint,
 used_temp_space bigint,
 total_temp_space bigint,
 used_spill_space bigint,
 total_spill_space bigint,
 read_kbytes bigint,
 write_kbytes bigint,
 read_counts bigint,
 write_counts bigint,
 read_speed float,
 write_speed float
);

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_session_respool() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5021;
CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_session_respool(input bigint, OUT datid oid, OUT threadid bigint, OUT sessionid bigint, OUT threadpid integer, OUT usesysid oid, OUT cgroup text, OUT session_respool name) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_session_respool';


DROP FUNCTION IF EXISTS pg_catalog.pg_cbm_rotate_file(in rotate_lsn text) CASCADE;


DROP FUNCTION IF EXISTS pg_catalog.distributed_count(text,text);
DROP FUNCTION IF EXISTS pg_catalog.table_skewness_with_schema(text,text);

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_current_instance_info(text, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
    row_data gs_wlm_instance_history%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    query_str_temp text;
    node_name text;
    flag boolean;
    special text := '[ ;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT pg_catalog.regexp_like(:1,:2);' INTO flag USING IN $1, IN special;
        IF flag = true THEN
            RAISE WARNING 'Illegal character entered for function';
            return;
        END IF;

        IF $2 is null THEN
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info()';
        ELSE
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info() limit '||$2;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='''||$1||'''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_history_instance_info(text, TIMESTAMP, TIMESTAMP, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
	row_data gs_wlm_instance_history%rowtype;
	row_name record;
	query_str text;
	query_str_nodes text;
    query_str_temp text;
    node_name text;
    flag boolean;
    special text := '[ ;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT pg_catalog.regexp_like(:1,:2);' INTO flag USING IN $1, IN special;
        IF flag = true THEN
            RAISE WARNING 'Illegal character entered for function';
            return;
        END IF;

        IF $4 is null THEN
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'''''||$2||''''' and timestamp <'''''||$3||''''' ';
        ELSE
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'''''||$2||''''' and timestamp <'''''||$3||''''' limit '||$4;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='''||$1||'''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    total_num bigint;
    flag boolean;
    special text := '[;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN $1, IN special;
        IF flag = true THEN
            raise WARNING 'illegal character entered for function';
            RETURN;
        END IF;
        EXECUTE 'SELECT count(1) FROM ' || $1
            INTO total_num;

		--Get the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node, pgxc_class where node_type IN (''C'', ''D'') AND is_oid_in_group_members(oid, nodeoids) AND
			pcrelid=''' || $1 || '''::regclass::oid';

        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''DN_name'''' as dnname1, count(1) as count1 from ' || $1 || '''';

            FOR row_data IN EXECUTE(query_str) LOOP
                row_data.dnname1 := CASE
                    WHEN pg_catalog.LENGTH(row_name.node_name)<20
                    THEN row_name.node_name || right('                    ',20-pg_catalog.length(row_name.node_name))
                    ELSE pg_catalog.SUBSTR(row_name.node_name,1,20)
                    END;
                DNName := row_data.dnname1;
                Num := row_data.count1;
                IF total_num = 0 THEN
                Ratio := 0.000 ||'%';
                ELSE
                Ratio := ROUND(row_data.count1/total_num*100,3) || '%';
                END IF;
                RETURN next;
            END LOOP;
        END LOOP;

        RETURN;
    END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.table_skewness(text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
declare
flag boolean;
special text := '[;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN $1, IN special;
        IF flag = true THEN
            raise WARNING 'illegal character entered for function';
            return NEXT;
        ELSE
            RETURN QUERY EXECUTE 'SELECT * FROM pg_catalog.distributed_count(''' || $1 || ''') ORDER BY num DESC, dnname';
        END IF;
    END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.gs_get_table_distribution( IN table_name TEXT, IN schema_name TEXT)
RETURNS TEXT
AS
$$
DECLARE
nodename  text;
query_str text;
row_data record;
row_data1 record;
nodelist  text;
flag boolean;
special text := '[ ;|-]';
BEGIN
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN table_name, IN special;
    IF flag = true THEN
        raise WARNING 'Illegal character entered for function';
        return nodelist;
    END IF;

    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN schema_name, IN special;
    IF flag = true THEN
        raise WARNING 'Illegal character entered for function';
        return nodelist;
    END IF;

    query_str := 'SELECT oid,node_name FROM pgxc_node WHERE node_type=''D'' order by node_name';

    FOR row_data IN EXECUTE(query_str) LOOP
        query_str := 'EXECUTE DIRECT ON (' || row_data.node_name || ') ''SELECT count(*) AS num  FROM pg_class c LEFT JOIN pg_namespace n  ON c.relnamespace = n.oid WHERE relname='''''||table_name||''''' AND n.nspname='''''||schema_name||''''' ''';
        FOR row_data1 IN EXECUTE(query_str) LOOP
            IF row_data1.num = 1 THEN
                nodelist := nodelist || ' '|| row_data.oid;
            END IF;
        END LOOP;
   END LOOP;

   RETURN nodelist;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_catalog.get_shard_oids_byname(IN node_name varchar)
RETURNS Oid[] AS $$
declare
    node_oid Oid;
    nodes Oid[];
    nodenames_query text;
    total_num bigint;
    flag boolean;
    special text := '[;|-]';
BEGIN
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN $1, IN special;

    IF flag = true THEN
        raise WARNING 'illegal character entered for function';
        return '{0}';
    ELSE
        nodenames_query := 'SELECT oid FROM pgxc_node WHERE node_name = $1 ORDER BY oid';
        FOR node_oid IN EXECUTE nodenames_query USING node_name LOOP
            nodes := array_append(nodes, node_oid);
        END LOOP;
        RETURN nodes;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
CREATE FUNCTION pg_catalog.pg_stat_get_activity(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT connection_info TEXT) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity_with_conninfo';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_session_wlmstat(INT4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3502;
CREATE FUNCTION pg_catalog.pg_stat_get_session_wlmstat(INT4, OUT datid OID, OUT threadid INT8, OUT sessionid INT8, OUT threadpid INT4, OUT usesysid OID, OUT appname TEXT, OUT query TEXT,
OUT priority INT8, OUT block_time INT8, OUT elapsed_time INT8, OUT total_cpu_time INT8, OUT skew_percent INT4, OUT statement_mem INT4, OUT active_points INT4, OUT dop_value INT4, OUT current_cgroup TEXT,
OUT current_status TEXT, OUT enqueue_state TEXT, OUT attribute TEXT, OUT is_plana BOOL, OUT node_group TEXT) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_session_wlmstat';

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
        WHERE S.datid = D.oid AND
              S.usesysid = U.oid AND
              T.sessionid = S.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.sessionid = N.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_session_wlmstat AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.sessionid,
            S.threadpid AS processid,
            S.usesysid,
            S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
            S.skew_percent AS cpu_skew_percent,
            S.statement_mem,
            S.active_points,
            S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, gs_wlm_session_respool(0) AS T
        WHERE S.datid = D.oid AND
              S.usesysid = U.oid AND
              T.sessionid = S.sessionid;


DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE VIEW dbe_perf.session_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
          T.threadid = S.pid;


    CREATE OR REPLACE VIEW DBE_PERF.replication_stat AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
          S.pid = W.pid;

    CREATE OR REPLACE VIEW DBE_PERF.statement_wlmstat_complex_runtime AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.threadpid AS processid,
            S.usesysid,
            S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
            S.skew_percent AS cpu_skew_percent,
            S.statement_mem,
            S.active_points,
            S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
          T.threadid = S.threadid;
    end if;
END $DO$;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_replication AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_catalog.pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_catalog.pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
            S.pid = W.pid;

CREATE OR REPLACE VIEW pg_catalog.DV_SESSIONS AS
    SELECT
            sa.sessionid AS SID,
            0::integer AS SERIAL#,
            sa.usesysid AS USER#,
            ad.rolname AS USERNAME
    FROM pg_catalog.pg_stat_get_activity(NULL) AS sa
        LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
    WHERE sa.application_name <> 'JobScheduler';

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_workload_records AS
    SELECT
            P.node_name,
            S.threadid AS thread_id,
            S.threadpid AS processid,
            P.start_time AS time_stamp,
            U.rolname AS username,
            P.memory,
            P.actpts AS active_points,
            P.maxpts AS max_points,
            P.priority,
            P.resource_pool,
            S.current_status AS status,
            S.current_cgroup AS control_group,
            P.queue_type AS enqueue,
            S.query,
            P.node_group
    FROM pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U,
            pg_catalog.gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
          S.usesysid = U.oid;

CREATE OR REPLACE FUNCTION pg_catalog.get_large_table_name(relfile_node text, threshold_size_gb int8)
RETURNS text
AS $$
DECLARE
    query_str text;
    relname text;
BEGIN
    query_str := 'SELECT
                    NVL
                    (
                        (SELECT
                            (case pg_catalog.pg_table_size(c.oid)/1024/1024/1024 > ' || threshold_size_gb || '
                                when true then pg_catalog.concat(concat(n.nspname, ''.''), c.relname)
                                else ''null''
                                end
                            ) relname
                         FROM
                            pg_catalog.pg_class c,
                            pg_catalog.pg_namespace n
                         WHERE
                            c.relnamespace = n.oid
                            AND
                            c.relfilenode = ' || $1 || '
                        )
                        , ''null''
                    ) relname';
    FOR relname IN EXECUTE(query_str) LOOP
        return relname;
    END LOOP;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_user_info(input integer, OUT userid oid, OUT sysadmin boolean, OUT rpoid oid, OUT parentid oid, OUT totalspace bigint, OUT spacelimit bigint, OUT childcount integer, OUT childlist text) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_user_info';

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_resource_pool_info(input integer, OUT respool_oid oid, OUT ref_count integer, OUT active_points integer, OUT running_count integer, OUT waiting_count integer, OUT iops_limits integer, OUT io_priority integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_resource_pool_info';

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_workload_records(input integer, OUT node_idx oid, OUT query_pid bigint, OUT start_time bigint, OUT memory integer, OUT actpts integer, OUT maxpts integer, OUT priority integer, OUT resource_pool text, OUT node_name text, OUT queue_type text, OUT node_group text) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_workload_records';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_control_group_info(out group_name text, out group_type text, out gid bigint, out classgid bigint, out class text, out group_workload text, out shares bigint, out limits bigint, out wdlevel bigint, out cpucores text, out nodegroup text, out group_kind text) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_control_group_info()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT group_name,group_kind FROM pgxc_group WHERE group_kind = ''v'' OR group_kind = ''i'' ';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            IF row_name.group_kind = 'i' THEN
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''installation'')';
            ELSE
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''' ||row_name.        group_name||''')';
            END IF;
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

-- the view for function gs_get_control_group_info.
DROP VIEW IF EXISTS pg_catalog.gs_get_control_group_info CASCADE;
CREATE VIEW pg_catalog.gs_get_control_group_info AS
    SELECT * from gs_get_control_group_info() AS
    (
         name         text,
         type         text,
         gid          bigint,
         classgid     bigint,
         class        text,
         workload     text,
         shares       bigint,
         limits       bigint,
         wdlevel      bigint,
         cpucores     text,
         nodegroup    text,
         group_kind   text
    );


DO $DO$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_wlm_controlgroup_ng_config(out group_name text, out group_type text, out gid bigint, out classgid bigint, out class text, out group_workload text, out shares bigint, out limits bigint, out wdlevel bigint, out cpucores text, out nodegroup text, out group_kind text) CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.wlm_controlgroup_ng_config CASCADE;
        CREATE OR REPLACE FUNCTION DBE_PERF.get_wlm_controlgroup_ng_config()
        RETURNS setof record
        AS $$
        DECLARE
            row_data record;
            row_name record;
            query_str text;
            query_str_nodes text;
            BEGIN
                query_str_nodes := 'SELECT group_name,group_kind FROM pgxc_group WHERE group_kind = ''v'' OR group_kind = ''i'' ';
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                    IF row_name.group_kind = 'i' THEN
                        query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''installation'')';
                    ELSE
                        query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''' ||row_name.        group_name||''')';
                    END IF;
                    FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        -- the view for function gs_get_control_group_info.
        CREATE OR REPLACE VIEW DBE_PERF.wlm_controlgroup_ng_config AS
          SELECT * FROM DBE_PERF.get_wlm_controlgroup_ng_config() AS
            (name         text,
             type         text,
             gid          bigint,
             classgid     bigint,
             class        text,
             workload     text,
             shares       bigint,
             limits       bigint,
             wdlevel      bigint,
             cpucores     text,
             nodegroup    text,
             group_kind   text);
    end if;
END $DO$;


DROP FUNCTION IF EXISTS pg_catalog.pg_get_flush_lsn() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_get_sync_flush_lsn() CASCADE;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_gs_asp() CASCADE;
        DROP FUNCTION IF EXISTS DBE_PERF.get_datanode_active_session() CASCADE;
        DROP FUNCTION IF EXISTS DBE_PERF.get_datanode_active_session_hist() CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.global_active_session CASCADE;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_active_session() CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.local_active_session CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.global_thread_wait_status CASCADE;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_thread_wait_status() CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.thread_wait_status CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.locks CASCADE;
    end if;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.working_version_num() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.locktag_decode() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session() CASCADE;
DROP VIEW IF EXISTS pg_catalog.pgxc_thread_wait_status CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_thread_wait_status() CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_thread_wait_status CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_status() CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_locks CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_lock_status() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1371;
CREATE OR REPLACE FUNCTION pg_catalog.pg_lock_status
(
  OUT locktype pg_catalog.text,
  OUT database pg_catalog.oid,
  OUT relation pg_catalog.oid,
  OUT page pg_catalog.int4,
  OUT tuple pg_catalog.int2,
  OUT bucket pg_catalog.int4,
  OUT virtualxid pg_catalog.text,
  OUT transactionid pg_catalog.xid,
  OUT classid pg_catalog.oid,
  OUT objid pg_catalog.oid,
  OUT objsubid pg_catalog.int2,
  OUT virtualtransaction pg_catalog.text,
  OUT pid pg_catalog.int8,
  OUT sessionid pg_catalog.int8,
  OUT mode pg_catalog.text,
  OUT granted pg_catalog.bool,
  OUT fastpath pg_catalog.bool)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1000 VOLATILE STRICT as 'pg_lock_status';
CREATE OR REPLACE VIEW pg_catalog.pg_locks AS
  SELECT DISTINCT * from pg_catalog.pg_lock_status();

set local inplace_upgrade_next_system_object_oids = IUO_PROC, 3980;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_status
(
  IN  tid pg_catalog.int8,
  OUT node_name pg_catalog.text,
  OUT db_name pg_catalog.text,
  OUT thread_name pg_catalog.text,
  OUT query_id pg_catalog.int8,
  OUT tid pg_catalog.int8,
  OUT sessionid pg_catalog.int8,
  OUT lwtid pg_catalog.int4,
  OUT psessionid pg_catalog.int8,
  OUT tlevel pg_catalog.int4,
  OUT smpid pg_catalog.int4,
  OUT wait_status pg_catalog.text,
  OUT wait_event pg_catalog.text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1000 VOLATILE STRICT as 'pg_stat_get_status';
CREATE VIEW pg_catalog.pg_thread_wait_status AS
    SELECT * FROM pg_catalog.pg_stat_get_status(NULL);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3591;
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_thread_wait_status(
  OUT node_name pg_catalog.text,
  OUT db_name pg_catalog.text,
  OUT thread_name pg_catalog.text,
  OUT query_id pg_catalog.int8,
  OUT tid pg_catalog.int8,
  OUT sessionid pg_catalog.int8,
  OUT lwtid pg_catalog.int4,
  OUT psessionid pg_catalog.int8,
  OUT tlevel pg_catalog.int4,
  OUT smpid pg_catalog.int4,
  OUT wait_status pg_catalog.text,
  OUT wait_event pg_catalog.text)
RETURNS SETOF RECORD
LANGUAGE INTERNAL COST 100 ROWS 1000 AS 'pgxc_stat_get_status';
CREATE OR REPLACE VIEW pg_catalog.pgxc_thread_wait_status AS
    SELECT * FROM pg_catalog.pgxc_get_thread_wait_status();

set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5721;
CREATE OR REPLACE FUNCTION pg_catalog.get_local_active_session
  (OUT sampleid bigint,
   OUT sample_time timestamp with time zone,
   OUT need_flush_sample boolean,
   OUT databaseid oid,
   OUT thread_id bigint,
   OUT sessionid bigint,
   OUT start_time timestamp with time zone,
   OUT event text,
   OUT lwtid integer,
   OUT psessionid bigint,
   OUT tlevel integer,
   OUT smpid integer,
   OUT userid oid,
   OUT application_name text,
   OUT client_addr inet,
   OUT client_hostname text,
   OUT client_port integer,
   OUT query_id bigint,
   OUT unique_query_id bigint,
   OUT user_id oid,
   OUT cn_id integer,
   OUT unique_query text)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_local_active_session';

DO $DO$
DECLARE
    ans boolean;
    user_name text;
    global_query_str text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE VIEW dbe_perf.locks AS
          SELECT * FROM pg_catalog.pg_lock_status() AS L;

        CREATE OR REPLACE VIEW DBE_PERF.thread_wait_status AS
          SELECT * FROM pg_catalog.pg_stat_get_status(NULL);

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_thread_wait_status()
        RETURNS setof DBE_PERF.thread_wait_status
        AS $$
        DECLARE
          row_data DBE_PERF.thread_wait_status%rowtype;
          row_name record;
          query_str text;
          query_str_nodes text;
          BEGIN
            --Get all cn dn node names
            query_str_nodes := 'SELECT node_name FROM pg_catalog.pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
              query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.thread_wait_status''';
              FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
              END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        CREATE VIEW DBE_PERF.global_thread_wait_status AS
          SELECT * FROM DBE_PERF.get_global_thread_wait_status();

        CREATE OR REPLACE FUNCTION DBE_PERF.get_datanode_active_session_hist
        (  IN datanode text,
           IN start_ts timestamp without time zone,
           IN end_ts timestamp without time zone,
           OUT node_name text,
           OUT sampleid bigint,
           OUT sample_time timestamp without time zone,
           OUT need_flush_sample boolean,
           OUT databaseid oid,
           OUT thread_id bigint,
           OUT sessionid bigint,
           OUT start_time timestamp without time zone,
           OUT event text,
           OUT lwtid integer,
           OUT psessionid bigint,
           OUT tlevel integer,
           OUT smpid integer,
           OUT userid oid,
           OUT application_name text,
           OUT client_addr inet,
           OUT client_hostname text,
           OUT client_port integer,
           OUT query_id bigint,
           OUT unique_query_id bigint,
           OUT user_id oid,
           OUT cn_id integer,
           OUT unique_query text)
        RETURNS SETOF record
        AS $$
        DECLARE
            ROW_DATA pg_catalog.gs_asp%ROWTYPE;
            cn_name record;
            query_text record;
            QUERY_STR text;
            query_cn text;
            query_str_cn_name text;
            node_str text;
        BEGIN
                node_str := 'SELECT * FROM pg_catalog.gs_asp where sample_time > '''''||$2||''''' and sample_time < '''''||$3||'''';
                QUERY_STR := 'EXECUTE DIRECT ON (' || $1 || ') '''|| node_str ||''''';';
            FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
                node_name := datanode;
                sampleid := ROW_DATA.sampleid;
                sample_time := ROW_DATA.sample_time;
                need_flush_sample := ROW_DATA.need_flush_sample;
                databaseid := ROW_DATA.databaseid;
                thread_id := ROW_DATA.thread_id;
                sessionid := ROW_DATA.sessionid;
                start_time := ROW_DATA.start_time;
                event := ROW_DATA.event;
                lwtid := ROW_DATA.lwtid;
                psessionid := ROW_DATA.psessionid;
                tlevel := ROW_DATA.tlevel;
                smpid := ROW_DATA.smpid;
                userid := ROW_DATA.userid;
                application_name := ROW_DATA.application_name;
                client_addr := ROW_DATA.client_addr;
                client_hostname := ROW_DATA.client_hostname;
                client_port := ROW_DATA.client_port;
                query_id := ROW_DATA.query_id;
                unique_query_id := ROW_DATA.unique_query_id;
                user_id := ROW_DATA.user_id;
                cn_id := ROW_DATA.cn_id;
                IF ROW_DATA.cn_id IS NULL THEN
                      unique_query := ROW_DATA.unique_query;
                ELSE
                    query_str_cn_name := 'SELECT node_name FROM pg_catalog.pgxc_node WHERE node_id='||ROW_DATA.cn_id||' AND nodeis_active = true';
                    FOR cn_name IN EXECUTE(query_str_cn_name) LOOP
                        query_cn := 'EXECUTE DIRECT ON (' || cn_name.node_name || ') ''SELECT unique_query FROM pg_catalog.gs_asp where unique_query_id = '|| ROW_DATA.unique_query_id||' limit 1''';
                        FOR query_text IN EXECUTE(query_cn) LOOP
                              unique_query := query_text.unique_query;
                        END LOOP;
                    END LOOP;
                END IF;
                RETURN NEXT;
            END LOOP;
            RETURN;
        END; $$
        LANGUAGE 'plpgsql';

        CREATE OR REPLACE FUNCTION DBE_PERF.get_global_gs_asp
          (IN start_ts timestamp without time zone,
           IN end_ts timestamp without time zone,
           OUT node_name text,
           OUT sampleid bigint,
           OUT sample_time timestamp without time zone,
           OUT need_flush_sample boolean,
           OUT databaseid oid,
           OUT thread_id bigint,
           OUT sessionid bigint,
           OUT start_time timestamp without time zone,
           OUT event text,
           OUT lwtid integer,
           OUT psessionid bigint,
           OUT tlevel integer,
           OUT smpid integer,
           OUT userid oid,
           OUT application_name text,
           OUT client_addr inet,
           OUT client_hostname text,
           OUT client_port integer,
           OUT query_id bigint,
           OUT unique_query_id bigint,
           OUT user_id oid,
           OUT cn_id integer,
           OUT unique_query text)
        RETURNS SETOF record
        AS $$
        DECLARE
          ROW_DATA pg_catalog.gs_asp%ROWTYPE;
          ROW_NAME RECORD;
          QUERY_STR TEXT;
          QUERY_STR_NODES TEXT;
          node_str TEXT;
        BEGIN
          QUERY_STR_NODES := 'SELECT NODE_NAME FROM pg_catalog.PGXC_NODE WHERE NODE_TYPE IN (''C'', ''D'')';
          node_str := 'SELECT * FROM pg_catalog.gs_asp where sample_time > '''''||$1||''''' and sample_time < '''''||$2||'''';
          FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
            QUERY_STR := 'EXECUTE DIRECT ON (' || ROW_NAME.NODE_NAME || ') '''|| node_str ||''''';';
            FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
                  node_name := ROW_NAME.NODE_NAME;
                  sampleid := ROW_DATA.sampleid;
                  sample_time := ROW_DATA.sample_time;
                  need_flush_sample := ROW_DATA.need_flush_sample;
                  databaseid := ROW_DATA.databaseid;
                  thread_id := ROW_DATA.thread_id;
                  sessionid := ROW_DATA.sessionid;
                  start_time := ROW_DATA.start_time;
                  event := ROW_DATA.event;
                  lwtid := ROW_DATA.lwtid;
                  psessionid := ROW_DATA.psessionid;
                  tlevel := ROW_DATA.tlevel;
                  smpid := ROW_DATA.smpid;
                  userid := ROW_DATA.userid;
                  application_name := ROW_DATA.application_name;
                  client_addr := ROW_DATA.client_addr;
                  client_hostname := ROW_DATA.client_hostname;
                  client_port := ROW_DATA.client_port;
                  query_id := ROW_DATA.query_id;
                  unique_query_id := ROW_DATA.unique_query_id;
                  user_id := ROW_DATA.user_id;
                  cn_id := ROW_DATA.cn_id;
                  unique_query := ROW_DATA.unique_query;
              RETURN NEXT;
            END LOOP;
          END LOOP;
          RETURN;
        END; $$
        LANGUAGE 'plpgsql';

        SELECT SESSION_USER INTO user_name;
        global_query_str := 'GRANT ALL ON TABLE DBE_PERF.locks TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE global_query_str;
        global_query_str := 'GRANT ALL ON TABLE DBE_PERF.global_thread_wait_status TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE global_query_str;
        global_query_str := 'GRANT ALL ON TABLE DBE_PERF.thread_wait_status TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE global_query_str;

        GRANT SELECT ON DBE_PERF.locks TO public;
        GRANT SELECT ON DBE_PERF.global_thread_wait_status TO public;
        GRANT SELECT ON DBE_PERF.thread_wait_status TO public;


    end if;
END $DO$;

GRANT SELECT ON pg_catalog.pg_locks TO public;
GRANT SELECT ON pg_catalog.pg_thread_wait_status TO public;
GRANT SELECT ON pg_catalog.pgxc_thread_wait_status TO public;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_thread_wait_status' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_global_thread_wait_status
    DROP COLUMN IF EXISTS snap_locktag,
    DROP COLUMN IF EXISTS snap_lockmode,
    DROP COLUMN IF EXISTS snap_block_sessionid;
  end if;
END$DO$;

-- ----------------------------------------------------------------
-- rollback pg_pooler_status
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8, OUT used_count int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3955;
CREATE FUNCTION pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'pg_stat_get_pooler_status';

DROP VIEW IF EXISTS pg_catalog.pg_pooler_status;
CREATE VIEW pg_catalog.pg_pooler_status AS
    SELECT
            S.database_name AS database,
            S.user_name AS user_name,
            S.tid AS tid,
            S.node_oid AS node_oid,
            D.node_name AS node_name,
            S.in_use AS in_use,
            D.node_port AS node_port,
            S.fdsock AS fdsock,
            S.remote_pid AS remote_pid,
            S.session_params AS session_params
    FROM pgxc_node D, pg_stat_get_pooler_status() AS S
    WHERE D.oid = S.node_oid;

GRANT SELECT ON pg_catalog.pg_pooler_status TO public;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP VIEW IF EXISTS DBE_PERF.summary_statement cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_statement(
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
      OUT net_stream_recv_info text
    ) cascade;
    DROP VIEW IF EXISTS DBE_PERF.STATEMENT cascade;
  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.get_instr_unique_sql() cascade;
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
  OUT net_send_time bigint,
  OUT data_io_time bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_unique_sql';

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE VIEW DBE_PERF.statement AS
      SELECT * FROM get_instr_unique_sql();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_statement()
    RETURNS setof DBE_PERF.statement
    AS $$
    DECLARE
      row_data DBE_PERF.statement%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
      BEGIN
        --Get all the node names
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
          query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.statement''';
            FOR row_data IN EXECUTE(query_str) LOOP
              return next row_data;
           END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_statement AS
      SELECT * FROM DBE_PERF.get_summary_statement();
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.STATEMENT TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.summary_statement TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;
  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_summary_statement' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_summary_statement
    DROP COLUMN IF EXISTS snap_net_send_info,
    DROP COLUMN IF EXISTS snap_net_recv_info,
    DROP COLUMN IF EXISTS snap_net_stream_send_info,
    DROP COLUMN IF EXISTS snap_net_stream_recv_info;
  end if;
END$DO$;

DROP VIEW IF EXISTS pg_catalog.gs_matviews CASCADE;

do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.global_get_bgwriter_status CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.local_bgwriter_stat();
        DROP FUNCTION IF EXISTS pg_catalog.remote_bgwriter_stat();
    end if;
END$$;

grant ALL on table pg_catalog.gs_wlm_session_query_info_all to public;
grant ALL on table pg_catalog.gs_wlm_operator_info to public;
grant ALL on table pg_catalog.gs_wlm_ec_operator_info to public;
grant all on pg_catalog.gs_wlm_session_info to public;
grant all on pg_catalog.gs_wlm_user_session_info to public;
grant all on pg_catalog.gs_wlm_workload_records to public;

DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS dbe_perf.track_memory_context(IN contexts text) cascade;
    DROP FUNCTION IF EXISTS dbe_perf.track_memory_context_detail(OUT context_name text, OUT file text, OUT line int, OUT size bigint) cascade;
    DROP VIEW IF EXISTS dbe_perf.track_memory_context_detail cascade;
  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
CREATE FUNCTION pg_catalog.pg_stat_get_activity(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT connection_info TEXT) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity_with_conninfo';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_session_wlmstat(INT4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3502;
CREATE FUNCTION pg_catalog.pg_stat_get_session_wlmstat(INT4, OUT datid OID, OUT threadid INT8, OUT sessionid INT8, OUT threadpid INT4, OUT usesysid OID, OUT appname TEXT, OUT query TEXT,
OUT priority INT8, OUT block_time INT8, OUT elapsed_time INT8, OUT total_cpu_time INT8, OUT skew_percent INT4, OUT statement_mem INT4, OUT active_points INT4, OUT dop_value INT4, OUT current_cgroup TEXT,
OUT current_status TEXT, OUT enqueue_state TEXT, OUT attribute TEXT, OUT is_plana BOOL, OUT node_group TEXT) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_session_wlmstat';

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
        WHERE S.datid = D.oid AND
              S.usesysid = U.oid AND
              T.sessionid = S.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.sessionid = N.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_session_wlmstat AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.sessionid,
            S.threadpid AS processid,
            S.usesysid,
            S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
            S.skew_percent AS cpu_skew_percent,
            S.statement_mem,
            S.active_points,
            S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, gs_wlm_session_respool(0) AS T
        WHERE S.datid = D.oid AND
              S.usesysid = U.oid AND
              T.sessionid = S.sessionid;


DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE VIEW dbe_perf.session_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query_id,
            S.query
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
          T.threadid = S.pid;


    CREATE OR REPLACE VIEW DBE_PERF.replication_stat AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
          S.pid = W.pid;

    CREATE OR REPLACE VIEW DBE_PERF.statement_wlmstat_complex_runtime AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.threadpid AS processid,
            S.usesysid,
            S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
            S.skew_percent AS cpu_skew_percent,
            S.statement_mem,
            S.active_points,
            S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
          T.threadid = S.threadid;
    end if;
END $DO$;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_replication AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_catalog.pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_catalog.pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
            S.pid = W.pid;

CREATE OR REPLACE VIEW pg_catalog.DV_SESSIONS AS
    SELECT
            sa.sessionid AS SID,
            0::integer AS SERIAL#,
            sa.usesysid AS USER#,
            ad.rolname AS USERNAME
    FROM pg_catalog.pg_stat_get_activity(NULL) AS sa
        LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
    WHERE sa.application_name <> 'JobScheduler';

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_workload_records AS
    SELECT
            P.node_name,
            S.threadid AS thread_id,
            S.threadpid AS processid,
            P.start_time AS time_stamp,
            U.rolname AS username,
            P.memory,
            P.actpts AS active_points,
            P.maxpts AS max_points,
            P.priority,
            P.resource_pool,
            S.current_status AS status,
            S.current_cgroup AS control_group,
            P.queue_type AS enqueue,
            S.query,
            P.node_group
    FROM pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U,
            pg_catalog.gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
          S.usesysid = U.oid;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4,in schema text, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
                        FROM pg_class p,
                        (SELECT  relname, schemaname, SUM(n_tup_ins) n_tup_ins, SUM(n_tup_upd) n_tup_upd, SUM(n_tup_del) n_tup_del, SUM(n_live_tup) n_live_tup, SUM(n_dead_tup) n_dead_tup, CAST((SUM(n_dead_tup) / SUM(n_dead_tup + n_live_tup + 0.00001) * 100)
                        AS NUMERIC(5,2)) dirty_page_rate FROM pg_catalog.pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||','''||schema||''') GROUP BY (relname,schemaname)) s
                        WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
        FOR row_data IN EXECUTE(query_str) LOOP
            relid = row_data.relid;
            relname = row_data.relname;
            schemaname = row_data.schemaname;
            n_tup_ins = row_data.n_tup_ins;
            n_tup_upd = row_data.n_tup_upd;
            n_tup_del = row_data.n_tup_del;
            n_live_tup = row_data.n_live_tup;
            n_dead_tup = row_data.n_dead_tup;
            dirty_page_rate = row_data.dirty_page_rate;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

drop extension if exists gsredistribute cascade;

DROP VIEW IF EXISTS pg_catalog.pg_stat_replication;
  CREATE VIEW pg_catalog.pg_stat_replication AS
      SELECT
              S.pid,
              S.usesysid,
              U.rolname AS usename,
              S.application_name,
              S.client_addr,
              S.client_hostname,
              S.client_port,
              S.backend_start,
              W.state,
              W.sender_sent_location,
              W.receiver_write_location,
              W.receiver_flush_location,
              W.receiver_replay_location,
              W.sync_priority,
              W.sync_state
      FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
              pg_stat_get_wal_senders() AS W
      WHERE S.usesysid = U.oid AND
            S.pid = W.sender_pid;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP VIEW IF EXISTS dbe_perf.replication_stat;
    CREATE VIEW dbe_perf.replication_stat AS
      SELECT
        S.pid,
        S.usesysid,
        U.rolname AS usename,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        S.backend_start,
        W.state,
        W.sender_sent_location,
        W.receiver_write_location,
        W.receiver_flush_location,
        W.receiver_replay_location,
        W.sync_priority,
        W.sync_state
        FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
             pg_stat_get_wal_senders() AS W
        WHERE S.usesysid = U.oid AND
              S.pid = W.sender_pid;
   end if;
END$DO$;

CREATE or REPLACE FUNCTION pg_catalog.gs_get_table_distribution( IN table_name TEXT, IN schema_name TEXT)
RETURNS TEXT
AS
$$
DECLARE
nodename  text;
query_str text;
row_data record;
row_data1 record;
nodelist  text;
flag boolean;
special text := '[;|-]';
BEGIN
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN table_name, IN special;
    IF flag = true THEN
        raise WARNING 'Illegal character entered for function';
        return nodelist;
    END IF;

    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN schema_name, IN special;
    IF flag = true THEN
        raise WARNING 'Illegal character entered for function';
        return nodelist;
    END IF;

    query_str := 'SELECT oid,node_name FROM pgxc_node WHERE node_type=''D'' order by node_name';

    FOR row_data IN EXECUTE(query_str) LOOP
        query_str := 'EXECUTE DIRECT ON (' || row_data.node_name || ') ''SELECT count(*) AS num  FROM pg_class c LEFT JOIN pg_namespace n  ON c.relnamespace = n.oid WHERE relname='''''||table_name||''''' AND n.nspname='''''||schema_name||''''' ''';
        FOR row_data1 IN EXECUTE(query_str) LOOP
            IF row_data1.num = 1 THEN
                nodelist := nodelist || ' '|| row_data.oid;
            END IF;
        END LOOP;
   END LOOP;

   RETURN nodelist;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_current_instance_info(text, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
    row_data gs_wlm_instance_history%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    query_str_temp text;
    node_name text;
    BEGIN
        IF $2 is null THEN
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info()';
        ELSE
            query_str_temp := 'SELECT * FROM pg_stat_get_wlm_instance_info() limit '||$2;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='''||$1||'''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_history_instance_info(text, TIMESTAMP, TIMESTAMP, int default null)
RETURNS setof gs_wlm_instance_history
AS $$
DECLARE
    row_data gs_wlm_instance_history%rowtype;
    row_name record;
    query_str text;
    query_str_nodes text;
    query_str_temp text;
    node_name text;
    BEGIN
        IF $4 is null THEN
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'''''||$2||''''' and timestamp <'''''||$3||''''' ';
        ELSE
            query_str_temp := 'SELECT * FROM gs_wlm_instance_history where timestamp >'''''||$2||''''' and timestamp <'''''||$3||''''' limit '||$4;
        END IF;

        IF $1 IN ('C','D') THEN
            query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type='''||$1||'''';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSIF $1 ='ALL' THEN
            query_str_nodes := 'SELECT distinct node_name FROM pgxc_node';
            FOR row_name IN EXECUTE(query_str_nodes) LOOP
                query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        ELSE
            FOR node_name in select pg_catalog.regexp_split_to_table('' || $1 || '',',') LOOP
                query_str := 'EXECUTE DIRECT ON ('||node_name||') ''' || query_str_temp ||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                    return next row_data;
                END LOOP;
            END LOOP;
        END IF;
    return;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP VIEW IF EXISTS pg_catalog.pg_roles cascade;
CREATE OR REPLACE VIEW pg_catalog.pg_roles AS
    SELECT
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcatupdate,
        rolcanlogin,
        rolreplication,
        rolauditadmin,
        rolsystemadmin,
        rolconnlimit,
        '********'::text as rolpassword,
        rolvalidbegin,
        rolvaliduntil,
        rolrespool,
        rolparentid,
        roltabspace,
        setconfig as rolconfig,
        pg_authid.oid,
        roluseft,
        rolkind,
        pgxc_group.group_name as nodegroup,
        roltempspace,
        rolspillspace,
        rolmonitoradmin,
        roloperatoradmin,
        rolpolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid);

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.pg_roles TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

CREATE OR REPLACE VIEW pg_catalog.pg_rlspolicies AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS tablename,
        pol.polname AS policyname,
        CASE
            WHEN pol.polpermissive THEN
                'PERMISSIVE'
            ELSE
                'RESTRICTIVE'
        END AS policypermissive,
        CASE
            WHEN pol.polroles = '{0}' THEN
                string_to_array('public', ' ')
            ELSE
                ARRAY
                (
                    SELECT rolname
                    FROM pg_catalog.pg_roles
                    WHERE oid = ANY (pol.polroles) ORDER BY 1
                )
        END AS policyroles,
        CASE pol.polcmd
            WHEN 'r' THEN 'SELECT'
            WHEN 'a' THEN 'INSERT'
            WHEN 'w' THEN 'UPDATE'
            WHEN 'd' THEN 'DELETE'
            WHEN '*' THEN 'ALL'
        END AS policycmd,
        pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS policyqual
    FROM pg_catalog.pg_rlspolicy pol
    JOIN pg_catalog.pg_class C ON (C.oid = pol.polrelid)
    LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace);

GRANT SELECT ON TABLE pg_catalog.pg_rlspolicies TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_user_info AS
SELECT
		T.userid,
		S.rolname AS username,
		T.sysadmin,
		T.rpoid,
		R.respool_name AS respool,
		T.parentid,
		T.totalspace,
		T.spacelimit,
		T.childcount,
		T.childlist
FROM pg_roles AS S, pg_catalog.gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
WHERE S.oid = T.userid AND T.rpoid = R.oid;

GRANT SELECT ON TABLE pg_catalog.gs_wlm_user_info TO PUBLIC;

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE VIEW dbe_perf.wlm_user_resource_config AS
      SELECT
      T.userid,
      S.rolname AS username,
      T.sysadmin,
      T.rpoid,
      R.respool_name AS respool,
      T.parentid,
      T.totalspace,
      T.spacelimit,
      T.childcount,
      T.childlist
    FROM pg_roles AS S, gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
      WHERE S.oid = T.userid AND T.rpoid = R.oid;

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE dbe_perf.wlm_user_resource_config TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE dbe_perf.wlm_user_resource_config TO PUBLIC;
  end if;
END $DO$;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_delta_info(IN rel TEXT, IN schema_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_info_str text;
    query_str text;
    query_part_str text;
    query_select_str text;
    query_size_str text;
    row_info_data record;
    row_data record;
    row_part_info record;
    BEGIN
        query_info_str := 'SELECT C.oid,C.reldeltarelid,C.parttype FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)  WHERE C.relname = '''|| rel ||''' and N.nspname = '''|| schema_name ||'''';
        FOR row_info_data IN EXECUTE(query_info_str) LOOP
        IF row_info_data.parttype = 'n' THEN
            query_str := 'SELECT relname,oid from pg_class where oid= '||row_info_data.reldeltarelid||'';
            EXECUTE(query_str) INTO row_data;
            query_select_str := 'select count(*) from cstore.' || row_data.relname || '';
            EXECUTE (query_select_str) INTO live_tuple;
            query_size_str := 'select * from pg_catalog.pg_relation_size(' || row_data.oid || ')';
            EXECUTE (query_size_str) INTO data_size;
            blockNum := data_size/8192;
            part_name := 'non partition table';
            return next;
        ELSE
            query_part_str := 'SELECT relname,reldeltarelid from pg_partition where parentid = '||row_info_data.oid|| 'and relname <> '''||rel||'''';
            FOR row_part_info IN EXECUTE(query_part_str) LOOP
                query_str := 'SELECT relname,oid from pg_class where  oid = '||row_part_info.reldeltarelid||'';
                part_name := row_part_info.relname;
                FOR row_data IN EXECUTE(query_str) LOOP
                    query_select_str := 'select count(*) from cstore.' || row_data.relname || '';
                    EXECUTE (query_select_str) INTO live_tuple;
                    query_size_str := 'select * from pg_catalog.pg_relation_size(' || row_data.oid || ')';
                    EXECUTE (query_size_str) INTO data_size;
                END LOOP;
                blockNum := data_size/8192;
                return next;
            END LOOP;
        END IF;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_delta_info(IN rel TEXT, OUT node_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str_nodes text;
    query_str text;
    query_part_str text;
    rel_info record;
    row_name record;
    row_data record;
    BEGIN
        EXECUTE ('select c.relname,n.nspname from pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) where C.oid = (select '''|| rel ||'''::regclass::oid)') into rel_info;
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_catalog.pg_get_delta_info('''''||rel_info.relname||''''','''''||rel_info.nspname||''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                live_tuple := row_data.live_tuple;
                data_size := row_data.data_size;
                blockNum := row_data.blockNum;
                part_name := row_data.part_name;
                return next;
            END LOOP;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.get_delta_info(IN rel TEXT, OUT part_name TEXT, OUT total_live_tuple INT8, OUT total_data_size INT8, OUT max_blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'select part_name, sum(live_tuple) total_live_tuple, sum(data_size) total_data_size, max(blocknum) max_blocknum from (select * from pg_catalog.pgxc_get_delta_info('''||$1||''')) group by part_name order by max_blocknum desc';
        FOR row_data IN EXECUTE(query_str) LOOP
            part_name := row_data.part_name;
            total_live_tuple := row_data.total_live_tuple;
            total_data_size := row_data.total_data_size;
            max_blockNum := row_data.max_blocknum;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.pg_terminate_session(bigint, bigint) cascade;

CREATE OR REPLACE VIEW pg_catalog.gs_session_memory_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_peak_memory,
        T.max_peak_memory,
        T.spill_info,
        S.query,
        S.node_group,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.gs_session_cpu_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        S.query,
        S.node_group,
        T.top_cpu_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_session_statistics AS
SELECT
        S.datid AS datid,
        S.datname AS dbname,
        T.schemaname,
        T.nodename,
        S.usename AS username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        T.query_band,
        S.pid,
        S.sessionid,
        T.block_time,
        S.query_start AS start_time,
        T.duration,
        T.estimate_total_time,
        T.estimate_left_time,
        S.enqueue,
        S.resource_pool,
        T.control_group,
        T.estimate_memory,
        T.min_peak_memory,
        T.max_peak_memory,
        T.average_peak_memory,
        T.memory_skew_percent,
        T.spill_info,
        T.min_spill_size,
        T.max_spill_size,
        T.average_spill_size,
        T.spill_skew_percent,
        T.min_dn_time,
        T.max_dn_time,
        T.average_dn_time,
        T.dntime_skew_percent,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        T.cpu_skew_percent,
        T.min_peak_iops,
        T.max_peak_iops,
        T.average_peak_iops,
        T.iops_skew_percent,
        T.warning,
        S.query_id AS queryid,
        T.query,
        T.query_plan,
        S.node_group,
        T.top_cpu_dn,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE FUNCTION pg_catalog.pv_session_memory_detail_tp(OUT sessid TEXT, OUT sesstype TEXT, OUT contextname TEXT, OUT level INT2, OUT parent TEXT, OUT totalsize INT8, OUT freesize INT8, OUT usedsize INT8)
RETURNS setof record
AS $$
DECLARE
  enable_threadpool bool;
  row_data record;
  query_str text;
BEGIN
  show enable_thread_pool into enable_threadpool;

  IF enable_threadpool THEN
    query_str := 'with SM AS
                   (SELECT
                      S.sessid AS sessid,
                      T.thrdtype AS sesstype,
                      S.contextname AS contextname,
                      S.level AS level,
                      S.parent AS parent,
                      S.totalsize AS totalsize,
                      S.freesize AS freesize,
                      S.usedsize AS usedsize
                    FROM
                      pv_session_memory_context S
                      LEFT JOIN
                     (SELECT DISTINCT thrdtype, tid
                      FROM pv_thread_memory_context) T
                      on S.threadid = T.tid
                   ),
                   TM AS
                   (SELECT
                      S.sessid AS Ssessid,
                      T.thrdtype AS sesstype,
                      T.threadid AS Tsessid,
                      T.contextname AS contextname,
                      T.level AS level,
                      T.parent AS parent,
                      T.totalsize AS totalsize,
                      T.freesize AS freesize,
                      T.usedsize AS usedsize
                    FROM
                      pv_thread_memory_context T
                      LEFT JOIN
                      (SELECT DISTINCT sessid, threadid
                       FROM pv_session_memory_context) S
                      ON T.tid = S.threadid
                   )
                   SELECT * from SM
                   UNION
                   SELECT
                     Ssessid AS sessid, sesstype, contextname, level, parent, totalsize, freesize, usedsize
                   FROM TM WHERE Ssessid IS NOT NULL
                   UNION
                   SELECT
                     Tsessid AS sessid, sesstype, contextname, level, parent, totalsize, freesize, usedsize
                   FROM TM WHERE Ssessid IS NULL;';
    FOR row_data IN EXECUTE(query_str) LOOP
      sessid = row_data.sessid;
      sesstype = row_data.sesstype;
      contextname = row_data.contextname;
      level = row_data.level;
      parent = row_data.parent;
      totalsize = row_data.totalsize;
      freesize = row_data.freesize;
      usedsize = row_data.usedsize;
      return next;
    END LOOP;
  ELSE
    query_str := 'SELECT
                    T.threadid AS sessid,
                    T.thrdtype AS sesstype,
                    T.contextname AS contextname,
                    T.level AS level,
                    T.parent AS parent,
                    T.totalsize AS totalsize,
                    T.freesize AS freesize,
                    T.usedsize AS usedsize
                  FROM pg_catalog.pv_thread_memory_detail() T;';
    FOR row_data IN EXECUTE(query_str) LOOP
      sessid = row_data.sessid;
      sesstype = row_data.sesstype;
      contextname = row_data.contextname;
      level = row_data.level;
      parent = row_data.parent;
      totalsize = row_data.totalsize;
      freesize = row_data.freesize;
      usedsize = row_data.usedsize;
      return next;
    END LOOP;
  END IF;
  RETURN;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    total_num bigint;
    flag boolean;
    special text := '[;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN _table_name, IN special;
        IF flag = true THEN
            raise WARNING 'illegal character entered for function';
            RETURN;
        END IF;
        EXECUTE 'SELECT count(1) FROM ' || _table_name
            INTO total_num;

		--Get the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node, pgxc_class where node_type IN (''C'', ''D'') AND is_oid_in_group_members(oid, nodeoids) AND
			pcrelid=''' || _table_name || '''::regclass::oid';

        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''DN_name'''' as dnname1, count(1) as count1 from ' || $1 || '''';

            FOR row_data IN EXECUTE(query_str) LOOP
                row_data.dnname1 := CASE
                    WHEN LENGTH(row_name.node_name)<20
                    THEN row_name.node_name || right('                    ',20-length(row_name.node_name))
                    ELSE SUBSTR(row_name.node_name,1,20)
                    END;
                DNName := row_data.dnname1;
                Num := row_data.count1;
                IF total_num = 0 THEN
                Ratio := 0.000 ||'%';
                ELSE
                Ratio := ROUND(row_data.count1/total_num*100,3) || '%';
                END IF;
                RETURN next;
            END LOOP;
        END LOOP;

        RETURN;
    END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.get_delta_info(IN rel TEXT, OUT part_name TEXT, OUT total_live_tuple INT8, OUT total_data_size INT8, OUT max_blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'select part_name, sum(live_tuple) total_live_tuple, sum(data_size) total_data_size, max(blocknum) max_blocknum from (select * from pg_catalog.pgxc_get_delta_info('''||rel||''')) group by part_name order by max_blocknum desc';
        FOR row_data IN EXECUTE(query_str) LOOP
            part_name := row_data.part_name;
            total_live_tuple := row_data.total_live_tuple;
            total_data_size := row_data.total_data_size;
            max_blockNum := row_data.max_blocknum;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.get_shard_oids_byname(IN node_name varchar)
RETURNS Oid[] AS $$
declare
    node_oid Oid;
    nodes Oid[];
    nodenames_query text;
    total_num bigint;
    flag boolean;
    special text := '[;|-]';
BEGIN
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN node_name, IN special;

    IF flag = true THEN
        raise WARNING 'illegal character entered for function';
        return '{0}';
    ELSE
        nodenames_query := 'SELECT oid FROM pgxc_node WHERE node_name = ''' || node_name || ''' ORDER BY oid';
        FOR node_oid IN EXECUTE nodenames_query LOOP
            nodes := array_append(nodes, node_oid);
        END LOOP;
        RETURN nodes;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.get_large_table_name(relfile_node text, threshold_size_gb int8)
RETURNS text
AS $$
DECLARE
    query_str text;
    relname text;
BEGIN
    query_str := 'SELECT
                    NVL
                    (
                        (SELECT
                            (case pg_catalog.pg_table_size(c.oid)/1024/1024/1024 > ' || threshold_size_gb || '
                                when true then pg_catalog.concat(concat(n.nspname, ''.''), c.relname)
                                else ''null''
                                end
                            ) relname
                         FROM
                            pg_catalog.pg_class c,
                            pg_catalog.pg_namespace n
                         WHERE
                            c.relnamespace = n.oid
                            AND
                            c.relfilenode = ' || relfile_node || '
                        )
                        , ''null''
                    ) relname';
    FOR relname IN EXECUTE(query_str) LOOP
        return relname;
    END LOOP;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

create or replace function pg_catalog.table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    BEGIN
        if row_num = 0 then
            EXECUTE 'select count(1) from ' || table_name into tolal_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ' limit ' || row_num ||') group by seqNum order by num DESC';
        end if;

        if tolal_num = 0 then
            seqNum = 0;
            Num = 0;
            Ratio = ROUND(0, 3) || '%';
            return;
        end if;

        for row_data in EXECUTE execute_query loop
            seqNum = row_data.seqNum;
            Num = row_data.num;
            Ratio = ROUND(row_data.num / tolal_num * 100, 3) || '%';
            RETURN next;
        end loop;
    END;
$$LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                        WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE T.session_respool
                        END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S,
            pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                        WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE T.session_respool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N,
            pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.sessionid = N.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_session_wlmstat AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.sessionid,
            S.threadpid AS processid,
            S.usesysid,
                        S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
                        S.skew_percent AS cpu_skew_percent,
                        S.statement_mem,
                        S.active_points,
                        S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                                ELSE T.session_respool
                                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S,
            pg_authid AS U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid;

CREATE OR REPLACE VIEW pg_catalog.pg_wlm_statistics AS
    SELECT
            statement,
            block_time,
            elapsed_time,
            total_cpu_time,
            qualification_time,
            skew_percent AS cpu_skew_percent,
            control_group,
            status,
            action
    FROM pg_stat_get_wlm_statistics(NULL);

CREATE OR REPLACE VIEW pg_catalog.gs_session_memory_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_peak_memory,
        T.max_peak_memory,
        T.spill_info,
        S.query,
        S.node_group,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.pg_session_iostat AS
    SELECT
        S.query_id,
        T.mincurr_iops as mincurriops,
        T.maxcurr_iops as maxcurriops,
        T.minpeak_iops as minpeakiops,
        T.maxpeak_iops as maxpeakiops,
        T.iops_limits as io_limits,
        CASE WHEN T.io_priority = 0 THEN 'None'::text
             WHEN T.io_priority = 10 THEN 'Low'::text
             WHEN T.io_priority = 20 THEN 'Medium'::text
             WHEN T.io_priority = 50 THEN 'High'::text END AS io_priority,
        S.query,
        S.node_group
FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_session_iostat_info(0) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.gs_cluster_resource_info AS SELECT * FROM pg_stat_get_wlm_node_resource_info(0);

CREATE OR REPLACE VIEW pg_catalog.gs_session_cpu_statistics AS
SELECT
        S.datid AS datid,
        S.usename,
        S.pid,
        S.query_start AS start_time,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        S.query,
        S.node_group,
        T.top_cpu_dn
FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_session_statistics AS
SELECT
        S.datid AS datid,
        S.datname AS dbname,
        T.schemaname,
        T.nodename,
        S.usename AS username,
        S.application_name,
        S.client_addr,
        S.client_hostname,
        S.client_port,
        T.query_band,
        S.pid,
        S.sessionid,
        T.block_time,
        S.query_start AS start_time,
        T.duration,
        T.estimate_total_time,
        T.estimate_left_time,
        S.enqueue,
        S.resource_pool,
        T.control_group,
        T.estimate_memory,
        T.min_peak_memory,
        T.max_peak_memory,
        T.average_peak_memory,
        T.memory_skew_percent,
        T.spill_info,
        T.min_spill_size,
        T.max_spill_size,
        T.average_spill_size,
        T.spill_skew_percent,
        T.min_dn_time,
        T.max_dn_time,
        T.average_dn_time,
        T.dntime_skew_percent,
        T.min_cpu_time,
        T.max_cpu_time,
        T.total_cpu_time,
        T.cpu_skew_percent,
        T.min_peak_iops,
        T.max_peak_iops,
        T.average_peak_iops,
        T.iops_skew_percent,
        T.warning,
        S.query_id AS queryid,
        T.query,
        T.query_plan,
        S.node_group,
        T.top_cpu_dn,
        T.top_mem_dn
FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_realtime_session_info(NULL) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_all_user_resource_info()
RETURNS setof record
AS $$
DECLARE
        row_data record;
        row_name record;
        query_str text;
        query_str2 text;
        BEGIN
                query_str := 'SELECT rolname FROM pg_authid';
                FOR row_name IN EXECUTE(query_str) LOOP
                        query_str2 := 'SELECT * FROM gs_wlm_user_resource_info(''' || row_name.rolname || ''')';
                        FOR row_data IN EXECUTE(query_str2) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_session_info_all AS
SELECT * FROM pg_stat_get_wlm_session_info(0);

CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_session_info(IN flag int)
RETURNS int
AS $$
DECLARE
        query_str text;
        record_cnt int;
        BEGIN
                record_cnt := 0;

                query_str := 'SELECT * FROM pg_stat_get_wlm_session_info(1)';

                IF flag > 0 THEN
                        EXECUTE 'INSERT INTO gs_wlm_session_query_info_all ' || query_str;
                ELSE
                        EXECUTE query_str;
                END IF;

                RETURN record_cnt;
        END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_cgroup_info AS
    SELECT
            cgroup_name,
            percent AS priority,
            usage_percent AS usage_percent,
            shares,
            usage AS cpuacct,
            cpuset,
            relpath,
            valid,
            node_group
    FROM pg_stat_get_cgroup_info(NULL);

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_user_info AS
SELECT
                T.userid,
                S.rolname AS username,
                T.sysadmin,
                T.rpoid,
                R.respool_name AS respool,
                T.parentid,
                T.totalspace,
                T.spacelimit,
                T.childcount,
                T.childlist
FROM pg_roles AS S, gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
WHERE S.oid = T.userid AND T.rpoid = R.oid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_resource_pool AS
SELECT
                T.respool_oid AS rpoid,
                R.respool_name AS respool,
                R.control_group AS control_group,
                R.parentid AS parentid,
                T.ref_count,
                T.active_points,
                T.running_count,
                T.waiting_count,
                T.iops_limits as io_limits,
                T.io_priority
FROM gs_wlm_get_resource_pool_info(0) AS T, pg_resource_pool AS R
WHERE T.respool_oid = R.oid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_rebuild_user_resource_pool AS
        SELECT * FROM gs_wlm_rebuild_user_resource_pool(0);

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_workload_records AS
    SELECT
                        P.node_name,
            S.threadid AS thread_id,
            S.threadpid AS processid,
                        P.start_time AS time_stamp,
            U.rolname AS username,
                        P.memory,
                        P.actpts AS active_points,
                        P.maxpts AS max_points,
                        P.priority,
                        P.resource_pool,
                        S.current_status AS status,
                        S.current_cgroup AS control_group,
                        P.queue_type AS enqueue,
            S.query,
            P.node_group
    FROM pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U,
            gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
            S.usesysid = U.oid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_replication AS
    SELECT
            S.pid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            W.state,
            W.sender_sent_location,
            W.receiver_write_location,
            W.receiver_flush_location,
            W.receiver_replay_location,
            W.sync_priority,
            W.sync_state
    FROM pg_stat_get_activity(NULL) AS S, pg_authid U,
            pg_stat_get_wal_senders() AS W
    WHERE S.usesysid = U.oid AND
            S.pid = W.sender_pid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_database AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_numbackends(D.oid) AS numbackends,
            pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
            pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
            pg_stat_get_db_blocks_fetched(D.oid) -
                    pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
            pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
            pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
            pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
            pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
            pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
            pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
            pg_stat_get_db_conflict_all(D.oid) AS conflicts,
            pg_stat_get_db_temp_files(D.oid) AS temp_files,
            pg_stat_get_db_temp_bytes(D.oid) AS temp_bytes,
            pg_stat_get_db_deadlocks(D.oid) AS deadlocks,
            pg_stat_get_db_blk_read_time(D.oid) AS blk_read_time,
            pg_stat_get_db_blk_write_time(D.oid) AS blk_write_time,
            pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
    FROM pg_database D;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_database_conflicts AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
            pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
            pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
            pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
            pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
    FROM pg_database D;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_stat_get_function_calls(P.oid) AS calls,
            pg_stat_get_function_total_time(P.oid) AS total_time,
            pg_stat_get_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_stat_get_function_calls(P.oid) IS NOT NULL;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_xact_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_stat_get_xact_function_calls(P.oid) AS calls,
            pg_stat_get_xact_function_total_time(P.oid) AS total_time,
            pg_stat_get_xact_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_stat_get_xact_function_calls(P.oid) IS NOT NULL;

CREATE OR REPLACE VIEW pg_catalog.DV_SESSIONS AS
        SELECT
                sa.sessionid AS SID,
                0::integer AS SERIAL#,
                sa.usesysid AS USER#,
                ad.rolname AS USERNAME
        FROM pg_stat_get_activity(NULL) AS sa
        LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
        WHERE sa.application_name <> 'JobScheduler';

CREATE OR REPLACE FUNCTION pg_catalog.pg_nodes_memmon(OUT InnerNName text, OUT InnerUsedMem int8, OUT InnerTopCtxt int8, OUT NName text, OUT UsedMem text, OUT SharedBufferCache text,  OUT TopContext text)
RETURNS setof record
AS $$
DECLARE
  enable_threadpool bool;
  row_data record;
  row_data1 record;
  row_name record;
  query_str text;
  query_str1 text;
  shared_bcache text;
  query_str_nodes text;
  itr integer;
  BEGIN
    --Get all the node names
    query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';

    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''N_name'''' as nname1, sum(usedsize) as usedmem1 from pv_session_memory_detail where 1=1 ''';
      query_str1 := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select contextname as contextname1, sum(usedsize) as usedsize1 from pv_session_memory_detail group by contextname order by usedsize1 desc limit 3 ''';

      FOR row_data IN EXECUTE(query_str) LOOP
        row_data.nname1 := CASE
          WHEN LENGTH(row_name.node_name)<20
          THEN row_name.node_name || right('                    ',20-length(row_name.node_name))
          ELSE SUBSTR(row_name.node_name,1,20)
          END;

        InnerNName := row_data.nname1;
        NName := row_data.nname1;
        InnerUsedMem := row_data.usedmem1;
        UsedMem := pg_size_pretty(row_data.usedmem1);

        EXECUTE 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT pg_size_pretty(count(*) * 8192) || ''''(Utilization: '''' || pg_size_pretty((SELECT setting FROM pg_settings WHERE name =''''shared_buffers'''') * 8192) || ''''/'''' || round(count(*)/(SELECT setting FROM pg_settings WHERE name =''''shared_buffers''''),2)*100 || ''''%)'''' FROM pg_buffercache_pages() WHERE relfilenode IS NOT NULL '''
        INTO shared_bcache;

        SharedBufferCache := shared_bcache;

        itr := 1;
        FOR row_data1 IN EXECUTE(query_str1) LOOP
          --We should have 3 return rows
          TopContext := row_data1.contextname1 || '(' || pg_size_pretty(row_data1.usedsize1) || ')';
          InnerTopCtxt := row_data1.usedsize1;
          IF itr = 1 THEN
            RETURN next;
          ELSE
            NName := '';
            UsedMem := '';
            SharedBufferCache := '';
            RETURN next;
          END IF;
          itr := itr + 1;
        END LOOP;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text)
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    total_num bigint;
    flag boolean;
    special text := '[;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN _table_name, IN special;
        IF flag = true THEN
            raise WARNING 'illegal character entered for function';
            RETURN;
        END IF;
        EXECUTE 'SELECT count(1) FROM ' || _table_name
            INTO total_num;

                --Get the node names
                query_str_nodes := 'SELECT node_name FROM pgxc_node, pgxc_class where node_type IN (''C'', ''D'') AND is_oid_in_group_members(oid, nodeoids) AND
                        pcrelid=''' || _table_name || '''::regclass::oid';

        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select ''''DN_name'''' as dnname1, count(1) as count1 from ' || $1 || '''';

            FOR row_data IN EXECUTE(query_str) LOOP
                row_data.dnname1 := CASE
                    WHEN LENGTH(row_name.node_name)<20
                    THEN row_name.node_name || right('                    ',20-length(row_name.node_name))
                    ELSE SUBSTR(row_name.node_name,1,20)
                    END;
                DNName := row_data.dnname1;
                Num := row_data.count1;
                IF total_num = 0 THEN
                Ratio := 0.000 ||'%';
                ELSE
                Ratio := ROUND(row_data.count1/total_num*100,3) || '%';
                END IF;
                RETURN next;
            END LOOP;
        END LOOP;

        RETURN;
    END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
BEGIN
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        IF FOUND THEN
            return next;
        END IF;
        CLOSE success_cursor;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
        LOOP
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
            EXIT WHEN NOT FOUND;
            IF mytype = 'login_failed' THEN
                return next;
            ELSE
            -- must be login_success
                EXIT;
            END IF;
        END LOOP;
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
BEGIN
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        IF FOUND THEN
            return next;
        END IF;
        CLOSE success_cursor;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
        LOOP
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
            EXIT WHEN NOT FOUND;
            IF mytype = 'login_failed' THEN
                return next;
            ELSE
            -- must be login_success
                EXIT;
            END IF;
        END LOOP;
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages_pid(flag boolean)
 RETURNS TABLE(username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text, backendid bigint) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
mybackendid bigint;
curSessionFound boolean;
BEGIN
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    SELECT pg_backend_pid() INTO mybackendid;
    curSessionFound = false;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            IF FOUND THEN
                return next;
            END IF;
        END IF;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid AND mytype = 'login_success' THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            LOOP
                FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid ;
                EXIT WHEN NOT FOUND;
                IF mytype = 'login_failed' THEN
                    return next;
                ELSE
                -- must be login_success
                    EXIT;
                END IF;
            END LOOP;
        END IF; --curSessionFound
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_thread_wait_status AS
    SELECT * FROM pg_stat_get_status(NULL);

create or replace function pg_catalog.pgxc_cgroup_map_ng_conf(IN ngname text)
returns bool
AS $$
declare
    host_info record;
    query_str text;
    query_string text;
    flag boolean;
    special text := '[^A-z0-9_]';
begin
    EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN ngname, IN special;
    IF flag = true THEN
        return false;
    ELSE
        query_str = 'SELECT node_name FROM pgxc_node WHERE node_type IN (''C'', ''D'') AND nodeis_active = true';
        for host_info in execute(query_str) loop
                query_string := 'execute direct on (' || host_info.node_name || ') ''SELECT * from gs_cgroup_map_ng_conf(''''' || ngname || ''''')''';
                execute query_string;
        end loop;
    END IF;
    return true;
end;
$$language plpgsql NOT FENCED;

create or replace function pg_catalog.table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    BEGIN
        if row_num = 0 then
            EXECUTE 'select count(1) from ' || table_name into tolal_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select table_data_skewness(row(' || column_name ||'), ''H'') as seqNum from ' || table_name ||
                            ' limit ' || row_num ||') group by seqNum order by num DESC';
        end if;

        if tolal_num = 0 then
            seqNum = 0;
            Num = 0;
            Ratio = ROUND(0, 3) || '%';
            return;
        end if;

        for row_data in EXECUTE execute_query loop
            seqNum = row_data.seqNum;
            Num = row_data.num;
            Ratio = ROUND(row_data.num / tolal_num * 100, 3) || '%';
            RETURN next;
        end loop;
    END;
$$LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_get_invalid_backends AS
    SELECT
            C.pid,
            C.node_name,
            S.datname AS dbname,
            S.backend_start,
            S.query
    FROM pg_pool_validate(false, ' ') AS C LEFT JOIN pg_stat_activity AS S
        ON (C.pid = S.sessionid);

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_statistics AS
SELECT t.*
FROM pg_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
where s.query_id = t.queryid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_history AS
SELECT * FROM pg_stat_get_wlm_operator_info(0);

CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_operator_info(IN flag int)
RETURNS int
AS $$
DECLARE
    query_ec_str text;
    query_str text;
    record_cnt int;
    BEGIN
        record_cnt := 0;

        query_ec_str := 'SELECT
                            queryid,
                            plan_node_id,
                            start_time,
                            duration,
                            tuple_processed,
                            min_peak_memory,
                            max_peak_memory,
                            average_peak_memory,
                            ec_status,
                            ec_execute_datanode,
                            ec_dsn,
                            ec_username,
                            ec_query,
                            ec_libodbc_type
                        FROM pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0';

        query_str := 'SELECT * FROM pg_stat_get_wlm_operator_info(1)';

        IF flag > 0 THEN
            EXECUTE 'INSERT INTO gs_wlm_ec_operator_info ' || query_ec_str;
            EXECUTE 'INSERT INTO gs_wlm_operator_info ' || query_str;
        ELSE
            EXECUTE query_ec_str;
            EXECUTE query_str;
        END IF;

        RETURN record_cnt;
    END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.gs_get_control_group_info()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    row_name record;
    query_str text;
    query_str_nodes text;
    BEGIN
        query_str_nodes := 'SELECT group_name,group_kind FROM pgxc_group WHERE group_kind = ''v'' OR group_kind = ''i'' ';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            IF row_name.group_kind = 'i' THEN
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM gs_all_nodegroup_control_group_info(''installation'')';
            ELSE
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM gs_all_nodegroup_control_group_info(''' ||row_name.group_name||''')';
            END IF;
            FOR row_data IN EXECUTE(query_str) LOOP
                return next row_data;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_ec_operator_statistics AS
SELECT
    t.queryid,
    t.plan_node_id,
    t.start_time,
    t.ec_status,
    t.ec_execute_datanode,
    t.ec_dsn,
    t.ec_username,
    t.ec_query,
    t.ec_libodbc_type,
    t.ec_fetch_count
FROM pg_stat_activity AS s, pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
where s.query_id = t.queryid and t.ec_operator > 0;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_ec_operator_history AS
SELECT
    queryid,
    plan_node_id,
    start_time,
    duration,
    tuple_processed,
    min_peak_memory,
    max_peak_memory,
    average_peak_memory,
    ec_status,
    ec_execute_datanode,
    ec_dsn,
    ec_username,
    ec_query,
    ec_libodbc_type
FROM pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_delta_info(IN rel TEXT, IN schema_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_info_str text;
    query_str text;
    query_part_str text;
    query_select_str text;
    query_size_str text;
    row_info_data record;
    row_data record;
    row_part_info record;
    BEGIN
        query_info_str := 'SELECT C.oid,C.reldeltarelid,C.parttype FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)  WHERE C.relname = '''|| rel ||''' and N.nspname = '''|| schema_name ||'''';
        FOR row_info_data IN EXECUTE(query_info_str) LOOP
        IF row_info_data.parttype = 'n' THEN
            query_str := 'SELECT relname,oid from pg_class where oid= '||row_info_data.reldeltarelid||'';
            EXECUTE(query_str) INTO row_data;
            query_select_str := 'select count(*) from cstore.' || row_data.relname || '';
            EXECUTE (query_select_str) INTO live_tuple;
            query_size_str := 'select * from pg_relation_size(' || row_data.oid || ')';
            EXECUTE (query_size_str) INTO data_size;
            blockNum := data_size/8192;
            part_name := 'non partition table';
            return next;
        ELSE
            query_part_str := 'SELECT relname,reldeltarelid from pg_partition where parentid = '||row_info_data.oid|| 'and relname <> '''||rel||'''';
            FOR row_part_info IN EXECUTE(query_part_str) LOOP
                query_str := 'SELECT relname,oid from pg_class where  oid = '||row_part_info.reldeltarelid||'';
                part_name := row_part_info.relname;
                FOR row_data IN EXECUTE(query_str) LOOP
                    query_select_str := 'select count(*) from cstore.' || row_data.relname || '';
                    EXECUTE (query_select_str) INTO live_tuple;
                    query_size_str := 'select * from pg_relation_size(' || row_data.oid || ')';
                    EXECUTE (query_size_str) INTO data_size;
                END LOOP;
                blockNum := data_size/8192;
                return next;
            END LOOP;
        END IF;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_delta_info(IN rel TEXT, OUT node_name TEXT, OUT part_name TEXT, OUT live_tuple INT8, OUT data_size INT8, OUT blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str_nodes text;
    query_str text;
    query_part_str text;
    rel_info record;
    row_name record;
    row_data record;
    BEGIN
        EXECUTE ('select c.relname,n.nspname from pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) where C.oid = (select '''|| rel ||'''::regclass::oid)') into rel_info;
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''D''';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_get_delta_info('''''||rel_info.relname||''''','''''||rel_info.nspname||''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                node_name := row_name.node_name;
                live_tuple := row_data.live_tuple;
                data_size := row_data.data_size;
                blockNum := row_data.blockNum;
                part_name := row_data.part_name;
                return next;
            END LOOP;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.get_delta_info(IN rel TEXT, OUT part_name TEXT, OUT total_live_tuple INT8, OUT total_data_size INT8, OUT max_blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'select part_name, sum(live_tuple) total_live_tuple, sum(data_size) total_data_size, max(blocknum) max_blocknum from (select * from pgxc_get_delta_info('''||rel||''')) group by part_name order by max_blocknum desc';
        FOR row_data IN EXECUTE(query_str) LOOP
            part_name := row_data.part_name;
            total_live_tuple := row_data.total_live_tuple;
            total_data_size := row_data.total_data_size;
            max_blockNum := row_data.max_blocknum;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION pg_catalog.pgxc_query_audit;
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_query_audit(IN starttime TIMESTAMPTZ, IN endtime TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT "type" text, OUT "result" text, OUT userid text, OUT username text, OUT database text, OUT client_conninfo text, OUT "object_name" text, OUT detail_info text, OUT node_name text, OUT thread_id text, OUT local_port text, OUT remote_port text)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    row_data record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get the node names of all CNs
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_query_audit(''''' || starttime || ''''',''''' || endtime || ''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                "time"   := row_data."time";
                "type"   := row_data."type";
                "result" := row_data."result";
                userid := row_data.userid;
                username := row_data.username;
                database := row_data.database;
                client_conninfo := row_data.client_conninfo;
                "object_name"   := row_data."object_name";
                detail_info := row_data.detail_info;
                node_name   := row_data.node_name;
                thread_id   := row_data.thread_id;
                local_port  := row_data.local_port;
                remote_port := row_data.remote_port;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.is_oid_in_group_members(IN node_oid Oid, IN group_members oidvector_extend)
RETURNS BOOLEAN AS $$
DECLARE
query_str text;
match_count int;
node_list text;
BEGIN
    match_count := 0;
    select array_to_string($2, ',') into node_list;

    query_str := 'SELECT count(1) FROM pgxc_node n WHERE n.oid = ' || node_oid  || ' AND n.node_name IN (SELECT n1.node_name FROM pgxc_node n1 WHERE n1.oid in (' || node_list || '))';
    EXECUTE query_str into match_count;

    IF match_count = 1 THEN
        RETURN true;
    END IF;
    RETURN false;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.lock_cluster_ddl()
RETURNS boolean
AS $$
DECLARE
    databse_name record;
    lock_str text;
    query_database_oid text;
    lock_result  boolean = false;
    return_result  bool = true;
    BEGIN
        query_database_oid := 'SELECT datname FROM pg_database WHERE datallowconn = true order by datname';
        for databse_name in EXECUTE(query_database_oid) LOOP
            lock_str = format('SELECT * FROM pgxc_lock_for_sp_database(''%s'')', databse_name.datname);
            begin
                EXECUTE(lock_str) into lock_result;
                if lock_result = 'f' then
                    return_result = false;
                    return return_result;
                end if;
            end;
        end loop;
        return return_result;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.unlock_cluster_ddl()
RETURNS bool
AS $$
DECLARE
    databse_name record;
    unlock_str text;
    query_database_oid text;
    unlock_result  boolean = false;
    return_result  bool = true;
    BEGIN
        query_database_oid := 'SELECT datname FROM pg_database WHERE datallowconn = true order by datname';
        for databse_name in EXECUTE(query_database_oid) LOOP
            unlock_str = format('SELECT * FROM pgxc_unlock_for_sp_database(''%s'')', databse_name.datname);
            begin
                EXECUTE(unlock_str) into unlock_result;
                if unlock_result = 'f' then
                    return_result = false;
                    return return_result;
                end if;
            end;
        end loop;
        return return_result;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
                        FROM pg_class p,
                        (SELECT  relname, schemaname, SUM(n_tup_ins) n_tup_ins, SUM(n_tup_upd) n_tup_upd, SUM(n_tup_del) n_tup_del, SUM(n_live_tup) n_live_tup, SUM(n_dead_tup) n_dead_tup, CAST((SUM(n_dead_tup) / SUM(n_dead_tup + n_live_tup + 0.00001) * 100)
                        AS NUMERIC(5,2)) dirty_page_rate FROM pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||') GROUP BY (relname,schemaname)) s
                        WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
        FOR row_data IN EXECUTE(query_str) LOOP
            relid = row_data.relid;
            relname = row_data.relname;
            schemaname = row_data.schemaname;
            n_tup_ins = row_data.n_tup_ins;
            n_tup_upd = row_data.n_tup_upd;
            n_tup_del = row_data.n_tup_del;
            n_live_tup = row_data.n_live_tup;
            n_dead_tup = row_data.n_dead_tup;
            dirty_page_rate = row_data.dirty_page_rate;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4,in schema text, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
                        FROM pg_class p,
                        (SELECT  relname, schemaname, SUM(n_tup_ins) n_tup_ins, SUM(n_tup_upd) n_tup_upd, SUM(n_tup_del) n_tup_del, SUM(n_live_tup) n_live_tup, SUM(n_dead_tup) n_dead_tup, CAST((SUM(n_dead_tup) / SUM(n_dead_tup + n_live_tup + 0.00001) * 100)
                        AS NUMERIC(5,2)) dirty_page_rate FROM pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||','''||schema||''') GROUP BY (relname,schemaname)) s
                        WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
        FOR row_data IN EXECUTE(query_str) LOOP
            relid = row_data.relid;
            relname = row_data.relname;
            schemaname = row_data.schemaname;
            n_tup_ins = row_data.n_tup_ins;
            n_tup_upd = row_data.n_tup_upd;
            n_tup_del = row_data.n_tup_del;
            n_live_tup = row_data.n_live_tup;
            n_dead_tup = row_data.n_dead_tup;
            dirty_page_rate = row_data.dirty_page_rate;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                        WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE T.session_respool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.pid = T.threadid AND
            S.sessionid = N.sessionid AND
            S.pid = N.pid;

drop function if exists pg_catalog.login_audit_messages(boolean);
drop function if exists pg_catalog.login_audit_messages_pid(boolean);

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
BEGIN
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        IF FOUND THEN
            return next;
        END IF;
        CLOSE success_cursor;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
        LOOP
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
            EXIT WHEN NOT FOUND;
            IF mytype = 'login_failed' THEN
                return next;
            ELSE
            -- must be login_success
                EXIT;
            END IF;
        END LOOP;
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages_pid(flag boolean)
 RETURNS TABLE(username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text, backendid bigint) AUTHID DEFINER
AS $$
DECLARE
user_id text;
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
mybackendid bigint;
curSessionFound boolean;
BEGIN
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    SELECT pg_backend_pid() INTO mybackendid;
    curSessionFound = false;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            IF FOUND THEN
                return next;
            END IF;
        END IF;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ' AND userid =' || quote_literal(user_id) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid AND mytype = 'login_success' THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            LOOP
                FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid ;
                EXIT WHEN NOT FOUND;
                IF mytype = 'login_failed' THEN
                    return next;
                ELSE
                -- must be login_success
                    EXIT;
                END IF;
            END LOOP;
        END IF; --curSessionFound
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_pg_asp
     (in start_ts timestamp without time zone,
      in end_ts timestamp without time zone,
      OUT node_name text,
      OUT sampleid bigint,
      OUT sample_time timestamp without time zone,
      OUT need_flush_sample boolean,
      OUT databaseid oid,
      OUT thread_id bigint,
      OUT sessionid bigint,
      OUT start_time timestamp without time zone,
      OUT event text,
      OUT lwtid integer,
      OUT psessionid bigint,
      OUT tlevel integer,
      OUT smpid integer,
      OUT userid oid,
      OUT application_name text,
      OUT client_addr inet,
      OUT client_hostname text,
      OUT client_port integer,
      OUT query_id bigint,
      OUT unique_query_id bigint,
      OUT user_id oid,
      OUT cn_id integer,
      OUT unique_query text) cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_datanode_active_session_hist
     (in datanode text,
      OUT node_name text,
      OUT sampleid bigint,
      OUT sample_time timestamp without time zone,
      OUT need_flush_sample boolean,
      OUT databaseid oid,
      OUT thread_id bigint,
      OUT sessionid bigint,
      OUT start_time timestamp without time zone,
      OUT event text,
      OUT lwtid integer,
      OUT psessionid bigint,
      OUT tlevel integer,
      OUT smpid integer,
      OUT userid oid,
      OUT application_name text,
      OUT client_addr inet,
      OUT client_hostname text,
      OUT client_port integer,
      OUT query_id bigint,
      OUT unique_query_id bigint,
      OUT user_id oid,
      OUT cn_id integer,
      OUT unique_query text) cascade;
    DROP FUNCTION IF EXISTS DBE_PERF.get_datanode_active_session_hist
     (in datanode text,
      in start_ts timestamp without time zone,
      in end_ts timestamp without time zone,
      OUT node_name text,
      OUT sampleid bigint,
      OUT sample_time timestamp without time zone,
      OUT need_flush_sample boolean,
      OUT databaseid oid,
      OUT thread_id bigint,
      OUT sessionid bigint,
      OUT start_time timestamp without time zone,
      OUT event text,
      OUT lwtid integer,
      OUT psessionid bigint,
      OUT tlevel integer,
      OUT smpid integer,
      OUT userid oid,
      OUT application_name text,
      OUT client_addr inet,
      OUT client_hostname text,
      OUT client_port integer,
      OUT query_id bigint,
      OUT unique_query_id bigint,
      OUT user_id oid,
      OUT cn_id integer,
      OUT unique_query text) cascade;
    CREATE OR REPLACE FUNCTION DBE_PERF.get_datanode_active_session_hist
      (in datanode text,
       in start_ts timestamp without time zone,
       in end_ts timestamp without time zone,
       OUT node_name text,
       OUT sampleid bigint,
       OUT sample_time timestamp without time zone,
       OUT need_flush_sample boolean,
       OUT databaseid oid,
       OUT thread_id bigint,
       OUT sessionid bigint,
       OUT start_time timestamp without time zone,
       OUT event text,
       OUT lwtid integer,
       OUT psessionid bigint,
       OUT tlevel integer,
       OUT smpid integer,
       OUT userid oid,
       OUT application_name text,
       OUT client_addr inet,
       OUT client_hostname text,
       OUT client_port integer,
       OUT query_id bigint,
       OUT unique_query_id bigint,
       OUT user_id oid,
       OUT cn_id integer,
       OUT unique_query text)
    RETURNS SETOF record
    AS $$
    DECLARE
        ROW_DATA pg_catalog.gs_asp%ROWTYPE;
        cn_name record;
        query_text record;
        QUERY_STR text;
        query_cn text;
        query_str_cn_name text;
        node_str text;
    BEGIN
        --Get all the node names
        node_str := 'SELECT * FROM pg_catalog.gs_asp where sample_time > '''''||$2||''''' and sample_time < '''''||$3||'''';
        QUERY_STR := 'EXECUTE DIRECT ON (' || $1 || ') '''|| node_str ||''''';';
        FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
          node_name := datanode;
          sampleid := ROW_DATA.sampleid;
          sample_time := ROW_DATA.sample_time;
          need_flush_sample := ROW_DATA.need_flush_sample;
          databaseid := ROW_DATA.databaseid;
          thread_id := ROW_DATA.thread_id;
          sessionid := ROW_DATA.sessionid;
          start_time := ROW_DATA.start_time;
          event := ROW_DATA.event;
          lwtid := ROW_DATA.lwtid;
          psessionid := ROW_DATA.psessionid;
          tlevel := ROW_DATA.tlevel;
          smpid := ROW_DATA.smpid;
          userid := ROW_DATA.userid;
          application_name := ROW_DATA.application_name;
          client_addr := ROW_DATA.client_addr;
          client_hostname := ROW_DATA.client_hostname;
          client_port := ROW_DATA.client_port;
          query_id := ROW_DATA.query_id;
          unique_query_id := ROW_DATA.unique_query_id;
          user_id := ROW_DATA.user_id;
          cn_id := ROW_DATA.cn_id;
          IF ROW_DATA.cn_id IS NULL THEN
            unique_query := ROW_DATA.unique_query;
          ELSE
            query_str_cn_name := 'SELECT node_name FROM pgxc_node WHERE node_id='||ROW_DATA.cn_id||' AND nodeis_active = true';
            FOR cn_name IN EXECUTE(query_str_cn_name) LOOP
                    query_cn := 'EXECUTE DIRECT ON (' || cn_name.node_name || ') ''SELECT unique_query FROM pg_catalog.gs_asp where unique_query_id = '|| ROW_DATA.unique_query_id||' limit 1''';
                    FOR query_text IN EXECUTE(query_cn) LOOP
                              unique_query := query_text.unique_query;
                    END LOOP;
            END LOOP;
          END IF;
          RETURN NEXT;
        END LOOP;
        RETURN;
    END; $$
    LANGUAGE 'plpgsql';
   end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_activity_timeout() cascade;
DROP FUNCTION IF EXISTS pg_catalog.global_stat_activity_timeout() cascade;

DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS dbe_perf.gs_stat_activity_timeout() cascade;
    DROP FUNCTION IF EXISTS dbe_perf.global_stat_activity_timeout() cascade;
    end if;
END $DO$;
drop function if exists pg_catalog.login_audit_messages;
drop function if exists pg_catalog.login_audit_messages_pid;
drop function if exists pg_catalog.pg_query_audit(timestamptz, timestamptz) cascade;
drop function if exists pg_catalog.pg_query_audit(timestamptz, timestamptz, text) cascade;
drop function if exists pg_catalog.pgxc_query_audit(timestamptz, timestamptz) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3780;
create function pg_catalog.pg_query_audit(TIMESTAMPTZ, TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT type TEXT, OUT result TEXT, OUT username TEXT, OUT database TEXT, OUT client_conninfo TEXT, OUT object_name TEXT, OUT detail_info TEXT, OUT node_name TEXT, OUT thread_id TEXT, OUT local_port TEXT, OUT remote_port TEXT) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 100 STRICT as 'pg_query_audit';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3782;
create function pg_catalog.pg_query_audit(TIMESTAMPTZ, TIMESTAMPTZ, TEXT, OUT "time" TIMESTAMPTZ, OUT type TEXT, OUT result TEXT, OUT username TEXT, OUT database TEXT, OUT client_conninfo TEXT, OUT object_name TEXT, OUT detail_info TEXT, OUT node_name TEXT, OUT thread_id TEXT, OUT local_port TEXT, OUT remote_port TEXT) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 100 STRICT as 'pg_query_audit';

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages(in flag boolean) returns table (username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text) AUTHID DEFINER
AS $$
DECLARE
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
BEGIN
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo;
        IF FOUND THEN
            return next;
        END IF;
        CLOSE success_cursor;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
        LOOP
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo;
            EXIT WHEN NOT FOUND;
            IF mytype = 'login_failed' THEN
                return next;
            ELSE
            -- must be login_success
                EXIT;
            END IF;
        END LOOP;
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.login_audit_messages_pid(flag boolean)
 RETURNS TABLE(username text, database text, logintime timestamp with time zone, mytype text, result text, client_conninfo text, backendid bigint) AUTHID DEFINER
AS $$
DECLARE
user_name text;
db_name text;
SQL_STMT VARCHAR2(500);
fail_cursor REFCURSOR;
success_cursor REFCURSOR;
mybackendid bigint;
curSessionFound boolean;
BEGIN
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    SELECT pg_backend_pid() INTO mybackendid;
    curSessionFound = false;
    IF flag = true THEN
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ';';
        OPEN success_cursor FOR EXECUTE SQL_STMT;
        --search bottom up for all the success login info
        FETCH LAST FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            FETCH BACKWARD FROM success_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            IF FOUND THEN
                return next;
            END IF;
        END IF;
    ELSE
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE
                    type IN (''login_success'', ''login_failed'') AND client_conninfo not LIKE ''gs_clean_%'' AND client_conninfo not LIKE ''pgxc_%'' AND username =' || quote_literal(user_name) ||
                    ' AND database =' || quote_literal(db_name) || ';';
        OPEN fail_cursor FOR EXECUTE SQL_STMT;
        --search bottom up
        FETCH LAST FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
        LOOP
            IF backendid = mybackendid AND mytype = 'login_success' THEN
                --found the login info for the current session
                curSessionFound = true;
                EXIT;
            END IF;
            FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid;
            EXIT WHEN NOT FOUND;
        END LOOP;
        IF curSessionFound THEN
            LOOP
                FETCH BACKWARD FROM fail_cursor into username, database, logintime, mytype, result, client_conninfo, backendid ;
                EXIT WHEN NOT FOUND;
                IF mytype = 'login_failed' THEN
                    return next;
                ELSE
                -- must be login_success
                    EXIT;
                END IF;
            END LOOP;
        END IF; --curSessionFound
        CLOSE fail_cursor;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;

--get audit log from all CNs
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_query_audit(IN starttime TIMESTAMPTZ, IN endtime TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT "type" text, OUT "result" text, OUT username text, OUT database text, OUT client_conninfo text, OUT "object_name" text, OUT detail_info text, OUT node_name text, OUT thread_id text, OUT local_port text, OUT remote_port text)
RETURNS setof record
AS $$
DECLARE
    row_name record;
    row_data record;
    query_str text;
    query_str_nodes text;
    BEGIN
        --Get the node names of all CNs
        query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_query_audit(''''' || starttime || ''''',''''' || endtime || ''''')''';
            FOR row_data IN EXECUTE(query_str) LOOP
                "time"   := row_data."time";
                "type"   := row_data."type";
                "result" := row_data."result";
                username := row_data.username;
                database := row_data.database;
                client_conninfo := row_data.client_conninfo;
                "object_name"   := row_data."object_name";
                detail_info := row_data.detail_info;
                node_name   := row_data.node_name;
                thread_id   := row_data.thread_id;
                local_port  := row_data.local_port;
                remote_port := row_data.remote_port;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
			WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
			ELSE T.session_respool
			END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_activity_ng AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.pid,
            S.sessionid,
            S.usesysid,
            U.rolname AS usename,
            S.application_name,
            S.client_addr,
            S.client_hostname,
            S.client_port,
            S.backend_start,
            S.xact_start,
            S.query_start,
            S.state_change,
            S.waiting,
            S.enqueue,
            S.state,
            CASE
                        WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE T.session_respool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            S.pid = T.threadid AND
            S.sessionid = N.sessionid AND
            S.pid = N.pid;

CREATE OR REPLACE VIEW pg_catalog.pg_session_wlmstat AS
    SELECT
            S.datid AS datid,
            D.datname AS datname,
            S.threadid,
            S.sessionid,
            S.threadpid AS processid,
            S.usesysid,
            S.appname,
            U.rolname AS usename,
            S.priority,
            S.attribute,
            S.block_time,
            S.elapsed_time,
            S.total_cpu_time,
            S.skew_percent AS cpu_skew_percent,
            S.statement_mem,
            S.active_points,
            S.dop_value,
            S.current_cgroup AS control_group,
            S.current_status AS status,
            S.enqueue_state AS enqueue,
            CASE
                WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
                ELSE T.session_respool
                END AS resource_pool,
            S.query,
            S.is_plana,
            S.node_group
    FROM pg_database D, pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
            T.threadid = S.threadid;

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    drop view if exists dbe_perf.wlm_user_resource_runtime cascade;

    CREATE OR REPLACE FUNCTION DBE_PERF.get_wlm_user_resource_runtime
      (OUT userid oid, OUT used_memory integer, OUT total_memory integer,
       OUT used_cpu integer, OUT total_cpu integer, OUT used_space bigint,
       OUT total_space bigint, OUT used_temp_space bigint, OUT total_temp_space bigint,
       OUT used_spill_space bigint, OUT total_spill_space bigint)
    RETURNS setof record
    AS $$
    DECLARE
      row_data record;
      row_name record;
      query_str text;
      query_str2 text;
      BEGIN
        query_str := 'SELECT rolname FROM pg_authid';
        FOR row_name IN EXECUTE(query_str) LOOP
          query_str2 := 'SELECT * FROM gs_wlm_user_resource_info(''' || row_name.rolname || ''')';
          FOR row_data IN EXECUTE(query_str2) LOOP
            userid = row_data.userid;
            used_memory = row_data.used_memory;
            total_memory = row_data.total_memory;
            used_cpu = row_data.used_cpu;
            total_cpu = row_data.total_cpu;
            used_space = row_data.used_space;
            total_space = row_data.total_space;
            used_temp_space = row_data.used_temp_space;
            total_temp_space = row_data.total_temp_space;
            used_spill_space = row_data.used_spill_space;
            total_spill_space = row_data.total_spill_space;
            return next;
          END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.wlm_user_resource_runtime AS
      SELECT
        S.usename AS username,
        T.used_memory,
        T.total_memory,
        T.used_cpu,
        T.total_cpu,
        T.used_space,
        T.total_space,
        T.used_temp_space,
        T.total_temp_space,
        T.used_spill_space,
        T.total_spill_space
      FROM pg_user AS S join (select * from DBE_PERF.get_wlm_user_resource_runtime()) AS T
        on S.usesysid = T.userid;

    select session_user into user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.wlm_user_resource_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON DBE_PERF.wlm_user_resource_runtime TO PUBLIC;
  end if;
END$DO$;

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select relname from pg_class where relname='gs_asp_sample_time_index' limit 1) into ans;
  if ans = true then
    DROP INDEX IF EXISTS pg_catalog.gs_asp_sample_time_index;
  end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select relname from pg_class where relname='gs_asp' limit 1) into ans;
  if ans = true then
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2998;
    CREATE UNIQUE INDEX gs_asp_sample_time_index ON pg_catalog.gs_asp USING BTREE(sample_time timestamptz_ops);
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
  end if;
END$$;

CREATE OR REPLACE FUNCTION pg_catalog.remove_create_partition_policy(
                  IN        relationname         name,
                  IN        username             name = NULL
)
RETURNS integer
AS
$$
DECLARE
    sql                 text;
    check_count         int;
    job_id              int;
    current_username    name;
    is_admin            bool = false;
    user_logic_flag     bool = true;
    database            name;
    namespace           name;
    table_name          name;
    pos                 integer;
BEGIN
    /* check tsdb_enable */
    IF check_engine_enable() = false
    THEN
        RAISE EXCEPTION 'tsdb engine is not exist';
    END IF;
    /* check tsdb_enable end */

    /* check parameter */
    IF relationname is null THEN
        RAISE EXCEPTION 'parameter ''relationname'' cannot be null';
    END IF;
    pos = is_contain_namespace(relationname);
    IF pos != 0 THEN
        namespace = substring(relationname from 1 for pos -1);
        table_name = substring(relationname from pos+1 for char_length(relationname) - pos);
    END IF;
    /* check parameter end */

    /* check user or admin */
    sql := 'select current_user';
    EXECUTE(sql) into current_username;
    is_admin = is_super_user_or_sysadm(current_username);

    IF is_admin is false AND username is not null AND lower(current_username) != lower(username) THEN
        RAISE EXCEPTION 'user is not allowed to set the second parameter ''username'' unless their owned name';
    END IF;

    IF is_admin is true AND username is not null THEN
        user_logic_flag = false;
        sql := 'select count(*) from pg_user where usename = lower(''' || username || ''');';
        EXECUTE sql into check_count;
        IF check_count = 0 THEN
            RAISE EXCEPTION  'please input the correct username!';
        END IF;
    END IF;
    /* check user or admin end */

    /* check table exist */
    IF user_logic_flag is true THEN
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || relationname || '''
                AND pg_catalog.pg_table_is_visible(c.oid);';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || table_name || '''
                AND n.nspname = ''' || namespace || ''';';
        END IF;
    ELSE
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || relationname || '''
                AND pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || table_name || '''
                AND n.nspname = ''' || namespace || ''' AND pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
        END IF;
    END IF;
    EXECUTE sql into check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  'please input the correct relation name!';
    END IF;
    /* check table exist end */

    /* check table owner privilege */
    IF user_logic_flag is true THEN
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =
                ''' || relationname || '''  AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || current_username || ''';';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =
                ''' || table_name || '''  AND n.nspname = ''' || namespace || ''' and pg_catalog.pg_get_userbyid(c.relowner) = ''' || current_username || ''';';
        END IF;
        EXECUTE sql into check_count;
        IF check_count = 0 THEN
            RAISE EXCEPTION 'permission denied for relation %', relationname
            USING HINT = 'please assure you have the privilege';
        END IF;
    END IF;
    /* check table owner privilege end */

    /* check add partition policy exists */
    sql = 'select current_database();';
    EXECUTE sql INTO database;
    IF user_logic_flag is true THEN
        sql := 'select count(*) from pg_job a, pg_job_proc b where b.what like ''%proc_add_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| current_username ||''' and dbname = ''' || database || ''';';
    ELSE
        sql = 'select count(*) from pg_job a, pg_job_proc b where b.what like ''%proc_add_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| username ||''' and dbname = ''' || database || ''';';
    END IF;

    EXECUTE sql into check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  ' % does not have create policy!',relationname;
    END IF;
    /* check add partition policy exists end */

    IF user_logic_flag is true THEN
        sql := 'select a.job_id from pg_job a, pg_job_proc b where b.what like ''%proc_add_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| current_username ||'''  and dbname = ''' || database || ''';';
    ELSE
        sql = 'select a.job_id from pg_job a, pg_job_proc b where b.what like ''%proc_add_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| username ||'''  and dbname = ''' || database || ''';';
    END IF;
    EXECUTE sql into job_id;
    sql := 'SELECT remove_job_class_depend(' || job_id || ')';
    EXECUTE sql;
    sql := 'SELECT DBE_TASK.cancel('||job_id||')';
    EXECUTE(sql);

    RETURN job_id;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION is_super_user_or_sysadm(
 IN i_user name
)
RETURNS bool
AS
$$
BEGIN
    IF (
    select count(*) from pg_roles where rolname = i_user and (rolsystemadmin=true or rolsuper=true)
    ) < 1
    THEN
        RETURN FALSE;
    ELSE
        RETURN TRUE;
    END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_catalog.remove_drop_partition_policy(
                  IN        relationname         name,
                  IN        username             name=null
)
RETURNS integer
AS
$$
DECLARE
    sql                text;
    check_count        int;
    job_id             int;
    table_oid          int;
    current_username   name;
    is_admin           bool = false;
    user_logic_flag    bool = true;
    database           name;
    namespace          name;
    table_name         name;
    pos                integer;
BEGIN
    /* check tsdb_enable */
    IF check_engine_enable() = false
    THEN
        RAISE EXCEPTION 'tsdb engine is not exist';
    END IF;
    /* check tsdb_enable end */

    /* check parameter */
    IF relationname is null THEN
        RAISE EXCEPTION 'parameter ''relationname'' cannot be null';
    END IF;
    pos = is_contain_namespace(relationname);
    IF pos != 0 THEN
        namespace = substring(relationname from 1 for pos -1);
        table_name = substring(relationname from pos+1 for char_length(relationname) - pos);
    END IF;
    /* check parameter end */

    /* check user or admin */
    sql := 'select current_user';
    EXECUTE(sql) into current_username;
    is_admin = is_super_user_or_sysadm(current_username);

    IF is_admin is false AND username is not null AND lower(current_username) != lower(username) THEN
        RAISE EXCEPTION 'user is not allowed to set the second parameter ''username'' unless their owned name';
    END IF;

    IF is_admin is true AND username is not null THEN
        user_logic_flag = false;
        sql := 'select count(*) from pg_user where usename = lower(''' || username || ''');';
        EXECUTE sql into check_count;
        IF check_count = 0 THEN
            RAISE EXCEPTION  'please input the correct username!';
        END IF;
    END IF;
    /* check user or admin end */

    /* check table exist */
    IF user_logic_flag is true THEN
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || relationname || '''
                AND pg_catalog.pg_table_is_visible(c.oid);';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || table_name || '''
                AND n.nspname = ''' || namespace || ''';';
        END IF;
    ELSE
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || relationname || '''
                AND pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =''' || table_name || '''
                AND n.nspname = ''' || namespace || ''' AND pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';

        END IF;
    END IF;
    EXECUTE sql into check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  'please input the correct relation name!';
    END IF;
    /* check table exist end */

    /* check table owner privilege */
    IF user_logic_flag is true THEN
        IF pos = 0 THEN
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =
                ''' || relationname || '''  AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || current_username || ''';';
        ELSE
            sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname =
                ''' || table_name || '''  AND n.nspname = ''' || namespace || ''' and pg_catalog.pg_get_userbyid(c.relowner) = ''' || current_username || ''';';
        END IF;
        EXECUTE sql into check_count;
        IF check_count = 0 THEN
            RAISE EXCEPTION 'permission denied for relation %', relationname
            USING HINT = 'please assure you have the privilege';
        END IF;
    END IF;
    /* check table owner privilege end */

    sql = 'select current_database();';
    EXECUTE sql INTO database;
    IF user_logic_flag is true THEN
        sql := 'select count(*) from pg_job a, pg_job_proc b where b.what like ''%proc_drop_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| current_username ||''' and dbname = ''' || database || ''';';
    ELSE
        sql = 'select count(*) from pg_job a, pg_job_proc b where b.what like ''%proc_drop_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| username ||''' and dbname = ''' || database || ''';';
    END IF;

    EXECUTE sql into check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  ' % does not have drop policy!',relationname;
    END IF;

    IF user_logic_flag is true THEN
        sql = 'select a.job_id from pg_job a, pg_job_proc b where b.what like ''%proc_drop_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| current_username ||''' and dbname = ''' || database || ''';';
    ELSE
        sql = 'select a.job_id from pg_job a, pg_job_proc b where b.what like ''%proc_drop_partition(''''' || relationname || '''''%''
        and a.job_id = b.job_id and a.priv_user='''|| username ||''' and dbname = ''' || database || ''';';
    END IF;
    EXECUTE sql into job_id;
    sql := 'SELECT remove_job_class_depend(' || job_id || ')';
    EXECUTE sql;
    sql := 'SELECT DBE_TASK.cancel('||job_id||')';
    EXECUTE(sql);

    RETURN job_id ;
END;
$$
LANGUAGE plpgsql;

DO $$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS dbe_perf.gs_stat_activity_timeout() cascade;
    DROP FUNCTION IF EXISTS dbe_perf.global_stat_activity_timeout() cascade;
  end if;
END$$;

DROP EXTENSION IF EXISTS security_plugin CASCADE;

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select relname from pg_class where relname='pg_asp' limit 1) into ans;
  if ans = true then
    DROP INDEX IF EXISTS pg_catalog.pg_asp_oid_index;
    DROP TYPE IF EXISTS pg_catalog.pg_asp;
    DROP TABLE IF EXISTS pg_catalog.pg_asp;
  end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9027, 84, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.pg_asp
 (sampleid bigint nocompress not null,
  sample_time timestamp with time zone nocompress not null,
  need_flush_sample boolean nocompress not null,
  databaseid oid nocompress not null,
  thread_id bigint nocompress not null,
  sessionid bigint nocompress not null,
  start_time timestamp with time zone nocompress not null,
  event text nocompress,
  lwtid integer nocompress,
  psessionid bigint nocompress,
  tlevel integer nocompress,
  smpid integer nocompress,
  userid oid nocompress,
  application_name text nocompress,
  client_addr inet nocompress,
  client_hostname text nocompress,
  client_port integer nocompress,
  query_id bigint nocompress,
  unique_query_id bigint nocompress,
  user_id oid nocompress,
  cn_id integer nocompress,
  unique_query text nocompress
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2997;
CREATE UNIQUE INDEX pg_asp_oid_index ON pg_catalog.pg_asp USING BTREE(oid oid_ops);
GRANT SELECT ON pg_catalog.pg_asp TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_pg_asp
      (in start_ts timestamp without time zone,
       in end_ts timestamp without time zone,
       OUT node_name text,
       OUT sampleid bigint,
       OUT sample_time timestamp without time zone,
       OUT need_flush_sample boolean,
       OUT databaseid oid,
       OUT thread_id bigint,
       OUT sessionid bigint,
       OUT start_time timestamp without time zone,
       OUT event text,
       OUT lwtid integer,
       OUT psessionid bigint,
       OUT tlevel integer,
       OUT smpid integer,
       OUT userid oid,
       OUT application_name text,
       OUT client_addr inet,
       OUT client_hostname text,
       OUT client_port integer,
       OUT query_id bigint,
       OUT unique_query_id bigint,
       OUT user_id oid,
       OUT cn_id integer,
       OUT unique_query text)
    RETURNS SETOF record
    AS $$
    DECLARE
      ROW_DATA pg_catalog.pg_asp%ROWTYPE;
      ROW_NAME RECORD;
      QUERY_STR TEXT;
      QUERY_STR_NODES TEXT;
      node_str TEXT;
    BEGIN
      QUERY_STR_NODES := 'SELECT NODE_NAME FROM PGXC_NODE WHERE NODE_TYPE IN (''C'', ''D'')';
      node_str := 'SELECT * FROM pg_catalog.pg_asp where sample_time > '''''||$1||''''' and sample_time < '''''||$2||'''';
      FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
        QUERY_STR := 'EXECUTE DIRECT ON (' || ROW_NAME.NODE_NAME || ') '''|| node_str ||''''';';
        FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
              node_name := ROW_NAME.NODE_NAME;
              sampleid := ROW_DATA.sampleid;
              sample_time := ROW_DATA.sample_time;
              need_flush_sample := ROW_DATA.need_flush_sample;
              databaseid := ROW_DATA.databaseid;
              thread_id := ROW_DATA.thread_id;
              sessionid := ROW_DATA.sessionid;
              start_time := ROW_DATA.start_time;
              event := ROW_DATA.event;
              lwtid := ROW_DATA.lwtid;
              psessionid := ROW_DATA.psessionid;
              tlevel := ROW_DATA.tlevel;
              smpid := ROW_DATA.smpid;
              userid := ROW_DATA.userid;
              application_name := ROW_DATA.application_name;
              client_addr := ROW_DATA.client_addr;
              client_hostname := ROW_DATA.client_hostname;
              client_port := ROW_DATA.client_port;
              query_id := ROW_DATA.query_id;
              unique_query_id := ROW_DATA.unique_query_id;
              user_id := ROW_DATA.user_id;
              cn_id := ROW_DATA.cn_id;
              unique_query := ROW_DATA.unique_query;
          RETURN NEXT;
        END LOOP;
      END LOOP;
      RETURN;
    END; $$
    LANGUAGE 'plpgsql';
  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
	select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
	if ans = true then
		DROP VIEW IF EXISTS DBE_PERF.global_pg_asp cascade;
		DROP FUNCTION IF EXISTS DBE_PERF.get_global_pg_asp() cascade;
		DROP FUNCTION IF EXISTS DBE_PERF.get_datanode_active_session_hist() cascade;
		CREATE OR REPLACE FUNCTION DBE_PERF.get_datanode_active_session_hist
		(  in datanode text,
		   OUT node_name text,
		   OUT sampleid bigint,
		   OUT sample_time timestamp without time zone,
		   OUT need_flush_sample boolean,
		   OUT databaseid oid,
		   OUT thread_id bigint,
		   OUT sessionid bigint,
		   OUT start_time timestamp without time zone,
		   OUT event text,
		   OUT lwtid integer,
		   OUT psessionid bigint,
		   OUT tlevel integer,
		   OUT smpid integer,
		   OUT userid oid,
		   OUT application_name text,
		   OUT client_addr inet,
		   OUT client_hostname text,
		   OUT client_port integer,
		   OUT query_id bigint,
		   OUT unique_query_id bigint,
		   OUT user_id oid,
		   OUT cn_id integer,
		   OUT unique_query text)
		RETURNS SETOF record
		AS $$
		DECLARE
				ROW_DATA pg_catalog.pg_asp%ROWTYPE;
				cn_name record;
				query_text record;
				QUERY_STR text;
				query_cn text;
				query_str_cn_name text;
		BEGIN
				--Get all the node names
				QUERY_STR := 'EXECUTE DIRECT ON (' || $1 || ') ''SELECT * FROM pg_catalog.pg_asp''';
				FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
						node_name :=  datanode;
						sampleid := ROW_DATA.sampleid;
						sample_time := ROW_DATA.sample_time;
						need_flush_sample := ROW_DATA.need_flush_sample;
						databaseid := ROW_DATA.databaseid;
						thread_id := ROW_DATA.thread_id;
						sessionid := ROW_DATA.sessionid;
						start_time := ROW_DATA.start_time;
						event := ROW_DATA.event;
						lwtid := ROW_DATA.lwtid;
						psessionid := ROW_DATA.psessionid;
						tlevel := ROW_DATA.tlevel;
						smpid := ROW_DATA.smpid;
						userid := ROW_DATA.userid;
						application_name := ROW_DATA.application_name;
						client_addr := ROW_DATA.client_addr;
						client_hostname := ROW_DATA.client_hostname;
						client_port := ROW_DATA.client_port;
						query_id := ROW_DATA.query_id;
						unique_query_id := ROW_DATA.unique_query_id;
						user_id := ROW_DATA.user_id;
						cn_id := ROW_DATA.cn_id;
						IF ROW_DATA.cn_id IS NULL THEN
								  unique_query := ROW_DATA.unique_query;
						ELSE
								query_str_cn_name := 'SELECT node_name FROM pgxc_node WHERE node_id='||ROW_DATA.cn_id||' AND nodeis_active = true';
								FOR cn_name IN EXECUTE(query_str_cn_name) LOOP
										query_cn := 'EXECUTE DIRECT ON (' || cn_name.node_name || ') ''SELECT unique_query FROM pg_catalog.pg_asp where unique_query_id = '|| ROW_DATA.unique_query_id||' limit 1''';
										FOR query_text IN EXECUTE(query_cn) LOOP
												  unique_query := query_text.unique_query;
										END LOOP;
								END LOOP;
						END IF;
						RETURN NEXT;
				END LOOP;
				RETURN;
		END; $$
		LANGUAGE 'plpgsql';
		CREATE OR REPLACE FUNCTION DBE_PERF.get_global_pg_asp
		  (OUT node_name text,
		   OUT sampleid bigint,
		   OUT sample_time timestamp without time zone,
		   OUT need_flush_sample boolean,
		   OUT databaseid oid,
		   OUT thread_id bigint,
		   OUT sessionid bigint,
		   OUT start_time timestamp without time zone,
		   OUT event text,
		   OUT lwtid integer,
		   OUT psessionid bigint,
		   OUT tlevel integer,
		   OUT smpid integer,
		   OUT userid oid,
		   OUT application_name text,
		   OUT client_addr inet,
		   OUT client_hostname text,
		   OUT client_port integer,
		   OUT query_id bigint,
		   OUT unique_query_id bigint,
		   OUT user_id oid,
		   OUT cn_id integer,
		   OUT unique_query text)
		RETURNS SETOF record
		AS $$
		DECLARE
		  ROW_DATA pg_catalog.pg_asp%ROWTYPE;
		  ROW_NAME RECORD;
		  QUERY_STR TEXT;
		  QUERY_STR_NODES TEXT;
		BEGIN
		  QUERY_STR_NODES := 'SELECT NODE_NAME FROM PGXC_NODE WHERE NODE_TYPE IN (''C'', ''D'')';
		  FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
			QUERY_STR := 'EXECUTE DIRECT ON (' || ROW_NAME.NODE_NAME || ') ''SELECT * FROM pg_asp''';
			FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
				  node_name := ROW_NAME.NODE_NAME;
				  sampleid := ROW_DATA.sample_id;
				  sample_time := ROW_DATA.sample_time;
				  need_flush_sample := ROW_DATA.need_flush_sample;
				  databaseid := ROW_DATA.databaseid;
				  thread_id := ROW_DATA.thread_id;
				  sessionid := ROW_DATA.sessionid;
				  start_time := ROW_DATA.start_time;
				  event := ROW_DATA.event;
				  lwtid := ROW_DATA.lwtid;
				  psessionid := ROW_DATA.psessionid;
				  tlevel := ROW_DATA.tlevel;
				  smpid := ROW_DATA.smpid;
				  userid := ROW_DATA.userid;
				  application_name := ROW_DATA.application_name;
				  client_addr := ROW_DATA.client_addr;
				  client_hostname := ROW_DATA.client_hostname;
				  client_port := ROW_DATA.client_port;
				  query_id := ROW_DATA.query_id;
				  unique_query_id := ROW_DATA.unique_query_id;
				  user_id := ROW_DATA.user_id;
				  cn_id := ROW_DATA.cn_id;
				  unique_query := ROW_DATA.unique_query;
			  RETURN NEXT;
			END LOOP;
		  END LOOP;
		  RETURN;
		END; $$
		LANGUAGE 'plpgsql';
		CREATE VIEW DBE_PERF.global_pg_asp AS
		  SELECT * FROM DBE_PERF.get_global_pg_asp();
		DECLARE
			user_name text;
			query_str text;
		BEGIN
			SELECT SESSION_USER INTO user_name;
			query_str := 'GRANT ALL ON TABLE DBE_PERF.global_pg_asp TO ' || quote_ident(user_name) || ';';
			EXECUTE IMMEDIATE query_str;
		END;
		GRANT SELECT ON TABLE DBE_PERF.global_pg_asp TO PUBLIC;
	end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.sys_context(text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 731;
CREATE OR REPLACE FUNCTION pg_catalog.sys_context(text, text) RETURNS text LANGUAGE INTERNAL NOT FENCED STRICT as 'sys_context';

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='pkg_service' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS pkg_service.job_cancel(bigint) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_finish(bigint, boolean, timestamp) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_submit(bigint, text, timestamp, text) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_update(bigint, timestamp, text, text) cascade;
            DROP FUNCTION IF EXISTS pkg_service.isubmit_on_nodes(bigint, name, name, text, timestamp without time zone, text) cascade;
            DROP FUNCTION IF EXISTS pkg_service.submit_on_nodes(name, name, text, timestamp without time zone, text, OUT integer) cascade;
            DROP FUNCTION IF EXISTS pkg_service.isubmit_on_nodes_internal(bigint, name, name, text, timestamp without time zone, text) cascade;
		end if;
        exit;
    END LOOP;
END$$;
DROP SCHEMA IF EXISTS pkg_service cascade;

CREATE OR REPLACE VIEW pg_catalog.ALL_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
            WHEN cs.relkind IN ('r', 'f')
            	THEN 'table'::NAME
            WHEN cs.relkind='i'
            	THEN 'index'::NAME
            WHEN cs.relkind='S'
            	THEN 'sequence'::NAME
            WHEN cs.relkind='v'
            	THEN 'view'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_class cs left join pg_object po
		on (po.object_oid = cs.oid and po.object_type in('r', 'f', 'i', 's', 'v'))
		where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_proc pc left join pg_object po
		on (po.object_oid = pc.oid and po.object_type = 'P')
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_rewrite re
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_operator op
	UNION ALL
	SELECT
		pg_get_userbyid(syn.synowner) AS OWNER,
		syn.synname AS OBJECT_NAME,
		syn.oid AS OBJECT_ID,
		'synonym'::NAME AS OBJECT_TYPE,
		syn.synnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_synonym syn;



CREATE OR REPLACE VIEW pg_catalog.USER_OBJECTS AS
	SELECT
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		CASE
            WHEN cs.relkind IN ('r', 'f')
            	THEN 'table'::NAME
            WHEN cs.relkind='i'
            	THEN 'index'::NAME
            WHEN cs.relkind='S'
            	THEN 'sequence'::NAME
            WHEN cs.relkind='v'
            	THEN 'view'::NAME
		END AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_class cs left join pg_object po
		on (po.object_oid = cs.oid and po.object_type in('r', 'f', 'i', 's', 'v'))
	WHERE cs.relkind in('r', 'f', 'i', 'S', 'v')
		AND pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'procedure'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_proc pc left join pg_object po
		on (po.object_oid = pc.oid and po.object_type = 'P')
	WHERE pg_get_userbyid(pc.proowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'rule'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_rewrite re
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'trigger'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	WHERE pg_get_userbyid(cs.relowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_type te
	WHERE pg_get_userbyid(te.typowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION
	SELECT
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_operator op
	WHERE pg_get_userbyid(op.oprowner)=SYS_CONTEXT('USERENV','CURRENT_USER')
	UNION ALL
	SELECT
		syn.synname AS OBJECT_NAME,
		syn.oid AS OBJECT_ID,
		'synonym'::NAME AS OBJECT_TYPE,
		syn.synnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_synonym syn
	WHERE pg_get_userbyid(syn.synowner) = SYS_CONTEXT('USERENV','CURRENT_USER');

CREATE OR REPLACE VIEW pg_catalog.DBA_OBJECTS AS
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		cs.relname AS OBJECT_NAME,
		cs.oid AS OBJECT_ID,
		(CASE
            WHEN cs.relkind IN ('r', 'f')
            	THEN 'TABLE'::NAME
            WHEN cs.relkind='i'
            	THEN 'INDEX'::NAME
            WHEN cs.relkind='S'
            	THEN 'SEQUENCE'::NAME
            WHEN cs.relkind='v'
            	THEN 'VIEW'::NAME
		END) AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_class cs left join pg_object po
		on (po.object_oid = cs.oid and po.object_type in('r', 'f', 'i', 's', 'v'))
		where cs.relkind in('r', 'f', 'i', 'S', 'v')
	UNION
	SELECT
		pg_get_userbyid(pc.proowner) AS OWNER,
		pc.proname AS OBJECT_NAME,
		pc.oid AS OBJECT_ID,
		'PROCEDURE'::NAME AS OBJECT_TYPE,
		pc.pronamespace AS NAMESPACE,
		po.ctime AS CREATED,
		po.mtime AS LAST_DDL_TIME
	FROM pg_proc pc left join pg_object po
		on (po.object_oid = pc.oid and po.object_type = 'P')
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		re.rulename AS OBJECT_NAME,
		re.oid AS OBJECT_ID,
		'RULE'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_rewrite re
		LEFT JOIN pg_class cs ON (cs.oid = re.ev_class)
	UNION
	SELECT
		pg_get_userbyid(cs.relowner) AS OWNER,
		tr.tgname AS OBJECT_NAME,
		tr.oid AS OBJECT_ID,
		'TRIGGER'::NAME AS OBJECT_TYPE,
		cs.relnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_trigger tr
		LEFT JOIN pg_class cs ON (cs.oid = tr.tgrelid)
	UNION
	SELECT
		pg_get_userbyid(te.typowner) AS OWNER,
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'TYPE'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_type te
	UNION
	SELECT
		pg_get_userbyid(op.oprowner) AS OWNER,
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'OPERATOR'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_operator op
	UNION ALL
		SELECT
		pg_get_userbyid(syn.synowner) AS OWNER,
		syn.synname AS OBJECT_NAME,
		syn.oid AS OBJECT_ID,
		'synonym'::NAME AS OBJECT_TYPE,
		syn.synnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_synonym syn;