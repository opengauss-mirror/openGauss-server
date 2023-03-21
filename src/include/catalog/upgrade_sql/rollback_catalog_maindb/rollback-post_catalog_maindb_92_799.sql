DO $upgrade$
BEGIN
IF working_version_num() < 92608 then
DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_tmpdir() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_tmpdir(oid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_waldir() CASCADE;

------------------------------------------------------------------------------------------------------------------------------------
DECLARE
ans boolean;
BEGIN
select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
if ans = true then

-----------------------------------------------------------------------------------------------------------------------------------------------------
DROP VIEW IF EXISTS DBE_PERF.local_active_session cascade;
DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text, OUT xact_start_time timestamp with time zone, OUT query_start_time timestamp with time zone, OUT state text) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5721;
CREATE OR REPLACE FUNCTION pg_catalog.get_local_active_session
		(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text)
		RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_local_active_session';
    CREATE OR REPLACE VIEW DBE_PERF.local_active_session AS
	  WITH RECURSIVE
		las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
			tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
			user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid)
		  AS (select t.* from get_local_active_session() as t),
		tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
		   tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
		   user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, final_block_sessionid, level, head)
		  AS(SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, array_append('{}', las.sessionid) AS head FROM las
			UNION ALL
			 SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid,
					tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id,
					tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, tt.global_sessionid,                                                                                                                  las.block_sessionid AS final_block_sessionid, tt.level + 1 AS level, array_append(tt.head, las.sessionid) AS head
			 FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
			 WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0)
			   AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
       tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
       user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status, global_sessionid FROM tt
WHERE level = (SELECT MAX(level) FROM tt t1 WHERE t1.sampleid =  tt.sampleid AND t1.sessionid = tt.sessionid);
end if;
END;

DECLARE
ans boolean;
  user_name text;
  query_str text;
BEGIN
select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
if ans = true then
SELECT SESSION_USER INTO user_name;

query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.local_active_session TO ' || quote_ident(user_name) || ';';
EXECUTE IMMEDIATE query_str;
GRANT SELECT ON TABLE DBE_PERF.local_active_session TO PUBLIC;

GRANT SELECT ON TABLE pg_catalog.gs_asp TO PUBLIC;

end if;
END;

DECLARE
  ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_full_sql_by_timestamp() cascade;
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_slow_sql_by_timestamp() cascade;

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
           OUT is_slow_sql bool,
           OUT trace_id text)
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
           OUT is_slow_sql bool,
           OUT trace_id text)
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
                  return next;
               END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;
    end if;
END;

DROP FUNCTION IF EXISTS pg_catalog.gs_stack() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_stack(INT8) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_stack(pid bigint) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_stack(OUT tid bigint, OUT lwtid bigint, OUT stack text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9997;
CREATE OR REPLACE FUNCTION pg_catalog.gs_stack(pid bigint)
    RETURNS SETOF text
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 1 ROWS 5
AS $function$gs_stack$function$;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9998;
CREATE OR REPLACE FUNCTION pg_catalog.gs_stack(OUT tid bigint, OUT lwtid bigint, OUT stack text)
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 1 ROWS 5
AS $function$gs_stack$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP VIEW IF EXISTS pg_catalog.gs_session_cpu_statistics cascade;
CREATE VIEW pg_catalog.gs_session_cpu_statistics AS
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
WHERE S.sessionid = T.threadid;
GRANT SELECT ON TABLE pg_catalog.gs_session_cpu_statistics TO PUBLIC;
DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_sql_util' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS dbe_sql_util.create_hint_sql_patch(name, bigint, text, text, boolean);
            DROP FUNCTION IF EXISTS dbe_sql_util.create_abort_sql_patch(name, bigint, text, boolean);
            DROP FUNCTION IF EXISTS dbe_sql_util.enable_sql_patch(name);
            DROP FUNCTION IF EXISTS dbe_sql_util.disable_sql_patch(name);
            DROP FUNCTION IF EXISTS dbe_sql_util.show_sql_patch(name);
            DROP FUNCTION IF EXISTS dbe_sql_util.drop_sql_patch(name);
        end if;
        exit;
    END LOOP;
END;

DROP SCHEMA IF EXISTS dbe_sql_util cascade;

DROP INDEX IF EXISTS pg_catalog.gs_sql_patch_unique_sql_id_index;
DROP INDEX IF EXISTS pg_catalog.gs_sql_patch_patch_name_index;
DROP TYPE IF EXISTS pg_catalog.gs_sql_patch;
DROP TABLE IF EXISTS pg_catalog.gs_sql_patch;CREATE OR REPLACE FUNCTION pg_catalog.gs_session_memory_detail_tp(OUT sessid TEXT, OUT sesstype TEXT, OUT contextname TEXT, OUT level INT2, OUT parent TEXT, OUT totalsize INT8, OUT freesize INT8, OUT usedsize INT8)
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
                      gs_session_memory_context S
                      LEFT JOIN
                     (SELECT DISTINCT thrdtype, tid
                      FROM gs_thread_memory_context) T
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
                      gs_thread_memory_context T
                      LEFT JOIN
                      (SELECT DISTINCT sessid, threadid
                       FROM gs_session_memory_context) S
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
LANGUAGE plpgsql NOT FENCED;DROP FUNCTION IF EXISTS pg_catalog.gs_get_history_memory_detail() cascade;DROP FUNCTION IF EXISTS gs_is_dw_io_blocked() CASCADE;
DROP FUNCTION IF EXISTS gs_block_dw_io(integer, text) CASCADE;
END IF;
END $upgrade$;
