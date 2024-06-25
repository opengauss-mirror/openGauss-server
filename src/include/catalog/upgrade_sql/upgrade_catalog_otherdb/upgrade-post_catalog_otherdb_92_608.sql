DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info()
 RETURNS text
 LANGUAGE internal
 STABLE STRICT NOT FENCED SHIPPABLE
AS $function$get_client_info$function$;
comment on function PG_CATALOG.get_client_info() is 'read current client';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info(OUT sid bigint, OUT client_info text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED SHIPPABLE ROWS 100
AS $function$get_client_info$function$;
comment on function PG_CATALOG.get_client_info() is 'read current client';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
-- pg_ls_tmpdir_noargs
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_tmpdir() CASCADE;
drop function if exists pg_catalog.pg_ls_tmpdir(out name text, out size bigint, out modification timestamptz) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3354;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ls_tmpdir(out name text, out size bigint, out modification timestamptz)
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 10 ROWS 20
AS $function$pg_ls_tmpdir_noargs$function$;
comment on function PG_CATALOG.pg_ls_tmpdir(out name text, out size bigint, out modification timestamptz) is
  'list of temporary files in the pg_default tablespace''s pgsql_tmp directory';

-- pg_ls_tmpdir_1arg
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_tmpdir(oid oid) CASCADE;
drop function if exists pg_catalog.pg_ls_tmpdir(oid oid, out name text, out size bigint, out modification timestamptz) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3355;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ls_tmpdir(oid oid, out name text, out size bigint, out modification timestamptz)
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 10 ROWS 20
AS $function$pg_ls_tmpdir_1arg$function$;
comment on function PG_CATALOG.pg_ls_tmpdir(oid oid, out name text, out size bigint, out modification timestamptz) is
  'list of temporary files in the specified tablespace''s pgsql_tmp directory';

-- pg_ls_waldir
DROP FUNCTION IF EXISTS pg_catalog.pg_ls_waldir() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3356;
CREATE OR REPLACE FUNCTION pg_catalog.pg_ls_waldir(out name text, out size bigint, out modification timestamptz)
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 10 ROWS 20
AS $function$pg_ls_waldir$function$;
comment on function PG_CATALOG.pg_ls_waldir() is 'list of files in the WAL directory';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

------------------------------------------------------------------------------------------------------------------------------------
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
-----------------------------------------------------------------------------------------------------------------------------------------------------
	DROP VIEW IF EXISTS DBE_PERF.local_active_session cascade;
	DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5721;
	CREATE OR REPLACE FUNCTION pg_catalog.get_local_active_session
		(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text, OUT global_sessionid text, OUT xact_start_time timestamp with time zone, OUT query_start_time timestamp with time zone, OUT state text)
		RETURNS setof record LANGUAGE INTERNAL rows 100 VOLATILE NOT FENCED as 'get_local_active_session';
    CREATE OR REPLACE VIEW DBE_PERF.local_active_session AS
	  WITH RECURSIVE
		las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
			tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
			user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state)
		  AS (select t.* from get_local_active_session() as t),
		tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
		   tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
		   user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state, final_block_sessionid, level, head)
		  AS(SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, array_append('{}', las.sessionid) AS head FROM las
			UNION ALL
			 SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid,
					tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id,
					tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, tt.global_sessionid, tt.xact_start_time, tt.query_start_time, tt.state,                                                                                                                 las.block_sessionid AS final_block_sessionid, tt.level + 1 AS level, array_append(tt.head, las.sessionid) AS head
			 FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
			 WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0)
			   AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
	  SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
			 tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
			 user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state FROM tt
		WHERE level = (SELECT MAX(level) FROM tt t1 WHERE t1.sampleid =  tt.sampleid AND t1.sessionid = tt.sessionid);
  end if;
END$DO$;

DO $DO$
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
END$DO$;

DROP EXTENSION IF EXISTS security_plugin CASCADE;

CREATE EXTENSION IF NOT EXISTS security_plugin;
DO $DO$
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
END$DO$;
-- gs_stack_int8
DROP FUNCTION IF EXISTS pg_catalog.gs_stack(INT8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9997;
CREATE OR REPLACE FUNCTION pg_catalog.gs_stack(INT8)
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 1 ROWS 5
AS $function$gs_stack_int8$function$;

-- gs_stack_noargs
DROP FUNCTION IF EXISTS pg_catalog.gs_stack() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9998;
CREATE OR REPLACE FUNCTION pg_catalog.gs_stack()
    RETURNS SETOF record
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE COST 1 ROWS 5
AS $function$gs_stack_noargs$function$;
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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
------------------------------------------------------------------------------------------------------------------------------------
DO $DO$
DECLARE
  ans boolean;
  user_name text;
  grant_query text;
BEGIN
select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;

-----------------------------------------------------------------------------
-- DROP: pg_catalog.pg_replication_slots
DROP VIEW IF EXISTS pg_catalog.pg_replication_slots CASCADE;

-- DROP: pg_get_replication_slot()
DROP FUNCTION IF EXISTS pg_catalog.pg_get_replication_slots(OUT slot_name text, OUT plugin text, OUT slot_type text, OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn text, OUT dummy_standby boolean) CASCADE;

-- DROP: gs_get_parallel_decode_status()
DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text) CASCADE;

-----------------------------------------------------------------------------
-- CREATE: pg_get_replication_slots
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3784;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_replication_slots(OUT slot_name text, OUT plugin text, OUT slot_type text,
    OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn text, OUT dummy_standby boolean,
    OUT confirmed_flush text)
  RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED rows 10 as 'pg_get_replication_slots';

COMMENT ON FUNCTION pg_catalog.pg_get_replication_slots() is 'information about replication slots currently in use';

-- CREATE: pg_catalog.pg_replication_slots
CREATE VIEW pg_catalog.pg_replication_slots AS
    SELECT
            L.slot_name,
            L.plugin,
            L.slot_type,
            L.datoid,
            D.datname AS database,
            L.active,
            L.xmin,
            L.catalog_xmin,
            L.restart_lsn,
            L.dummy_standby,
            L.confirmed_flush
    FROM pg_catalog.pg_get_replication_slots() AS L
            LEFT JOIN pg_database D ON (L.datoid = D.oid);

-- CREATE: dbe_perf.replication_slots
IF ans = true THEN
 
CREATE OR REPLACE VIEW dbe_perf.replication_slots AS
  SELECT
    L.slot_name,
    L.plugin,
    L.slot_type,
    L.datoid,
    D.datname AS database,
    L.active,
    L.xmin,
    L.catalog_xmin,
    L.restart_lsn,
    L.dummy_standby
    FROM pg_get_replication_slots() AS L
         LEFT JOIN pg_database D ON (L.datoid = D.oid);
 
END IF;

-- CREATE: gs_get_parallel_decode_status
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9377;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text, OUT reader_lsn text, OUT working_txn_cnt int8, OUT working_txn_memory int8)
 RETURNS SETOF RECORD
 LANGUAGE internal
AS $function$gs_get_parallel_decode_status$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-----------------------------------------------------------------------------
-- privileges
SELECT SESSION_USER INTO user_name;
 
-- dbe_perf
IF ans = true THEN
grant_query := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON dbe_perf.replication_slots TO ' || quote_ident(user_name) || ';';
EXECUTE IMMEDIATE grant_query;
 
GRANT SELECT ON dbe_perf.replication_slots TO PUBLIC;
END IF;

-- pg_catalog
GRANT SELECT ON pg_catalog.pg_replication_slots TO PUBLIC;

-----------------------------------------------------------------------------
END$DO$;
DROP VIEW IF EXISTS pg_catalog.gs_session_cpu_statistics cascade;
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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9050, 9051, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_sql_patch (
    patch_name name NOT NULL,
    unique_sql_id bigint NOT NULL,
    owner Oid NOT NULL,
    enable boolean NOT NULL,
    status "char" NOT NULL,
    abort boolean NOT NULL,
    hint_string text,
    hint_node pg_node_tree,
    original_query text,
    original_query_tree pg_node_tree,
    patched_query text,
    patched_query_tree pg_node_tree,
    description text
);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9053;
CREATE UNIQUE INDEX pg_catalog.gs_sql_patch_patch_name_index ON pg_catalog.gs_sql_patch USING btree (patch_name);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9054;
CREATE INDEX pg_catalog.gs_sql_patch_unique_sql_id_index ON pg_catalog.gs_sql_patch USING btree (unique_sql_id);

GRANT SELECT ON TABLE pg_catalog.gs_sql_patch TO PUBLIC;

DROP SCHEMA IF EXISTS dbe_sql_util cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 9049;
CREATE SCHEMA dbe_sql_util;
GRANT USAGE ON SCHEMA dbe_sql_util TO PUBLIC;
comment on schema dbe_sql_util is 'sql util schema';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9060;
CREATE OR REPLACE FUNCTION dbe_sql_util.create_hint_sql_patch(name, bigint, text, text DEFAULT NULL::text, boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$create_sql_patch_by_id_hint$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9064;
CREATE OR REPLACE FUNCTION dbe_sql_util.create_abort_sql_patch(name, bigint, text DEFAULT NULL::text, boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$create_abort_patch_by_id$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9061;
CREATE OR REPLACE FUNCTION dbe_sql_util.enable_sql_patch(name)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$enable_sql_patch$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9062;
CREATE OR REPLACE FUNCTION dbe_sql_util.disable_sql_patch(name)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$disable_sql_patch$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9063;
CREATE OR REPLACE FUNCTION dbe_sql_util.drop_sql_patch(name)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$drop_sql_patch$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9065;
CREATE OR REPLACE FUNCTION dbe_sql_util.show_sql_patch(patch_name name, OUT unique_sql_id bigint, OUT enable boolean, OUT abort boolean, OUT hint_str text)
 RETURNS SETOF record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$show_sql_patch$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;CREATE OR REPLACE FUNCTION pg_catalog.gs_session_memory_detail_tp(OUT sessid TEXT, OUT sesstype TEXT, OUT contextname TEXT, OUT level INT2, OUT parent TEXT, OUT totalsize INT8, OUT freesize INT8, OUT usedsize INT8)
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
                   UNION ALL
                   SELECT 
                     Ssessid AS sessid, sesstype, contextname, level, parent, totalsize, freesize, usedsize
                   FROM TM WHERE Ssessid IS NOT NULL
                   UNION ALL
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
LANGUAGE plpgsql NOT FENCED;DROP VIEW IF EXISTS pg_catalog.pg_gtt_attached_pids;
DROP FUNCTION IF EXISTS pg_catalog.pg_gtt_attached_pid(relid oid, OUT relid oid, OUT pid bigint);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3598;
CREATE OR REPLACE FUNCTION pg_catalog.pg_gtt_attached_pid(relid oid, OUT relid oid, OUT pid bigint, OUT sessionid bigint)
 RETURNS SETOF record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE ROWS 10
AS $function$pg_gtt_attached_pid$function$;

CREATE OR REPLACE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS pids,
    array(select sessionid from pg_catalog.pg_gtt_attached_pid(c.oid)) AS sessionids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r', 'S', 'L');
GRANT SELECT ON pg_catalog.pg_gtt_attached_pids TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_history_memory_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5257;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_history_memory_detail(
memlog_filename cstring,
OUT memory_info text) RETURNS SETOF TEXT LANGUAGE INTERNAL as 'gs_get_history_memory_detail';DROP FUNCTION IF EXISTS gs_is_dw_io_blocked() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4772;
CREATE OR REPLACE FUNCTION pg_catalog.gs_is_dw_io_blocked(OUT result boolean)
 RETURNS SETOF boolean
 LANGUAGE internal rows 100
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_is_dw_io_blocked$function$;

DROP FUNCTION IF EXISTS gs_block_dw_io(integer, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4773;
CREATE OR REPLACE FUNCTION pg_catalog.gs_block_dw_io(timeout integer, identifier text, OUT result boolean)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_block_dw_io$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DO $DO$
DECLARE
  ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.statement_history cascade;
    end if;
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
    trace_id text,
    advise text
)
WITH (orientation=row, compression=no);
REVOKE ALL on table pg_catalog.statement_history FROM public;
create index pg_catalog.statement_history_time_idx on pg_catalog.statement_history USING btree (start_time, is_slow_sql);

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
        
        SELECT SESSION_USER INTO username;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='statement_history') THEN
            querystr := 'REVOKE ALL ON TABLE dbe_perf.statement_history FROM ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            querystr := 'REVOKE ALL ON TABLE pg_catalog.statement_history FROM ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            querystr := 'REVOKE SELECT on table dbe_perf.statement_history FROM public;';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE pg_catalog.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
        END IF;
    end if;
END$DO$;DROP FUNCTION IF EXISTS pg_catalog.array_count(anyarray, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2330;
CREATE OR REPLACE FUNCTION pg_catalog.array_count(anyarray, integer)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$array_count$function$;
COMMENT ON FUNCTION pg_catalog.array_count(anyarray, integer) IS 'array count';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.gs_lwlock_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8888;
CREATE FUNCTION pg_catalog.gs_lwlock_status
(
    OUT node_name pg_catalog.text,
    OUT lock_name pg_catalog.text,
    OUT lock_unique_id pg_catalog.int8,
    OUT pid pg_catalog.int8,
    OUT sessionid pg_catalog.int8,
    OUT global_sessionid pg_catalog.text,
    OUT mode pg_catalog.text,
    OUT granted pg_catalog.bool,
    OUT start_time pg_catalog.timestamptz
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_lwlock_status';
comment on function pg_catalog.gs_lwlock_status() is 'View system lwlock information';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DO
$do$
    DECLARE
        index_exists boolean := true;
    BEGIN
        select case when count(*)=1 then true else false end from (select 1 from pg_class where oid = 3480 and relkind = 'i' and relname = 'pg_partition_tblspc_relfilenode_index') into index_exists;
        IF index_exists = false then
            -- ADD INDEX pg_partition_tblspc_relfilenode_index for pg_partition(reltablespace, relfilenode)
            -- set GUC for pg_partition_tblspc_relfilenode_index
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3480;

            -- create index
            CREATE INDEX pg_partition_tblspc_relfilenode_index ON pg_catalog.pg_partition
                USING BTREE(reltablespace oid_ops, relfilenode oid_ops);
        END IF;
    END
$do$;
DROP EXTENSION IF EXISTS hdfs_fdw;
