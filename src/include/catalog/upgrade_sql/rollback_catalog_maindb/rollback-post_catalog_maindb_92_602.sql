
DO
$do$
DECLARE
query_str text;
type array_t is varray(10) of varchar2(50);
rel_array array_t := array[
  'pg_catalog.pg_proc',
  'pg_catalog.pg_type',
  'pg_catalog.pg_attrdef',
  'pg_catalog.pg_constraint',
  'pg_catalog.pg_rewrite',
  'pg_catalog.pg_rewrite',
  'pg_catalog.pg_trigger',
  'pg_catalog.pg_rlspolicy'
];
att_array array_t := array[
  'proargdefaults',
  'typdefaultbin',
  'adbin',
  'conbin',
  'ev_qual',
  'ev_action',
  'tgqual',
  'polqual'
];
ans boolean;
old_version boolean;
has_version_proc boolean;
need_upgrade boolean;
BEGIN
  FOR ans in select case when count(*) = 1 then true else false end as ans from (select 1 from pg_catalog.pg_proc where proname = 'large_seq_rollback_ntree' limit 1) LOOP
    IF ans = true then
      need_upgrade = false;
      select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
      IF has_version_proc = true then
        select working_version_num < 92455 as old_version from working_version_num() into old_version;
        IF old_version = true then
          raise info 'Processing sequence APIs';
          FOR i IN 1..rel_array.count LOOP
            raise info '%.%',rel_array[i],att_array[i];
            query_str := 'UPDATE ' || rel_array[i] || ' SET ' || att_array[i] || ' = large_seq_rollback_ntree(' || att_array[i] || ' ) WHERE ' || att_array[i] || ' LIKE ''%:funcid 1574 :%'' OR ' || att_array[i] || ' LIKE ''%:funcid 1575 :%'' OR ' || att_array[i] || ' LIKE ''%:funcid 2559 :%'';';
            EXECUTE query_str;
          END LOOP;
        END IF;
      END IF;
    END IF;
  END LOOP;
END
$do$;

DROP FUNCTION IF EXISTS pg_catalog.large_seq_upgrade_ntree(pg_node_tree);
DROP FUNCTION IF EXISTS pg_catalog.large_seq_rollback_ntree(pg_node_tree);
DROP FUNCTION IF EXISTS pg_catalog.gs_get_shared_memctx_detail() cascade;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_thread_memctx_detail() cascade;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_session_memctx_detail() cascade;DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_index_advise(cstring);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4888;
CREATE OR REPLACE FUNCTION pg_catalog.gs_index_advise(sql_string cstring, OUT schema text, OUT "table" text, OUT "column" text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE
AS $function$gs_index_advise$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DROP FUNCTION IF EXISTS pg_catalog.gs_parse_page_bypath(text, bigint, text, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_lsn(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_xid(xid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_tablepath(text, bigint, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_parsepage_tablepath(text, bigint, text, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.local_xlog_redo_statics() cascade;
DROP FUNCTION IF EXISTS pg_catalog.local_redo_time_count() cascade;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_ec_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_get_invalid_backends CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_stat_activity cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT connection_info text, OUT srespool name, OUT global_sessionid text, OUT unique_sql_id bigint) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo
(
	IN pid bigint,
	OUT datid oid,
	OUT pid bigint,
	OUT sessionid bigint,
	OUT usesysid oid,
	OUT application_name text,
	OUT state text,
	OUT query text,
	OUT waiting boolean,
	OUT xact_start timestamp with time zone,
	OUT query_start timestamp with time zone,
	OUT backend_start timestamp with time zone,
	OUT state_change timestamp with time zone,
	OUT client_addr inet,
	OUT client_hostname text,
	OUT client_port integer,
	OUT enqueue text,
	OUT query_id bigint,
	OUT connection_info text,
	OUT srespool name,
	OUT global_sessionid text
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_activity_with_conninfo';

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
                        WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE S.srespool
                        END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid;

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
FROM pg_catalog.pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
where s.query_id = t.queryid and t.ec_operator > 0;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_statistics AS
SELECT t.*
FROM pg_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
where s.query_id = t.queryid;

CREATE OR REPLACE VIEW pg_catalog.pg_get_invalid_backends AS
	SELECT
			C.pid,
			C.node_name,
			S.datname AS dbname,
			S.backend_start,
			S.query
	FROM pg_pool_validate(false, ' ') AS C LEFT JOIN pg_stat_activity AS S
			ON (C.pid = S.sessionid);

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wlm_session_iostat_info(integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5014;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_wlm_session_iostat_info(IN unuseattr integer, OUT threadid bigint, OUT maxcurr_iops integer, OUT mincurr_iops integer, OUT maxpeak_iops integer, OUT minpeak_iops integer, OUT iops_limits integer, OUT io_priority integer, OUT curr_io_limits integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_session_iostat_info';

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
	DROP VIEW IF EXISTS DBE_PERF.global_session_stat_activity cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_session_stat_activity() cascade;

	DROP VIEW IF EXISTS DBE_PERF.global_operator_runtime  cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_operator_runtime() cascade;
	DROP VIEW IF EXISTS DBE_PERF.operator_runtime  cascade;

	DROP VIEW IF EXISTS DBE_PERF.session_stat_activity cascade;

	DROP VIEW IF EXISTS DBE_PERF.global_replication_stat cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_replication_stat() cascade;
	DROP VIEW IF EXISTS DBE_PERF.replication_stat cascade;
				
			
	DROP VIEW IF EXISTS DBE_PERF.session_cpu_runtime cascade;
	DROP VIEW IF EXISTS DBE_PERF.session_memory_runtime cascade;
	DROP VIEW IF EXISTS DBE_PERF.global_statement_complex_runtime cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_statement_complex_runtime() cascade;
	DROP VIEW IF EXISTS DBE_PERF.statement_complex_runtime cascade;
				
	DROP VIEW IF EXISTS DBE_PERF.statement_iostat_complex_runtime cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_session_memory_statistics cascade;
	DROP VIEW IF EXISTS pg_catalog.pg_session_iostat cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_session_cpu_statistics cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_statistics cascade;

	DROP VIEW IF EXISTS pg_catalog.pg_stat_activity_ng cascade;
	DROP VIEW IF EXISTS pg_catalog.pg_stat_replication cascade;

	DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT srespool name, OUT global_sessionid text, OUT unique_sql_id bigint) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
	CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_activity
	(
		IN pid bigint,
		OUT datid oid,
		OUT pid bigint,
		OUT sessionid bigint,
		OUT usesysid oid,
		OUT application_name text,
		OUT state text,
		OUT query text,
		OUT waiting boolean,
		OUT xact_start timestamp with time zone,
		OUT query_start timestamp with time zone,
		OUT backend_start timestamp with time zone,
		OUT state_change timestamp with time zone,
		OUT client_addr inet,
		OUT client_hostname text,
		OUT client_port integer,
		OUT enqueue text,
		OUT query_id bigint,
		OUT srespool name,
		OUT global_sessionid text
	)
	RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_activity';

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
	  WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
	ELSE S.srespool
	END AS resource_pool,
	    S.query_id,
	    S.query
	FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
	  WHERE S.datid = D.oid AND
	        S.usesysid = U.oid;


	CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_stat_activity
	  (out coorname text, out datid oid, out datname text, out pid bigint,
	   out usesysid oid, out usename text, out application_name text, out client_addr inet,
	   out client_hostname text, out client_port integer, out backend_start timestamptz,
	   out xact_start timestamptz, out query_start timestamptz, out state_change timestamptz,
	   out waiting boolean, out enqueue text, out state text, out resource_pool name,
	   out query_id bigint, out query text)
	RETURNS setof record
	AS $$
	DECLARE
	  row_data dbe_perf.session_stat_activity%rowtype;
	  coor_name record;
	  fet_active text;
	  fetch_coor text;
	  BEGIN
		--Get all cn node names
		fetch_coor := 'select * from dbe_perf.node_name';
		FOR coor_name IN EXECUTE(fetch_coor) LOOP
		  coorname :=  coor_name.node_name;
		  fet_active := 'SELECT * FROM dbe_perf.session_stat_activity';
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


	CREATE OR REPLACE VIEW DBE_PERF.global_session_stat_activity AS
	  SELECT * FROM DBE_PERF.get_global_session_stat_activity();
	  
	CREATE OR REPLACE VIEW dbe_perf.operator_runtime AS
	  SELECT t.*
	  FROM dbe_perf.session_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
		WHERE s.query_id = t.queryid;

	CREATE OR REPLACE FUNCTION dbe_perf.get_global_operator_runtime()
	RETURNS setof dbe_perf.operator_runtime
	AS $$
	DECLARE
	  row_data dbe_perf.operator_runtime%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.operator_runtime';
		  FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW dbe_perf.global_operator_runtime AS
	  SELECT * FROM dbe_perf.get_global_operator_runtime();


	CREATE OR REPLACE VIEW dbe_perf.replication_stat AS
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

	CREATE OR REPLACE FUNCTION dbe_perf.get_global_replication_stat
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
	  row_data dbe_perf.replication_stat%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		--Get all the node names
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.replication_stat';
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

	CREATE OR REPLACE VIEW dbe_perf.global_replication_stat AS
	  SELECT * FROM dbe_perf.get_global_replication_stat();


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
                        WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE S.srespool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            S.sessionid = N.sessionid;


	CREATE OR REPLACE VIEW dbe_perf.session_cpu_runtime AS
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
		FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE VIEW dbe_perf.session_memory_runtime AS
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
	  FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE VIEW dbe_perf.statement_complex_runtime AS
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
	  FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE FUNCTION dbe_perf.get_global_statement_complex_runtime()
	RETURNS setof dbe_perf.statement_complex_runtime
	AS $$
	DECLARE
	  row_data dbe_perf.statement_complex_runtime%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.statement_complex_runtime';
		  FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW dbe_perf.global_statement_complex_runtime AS
	  SELECT * FROM dbe_perf.get_global_statement_complex_runtime();

	CREATE OR REPLACE VIEW dbe_perf.statement_iostat_complex_runtime AS
	   SELECT
		 S.query_id,
		 T.mincurr_iops as mincurriops,
		 T.maxcurr_iops as maxcurriops,
		 T.minpeak_iops as minpeakiops,
		 T.maxpeak_iops as maxpeakiops,
		 T.iops_limits as io_limits,
		 CASE WHEN T.io_priority = 0 THEN 'None'::text
			  WHEN T.io_priority = 20 THEN 'Low'::text
			  WHEN T.io_priority = 50 THEN 'Medium'::text
			  WHEN T.io_priority = 80 THEN 'High'::text END AS io_priority,
		 S.query,
		 S.node_group,
		 T.curr_io_limits as curr_io_limits
	   FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
		 WHERE S.pid = T.threadid;

  end if;
END$DO$;

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
FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
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
        S.node_group,
        T.curr_io_limits as curr_io_limits
FROM pg_catalog.pg_stat_activity_ng AS S,  pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
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
            S.pid = W.pid;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_stat_activity TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
	GRANT SELECT ON TABLE DBE_PERF.session_stat_activity TO PUBLIC;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_session_stat_activity TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_session_stat_activity TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.operator_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.operator_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_operator_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_operator_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.replication_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.replication_stat TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_replication_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_replication_stat TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_activity_ng TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_cpu_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.session_cpu_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_memory_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.session_memory_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.statement_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.statement_complex_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_statement_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_statement_complex_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.statement_iostat_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.statement_iostat_complex_runtime TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_session_memory_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_session_iostat TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_session_cpu_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_replication TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_activity TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_ec_operator_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_operator_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_get_invalid_backends TO PUBLIC;
  end if;
END$DO$;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int, text) CASCADE;
do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.gs_get_active_archiving_standby();
        DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_get_warning_for_xlog_force_recycle();
		DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_clean_history_global_barriers();
        DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_archive_slot_force_advance();
    end if;
END$$;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_standby_cluster_barrier_status() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_set_standby_cluster_target_barrier_id() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_query_standby_cluster_barrier_id_exist() cascade;
do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP VIEW IF EXISTS DBE_PERF.global_streaming_hadr_rto_and_rpo_stat CASCADE;
    end if;
    DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_local_rto_and_rpo_stat();
    DROP FUNCTION IF EXISTS pg_catalog.gs_hadr_remote_rto_and_rpo_stat();
END$$;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5077;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hadr_local_rto_and_rpo_stat
(
OUT hadr_sender_node_name pg_catalog.text,
OUT hadr_receiver_node_name pg_catalog.text,
OUT source_ip pg_catalog.text,
OUT source_port pg_catalog.int4,
OUT dest_ip pg_catalog.text,
OUT dest_port pg_catalog.int4,
OUT current_rto pg_catalog.int8,
OUT target_rto pg_catalog.int8,
OUT current_rpo pg_catalog.int8,
OUT target_rpo pg_catalog.int8,
OUT current_sleep_time pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_hadr_local_rto_and_rpo_stat';
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5078;
CREATE OR REPLACE FUNCTION pg_catalog.gs_hadr_remote_rto_and_rpo_stat
(
OUT hadr_sender_node_name pg_catalog.text,
OUT hadr_receiver_node_name pg_catalog.text,
OUT source_ip pg_catalog.text,
OUT source_port pg_catalog.int4,
OUT dest_ip pg_catalog.text,
OUT dest_port pg_catalog.int4,
OUT current_rto pg_catalog.int8,
OUT target_rto pg_catalog.int8,
OUT current_rpo pg_catalog.int8,
OUT target_rpo pg_catalog.int8,
OUT current_sleep_time pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_hadr_remote_rto_and_rpo_stat';
CREATE OR REPLACE VIEW DBE_PERF.global_streaming_hadr_rto_and_rpo_stat AS
    SELECT hadr_sender_node_name, hadr_receiver_node_name, current_rto, target_rto, current_rpo, target_rpo, current_sleep_time
FROM pg_catalog.gs_hadr_local_rto_and_rpo_stat();
REVOKE ALL on DBE_PERF.global_streaming_hadr_rto_and_rpo_stat FROM PUBLIC;
DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_streaming_hadr_rto_and_rpo_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/
GRANT SELECT ON TABLE DBE_PERF.global_streaming_hadr_rto_and_rpo_stat TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP VIEW IF EXISTS pg_catalog.gs_gsc_memory_detail cascade;
DROP VIEW IF EXISTS pg_catalog.gs_lsc_memory_detail cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_gsc_table_detail() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_gsc_catalog_detail() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_gsc_dbstat_info() cascade;
DROP FUNCTION IF EXISTS pg_catalog.gs_gsc_clean() cascade;DROP VIEW IF EXISTS pg_catalog.gs_wlm_ec_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_get_invalid_backends CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_stat_activity cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT connection_info text, OUT srespool name, OUT global_sessionid text, OUT unique_sql_id bigint, OUT trace_id text) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo
(
	IN pid bigint,
	OUT datid oid,
	OUT pid bigint,
	OUT sessionid bigint,
	OUT usesysid oid,
	OUT application_name text,
	OUT state text,
	OUT query text,
	OUT waiting boolean,
	OUT xact_start timestamp with time zone,
	OUT query_start timestamp with time zone,
	OUT backend_start timestamp with time zone,
	OUT state_change timestamp with time zone,
	OUT client_addr inet,
	OUT client_hostname text,
	OUT client_port integer,
	OUT enqueue text,
	OUT query_id bigint,
	OUT connection_info text,
	OUT srespool name,
	OUT global_sessionid text,
	OUT unique_sql_id bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_activity_with_conninfo';

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
                        WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE S.srespool
                        END AS resource_pool,
            S.query_id,
            S.query,
            S.connection_info,
            S.unique_sql_id
    FROM pg_database D, pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid;

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
FROM pg_catalog.pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
where s.query_id = t.queryid and t.ec_operator > 0;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_statistics AS
SELECT t.*
FROM pg_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
where s.query_id = t.queryid;

CREATE OR REPLACE VIEW pg_catalog.pg_get_invalid_backends AS
	SELECT
			C.pid,
			C.node_name,
			S.datname AS dbname,
			S.backend_start,
			S.query
	FROM pg_pool_validate(false, ' ') AS C LEFT JOIN pg_stat_activity AS S
			ON (C.pid = S.sessionid);

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wlm_session_iostat_info(integer) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5014;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_wlm_session_iostat_info(IN unuseattr integer, OUT threadid bigint, OUT maxcurr_iops integer, OUT mincurr_iops integer, OUT maxpeak_iops integer, OUT minpeak_iops integer, OUT iops_limits integer, OUT io_priority integer, OUT curr_io_limits integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_session_iostat_info';

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
	DROP VIEW IF EXISTS DBE_PERF.global_session_stat_activity cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_session_stat_activity() cascade;

	DROP VIEW IF EXISTS DBE_PERF.global_operator_runtime  cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_operator_runtime() cascade;
	DROP VIEW IF EXISTS DBE_PERF.operator_runtime  cascade;

	DROP VIEW IF EXISTS DBE_PERF.session_stat_activity cascade;

	DROP VIEW IF EXISTS DBE_PERF.global_replication_stat cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_replication_stat() cascade;
	DROP VIEW IF EXISTS DBE_PERF.replication_stat cascade;
				
			
	DROP VIEW IF EXISTS DBE_PERF.session_cpu_runtime cascade;
	DROP VIEW IF EXISTS DBE_PERF.session_memory_runtime cascade;
	DROP VIEW IF EXISTS DBE_PERF.global_statement_complex_runtime cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_statement_complex_runtime() cascade;
	DROP VIEW IF EXISTS DBE_PERF.statement_complex_runtime cascade;
				
	DROP VIEW IF EXISTS DBE_PERF.statement_iostat_complex_runtime cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_session_memory_statistics cascade;
	DROP VIEW IF EXISTS pg_catalog.pg_session_iostat cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_session_cpu_statistics cascade;
	DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_statistics cascade;

	DROP VIEW IF EXISTS pg_catalog.pg_stat_activity_ng cascade;
	DROP VIEW IF EXISTS pg_catalog.pg_stat_replication cascade;

	DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT srespool name, OUT global_sessionid text, OUT unique_sql_id bigint, OUT trace_id text) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
	CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_activity
	(
		IN pid bigint,
		OUT datid oid,
		OUT pid bigint,
		OUT sessionid bigint,
		OUT usesysid oid,
		OUT application_name text,
		OUT state text,
		OUT query text,
		OUT waiting boolean,
		OUT xact_start timestamp with time zone,
		OUT query_start timestamp with time zone,
		OUT backend_start timestamp with time zone,
		OUT state_change timestamp with time zone,
		OUT client_addr inet,
		OUT client_hostname text,
		OUT client_port integer,
		OUT enqueue text,
		OUT query_id bigint,
		OUT srespool name,
		OUT global_sessionid text,
		OUT unique_sql_id bigint
	)
	RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_activity';

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
	  WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
	ELSE S.srespool
	END AS resource_pool,
	    S.query_id,
	    S.query,
	    S.unique_sql_id
	FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
	  WHERE S.datid = D.oid AND
	        S.usesysid = U.oid;


	CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_stat_activity
	  (out coorname text, out datid oid, out datname text, out pid bigint,
	   out usesysid oid, out usename text, out application_name text, out client_addr inet,
	   out client_hostname text, out client_port integer, out backend_start timestamptz,
	   out xact_start timestamptz, out query_start timestamptz, out state_change timestamptz,
	   out waiting boolean, out enqueue text, out state text, out resource_pool name,
	   out query_id bigint, out query text, out unique_sql_id bigint)
	RETURNS setof record
	AS $$
	DECLARE
	  row_data dbe_perf.session_stat_activity%rowtype;
	  coor_name record;
	  fet_active text;
	  fetch_coor text;
	  BEGIN
		--Get all cn node names
		fetch_coor := 'select * from dbe_perf.node_name';
		FOR coor_name IN EXECUTE(fetch_coor) LOOP
		  coorname :=  coor_name.node_name;
		  fet_active := 'SELECT * FROM dbe_perf.session_stat_activity';
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
			unique_sql_id := row_data.unique_sql_id;
			return next;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;


	CREATE OR REPLACE VIEW DBE_PERF.global_session_stat_activity AS
	  SELECT * FROM DBE_PERF.get_global_session_stat_activity();
	  
	CREATE OR REPLACE VIEW dbe_perf.operator_runtime AS
	  SELECT t.*
	  FROM dbe_perf.session_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
		WHERE s.query_id = t.queryid;

	CREATE OR REPLACE FUNCTION dbe_perf.get_global_operator_runtime()
	RETURNS setof dbe_perf.operator_runtime
	AS $$
	DECLARE
	  row_data dbe_perf.operator_runtime%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.operator_runtime';
		  FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW dbe_perf.global_operator_runtime AS
	  SELECT * FROM dbe_perf.get_global_operator_runtime();


	CREATE OR REPLACE VIEW dbe_perf.replication_stat AS
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

	CREATE OR REPLACE FUNCTION dbe_perf.get_global_replication_stat
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
	  row_data dbe_perf.replication_stat%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		--Get all the node names
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.replication_stat';
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

	CREATE OR REPLACE VIEW dbe_perf.global_replication_stat AS
	  SELECT * FROM dbe_perf.get_global_replication_stat();


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
                        WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                        ELSE S.srespool
                        END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            S.sessionid = N.sessionid;


	CREATE OR REPLACE VIEW dbe_perf.session_cpu_runtime AS
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
		FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE VIEW dbe_perf.session_memory_runtime AS
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
	  FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE VIEW dbe_perf.statement_complex_runtime AS
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
	  FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
		WHERE S.pid = T.threadid;


	CREATE OR REPLACE FUNCTION dbe_perf.get_global_statement_complex_runtime()
	RETURNS setof dbe_perf.statement_complex_runtime
	AS $$
	DECLARE
	  row_data dbe_perf.statement_complex_runtime%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.statement_complex_runtime';
		  FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW dbe_perf.global_statement_complex_runtime AS
	  SELECT * FROM dbe_perf.get_global_statement_complex_runtime();

	CREATE OR REPLACE VIEW dbe_perf.statement_iostat_complex_runtime AS
	   SELECT
		 S.query_id,
		 T.mincurr_iops as mincurriops,
		 T.maxcurr_iops as maxcurriops,
		 T.minpeak_iops as minpeakiops,
		 T.maxpeak_iops as maxpeakiops,
		 T.iops_limits as io_limits,
		 CASE WHEN T.io_priority = 0 THEN 'None'::text
			  WHEN T.io_priority = 20 THEN 'Low'::text
			  WHEN T.io_priority = 50 THEN 'Medium'::text
			  WHEN T.io_priority = 80 THEN 'High'::text END AS io_priority,
		 S.query,
		 S.node_group,
		 T.curr_io_limits as curr_io_limits
	   FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
		 WHERE S.pid = T.threadid;

  end if;
END$DO$;

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
FROM pg_catalog.pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
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
        S.node_group,
        T.curr_io_limits as curr_io_limits
FROM pg_catalog.pg_stat_activity_ng AS S,  pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
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
            S.pid = W.pid;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DO $DO$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_stat_activity TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
	GRANT SELECT ON TABLE DBE_PERF.session_stat_activity TO PUBLIC;

    query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_session_stat_activity TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_session_stat_activity TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.operator_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.operator_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_operator_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_operator_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.replication_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.replication_stat TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_replication_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_replication_stat TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_activity_ng TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_cpu_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.session_cpu_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.session_memory_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.session_memory_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.statement_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.statement_complex_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_statement_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_statement_complex_runtime TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.statement_iostat_complex_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.statement_iostat_complex_runtime TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_session_memory_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_session_iostat TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_session_cpu_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_replication TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_stat_activity TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_ec_operator_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_wlm_operator_statistics TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_get_invalid_backends TO PUBLIC;
  end if;
END$DO$;

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
    is_slow_sql boolean
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
           OUT details bytea,
           OUT is_slow_sql boolean)
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
                    is_slow_sql := row_data.is_slow_sql;
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
           OUT is_slow_sql boolean)
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
                    is_slow_sql := row_data.is_slow_sql;
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
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='statement_history') THEN
            querystr := 'REVOKE SELECT on table dbe_perf.statement_history FROM public;';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            querystr := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(username) || ';';
            EXECUTE IMMEDIATE querystr;
            GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
        END IF;
    end if;
END$DO$;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_shared_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5255;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_shared_memctx_detail(
IN context_name cstring,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_shared_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_thread_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5256;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_thread_memctx_detail(
IN threadid int8,
IN context_name cstring,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_thread_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_session_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5254;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_session_memctx_detail(
IN context_name cstring,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_session_memctx_detail';DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_set() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_init() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_clear() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_status() CASCADE;DROP FUNCTION IF EXISTS pg_catalog.gs_verify_data_file(bool) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_repair_file(Oid, text, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_verify_and_tryrepair_page(text, oid, boolean, boolean) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_repair_page(text, oid, bool, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.local_bad_block_info() CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.local_clear_bad_block_info() CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_from_remote(oid, oid, oid, integer, integer, integer, xid, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_size_from_remote(oid, oid, oid, integer, integer, xid, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_explain_model(text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float8_array(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_bool(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float4(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float8(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_int32(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_int64(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_numeric(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_text(text, VARIADIC "any") cascade;
do $$DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_clean_history_global_barriers();
        SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4581;
	CREATE FUNCTION pg_catalog.gs_pitr_clean_history_global_barriers
        (
        IN stop_barrier_timestamp pg_catalog.timestamptz,
        OUT oldest_barrier_record pg_catalog.text
        ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_pitr_clean_history_global_barriers';
        DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_archive_slot_force_advance();
	SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4580;
        CREATE FUNCTION pg_catalog.gs_pitr_archive_slot_force_advance
        (
        IN stop_barrier_timestamp pg_catalog.timestamptz,
        OUT archive_restart_lsn pg_catalog.text
        ) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_pitr_archive_slot_force_advance';

    end if;
END$$;
DO
$do$
    DECLARE
        v5r1c20_and_later_version boolean;
        has_version_proc boolean;
        need_upgrade boolean;
    BEGIN
        need_upgrade = false;
        select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
        IF has_version_proc = true  then
            select working_version_num >= 92584 as v5r1c20_and_later_version from working_version_num() into v5r1c20_and_later_version;
            IF v5r1c20_and_later_version = true then
                need_upgrade = true;
            end IF;
        END IF;
        IF need_upgrade = true then

comment on function PG_CATALOG.regexp_count(text, text) is '';
comment on function PG_CATALOG.regexp_count(text, text, integer) is '';
comment on function PG_CATALOG.regexp_count(text, text, integer, text) is '';
comment on function PG_CATALOG.regexp_instr(text, text) is '';
comment on function PG_CATALOG.regexp_instr(text, text, integer) is '';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer) is '';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer) is '';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer, text) is '';
comment on function PG_CATALOG.lpad(text, integer, text) is '';
comment on function PG_CATALOG.rpad(text, integer, text) is '';
comment on function PG_CATALOG.regexp_replace(text, text) is '';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer) is '';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer) is '';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer, text) is '';
comment on function PG_CATALOG.line_in(cstring) is '';
comment on function PG_CATALOG.regexp_substr(text, text, integer) is '';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer) is '';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer, text) is '';
comment on function PG_CATALOG.pg_stat_get_activity(bigint) is '';
comment on function PG_CATALOG.to_char(timestamp without time zone, text) is '';
comment on function PG_CATALOG.pg_replication_origin_advance(text, text) is '';
comment on function PG_CATALOG.pg_replication_origin_create(text) is '';
comment on function PG_CATALOG.pg_replication_origin_drop(text) is '';
comment on function PG_CATALOG.pg_replication_origin_oid(text) is '';
comment on function PG_CATALOG.pg_replication_origin_progress(text, boolean) is '';
comment on function PG_CATALOG.pg_replication_origin_session_is_setup() is '';
comment on function PG_CATALOG.pg_replication_origin_session_progress(boolean) is '';
comment on function PG_CATALOG.pg_replication_origin_session_reset() is '';
comment on function PG_CATALOG.pg_replication_origin_session_setup(text) is '';
comment on function PG_CATALOG.pg_replication_origin_xact_reset() is '';
comment on function PG_CATALOG.pg_replication_origin_xact_setup(text, timestamp with time zone) is '';
comment on function PG_CATALOG.pg_show_replication_origin_status() is '';
comment on function PG_CATALOG.pg_get_publication_tables(text) is '';
comment on function PG_CATALOG.pg_stat_get_subscription(oid) is '';
comment on function PG_CATALOG.xpath(text, xml, text[]) is '';
comment on function PG_CATALOG.xpath(text, xml) is '';
comment on function PG_CATALOG.xpath_exists(text, xml, text[]) is '';
comment on function PG_CATALOG.json_array_element_text(json, integer) is '';
comment on function PG_CATALOG.json_extract_path_op(json, text[]) is '';
comment on function PG_CATALOG.json_extract_path_text_op(json, text[]) is '';
comment on function PG_CATALOG.jsonb_extract_path_op(jsonb, text[]) is '';
comment on function PG_CATALOG.json_object_field(json, text) is '';
comment on function PG_CATALOG.jsonb_array_element(jsonb, integer) is '';
comment on function PG_CATALOG.jsonb_array_element_text(jsonb, integer) is '';
comment on function PG_CATALOG.jsonb_contains(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_eq(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_exists(jsonb, text) is '';
comment on function PG_CATALOG.jsonb_exists_all(jsonb, text[]) is '';
comment on function PG_CATALOG.jsonb_exists_any(jsonb, text[]) is '';
comment on function PG_CATALOG.jsonb_extract_path_text_op(jsonb, text[]) is '';
comment on function PG_CATALOG.jsonb_ge(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_gt(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_le(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_ne(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_object_field(jsonb, text) is '';
comment on function PG_CATALOG.jsonb_object_field_text(jsonb, text) is '';
comment on function PG_CATALOG.json_object_field_text(json, text) is '';
comment on function PG_CATALOG.json_array_element(json, integer) is '';
comment on function PG_CATALOG.jsonb_lt(jsonb, jsonb) is '';
comment on function PG_CATALOG.jsonb_contained(jsonb, jsonb) is '';
comment on function PG_CATALOG.has_any_privilege(name, text) is '';
comment on function PG_CATALOG.int16eq(int16, int16) is '';
comment on function PG_CATALOG.int16ne(int16, int16) is '';
comment on function PG_CATALOG.int16lt(int16, int16) is '';
comment on function PG_CATALOG.int16le(int16, int16) is '';
comment on function PG_CATALOG.int16gt(int16, int16) is '';
comment on function PG_CATALOG.int16ge(int16, int16) is '';
comment on function PG_CATALOG.int16pl(int16, int16) is '';
comment on function PG_CATALOG.int16eq(int16, int16) is '';
comment on function PG_CATALOG.int16mi(int16, int16) is '';
comment on function PG_CATALOG.int16mul(int16, int16) is '';
comment on function PG_CATALOG.int16div(int16, int16) is '';
comment on function PG_CATALOG.array_varchar_first(anyarray) is '';
comment on function PG_CATALOG.array_varchar_last(anyarray) is '';
comment on function PG_CATALOG.array_integer_first(anyarray) is '';
comment on function PG_CATALOG.array_integer_last(anyarray) is '';
comment on function PG_CATALOG.array_indexby_length(anyarray, integer) is '';
comment on function PG_CATALOG.gs_index_verify() is '';
comment on function PG_CATALOG.gs_index_recycle_queue() is '';
comment on function PG_CATALOG.int16div(int16, int16) is '';
        END IF;
    END
$do$;
DROP FUNCTION IF EXISTS pg_catalog.login_audit_messages(boolean); 
DROP FUNCTION IF EXISTS pg_catalog.login_audit_messages_pid(boolean);

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
LANGUAGE plpgsql NOT FENCED;DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, oid, oid, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4770;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer)
 RETURNS bytea
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_segment_block_from_remote$function$;

DROP FUNCTION IF EXISTS pg_catalog.gs_read_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, boolean, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_read_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4767;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, boolean)
 RETURNS bytea
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_block_from_remote$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;


DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4130;
CREATE FUNCTION pg_catalog.pg_buffercache_pages(
 OUT bufferid integer,
 OUT relfilenode oid,
 OUT bucketid smallint,
 OUT storage_type bigint,
 OUT reltablespace oid,
 OUT reldatabase oid,
 OUT relforknumber integer,
 OUT relblocknumber oid,
 OUT isdirty boolean,
 OUT usage_count smallint)
RETURNS SETOF record
LANGUAGE internal
STABLE NOT FENCED NOT SHIPPABLE ROWS 100
AS 'pg_buffercache_pages';


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DO $DO$
DECLARE
  ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select tablename from PG_TABLES where tablename='statement_history' and schemaname='pg_catalog' limit 1) into ans;
    if ans = true then
        TRUNCATE TABLE pg_catalog.statement_history;
        DROP INDEX IF EXISTS pg_catalog.statement_history_time_idx;
        create index pg_catalog.statement_history_time_idx on pg_catalog.statement_history USING btree (start_time, is_slow_sql);
    end if;
END$DO$;DROP FUNCTION IF EXISTS pg_catalog.pg_create_physical_replication_slot_extern(name, boolean, text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_create_physical_replication_slot_extern(name, boolean, text, boolean) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3790;
CREATE OR REPLACE FUNCTION pg_catalog.pg_create_physical_replication_slot_extern(slotname name, dummy_standby boolean, extra_content text, OUT slotname text, OUT xlog_position text)
 RETURNS record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$pg_create_physical_replication_slot_extern$function$;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_count(text, text, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_instr(text, text, int, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_replace(text, text, text, int, int, text) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.regexp_substr(text, text, int, int, text) CASCADE;
