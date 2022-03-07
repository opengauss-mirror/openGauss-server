DROP FUNCTION IF EXISTS pg_catalog.large_seq_rollback_ntree(pg_node_tree);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6016;
CREATE OR REPLACE FUNCTION pg_catalog.large_seq_rollback_ntree(pg_node_tree)
 RETURNS pg_node_tree
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$large_sequence_rollback_node_tree$function$;

DROP FUNCTION IF EXISTS pg_catalog.large_seq_upgrade_ntree(pg_node_tree);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6017;
CREATE OR REPLACE FUNCTION pg_catalog.large_seq_upgrade_ntree(pg_node_tree)
 RETURNS pg_node_tree
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$large_sequence_upgrade_node_tree$function$;

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
BEGIN
  raise info 'Processing sequence APIs';
  FOR i IN 1..rel_array.count LOOP
    raise info '%.%',rel_array[i],att_array[i];
    query_str := 'UPDATE ' || rel_array[i] || ' SET ' || att_array[i] || ' = large_seq_upgrade_ntree(' || att_array[i] || ' ) WHERE ' || att_array[i] || ' LIKE ''%:funcid 1574 :%'' OR ' || att_array[i] || ' LIKE ''%:funcid 1575 :%'' OR ' || att_array[i] || ' LIKE ''%:funcid 2559 :%'';';
    EXECUTE query_str;
  END LOOP;
END
$do$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_all_args_nsp_index;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9666;
CREATE INDEX pg_catalog.pg_proc_proname_all_args_nsp_index on pg_catalog.pg_proc USING BTREE(proname name_ops, allargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
REINDEX INDEX pg_catalog.pg_proc_proname_all_args_nsp_index;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_shared_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5255;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_shared_memctx_detail(
text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_shared_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_thread_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5256;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_thread_memctx_detail(
int8,
text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_thread_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_session_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5254;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_session_memctx_detail(
text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_session_memctx_detail';DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9377;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text)
 RETURNS SETOF RECORD
 LANGUAGE internal
AS $function$gs_get_parallel_decode_status$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DROP FUNCTION IF EXISTS pg_catalog.gs_index_advise(cstring);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4888;
CREATE OR REPLACE FUNCTION pg_catalog.gs_index_advise(sql_string cstring, OUT schema text, OUT "table" text, OUT "column" text, OUT indextype text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$gs_index_advise$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- gs_parse_page_bypath 
DROP FUNCTION IF EXISTS pg_catalog.gs_parse_page_bypath(text, bigint, text, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2620;
CREATE OR REPLACE FUNCTION pg_catalog.gs_parse_page_bypath(path text, blocknum bigint, relation_type text, read_memory boolean, OUT output_filepath text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_parse_page_bypath$function$;
comment on function PG_CATALOG.gs_parse_page_bypath(path text, blocknum bigint, relation_type text, read_memory boolean) is 'parse data page to output file based on given filepath';

-- gs_xlogdump_lsn
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_lsn(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2619;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlogdump_lsn(start_lsn text, end_lsn text, OUT output_filepath text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_xlogdump_lsn$function$;
comment on function PG_CATALOG.gs_xlogdump_lsn(start_lsn text, end_lsn text) is 'dump xlog records to output file based on the given start_lsn and end_lsn';

-- gs_xlogdump_xid
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_xid(xid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2617;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlogdump_xid(c_xid xid, OUT output_filepath text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_xlogdump_xid$function$;
comment on function PG_CATALOG.gs_xlogdump_xid(c_xid xid) is 'dump xlog records to output file based on the given xid';

-- gs_xlogdump_tablepath
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_tablepath(text, bigint, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2616;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlogdump_tablepath(path text, blocknum bigint, relation_type text, OUT output_filepath text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_xlogdump_tablepath$function$;
comment on function PG_CATALOG.gs_xlogdump_tablepath(path text, blocknum bigint, relation_type text) is 'dump xlog records to output file based on given filepath';

-- gs_xlogdump_parsepage_tablepath
DROP FUNCTION IF EXISTS pg_catalog.gs_xlogdump_parsepage_tablepath(text, bigint, text, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2618;
CREATE OR REPLACE FUNCTION pg_catalog.gs_xlogdump_parsepage_tablepath(path text, blocknum bigint, relation_type text, read_memory boolean, OUT output_filepath text)
    RETURNS text
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_xlogdump_parsepage_tablepath$function$;
comment on function PG_CATALOG.gs_xlogdump_parsepage_tablepath(path text, blocknum bigint, relation_type text, read_memory boolean) is 'parse data page to output file based on given filepath';
DROP FUNCTION IF EXISTS pg_catalog.local_xlog_redo_statics();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4390;
CREATE FUNCTION pg_catalog.local_xlog_redo_statics
(
OUT xlog_type pg_catalog.text,
OUT rmid pg_catalog.int4,
OUT info pg_catalog.int4,
OUT num pg_catalog.int8,
OUT extra pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_xlog_redo_statics';

DROP FUNCTION IF EXISTS pg_catalog.local_redo_time_count();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4391;
CREATE FUNCTION pg_catalog.local_redo_time_count
(
OUT thread_name pg_catalog.text,
OUT step1_total pg_catalog.int8,
OUT step1_count pg_catalog.int8,
OUT step2_total pg_catalog.int8,
OUT step2_count pg_catalog.int8,
OUT step3_total pg_catalog.int8,
OUT step3_count pg_catalog.int8,
OUT step4_total pg_catalog.int8,
OUT step4_count pg_catalog.int8,
OUT step5_total pg_catalog.int8,
OUT step5_count pg_catalog.int8,
OUT step6_total pg_catalog.int8,
OUT step6_count pg_catalog.int8,
OUT step7_total pg_catalog.int8,
OUT step7_count pg_catalog.int8,
OUT step8_total pg_catalog.int8,
OUT step8_count pg_catalog.int8,
OUT step9_total pg_catalog.int8,
OUT step9_count pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_redo_time_count';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP VIEW IF EXISTS pg_catalog.gs_wlm_ec_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_get_invalid_backends CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_stat_activity cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT connection_info text, OUT srespool name, OUT global_sessionid text) cascade;

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

	DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT srespool name, OUT global_sessionid text) cascade;

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
                        unique_sql_id := unique_sql_id;
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
-- regexp_count
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 385;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 386;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 387;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text, int, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_matchopt$function$;

-- regexp_instr
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 630;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 631;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 632;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_occurren$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 633;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_returnopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 634;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int, int, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_matchopt$function$;

-- regexp_replace
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1116;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1117;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1118;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_occur$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1119;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int, int, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_matchopt$function$;

-- regexp_substr
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1566;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1567;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_occur$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1568;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int, int, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_opt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;DROP FUNCTION IF EXISTS pg_catalog.local_double_write_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4384;
CREATE FUNCTION pg_catalog.local_double_write_stat
(
	OUT node_name pg_catalog.text,
	OUT file_id pg_catalog.int8,
	OUT curr_dwn pg_catalog.int8,
	OUT curr_start_page pg_catalog.int8,
	OUT file_trunc_num pg_catalog.int8,
	OUT file_reset_num pg_catalog.int8,
	OUT total_writes pg_catalog.int8,
	OUT low_threshold_writes pg_catalog.int8,
	OUT high_threshold_writes pg_catalog.int8,
	OUT total_pages pg_catalog.int8,
	OUT low_threshold_pages pg_catalog.int8,
	OUT high_threshold_pages pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_double_write_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_double_write_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4385;
CREATE FUNCTION pg_catalog.remote_double_write_stat
(
	OUT node_name pg_catalog.text,
	OUT file_id pg_catalog.int8,
	OUT curr_dwn pg_catalog.int8,
	OUT curr_start_page pg_catalog.int8,
	OUT file_trunc_num pg_catalog.int8,
	OUT file_reset_num pg_catalog.int8,
	OUT total_writes pg_catalog.int8,
	OUT low_threshold_writes pg_catalog.int8,
	OUT high_threshold_writes pg_catalog.int8,
	OUT total_pages pg_catalog.int8,
	OUT low_threshold_pages pg_catalog.int8,
	OUT high_threshold_pages pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_double_write_stat';

DROP VIEW IF EXISTS DBE_PERF.global_double_write_status CASCADE;
CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
    SELECT node_name, file_id, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           total_writes, low_threshold_writes, high_threshold_writes,
           total_pages, low_threshold_pages, high_threshold_pages
    FROM pg_catalog.local_double_write_stat();

REVOKE ALL on DBE_PERF.global_double_write_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_double_write_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_double_write_status TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.gs_get_active_archiving_standby();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4579;
CREATE FUNCTION pg_catalog.gs_get_active_archiving_standby
(
OUT standby_name pg_catalog.text,
OUT archive_location pg_catalog.text,
OUT archived_file_num pg_catalog.int4
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_get_active_archiving_standby';

DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_get_warning_for_xlog_force_recycle();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4582;
CREATE FUNCTION pg_catalog.gs_pitr_get_warning_for_xlog_force_recycle
(
OUT xlog_force_recycled pg_catalog.bool
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_pitr_get_warning_for_xlog_force_recycle';

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
DROP FUNCTION IF EXISTS pg_catalog.gs_get_standby_cluster_barrier_status() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9039;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_standby_cluster_barrier_status
(  OUT barrier_id text,
   OUT barrier_lsn text,
   OUT recovery_id text,
   OUT target_id text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_standby_cluster_barrier_status';
DROP FUNCTION IF EXISTS pg_catalog.gs_set_standby_cluster_target_barrier_id() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9037;
CREATE OR REPLACE FUNCTION pg_catalog.gs_set_standby_cluster_target_barrier_id
(  IN barrier_id text,
   OUT target_id text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_set_standby_cluster_target_barrier_id';
DROP FUNCTION IF EXISTS pg_catalog.gs_query_standby_cluster_barrier_id_exist() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9038;
CREATE OR REPLACE FUNCTION pg_catalog.gs_query_standby_cluster_barrier_id_exist
(  IN barrier_id text,
   OUT target_id bool)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_query_standby_cluster_barrier_id_exist';
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
OUT rto_sleep_time pg_catalog.int8,
OUT rpo_sleep_time pg_catalog.int8
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
OUT rto_sleep_time pg_catalog.int8,
OUT rpo_sleep_time pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_hadr_remote_rto_and_rpo_stat';
CREATE OR REPLACE VIEW DBE_PERF.global_streaming_hadr_rto_and_rpo_stat AS
    SELECT hadr_sender_node_name, hadr_receiver_node_name, current_rto, target_rto, current_rpo, target_rpo, rto_sleep_time, rpo_sleep_time
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
CREATE OR REPLACE VIEW pg_catalog.gs_gsc_memory_detail AS
    SELECT db_id, sum(totalsize) AS totalsize, sum(freesize) AS freesize, sum(usedsize) AS usedsize 
    FROM (
        SELECT 
        	CASE WHEN contextname like '%GlobalSysDBCacheEntryMemCxt%' THEN substring(contextname, 29)
        	ELSE substring(parent, 29) END AS db_id,
        	totalsize,
        	freesize,
        	usedsize
        FROM pg_shared_memory_detail()  
        WHERE contextname LIKE '%GlobalSysDBCacheEntryMemCxt%' OR parent LIKE '%GlobalSysDBCacheEntryMemCxt%'
        )a 
    GROUP BY db_id;
GRANT SELECT ON TABLE pg_catalog.gs_gsc_memory_detail TO PUBLIC;
CREATE OR REPLACE VIEW pg_catalog.gs_lsc_memory_detail AS
SELECT * FROM pv_thread_memory_detail() WHERE contextname LIKE '%LocalSysCache%' OR parent LIKE '%LocalSysCache%';
GRANT SELECT ON TABLE pg_catalog.gs_lsc_memory_detail TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9123;
CREATE OR REPLACE FUNCTION pg_catalog.gs_gsc_table_detail(database_oid bigint DEFAULT NULL::bigint, rel_oid bigint DEFAULT NULL::bigint, OUT database_oid oid, OUT database_name text, OUT reloid oid, OUT relname text, OUT relnamespace oid, OUT reltype oid, OUT reloftype oid, OUT relowner oid, OUT relam oid, OUT relfilenode oid, OUT reltablespace oid, OUT relhasindex boolean, OUT relisshared boolean, OUT relkind "char", OUT relnatts smallint, OUT relhasoids boolean, OUT relhaspkey boolean, OUT parttype "char", OUT tdhasuids boolean, OUT attnames text, OUT extinfo text)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$gs_gsc_table_detail$function$;


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9122;
CREATE OR REPLACE FUNCTION pg_catalog.gs_gsc_catalog_detail(database_id bigint DEFAULT NULL::bigint, rel_id bigint DEFAULT NULL::bigint, OUT database_id bigint, OUT database_name text, OUT rel_id bigint, OUT rel_name text, OUT cache_id bigint, OUT self text, OUT ctid text, OUT infomask bigint, OUT infomask2 bigint, OUT hash_value bigint, OUT refcount bigint)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$gs_gsc_catalog_detail$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9121;
CREATE OR REPLACE FUNCTION pg_catalog.gs_gsc_dbstat_info(database_id bigint DEFAULT NULL::bigint, OUT database_id bigint, OUT database_name text, OUT tup_searches bigint, OUT tup_hits bigint, OUT tup_miss bigint, OUT tup_count bigint, OUT tup_dead bigint, OUT tup_memory bigint, OUT rel_searches bigint, OUT rel_hits bigint, OUT rel_miss bigint, OUT rel_count bigint, OUT rel_dead bigint, OUT rel_memory bigint, OUT part_searches bigint, OUT part_hits bigint, OUT part_miss bigint, OUT part_count bigint, OUT part_dead bigint, OUT part_memory bigint, OUT total_memory bigint, OUT swapout_count bigint, OUT refcount bigint)
 RETURNS SETOF record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$gs_gsc_dbstat_info$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9120;
CREATE OR REPLACE FUNCTION pg_catalog.gs_gsc_clean(database_id bigint DEFAULT NULL::bigint)
 RETURNS boolean
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$gs_gsc_clean$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
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
	OUT global_sessionid text,
	OUT unique_sql_id bigint,
	OUT trace_id text
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
            S.unique_sql_id,
            S.trace_id
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
		OUT global_sessionid text,
		OUT unique_sql_id bigint,
		OUT trace_id text
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
	    S.unique_sql_id,
	    S.trace_id
	FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
	  WHERE S.datid = D.oid AND
	        S.usesysid = U.oid;


	CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_stat_activity
	  (out coorname text, out datid oid, out datname text, out pid bigint,
	   out usesysid oid, out usename text, out application_name text, out client_addr inet,
	   out client_hostname text, out client_port integer, out backend_start timestamptz,
	   out xact_start timestamptz, out query_start timestamptz, out state_change timestamptz,
	   out waiting boolean, out enqueue text, out state text, out resource_pool name,
	   out query_id bigint, out query text, out unique_sql_id bigint, out trace_id text)
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
            trace_id := row_data.trace_id;
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
           OUT is_slow_sql boolean,
           OUT trace_id text)
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
                    trace_id := row_data.trace_id;
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
END$DO$;DROP FUNCTION IF EXISTS pg_catalog.gs_get_shared_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5255;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_shared_memctx_detail(
IN context_name text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_shared_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_thread_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5256;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_thread_memctx_detail(
IN threadid int8,
IN context_name text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_thread_memctx_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_session_memctx_detail() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5254;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_session_memctx_detail(
IN context_name text,
OUT file text,
OUT line int8,
OUT size int8) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_get_session_memctx_detail';DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_set() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3268;
CREATE FUNCTION pg_catalog.pgxc_disaster_read_set
(text, OUT set_ok boolean)
RETURNS SETOF boolean LANGUAGE INTERNAL ROWS 1 STRICT as 'pgxc_disaster_read_set';

DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_init() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3269;
CREATE FUNCTION pg_catalog.pgxc_disaster_read_init
(OUT init_ok boolean)
RETURNS SETOF boolean LANGUAGE INTERNAL ROWS 1 STRICT as 'pgxc_disaster_read_init';

DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_clear() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3271;
CREATE FUNCTION pg_catalog.pgxc_disaster_read_clear
(OUT clear_ok boolean)
RETURNS SETOF boolean LANGUAGE INTERNAL ROWS 1 STRICT as 'pgxc_disaster_read_clear';

DROP FUNCTION IF EXISTS pg_catalog.pgxc_disaster_read_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3273;
CREATE FUNCTION pg_catalog.pgxc_disaster_read_status
(
OUT node_oid pg_catalog.oid,
OUT node_type pg_catalog.text,
OUT host pg_catalog.text,
OUT port pg_catalog.int4,
OUT host1 pg_catalog.text,
OUT port1 pg_catalog.int4,
OUT xlogMaxCSN pg_catalog.int8,
OUT consistency_point_csn pg_catalog.int8
)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE ROWS 100 COST 100 STRICT as 'pgxc_disaster_read_status';DROP FUNCTION IF EXISTS pg_catalog.gs_verify_data_file(bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4571;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_data_file(verify_segment boolean DEFAULT false, OUT node_name text, OUT rel_oid oid, OUT rel_name text, OUT miss_file_path text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_verify_data_file$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_repair_file(Oid, text, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4771;
CREATE OR REPLACE FUNCTION pg_catalog.gs_repair_file(tableoid oid, path text, timeout integer)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_repair_file$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_verify_and_tryrepair_page(text, oid, boolean, boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4569;
CREATE OR REPLACE FUNCTION pg_catalog.gs_verify_and_tryrepair_page(path text, blocknum oid, verify_mem boolean, is_segment boolean, OUT node_name text, OUT path text, OUT blocknum oid, OUT disk_page_res text, OUT mem_page_res text, OUT is_repair boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_verify_and_tryrepair_page$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_repair_page(text, oid, bool, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4570;
CREATE OR REPLACE FUNCTION pg_catalog.gs_repair_page(path text, blocknum oid, is_segment boolean, timeout integer, OUT result boolean)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_repair_page$function$;


DROP FUNCTION IF EXISTS pg_catalog.local_bad_block_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4567;
CREATE OR REPLACE FUNCTION pg_catalog.local_bad_block_info(OUT node_name text, OUT spc_node oid, OUT db_node oid, OUT rel_node oid, OUT bucket_node integer, OUT fork_num integer, OUT block_num integer, OUT file_path text, OUT check_time timestamp with time zone, OUT repair_time timestamp with time zone)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE
AS $function$local_bad_block_info$function$;


DROP FUNCTION IF EXISTS pg_catalog.local_clear_bad_block_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4568;
CREATE OR REPLACE FUNCTION pg_catalog.local_clear_bad_block_info(OUT result boolean)
 RETURNS SETOF boolean
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE
AS $function$local_clear_bad_block_info$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_from_remote(oid, oid, oid, integer, integer, integer, xid, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4768;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_file_from_remote(oid, oid, oid, integer, integer, integer, xid, integer, OUT bytea, OUT xid)
 RETURNS record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_file_from_remote$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_read_file_size_from_remote(oid, oid, oid, integer, integer, xid, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4769;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_file_size_from_remote(oid, oid, oid, integer, integer, xid, integer, OUT bigint)
 RETURNS bigint
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_file_size_from_remote$function$;


DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4770;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer)
 RETURNS bytea
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_segment_block_from_remote$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;


DROP FUNCTION IF EXISTS pg_catalog.gs_explain_model(text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float8_array(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_bool(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float4(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_float8(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_int32(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_int64(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_numeric(text, VARIADIC "any") cascade;
DROP FUNCTION IF EXISTS pg_catalog.db4ai_predict_by_text(text, VARIADIC "any") cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7101;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_bool(text, VARIADIC "any")
 RETURNS boolean
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_bool$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7105;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_float4(text, VARIADIC "any")
 RETURNS real
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_float4$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7106;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_float8(text, VARIADIC "any")
 RETURNS double precision
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_float8$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7102;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_int32(text, VARIADIC "any")
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_int32$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7103;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_int64(text, VARIADIC "any")
 RETURNS bigint
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_int64$function$;


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7107;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_numeric(text, VARIADIC "any")
 RETURNS numeric
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_numeric$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7108;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_text(text, VARIADIC "any")
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_text$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7109;
CREATE OR REPLACE FUNCTION pg_catalog.db4ai_predict_by_float8_array(text, VARIADIC "any")
 RETURNS double precision[]
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$db4ai_predict_by_float8_array$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7110;
CREATE OR REPLACE FUNCTION pg_catalog.gs_explain_model(text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_explain_model$function$;
comment on function PG_CATALOG.gs_explain_model(text) is 'explain machine learning model';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_clean_history_global_barriers();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4581;
CREATE FUNCTION pg_catalog.gs_pitr_clean_history_global_barriers
(
IN stop_barrier_timestamp cstring,
OUT oldest_barrier_record text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_pitr_clean_history_global_barriers';

DROP FUNCTION IF EXISTS pg_catalog.gs_pitr_archive_slot_force_advance();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4580;
CREATE FUNCTION pg_catalog.gs_pitr_archive_slot_force_advance
(
IN stop_barrier_timestamp cstring,
OUT archive_restart_lsn text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_pitr_archive_slot_force_advance';
DROP FUNCTION IF EXISTS pg_catalog.local_double_write_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4384;
CREATE FUNCTION pg_catalog.local_double_write_stat
(
    OUT node_name pg_catalog.text,
    OUT curr_dwn pg_catalog.int8,
    OUT curr_start_page pg_catalog.int8,
    OUT file_trunc_num pg_catalog.int8,
    OUT file_reset_num pg_catalog.int8,
    OUT total_writes pg_catalog.int8,
    OUT low_threshold_writes pg_catalog.int8,
    OUT high_threshold_writes pg_catalog.int8,
    OUT total_pages pg_catalog.int8,
    OUT low_threshold_pages pg_catalog.int8,
    OUT high_threshold_pages pg_catalog.int8,
    OUT file_id pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_double_write_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_double_write_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4385;
CREATE FUNCTION pg_catalog.remote_double_write_stat
(
    OUT node_name pg_catalog.text,
    OUT curr_dwn pg_catalog.int8,
    OUT curr_start_page pg_catalog.int8,
    OUT file_trunc_num pg_catalog.int8,
    OUT file_reset_num pg_catalog.int8,
    OUT total_writes pg_catalog.int8,
    OUT low_threshold_writes pg_catalog.int8,
    OUT high_threshold_writes pg_catalog.int8,
    OUT total_pages pg_catalog.int8,
    OUT low_threshold_pages pg_catalog.int8,
    OUT high_threshold_pages pg_catalog.int8,
    OUT file_id pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_double_write_stat';

DROP VIEW IF EXISTS DBE_PERF.global_double_write_status CASCADE;
CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
    SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           total_writes, low_threshold_writes, high_threshold_writes,
           total_pages, low_threshold_pages, high_threshold_pages, file_id
    FROM pg_catalog.local_double_write_stat();

REVOKE ALL on DBE_PERF.global_double_write_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_double_write_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_double_write_status TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_double_write_status' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
        alter table snapshot.snap_global_double_write_status
                ADD COLUMN snap_file_id int8;
  end if;
END$DO$;
comment on function PG_CATALOG.regexp_count(text, text) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_count(text, text, integer) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_count(text, text, integer, text) is 'find match(es) count for regexp';
comment on function PG_CATALOG.regexp_instr(text, text) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer) is 'find match(es) position for regexp';
comment on function PG_CATALOG.regexp_instr(text, text, integer, integer, integer, text) is 'find match(es) position for regexp';
comment on function PG_CATALOG.lpad(text, integer, text) is 'left-pad string to length';
comment on function PG_CATALOG.rpad(text, integer, text) is 'right-pad string to length';
comment on function PG_CATALOG.regexp_replace(text, text) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer) is 'replace text using regexp';
comment on function PG_CATALOG.regexp_replace(text, text, text, integer, integer, text) is 'replace text using regexp';
comment on function PG_CATALOG.line_in(cstring) is 'I/O';
comment on function PG_CATALOG.regexp_substr(text, text, integer) is 'extract text matching regular expression';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer) is 'extract text matching regular expression';
comment on function PG_CATALOG.regexp_substr(text, text, integer, integer, text) is 'extract text matching regular expression';
comment on function PG_CATALOG.pg_stat_get_activity(bigint) is 'statistics: information about currently active backends';
comment on function PG_CATALOG.to_char(timestamp without time zone, text) is 'format timestamp to text';
comment on function PG_CATALOG.pg_replication_origin_advance(text, text) is 'advance replication itentifier to specific location';
comment on function PG_CATALOG.pg_replication_origin_create(text) is 'create a replication origin';
comment on function PG_CATALOG.pg_replication_origin_drop(text) is 'drop replication origin identified by its name';
comment on function PG_CATALOG.pg_replication_origin_oid(text) is 'translate the replication origin\''s name to its id';
comment on function PG_CATALOG.pg_replication_origin_progress(text, boolean) is 'get an individual replication origin\''s replication progress';
comment on function PG_CATALOG.pg_replication_origin_session_is_setup() is 'is a replication origin configured in this session';
comment on function PG_CATALOG.pg_replication_origin_session_progress(boolean) is 'get the replication progress of the current session';
comment on function PG_CATALOG.pg_replication_origin_session_reset() is 'teardown configured replication progress tracking';
comment on function PG_CATALOG.pg_replication_origin_session_setup(text) is 'configure session to maintain replication progress tracking for the passed in origin';
comment on function PG_CATALOG.pg_replication_origin_xact_reset() is 'reset the transaction\''s origin lsn and timestamp';
comment on function PG_CATALOG.pg_replication_origin_xact_setup(text, timestamp with time zone) is 'setup the transaction\''s origin lsn and timestamp';
comment on function PG_CATALOG.pg_show_replication_origin_status() is 'get progress for all replication origins';
comment on function PG_CATALOG.pg_get_publication_tables(text) is 'get OIDs of tables in a publication';
comment on function PG_CATALOG.pg_stat_get_subscription(oid) is 'statistics: information about subscription';
comment on function PG_CATALOG.xpath(text, xml, text[]) is 'evaluate XPath expression, with namespaces support';
comment on function PG_CATALOG.xpath(text, xml) is 'evaluate XPath expression';
comment on function PG_CATALOG.xpath_exists(text, xml, text[]) is 'test XML value against XPath expression, with namespace support';
comment on function PG_CATALOG.json_array_element_text(json, integer) is 'implementation of ->> operator';
comment on function PG_CATALOG.json_extract_path_op(json, text[]) is 'implementation of #> operator';
comment on function PG_CATALOG.json_extract_path_text_op(json, text[]) is 'implementation of #>> operator';
comment on function PG_CATALOG.jsonb_extract_path_op(jsonb, text[]) is 'implementation of #> operator';
comment on function PG_CATALOG.json_object_field(json, text) is 'implementation of -> operator';
comment on function PG_CATALOG.jsonb_array_element(jsonb, integer) is 'implementation of -> operator';
comment on function PG_CATALOG.jsonb_array_element_text(jsonb, integer) is 'implementation of ->> operator';
comment on function PG_CATALOG.jsonb_contains(jsonb, jsonb) is 'implementation of @> operator';
comment on function PG_CATALOG.jsonb_eq(jsonb, jsonb) is 'implementation of = operator';
comment on function PG_CATALOG.jsonb_exists(jsonb, text) is 'implementation of ? operator';
comment on function PG_CATALOG.jsonb_exists_all(jsonb, text[]) is 'implementation of ?& operator';
comment on function PG_CATALOG.jsonb_exists_any(jsonb, text[]) is 'implementation of ?| operator';
comment on function PG_CATALOG.jsonb_extract_path_text_op(jsonb, text[]) is 'implementation of #>> operator';
comment on function PG_CATALOG.jsonb_ge(jsonb, jsonb) is 'implementation of >= operator';
comment on function PG_CATALOG.jsonb_gt(jsonb, jsonb) is 'implementation of > operator';
comment on function PG_CATALOG.jsonb_le(jsonb, jsonb) is 'implementation of <= operator';
comment on function PG_CATALOG.jsonb_ne(jsonb, jsonb) is 'implementation of <> operator';
comment on function PG_CATALOG.jsonb_object_field(jsonb, text) is 'implementation of -> operator';
comment on function PG_CATALOG.jsonb_object_field_text(jsonb, text) is 'implementation of ->> operator';
comment on function PG_CATALOG.json_object_field_text(json, text) is 'implementation of ->> operator';
comment on function PG_CATALOG.json_array_element(json, integer) is 'implementation of -> operator';
comment on function PG_CATALOG.jsonb_lt(jsonb, jsonb) is 'implementation of < operator';
comment on function PG_CATALOG.jsonb_contained(jsonb, jsonb) is 'implementation of <@ operator';
comment on function PG_CATALOG.has_any_privilege(name, text) is 'current user privilege on database level';
comment on function PG_CATALOG.int16eq(int16, int16) is 'implementation of = operator';
comment on function PG_CATALOG.int16ne(int16, int16) is 'implementation of <> operator';
comment on function PG_CATALOG.int16lt(int16, int16) is 'implementation of < operator';
comment on function PG_CATALOG.int16le(int16, int16) is 'implementation of <= operator';
comment on function PG_CATALOG.int16gt(int16, int16) is 'implementation of > operator';
comment on function PG_CATALOG.int16ge(int16, int16) is 'implementation of >= operator';
comment on function PG_CATALOG.int16pl(int16, int16) is 'implementation of + operator';
comment on function PG_CATALOG.int16eq(int16, int16) is 'implementation of = operator';
comment on function PG_CATALOG.int16mi(int16, int16) is 'implementation of - operator';
comment on function PG_CATALOG.int16mul(int16, int16) is 'implementation of * operator';
comment on function PG_CATALOG.int16div(int16, int16) is 'implementation of / operator';
comment on function PG_CATALOG.array_varchar_first(anyarray) is 'array_varchar_first';
comment on function PG_CATALOG.array_varchar_last(anyarray) is 'array_varchar_last';
comment on function PG_CATALOG.array_integer_first(anyarray) is 'array_integer_first';
comment on function PG_CATALOG.array_integer_last(anyarray) is 'array_integer_last';
comment on function PG_CATALOG.array_indexby_length(anyarray, integer) is 'array index by length';
comment on function PG_CATALOG.gs_index_verify() is 'array index by length';
comment on function PG_CATALOG.gs_index_recycle_queue() is 'array index by length';
comment on function PG_CATALOG.int16div(int16, int16) is 'array index by length';
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
    SELECT text(oid) FROM pg_catalog.pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT pg_catalog.CURRENT_DATABASE() INTO db_name;
    IF flag = true THEN 
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
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
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
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
    SELECT text(oid) FROM pg_catalog.pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT pg_catalog.CURRENT_DATABASE() INTO db_name;
    SELECT pg_catalog.pg_backend_pid() INTO mybackendid;
    curSessionFound = false;    
    IF flag = true THEN 
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
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
        SQL_STMT := 'SELECT username,database,time,type,result,client_conninfo, split_part(thread_id,''@'',1) backendid FROM pg_catalog.pg_query_audit(''1970-1-1'',''9999-12-31'') WHERE 
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
LANGUAGE plpgsql NOT FENCED;DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_read_segment_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, oid, oid, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4770;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_segment_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, oid, oid, integer)
 RETURNS bytea
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS $function$gs_read_segment_block_from_remote$function$;

DROP FUNCTION IF EXISTS pg_catalog.gs_read_block_from_remote(integer, integer, integer, smallint, integer, xid, integer, xid, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_read_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, boolean, integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4767;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote(oid, oid, oid, smallint, integer, xid, integer, xid, boolean, integer)
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
 OUT bucketid integer,
 OUT storage_type bigint,
 OUT reltablespace oid,
 OUT reldatabase oid,
 OUT relforknumber integer,
 OUT relblocknumber oid,
 OUT isdirty boolean,
 OUT isvalid boolean,
 OUT usage_count smallint,
 OUT pinning_backends integer)
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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3790;
CREATE OR REPLACE FUNCTION pg_catalog.pg_create_physical_replication_slot_extern(slotname name, dummy_standby boolean, extra_content text, need_recycle_xlog boolean, OUT slotname text, OUT xlog_position text)
 RETURNS record
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS $function$pg_create_physical_replication_slot_extern$function$;
SET search_path TO information_schema;

-- element_types is generated by data_type_privileges
DROP VIEW IF EXISTS information_schema.element_types CASCADE;

-- data_type_privileges is generated by columns
DROP VIEW IF EXISTS information_schema.data_type_privileges CASCADE;
-- data_type_privileges is generated by table_privileges
DROP VIEW IF EXISTS information_schema.role_column_grants CASCADE;
-- data_type_privileges is generated by column_privileges
DROP VIEW IF EXISTS information_schema.role_table_grants CASCADE;

-- other views need upgrade for matview
DROP VIEW IF EXISTS information_schema.column_domain_usage CASCADE;
DROP VIEW IF EXISTS information_schema.column_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.column_udt_usage CASCADE;
DROP VIEW IF EXISTS information_schema.columns CASCADE;
DROP VIEW IF EXISTS information_schema.table_privileges CASCADE;
DROP VIEW IF EXISTS information_schema.tables CASCADE;
DROP VIEW IF EXISTS information_schema.view_column_usage CASCADE;
DROP VIEW IF EXISTS information_schema.view_table_usage CASCADE;

CREATE VIEW information_schema.column_domain_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS domain_catalog,
           CAST(nt.nspname AS sql_identifier) AS domain_schema,
           CAST(t.typname AS sql_identifier) AS domain_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_type t, pg_namespace nt, pg_class c, pg_namespace nc,
         pg_attribute a

    WHERE t.typnamespace = nt.oid
          AND c.relnamespace = nc.oid
          AND a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND t.typtype = 'd'
          AND c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND pg_has_role(t.typowner, 'USAGE');

CREATE VIEW information_schema.column_privileges AS
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
                 WHERE relkind IN ('r', 'm', 'v', 'f')
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
                 AND relkind IN ('r', 'm', 'v', 'f')
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
          AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES', 'COMMENT')
          AND (x.relname not like 'mlog\_%' AND x.relname not like 'matviewmap\_%')
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.column_udt_usage AS
    SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_attribute a, pg_class c, pg_namespace nc,
         (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid))
           LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)

    WHERE a.attrelid = c.oid
          AND a.atttypid = t.oid
          AND nc.oid = c.relnamespace
          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
          AND pg_has_role(coalesce(bt.typowner, t.typowner), 'USAGE');

CREATE VIEW information_schema.columns AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name,
           CAST(a.attnum AS cardinal_number) AS ordinal_position,
           CAST(CASE WHEN ad.adgencol <> 's' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS column_default,
           CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
             AS yes_or_no)
             AS is_nullable,

           CAST(
             CASE WHEN t.typtype = 'd' THEN
               CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                    WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                    ELSE 'USER-DEFINED' END
             ELSE
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                    ELSE 'USER-DEFINED' END
             END
             AS character_data)
             AS data_type,

           CAST(
             _pg_char_max_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_maximum_length,

           CAST(
             _pg_char_octet_length(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS character_octet_length,

           CAST(
             _pg_numeric_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision,

           CAST(
             _pg_numeric_precision_radix(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_precision_radix,

           CAST(
             _pg_numeric_scale(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS numeric_scale,

           CAST(
             _pg_datetime_precision(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS cardinal_number)
             AS datetime_precision,

           CAST(
             _pg_interval_type(_pg_truetypid(a, t), _pg_truetypmod(a, t))
             AS character_data)
             AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,

           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,

           CAST(CASE WHEN t.typtype = 'd' THEN current_database() ELSE null END
             AS sql_identifier) AS domain_catalog,
           CAST(CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE null END
             AS sql_identifier) AS domain_schema,
           CAST(CASE WHEN t.typtype = 'd' THEN t.typname ELSE null END
             AS sql_identifier) AS domain_name,

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
           CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST(a.attnum AS sql_identifier) AS dtd_identifier,
           CAST('NO' AS yes_or_no) AS is_self_referencing,

           CAST('NO' AS yes_or_no) AS is_identity,
           CAST(null AS character_data) AS identity_generation,
           CAST(null AS character_data) AS identity_start,
           CAST(null AS character_data) AS identity_increment,
           CAST(null AS character_data) AS identity_maximum,
           CAST(null AS character_data) AS identity_minimum,
           CAST(null AS yes_or_no) AS identity_cycle,

           CAST(CASE WHEN ad.adgencol = 's' THEN 'ALWAYS' ELSE 'NEVER' END AS character_data) AS is_generated,
           CAST(CASE WHEN ad.adgencol = 's' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS generation_expression,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '2' AND is_instead)
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '4' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable

    FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
         JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
         JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
         LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
           ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE (NOT pg_is_other_temp_schema(nc.oid))

          AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind in ('r', 'm', 'v', 'f')

          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')

          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_column_privilege(c.oid, a.attnum,
                                       'SELECT, INSERT, UPDATE, REFERENCES'));

CREATE VIEW information_schema.table_privileges AS
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
          AND c.relkind IN ('r', 'm', 'v')
          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
          AND c.grantee = grantee.oid
          AND c.grantor = u_grantor.oid
          AND (c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
               OR c.prtype IN ('ALTER', 'DROP', 'COMMENT', 'INDEX', 'VACUUM')
          )
          AND (pg_has_role(u_grantor.oid, 'USAGE')
               OR pg_has_role(grantee.oid, 'USAGE')
               OR grantee.rolname = 'PUBLIC');

CREATE VIEW information_schema.tables AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,

           CAST(
             CASE WHEN nc.oid = pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                  WHEN c.relkind = 'r' THEN 'BASE TABLE'
                  WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'
                  WHEN c.relkind = 'v' THEN 'VIEW'
                  WHEN c.relkind = 'f' THEN 'FOREIGN TABLE'
                  ELSE null END
             AS character_data) AS table_type,

           CAST(null AS sql_identifier) AS self_referencing_column_name,
           CAST(null AS character_data) AS reference_generation,

           CAST(CASE WHEN t.typname IS NOT NULL THEN current_database() ELSE null END AS sql_identifier) AS user_defined_type_catalog,
           CAST(nt.nspname AS sql_identifier) AS user_defined_type_schema,
           CAST(t.typname AS sql_identifier) AS user_defined_type_name,

           CAST(CASE WHEN c.relkind = 'r'
                          OR (c.relkind = 'v'
                              AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = '3' AND is_instead))
                THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_insertable_into,

           CAST(CASE WHEN t.typname IS NOT NULL THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_typed,
           CAST(null AS character_data) AS commit_action

    FROM pg_namespace nc JOIN pg_class c ON (nc.oid = c.relnamespace)
           LEFT JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON (c.reloftype = t.oid)

    WHERE c.relkind IN ('r', 'm', 'v', 'f')
          AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
          AND (NOT pg_is_other_temp_schema(nc.oid))
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
               OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );

CREATE VIEW information_schema.view_column_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name,
           CAST(a.attname AS sql_identifier) AS column_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt,
         pg_attribute a

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog\_%' AND t.relname not like 'matviewmap\_%')
          AND t.oid = a.attrelid
          AND dt.refobjsubid = a.attnum
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.view_table_usage AS
    SELECT DISTINCT
           CAST(current_database() AS sql_identifier) AS view_catalog,
           CAST(nv.nspname AS sql_identifier) AS view_schema,
           CAST(v.relname AS sql_identifier) AS view_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nt.nspname AS sql_identifier) AS table_schema,
           CAST(t.relname AS sql_identifier) AS table_name

    FROM pg_namespace nv, pg_class v, pg_depend dv,
         pg_depend dt, pg_class t, pg_namespace nt

    WHERE nv.oid = v.relnamespace
          AND v.relkind = 'v'
          AND v.oid = dv.refobjid
          AND dv.refclassid = 'pg_catalog.pg_class'::regclass
          AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dv.deptype = 'i'
          AND dv.objid = dt.objid
          AND dv.refobjid <> dt.refobjid
          AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
          AND dt.refclassid = 'pg_catalog.pg_class'::regclass
          AND dt.refobjid = t.oid
          AND t.relnamespace = nt.oid
          AND t.relkind IN ('r', 'm', 'v', 'f')
          AND (t.relname not like 'mlog\_%' AND t.relname not like 'matviewmap\_%')
          AND pg_has_role(t.relowner, 'USAGE');

CREATE VIEW information_schema.data_type_privileges AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(x.objschema AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS dtd_identifier

    FROM
      (
        SELECT udt_schema, udt_name, 'USER-DEFINED TYPE'::text, dtd_identifier FROM attributes
        UNION ALL
        SELECT table_schema, table_name, 'TABLE'::text, dtd_identifier FROM columns
        UNION ALL
        SELECT domain_schema, domain_name, 'DOMAIN'::text, dtd_identifier FROM domains
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM parameters
        UNION ALL
        SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM routines
      ) AS x (objschema, objname, objtype, objdtdid);

CREATE VIEW information_schema.role_column_grants AS
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

CREATE VIEW information_schema.role_table_grants AS
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

CREATE VIEW information_schema.element_types AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
           CAST(
             CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
                  ELSE 'USER-DEFINED' END AS character_data) AS data_type,

           CAST(null AS cardinal_number) AS character_maximum_length,
           CAST(null AS cardinal_number) AS character_octet_length,
           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,
           CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
           CAST(nco.nspname AS sql_identifier) AS collation_schema,
           CAST(co.collname AS sql_identifier) AS collation_name,
           CAST(null AS cardinal_number) AS numeric_precision,
           CAST(null AS cardinal_number) AS numeric_precision_radix,
           CAST(null AS cardinal_number) AS numeric_scale,
           CAST(null AS cardinal_number) AS datetime_precision,
           CAST(null AS character_data) AS interval_type,
           CAST(null AS cardinal_number) AS interval_precision,

           CAST(null AS character_data) AS domain_default, -- XXX maybe a bug in the standard

           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(nbt.nspname AS sql_identifier) AS udt_schema,
           CAST(bt.typname AS sql_identifier) AS udt_name,

           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,

           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST('a' || CAST(x.objdtdid AS text) AS sql_identifier) AS dtd_identifier

    FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
         (
           /* columns, attributes */
           SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
                  CASE WHEN c.relkind = 'c' THEN 'USER-DEFINED TYPE'::text ELSE 'TABLE'::text END,
                  a.attnum, a.atttypid, a.attcollation
           FROM pg_class c, pg_attribute a
           WHERE c.oid = a.attrelid
                 AND c.relkind IN ('r', 'm', 'v', 'f', 'c')
                 AND (c.relname not like 'mlog\_%' AND c.relname not like 'matviewmap\_%')
                 AND attnum > 0 AND NOT attisdropped

           UNION ALL

           /* domains */
           SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
                  'DOMAIN'::text, 1, t.typbasetype, t.typcollation
           FROM pg_type t
           WHERE t.typtype = 'd'

           UNION ALL

           /* parameters */
           SELECT pronamespace, CAST(proname || '_' || CAST(oid AS text) AS sql_identifier),
                  'ROUTINE'::text, (ss.x).n, (ss.x).x, 0
           FROM (SELECT p.pronamespace, p.proname, p.oid,
                        _pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
                 FROM pg_proc p) AS ss

           UNION ALL

           /* result types */
           SELECT p.pronamespace, CAST(p.proname || '_' || CAST(p.oid AS text) AS sql_identifier),
                  'ROUTINE'::text, 0, p.prorettype, 0
           FROM pg_proc p

         ) AS x (objschema, objname, objtype, objdtdid, objtypeid, objcollation)
         LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
           ON x.objcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

    WHERE n.oid = x.objschema
          AND at.oid = x.objtypeid
          AND (at.typelem <> 0 AND at.typlen = -1)
          AND at.typelem = bt.oid
          AND nbt.oid = bt.typnamespace

          AND (n.nspname, x.objname, x.objtype, CAST(x.objdtdid AS sql_identifier)) IN
              ( SELECT object_schema, object_name, object_type, dtd_identifier
                    FROM data_type_privileges );

do $$DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.element_types TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.data_type_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.role_column_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.role_table_grants TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_domain_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.column_udt_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.columns TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.table_privileges TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.tables TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.view_column_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON information_schema.view_table_usage TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END$$;

GRANT SELECT ON information_schema.element_types TO PUBLIC;
GRANT SELECT ON information_schema.data_type_privileges TO PUBLIC;
GRANT SELECT ON information_schema.role_column_grants TO PUBLIC;
GRANT SELECT ON information_schema.role_table_grants TO PUBLIC;
GRANT SELECT ON information_schema.column_domain_usage TO PUBLIC;
GRANT SELECT ON information_schema.column_privileges TO PUBLIC;
GRANT SELECT ON information_schema.column_udt_usage TO PUBLIC;
GRANT SELECT ON information_schema.columns TO PUBLIC;
GRANT SELECT ON information_schema.table_privileges TO PUBLIC;
GRANT SELECT ON information_schema.tables TO PUBLIC;
GRANT SELECT ON information_schema.view_column_usage TO PUBLIC;
GRANT SELECT ON information_schema.view_table_usage TO PUBLIC;

RESET search_path;
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
