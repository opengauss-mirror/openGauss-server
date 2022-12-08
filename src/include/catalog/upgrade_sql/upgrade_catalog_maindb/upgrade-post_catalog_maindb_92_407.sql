DROP VIEW IF EXISTS pg_catalog.gs_wlm_ec_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_operator_statistics CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_get_invalid_backends CASCADE;

DROP VIEW IF EXISTS pg_catalog.pg_stat_activity cascade;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT connection_info text, OUT srespool name) cascade;

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
				WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
				ELSE T.session_respool
				END AS resource_pool,
		S.query_id,
		S.query,
		S.connection_info
    FROM pg_catalog.pg_database D, pg_catalog.pg_stat_get_activity_with_conninfo(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid;

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

	DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(IN pid bigint, OUT datid oid, OUT pid bigint, OUT sessionid bigint, OUT usesysid oid, OUT application_name text, OUT state text, OUT query text, OUT waiting boolean, OUT xact_start timestamp with time zone, OUT query_start timestamp with time zone, OUT backend_start timestamp with time zone, OUT state_change timestamp with time zone, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT enqueue text, OUT query_id bigint, OUT srespool name) cascade;

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
		  WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
		ELSE T.session_respool
		END AS resource_pool,
			  S.query_id,
			  S.query
	  FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U, gs_wlm_session_respool(0) AS T
		WHERE S.datid = D.oid AND
			  S.usesysid = U.oid AND
			  T.threadid = S.pid;


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
				WHEN T.session_respool = 'unknown' THEN (U.rolrespool) :: name
				ELSE T.session_respool
				END AS resource_pool,
            S.query_id,
            S.query,
            N.node_group
    FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_stat_get_activity_ng(NULL) AS N, pg_authid U, gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
            S.usesysid = U.oid AND
            T.sessionid = S.sessionid AND
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
		 S.node_group
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
        S.node_group
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
------------------------------------------------------------------------------------------------------------------------------------------
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
	DROP VIEW IF EXISTS DBE_PERF.global_locks cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_locks(OUT node_name name, OUT locktype text, OUT database oid, OUT relation oid, OUT page integer, OUT tuple smallint, OUT virtualxid text, OUT transactionid xid, OUT classid oid, OUT objid oid, OUT objsubid smallint, OUT virtualtransaction text, OUT pid bigint, OUT mode text, OUT granted boolean, OUT fastpath boolean) cascade;
	DROP VIEW IF EXISTS DBE_PERF.locks cascade;

	DROP VIEW IF EXISTS pg_catalog.pg_locks cascade;
	DROP FUNCTION IF EXISTS pg_catalog.pg_lock_status(OUT locktype text, OUT database oid, OUT relation oid, OUT page integer, OUT tuple smallint, OUT bucket integer, OUT virtualxid text, OUT transactionid xid, OUT classid oid, OUT objid oid, OUT objsubid smallint, OUT virtualtransaction text, OUT pid bigint, OUT sessionid bigint, OUT mode text, OUT granted boolean, OUT fastpath boolean, OUT locktag text) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1371;
	CREATE OR REPLACE FUNCTION pg_catalog.pg_lock_status
	(
		OUT locktype text,
		OUT database oid,
		OUT relation oid,
		OUT page integer,
		OUT tuple smallint,
		OUT bucket integer,
		OUT virtualxid text,
		OUT transactionid xid,
		OUT classid oid,
		OUT objid oid,
		OUT objsubid smallint,
		OUT virtualtransaction text,
		OUT pid bigint,
		OUT sessionid bigint,
		OUT mode text,
		OUT granted boolean,
		OUT fastpath boolean,
		OUT locktag text,
		OUT global_sessionid text
	)
	RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_lock_status';

	CREATE OR REPLACE VIEW pg_catalog.pg_locks AS
		SELECT * FROM pg_catalog.pg_lock_status() AS L;
	
	
	CREATE OR REPLACE VIEW DBE_PERF.locks AS
	  SELECT * FROM pg_catalog.pg_lock_status() AS L;
	CREATE OR REPLACE FUNCTION dbe_perf.get_global_locks
	  (OUT node_name name,
	   OUT locktype text,
	   OUT database oid,
	   OUT relation oid,
	   OUT page integer,
	   OUT tuple smallint,
	   OUT bucket integer,
	   OUT virtualxid text,
	   OUT transactionid xid,
	   OUT classid oid,
	   OUT objid oid,
	   OUT objsubid smallint,
	   OUT virtualtransaction text,
	   OUT pid bigint,
	   OUT sessionid bigint,
	   OUT mode text,
	   OUT granted boolean,
	   OUT fastpath boolean,
	   OUT locktag text)
	RETURNS setof record
	AS $$
	DECLARE
	  row_data dbe_perf.locks%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		--Get all the node names
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.locks';
		  FOR row_data IN EXECUTE(query_str) LOOP
			node_name := row_name.node_name;
			locktype := row_data.locktype;
			database := row_data.database;
			relation := row_data.relation;
			page := row_data.page;
			tuple := row_data.tuple;
			bucket := row_data.bucket;
			virtualxid := row_data.virtualxid;
			transactionid := row_data.transactionid;
			objid := row_data.objid;
			objsubid := row_data.objsubid;
			virtualtransaction := row_data.virtualtransaction;
			pid := row_data.pid;
			sessionid := row_data.sessionid;
			mode := row_data.mode;
			granted := row_data.granted;
			fastpath := row_data.fastpath;
			locktag := row_data.locktag;
			return next;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW DBE_PERF.global_locks AS
	  SELECT * FROM DBE_PERF.get_global_locks();
  end if;
END$DO$;
------------------------------------------------------------------------------------------------------------------------------------
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
	DROP VIEW IF EXISTS DBE_PERF.global_thread_wait_status cascade;
	DROP FUNCTION IF EXISTS DBE_PERF.get_global_thread_wait_status() cascade;
	DROP VIEW IF EXISTS DBE_PERF.thread_wait_status cascade;

	DROP VIEW IF EXISTS pg_catalog.pg_thread_wait_status cascade;
	DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_status(IN tid bigint, OUT node_name text, OUT db_name text, OUT thread_name text, OUT query_id bigint, OUT tid bigint, OUT sessionid bigint, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT wait_status text, OUT wait_event text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT global_sessionid text) cascade;

	SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3980;
	CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_status
	(
		IN tid bigint,
		OUT node_name text,
		OUT db_name text,
		OUT thread_name text,
		OUT query_id bigint,
		OUT tid bigint,
		OUT sessionid bigint,
		OUT lwtid integer,
		OUT psessionid bigint,
		OUT tlevel integer,
		OUT smpid integer,
		OUT wait_status text,
		OUT wait_event text,
		OUT locktag text,
		OUT lockmode text,
		OUT block_sessionid bigint,
		OUT global_sessionid text
	)
	RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_status';

	CREATE OR REPLACE VIEW pg_catalog.pg_thread_wait_status AS
		SELECT * FROM pg_catalog.pg_stat_get_status(NULL);

	CREATE OR REPLACE VIEW DBE_PERF.thread_wait_status AS
		SELECT * FROM pg_catalog.pg_stat_get_status(NULL);
	CREATE OR REPLACE FUNCTION dbe_perf.get_global_thread_wait_status()
	RETURNS setof dbe_perf.thread_wait_status
	AS $$
	DECLARE
	  row_data dbe_perf.thread_wait_status%rowtype;
	  row_name record;
	  query_str text;
	  query_str_nodes text;
	  BEGIN
		--Get all cn dn node names
		query_str_nodes := 'select * from dbe_perf.node_name';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
		  query_str := 'SELECT * FROM dbe_perf.thread_wait_status';
		  FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		  END LOOP;
		END LOOP;
		return;
	  END; $$
	LANGUAGE 'plpgsql' NOT FENCED;

	CREATE OR REPLACE VIEW DBE_PERF.global_thread_wait_status AS
	  SELECT * FROM DBE_PERF.get_global_thread_wait_status();
-----------------------------------------------------------------------------------------------------------------------------------------------------
	DROP VIEW IF EXISTS DBE_PERF.local_active_session cascade;
	DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session(OUT sampleid bigint, OUT sample_time timestamp with time zone, OUT need_flush_sample boolean, OUT databaseid oid, OUT thread_id bigint, OUT sessionid bigint, OUT start_time timestamp with time zone, OUT event text, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT userid oid, OUT application_name text, OUT client_addr inet, OUT client_hostname text, OUT client_port integer, OUT query_id bigint, OUT unique_query_id bigint, OUT user_id oid, OUT cn_id integer, OUT unique_query text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint, OUT wait_status text) cascade;

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
END$DO$;

DROP VIEW IF EXISTS pg_catalog.pgxc_thread_wait_status cascade;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_thread_wait_status(OUT node_name text, OUT db_name text, OUT thread_name text, OUT query_id bigint, OUT tid bigint, OUT sessionid bigint, OUT lwtid integer, OUT psessionid bigint, OUT tlevel integer, OUT smpid integer, OUT wait_status text, OUT wait_event text, OUT locktag text, OUT lockmode text, OUT block_sessionid bigint) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3591;
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_thread_wait_status
(
        OUT node_name text,
        OUT db_name text,
        OUT thread_name text,
        OUT query_id bigint,
        OUT tid bigint,
        OUT sessionid bigint,
        OUT lwtid integer,
        OUT psessionid bigint,
        OUT tlevel integer,
        OUT smpid integer,
        OUT wait_status text,
        OUT wait_event text,
        OUT locktag text,
        OUT lockmode text,
        OUT block_sessionid bigint,
        OUT global_sessionid text
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pgxc_get_thread_wait_status';

CREATE OR REPLACE VIEW pg_catalog.pgxc_thread_wait_status AS
    SELECT * FROM pg_catalog.pgxc_get_thread_wait_status();
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

    GRANT SELECT ON TABLE pg_catalog.pg_locks TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.locks TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE dbe_perf.locks TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.global_locks TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE dbe_perf.global_locks TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pg_thread_wait_status TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.thread_wait_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.thread_wait_status TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.global_thread_wait_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.global_thread_wait_status TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.pgxc_thread_wait_status TO PUBLIC;

	query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE DBE_PERF.local_active_session TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.local_active_session TO PUBLIC;

    GRANT SELECT ON TABLE pg_catalog.gs_asp TO PUBLIC;

  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_thread_wait_status' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
	alter table snapshot.snap_global_thread_wait_status
		ADD COLUMN snap_global_sessionid text;
  end if;
END$DO$;
