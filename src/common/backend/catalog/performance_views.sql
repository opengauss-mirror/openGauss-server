/*
 * PostgreSQL Performance Views
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * src/backend/catalog/performance_views.sql
 */
/*in order to detect the perf schema with oid, SCHEMA dbe_perf is created by default case;*/

/* OS */
CREATE VIEW dbe_perf.os_runtime AS 
  SELECT * FROM pv_os_run_info();

CREATE VIEW dbe_perf.node_name AS
  SELECT * FROM pgxc_node_str() AS node_name;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_os_runtime
  (OUT node_name name, OUT id integer, OUT name text, OUT value numeric, OUT comments text, OUT cumulative boolean)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.os_runtime%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.os_runtime';
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

CREATE VIEW dbe_perf.global_os_runtime AS
  SELECT DISTINCT * FROM dbe_perf.get_global_os_runtime();

CREATE VIEW dbe_perf.os_threads AS
  SELECT
    S.node_name,
    S.pid,
    S.lwpid,
    S.thread_name,
    S.creation_time
    FROM pg_stat_get_thread() AS S;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_os_threads()
RETURNS setof dbe_perf.os_threads
AS $$
DECLARE
  row_data dbe_perf.os_threads%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.os_threads';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_os_threads AS
  SELECT DISTINCT * FROM dbe_perf.get_global_os_threads();

/* instance */
CREATE VIEW dbe_perf.instance_time AS
  SELECT * FROM pv_instance_time();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_instance_time
  (OUT node_name name, OUT stat_id integer, OUT stat_name text, OUT value bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.instance_time%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all CN DN node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.instance_time';
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

CREATE VIEW dbe_perf.global_instance_time AS
  SELECT DISTINCT * FROM dbe_perf.get_global_instance_time();

/* workload */
CREATE VIEW dbe_perf.workload_sql_count AS
  SELECT
    pg_user.respool as workload,
    pg_catalog.sum(S.select_count)::bigint AS select_count,
    pg_catalog.sum(S.update_count)::bigint AS update_count,
    pg_catalog.sum(S.insert_count)::bigint AS insert_count,
    pg_catalog.sum(S.delete_count)::bigint AS delete_count,
    pg_catalog.sum(S.ddl_count)::bigint AS ddl_count,
    pg_catalog.sum(S.dml_count)::bigint AS dml_count,
    pg_catalog.sum(S.dcl_count)::bigint AS dcl_count
    FROM
      pg_user left join pg_stat_get_sql_count() AS S on pg_user.usename = S.user_name
    GROUP by pg_user.respool;

CREATE VIEW dbe_perf.workload_sql_elapse_time AS
  SELECT
    pg_user.respool as workload,
    pg_catalog.sum(S.total_select_elapse)::bigint AS total_select_elapse,
    pg_catalog.MAX(S.max_select_elapse) AS max_select_elapse,
    pg_catalog.MIN(S.min_select_elapse) AS min_select_elapse,
    ((pg_catalog.sum(S.total_select_elapse) / greatest(pg_catalog.sum(S.select_count), 1))::bigint) AS avg_select_elapse,
    pg_catalog.sum(S.total_update_elapse)::bigint AS total_update_elapse,
    pg_catalog.MAX(S.max_update_elapse) AS max_update_elapse,
    pg_catalog.MIN(S.min_update_elapse) AS min_update_elapse,
    ((pg_catalog.sum(S.total_update_elapse) / greatest(pg_catalog.sum(S.update_count), 1))::bigint) AS avg_update_elapse,
    pg_catalog.sum(S.total_insert_elapse)::bigint AS total_insert_elapse,
    pg_catalog.MAX(S.max_insert_elapse) AS max_insert_elapse,
    pg_catalog.MIN(S.min_insert_elapse) AS min_insert_elapse,
    ((pg_catalog.sum(S.total_insert_elapse) / greatest(pg_catalog.sum(S.insert_count), 1))::bigint) AS avg_insert_elapse,
    pg_catalog.sum(S.total_delete_elapse)::bigint AS total_delete_elapse,
    pg_catalog.MAX(S.max_delete_elapse) AS max_delete_elapse,
    pg_catalog.MIN(S.min_delete_elapse) AS min_delete_elapse,
    ((pg_catalog.sum(S.total_delete_elapse) / greatest(pg_catalog.sum(S.delete_count), 1))::bigint) AS avg_delete_elapse
    FROM
      pg_user left join pg_stat_get_sql_count() AS S on pg_user.usename = S.user_name
    GROUP by pg_user.respool;

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_workload_sql_count
  (OUT node_name name, OUT workload name, OUT select_count bigint,
   OUT update_count bigint, OUT insert_count bigint, OUT delete_count bigint,
   OUT ddl_count bigint, OUT dml_count bigint, OUT dcl_count bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.workload_sql_count%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.workload_sql_count';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        workload := row_data.workload;
        select_count := row_data.select_count;
        update_count := row_data.update_count;
        insert_count := row_data.insert_count;
        delete_count := row_data.delete_count;
        ddl_count := row_data.ddl_count;
        dml_count := row_data.dml_count;
        dcl_count := row_data.dcl_count;
        return next;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.summary_workload_sql_count AS
  SELECT * FROM dbe_perf.get_summary_workload_sql_count();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_workload_sql_elapse_time
  (OUT node_name name, OUT workload name,
   OUT total_select_elapse bigint, OUT max_select_elapse bigint, OUT min_select_elapse bigint, OUT avg_select_elapse bigint,
   OUT total_update_elapse bigint, OUT max_update_elapse bigint, OUT min_update_elapse bigint, OUT avg_update_elapse bigint,
   OUT total_insert_elapse bigint, OUT max_insert_elapse bigint, OUT min_insert_elapse bigint, OUT avg_insert_elapse bigint,
   OUT total_delete_elapse bigint, OUT max_delete_elapse bigint, OUT min_delete_elapse bigint, OUT avg_delete_elapse bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.workload_sql_elapse_time%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.workload_sql_elapse_time';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        workload := row_data.workload;
        total_select_elapse := row_data.total_select_elapse;
        max_select_elapse := row_data.max_select_elapse;
        min_select_elapse := row_data.min_select_elapse;
        avg_select_elapse := row_data.avg_select_elapse;
        total_update_elapse := row_data.total_update_elapse;
        max_update_elapse := row_data.max_update_elapse;
        min_update_elapse := row_data.min_update_elapse;
        avg_update_elapse := row_data.avg_update_elapse;
        total_insert_elapse := row_data.total_insert_elapse;
        max_insert_elapse := row_data.max_insert_elapse;
        min_insert_elapse := row_data.min_insert_elapse;
        avg_insert_elapse := row_data.avg_insert_elapse;
        total_delete_elapse := row_data.total_delete_elapse;
        max_delete_elapse := row_data.max_delete_elapse;
        min_delete_elapse := row_data.min_delete_elapse;
        avg_delete_elapse := row_data.avg_delete_elapse;
        return next;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.summary_workload_sql_elapse_time AS
  SELECT * FROM dbe_perf.get_summary_workload_sql_elapse_time();

/* user transaction */
CREATE VIEW dbe_perf.user_transaction AS 
SELECT
    pg_user.usename as usename,
    giwi.commit_counter as commit_counter,
    giwi.rollback_counter as rollback_counter,
    giwi.resp_min as resp_min,
    giwi.resp_max as resp_max,
    giwi.resp_avg as resp_avg,
    giwi.resp_total as resp_total,
    giwi.bg_commit_counter as bg_commit_counter,
    giwi.bg_rollback_counter as bg_rollback_counter,
    giwi.bg_resp_min as bg_resp_min,
    giwi.bg_resp_max as bg_resp_max,
    giwi.bg_resp_avg as bg_resp_avg,
    giwi.bg_resp_total as bg_resp_total
FROM
    pg_user left join pg_catalog.get_instr_workload_info(0) AS giwi on pg_user.usesysid = giwi.user_oid;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_user_transaction
  (OUT node_name name, OUT usename name, OUT commit_counter bigint,
   OUT rollback_counter bigint, OUT resp_min bigint, OUT resp_max bigint,
   OUT resp_avg bigint, OUT resp_total bigint, OUT bg_commit_counter bigint,
   OUT bg_rollback_counter bigint, OUT bg_resp_min bigint, OUT bg_resp_max bigint, 
   OUT bg_resp_avg bigint, OUT bg_resp_total bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.user_transaction%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.user_transaction';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        usename := row_data.usename;
        commit_counter := row_data.commit_counter;
        rollback_counter := row_data.rollback_counter;
        resp_min := row_data.resp_min;
        resp_max := row_data.resp_max;
        resp_avg := row_data.resp_avg;
        resp_total := row_data.resp_total;
        bg_commit_counter := row_data.bg_commit_counter;
        bg_rollback_counter := row_data.bg_rollback_counter;
        bg_resp_min := row_data.bg_resp_min;
        bg_resp_max := row_data.bg_resp_max;
        bg_resp_avg := row_data.bg_resp_avg;
        bg_resp_total := row_data.bg_resp_total;
        return next;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_user_transaction AS
  SELECT * FROM dbe_perf.get_global_user_transaction();

/* workload transaction */
CREATE VIEW dbe_perf.workload_transaction AS
select
    pg_user.respool as workload,
    pg_catalog.sum(W.commit_counter)::bigint as commit_counter,
    pg_catalog.sum(W.rollback_counter)::bigint as rollback_counter,
    pg_catalog.min(W.resp_min)::bigint as resp_min,
    pg_catalog.max(W.resp_max)::bigint as resp_max,
    ((pg_catalog.sum(W.resp_total) / greatest(pg_catalog.sum(W.commit_counter), 1))::bigint) AS resp_avg,
    pg_catalog.sum(W.resp_total)::bigint as resp_total,
    pg_catalog.sum(W.bg_commit_counter)::bigint as bg_commit_counter,
    pg_catalog.sum(W.bg_rollback_counter)::bigint as bg_rollback_counter,
    pg_catalog.min(W.bg_resp_min)::bigint as bg_resp_min,
    pg_catalog.max(W.bg_resp_max)::bigint as bg_resp_max,
    ((pg_catalog.sum(W.bg_resp_total) / greatest(pg_catalog.sum(W.bg_commit_counter), 1))::bigint) AS bg_resp_avg,
    pg_catalog.sum(W.bg_resp_total)::bigint as bg_resp_total
from
    pg_user left join dbe_perf.user_transaction AS W on pg_user.usename = W.usename
group by
    pg_user.respool;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_workload_transaction
  (OUT node_name name, OUT workload name, OUT commit_counter bigint,
   OUT rollback_counter bigint, OUT resp_min bigint, OUT resp_max bigint, 
   OUT resp_avg bigint, OUT resp_total bigint, OUT bg_commit_counter bigint,
   OUT bg_rollback_counter bigint, OUT bg_resp_min bigint, OUT bg_resp_max bigint, 
   OUT bg_resp_avg bigint, OUT bg_resp_total bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.workload_transaction%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.workload_transaction';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        workload := row_data.workload;
        commit_counter := row_data.commit_counter;
        rollback_counter := row_data.rollback_counter;
        resp_min := row_data.resp_min;
        resp_max := row_data.resp_max;
        resp_avg := row_data.resp_avg;
        resp_total := row_data.resp_total;
        bg_commit_counter := row_data.bg_commit_counter;
        bg_rollback_counter := row_data.bg_rollback_counter;
        bg_resp_min := row_data.bg_resp_min;
        bg_resp_max := row_data.bg_resp_max;
        bg_resp_avg := row_data.bg_resp_avg;
        bg_resp_total := row_data.bg_resp_total;
        return next;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_workload_transaction AS
  SELECT * FROM dbe_perf.get_global_workload_transaction();

CREATE VIEW dbe_perf.summary_workload_transaction AS
  SELECT 
    W.workload AS workload,
    pg_catalog.sum(W.commit_counter) AS commit_counter,
    pg_catalog.sum(W.rollback_counter) AS rollback_counter,
    coalesce(pg_catalog.min(NULLIF(W.resp_min, 0)), 0) AS resp_min,
    pg_catalog.max(W.resp_max) AS resp_max,
    ((pg_catalog.sum(W.resp_total) / greatest(pg_catalog.sum(W.commit_counter), 1))::bigint) AS resp_avg,
    pg_catalog.sum(W.resp_total) AS resp_total,
    pg_catalog.sum(W.bg_commit_counter) AS bg_commit_counter,
    pg_catalog.sum(W.bg_rollback_counter) AS bg_rollback_counter,
    coalesce(pg_catalog.min(NULLIF(W.bg_resp_min, 0)), 0) AS bg_resp_min,
    pg_catalog.max(W.bg_resp_max) AS bg_resp_max,
    ((pg_catalog.sum(W.bg_resp_total) / greatest(pg_catalog.sum(W.bg_commit_counter), 1))::bigint) AS bg_resp_avg,
    pg_catalog.sum(W.bg_resp_total) AS bg_resp_total
    FROM dbe_perf.get_global_workload_transaction() AS W
    GROUP by W.workload;

/* Session/Thread */
CREATE VIEW dbe_perf.session_stat AS 
  SELECT * FROM pv_session_stat();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_stat
  (OUT node_name name, OUT sessid text, OUT statid integer, OUT statname text, OUT statunit text, OUT value bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.session_stat%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.session_stat';
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

CREATE VIEW dbe_perf.global_session_stat AS
  SELECT DISTINCT * FROM dbe_perf.get_global_session_stat();

CREATE VIEW dbe_perf.session_time AS 
  SELECT * FROM pv_session_time();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_time
  (OUT node_name name, OUT sessid text, OUT stat_id integer, OUT stat_name text, OUT value bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.session_time%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.session_time';
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

CREATE VIEW dbe_perf.global_session_time AS
  SELECT DISTINCT * FROM dbe_perf.get_global_session_time();

CREATE VIEW dbe_perf.session_memory AS 
  SELECT * FROM pv_session_memory();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_memory
  (OUT node_name name, OUT sessid text, OUT init_mem integer, OUT used_mem integer, OUT peak_mem integer)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.session_memory%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.session_memory';
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

CREATE VIEW dbe_perf.global_session_memory AS
  SELECT DISTINCT * FROM dbe_perf.get_global_session_memory();

CREATE VIEW dbe_perf.session_memory_detail AS 
  SELECT * FROM gs_session_memory_detail_tp();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_session_memory_detail
  (OUT node_name name, OUT sessid text, OUT sesstype text, OUT contextname text, OUT level smallint, 
   OUT parent text, OUT totalsize bigint, OUT freesize bigint, OUT usedsize bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.session_memory_detail%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.session_memory_detail';
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

CREATE VIEW dbe_perf.global_session_memory_detail AS
  SELECT DISTINCT * FROM dbe_perf.get_global_session_memory_detail();

CREATE VIEW dbe_perf.session_cpu_runtime AS
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

CREATE VIEW dbe_perf.session_memory_runtime AS
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
  FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_authid U
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

CREATE VIEW dbe_perf.global_session_stat_activity AS 
  SELECT * FROM dbe_perf.get_global_session_stat_activity();

CREATE VIEW dbe_perf.thread_wait_status AS
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

CREATE VIEW dbe_perf.global_thread_wait_status AS
  SELECT * FROM dbe_perf.get_global_thread_wait_status();

/* WLM */
CREATE VIEW DBE_PERF.wlm_user_resource_runtime AS 
  SELECT
    T.usename AS username,
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
  FROM (select usename, (pg_catalog.gs_wlm_user_resource_info(usename::cstring)).* from pg_user) T;
    
CREATE VIEW dbe_perf.wlm_user_resource_config AS
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
  FROM pg_authid AS S, pg_catalog.gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
    WHERE S.oid = T.userid AND T.rpoid = R.oid;

CREATE VIEW dbe_perf.operator_history_table AS
  SELECT * FROM gs_wlm_operator_info; 

--createing history operator-level view for test in multi-CN from single CN
CREATE OR REPLACE FUNCTION dbe_perf.get_global_operator_history_table()
RETURNS setof dbe_perf.operator_history_table
AS $$
DECLARE
  row_data dbe_perf.operator_history_table%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the CN node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.operator_history_table';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_operator_history_table AS
  SELECT * FROM dbe_perf.get_global_operator_history_table();

--history operator-level view for DM in single CN
CREATE VIEW dbe_perf.operator_history AS
  SELECT * FROM pg_catalog.pg_stat_get_wlm_operator_info(0);

CREATE OR REPLACE FUNCTION dbe_perf.get_global_operator_history()
RETURNS setof dbe_perf.operator_history
AS $$
DECLARE
  row_data dbe_perf.operator_history%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.operator_history';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW dbe_perf.global_operator_history AS
    SELECT * FROM dbe_perf.get_global_operator_history();

--real time operator-level view in single CN
CREATE VIEW dbe_perf.operator_runtime AS
  SELECT t.*
  FROM dbe_perf.session_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_operator_info(NULL) as t
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

CREATE VIEW dbe_perf.global_operator_runtime AS
  SELECT * FROM dbe_perf.get_global_operator_runtime();

/* Query-AP */
CREATE VIEW dbe_perf.statement_complex_history AS
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

CREATE VIEW dbe_perf.global_statement_complex_history AS
  SELECT * FROM dbe_perf.get_global_statement_complex_history();

CREATE VIEW dbe_perf.statement_complex_history_table AS
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

CREATE VIEW dbe_perf.global_statement_complex_history_table AS
  SELECT * FROM dbe_perf.get_global_statement_complex_history_table();

CREATE VIEW dbe_perf.statement_complex_runtime AS
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
  FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_realtime_session_info(NULL) AS T
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

CREATE VIEW dbe_perf.global_statement_complex_runtime AS
  SELECT * FROM dbe_perf.get_global_statement_complex_runtime();

CREATE VIEW dbe_perf.statement_iostat_complex_runtime AS
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
   FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
     WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW dbe_perf.statement_wlmstat_complex_runtime AS
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
  FROM pg_database D, pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S, pg_authid AS U, pg_catalog.gs_wlm_session_respool(0) AS T
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
          T.threadid = S.threadid;

/* Memory */
CREATE VIEW dbe_perf.memory_node_detail AS 
  SELECT * FROM pv_total_memory_detail();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_memory_node_detail()
RETURNS setof dbe_perf.memory_node_detail
AS $$
DECLARE
  row_name record;
  row_data dbe_perf.memory_node_detail%rowtype;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.memory_node_detail';
        FOR row_data IN EXECUTE(query_str) LOOP
            RETURN NEXT row_data;
        END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_memory_node_detail AS
  SELECT DISTINCT * FROM dbe_perf.get_global_memory_node_detail();

CREATE VIEW dbe_perf.shared_memory_detail AS 
  SELECT * FROM pg_shared_memory_detail();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_shared_memory_detail
  (OUT node_name name, OUT contextname text, OUT level smallint, OUT parent text,
   OUT totalsize bigint, OUT freesize bigint, OUT usedsize bigint)
RETURNS setof record
AS $$
DECLARE
  row_name record;
  row_data dbe_perf.shared_memory_detail%rowtype;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.shared_memory_detail';
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

CREATE VIEW dbe_perf.global_shared_memory_detail AS
  SELECT DISTINCT * FROM dbe_perf.get_global_shared_memory_detail();

/* cache I/O */
CREATE VIEW dbe_perf.statio_all_indexes AS
  SELECT
    C.oid AS relid,
    I.oid AS indexrelid,
    N.nspname AS schemaname,
    C.relname AS relname,
    I.relname AS indexrelname,
    pg_catalog.pg_stat_get_blocks_fetched(I.oid) -
    pg_catalog.pg_stat_get_blocks_hit(I.oid) AS idx_blks_read,
    pg_catalog.pg_stat_get_blocks_hit(I.oid) AS idx_blks_hit
  FROM pg_class C JOIN
       pg_index X ON C.oid = X.indrelid JOIN
       pg_class I ON I.oid = X.indexrelid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't');

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_all_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_all_indexes';
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

CREATE VIEW dbe_perf.global_statio_all_indexes AS
  SELECT *  FROM dbe_perf.get_global_statio_all_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_local_toastname_and_toastindexname(OUT shemaname name, OUT relname name, OUT toastname name, OUT toastindexname name)
RETURNS setof record
AS $$
DECLARE
  query_str text;
  BEGIN
    query_str := '
    SELECT 
        N.nspname AS shemaname, 
        S.relname AS relname, 
        T.relname AS toastname,
        CI.relname AS toastindexname
    FROM pg_class S JOIN pg_namespace N ON N.oid = S.relnamespace
        LEFT JOIN pg_class T on T.oid = S.reltoastrelid
        JOIN pg_index I ON T.oid = I.indrelid
        JOIN pg_class CI ON CI.oid = I.indexrelid
    WHERE S.relkind IN (''r'', ''t'') AND T.relname is not NULL';
    return query execute query_str;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_all_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_blks_read AS idx_blks_read,
        T.idx_blks_hit AS idx_blks_hit
      FROM dbe_perf.statio_all_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_statio_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit 
    FROM dbe_perf.get_summary_statio_all_indexes() as Ti
         LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.statio_all_sequences AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_read,
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS blks_hit
  FROM pg_class C
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S' or C.relkind = 'L';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_all_sequences
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_all_sequences';
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

CREATE VIEW dbe_perf.global_statio_all_sequences AS
  SELECT * FROM dbe_perf.get_global_statio_all_sequences();

CREATE VIEW dbe_perf.summary_statio_all_sequences AS
  SELECT schemaname, relname, 
         pg_catalog.SUM(blks_read) blks_read, pg_catalog.SUM(blks_hit) blks_hit
   FROM dbe_perf.get_global_statio_all_sequences() 
   GROUP BY (schemaname, relname);

CREATE VIEW dbe_perf.statio_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_blocks_fetched(C.oid) -
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS heap_blks_read,
    pg_catalog.pg_stat_get_blocks_hit(C.oid) AS heap_blks_hit,
    pg_catalog.sum(pg_catalog.pg_stat_get_blocks_fetched(I.indexrelid) -
        pg_catalog.pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_read,
    pg_catalog.sum(pg_catalog.pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_hit,
    pg_catalog.pg_stat_get_blocks_fetched(T.oid) -
      pg_catalog.pg_stat_get_blocks_hit(T.oid) AS toast_blks_read,
    pg_catalog.pg_stat_get_blocks_hit(T.oid) AS toast_blks_hit,
    pg_catalog.pg_stat_get_blocks_fetched(X.oid) -
      pg_catalog.pg_stat_get_blocks_hit(X.oid) AS tidx_blks_read,
    pg_catalog.pg_stat_get_blocks_hit(X.oid) AS tidx_blks_hit
  FROM pg_class C LEFT JOIN
       pg_index I ON C.oid = I.indrelid LEFT JOIN
       pg_class T ON C.reltoastrelid = T.oid LEFT JOIN
       pg_class X ON T.reltoastidxid = X.oid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
  GROUP BY C.oid, N.nspname, C.relname, T.oid, X.oid;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_all_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_all_tables';
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
  
CREATE VIEW dbe_perf.global_statio_all_tables AS
  SELECT * FROM dbe_perf.get_global_statio_all_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_all_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
        FROM dbe_perf.statio_all_tables C 
            LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
            LEFT JOIN pg_namespace N ON O.relnamespace = N.oid';
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

CREATE OR REPLACE FUNCTION dbe_perf.get_local_toast_relation(OUT shemaname name, OUT relname name, OUT toastname name)
RETURNS setof record
AS $$
DECLARE
  query_str text;
  BEGIN
    query_str := '
    SELECT 
        N.nspname as shemaname,
        S.relname as relname,
        T.relname as toastname
    FROM pg_class S JOIN pg_namespace N ON N.oid = S.relnamespace
        LEFT JOIN pg_class T on T.oid=S.reltoastrelid
        WHERE T.relname is not NULL';
    return query execute query_str;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.summary_statio_all_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname,
         pg_catalog.SUM(Ti.heap_blks_read) heap_blks_read, pg_catalog.SUM(Ti.heap_blks_hit) heap_blks_hit,
         pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit,
         pg_catalog.SUM(Ti.toast_blks_read) toast_blks_read, pg_catalog.SUM(Ti.toast_blks_hit) toast_blks_hit,
         pg_catalog.SUM(Ti.tidx_blks_read) tidx_blks_read, pg_catalog.SUM(Ti.tidx_blks_hit) tidx_blks_hit
    FROM dbe_perf.get_summary_statio_all_tables() Ti left join dbe_perf.get_local_toast_relation() Tn on Tn.shemaname = Ti.toastrelschemaname and Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE VIEW dbe_perf.statio_sys_indexes AS
  SELECT * FROM dbe_perf.statio_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'snapshot') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_sys_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_sys_indexes';
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

CREATE VIEW dbe_perf.global_statio_sys_indexes AS
  SELECT * FROM dbe_perf.get_global_statio_sys_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_sys_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_blks_read AS idx_blks_read,
        T.idx_blks_hit AS idx_blks_hit
      FROM dbe_perf.statio_sys_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_statio_sys_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit
  FROM dbe_perf.get_summary_statio_sys_indexes() AS Ti
      LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.statio_sys_sequences AS
  SELECT * FROM dbe_perf.statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_sys_sequences
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_sys_sequences';
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

CREATE VIEW dbe_perf.global_statio_sys_sequences AS
  SELECT * FROM dbe_perf.get_global_statio_sys_sequences();

CREATE VIEW dbe_perf.summary_statio_sys_sequences AS
  SELECT schemaname, relname, 
         pg_catalog.SUM(blks_read) blks_read, pg_catalog.SUM(blks_hit) blks_hit
    FROM dbe_perf.get_global_statio_sys_sequences() 
  GROUP BY (schemaname, relname);

CREATE VIEW dbe_perf.statio_sys_tables AS
  SELECT * FROM dbe_perf.statio_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'snapshot') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_sys_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_sys_tables';
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
  
CREATE VIEW dbe_perf.global_statio_sys_tables AS
  SELECT * FROM dbe_perf.get_global_statio_sys_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_sys_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.statio_sys_tables C
          LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
          LEFT JOIN pg_namespace N ON O.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_statio_sys_tables AS
  SELECT 
    Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) as relname, 
    pg_catalog.SUM(Ti.heap_blks_read) heap_blks_read, pg_catalog.SUM(Ti.heap_blks_hit) heap_blks_hit,
    pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit,
    pg_catalog.SUM(Ti.toast_blks_read) toast_blks_read, pg_catalog.SUM(Ti.toast_blks_hit) toast_blks_hit,
    pg_catalog.SUM(Ti.tidx_blks_read) tidx_blks_read, pg_catalog.SUM(Ti.tidx_blks_hit) tidx_blks_hit
  FROM dbe_perf.get_summary_statio_sys_tables() as Ti 
    LEFT JOIN dbe_perf.get_local_toast_relation() Tn 
    ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE VIEW dbe_perf.statio_user_indexes AS
  SELECT * FROM dbe_perf.statio_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'snapshot') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_user_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_user_indexes';
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

CREATE VIEW dbe_perf.global_statio_user_indexes AS
  SELECT *  FROM dbe_perf.get_global_statio_user_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_user_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_blks_read AS idx_blks_read,
        T.idx_blks_hit AS idx_blks_hit
      FROM dbe_perf.statio_user_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_statio_user_indexes AS
  SELECT
      Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit 
  FROM dbe_perf.get_summary_statio_user_indexes() AS Ti
      LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.statio_user_sequences AS
  SELECT * FROM dbe_perf.statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_user_sequences
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_user_sequences';
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

CREATE VIEW dbe_perf.global_statio_user_sequences AS
  SELECT * FROM dbe_perf.get_global_statio_user_sequences();

CREATE VIEW dbe_perf.summary_statio_user_sequences AS
  SELECT schemaname, relname, 
         pg_catalog.SUM(blks_read) blks_read, pg_catalog.SUM(blks_hit) blks_hit
    FROM dbe_perf.get_global_statio_user_sequences() 
  GROUP BY (schemaname, relname);

CREATE VIEW dbe_perf.statio_user_tables AS
  SELECT * FROM dbe_perf.statio_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'snapshot') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statio_user_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statio_user_tables';
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
  
CREATE VIEW dbe_perf.global_statio_user_tables AS
  SELECT * FROM dbe_perf.get_global_statio_user_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_statio_user_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.statio_user_tables C
        LEFT JOIN pg_class O ON C.relid = O.reltoastrelid
        LEFT JOIN pg_namespace N ON O.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_statio_user_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname, 
         pg_catalog.SUM(Ti.heap_blks_read) heap_blks_read, pg_catalog.SUM(Ti.heap_blks_hit) heap_blks_hit,
         pg_catalog.SUM(Ti.idx_blks_read) idx_blks_read, pg_catalog.SUM(Ti.idx_blks_hit) idx_blks_hit,
         pg_catalog.SUM(Ti.toast_blks_read) toast_blks_read, pg_catalog.SUM(Ti.toast_blks_hit) toast_blks_hit,
         pg_catalog.SUM(Ti.tidx_blks_read) tidx_blks_read, pg_catalog.SUM(Ti.tidx_blks_hit) tidx_blks_hit
  FROM dbe_perf.get_summary_statio_user_tables() AS Ti LEFT JOIN dbe_perf.get_local_toast_relation() Tn 
         ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE OR REPLACE FUNCTION dbe_perf.get_stat_db_cu
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT  D.datname AS datname,
      pg_catalog.pg_stat_get_db_cu_mem_hit(D.oid) AS mem_hit,
      pg_catalog.pg_stat_get_db_cu_hdd_sync(D.oid) AS hdd_sync_read,
      pg_catalog.pg_stat_get_db_cu_hdd_asyn(D.oid) AS hdd_asyn_read
      FROM pg_database D;';
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

CREATE VIEW dbe_perf.global_stat_db_cu AS
  SELECT * FROM dbe_perf.get_stat_db_cu();

CREATE VIEW dbe_perf.global_stat_session_cu AS
  SELECT * FROM pg_stat_session_cu();

/* Object */
CREATE VIEW dbe_perf.stat_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_numscans(C.oid) AS seq_scan,
    pg_catalog.pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
    pg_catalog.sum(pg_catalog.pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
    pg_catalog.sum(pg_catalog.pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
    pg_catalog.pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
    pg_catalog.pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
    pg_catalog.pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
    pg_catalog.pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
    pg_catalog.pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
    pg_catalog.pg_stat_get_live_tuples(C.oid) AS n_live_tup,
    pg_catalog.pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
    pg_catalog.pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
    pg_catalog.pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
    pg_catalog.pg_stat_get_last_analyze_time(C.oid) as last_analyze,
    pg_catalog.pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
    pg_catalog.pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
    pg_catalog.pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
    pg_catalog.pg_stat_get_analyze_count(C.oid) AS analyze_count,
    pg_catalog.pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_all_tables
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
        query_str_nodes := 'select * from dbe_perf.node_name';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'SELECT * FROM dbe_perf.stat_all_tables';
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

CREATE VIEW dbe_perf.global_stat_all_tables AS
	SELECT * FROM dbe_perf.get_global_stat_all_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_all_tables
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
        query_str_nodes := 'select * from dbe_perf.node_name';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := '
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
                FROM dbe_perf.stat_all_tables T 
                    LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
                    LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_all_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) as relname,
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd, 
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         pg_catalog.SUM(Ti.n_live_tup) n_live_tup, pg_catalog.SUM(Ti.n_dead_tup) n_dead_tup,
         pg_catalog.MAX(Ti.last_vacuum) last_vacuum, pg_catalog.MAX(Ti.last_autovacuum) last_autovacuum,
         pg_catalog.MAX(Ti.last_analyze) last_analyze, pg_catalog.MAX(Ti.last_autoanalyze) last_autoanalyze,
         pg_catalog.SUM(Ti.vacuum_count) vacuum_count, pg_catalog.SUM(Ti.autovacuum_count) autovacuum_count,
         pg_catalog.SUM(Ti.analyze_count) analyze_count, pg_catalog.SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM (SELECT * FROM dbe_perf.get_summary_stat_all_tables()) AS Ti
    LEFT JOIN dbe_perf.get_local_toast_relation() Tn
         ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_all_indexes AS
  SELECT
    C.oid AS relid,
    I.oid AS indexrelid,
    N.nspname AS schemaname,
    C.relname AS relname,
    I.relname AS indexrelname,
    pg_catalog.pg_stat_get_numscans(I.oid) AS idx_scan,
    pg_catalog.pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
    pg_catalog.pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't');

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_all_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_all_indexes';
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
  
CREATE VIEW dbe_perf.global_stat_all_indexes AS
  SELECT * FROM dbe_perf.get_global_stat_all_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_all_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT 
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_scan AS idx_scan,
        T.idx_tup_read AS idx_tup_read,
        T.idx_tup_fetch AS idx_tup_fetch
      FROM dbe_perf.stat_all_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM dbe_perf.get_summary_stat_all_indexes() AS Ti
         LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.stat_sys_tables AS
  SELECT * FROM dbe_perf.stat_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_sys_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_sys_tables';
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
  
CREATE VIEW dbe_perf.global_stat_sys_tables AS
  SELECT * FROM dbe_perf.get_global_stat_sys_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_sys_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.stat_sys_tables T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_sys_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname, 
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd, 
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         pg_catalog.SUM(Ti.n_live_tup) n_live_tup, pg_catalog.SUM(Ti.n_dead_tup) n_dead_tup,
         pg_catalog.MAX(Ti.last_vacuum) last_vacuum, pg_catalog.MAX(Ti.last_autovacuum) last_autovacuum,
         pg_catalog.MAX(Ti.last_analyze) last_analyze, pg_catalog.MAX(Ti.last_autoanalyze) last_autoanalyze,
         pg_catalog.SUM(Ti.vacuum_count) vacuum_count, pg_catalog.SUM(Ti.autovacuum_count) autovacuum_count,
         pg_catalog.SUM(Ti.analyze_count) analyze_count, pg_catalog.SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM dbe_perf.get_summary_stat_sys_tables() as Ti LEFT JOIN dbe_perf.get_local_toast_relation() as Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_sys_indexes AS
  SELECT * FROM dbe_perf.stat_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_sys_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_sys_indexes';
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
  
CREATE VIEW dbe_perf.global_stat_sys_indexes AS
  SELECT * FROM dbe_perf.get_global_stat_sys_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_sys_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT 
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_scan AS idx_scan,
        T.idx_tup_read AS idx_tup_read,
        T.idx_tup_fetch AS idx_tup_fetch
      FROM dbe_perf.stat_sys_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_sys_indexes AS
  SELECT Ti.schemaname AS schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM dbe_perf.get_summary_stat_sys_indexes() AS Ti
         LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.stat_user_tables AS
  SELECT * FROM dbe_perf.stat_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_user_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_user_tables';
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
  
CREATE VIEW dbe_perf.global_stat_user_tables AS
  SELECT * FROM dbe_perf.get_global_stat_user_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_user_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.stat_user_tables T 
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_user_tables AS
  SELECT Ti.schemaname AS schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname, 
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd, 
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         pg_catalog.SUM(Ti.n_live_tup) n_live_tup, pg_catalog.SUM(Ti.n_dead_tup) n_dead_tup,
         pg_catalog.MAX(Ti.last_vacuum) last_vacuum, pg_catalog.MAX(Ti.last_autovacuum) last_autovacuum,
         pg_catalog.MAX(Ti.last_analyze) last_analyze, pg_catalog.MAX(Ti.last_autoanalyze) last_autoanalyze,
         pg_catalog.SUM(Ti.vacuum_count) vacuum_count, pg_catalog.SUM(Ti.autovacuum_count) autovacuum_count,
         pg_catalog.SUM(Ti.analyze_count) analyze_count, pg_catalog.SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM dbe_perf.get_summary_stat_user_tables() AS Ti LEFT JOIN dbe_perf.get_local_toast_relation() AS Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_user_indexes AS
  SELECT * FROM dbe_perf.stat_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_user_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_user_indexes';
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
  
CREATE VIEW dbe_perf.global_stat_user_indexes AS
  SELECT * FROM dbe_perf.get_global_stat_user_indexes();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_user_indexes
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
      SELECT
        T.relname AS relname,
        T.schemaname AS schemaname,
        C.relname AS toastrelname,
        N.nspname AS toastrelschemaname,
        T.indexrelname AS indexrelname,
        T.idx_scan AS idx_scan,
        T.idx_tup_read AS idx_tup_read,
        T.idx_tup_fetch AS idx_tup_fetch
      FROM dbe_perf.stat_user_indexes T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_user_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         pg_catalog.SUM(Ti.idx_scan) idx_scan, pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read, pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM dbe_perf.get_summary_stat_user_indexes() as Ti
         LEFT JOIN dbe_perf.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW dbe_perf.stat_database AS
  SELECT
    D.oid AS datid,
    D.datname AS datname,
    pg_catalog.pg_stat_get_db_numbackends(D.oid) AS numbackends,
    pg_catalog.pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
    pg_catalog.pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
    pg_catalog.pg_stat_get_db_blocks_fetched(D.oid) -
    pg_catalog.pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
    pg_catalog.pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
    pg_catalog.pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
    pg_catalog.pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
    pg_catalog.pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
    pg_catalog.pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
    pg_catalog.pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
    pg_catalog.pg_stat_get_db_conflict_all(D.oid) AS conflicts,
    pg_catalog.pg_stat_get_db_temp_files(D.oid) AS temp_files,
    pg_catalog.pg_stat_get_db_temp_bytes(D.oid) AS temp_bytes,
    pg_catalog.pg_stat_get_db_deadlocks(D.oid) AS deadlocks,
    pg_catalog.pg_stat_get_db_blk_read_time(D.oid) AS blk_read_time,
    pg_catalog.pg_stat_get_db_blk_write_time(D.oid) AS blk_write_time,
    pg_catalog.pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
  FROM pg_database D;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_database
  (OUT node_name name, OUT datid oid, OUT datname name, OUT numbackends integer, OUT xact_commit bigint,
   OUT xact_rollback bigint, OUT blks_read bigint, OUT blks_hit bigint, OUT tup_returned bigint, OUT tup_fetched bigint,
   OUT tup_inserted bigint, OUT tup_updated bigint, OUT tup_deleted bigint, OUT conflicts bigint, OUT temp_files bigint,
   OUT temp_bytes bigint, OUT deadlocks bigint, OUT blk_read_time double precision, OUT blk_write_time double precision,
   OUT stats_reset timestamp with time zone)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_database%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_database';
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

CREATE VIEW dbe_perf.global_stat_database AS
  SELECT * FROM dbe_perf.get_global_stat_database();

CREATE VIEW dbe_perf.summary_stat_database AS
  SELECT ALL_NODES.datname,
         ALL_NODES.numbackends, ALL_NODES.xact_commit, ALL_NODES.xact_rollback,
         ALL_NODES.blks_read, ALL_NODES.blks_hit, ALL_NODES.tup_returned, ALL_NODES.tup_fetched, 
         SUMMARY_ITEM.tup_inserted, SUMMARY_ITEM.tup_updated, SUMMARY_ITEM.tup_deleted, 
         SUMMARY_ITEM.conflicts, ALL_NODES.temp_files, ALL_NODES.temp_bytes, SUMMARY_ITEM.deadlocks, 
         ALL_NODES.blk_read_time, ALL_NODES.blk_write_time, ALL_NODES.stats_reset
    FROM
      dbe_perf.stat_database AS SUMMARY_ITEM,
      (SELECT datname,
         pg_catalog.SUM(numbackends) numbackends, pg_catalog.SUM(xact_commit) xact_commit, pg_catalog.SUM(xact_rollback) xact_rollback,
         pg_catalog.SUM(blks_read) blks_read, pg_catalog.SUM(blks_hit) blks_hit, pg_catalog.SUM(tup_returned) tup_returned,
         pg_catalog.SUM(tup_fetched) tup_fetched, pg_catalog.SUM(temp_files) temp_files, 
         pg_catalog.SUM(temp_bytes) temp_bytes, pg_catalog.SUM(blk_read_time) blk_read_time, 
         pg_catalog.SUM(blk_write_time) blk_write_time, pg_catalog.MAX(stats_reset) stats_reset
      FROM dbe_perf.get_global_stat_database() GROUP BY (datname)) AS ALL_NODES
    WHERE ALL_NODES.datname = SUMMARY_ITEM.datname;
      
CREATE VIEW dbe_perf.stat_database_conflicts AS
  SELECT
    D.oid AS datid,
    D.datname AS datname,
    pg_catalog.pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_catalog.pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_catalog.pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_catalog.pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_catalog.pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
  FROM pg_database D;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_database_conflicts
  (OUT node_name name, OUT datid oid, OUT datname name, OUT confl_tablespace bigint,
   OUT confl_lock bigint, OUT confl_snapshot bigint, OUT confl_bufferpin bigint, OUT confl_deadlock bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_database_conflicts%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_database_conflicts';
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

CREATE VIEW dbe_perf.global_stat_database_conflicts AS
  SELECT * FROM dbe_perf.get_global_stat_database_conflicts();

CREATE VIEW dbe_perf.summary_stat_database_conflicts AS
  SELECT
    D.datname AS datname,
    pg_catalog.pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_catalog.pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_catalog.pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_catalog.pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_catalog.pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
  FROM pg_database D;

CREATE VIEW dbe_perf.stat_xact_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_catalog.pg_stat_get_xact_numscans(C.oid) AS seq_scan,
    pg_catalog.pg_stat_get_xact_tuples_returned(C.oid) AS seq_tup_read,
    pg_catalog.sum(pg_catalog.pg_stat_get_xact_numscans(I.indexrelid))::bigint AS idx_scan,
    pg_catalog.sum(pg_catalog.pg_stat_get_xact_tuples_fetched(I.indexrelid))::bigint +
    pg_catalog.pg_stat_get_xact_tuples_fetched(C.oid) AS idx_tup_fetch,
    pg_catalog.pg_stat_get_xact_tuples_inserted(C.oid) AS n_tup_ins,
    pg_catalog.pg_stat_get_xact_tuples_updated(C.oid) AS n_tup_upd,
    pg_catalog.pg_stat_get_xact_tuples_deleted(C.oid) AS n_tup_del,
    pg_catalog.pg_stat_get_xact_tuples_hot_updated(C.oid) AS n_tup_hot_upd
  FROM pg_class C LEFT JOIN
       pg_index I ON C.oid = I.indrelid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
  GROUP BY C.oid, N.nspname, C.relname;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_xact_all_tables
  (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
   OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
   OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_xact_all_tables%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.stat_xact_all_tables';
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.stat_xact_all_tables where schemaname = ''pg_catalog'' or schemaname =''pg_toast'' ';
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

CREATE VIEW dbe_perf.global_stat_xact_all_tables AS
  SELECT * FROM dbe_perf.get_global_stat_xact_all_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_xact_all_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := '
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
        FROM dbe_perf.stat_xact_all_tables T
            LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
            LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_xact_all_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname,
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read, pg_catalog.SUM(Ti.idx_scan) idx_scan,
         pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch, pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd,
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM dbe_perf.get_summary_stat_xact_all_tables() as Ti LEFT JOIN dbe_perf.get_local_toast_relation() AS Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_xact_sys_tables AS
  SELECT * FROM dbe_perf.stat_xact_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_xact_sys_tables
  (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
   OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
   OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_xact_sys_tables%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_xact_sys_tables';
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

CREATE VIEW dbe_perf.global_stat_xact_sys_tables AS
  SELECT * FROM dbe_perf.get_global_stat_xact_sys_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_xact_sys_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.stat_xact_sys_tables T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_xact_sys_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read, pg_catalog.SUM(Ti.idx_scan) idx_scan,
         pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch, pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd,
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM dbe_perf.get_summary_stat_xact_sys_tables() as Ti LEFT JOIN dbe_perf.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_xact_user_tables AS
  SELECT * FROM dbe_perf.stat_xact_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_xact_user_tables
  (OUT node_name name, OUT relid oid, OUT schemaname name, OUT relname name, OUT seq_scan bigint,
   OUT seq_tup_read bigint, OUT idx_scan bigint, OUT idx_tup_fetch bigint, OUT n_tup_ins bigint,
   OUT n_tup_upd bigint, OUT n_tup_del bigint, OUT n_tup_hot_upd bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_xact_user_tables%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_xact_user_tables';
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

CREATE VIEW dbe_perf.global_stat_xact_user_tables AS
  SELECT * FROM dbe_perf.get_global_stat_xact_user_tables();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_stat_xact_user_tables
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := '
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
      FROM dbe_perf.stat_xact_user_tables T
        LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
        LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
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

CREATE VIEW dbe_perf.summary_stat_xact_user_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         pg_catalog.SUM(Ti.seq_scan) seq_scan, pg_catalog.SUM(Ti.seq_tup_read) seq_tup_read, pg_catalog.SUM(Ti.idx_scan) idx_scan,
         pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch, pg_catalog.SUM(Ti.n_tup_ins) n_tup_ins, pg_catalog.SUM(Ti.n_tup_upd) n_tup_upd,
         pg_catalog.SUM(Ti.n_tup_del) n_tup_del, pg_catalog.SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM dbe_perf.get_summary_stat_xact_user_tables() AS Ti LEFT JOIN dbe_perf.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW dbe_perf.stat_user_functions AS
  SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_catalog.pg_stat_get_function_calls(P.oid) AS calls,
            pg_catalog.pg_stat_get_function_total_time(P.oid) AS total_time,
            pg_catalog.pg_stat_get_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_stat_get_function_calls(P.oid) IS NOT NULL;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_user_functions
  (OUT node_name name, OUT funcid oid, OUT schemaname name, OUT funcname name, OUT calls bigint,
   OUT total_time double precision, OUT self_time double precision)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_user_functions%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_user_functions';
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

CREATE VIEW dbe_perf.global_stat_user_functions AS
  SELECT * FROM dbe_perf.get_global_stat_user_functions();

CREATE VIEW dbe_perf.summary_stat_user_functions AS
  SELECT schemaname, funcname,
         pg_catalog.SUM(calls) calls, pg_catalog.SUM(total_time) total_time, pg_catalog.SUM(self_time) self_time
    FROM dbe_perf.get_global_stat_user_functions() 
    GROUP BY (schemaname, funcname);

CREATE VIEW dbe_perf.stat_xact_user_functions AS
  SELECT
    P.oid AS funcid,
    N.nspname AS schemaname,
    P.proname AS funcname,
    pg_catalog.pg_stat_get_xact_function_calls(P.oid) AS calls,
    pg_catalog.pg_stat_get_xact_function_total_time(P.oid) AS total_time,
    pg_catalog.pg_stat_get_xact_function_self_time(P.oid) AS self_time
  FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_catalog.pg_stat_get_xact_function_calls(P.oid) IS NOT NULL;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_xact_user_functions
  (OUT node_name name, OUT funcid oid, OUT schemaname name, OUT funcname name, OUT calls bigint,
   OUT total_time double precision, OUT self_time double precision)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.stat_xact_user_functions%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_xact_user_functions';
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

CREATE VIEW dbe_perf.global_stat_xact_user_functions AS
  SELECT * FROM dbe_perf.get_global_stat_xact_user_functions();

CREATE VIEW dbe_perf.summary_stat_xact_user_functions AS
  SELECT schemaname, funcname,
         pg_catalog.SUM(calls) calls, pg_catalog.SUM(total_time) total_time, pg_catalog.SUM(self_time) self_time
    FROM dbe_perf.get_global_stat_xact_user_functions() 
    GROUP BY (schemaname, funcname);

CREATE VIEW dbe_perf.stat_bad_block AS
  SELECT DISTINCT * from pg_stat_bad_block();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_stat_bad_block
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
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.stat_bad_block';
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

CREATE VIEW dbe_perf.global_stat_bad_block AS
  SELECT DISTINCT * from dbe_perf.get_global_stat_bad_block();

CREATE VIEW dbe_perf.summary_stat_bad_block AS
  SELECT databaseid, tablespaceid, relfilenode,
         pg_catalog.SUM(forknum) forknum, pg_catalog.SUM(error_count) error_count,
         pg_catalog.MIN(first_time) first_time, pg_catalog.MAX(last_time) last_time
    FROM dbe_perf.get_global_stat_bad_block() 
    GROUP BY (databaseid, tablespaceid, relfilenode);

/* File */
CREATE VIEW dbe_perf.file_redo_iostat AS 
  SELECT * FROM pg_stat_get_redo_stat();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_file_redo_iostat
  (OUT node_name name, OUT phywrts bigint, OUT phyblkwrt bigint, OUT writetim bigint, 
   OUT avgiotim bigint, OUT lstiotim bigint, OUT miniotim bigint, OUT maxiowtm bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.file_redo_iostat%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.file_redo_iostat';
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

CREATE VIEW dbe_perf.global_file_redo_iostat AS 
  SELECT * FROM dbe_perf.get_global_file_redo_iostat();

CREATE VIEW dbe_perf.summary_file_redo_iostat AS 
  SELECT 
    pg_catalog.sum(phywrts) AS phywrts,
    pg_catalog.sum(phyblkwrt) AS phyblkwrt,
    pg_catalog.sum(writetim) AS writetim,
    ((pg_catalog.sum(writetim) / greatest(pg_catalog.sum(phywrts), 1))::bigint) AS avgiotim,
    pg_catalog.max(lstiotim) AS lstiotim,
    pg_catalog.min(miniotim) AS miniotim,
    pg_catalog.max(maxiowtm) AS maxiowtm
    FROM dbe_perf.get_global_file_redo_iostat();

CREATE VIEW dbe_perf.local_rel_iostat AS
  SELECT * FROM get_local_rel_iostat();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_rel_iostat
  (OUT node_name name, OUT phyrds bigint,
  OUT phywrts bigint, OUT phyblkrd bigint, OUT phyblkwrt bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.local_rel_iostat%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.local_rel_iostat';
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

CREATE VIEW dbe_perf.global_rel_iostat AS
  SELECT * FROM dbe_perf.get_global_rel_iostat();

CREATE VIEW dbe_perf.summary_rel_iostat AS
  SELECT
    pg_catalog.sum(phyrds) AS phyrds, pg_catalog.sum(phywrts) AS phywrts, pg_catalog.sum(phyblkrd) AS phyblkrd,
    pg_catalog.sum(phyblkwrt) AS phyblkwrt
    FROM dbe_perf.get_global_rel_iostat();


CREATE VIEW dbe_perf.file_iostat AS 
  SELECT * FROM pg_stat_get_file_stat();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_file_iostat
  (OUT node_name name, OUT filenum oid, OUT dbid oid, OUT spcid oid, OUT phyrds bigint,
   OUT phywrts bigint, OUT phyblkrd bigint, OUT phyblkwrt bigint, OUT readtim bigint, 
   OUT writetim bigint, OUT avgiotim bigint, OUT lstiotim bigint, OUT miniotim bigint, OUT maxiowtm bigint)
RETURNS setof record
AS $$
DECLARE
  row_data dbe_perf.file_iostat%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.file_iostat';
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

CREATE VIEW dbe_perf.global_file_iostat AS 
  SELECT * FROM dbe_perf.get_global_file_iostat();

CREATE VIEW dbe_perf.summary_file_iostat AS 
  SELECT 
    filenum, dbid, spcid,
    pg_catalog.sum(phyrds) AS phyrds, pg_catalog.sum(phywrts) AS phywrts, pg_catalog.sum(phyblkrd) AS phyblkrd,
    pg_catalog.sum(phyblkwrt) AS phyblkwrt, pg_catalog.sum(readtim) AS readtim, pg_catalog.sum(writetim) AS writetim,
    ((pg_catalog.sum(readtim + writetim) / greatest(pg_catalog.sum(phyrds + phywrts), 1))::bigint) AS avgiotim,
    pg_catalog.max(lstiotim) AS lstiotim, pg_catalog.min(miniotim) AS miniotim, pg_catalog.max(maxiowtm) AS maxiowtm
    FROM dbe_perf.get_global_file_iostat()
    GROUP by (filenum, dbid, spcid);


/* Lock*/
CREATE VIEW dbe_perf.locks AS
  SELECT * FROM pg_lock_status() AS L;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_locks
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
        virtualxid := row_data.virtualxid;
        transactionid := row_data.transactionid;
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

CREATE VIEW dbe_perf.global_locks AS
  SELECT * FROM dbe_perf.get_global_locks();

/* utility */
CREATE VIEW dbe_perf.replication_slots AS
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

CREATE OR REPLACE FUNCTION dbe_perf.get_global_replication_slots
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
  row_data dbe_perf.replication_slots%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.replication_slots';
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

CREATE VIEW dbe_perf.global_replication_slots AS
  SELECT * FROM dbe_perf.get_global_replication_slots();

CREATE VIEW dbe_perf.bgwriter_stat AS
  SELECT
    pg_stat_get_bgwriter_timed_checkpoints() AS checkpoints_timed,
    pg_stat_get_bgwriter_requested_checkpoints() AS checkpoints_req,
    pg_stat_get_checkpoint_write_time() AS checkpoint_write_time,
    pg_stat_get_checkpoint_sync_time() AS checkpoint_sync_time,
    pg_stat_get_bgwriter_buf_written_checkpoints() AS buffers_checkpoint,
    pg_stat_get_bgwriter_buf_written_clean() AS buffers_clean,
    pg_stat_get_bgwriter_maxwritten_clean() AS maxwritten_clean,
    pg_stat_get_buf_written_backend() AS buffers_backend,
    pg_stat_get_buf_fsync_backend() AS buffers_backend_fsync,
    pg_stat_get_buf_alloc() AS buffers_alloc,
    pg_stat_get_bgwriter_stat_reset_time() AS stats_reset;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_bgwriter_stat
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
  row_data dbe_perf.bgwriter_stat%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.bgwriter_stat';
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

CREATE VIEW dbe_perf.global_bgwriter_stat AS
  SELECT * FROM dbe_perf.get_global_bgwriter_stat();

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
    FROM pg_catalog.pg_stat_get_activity(NULL) AS S, pg_authid U,
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

CREATE VIEW dbe_perf.global_replication_stat AS
  SELECT * FROM dbe_perf.get_global_replication_stat();

/* transaction */
CREATE VIEW dbe_perf.transactions_running_xacts AS
  SELECT * FROM pg_get_running_xacts();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_transactions_running_xacts()
RETURNS setof dbe_perf.transactions_running_xacts
AS $$
DECLARE
  row_data dbe_perf.transactions_running_xacts%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn dn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.transactions_running_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_transactions_running_xacts AS
  SELECT DISTINCT * from dbe_perf.get_global_transactions_running_xacts();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_transactions_running_xacts()
RETURNS setof dbe_perf.transactions_running_xacts
AS $$
DECLARE
  row_data dbe_perf.transactions_running_xacts%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.transactions_running_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.summary_transactions_running_xacts AS
  SELECT DISTINCT * from dbe_perf.get_summary_transactions_running_xacts();

CREATE VIEW dbe_perf.transactions_prepared_xacts AS
  SELECT P.transaction, P.gid, P.prepared,
         U.rolname AS owner, D.datname AS database
    FROM pg_prepared_xact() AS P
         LEFT JOIN pg_authid U ON P.ownerid = U.oid
         LEFT JOIN pg_database D ON P.dbid = D.oid;

CREATE OR REPLACE FUNCTION dbe_perf.get_global_transactions_prepared_xacts()
RETURNS setof dbe_perf.transactions_prepared_xacts
AS $$
DECLARE
  row_data dbe_perf.transactions_prepared_xacts%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn dn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.transactions_prepared_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM dbe_perf.get_global_transactions_prepared_xacts();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_transactions_prepared_xacts()
RETURNS setof dbe_perf.transactions_prepared_xacts
AS $$
DECLARE
  row_data dbe_perf.transactions_prepared_xacts%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.transactions_prepared_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.summary_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM dbe_perf.get_summary_transactions_prepared_xacts();

/* Query unique SQL*/
CREATE VIEW dbe_perf.statement AS
  SELECT * FROM get_instr_unique_sql();

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

CREATE VIEW dbe_perf.summary_statement AS
  SELECT * FROM dbe_perf.get_summary_statement();

CREATE VIEW dbe_perf.statement_count AS
  SELECT 
    node_name,
    user_name, 
    select_count,
    update_count, 
    insert_count, 
    delete_count,
    mergeinto_count,
    ddl_count,
    dml_count,
    dcl_count,
    total_select_elapse,
    avg_select_elapse,
    max_select_elapse,
    min_select_elapse,
    total_update_elapse,
    avg_update_elapse,
    max_update_elapse,
    min_update_elapse,
    total_insert_elapse,
    avg_insert_elapse,
    max_insert_elapse,
    min_insert_elapse,
    total_delete_elapse,
    avg_delete_elapse,
    max_delete_elapse,
    min_delete_elapse
    FROM pg_stat_get_sql_count();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_statement_count()
RETURNS setof dbe_perf.statement_count
AS $$
DECLARE
  row_data dbe_perf.statement_count%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.statement_count';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW dbe_perf.global_statement_count AS
  SELECT * FROM dbe_perf.get_global_statement_count();

CREATE VIEW dbe_perf.summary_statement_count AS
  SELECT 
    user_name, 
    pg_catalog.SUM(select_count) AS select_count, pg_catalog.SUM(update_count) AS update_count,
    pg_catalog.SUM(insert_count) AS insert_count, pg_catalog.SUM(delete_count) AS delete_count,
    pg_catalog.SUM(mergeinto_count) AS mergeinto_count, pg_catalog.SUM(ddl_count) AS ddl_count,
    pg_catalog.SUM(dml_count) AS dml_count, pg_catalog.SUM(dcl_count) AS dcl_count,
    pg_catalog.SUM(total_select_elapse) AS total_select_elapse,
    ((pg_catalog.SUM(total_select_elapse) / greatest(pg_catalog.SUM(select_count), 1))::bigint) AS avg_select_elapse,
    pg_catalog.MAX(max_select_elapse) AS max_select_elapse, pg_catalog.MIN(min_select_elapse) AS min_select_elapse,
    pg_catalog.SUM(total_update_elapse) AS total_update_elapse,
    ((pg_catalog.SUM(total_update_elapse) / greatest(pg_catalog.SUM(update_count), 1))::bigint) AS avg_update_elapse,
    pg_catalog.MAX(max_update_elapse) AS max_update_elapse, pg_catalog.MIN(min_update_elapse) AS min_update_elapse,
    pg_catalog.SUM(total_insert_elapse) AS total_insert_elapse,
    ((pg_catalog.SUM(total_insert_elapse) / greatest(pg_catalog.SUM(insert_count), 1))::bigint) AS avg_insert_elapse,
    pg_catalog.MAX(max_insert_elapse) AS max_insert_elapse, pg_catalog.MIN(min_insert_elapse) AS min_insert_elapse,
    pg_catalog.SUM(total_delete_elapse) AS total_delete_elapse,
    ((pg_catalog.SUM(total_delete_elapse) / greatest(pg_catalog.SUM(delete_count), 1))::bigint) AS avg_delete_elapse,
    pg_catalog.MAX(max_delete_elapse) AS max_delete_elapse, pg_catalog.MIN(min_delete_elapse) AS min_delete_elapse
    FROM dbe_perf.get_global_statement_count() GROUP by (user_name);

/* configuration */
CREATE VIEW dbe_perf.config_settings AS
  SELECT * FROM pg_show_all_settings();

CREATE OR REPLACE FUNCTION dbe_perf.get_global_config_settings
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
  row_data dbe_perf.config_settings%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.config_settings';
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

CREATE VIEW dbe_perf.global_config_settings AS
  SELECT * FROM dbe_perf.get_global_config_settings();

/* waits*/
CREATE VIEW dbe_perf.wait_events AS
  SELECT * FROM pg_catalog.get_instr_wait_event(NULL);

CREATE OR REPLACE FUNCTION dbe_perf.get_global_wait_events()
RETURNS setof dbe_perf.wait_events
AS $$
DECLARE
  row_data dbe_perf.wait_events%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM dbe_perf.wait_events';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_wait_events AS
  SELECT * FROM dbe_perf.get_global_wait_events();

CREATE OR REPLACE FUNCTION dbe_perf.get_statement_responsetime_percentile(OUT p80 bigint, OUT p95 bigint)
RETURNS SETOF RECORD
AS $$
DECLARE
  ROW_DATA RECORD;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
  BEGIN
    QUERY_STR_NODES := 'select * from dbe_perf.node_name';
    FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
      QUERY_STR := 'SELECT * FROM pg_catalog.get_instr_rt_percentile(0)';
      FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
        p80 = ROW_DATA."P80";
        p95 = ROW_DATA."P95";
        RETURN NEXT;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';

/* the view query the cluster infomation and store in the CCN node */
CREATE VIEW dbe_perf.statement_responsetime_percentile AS
  select * from dbe_perf.get_statement_responsetime_percentile();

CREATE VIEW dbe_perf.user_login AS
  SELECT * FROM get_instr_user_login();

CREATE OR REPLACE FUNCTION dbe_perf.get_summary_user_login()
RETURNS SETOF dbe_perf.user_login
AS $$
DECLARE
  ROW_DATA dbe_perf.user_login%ROWTYPE;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
  BEGIN
    QUERY_STR_NODES := 'select * from dbe_perf.node_name';
    FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
      QUERY_STR := 'SELECT * FROM dbe_perf.user_login';
      FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
        RETURN NEXT ROW_DATA;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW dbe_perf.summary_user_login AS
  SELECT * FROM dbe_perf.get_summary_user_login();

CREATE VIEW dbe_perf.class_vital_info AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    C.relkind AS relkind
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i', 'I');

CREATE OR REPLACE FUNCTION dbe_perf.get_global_record_reset_time(OUT node_name text, OUT reset_time timestamp with time zone)
RETURNS setof record
AS $$
DECLARE
  row_data record;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from dbe_perf.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'select * from pg_catalog.get_node_stat_reset_time()';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        reset_time := row_data.get_node_stat_reset_time;
        return next;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW dbe_perf.global_candidate_status AS
       SELECT node_name, candidate_slots, get_buf_from_list, get_buf_clock_sweep, seg_candidate_slots, seg_get_buf_from_list, seg_get_buf_clock_sweep
       FROM pg_catalog.local_candidate_stat();

CREATE VIEW dbe_perf.global_ckpt_status AS
        SELECT node_name,ckpt_redo_point,ckpt_clog_flush_num,ckpt_csnlog_flush_num,ckpt_multixact_flush_num,ckpt_predicate_flush_num,ckpt_twophase_flush_num
        FROM pg_catalog.local_ckpt_stat();

CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
    SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           total_writes, low_threshold_writes, high_threshold_writes,
           total_pages, low_threshold_pages, high_threshold_pages, file_id
    FROM pg_catalog.local_double_write_stat();

CREATE OR REPLACE VIEW DBE_PERF.global_single_flush_dw_status AS
    SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num
    FROM pg_catalog.local_single_flush_dw_stat();

CREATE VIEW dbe_perf.global_pagewriter_status AS
        SELECT node_name,pgwr_actual_flush_total_num,pgwr_last_flush_num,remain_dirty_page_num,queue_head_page_rec_lsn,queue_rec_lsn,current_xlog_insert_lsn,ckpt_redo_point
        FROM pg_catalog.local_pagewriter_stat();

CREATE VIEW dbe_perf.global_record_reset_time AS
  SELECT * FROM dbe_perf.get_global_record_reset_time();

CREATE OR REPLACE VIEW dbe_perf.global_redo_status AS
    SELECT node_name, redo_start_ptr, redo_start_time, redo_done_time, curr_time, 
           min_recovery_point, read_ptr, last_replayed_read_ptr, recovery_done_ptr, 
           read_xlog_io_counter, read_xlog_io_total_dur, read_data_io_counter, read_data_io_total_dur, 
           write_data_io_counter, write_data_io_total_dur, process_pending_counter, process_pending_total_dur,
           apply_counter, apply_total_dur,
           speed, local_max_ptr, primary_flush_ptr, worker_info
    FROM pg_catalog.local_redo_stat();
  
CREATE OR REPLACE VIEW dbe_perf.global_rto_status AS
SELECT node_name, rto_info
FROM pg_catalog.local_rto_stat();

CREATE OR REPLACE VIEW dbe_perf.global_streaming_hadr_rto_and_rpo_stat AS
SELECT hadr_sender_node_name, hadr_receiver_node_name, current_rto, target_rto, current_rpo, target_rpo, rto_sleep_time, rpo_sleep_time
FROM pg_catalog.gs_hadr_local_rto_and_rpo_stat();

CREATE OR REPLACE VIEW dbe_perf.global_recovery_status AS
SELECT node_name, standby_node_name, source_ip, source_port, dest_ip, dest_port, current_rto, target_rto, current_sleep_time
FROM pg_catalog.local_recovery_status();
  
CREATE VIEW dbe_perf.local_threadpool_status AS
  SELECT * FROM threadpool_status();

CREATE OR REPLACE FUNCTION dbe_perf.global_threadpool_status()
RETURNS SETOF dbe_perf.local_threadpool_status
AS $$
DECLARE
  ROW_DATA dbe_perf.local_threadpool_status%ROWTYPE;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
BEGIN
  QUERY_STR_NODES := 'select * from dbe_perf.node_name';
  FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
    QUERY_STR := 'SELECT * FROM dbe_perf.local_threadpool_status';
    FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
      RETURN NEXT ROW_DATA;
    END LOOP;
  END LOOP;
  RETURN;
END; $$
LANGUAGE 'plpgsql';

CREATE VIEW dbe_perf.global_threadpool_status AS
  SELECT * FROM dbe_perf.global_threadpool_status();

CREATE VIEW dbe_perf.gs_slow_query_info AS
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

CREATE VIEW dbe_perf.gs_slow_query_history AS
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

CREATE OR REPLACE FUNCTION dbe_perf.global_slow_query_info_bytime(text, TIMESTAMP, TIMESTAMP, int)
RETURNS setof dbe_perf.gs_slow_query_info
AS $$
DECLARE
		row_data dbe_perf.gs_slow_query_info%rowtype;
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
				query_str_cn := 'SELECT * FROM dbe_perf.gs_slow_query_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
				FOR row_name IN EXECUTE(query_str_nodes) LOOP
						query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_cn||''';';
						FOR row_data IN EXECUTE(query_str) LOOP
								return next row_data;
						END LOOP;
				END LOOP;
				return;
		END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW dbe_perf.global_slow_query_history AS
SELECT * FROM dbe_perf.global_slow_query_history();

CREATE VIEW dbe_perf.global_slow_query_info AS
SELECT * FROM dbe_perf.global_slow_query_info();

CREATE VIEW DBE_PERF.global_plancache_status AS
  SELECT * FROM pg_catalog.plancache_status();

CREATE VIEW DBE_PERF.global_plancache_clean AS
  SELECT * FROM pg_catalog.plancache_clean();

CREATE OR REPLACE VIEW DBE_PERF.local_active_session AS
  WITH RECURSIVE
	las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
		tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
		user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state)
	  AS (select t.* from get_local_active_session() as t),
	tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
	   tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
	   user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state, final_block_sessionid, level, head)
	  AS(SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, pg_catalog.array_append('{}', las.sessionid) AS head FROM las
		UNION ALL
		 SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid,
				tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id,
				tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, tt.global_sessionid, tt.xact_start_time, tt.query_start_time, tt.state,
                las.block_sessionid AS final_block_sessionid, tt.level + 1 AS level, pg_catalog.array_append(tt.head, las.sessionid) AS head
		 FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
		 WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0)
		   AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
  SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
		 tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
		 user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status, global_sessionid, xact_start_time, query_start_time, state FROM tt
	WHERE level = (SELECT pg_catalog.MAX(level) FROM tt t1 WHERE t1.sampleid =  tt.sampleid AND t1.sessionid = tt.sessionid);
  
grant select on all tables in schema dbe_perf to public;
