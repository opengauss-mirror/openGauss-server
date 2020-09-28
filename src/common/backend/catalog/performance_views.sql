/*
 * PostgreSQL Performance Views
 *
 * Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 * src/backend/catalog/performance_views.sql
 */
/*in order to detect the perf schema with oid, SCHEMA DBE_PERF is created by default case;*/

/* OS */
CREATE VIEW DBE_PERF.os_runtime AS 
  SELECT * FROM pv_os_run_info();

CREATE VIEW DBE_PERF.node_name AS
  SELECT * FROM pgxc_node_str() AS node_name;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.os_runtime';
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

CREATE VIEW DBE_PERF.global_os_runtime AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_os_runtime();

CREATE VIEW DBE_PERF.os_threads AS
  SELECT
    S.node_name,
    S.pid,
    S.lwpid,
    S.thread_name,
    S.creation_time
    FROM pg_stat_get_thread() AS S;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.os_threads';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_os_threads AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_os_threads();

/* instance */
CREATE VIEW DBE_PERF.instance_time AS
  SELECT * FROM pv_instance_time();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.instance_time';
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

CREATE VIEW DBE_PERF.global_instance_time AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_instance_time();

/* workload */
CREATE VIEW DBE_PERF.workload_sql_count AS
  SELECT
    pg_user.respool as workload,
    sum(S.select_count)::bigint AS select_count,
    sum(S.update_count)::bigint AS update_count,
    sum(S.insert_count)::bigint AS insert_count,
    sum(S.delete_count)::bigint AS delete_count,
    sum(S.ddl_count)::bigint AS ddl_count,
    sum(S.dml_count)::bigint AS dml_count,
    sum(S.dcl_count)::bigint AS dcl_count
    FROM
      pg_user left join pg_stat_get_sql_count() AS S on pg_user.usename = S.user_name
    GROUP by pg_user.respool;

CREATE VIEW DBE_PERF.workload_sql_elapse_time AS
  SELECT
    pg_user.respool as workload,
    sum(S.total_select_elapse)::bigint AS total_select_elapse,
    MAX(S.max_select_elapse) AS max_select_elapse,
    MIN(S.min_select_elapse) AS min_select_elapse,
    ((sum(S.total_select_elapse) / greatest(sum(S.select_count), 1))::bigint) AS avg_select_elapse,
    sum(S.total_update_elapse)::bigint AS total_update_elapse,
    MAX(S.max_update_elapse) AS max_update_elapse,
    MIN(S.min_update_elapse) AS min_update_elapse,
    ((sum(S.total_update_elapse) / greatest(sum(S.update_count), 1))::bigint) AS avg_update_elapse,
    sum(S.total_insert_elapse)::bigint AS total_insert_elapse,
    MAX(S.max_insert_elapse) AS max_insert_elapse,
    MIN(S.min_insert_elapse) AS min_insert_elapse,
    ((sum(S.total_insert_elapse) / greatest(sum(S.insert_count), 1))::bigint) AS avg_insert_elapse,
    sum(S.total_delete_elapse)::bigint AS total_delete_elapse,
    MAX(S.max_delete_elapse) AS max_delete_elapse,
    MIN(S.min_delete_elapse) AS min_delete_elapse,
    ((sum(S.total_delete_elapse) / greatest(sum(S.delete_count), 1))::bigint) AS avg_delete_elapse
    FROM
      pg_user left join pg_stat_get_sql_count() AS S on pg_user.usename = S.user_name
    GROUP by pg_user.respool;

CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_workload_sql_count
  (OUT node_name name, OUT workload name, OUT select_count bigint,
   OUT update_count bigint, OUT insert_count bigint, OUT delete_count bigint,
   OUT ddl_count bigint, OUT dml_count bigint, OUT dcl_count bigint)
RETURNS setof record
AS $$
DECLARE
  row_data DBE_PERF.workload_sql_count%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.workload_sql_count';
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

CREATE VIEW DBE_PERF.summary_workload_sql_count AS
  SELECT * FROM DBE_PERF.get_summary_workload_sql_count();

CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_workload_sql_elapse_time
  (OUT node_name name, OUT workload name,
   OUT total_select_elapse bigint, OUT max_select_elapse bigint, OUT min_select_elapse bigint, OUT avg_select_elapse bigint,
   OUT total_update_elapse bigint, OUT max_update_elapse bigint, OUT min_update_elapse bigint, OUT avg_update_elapse bigint,
   OUT total_insert_elapse bigint, OUT max_insert_elapse bigint, OUT min_insert_elapse bigint, OUT avg_insert_elapse bigint,
   OUT total_delete_elapse bigint, OUT max_delete_elapse bigint, OUT min_delete_elapse bigint, OUT avg_delete_elapse bigint)
RETURNS setof record
AS $$
DECLARE
  row_data DBE_PERF.workload_sql_elapse_time%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.workload_sql_elapse_time';
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

CREATE VIEW DBE_PERF.summary_workload_sql_elapse_time AS
  SELECT * FROM DBE_PERF.get_summary_workload_sql_elapse_time();

/* user transaction */
CREATE VIEW DBE_PERF.user_transaction AS 
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
    pg_user left join get_instr_workload_info(0) AS giwi on pg_user.usesysid = giwi.user_oid;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_user_transaction
  (OUT node_name name, OUT usename name, OUT commit_counter bigint,
   OUT rollback_counter bigint, OUT resp_min bigint, OUT resp_max bigint,
   OUT resp_avg bigint, OUT resp_total bigint, OUT bg_commit_counter bigint,
   OUT bg_rollback_counter bigint, OUT bg_resp_min bigint, OUT bg_resp_max bigint, 
   OUT bg_resp_avg bigint, OUT bg_resp_total bigint)
RETURNS setof record
AS $$
DECLARE
  row_data DBE_PERF.user_transaction%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.user_transaction';
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

CREATE VIEW DBE_PERF.global_user_transaction AS
  SELECT * FROM DBE_PERF.get_global_user_transaction();

/* workload transaction */
CREATE VIEW DBE_PERF.workload_transaction AS
select
    pg_user.respool as workload,
    sum(W.commit_counter)::bigint as commit_counter,
    sum(W.rollback_counter)::bigint as rollback_counter,
    min(W.resp_min)::bigint as resp_min,
    max(W.resp_max)::bigint as resp_max,
    ((sum(W.resp_total) / greatest(sum(W.commit_counter), 1))::bigint) AS resp_avg,
    sum(W.resp_total)::bigint as resp_total,
    sum(W.bg_commit_counter)::bigint as bg_commit_counter,
    sum(W.bg_rollback_counter)::bigint as bg_rollback_counter,
    min(W.bg_resp_min)::bigint as bg_resp_min,
    max(W.bg_resp_max)::bigint as bg_resp_max,
    ((sum(W.bg_resp_total) / greatest(sum(W.bg_commit_counter), 1))::bigint) AS bg_resp_avg,
    sum(W.bg_resp_total)::bigint as bg_resp_total
from
    pg_user left join DBE_PERF.user_transaction AS W on pg_user.usename = W.usename
group by
    pg_user.respool;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_workload_transaction
  (OUT node_name name, OUT workload name, OUT commit_counter bigint,
   OUT rollback_counter bigint, OUT resp_min bigint, OUT resp_max bigint, 
   OUT resp_avg bigint, OUT resp_total bigint, OUT bg_commit_counter bigint,
   OUT bg_rollback_counter bigint, OUT bg_resp_min bigint, OUT bg_resp_max bigint, 
   OUT bg_resp_avg bigint, OUT bg_resp_total bigint)
RETURNS setof record
AS $$
DECLARE
  row_data DBE_PERF.workload_transaction%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.workload_transaction';
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

CREATE VIEW DBE_PERF.global_workload_transaction AS
  SELECT * FROM DBE_PERF.get_global_workload_transaction();

CREATE VIEW DBE_PERF.summary_workload_transaction AS
  SELECT 
    W.workload AS workload,
    sum(W.commit_counter) AS commit_counter,
    sum(W.rollback_counter) AS rollback_counter,
    coalesce(min(NULLIF(W.resp_min, 0)), 0) AS resp_min,
    max(W.resp_max) AS resp_max,
    ((sum(W.resp_total) / greatest(sum(W.commit_counter), 1))::bigint) AS resp_avg,
    sum(W.resp_total) AS resp_total,
    sum(W.bg_commit_counter) AS bg_commit_counter,
    sum(W.bg_rollback_counter) AS bg_rollback_counter,
    coalesce(min(NULLIF(W.bg_resp_min, 0)), 0) AS bg_resp_min,
    max(W.bg_resp_max) AS bg_resp_max,
    ((sum(W.bg_resp_total) / greatest(sum(W.bg_commit_counter), 1))::bigint) AS bg_resp_avg,
    sum(W.bg_resp_total) AS bg_resp_total
    FROM DBE_PERF.get_global_workload_transaction() AS W
    GROUP by W.workload;

/* Session/Thread */
CREATE VIEW DBE_PERF.session_stat AS 
  SELECT * FROM pv_session_stat();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.session_stat';
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

CREATE VIEW DBE_PERF.global_session_stat AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_stat();

CREATE VIEW DBE_PERF.session_time AS 
  SELECT * FROM pv_session_time();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.session_time';
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

CREATE VIEW DBE_PERF.global_session_time AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_time();

CREATE VIEW DBE_PERF.session_memory AS 
  SELECT * FROM pv_session_memory();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.session_memory';
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

CREATE VIEW DBE_PERF.global_session_memory AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_memory();

CREATE VIEW DBE_PERF.session_memory_detail AS 
  SELECT * FROM gs_session_memory_detail_tp();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.session_memory_detail';
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

CREATE VIEW DBE_PERF.global_session_memory_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_memory_detail();

CREATE VIEW DBE_PERF.session_cpu_runtime AS
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

CREATE VIEW DBE_PERF.session_memory_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.session_stat_activity AS
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
    fetch_coor := 'select * from DBE_PERF.node_name';
    FOR coor_name IN EXECUTE(fetch_coor) LOOP
      coorname :=  coor_name.node_name;
      fet_active := 'SELECT * FROM DBE_PERF.session_stat_activity';
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

CREATE VIEW DBE_PERF.global_session_stat_activity AS 
  SELECT * FROM DBE_PERF.get_global_session_stat_activity();

CREATE VIEW DBE_PERF.thread_wait_status AS
  SELECT * FROM pg_stat_get_status(NULL);

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.thread_wait_status';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_thread_wait_status AS
  SELECT * FROM DBE_PERF.get_global_thread_wait_status();

/* WLM */
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
    
CREATE VIEW DBE_PERF.wlm_user_resource_config AS
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

CREATE VIEW DBE_PERF.operator_history_table AS
  SELECT * FROM gs_wlm_operator_info; 

--createing history operator-level view for test in multi-CN from single CN
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.operator_history_table';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_operator_history_table AS
  SELECT * FROM DBE_PERF.get_global_operator_history_table();

--history operator-level view for DM in single CN
CREATE VIEW DBE_PERF.operator_history AS
  SELECT * FROM pg_stat_get_wlm_operator_info(0);

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.operator_history';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW DBE_PERF.global_operator_history AS
    SELECT * FROM DBE_PERF.get_global_operator_history();

--real time operator-level view in single CN
CREATE VIEW DBE_PERF.operator_runtime AS
  SELECT t.*
  FROM DBE_PERF.session_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
    WHERE s.query_id = t.queryid;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_operator_runtime()
RETURNS setof DBE_PERF.operator_runtime
AS $$
DECLARE
  row_data DBE_PERF.operator_runtime%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.operator_runtime';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_operator_runtime AS
  SELECT * FROM DBE_PERF.get_global_operator_runtime();

/* Query-AP */
CREATE VIEW DBE_PERF.statement_complex_history AS
  SELECT * FROM pg_stat_get_wlm_session_info(0);

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.statement_complex_history';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_statement_complex_history AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history();

CREATE VIEW DBE_PERF.statement_complex_history_table AS
  SELECT * FROM gs_wlm_session_info;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statement_complex_history_table()
RETURNS setof DBE_PERF.statement_complex_history_table
AS $$
DECLARE
  row_data DBE_PERF.statement_complex_history_table%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all cn node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statement_complex_history_table';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_statement_complex_history_table AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history_table();

CREATE VIEW DBE_PERF.statement_user_complex_history AS
  SELECT * FROM DBE_PERF.statement_complex_history_table
    WHERE username = current_user::text;

CREATE VIEW DBE_PERF.statement_complex_runtime AS
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
  FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_realtime_session_info(NULL) AS T
    WHERE S.pid = T.threadid;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_statement_complex_runtime()
RETURNS setof DBE_PERF.statement_complex_runtime
AS $$
DECLARE
  row_data DBE_PERF.statement_complex_runtime%rowtype;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statement_complex_runtime';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_statement_complex_runtime AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_runtime();

CREATE VIEW DBE_PERF.statement_iostat_complex_runtime AS
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
   FROM pg_stat_activity_ng AS S, pg_stat_get_wlm_session_iostat_info(0) AS T
     WHERE S.pid = T.threadid;

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

/* Memory */
CREATE VIEW DBE_PERF.memory_node_detail AS 
  SELECT * FROM pv_total_memory_detail();

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_memory_node_detail()
RETURNS setof DBE_PERF.memory_node_detail
AS $$
DECLARE
  row_name record;
  row_data DBE_PERF.memory_node_detail%rowtype;
  query_str text;
  query_str_nodes text;
  BEGIN
    --Get all the node names
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.memory_node_detail';
        FOR row_data IN EXECUTE(query_str) LOOP
            RETURN NEXT row_data;
        END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_memory_node_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_memory_node_detail();

CREATE VIEW DBE_PERF.shared_memory_detail AS 
  SELECT * FROM pg_shared_memory_detail();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.shared_memory_detail';
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

CREATE VIEW DBE_PERF.global_shared_memory_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_shared_memory_detail();

/* cache I/O */
CREATE VIEW DBE_PERF.statio_all_indexes AS
  SELECT
    C.oid AS relid,
    I.oid AS indexrelid,
    N.nspname AS schemaname,
    C.relname AS relname,
    I.relname AS indexrelname,
    pg_stat_get_blocks_fetched(I.oid) -
    pg_stat_get_blocks_hit(I.oid) AS idx_blks_read,
    pg_stat_get_blocks_hit(I.oid) AS idx_blks_hit
  FROM pg_class C JOIN
       pg_index X ON C.oid = X.indrelid JOIN
       pg_class I ON I.oid = X.indexrelid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't');

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_all_indexes';
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

CREATE VIEW DBE_PERF.global_statio_all_indexes AS
  SELECT *  FROM DBE_PERF.get_global_statio_all_indexes();

CREATE OR REPLACE FUNCTION DBE_PERF.get_local_toastname_and_toastindexname(OUT shemaname name, OUT relname name, OUT toastname name, OUT toastindexname name)
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.statio_all_indexes T
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

CREATE VIEW DBE_PERF.summary_statio_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit 
    FROM DBE_PERF.get_summary_statio_all_indexes() as Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.statio_all_sequences AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_stat_get_blocks_fetched(C.oid) -
    pg_stat_get_blocks_hit(C.oid) AS blks_read,
    pg_stat_get_blocks_hit(C.oid) AS blks_hit
  FROM pg_class C
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind = 'S';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_all_sequences';
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

CREATE VIEW DBE_PERF.global_statio_all_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_all_sequences();

CREATE VIEW DBE_PERF.summary_statio_all_sequences AS
  SELECT schemaname, relname, 
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
   FROM DBE_PERF.get_global_statio_all_sequences() 
   GROUP BY (schemaname, relname);

CREATE VIEW DBE_PERF.statio_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_stat_get_blocks_fetched(C.oid) -
    pg_stat_get_blocks_hit(C.oid) AS heap_blks_read,
    pg_stat_get_blocks_hit(C.oid) AS heap_blks_hit,
    sum(pg_stat_get_blocks_fetched(I.indexrelid) -
        pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_read,
    sum(pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_hit,
    pg_stat_get_blocks_fetched(T.oid) -
      pg_stat_get_blocks_hit(T.oid) AS toast_blks_read,
    pg_stat_get_blocks_hit(T.oid) AS toast_blks_hit,
    pg_stat_get_blocks_fetched(X.oid) -
      pg_stat_get_blocks_hit(X.oid) AS tidx_blks_read,
    pg_stat_get_blocks_hit(X.oid) AS tidx_blks_hit
  FROM pg_class C LEFT JOIN
       pg_index I ON C.oid = I.indrelid LEFT JOIN
       pg_class T ON C.reltoastrelid = T.oid LEFT JOIN
       pg_class X ON T.reltoastidxid = X.oid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
  GROUP BY C.oid, N.nspname, C.relname, T.oid, X.oid;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_all_tables';
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
  
CREATE VIEW DBE_PERF.global_statio_all_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_all_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
        FROM DBE_PERF.statio_all_tables C 
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

CREATE OR REPLACE FUNCTION DBE_PERF.get_local_toast_relation(OUT shemaname name, OUT relname name, OUT toastname name)
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

CREATE VIEW DBE_PERF.summary_statio_all_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname,
         SUM(Ti.heap_blks_read) heap_blks_read, SUM(Ti.heap_blks_hit) heap_blks_hit,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit,
         SUM(Ti.toast_blks_read) toast_blks_read, SUM(Ti.toast_blks_hit) toast_blks_hit,
         SUM(Ti.tidx_blks_read) tidx_blks_read, SUM(Ti.tidx_blks_hit) tidx_blks_hit
    FROM DBE_PERF.get_summary_statio_all_tables() Ti left join DBE_PERF.get_local_toast_relation() Tn on Tn.shemaname = Ti.toastrelschemaname and Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE VIEW DBE_PERF.statio_sys_indexes AS
  SELECT * FROM DBE_PERF.statio_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'snapshot') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_sys_indexes';
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

CREATE VIEW DBE_PERF.global_statio_sys_indexes AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_indexes();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.statio_sys_indexes T
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

CREATE VIEW DBE_PERF.summary_statio_sys_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit
  FROM DBE_PERF.get_summary_statio_sys_indexes() AS Ti
      LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.statio_sys_sequences AS
  SELECT * FROM DBE_PERF.statio_all_sequences
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_sys_sequences';
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

CREATE VIEW DBE_PERF.global_statio_sys_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_sequences();

CREATE VIEW DBE_PERF.summary_statio_sys_sequences AS
  SELECT schemaname, relname, 
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
    FROM DBE_PERF.get_global_statio_sys_sequences() 
  GROUP BY (schemaname, relname);

CREATE VIEW DBE_PERF.statio_sys_tables AS
  SELECT * FROM DBE_PERF.statio_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema', 'snapshot') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_sys_tables';
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
  
CREATE VIEW DBE_PERF.global_statio_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.statio_sys_tables C
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

CREATE VIEW DBE_PERF.summary_statio_sys_tables AS
  SELECT 
    Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) as relname, 
    SUM(Ti.heap_blks_read) heap_blks_read, SUM(Ti.heap_blks_hit) heap_blks_hit,
    SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit,
    SUM(Ti.toast_blks_read) toast_blks_read, SUM(Ti.toast_blks_hit) toast_blks_hit,
    SUM(Ti.tidx_blks_read) tidx_blks_read, SUM(Ti.tidx_blks_hit) tidx_blks_hit
  FROM DBE_PERF.get_summary_statio_sys_tables() as Ti 
    LEFT JOIN DBE_PERF.get_local_toast_relation() Tn 
    ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE VIEW DBE_PERF.statio_user_indexes AS
  SELECT * FROM DBE_PERF.statio_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'snapshot') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_user_indexes';
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

CREATE VIEW DBE_PERF.global_statio_user_indexes AS
  SELECT *  FROM DBE_PERF.get_global_statio_user_indexes();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.statio_user_indexes T
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

CREATE VIEW DBE_PERF.summary_statio_user_indexes AS
  SELECT
      Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit 
  FROM DBE_PERF.get_summary_statio_user_indexes() AS Ti
      LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.statio_user_sequences AS
  SELECT * FROM DBE_PERF.statio_all_sequences
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_user_sequences';
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

CREATE VIEW DBE_PERF.global_statio_user_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_user_sequences();

CREATE VIEW DBE_PERF.summary_statio_user_sequences AS
  SELECT schemaname, relname, 
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
    FROM DBE_PERF.get_global_statio_user_sequences() 
  GROUP BY (schemaname, relname);

CREATE VIEW DBE_PERF.statio_user_tables AS
  SELECT * FROM DBE_PERF.statio_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'snapshot') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statio_user_tables';
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
  
CREATE VIEW DBE_PERF.global_statio_user_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_user_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.statio_user_tables C
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

CREATE VIEW DBE_PERF.summary_statio_user_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname, 
         SUM(Ti.heap_blks_read) heap_blks_read, SUM(Ti.heap_blks_hit) heap_blks_hit,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit,
         SUM(Ti.toast_blks_read) toast_blks_read, SUM(Ti.toast_blks_hit) toast_blks_hit,
         SUM(Ti.tidx_blks_read) tidx_blks_read, SUM(Ti.tidx_blks_hit) tidx_blks_hit
  FROM DBE_PERF.get_summary_statio_user_tables() AS Ti LEFT JOIN DBE_PERF.get_local_toast_relation() Tn 
         ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT  D.datname AS datname,
      pg_stat_get_db_cu_mem_hit(D.oid) AS mem_hit,
      pg_stat_get_db_cu_hdd_sync(D.oid) AS hdd_sync_read,
      pg_stat_get_db_cu_hdd_asyn(D.oid) AS hdd_asyn_read
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

CREATE VIEW DBE_PERF.global_stat_db_cu AS
  SELECT * FROM DBE_PERF.get_stat_db_cu();

CREATE VIEW DBE_PERF.global_stat_session_cu AS
  SELECT * FROM pg_stat_session_cu();

/* Object */
CREATE VIEW DBE_PERF.stat_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_stat_get_numscans(C.oid) AS seq_scan,
    pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
    sum(pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
    sum(pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
    pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
    pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
    pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
    pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
    pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
    pg_stat_get_live_tuples(C.oid) AS n_live_tup,
    pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
    pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
    pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
    pg_stat_get_last_analyze_time(C.oid) as last_analyze,
    pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
    pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
    pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
    pg_stat_get_analyze_count(C.oid) AS analyze_count,
    pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_all_tables
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
        query_str_nodes := 'select * from DBE_PERF.node_name';
        FOR row_name IN EXECUTE(query_str_nodes) LOOP
            query_str := 'SELECT * FROM DBE_PERF.stat_all_tables';
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

CREATE VIEW DBE_PERF.global_stat_all_tables AS
	SELECT * FROM DBE_PERF.get_global_stat_all_tables();

CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_all_tables
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
        query_str_nodes := 'select * from DBE_PERF.node_name';
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
                FROM DBE_PERF.stat_all_tables T 
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

CREATE VIEW DBE_PERF.summary_stat_all_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) as relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd, 
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         SUM(Ti.n_live_tup) n_live_tup, SUM(Ti.n_dead_tup) n_dead_tup,
         MAX(Ti.last_vacuum) last_vacuum, MAX(Ti.last_autovacuum) last_autovacuum,
         MAX(Ti.last_analyze) last_analyze, MAX(Ti.last_autoanalyze) last_autoanalyze,
         SUM(Ti.vacuum_count) vacuum_count, SUM(Ti.autovacuum_count) autovacuum_count,
         SUM(Ti.analyze_count) analyze_count, SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM (SELECT * FROM DBE_PERF.get_summary_stat_all_tables()) AS Ti
    LEFT JOIN DBE_PERF.get_local_toast_relation() Tn
         ON Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_all_indexes AS
  SELECT
    C.oid AS relid,
    I.oid AS indexrelid,
    N.nspname AS schemaname,
    C.relname AS relname,
    I.relname AS indexrelname,
    pg_stat_get_numscans(I.oid) AS idx_scan,
    pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
    pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't');

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_all_indexes';
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
  
CREATE VIEW DBE_PERF.global_stat_all_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_all_indexes();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_all_indexes T
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

CREATE VIEW DBE_PERF.summary_stat_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_all_indexes() AS Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.stat_sys_tables AS
  SELECT * FROM DBE_PERF.stat_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_sys_tables';
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
  
CREATE VIEW DBE_PERF.global_stat_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_sys_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_sys_tables T
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

CREATE VIEW DBE_PERF.summary_stat_sys_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname, 
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd, 
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         SUM(Ti.n_live_tup) n_live_tup, SUM(Ti.n_dead_tup) n_dead_tup,
         MAX(Ti.last_vacuum) last_vacuum, MAX(Ti.last_autovacuum) last_autovacuum,
         MAX(Ti.last_analyze) last_analyze, MAX(Ti.last_autoanalyze) last_autoanalyze,
         SUM(Ti.vacuum_count) vacuum_count, SUM(Ti.autovacuum_count) autovacuum_count,
         SUM(Ti.analyze_count) analyze_count, SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM DBE_PERF.get_summary_stat_sys_tables() as Ti LEFT JOIN DBE_PERF.get_local_toast_relation() as Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_sys_indexes AS
  SELECT * FROM DBE_PERF.stat_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_sys_indexes';
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
  
CREATE VIEW DBE_PERF.global_stat_sys_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_sys_indexes();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_sys_indexes T
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

CREATE VIEW DBE_PERF.summary_stat_sys_indexes AS
  SELECT Ti.schemaname AS schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_sys_indexes() AS Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.stat_user_tables AS
  SELECT * FROM DBE_PERF.stat_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_user_tables';
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
  
CREATE VIEW DBE_PERF.global_stat_user_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_user_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_user_tables T 
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

CREATE VIEW DBE_PERF.summary_stat_user_tables AS
  SELECT Ti.schemaname AS schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname, 
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_fetch) idx_tup_fetch,
         SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd, 
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd,
         SUM(Ti.n_live_tup) n_live_tup, SUM(Ti.n_dead_tup) n_dead_tup,
         MAX(Ti.last_vacuum) last_vacuum, MAX(Ti.last_autovacuum) last_autovacuum,
         MAX(Ti.last_analyze) last_analyze, MAX(Ti.last_autoanalyze) last_autoanalyze,
         SUM(Ti.vacuum_count) vacuum_count, SUM(Ti.autovacuum_count) autovacuum_count,
         SUM(Ti.analyze_count) analyze_count, SUM(Ti.autoanalyze_count) autoanalyze_count
    FROM DBE_PERF.get_summary_stat_user_tables() AS Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_user_indexes AS
  SELECT * FROM DBE_PERF.stat_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_user_indexes';
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
  
CREATE VIEW DBE_PERF.global_stat_user_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_user_indexes();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_user_indexes T
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

CREATE VIEW DBE_PERF.summary_stat_user_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_user_indexes() as Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE VIEW DBE_PERF.stat_database AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_database';
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

CREATE VIEW DBE_PERF.global_stat_database AS
  SELECT * FROM DBE_PERF.get_global_stat_database();

CREATE VIEW DBE_PERF.summary_stat_database AS
  SELECT ALL_NODES.datname,
         ALL_NODES.numbackends, ALL_NODES.xact_commit, ALL_NODES.xact_rollback,
         ALL_NODES.blks_read, ALL_NODES.blks_hit, ALL_NODES.tup_returned, ALL_NODES.tup_fetched, 
         SUMMARY_ITEM.tup_inserted, SUMMARY_ITEM.tup_updated, SUMMARY_ITEM.tup_deleted, 
         SUMMARY_ITEM.conflicts, ALL_NODES.temp_files, ALL_NODES.temp_bytes, SUMMARY_ITEM.deadlocks, 
         ALL_NODES.blk_read_time, ALL_NODES.blk_write_time, ALL_NODES.stats_reset
    FROM
      DBE_PERF.stat_database AS SUMMARY_ITEM,
      (SELECT datname,
         SUM(numbackends) numbackends, SUM(xact_commit) xact_commit, SUM(xact_rollback) xact_rollback,
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit, SUM(tup_returned) tup_returned,
         SUM(tup_fetched) tup_fetched, SUM(temp_files) temp_files, 
         SUM(temp_bytes) temp_bytes, SUM(blk_read_time) blk_read_time, 
         SUM(blk_write_time) blk_write_time, MAX(stats_reset) stats_reset
      FROM DBE_PERF.get_global_stat_database() GROUP BY (datname)) AS ALL_NODES
    WHERE ALL_NODES.datname = SUMMARY_ITEM.datname;
      
CREATE VIEW DBE_PERF.stat_database_conflicts AS
  SELECT
    D.oid AS datid,
    D.datname AS datname,
    pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
  FROM pg_database D;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_database_conflicts';
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

CREATE VIEW DBE_PERF.global_stat_database_conflicts AS
  SELECT * FROM DBE_PERF.get_global_stat_database_conflicts();

CREATE VIEW DBE_PERF.summary_stat_database_conflicts AS
  SELECT
    D.datname AS datname,
    pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
  FROM pg_database D;

CREATE VIEW DBE_PERF.stat_xact_all_tables AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    pg_stat_get_xact_numscans(C.oid) AS seq_scan,
    pg_stat_get_xact_tuples_returned(C.oid) AS seq_tup_read,
    sum(pg_stat_get_xact_numscans(I.indexrelid))::bigint AS idx_scan,
    sum(pg_stat_get_xact_tuples_fetched(I.indexrelid))::bigint +
    pg_stat_get_xact_tuples_fetched(C.oid) AS idx_tup_fetch,
    pg_stat_get_xact_tuples_inserted(C.oid) AS n_tup_ins,
    pg_stat_get_xact_tuples_updated(C.oid) AS n_tup_upd,
    pg_stat_get_xact_tuples_deleted(C.oid) AS n_tup_del,
    pg_stat_get_xact_tuples_hot_updated(C.oid) AS n_tup_hot_upd
  FROM pg_class C LEFT JOIN
       pg_index I ON C.oid = I.indrelid
       LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't')
  GROUP BY C.oid, N.nspname, C.relname;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.stat_xact_all_tables';
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM DBE_PERF.stat_xact_all_tables where schemaname = ''pg_catalog'' or schemaname =''pg_toast'' ';
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

CREATE VIEW DBE_PERF.global_stat_xact_all_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_all_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
        FROM DBE_PERF.stat_xact_all_tables T
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

CREATE VIEW DBE_PERF.summary_stat_xact_all_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_all_tables() as Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn 
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_xact_sys_tables AS
  SELECT * FROM DBE_PERF.stat_xact_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_xact_sys_tables';
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

CREATE VIEW DBE_PERF.global_stat_xact_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_sys_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_xact_sys_tables T
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

CREATE VIEW DBE_PERF.summary_stat_xact_sys_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_sys_tables() as Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_xact_user_tables AS
  SELECT * FROM DBE_PERF.stat_xact_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_xact_user_tables';
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

CREATE VIEW DBE_PERF.global_stat_xact_user_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_user_tables();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
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
      FROM DBE_PERF.stat_xact_user_tables T
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

CREATE VIEW DBE_PERF.summary_stat_xact_user_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_user_tables() AS Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE VIEW DBE_PERF.stat_user_functions AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_user_functions';
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

CREATE VIEW DBE_PERF.global_stat_user_functions AS
  SELECT * FROM DBE_PERF.get_global_stat_user_functions();

CREATE VIEW DBE_PERF.summary_stat_user_functions AS
  SELECT schemaname, funcname,
         SUM(calls) calls, SUM(total_time) total_time, SUM(self_time) self_time
    FROM DBE_PERF.get_global_stat_user_functions() 
    GROUP BY (schemaname, funcname);

CREATE VIEW DBE_PERF.stat_xact_user_functions AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_xact_user_functions';
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

CREATE VIEW DBE_PERF.global_stat_xact_user_functions AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_user_functions();

CREATE VIEW DBE_PERF.summary_stat_xact_user_functions AS
  SELECT schemaname, funcname,
         SUM(calls) calls, SUM(total_time) total_time, SUM(self_time) self_time
    FROM DBE_PERF.get_global_stat_xact_user_functions() 
    GROUP BY (schemaname, funcname);

CREATE VIEW DBE_PERF.stat_bad_block AS
  SELECT DISTINCT * from pg_stat_bad_block();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.stat_bad_block';
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

CREATE VIEW DBE_PERF.global_stat_bad_block AS
  SELECT DISTINCT * from DBE_PERF.get_global_stat_bad_block();

CREATE VIEW DBE_PERF.summary_stat_bad_block AS
  SELECT databaseid, tablespaceid, relfilenode,
         SUM(forknum) forknum, SUM(error_count) error_count,
         MIN(first_time) first_time, MAX(last_time) last_time
    FROM DBE_PERF.get_global_stat_bad_block() 
    GROUP BY (databaseid, tablespaceid, relfilenode);

/* File */
CREATE VIEW DBE_PERF.file_redo_iostat AS 
  SELECT * FROM pg_stat_get_redo_stat();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.file_redo_iostat';
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

CREATE VIEW DBE_PERF.global_file_redo_iostat AS 
  SELECT * FROM DBE_PERF.get_global_file_redo_iostat();

CREATE VIEW DBE_PERF.summary_file_redo_iostat AS 
  SELECT 
    sum(phywrts) AS phywrts,
    sum(phyblkwrt) AS phyblkwrt,
    sum(writetim) AS writetim,
    ((sum(writetim) / greatest(sum(phywrts), 1))::bigint) AS avgiotim,
    max(lstiotim) AS lstiotim,
    min(miniotim) AS miniotim,
    max(maxiowtm) AS maxiowtm
    FROM DBE_PERF.get_global_file_redo_iostat();

CREATE VIEW DBE_PERF.local_rel_iostat AS
  SELECT * FROM get_local_rel_iostat();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.local_rel_iostat';
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

CREATE VIEW DBE_PERF.global_rel_iostat AS
  SELECT * FROM DBE_PERF.get_global_rel_iostat();

CREATE VIEW DBE_PERF.summary_rel_iostat AS
  SELECT
    sum(phyrds) AS phyrds, sum(phywrts) AS phywrts, sum(phyblkrd) AS phyblkrd,
    sum(phyblkwrt) AS phyblkwrt
    FROM DBE_PERF.get_global_rel_iostat();


CREATE VIEW DBE_PERF.file_iostat AS 
  SELECT * FROM pg_stat_get_file_stat();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.file_iostat';
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

CREATE VIEW DBE_PERF.global_file_iostat AS 
  SELECT * FROM DBE_PERF.get_global_file_iostat();

CREATE VIEW DBE_PERF.summary_file_iostat AS 
  SELECT 
    filenum, dbid, spcid,
    sum(phyrds) AS phyrds, sum(phywrts) AS phywrts, sum(phyblkrd) AS phyblkrd,
    sum(phyblkwrt) AS phyblkwrt, sum(readtim) AS readtim, sum(writetim) AS writetim,
    ((sum(readtim + writetim) / greatest(sum(phyrds + phywrts), 1))::bigint) AS avgiotim,
    max(lstiotim) AS lstiotim, min(miniotim) AS miniotim, max(maxiowtm) AS maxiowtm
    FROM DBE_PERF.get_global_file_iostat()
    GROUP by (filenum, dbid, spcid);


/* Lock*/
CREATE VIEW DBE_PERF.locks AS
  SELECT * FROM pg_lock_status() AS L;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.locks';
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

CREATE VIEW DBE_PERF.global_locks AS
  SELECT * FROM DBE_PERF.get_global_locks();

/* utility */
CREATE VIEW DBE_PERF.replication_slots AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.replication_slots';
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

CREATE VIEW DBE_PERF.global_replication_slots AS
  SELECT * FROM DBE_PERF.get_global_replication_slots();

CREATE VIEW DBE_PERF.bgwriter_stat AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.bgwriter_stat';
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

CREATE VIEW DBE_PERF.global_bgwriter_stat AS
  SELECT * FROM DBE_PERF.get_global_bgwriter_stat();

CREATE VIEW DBE_PERF.replication_stat AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.replication_stat';
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

CREATE VIEW DBE_PERF.global_replication_stat AS
  SELECT * FROM DBE_PERF.get_global_replication_stat();

/* transaction */
CREATE VIEW DBE_PERF.transactions_running_xacts AS
  SELECT * FROM pg_get_running_xacts();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.transactions_running_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_transactions_running_xacts AS
  SELECT DISTINCT * from DBE_PERF.get_global_transactions_running_xacts();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.transactions_running_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.summary_transactions_running_xacts AS
  SELECT DISTINCT * from DBE_PERF.get_summary_transactions_running_xacts();

CREATE VIEW DBE_PERF.transactions_prepared_xacts AS
  SELECT P.transaction, P.gid, P.prepared,
         U.rolname AS owner, D.datname AS database
    FROM pg_prepared_xact() AS P
         LEFT JOIN pg_authid U ON P.ownerid = U.oid
         LEFT JOIN pg_database D ON P.dbid = D.oid;

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.transactions_prepared_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_transactions_prepared_xacts();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.transactions_prepared_xacts';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.summary_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM DBE_PERF.get_summary_transactions_prepared_xacts();

/* Query unique SQL*/
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statement';
        FOR row_data IN EXECUTE(query_str) LOOP
          return next row_data;
       END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.summary_statement AS
  SELECT * FROM DBE_PERF.get_summary_statement();

CREATE VIEW DBE_PERF.statement_count AS
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.statement_count';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW DBE_PERF.global_statement_count AS
  SELECT * FROM DBE_PERF.get_global_statement_count();

CREATE VIEW DBE_PERF.summary_statement_count AS
  SELECT 
    user_name, 
    SUM(select_count) AS select_count, SUM(update_count) AS update_count,
    SUM(insert_count) AS insert_count, SUM(delete_count) AS delete_count,
    SUM(mergeinto_count) AS mergeinto_count, SUM(ddl_count) AS ddl_count,
    SUM(dml_count) AS dml_count, SUM(dcl_count) AS dcl_count,
    SUM(total_select_elapse) AS total_select_elapse,
    ((SUM(total_select_elapse) / greatest(SUM(select_count), 1))::bigint) AS avg_select_elapse,
    MAX(max_select_elapse) AS max_select_elapse, MIN(min_select_elapse) AS min_select_elapse,
    SUM(total_update_elapse) AS total_update_elapse,
    ((SUM(total_update_elapse) / greatest(SUM(update_count), 1))::bigint) AS avg_update_elapse,
    MAX(max_update_elapse) AS max_update_elapse, MIN(min_update_elapse) AS min_update_elapse,
    SUM(total_insert_elapse) AS total_insert_elapse,
    ((SUM(total_insert_elapse) / greatest(SUM(insert_count), 1))::bigint) AS avg_insert_elapse,
    MAX(max_insert_elapse) AS max_insert_elapse, MIN(min_insert_elapse) AS min_insert_elapse,
    SUM(total_delete_elapse) AS total_delete_elapse,
    ((SUM(total_delete_elapse) / greatest(SUM(delete_count), 1))::bigint) AS avg_delete_elapse,
    MAX(max_delete_elapse) AS max_delete_elapse, MIN(min_delete_elapse) AS min_delete_elapse
    FROM DBE_PERF.get_global_statement_count() GROUP by (user_name);

/* configuration */
CREATE VIEW DBE_PERF.config_settings AS
  SELECT * FROM pg_show_all_settings();

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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.config_settings';
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

CREATE VIEW DBE_PERF.global_config_settings AS
  SELECT * FROM DBE_PERF.get_global_config_settings();

/* waits*/
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
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'SELECT * FROM DBE_PERF.wait_events';
      FOR row_data IN EXECUTE(query_str) LOOP
        return next row_data;
      END LOOP;
    END LOOP;
    return;
  END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW DBE_PERF.global_wait_events AS
  SELECT * FROM DBE_PERF.get_global_wait_events();

CREATE OR REPLACE FUNCTION DBE_PERF.get_statement_responsetime_percentile(OUT p80 bigint, OUT p95 bigint)
RETURNS SETOF RECORD
AS $$
DECLARE
  ROW_DATA RECORD;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
  BEGIN
    QUERY_STR_NODES := 'select * from DBE_PERF.node_name';
    FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
      QUERY_STR := 'SELECT * FROM get_instr_rt_percentile(0)';
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
CREATE VIEW DBE_PERF.statement_responsetime_percentile AS
  select * from DBE_PERF.get_statement_responsetime_percentile();

CREATE VIEW DBE_PERF.user_login AS
  SELECT * FROM get_instr_user_login();

CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_user_login()
RETURNS SETOF DBE_PERF.user_login
AS $$
DECLARE
  ROW_DATA DBE_PERF.user_login%ROWTYPE;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
  BEGIN
    QUERY_STR_NODES := 'select * from DBE_PERF.node_name';
    FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
      QUERY_STR := 'SELECT * FROM DBE_PERF.user_login';
      FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
        RETURN NEXT ROW_DATA;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW DBE_PERF.summary_user_login AS
  SELECT * FROM DBE_PERF.get_summary_user_login();

CREATE VIEW DBE_PERF.class_vital_info AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    C.relkind AS relkind
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i');

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_record_reset_time(OUT node_name text, OUT reset_time timestamp with time zone)
RETURNS setof record
AS $$
DECLARE
  row_data record;
  row_name record;
  query_str text;
  query_str_nodes text;
  BEGIN
    query_str_nodes := 'select * from DBE_PERF.node_name';
    FOR row_name IN EXECUTE(query_str_nodes) LOOP
      query_str := 'select * from get_node_stat_reset_time()';
      FOR row_data IN EXECUTE(query_str) LOOP
        node_name := row_name.node_name;
        reset_time := row_data.get_node_stat_reset_time;
        return next;
      END LOOP;
    END LOOP;
    RETURN;
  END; $$
LANGUAGE 'plpgsql';

CREATE VIEW DBE_PERF.global_ckpt_status AS
        SELECT node_name,ckpt_redo_point,ckpt_clog_flush_num,ckpt_csnlog_flush_num,ckpt_multixact_flush_num,ckpt_predicate_flush_num,ckpt_twophase_flush_num
        FROM pg_catalog.local_ckpt_stat();

CREATE OR REPLACE VIEW DBE_PERF.global_double_write_status AS
    SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           total_writes, low_threshold_writes, high_threshold_writes,
           total_pages, low_threshold_pages, high_threshold_pages
    FROM pg_catalog.local_double_write_stat();

CREATE VIEW DBE_PERF.global_get_bgwriter_status AS
        SELECT node_name,bgwr_actual_flush_total_num,bgwr_last_flush_num,candidate_slots,get_buffer_from_list,get_buf_clock_sweep
        FROM pg_catalog.remote_bgwriter_stat()
        UNION ALL
        SELECT node_name,bgwr_actual_flush_total_num,bgwr_last_flush_num,candidate_slots,get_buffer_from_list,get_buf_clock_sweep
        FROM pg_catalog.local_bgwriter_stat();

CREATE VIEW DBE_PERF.global_pagewriter_status AS
        SELECT node_name,pgwr_actual_flush_total_num,pgwr_last_flush_num,remain_dirty_page_num,queue_head_page_rec_lsn,queue_rec_lsn,current_xlog_insert_lsn,ckpt_redo_point
        FROM pg_catalog.local_pagewriter_stat();

CREATE VIEW DBE_PERF.global_record_reset_time AS
  SELECT * FROM DBE_PERF.get_global_record_reset_time();

CREATE OR REPLACE VIEW DBE_PERF.global_redo_status AS
    SELECT node_name, redo_start_ptr, redo_start_time, redo_done_time, curr_time, 
           min_recovery_point, read_ptr, last_replayed_read_ptr, recovery_done_ptr, 
           read_xlog_io_counter, read_xlog_io_total_dur, read_data_io_counter, read_data_io_total_dur, 
           write_data_io_counter, write_data_io_total_dur, process_pending_counter, process_pending_total_dur,
           apply_counter, apply_total_dur,
           speed, local_max_ptr, primary_flush_ptr, worker_info
    FROM pg_catalog.local_redo_stat();
  
CREATE OR REPLACE VIEW DBE_PERF.global_rto_status AS
SELECT node_name, rto_info
FROM pg_catalog.local_rto_stat();

CREATE OR REPLACE VIEW DBE_PERF.global_recovery_status AS
SELECT node_name, standby_node_name, source_ip, source_port, dest_ip, dest_port, current_rto, target_rto, current_sleep_time
FROM pg_catalog.local_recovery_status();
  
CREATE VIEW DBE_PERF.local_threadpool_status AS
  SELECT * FROM threadpool_status();

CREATE OR REPLACE FUNCTION DBE_PERF.global_threadpool_status()
RETURNS SETOF DBE_PERF.local_threadpool_status
AS $$
DECLARE
  ROW_DATA DBE_PERF.local_threadpool_status%ROWTYPE;
  ROW_NAME RECORD;
  QUERY_STR TEXT;
  QUERY_STR_NODES TEXT;
BEGIN
  QUERY_STR_NODES := 'select * from DBE_PERF.node_name';
  FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
    QUERY_STR := 'SELECT * FROM DBE_PERF.local_threadpool_status';
    FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
      RETURN NEXT ROW_DATA;
    END LOOP;
  END LOOP;
  RETURN;
END; $$
LANGUAGE 'plpgsql';

CREATE VIEW DBE_PERF.global_threadpool_status AS
  SELECT * FROM DBE_PERF.global_threadpool_status();

grant select on all tables in schema dbe_perf to public;
