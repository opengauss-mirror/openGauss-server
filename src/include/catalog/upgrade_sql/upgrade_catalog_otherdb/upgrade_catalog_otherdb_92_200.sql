--074
-- ----------------------------------------------------------------
-- upgrade comm_check_connection_status
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.comm_check_connection_status(OUT node_name text, OUT remote_name text, OUT remote_host text, OUT remote_port int4, OUT is_connected bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1982;
CREATE FUNCTION pg_catalog.comm_check_connection_status(OUT node_name text, OUT remote_name text, OUT remote_host text, OUT remote_port int4, OUT is_connected bool) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'comm_check_connection_status';

--075
/* add shared table gs_global_config */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, false, 9080, 9081, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_global_config
(
   name NAME NOCOMPRESS NOT NULL,
   value TEXT NOCOMPRESS
)WITHOUT OIDS TABLESPACE pg_global;

GRANT SELECT ON pg_catalog.gs_global_config TO public;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

--add new catalog gs_policy_label and indexes
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9500, 9503, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_policy_label
(
    labelname Name NOCOMPRESS NOT NULL,
    labeltype Name NOCOMPRESS NOT NULL,
    fqdnnamespace Oid NOCOMPRESS NOT NULL,
    fqdnid Oid NOCOMPRESS NOT NULL,
    relcolumn Name NOCOMPRESS NOT NULL,
    fqdntype Name NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9501;

CREATE UNIQUE INDEX gs_policy_label_oid_index ON pg_catalog.gs_policy_label USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9502;

CREATE INDEX gs_policy_label_name_index ON pg_catalog.gs_policy_label USING BTREE(labelname NAME_OPS, fqdnnamespace OID_OPS, fqdnid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

--add new view gs_labels
create or replace view pg_catalog.gs_labels as
SELECT labelname
    ,labeltype
    ,fqdntype
    ,CASE fqdntype
        WHEN 'column' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'table' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'view' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'schema' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        WHEN 'function' THEN (select nspname from pg_namespace where oid = fqdnnamespace)
        ELSE ''
    END AS schemaname
    ,CASE fqdntype
        WHEN 'column' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'table' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'view' THEN (select relname from pg_class where oid = fqdnid)
        WHEN 'function' THEN (select proname from pg_proc where oid=fqdnid)
        WHEN 'label' THEN relcolumn
        ELSE ''
    END AS fqdnname
    ,CASE fqdntype
        WHEN 'column' THEN relcolumn
        ELSE ''
    END AS columnname
FROM gs_policy_label WHERE length(fqdntype)>0 ORDER BY labelname, labeltype ,fqdntype;

GRANT SELECT ON TABLE pg_catalog.gs_policy_label TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_labels TO PUBLIC;
DROP FUNCTION IF EXISTS prepare_statement_status() CASCADE;
DROP FUNCTION IF EXISTS plancache_status() CASCADE;
DROP FUNCTION IF EXISTS plancache_clean() CASCADE;

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3959;
CREATE OR REPLACE FUNCTION pg_catalog.prepare_statement_status(OUT nodename text, OUT global_sess_id int8, OUT statement_name text, OUT refcount int4, OUT is_shared bool) RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_prepare_status';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3957;
CREATE OR REPLACE FUNCTION pg_catalog.plancache_status(OUT nodename text, OUT query text, OUT refcount int4, OUT valid bool, OUT DatabaseID oid, OUT schema_name text, OUT params_num int4) RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_status';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3958;
CREATE OR REPLACE FUNCTION pg_catalog.plancache_clean() RETURNS bool LANGUAGE INTERNAL AS 'GPCPlanClean';

DROP FUNCTION IF EXISTS pg_catalog.pgxc_lock_for_transfer() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9018;
CREATE FUNCTION pg_catalog.pgxc_lock_for_transfer(OUT pgxc_lock_for_transfer bool, IN schemaName Name)
RETURNS BOOL LANGUAGE INTERNAL strict volatile as 'pgxc_lock_for_transfer';

DROP FUNCTION IF EXISTS pg_catalog.pgxc_unlock_for_transfer() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9019;
CREATE FUNCTION pg_catalog.pgxc_unlock_for_transfer(OUT pgxc_unlock_for_transfer bool,IN schemaName Name )
RETURNS BOOL LANGUAGE INTERNAL strict volatile as 'pgxc_unlock_for_transfer';

DROP FUNCTION IF EXISTS pg_catalog.table_skewness(text, OUT DNName text, OUT Num text, OUT Ratio text);
DROP FUNCTION IF EXISTS pg_catalog.distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text);

-- upgrade 3956
DROP FUNCTION IF EXISTS pg_catalog.threadpool_status;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,3956;
CREATE OR REPLACE FUNCTION pg_catalog.threadpool_status(
    OUT node_name TEXT,
    OUT group_id INT,
    OUT bind_numa_id INT,
    OUT bind_cpu_number INT,
    OUT listener INT,
    OUT worker_info TEXT,
    OUT session_info TEXT,
    OUT stream_info TEXT)
RETURNS RECORD LANGUAGE INTERNAL as 'gs_threadpool_status';

-- backup for views pg_roles, pg_user, pg_shadow and pg_rlspolicies
CREATE OR REPLACE VIEW pg_catalog.pg_roles_v1 AS
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
        rolspillspace
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid);

CREATE OR REPLACE VIEW pg_catalog.pg_user_v1 AS
    SELECT
        rolname AS usename,
        pg_authid.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        '********'::text AS passwd,
        rolvalidbegin AS valbegin,
        rolvaliduntil AS valuntil,
        rolrespool AS respool,
        rolparentid AS parent,
        roltabspace AS spacelimit,
        setconfig AS useconfig,
        pgxc_group.group_name AS nodegroup,
        roltempspace AS tempspacelimit,
        rolspillspace AS spillspacelimit
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE rolcanlogin;

CREATE OR REPLACE VIEW pg_catalog.pg_shadow_v1 AS
    SELECT
        rolname AS usename,
        pg_authid.oid AS usesysid,
        rolcreatedb AS usecreatedb,
        rolsuper AS usesuper,
        rolcatupdate AS usecatupd,
        rolreplication AS userepl,
        rolpassword AS passwd,
        rolvalidbegin AS valbegin,
        rolvaliduntil AS valuntil,
        rolrespool AS respool,
        rolparentid AS parent,
        roltabspace AS spacelimit,
        setconfig AS useconfig,
        roltempspace AS tempspacelimit,
        rolspillspace AS spillspacelimit
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    WHERE rolcanlogin;

CREATE OR REPLACE VIEW pg_catalog.pg_rlspolicies_v1 AS
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
                    FROM pg_catalog.pg_roles_v1
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

CREATE OR REPLACE FUNCTION pg_catalog.delta_internal(s anyarray, v anyelement)
RETURNS anyarray AS $$
DECLARE
  is_numeric bool;
BEGIN
  is_numeric := v ~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$';
  IF NOT is_numeric THEN
    RAISE EXCEPTION 'delta function only accept numeric input';
  END IF;
  RETURN  array_append(s,v);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_catalog.delta_final(s anyarray)
RETURNS anyelement AS $$
DECLARE
 ary_cnt int;
BEGIN
 ary_cnt := array_length(s,1);
 RETURN  s[ary_cnt] - s[1];
END;
$$ LANGUAGE plpgsql;

drop aggregate if exists pg_catalog.delta(anyelement);
create aggregate pg_catalog.delta(anyelement) (
  SFUNC=delta_internal,
  STYPE=anyarray,
  finalfunc = delta_final,
  initcond = '{}'
);

CREATE OR REPLACE FUNCTION pg_catalog.spread_internal(s real[], v real)
RETURNS real[] AS $$
DECLARE
  is_numeric bool;
BEGIN
  IF s[1] is NULL OR s[2] is NULL THEN
    s[1] = v;
    s[2] = v;
  END IF;
  IF s[1] > v THEN
    s[1] = v;
  ELSEIF s[2] < v THEN
    s[2] = v;
  END IF;
  RETURN  s;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION pg_catalog.spread_final(s real[])
RETURNS real AS $$
BEGIN
 RETURN  s[2] - s[1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION pg_catalog.spread_collect(s real[], v real[])
RETURNS real[] AS $$
DECLARE
 new_array real[];
BEGIN
 IF v[1] is NULL OR (s[1] is NOT NULL AND s[1] < v[1]) THEN
  new_array[1] = s[1];
 ELSE
  new_array[1] = v[1];
 END IF;
 IF v[2] is NULL OR (s[2] is NOT NULL AND s[2] > v[2]) THEN
  new_array[2] = s[2];
 ELSE
  new_array[2] = v[2];
 END IF;
 RETURN  new_array;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

drop aggregate if exists pg_catalog.spread(real);
create aggregate pg_catalog.spread(real) (
  SFUNC=spread_internal,
  STYPE=real[],
  CFUNC =spread_collect,
  finalfunc = spread_final,
  initcond = '{}'
);

set local inplace_upgrade_next_system_object_oids = IUO_PROC,6006;
create or replace function pg_catalog.series(name) returns text language INTERNAL   NOT FENCED as 'series_internal';


CREATE OR REPLACE FUNCTION  pg_catalog.labels(
                  IN        relationname         name
)
RETURNS text[]
AS
$$
DECLARE
    pgclass_rec         pg_class%rowtype;
    sql                 text;
    attname             text;
    attnames            text[];
    check_count         int;
    username            name;
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

    /* check table exist */
    sql := 'select current_user';
    EXECUTE(sql) into username;
    IF pos = 0 THEN
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''
            AND pg_catalog.pg_table_is_visible(c.oid) ;';
    ELSE
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || table_name || '''
            AND n.nspname = ''' || namespace || ''';';
    END IF;
    EXECUTE sql INTO check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  'please input the correct relation name!';
    END IF;
    /* check table exist end */

    /* check table owner privilege */
    IF pos = 0 THEN
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''
            AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    ELSE
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || table_name || '''
            AND n.nspname = ''' || namespace || ''' and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    END IF;
    EXECUTE sql INTO check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION 'permission denied for relation %', relationname
        USING HINT = 'please assure you have the privilege';
    END IF;
    /* check table owner privilege end */
    sql := 'SELECT a.attname  FROM pg_catalog.pg_attribute a WHERE a.attrelid = (SELECT oid FROM PG_CLASS WHERE relname = ''' || relationname || '''
        ) AND a.attnum > 0 AND NOT a.attisdropped AND a.attkvtype = 1 ORDER BY a.attnum;';
    FOR attname IN EXECUTE(sql) LOOP
      IF attnames IS NULL THEN
        attnames = ARRAY[attname];
      ELSE
        attnames = array_append(attnames,attname);
      END IF;
    END LOOP;

    RETURN attnames;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION  pg_catalog.labels_count(
                  IN        relationname         name
)
RETURNS int
AS
$$
DECLARE
    pgclass_rec         pg_class%rowtype;
    sql                 text;
    attname             text;
    count               int;
    check_count         int;
    username            name;
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

    /* check table exist */
    sql := 'select current_user';
    EXECUTE(sql) into username;
    IF pos = 0 THEN
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''
            AND pg_catalog.pg_table_is_visible(c.oid) ;';
    ELSE
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || table_name || '''
            AND n.nspname = ''' || namespace || ''';';
    END IF;
    EXECUTE sql INTO check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION  'please input the correct relation name!';
    END IF;
    /* check table exist end */

    /* check table owner privilege */
    IF pos = 0 THEN
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''
            AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    ELSE
        sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || table_name || '''
            AND n.nspname = ''' || namespace || ''' and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    END IF;
    EXECUTE sql INTO check_count;
    IF check_count = 0 THEN
        RAISE EXCEPTION 'permission denied for relation %', relationname
        USING HINT = 'please assure you have the privilege';
    END IF;
    /* check table owner privilege end */
    sql := 'SELECT a.attname  FROM pg_catalog.pg_attribute a WHERE a.attrelid = (SELECT oid FROM PG_CLASS WHERE relname = ''' || relationname || '''
        ) AND a.attnum > 0 AND NOT a.attisdropped AND a.attkvtype = 1 ORDER BY a.attnum;';
    count = 0;
    FOR attname IN EXECUTE(sql) LOOP
      IF attname IS NOT NULL THEN
        count = count + 1;
      END IF;
    END LOOP;

    RETURN count;
END;
$$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_activity_timeout(IN timeout_threshold int4, OUT datid oid, OUT pid INT8, OUT sessionid INT8, OUT usesysid oid, OUT application_name text, OUT query text, OUT xact_start timestamptz, OUT query_start timestamptz, OUT query_id INT8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4520;
CREATE FUNCTION pg_catalog.gs_stat_activity_timeout(IN timeout_threshold int4, OUT datid oid, OUT pid INT8, OUT sessionid INT8, OUT usesysid oid, OUT application_name text, OUT query text, OUT xact_start timestamptz, OUT query_start timestamptz, OUT query_id INT8)
RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity_timeout';

DROP VIEW IF EXISTS pg_catalog.pgxc_wlm_session_history CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_history() CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_history CASCADE;

DROP VIEW IF EXISTS pg_catalog.pgxc_wlm_session_info CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_info() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_info_bytime(text, TIMESTAMP, TIMESTAMP, int) CASCADE;

create table pg_catalog.gs_wlm_session_query_info_all
(
    datid               Oid,
    dbname              text,
    schemaname          text,
    nodename            text,
    username            text,
    application_name    text,
    client_addr         inet,
    client_hostname     text,
    client_port         int,
    query_band          text,
    block_time          bigint,
    start_time          timestamp with time zone,
    finish_time         timestamp with time zone,
    duration            bigint,
    estimate_total_time bigint,
    status              text,
    abort_info          text,
    resource_pool       text,
    control_group       text,
    estimate_memory     int,
    min_peak_memory     int,
    max_peak_memory     int,
    average_peak_memory int,
    memory_skew_percent int,
    spill_info          text,
    min_spill_size      int,
    max_spill_size      int,
    average_spill_size  int,
    spill_skew_percent  int,
    min_dn_time         bigint,
    max_dn_time         bigint,
    average_dn_time     bigint,
    dntime_skew_percent int,
    min_cpu_time        bigint,
    max_cpu_time        bigint,
    total_cpu_time      bigint,
    cpu_skew_percent    int,
    min_peak_iops       int,
    max_peak_iops       int,
    average_peak_iops   int,
    iops_skew_percent   int,
    warning             text,
    queryid             bigint NOT NULL,
    query               text,
    query_plan          text,
    node_group      text,
    cpu_top1_node_name text,
    cpu_top2_node_name text,
    cpu_top3_node_name text,
    cpu_top4_node_name text,
    cpu_top5_node_name text,
    mem_top1_node_name text,
    mem_top2_node_name text,
    mem_top3_node_name text,
    mem_top4_node_name text,
    mem_top5_node_name text,
    cpu_top1_value bigint,
    cpu_top2_value bigint,
    cpu_top3_value bigint,
    cpu_top4_value bigint,
    cpu_top5_value bigint,
    mem_top1_value bigint,
    mem_top2_value bigint,
    mem_top3_value bigint,
    mem_top4_value bigint,
    mem_top5_value bigint,
    top_mem_dn text,
    top_cpu_dn text,
    n_returned_rows      bigint,
    n_tuples_fetched     bigint,
    n_tuples_returned    bigint,
    n_tuples_inserted    bigint,
    n_tuples_updated     bigint,
    n_tuples_deleted     bigint,
    n_blocks_fetched     bigint,
    n_blocks_hit         bigint,
    db_time              bigint,
    cpu_time             bigint,
    execution_time       bigint,
    parse_time           bigint,
    plan_time            bigint,
    rewrite_time         bigint,
    pl_execution_time    bigint,
    pl_compilation_time  bigint,
    net_send_time        bigint,
    data_io_time         bigint,
    is_slow_query        bigint
);

insert into pg_catalog.gs_wlm_session_query_info_all select * from pg_catalog.gs_wlm_session_info;

DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info_all CASCADE;
DROP TABLE IF EXISTS pg_catalog.gs_wlm_session_info CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.create_wlm_session_info(IN flag int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wlm_session_info(OID) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5002;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_wlm_session_info
(OID,
 OUT datid oid,
 OUT dbname text,
 OUT schemaname text,
 OUT nodename text,
 OUT username text,
 OUT application_name text,
 OUT client_addr inet,
 OUT client_hostname text,
 OUT client_port integer,
 OUT query_band text,
 OUT block_time bigint,
 OUT start_time timestamp with time zone,
 OUT finish_time timestamp with time zone,
 OUT duration bigint,
 OUT estimate_total_time bigint,
 OUT status text,
 OUT abort_info text,
 OUT resource_pool text,
 OUT control_group text,
 OUT estimate_memory integer,
 OUT min_peak_memory integer,
 OUT max_peak_memory integer,
 OUT average_peak_memory integer,
 OUT memory_skew_percent integer,
 OUT spill_info text,
 OUT min_spill_size integer,
 OUT max_spill_size integer,
 OUT average_spill_size integer,
 OUT spill_skew_percent integer,
 OUT min_dn_time bigint,
 OUT max_dn_time bigint,
 OUT average_dn_time bigint,
 OUT dntime_skew_percent integer,
 OUT min_cpu_time bigint,
 OUT max_cpu_time bigint,
 OUT total_cpu_time bigint,
 OUT cpu_skew_percent integer,
 OUT min_peak_iops integer,
 OUT max_peak_iops integer,
 OUT average_peak_iops integer,
 OUT iops_skew_percent integer,
 OUT warning text,
 OUT queryid bigint,
 OUT query text,
 OUT query_plan text,
 OUT node_group text,
 OUT cpu_top1_node_name text,
 OUT cpu_top2_node_name text,
 OUT cpu_top3_node_name text,
 OUT cpu_top4_node_name text,
 OUT cpu_top5_node_name text,
 OUT mem_top1_node_name text,
 OUT mem_top2_node_name text,
 OUT mem_top3_node_name text,
 OUT mem_top4_node_name text,
 OUT mem_top5_node_name text,
 OUT cpu_top1_value bigint,
 OUT cpu_top2_value bigint,
 OUT cpu_top3_value bigint,
 OUT cpu_top4_value bigint,
 OUT cpu_top5_value bigint,
 OUT mem_top1_value bigint,
 OUT mem_top2_value bigint,
 OUT mem_top3_value bigint,
 OUT mem_top4_value bigint,
 OUT mem_top5_value bigint,
 OUT top_mem_dn text,
 OUT top_cpu_dn text,
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
 OUT net_send_time bigint,
 OUT data_io_time bigint,
 OUT is_slow_query bigint) RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_wlm_session_info';

CREATE VIEW pg_catalog.gs_wlm_session_info_all AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(0);

CREATE VIEW pg_catalog.gs_wlm_session_info AS
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
FROM gs_wlm_session_query_info_all S;

CREATE VIEW pg_catalog.gs_wlm_session_history AS
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
FROM pg_catalog.gs_wlm_session_info_all S;

CREATE OR REPLACE FUNCTION pg_catalog.create_wlm_session_info(IN flag int)
RETURNS int
AS $$
DECLARE
	query_str text;
	record_cnt int;
	BEGIN
		record_cnt := 0;

		query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(1)';

		IF flag > 0 THEN
			EXECUTE 'INSERT INTO pg_catalog.gs_wlm_session_query_info_all ' || query_str;
		ELSE
			EXECUTE query_str;
		END IF;

		RETURN record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;

GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_query_info_all TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_history TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info_all TO PUBLIC;

DROP INDEX IF EXISTS pg_catalog.pg_asp_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_asp;
DROP TABLE IF EXISTS pg_catalog.pg_asp;

DROP FUNCTION IF EXISTS pg_catalog.get_wait_event_info() cascade;
--wait_event_info
set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5723;
CREATE OR REPLACE FUNCTION pg_catalog.get_wait_event_info
  (OUT module text,
   OUT type text,
   OUT event text)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_wait_event_info';


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

-- pg_asp table
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
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, false, 0, 0, 0, 0;


-- 1. create system relations:
--    gs_auditing_policy
--    gs_auditing_policy_access
--    gs_auditing_policy_filters
--    gs_auditing_policy_privileges and their indexes.
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_name_index;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_oid_index;
--DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy;
--DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_access_oid_index;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_access_row_index;
--DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_access;
--DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_access;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_filters_oid_index;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_filters_row_index;
--DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_filters;
--DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_filters;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_privileges_oid_index;
--DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_privileges_row_index;
--DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_privileges;
--DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_privileges;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9510, 9513, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_auditing_policy
(
    polname name nocompress not null,
    polcomments name nocompress not null,
    modifydate timestamp nocompress not null,
    polenabled "bool" nocompress not null
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9511;
CREATE UNIQUE INDEX gs_auditing_policy_oid_index ON pg_catalog.gs_auditing_policy USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9512;
CREATE UNIQUE INDEX gs_auditing_policy_name_index ON pg_catalog.gs_auditing_policy USING BTREE(polname name_ops);

-- access
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9520, 9523, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_auditing_policy_access
(
    accesstype name nocompress not null,
    labelname name nocompress not null,
    policyoid oid nocompress not null,
    modifydate timestamp nocompress not null
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9521;
CREATE UNIQUE INDEX gs_auditing_policy_access_oid_index ON pg_catalog.gs_auditing_policy_access USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9522;
CREATE UNIQUE INDEX gs_auditing_policy_access_row_index ON pg_catalog.gs_auditing_policy_access USING BTREE(accesstype name_ops, labelname name_ops, policyoid oid_ops);

-- filters
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9540, 9543, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_auditing_policy_filters
(
    filtertype name nocompress not null,
    labelname name nocompress not null,
    policyoid oid nocompress not null,
    modifydate timestamp nocompress not null,
    logicaloperator text nocompress
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9541;
CREATE UNIQUE INDEX gs_auditing_policy_filters_oid_index ON pg_catalog.gs_auditing_policy_filters USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9542;
CREATE UNIQUE INDEX gs_auditing_policy_filters_row_index ON pg_catalog.gs_auditing_policy_filters USING BTREE(policyoid oid_ops);

-- privileges
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9530, 9533, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_auditing_policy_privileges
(
    privilegetype name nocompress not null,
    labelname name nocompress not null,
    policyoid oid nocompress not null,
    modifydate timestamp nocompress not null
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9531;
CREATE UNIQUE INDEX gs_auditing_policy_privileges_oid_index ON pg_catalog.gs_auditing_policy_privileges USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9532;
CREATE UNIQUE INDEX gs_auditing_policy_privileges_row_index ON pg_catalog.gs_auditing_policy_privileges USING BTREE(privilegetype name_ops, labelname name_ops, policyoid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- 2. add system views:
--    gs_auditing_access
--    gs_auditing_privilege
--    gs_auditing
--DROP VIEW IF EXISTS pg_catalog.gs_auditing_access CASCADE;
--DROP VIEW IF EXISTS pg_catalog.gs_auditing_privilege CASCADE;
--DROP VIEW IF EXISTS pg_catalog.gs_auditing CASCADE;

create view pg_catalog.gs_auditing_access as
    select distinct
        p.polname,
        'access' as pol_type,
        p.polenabled,
        a.accesstype as access_type,
        a.labelname as label_name,
        --CONCAT(l.fqdntype, ':', l.columnname) as access_object,
        CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS access_object,
        (select
            logicaloperator
            from gs_auditing_policy_filters
            where p.Oid=policyoid) as filter_name
    from gs_auditing_policy p
        left join gs_auditing_policy_access a ON (a.policyoid=p.Oid)
        left join gs_labels l ON (a.labelname=l.labelname)
    where length(a.accesstype) > 0 order by 1,3;

create view pg_catalog.gs_auditing_privilege as
    select distinct
        p.polname,
        'privilege' as pol_type,
        p.polenabled,
        priv.privilegetype as access_type,
        priv.labelname as label_name,
        --CONCAT(l.fqdntype, ':', l.columnname) as priv_object,
        CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS priv_object,
        (select
            logicaloperator
            from gs_auditing_policy_filters
            where p.Oid=policyoid) as filter_name
        from gs_auditing_policy p
            left join gs_auditing_policy_privileges priv ON (priv.policyoid=p.Oid)
            left join gs_labels l ON (priv.labelname=l.labelname)
        where length(priv.privilegetype) > 0 order by 1,3;

create view pg_catalog.gs_auditing as
    select * from gs_auditing_privilege
    union all
    select * from gs_auditing_access order by polname;

GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_access TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_filters TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_privileges TO PUBLIC;

GRANT SELECT ON TABLE pg_catalog.gs_auditing_access TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_auditing_privilege TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_auditing TO PUBLIC;
DROP FUNCTION IF EXISTS pg_catalog.capture_view_to_json(text, integer) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5719;
CREATE FUNCTION pg_catalog.capture_view_to_json(view_name text, is_all_db integer)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'capture_view_to_json';

-- masking policy
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9610, 9613, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_masking_policy
(
    polname Name NOCOMPRESS NOT NULL,
    polcomments Name NOCOMPRESS NOT NULL,
    modifydate timestamp NOCOMPRESS NOT NULL,
    polenabled "bool" NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9611;
CREATE UNIQUE INDEX gs_masking_policy_oid_index ON pg_catalog.gs_masking_policy USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9612;
CREATE UNIQUE INDEX gs_masking_policy_name_index ON pg_catalog.gs_masking_policy USING BTREE(polname NAME_OPS);

-- masking policy action
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9650, 9654, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_masking_policy_actions
(
    actiontype Name NOCOMPRESS NOT NULL,
    actparams Name NOCOMPRESS NOT NULL,
    actlabelname Name NOCOMPRESS NOT NULL,
    policyoid oid NOCOMPRESS NOT NULL,
    actmodifydate timestamp NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9651;
CREATE UNIQUE INDEX gs_masking_policy_actions_oid_index ON pg_catalog.gs_masking_policy_actions USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9652;
CREATE UNIQUE INDEX gs_masking_policy_actions_row_index ON pg_catalog.gs_masking_policy_actions USING BTREE(actiontype NAME_OPS, actlabelname NAME_OPS, policyoid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9653;
CREATE INDEX gs_masking_policy_actions_policy_oid_index ON pg_catalog.gs_masking_policy_actions USING BTREE(policyoid OID_OPS);

-- masking policy filters
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9640, 9643, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_masking_policy_filters
(
    filtertype Name NOCOMPRESS NOT NULL,
    filterlabelname Name NOCOMPRESS NOT NULL,
    policyoid oid NOCOMPRESS NOT NULL,
    modifydate timestamp NOCOMPRESS NOT NULL,
    logicaloperator text NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9641;
CREATE UNIQUE INDEX gs_masking_policy_filters_oid_index ON pg_catalog.gs_masking_policy_filters USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9642;
CREATE UNIQUE INDEX gs_masking_policy_filters_row_index ON pg_catalog.gs_masking_policy_filters USING BTREE(policyoid OID_OPS);

-- reset as usual
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.gs_masking_policy TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_masking_policy_actions TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_masking_policy_filters TO PUBLIC;

--add new view gs_labels
create view pg_catalog.gs_masking as
select distinct p.polname,
p.polenabled,
a.actiontype as maskaction,
a.actlabelname as labelname,
CASE l.fqdntype
            WHEN 'column'   THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname || '.' || l.columnname
            WHEN 'table'    THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'view'     THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'schema'   THEN l.fqdntype || ':' || l.schemaname
            WHEN 'function' THEN l.fqdntype || ':' || l.schemaname || '.' || l.fqdnname
            WHEN 'label'    THEN l.fqdntype || ':' || l.columnname
            ELSE l.fqdntype || ':' || ''
        END AS masking_object,
(select
    logicaloperator
    from gs_masking_policy_filters
    where p.Oid=policyoid) as filter_name
from gs_masking_policy p join gs_masking_policy_actions a ON (p.Oid=a.policyoid ) join gs_labels l ON (a.actlabelname=l.labelname) WHERE l.fqdntype='column' or l.fqdntype='table' order by polname;

GRANT SELECT ON TABLE pg_catalog.gs_masking TO PUBLIC;


-- 1. create system relation streaming_stream and its indexes.
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9028, 7200, 0, 0;
CREATE TABLE pg_catalog.streaming_stream
(
    relid Oid NOCOMPRESS NOT NULL,
    queries bytea NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3228;
CREATE UNIQUE INDEX streaming_stream_oid_index ON pg_catalog.streaming_stream USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3229;
CREATE UNIQUE INDEX streaming_stream_relid_index ON pg_catalog.streaming_stream USING BTREE(relid oid_ops);

-- 2. create system relation streaming_cont_query and its indexes.
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9029, 7201, 0, 0;
CREATE TABLE pg_catalog.streaming_cont_query
(
    id int4 NOCOMPRESS NOT NULL,
    type "char" NOCOMPRESS NOT NULL,
    relid Oid NOCOMPRESS NOT NULL,
    defrelid Oid NOCOMPRESS NOT NULL,
    active bool NOCOMPRESS NOT NULL,
    streamrelid Oid NOCOMPRESS NOT NULL,
    matrelid Oid NOCOMPRESS NOT NULL,
    lookupidxid Oid NOCOMPRESS NOT NULL,
    step_factor int2 NOCOMPRESS NOT NULL,
    ttl int4 NOCOMPRESS NOT NULL,
    ttl_attno int2 NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3230;
CREATE UNIQUE INDEX streaming_cont_query_relid_index ON pg_catalog.streaming_cont_query USING BTREE(relid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3231;
CREATE UNIQUE INDEX streaming_cont_query_defrelid_index ON pg_catalog.streaming_cont_query USING BTREE(defrelid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3232;
CREATE UNIQUE INDEX streaming_cont_query_id_index ON pg_catalog.streaming_cont_query USING BTREE(id int4_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3233;
CREATE UNIQUE INDEX streaming_cont_query_oid_index ON pg_catalog.streaming_cont_query USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3234;
CREATE UNIQUE INDEX streaming_cont_query_matrelid_index ON pg_catalog.streaming_cont_query USING BTREE(matrelid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3235;
CREATE UNIQUE INDEX streaming_cont_query_lookupidxid_index ON pg_catalog.streaming_cont_query USING BTREE(lookupidxid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.streaming_stream TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.streaming_cont_query TO PUBLIC;

CREATE OR REPLACE FUNCTION  pg_catalog.proc_add_depend(
                  IN        relationname         name,
                  IN        dbname               name
)
RETURNS void
AS
$$
DECLARE
    sql                 text;
    job_id              int;
    relation_oid        int;
    job_num             int;
    username            name;
BEGIN
    sql := 'select current_user';
    EXECUTE(sql) into username;

    sql := 'SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''  AND
                pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    EXECUTE sql INTO relation_oid;

    sql := 'SELECT COUNT(a.job_id) from pg_job a , pg_job_proc b where b.what like ''%proc_add_partition%('''''|| relationname ||'''''%''
                  and a.job_id = b.job_id and a.priv_user='''|| username ||''' and a.dbname ='''|| dbname ||'''  ; ';
    EXECUTE sql INTO job_num;
    IF job_num = 1 THEN
        sql := 'SELECT a.job_id from pg_job a , pg_job_proc b where b.what like ''%proc_add_partition%('''''|| relationname ||'''''%''
                      and a.job_id = b.job_id and a.priv_user='''|| username ||''' and a.dbname ='''|| dbname ||'''  ; ';
        EXECUTE sql INTO job_id;
        sql := 'SELECT pg_catalog.add_job_class_depend(' || job_id || ',' || relation_oid || ')';
        EXECUTE sql;
    END IF;

    sql := 'SELECT COUNT(a.job_id) from pg_job a , pg_job_proc b where b.what like ''%proc_drop_partition%('''''|| relationname ||'''''%''
                  and a.job_id = b.job_id and a.priv_user='''|| username ||''' and a.dbname ='''|| dbname ||'''  ; ';
    EXECUTE sql INTO job_num;
    IF job_num = 1 THEN
        sql := 'SELECT a.job_id from pg_job a , pg_job_proc b where b.what like ''%proc_drop_partition%('''''|| relationname ||'''''%''
                      and a.job_id = b.job_id and a.priv_user='''|| username ||''' and a.dbname ='''|| dbname ||'''  ; ';
        EXECUTE sql INTO job_id;
        sql := 'SELECT pg_catalog.add_job_class_depend(' || job_id || ',' || relation_oid || ')';
        EXECUTE sql;
    END IF;

    RETURN ;
END;
$$
LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS pg_catalog.pg_lock_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1371;
CREATE FUNCTION pg_catalog.pg_lock_status
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

DROP VIEW IF EXISTS pg_catalog.pg_locks CASCADE;
CREATE OR REPLACE VIEW pg_catalog.pg_locks AS
  SELECT DISTINCT * from pg_lock_status();

GRANT SELECT ON pg_catalog.pg_locks TO public;

DROP VIEW IF EXISTS pg_catalog.all_all_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_col_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_cons_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_constraints CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_ind_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_dependencies CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_ind_expressions CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_procedures CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_sequences CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_source CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_tab_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_tab_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_users CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_views CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_col_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_cons_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_constraints CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_data_files CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_ind_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_ind_expressions CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_ind_partitions CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_part_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_part_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_procedures CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_sequences CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_source CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_tab_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_tab_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_tablespaces CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_tab_partitions CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_triggers CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_users CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_views CASCADE;
DROP VIEW IF EXISTS pg_catalog.dual CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_col_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_cons_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_constraints CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_ind_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_ind_expressions CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_ind_partitions CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_jobs CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_part_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_part_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_procedures CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_sequences CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_source CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_tab_columns CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_tab_comments CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_tab_partitions CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_triggers CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_views CASCADE;

drop view if exists pg_catalog.v$session CASCADE;
drop view if exists pg_catalog.V$SESSION_LONGOPS CASCADE;

DROP SCHEMA IF EXISTS dbms_job cascade;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_user_session_info AS
SELECT * FROM gs_wlm_session_info
WHERE username = CURRENT_USER::text;

--some function will use the new column that use
CREATE OR REPLACE VIEW PG_CATALOG.SYS_DUMMY AS (SELECT 'X'::TEXT AS DUMMY);
GRANT SELECT ON TABLE SYS_DUMMY TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.ADM_PART_TABLES AS

	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64)) AS TABLE_NAME ,
	----------------------------------------------------------------------
		CASE
			when p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE,
	----------------------------------------------------------------------

		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = c.oid AND ps.parttype = 'p') AS PARTITION_COUNT,

	----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT,
	----------------------------------------------------------------------
		CASE
			WHEN c.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::name
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE c.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA
	FROM
		PG_CLASS c , PG_PARTITION p , pg_authid a , pg_namespace n
	WHERE
		c.parttype = 'p'  AND  -- it is a partitioned table in pg_class
		c.relkind = 'r'  AND  	-- normal table ,it can be ignore, all partitioned table is  normal table
		c.oid = p.parentid	AND  -- join condition
		p.parttype = 'r'   	AND		-- it is a partitioned table in pg_partition
		c.relowner = a.oid	AND		-- the owner of table
		c.relnamespace = n.oid		-- namespace
	ORDER BY TABLE_OWNER,SCHEMA,PARTITIONING_TYPE, PARTITION_COUNT,DEF_TABLESPACE_NAME , PARTITIONING_KEY_COUNT;

CREATE OR REPLACE VIEW pg_catalog.MY_PART_TABLES AS
	SELECT
		*
	FROM ADM_PART_TABLES
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.ADM_TAB_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64))  AS TABLE_NAME,
	----------------------------------------------------------------------
		CAST(p.relname AS varchar2(64)) AS PARTITION_NAME,
	----------------------------------------------------------------------
		array_to_string(p.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
	----------------------------------------------------------------------
		CASE
			WHEN p.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::name
			ELSE (SELECT spc.spcname FROM pg_tablespace spc WHERE p.reltablespace = spc.oid)
		END
		AS TABLESPACE_NAME,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA
	----------------------------------------------------------------------
	FROM
		pg_class c , pg_partition p , pg_authid a,  pg_namespace n
	----------------------------------------------------------------------
	WHERE
		p.parentid = c.oid AND
	----------------------------------------------------------------------
		p.parttype = 'p' AND
		c.relowner = a.oid AND
		c.relnamespace = n.oid

	----------------------------------------------------------------------
	ORDER BY TABLE_OWNER,SCHEMA,TABLE_NAME, PARTITION_NAME, HIGH_VALUE, TABLESPACE_NAME;

CREATE OR REPLACE VIEW pg_catalog.MY_TAB_PARTITIONS AS
	SELECT
		*
	FROM ADM_TAB_PARTITIONS
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.ADM_PART_INDEXES AS
	SELECT
		----------------------------------------------------------------------
		CASE
			WHEN ci.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::name
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE ci.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
		----------------------------------------------------------------------
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME,
		----------------------------------------------------------------------
		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = ct.oid AND ps.parttype = 'p')
		AS PARTITION_COUNT,
		----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT,
		----------------------------------------------------------------------
		CASE
			WHEN p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ct.relname AS varchar2(64)) AS TABLE_NAME
	FROM
		pg_class ct , --table
		pg_class ci,  --index
		pg_index i,	  -- i.INDEXRELID is index     i.INDRELID is table
		pg_partition p,  -- partition attribute of table
		pg_authid a,
		pg_namespace n
	WHERE
		ci.parttype = 'p' AND
		ci.relkind = 'i'	AND   --find all the local partitioned index

		ci.oid = i.INDEXRELID 	AND --find table be indexed
		ct.oid = i.INDRELID		AND

		ct.oid = p.parentid		AND -- find the attribute of partitioned table
		p.parttype = 'r' 		AND

		ci.relowner = a.oid		AND
		ci.relnamespace = n.oid
	ORDER BY TABLE_NAME ,SCHEMA, INDEX_NAME, DEF_TABLESPACE_NAME,INDEX_OWNER;

CREATE OR REPLACE VIEW pg_catalog.MY_PART_INDEXES AS
	SELECT
		*
	FROM ADM_PART_INDEXES
	WHERE INDEX_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.ADM_IND_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME,
		----------------------------------------------------------------------
		CAST(pi.relname AS varchar2(64)) AS PARTITION_NAME,
		----------------------------------------------------------------------
		CASE
			WHEN pi.reltablespace = 0 THEN 'DEFAULT TABLESPACE'::name
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE pi.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
		----------------------------------------------------------------------
		array_to_string(pt.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
		----------------------------------------------------------------------
		pi.indisusable AS INDEX_PARTITION_USABLE,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA
		----------------------------------------------------------------------
	FROM
		pg_class ci , 	--the local partitioned index
		pg_partition pi,-- the partition index
		pg_partition pt,-- the partition indexed by partition index
		pg_authid a,
		pg_namespace n

	WHERE
		ci.parttype  = 'p'  AND -- the partitioned relation
		ci.relkind   = 'i'  AND -- choose index of partitioned relation
		ci.oid = pi.parentid 	AND --the index partitons
		pi.indextblid = pt.oid  AND-- partition indexed by partition index
		ci.relowner = a.oid	AND
		ci.relnamespace = n.oid
	ORDER BY INDEX_OWNER,SCHEMA,INDEX_NAME, PARTITION_NAME, HIGH_VALUE, DEF_TABLESPACE_NAME;

CREATE OR REPLACE VIEW pg_catalog.MY_IND_PARTITIONS AS
	SELECT
		*
	FROM ADM_IND_PARTITIONS
	WHERE INDEX_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.MY_JOBS AS
	SELECT
			j.job_id AS job,
			j.log_user AS log_user,
			j.priv_user AS priv_user,
			j.dbname AS dbname,
			j.start_date AS start_date,
			substr(to_char(j.start_date,'HH24:MI:SS'),1,8) AS start_suc,
			j.last_suc_date AS last_date,
			substr(to_char(j.last_suc_date,'HH24:MI:SS'),1,8) AS last_suc,
			j.this_run_date AS this_date,
			substr(to_char(j.this_run_date,'HH24:MI:SS'),1,8) AS this_suc,
			j.next_run_date AS next_date,
			substr(to_char(j.next_run_date,'HH24:MI:SS'),1,8) AS next_suc,
			CASE
				WHEN j.job_status = 's' OR j.job_status = 'r' OR j.job_status = 'f' THEN 'n'
				WHEN j.job_status = 'd' THEN 'y'
				ELSE '?'
			END AS broken,
			j.job_status AS status,
			j.interval AS interval,
			j.failure_count AS failures,
			p.what AS what
	FROM pg_authid a JOIN pg_job j ON (a.rolname = j.log_user)
	LEFT JOIN pg_job_proc p ON (j.job_id = p.job_id)
	WHERE a.rolname = current_user;

GRANT SELECT ON MY_JOBS TO public;

/* adapt DV_SESSIONS: display session information for each current session  */
CREATE OR REPLACE VIEW pg_catalog.DV_SESSIONS AS
	SELECT
		sa.sessionid AS SID,
		0::integer AS SERIAL#,
		sa.usesysid AS USER#,
		ad.rolname AS USERNAME
	FROM pg_stat_get_activity(NULL) AS sa
	LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
	WHERE sa.application_name <> 'JobScheduler';


/* adapt DV_SESSION_LONGOPS: displays the status of various operations */
CREATE OR REPLACE VIEW pg_catalog.DV_SESSION_LONGOPS AS
    SELECT
        sa.pid AS SID,
        0::integer AS SERIAL#,
        NULL::integer AS SOFAR,
        NULL::integer AS TOTALWORK
    FROM pg_stat_activity sa
    WHERE sa.application_name <> 'JobScheduler';

REVOKE ALL on PG_CATALOG.DV_SESSIONS FROM public;
REVOKE ALL on PG_CATALOG.DV_SESSION_LONGOPS FROM public;


CREATE OR REPLACE FUNCTION  pg_catalog.proc_add_partition(
                  IN        relationname         name,
                  IN        boundaries_interval  INTERVAL
)
RETURNS void
AS
$$
DECLARE
    sql                 text;
    part_relname        text;
    check_count         int;
    rel_oid             int;
    time_interval       int;
    part_boundary       timestamptz;
    partition_tmp       timestamptz;
    database            name;
    now_timestamp       timestamptz;
    namespace           name;
    table_name          name;
    pos                 integer;
BEGIN
    /* check parameter */
    IF relationname is null THEN
        RAISE EXCEPTION 'parameter ''relationname'' cannot be null';
    END IF;
    IF boundaries_interval is null THEN
        RAISE EXCEPTION 'parameter ''boundaries_interval'' cannot be null';
    END IF;
    IF boundaries_interval < interval '0' THEN
        RAISE EXCEPTION 'boundaries_interval must be greater than 0';
    END IF;
    pos = is_contain_namespace(relationname);
    IF pos = 0 THEN
        RAISE EXCEPTION 'parameter ''relationname'' must be format namespace.relname';
    END IF;
    namespace = substring(relationname from 1 for pos - 1);
    table_name = substring(relationname from pos + 1 for char_length(relationname) - pos);
    /* check parameter end */
    sql = 'select current_database();';
    EXECUTE sql INTO database;

    /* check table exist */
    sql := 'SELECT count(*) FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
            WHERE c.relname = $1 AND n.nspname = $2 and c.relkind = ''r'' and c.parttype = ''p'';';
    EXECUTE sql INTO check_count USING table_name, namespace;
    IF check_count = 0 THEN
        RAISE EXCEPTION  'please input the correct relation name, relation kind should be r and be a partition table!';
    END IF;
    /* check table exist end */

    sql := 'SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = $2;';
    EXECUTE sql into rel_oid USING table_name, namespace;
    /* check partition table end */

    /* iteratively checking time range for every partition*/
    sql := 'SELECT boundaries[1] FROM pg_partition WHERE parentid = ' || rel_oid ||' AND boundaries IS NOT NULL ORDER BY
            EXTRACT(epoch FROM CAST(boundaries[1] as timestamptz)) DESC LIMIT 1';
    EXECUTE sql INTO part_boundary;

    /* reinforce the job failed to throw error when inserting data */
    sql := 'select current_timestamp(0)';
    EXECUTE sql into now_timestamp;
    WHILE  part_boundary - 20 * boundaries_interval < now_timestamp  LOOP
        part_boundary = part_boundary + boundaries_interval;
        partition_tmp = date_trunc('second', part_boundary);
        EXECUTE format('ALTER TABLE '||relationname||' ADD PARTITION p'||EXTRACT(epoch FROM CAST(partition_tmp AS TIMESTAMPTZ))||' values less than (%L);', part_boundary);
    END LOOP;

    part_boundary := part_boundary + boundaries_interval;
    partition_tmp = date_trunc('second', part_boundary);
    EXECUTE 'ALTER TABLE '||relationname||' ADD PARTITION p'||EXTRACT(epoch FROM CAST(partition_tmp AS TIMESTAMPTZ))||' values less than ('''||part_boundary||''');';

END;
$$
LANGUAGE plpgsql;

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_pg_asp() cascade;
  end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9534, 3465, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_asp
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
) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2998;
CREATE UNIQUE INDEX gs_asp_sample_time_index ON pg_catalog.gs_asp USING BTREE(sample_time timestamptz_ops);

GRANT SELECT ON pg_catalog.gs_asp TO PUBLIC;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-- use relfilenode to get schemane.tablename if the table is larger than threshold_size_gb
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

REVOKE ALL ON pg_catalog.gs_auditing_policy FROM public;
REVOKE ALL ON pg_catalog.gs_auditing_policy_access FROM public;
REVOKE ALL ON pg_catalog.gs_auditing_policy_filters FROM public;
REVOKE ALL ON pg_catalog.gs_auditing_policy_privileges FROM public;
REVOKE ALL ON pg_catalog.gs_policy_label FROM public;
REVOKE ALL ON pg_catalog.gs_masking_policy FROM public;
REVOKE ALL ON pg_catalog.gs_masking_policy_actions FROM public;
REVOKE ALL ON pg_catalog.gs_masking_policy_filters FROM public;
REVOKE ALL ON pg_catalog.gs_labels FROM public;
REVOKE ALL ON pg_catalog.gs_masking FROM public;
REVOKE ALL ON pg_catalog.gs_auditing FROM public;
REVOKE ALL ON pg_catalog.gs_auditing_access FROM public;
REVOKE ALL ON pg_catalog.gs_auditing_privilege FROM public;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.gs_labels TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.gs_masking TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.gs_auditing TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.gs_auditing_access TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.gs_auditing_privilege TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/


DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select relname from pg_class where relname='gs_asp' limit 1) into ans;
  if ans = true then
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2999;
    CREATE INDEX gs_asp_sampletime_index ON pg_catalog.gs_asp USING BTREE(sample_time timestamptz_ops);
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
  end if;
END$$;

drop function if exists pg_catalog.report_fatal() cascade;
drop function if exists pg_catalog.signal_backend(bigint, int) cascade;

DROP FUNCTION IF EXISTS pg_catalog.top_key(name, int8) cascade;
set local inplace_upgrade_next_system_object_oids = IUO_PROC,6008;
create or replace function pg_catalog.top_key(name, int8) returns text language INTERNAL   NOT FENCED as 'top_key_internal';


-- 1. create system relations
--    gs_matview
--    gs_matview_dependency

--gs_matview
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9982, 9984, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_matview
(
    matviewid Oid NOCOMPRESS NOT NULL,
    mapid Oid NOCOMPRESS NOT NULL,
    ivm "bool" NOCOMPRESS NOT NULL,
    needrefresh "bool" NOCOMPRESS NOT NULL,
    refreshtime timestamp NOCOMPRESS NOT NULL
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9983;
CREATE UNIQUE INDEX gs_matview_oid_index ON pg_catalog.gs_matview USING BTREE(oid oid_ops);
CREATE UNIQUE INDEX gs_matview_matviewid_index ON pg_catalog.gs_matview USING BTREE(matviewid oid_ops);

--gs_matview_dependency
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9985, 9987, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_matview_dependency
(
    matviewid Oid NOCOMPRESS NOT NULL,
    relid Oid NOCOMPRESS NOT NULL,
    mlogid Oid NOCOMPRESS NOT NULL,
    mxmin int4 NOCOMPRESS NOT NULL
) WITH oids TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9986;
CREATE UNIQUE INDEX gs_matviewdep_oid_index ON pg_catalog.gs_matview_dependency USING BTREE(oid oid_ops);
CREATE UNIQUE INDEX gs_matview_dependency_matviewid_index ON pg_catalog.gs_matview_dependency USING BTREE(matviewid oid_ops);
CREATE UNIQUE INDEX gs_matview_dependency_relid_index ON pg_catalog.gs_matview_dependency USING BTREE(relid oid_ops);
CREATE UNIQUE INDEX gs_matview_dependency_mlogid_index ON pg_catalog.gs_matview_dependency USING BTREE(mlogid oid_ops);
CREATE UNIQUE INDEX gs_matview_dependency_bothid_index ON pg_catalog.gs_matview_dependency USING BTREE(matviewid oid_ops, relid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;


--add gs_encrypted_columns
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9700, 9703, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_encrypted_columns
(
    rel_id Oid NOCOMPRESS NOT NULL,
    column_name Name NOCOMPRESS NOT NULL,
    column_key_id Oid NOCOMPRESS NOT NULL,
    encryption_type int1 NOCOMPRESS NOT NULL,
    data_type_original_oid Oid NOCOMPRESS NOT NULL,
    data_type_original_mod integer NOCOMPRESS NOT NULL,
    create_date timestamp NOCOMPRESS NOT NULL
) WITH OIDS;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9701;

CREATE UNIQUE INDEX gs_encrypted_columns_oid_index ON pg_catalog.gs_encrypted_columns USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9702;

CREATE UNIQUE INDEX gs_encrypted_columns_rel_id_column_name_index ON pg_catalog.gs_encrypted_columns USING BTREE(rel_id OID_OPS, column_name NAME_OPS);

--add gs_client_global_keys
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9710, 9713, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_client_global_keys
(
    global_key_name Name NOCOMPRESS NOT NULL,
    key_namespace Oid NOCOMPRESS NOT NULL,
    key_owner Oid NOCOMPRESS NOT NULL,
    key_acl aclitem[1] NOCOMPRESS,
    create_date timestamp NOCOMPRESS
) WITH OIDS;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9711;

CREATE UNIQUE INDEX gs_client_global_keys_oid_index ON pg_catalog.gs_client_global_keys USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9712;

CREATE UNIQUE INDEX gs_client_global_keys_name_index ON pg_catalog.gs_client_global_keys USING BTREE(global_key_name NAME_OPS, key_namespace OID_OPS);

--add gs_column_keys
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9720, 9724, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_column_keys
(
    column_key_name Name NOCOMPRESS NOT NULL,
    column_key_distributed_id Oid NOCOMPRESS NOT NULL,
    global_key_id Oid NOCOMPRESS NOT NULL,
    key_namespace Oid NOCOMPRESS NOT NULL,
    key_owner Oid NOCOMPRESS NOT NULL,
    create_date timestamp NOCOMPRESS NOT NULL,
    key_acl aclitem[1] NOCOMPRESS
) WITH OIDS;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9721;

CREATE UNIQUE INDEX gs_column_keys_oid_index ON pg_catalog.gs_column_keys USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9722;

CREATE UNIQUE INDEX gs_column_keys_name_index ON pg_catalog.gs_column_keys USING BTREE(column_key_name NAME_OPS, key_namespace OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9723;

CREATE UNIQUE INDEX gs_column_keys_distributed_id_index ON pg_catalog.gs_column_keys USING BTREE(column_key_distributed_id OID_OPS);

--add gs_client_global_keys_args
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9730, 9733, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_client_global_keys_args
(
    global_key_id Oid NOCOMPRESS NOT NULL,
    function_name Name NOCOMPRESS NOT NULL,
    key Name NOCOMPRESS NOT NULL,
    value bytea NOCOMPRESS
) WITH OIDS;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9731;

CREATE UNIQUE INDEX gs_client_global_keys_args_oid_index ON pg_catalog.gs_client_global_keys_args USING BTREE(oid OID_OPS);

--add gs_column_keys_args
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9740, 9743, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_column_keys_args
(
    column_key_id Oid NOCOMPRESS NOT NULL,
    function_name Name NOCOMPRESS NOT NULL,
    key Name NOCOMPRESS NOT NULL,
    value bytea NOCOMPRESS
) WITH OIDS;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9741;

CREATE UNIQUE INDEX gs_column_keys_args_oid_index ON pg_catalog.gs_column_keys_args USING BTREE(oid OID_OPS);

GRANT SELECT ON TABLE pg_catalog.gs_encrypted_columns TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_client_global_keys TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_client_global_keys_args TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_column_keys TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_column_keys_args TO PUBLIC;

--define new catalog type and function
DROP TYPE IF EXISTS pg_catalog.byteawithoutorderwithequalcol;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 4402, 4404, b;
CREATE TYPE pg_catalog.byteawithoutorderwithequalcol;

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolin(cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4440;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolin(cstring) RETURNS byteawithoutorderwithequalcol LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolin';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolout(byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4446;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolout(byteawithoutorderwithequalcol) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolout';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolrecv(internal) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4444;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolrecv(internal) RETURNS byteawithoutorderwithequalcol LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolrecv';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolsend(byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4451;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolsend(byteawithoutorderwithequalcol) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolsend';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoltypmodin(_cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4449;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcoltypmodin(_cstring) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcoltypmodin';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoltypmodout(int4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4450;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcoltypmodout(int4) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcoltypmodout';

create type pg_catalog.byteawithoutorderwithequalcol (input=byteawithoutorderwithequalcolin, output=byteawithoutorderwithequalcolout, RECEIVE = byteawithoutorderwithequalcolrecv,
    SEND = byteawithoutorderwithequalcolsend, TYPMOD_IN = byteawithoutorderwithequalcoltypmodin, TYPMOD_OUT = byteawithoutorderwithequalcoltypmodout, STORAGE=EXTENDED);
COMMENT ON TYPE pg_catalog.byteawithoutorderwithequalcol IS 'encrypted data variable-length string, binary values escaped';
COMMENT ON TYPE pg_catalog._byteawithoutorderwithequalcol IS 'encrypted data variable-length string, binary values escaped';

DROP TYPE IF EXISTS pg_catalog.byteawithoutordercol;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 4403, 4405, b;
CREATE TYPE pg_catalog.byteawithoutordercol;

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolin(cstring) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4423;
CREATE FUNCTION pg_catalog.byteawithoutordercolin(cstring) RETURNS byteawithoutordercol LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutordercolin';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolout(byteawithoutordercol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4428;
CREATE FUNCTION pg_catalog.byteawithoutordercolout(byteawithoutordercol) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutordercolout';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolrecv(internal) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4424;
CREATE FUNCTION pg_catalog.byteawithoutordercolrecv(internal) RETURNS byteawithoutordercol LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutordercolrecv';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolsend(byteawithoutordercol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4425;
CREATE FUNCTION pg_catalog.byteawithoutordercolsend(byteawithoutordercol) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutordercolsend';

create type pg_catalog.byteawithoutordercol (input=byteawithoutordercolin, output=byteawithoutordercolout, RECEIVE = byteawithoutordercolrecv, SEND = byteawithoutordercolsend,
    TYPMOD_IN = byteawithoutorderwithequalcoltypmodin, TYPMOD_OUT = byteawithoutorderwithequalcoltypmodout, STORAGE=EXTENDED);
COMMENT ON TYPE pg_catalog.byteawithoutordercol IS 'encrypted data variable-length string, binary values escaped';
COMMENT ON TYPE pg_catalog._byteawithoutordercol IS 'encrypted data variable-length string, binary values escaped';

--define catalog view and function
DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmp(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4418;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolcmp(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolcmp';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmpbytear(byteawithoutorderwithequalcol, bytea) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4456;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolcmpbytear(byteawithoutorderwithequalcol, bytea) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolcmpbytear';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmpbyteal(bytea, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4463;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolcmpbyteal(bytea, byteawithoutorderwithequalcol) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'byteawithoutorderwithequalcolcmpbyteal';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleq(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4412;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcoleq(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) RETURNS  bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcoleq';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcoleq(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) IS 'implementation of = operator';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleqbyteal(bytea, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4457;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcoleqbyteal(bytea, byteawithoutorderwithequalcol) RETURNS  bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcoleqbyteal';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcoleqbyteal(bytea, byteawithoutorderwithequalcol) IS 'implementation of = operator';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleqbytear(byteawithoutorderwithequalcol, bytea) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4447;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcoleqbytear(byteawithoutorderwithequalcol, bytea) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcoleqbytear';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcoleqbytear(byteawithoutorderwithequalcol, bytea) IS 'implementation of = operator';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolne(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4417;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolne(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) RETURNS  bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcolne';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcolne(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) IS 'implementation of <> operator';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolnebyteal(bytea, byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4561;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolnebyteal(bytea, byteawithoutorderwithequalcol) RETURNS  bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcolnebyteal';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcolnebyteal(bytea, byteawithoutorderwithequalcol) IS 'implementation of <> operator';

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolnebytear(byteawithoutorderwithequalcol, bytea) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4460;
CREATE FUNCTION pg_catalog.byteawithoutorderwithequalcolnebytear(byteawithoutorderwithequalcol, bytea) RETURNS  bool LANGUAGE INTERNAL IMMUTABLE STRICT LEAKPROOF as 'byteawithoutorderwithequalcolnebytear';
COMMENT ON FUNCTION pg_catalog.byteawithoutorderwithequalcolnebytear(byteawithoutorderwithequalcol, bytea) IS 'implementation of <> operator';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(name, text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9130;
CREATE FUNCTION pg_catalog.has_cek_privilege(name, text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_name_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(name, oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9131;
CREATE FUNCTION pg_catalog.has_cek_privilege(name, oid, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_name_id';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9132;
CREATE FUNCTION pg_catalog.has_cek_privilege(oid, text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_id_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9133;
CREATE FUNCTION pg_catalog.has_cek_privilege(oid, oid, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_id_id';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9134;
CREATE FUNCTION pg_catalog.has_cek_privilege(text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9135;
CREATE FUNCTION pg_catalog.has_cek_privilege(oid, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cek_privilege_id';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(name, text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9024;
CREATE FUNCTION pg_catalog.has_cmk_privilege(name, text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_name_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(name, oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9025;
CREATE FUNCTION pg_catalog.has_cmk_privilege(name, oid, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_name_id';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9026;
CREATE FUNCTION pg_catalog.has_cmk_privilege(oid, text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_id_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9027;
CREATE FUNCTION pg_catalog.has_cmk_privilege(oid, oid, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_id_id';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(text, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9028;
CREATE FUNCTION pg_catalog.has_cmk_privilege(text, text) RETURNS  bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_name';

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9029;
CREATE FUNCTION pg_catalog.has_cmk_privilege(oid, text) RETURNS bool LANGUAGE INTERNAL STABLE STRICT as 'has_cmk_privilege_id';

DROP OPERATOR IF EXISTS pg_catalog.= (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4453;

CREATE OPERATOR pg_catalog.= (
   leftarg = byteawithoutorderwithequalcol, rightarg = byteawithoutorderwithequalcol, procedure = byteawithoutorderwithequalcoleq,
   commutator = operator(pg_catalog.=) ,
   restrict = eqsel, join = eqjoinsel,
   HASHES, MERGES
);
COMMENT ON OPERATOR pg_catalog.=(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) IS 'equal';

DROP OPERATOR IF EXISTS pg_catalog.<> (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4454;

CREATE OPERATOR pg_catalog.<> (
   leftarg = byteawithoutorderwithequalcol, rightarg = byteawithoutorderwithequalcol, procedure = byteawithoutorderwithequalcolne,
   negator = operator(pg_catalog.=) ,
   restrict = neqsel, join = neqjoinsel
);
COMMENT ON OPERATOR pg_catalog.<>(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) IS 'not equal';

DROP OPERATOR IF EXISTS pg_catalog.= (byteawithoutorderwithequalcol, bytea) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4422;

CREATE OPERATOR pg_catalog.= (
   leftarg = byteawithoutorderwithequalcol, rightarg = bytea, procedure = byteawithoutorderwithequalcoleqbytear,
   restrict = eqsel, join = eqjoinsel
);
COMMENT ON OPERATOR pg_catalog.=(byteawithoutorderwithequalcol, bytea) IS 'equal';

DROP OPERATOR IF EXISTS pg_catalog.<> (byteawithoutorderwithequalcol, bytea) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4423;

CREATE OPERATOR pg_catalog.<> (
   leftarg = byteawithoutorderwithequalcol, rightarg = bytea, procedure = byteawithoutorderwithequalcolnebytear,
   negator = operator(pg_catalog.=) ,
   restrict = neqsel, join = neqjoinsel
);
COMMENT ON OPERATOR pg_catalog.<>(byteawithoutorderwithequalcol, bytea) IS 'not equal';

DROP OPERATOR IF EXISTS pg_catalog.= (bytea,byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4431;

CREATE OPERATOR pg_catalog.= (
   leftarg = bytea, rightarg = byteawithoutorderwithequalcol, procedure = byteawithoutorderwithequalcoleqbyteal,
   commutator = operator(pg_catalog.=) ,
   restrict = eqsel, join = eqjoinsel
);
COMMENT ON OPERATOR pg_catalog.=(bytea, byteawithoutorderwithequalcol) IS 'equal';

DROP OPERATOR IF EXISTS pg_catalog.<> (bytea,byteawithoutorderwithequalcol) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4432;

CREATE OPERATOR pg_catalog.<> (
   leftarg = bytea, rightarg = byteawithoutorderwithequalcol, procedure = byteawithoutorderwithequalcolnebyteal,
   commutator = operator(pg_catalog.<>) , negator = operator(pg_catalog.=) ,
   restrict = neqsel, join = neqjoinsel
);
COMMENT ON OPERATOR pg_catalog.<>(bytea, byteawithoutorderwithequalcol) IS 'not equal';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 436;
CREATE OPERATOR FAMILY byteawithoutorderwithequalcol_ops USING BTREE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4470;
CREATE OPERATOR FAMILY byteawithoutorderwithequalcol_ops USING HASH;

CREATE OPERATOR CLASS byteawithoutorderwithequalcol_ops DEFAULT
   FOR TYPE byteawithoutorderwithequalcol USING BTREE FAMILY byteawithoutorderwithequalcol_ops as
   OPERATOR 3 pg_catalog.=(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol),
   FUNCTION 1 pg_catalog.byteawithoutorderwithequalcolcmp(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol),
   FUNCTION 2 (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) pg_catalog.bytea_sortsupport(internal);

CREATE OPERATOR CLASS byteawithoutorderwithequalcol_ops DEFAULT
   FOR TYPE byteawithoutorderwithequalcol USING HASH FAMILY byteawithoutorderwithequalcol_ops as
   OPERATOR 1 pg_catalog.=(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol),
   FUNCTION 1 (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) pg_catalog.hashvarlena(internal);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3236;
UPDATE pg_index SET indisunique = false WHERE indexrelid = 3234;
CREATE INDEX streaming_cont_query_schema_change_index ON pg_catalog.streaming_cont_query USING BTREE(matrelid oid_ops, active bool_ops);

-- 1. create system relation streaming_reaper_status and its indexes.
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9030, 7202, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.streaming_reaper_status
(
    id int4 NOCOMPRESS NOT NULL,
    contquery_name NAME NOCOMPRESS NOT NULL,
    gather_interval TEXT NOCOMPRESS,
    gather_completion_time TEXT NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9995;
CREATE UNIQUE INDEX streaming_reaper_status_id_index ON pg_catalog.streaming_reaper_status USING BTREE(id int4_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9999;
CREATE UNIQUE INDEX streaming_reaper_status_oid_index ON pg_catalog.streaming_reaper_status USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON TABLE pg_catalog.streaming_reaper_status TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3237;
CREATE INDEX streaming_gather_agg_index ON pg_catalog.pg_aggregate USING BTREE(aggtransfn oid_ops, aggcollectfn oid_ops, aggfinalfn oid_ops);

DROP INDEX IF EXISTS pg_catalog.pgxc_slice_relid_index;
DROP INDEX IF EXISTS pg_catalog.pgxc_slice_order_index;
DROP TYPE IF EXISTS pg_catalog.pgxc_slice;
DROP TABLE IF EXISTS pg_catalog.pgxc_slice;

-- Set GUC for catalog
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9035, 9032, 0, 0;

-- NEW CATALOG pgxc_slice
create table pg_catalog.pgxc_slice
(
    relname name NOCOMPRESS NOT NULL,
    type "char" NOCOMPRESS NOT NULL,
    strategy "char" NOCOMPRESS NOT NULL,
    relid oid NOCOMPRESS NOT NULL,
    referenceoid oid NOCOMPRESS NOT NULL,
    sindex integer NOCOMPRESS NOT NULL,
    "interval" text[] NOCOMPRESS,
    transitboundary text[] NOCOMPRESS,
    transitno integer NOCOMPRESS,
    nodeoid oid NOCOMPRESS,
    boundaries text[] NOCOMPRESS,
    specified boolean NOCOMPRESS,
    sliceorder integer NOCOMPRESS
);

-- set GUC for pgxc_slice's index
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9033;

-- create index
CREATE UNIQUE INDEX pgxc_slice_relid_index ON pg_catalog.pgxc_slice
    USING BTREE(relid oid_ops, type char_ops, relname name_ops, sindex int4_ops);

-- set GUC for pgxc_slice's order index
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9034;

-- create index
CREATE UNIQUE INDEX pgxc_slice_order_index ON pg_catalog.pgxc_slice
    USING BTREE(relid oid_ops, type char_ops, sliceorder int4_ops, sindex int4_ops);

GRANT SELECT ON pg_catalog.pgxc_slice TO PUBLIC;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

DROP VIEW IF EXISTS pg_catalog.pgxc_get_table_skewness CASCADE;
-- view for get the skew of the data distribution in all datanodes
CREATE OR REPLACE VIEW pg_catalog.pgxc_get_table_skewness AS
WITH skew AS
(
    SELECT
        schemaname,
        tablename,
        sum(dnsize) AS totalsize,
        avg(dnsize) AS avgsize,
        max(dnsize) AS maxsize,
        min(dnsize) AS minsize,
        (max(dnsize) - min(dnsize)) AS skewsize,
        stddev(dnsize) AS skewstddev
    FROM pg_catalog.pg_class c
    INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    INNER JOIN pg_catalog.table_distribution() s ON s.schemaname = n.nspname AND s.tablename = c.relname
    INNER JOIN pg_catalog.pgxc_class x ON c.oid = x.pcrelid AND x.pclocatortype IN('H', 'L', 'G')
    GROUP BY schemaname,tablename
)
SELECT
    schemaname,
    tablename,
    totalsize,
    avgsize::numeric(1000),
    (maxsize/totalsize)::numeric(4,3)  AS maxratio,
    (minsize/totalsize)::numeric(4,3)  AS minratio,
    skewsize,
    (skewsize/totalsize)::numeric(4,3)  AS skewratio,
    skewstddev::numeric(1000)
FROM skew
WHERE totalsize > 0;

GRANT SELECT ON TABLE pg_catalog.pgxc_get_table_skewness TO PUBLIC;


create or replace function pg_catalog.table_skewness(table_name text, column_name text,
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

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3442;
DROP FUNCTION IF EXISTS pg_catalog.pg_control_checkpoint() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_control_system() CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pg_control_checkpoint
(out checkpoint_lsn pg_catalog.int8,
out redo_lsn pg_catalog.int8,
out redo_wal_file pg_catalog.text,
out timeline_id pg_catalog.int4,
out full_page_writes pg_catalog.bool,
out next_oid pg_catalog.oid,
out next_multixact_id pg_catalog.xid,
out next_multi_offset pg_catalog.xid,
out oldest_xid pg_catalog.xid,
out oldest_xid_dbid pg_catalog.oid,
out oldest_active_xid pg_catalog.xid,
out checkpoint_time pg_catalog.timestamptz)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_control_checkpoint';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3441;
CREATE OR REPLACE FUNCTION pg_catalog.pg_control_system
(
out pg_control_version pg_catalog.int4,
out catalog_version_no pg_catalog.int4,
out system_identifier pg_catalog.int8,
out pg_control_last_modified pg_catalog.timestamptz)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_control_system';