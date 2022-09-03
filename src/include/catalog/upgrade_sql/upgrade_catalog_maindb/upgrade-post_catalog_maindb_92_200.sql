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

GRANT SELECT ON TABLE pg_catalog.pg_roles TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_user cascade;
CREATE OR REPLACE VIEW pg_catalog.pg_user AS
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
        rolspillspace AS spillspacelimit,
        rolmonitoradmin AS usemonitoradmin,
        roloperatoradmin AS useoperatoradmin,
        rolpolicyadmin AS usepolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE rolcanlogin;

GRANT SELECT ON TABLE pg_catalog.pg_user TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_shadow cascade;
CREATE OR REPLACE VIEW pg_catalog.pg_shadow AS
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
        rolspillspace AS spillspacelimit,
        rolmonitoradmin AS usemonitoradmin,
        roloperatoradmin AS useoperatoradmin,
        rolpolicyadmin AS usepolicyadmin
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    WHERE rolcanlogin;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE pg_catalog.pg_shadow TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

DROP VIEW IF EXISTS pg_catalog.pg_rlspolicies cascade;
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

DROP VIEW IF EXISTS pg_catalog.gs_wlm_user_info cascade;
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

GRANT SELECT ON TABLE pg_catalog.gs_wlm_user_info TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_total_user_resource_info cascade;
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
FROM pg_user AS S, pg_total_user_resource_info_oid AS T
WHERE S.usesysid = T.userid;

GRANT SELECT ON TABLE pg_catalog.pg_total_user_resource_info TO PUBLIC;

DROP AGGREGATE IF EXISTS delta(anyelement) CASCADE;
DROP FUNCTION IF EXISTS delta_internal(anyarray,anyelement);
DROP  FUNCTION IF EXISTS delta_final(anyarray);
DROP AGGREGATE IF EXISTS spread(real) CASCADE;
DROP FUNCTION IF EXISTS spread_internal(real[], real);
DROP FUNCTION IF EXISTS spread_final(real[]);
DROP  FUNCTION IF EXISTS spread_collect(real[], real[]);

drop function if exists pg_catalog.sys_context(text, text) cascade;

drop function if exists pg_catalog.raise_application_error(integer, text, boolean) cascade;
CREATE OR REPLACE FUNCTION pg_catalog.report_application_error(
    IN log text,
    IN code integer default null
)RETURNS void
AS '$libdir/plpgsql','report_application_error'
LANGUAGE C VOLATILE NOT FENCED;

DROP SCHEMA IF EXISTS pkg_service cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 3988;
CREATE SCHEMA pkg_service;
COMMENT ON schema pkg_service IS 'pkg_service schema';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4800;
CREATE OR REPLACE FUNCTION pkg_service.job_cancel(
    in id bigint
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
 as 'job_cancel';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4801;
CREATE OR REPLACE FUNCTION pkg_service.job_finish(
    in id bigint,
    in finished boolean,
    in next_time timestamp without time zone default sysdate
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_finish';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4802;
CREATE OR REPLACE FUNCTION pkg_service.job_submit(
    in id bigint,
    in content text,
    in next_date timestamp without time zone default sysdate,
    in interval_time text default 'null',
    out job integer
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_submit';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4803;
CREATE OR REPLACE FUNCTION pkg_service.job_update(
    in id bigint,
    in next_date timestamp without time zone,
    in interval_time text,
    in content text
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_update';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5717;
CREATE FUNCTION pkg_service.submit_on_nodes(node_name name, database name, what text, next_date timestamp without time zone, job_interval text, OUT job integer)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'submit_job_on_nodes';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5718;
CREATE FUNCTION pkg_service.isubmit_on_nodes(job bigint, node_name name, database name, what text, next_date timestamp without time zone, job_interval text)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'isubmit_job_on_nodes';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6007;
CREATE FUNCTION pkg_service.isubmit_on_nodes_internal(job bigint, node_name name, database name, what text, next_date timestamp without time zone, job_interval text)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'isubmit_job_on_nodes_internal';

DROP VIEW IF EXISTS pg_catalog.all_objects CASCADE;
DROP VIEW IF EXISTS pg_catalog.all_synonyms CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_objects CASCADE;
DROP VIEW IF EXISTS pg_catalog.dba_synonyms CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_objects CASCADE;
DROP VIEW IF EXISTS pg_catalog.user_synonyms CASCADE;

CREATE OR REPLACE VIEW pg_catalog.DB_OBJECTS AS
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

CREATE OR REPLACE VIEW pg_catalog.MY_OBJECTS AS
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
		AND pg_get_userbyid(cs.relowner)=current_user::text
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
	WHERE pg_get_userbyid(pc.proowner)=current_user::text
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
	WHERE pg_get_userbyid(cs.relowner)=current_user::text
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
	WHERE pg_get_userbyid(cs.relowner)=current_user::text
	UNION
	SELECT
		te.typname AS OBJECT_NAME,
		te.oid AS OBJECT_ID,
		'type'::NAME AS OBJECT_TYPE,
		te.typnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_type te
	WHERE pg_get_userbyid(te.typowner)=current_user::text
	UNION
	SELECT
		op.oprname AS OBJECT_NAME,
		op.oid AS OBJECT_ID,
		'operator'::NAME AS OBJECT_TYPE,
		op.oprnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_operator op
	WHERE pg_get_userbyid(op.oprowner)=current_user::text
	UNION ALL
	SELECT
		syn.synname AS OBJECT_NAME,
		syn.oid AS OBJECT_ID,
		'synonym'::NAME AS OBJECT_TYPE,
		syn.synnamespace AS NAMESPACE,
		NULL::timestamptz AS CREATED,
		NULL::timestamptz AS LAST_DDL_TIME
	FROM pg_synonym syn
	WHERE pg_get_userbyid(syn.synowner) = current_user::text;

CREATE OR REPLACE VIEW pg_catalog.ADM_OBJECTS AS
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
REVOKE ALL on PG_CATALOG.ADM_OBJECTS FROM public;

declare
    user_name text;
    query_str text;
begin
    select session_user into user_name;

    query_str := 'grant all on table pg_catalog.adm_objects to ' || quote_ident(user_name) || ';';
    execute immediate query_str;

end;
/

GRANT SELECT ON TABLE pg_catalog.db_objects TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.my_objects TO PUBLIC;

DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(int, name, name,text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(name, name, text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(name, name) cascade;

CREATE OR REPLACE VIEW DBE_PERF.os_runtime AS
  SELECT * FROM pv_os_run_info();

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

CREATE OR REPLACE VIEW DBE_PERF.global_os_runtime AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_os_runtime();

CREATE OR REPLACE VIEW DBE_PERF.os_threads AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_os_threads AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_os_threads();

/* instance */
CREATE OR REPLACE VIEW DBE_PERF.instance_time AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_instance_time AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_instance_time();

/* workload */
CREATE OR REPLACE VIEW DBE_PERF.workload_sql_count AS
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

CREATE OR REPLACE VIEW DBE_PERF.workload_sql_elapse_time AS
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

CREATE OR REPLACE VIEW DBE_PERF.summary_workload_sql_count AS
  SELECT * FROM DBE_PERF.get_summary_workload_sql_count();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_workload_sql_elapse_time AS
  SELECT * FROM DBE_PERF.get_summary_workload_sql_elapse_time();

/* user transaction */
CREATE OR REPLACE VIEW DBE_PERF.user_transaction AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_user_transaction AS
  SELECT * FROM DBE_PERF.get_global_user_transaction();

/* workload transaction */
CREATE OR REPLACE VIEW DBE_PERF.workload_transaction AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_workload_transaction AS
  SELECT * FROM DBE_PERF.get_global_workload_transaction();

CREATE OR REPLACE VIEW DBE_PERF.summary_workload_transaction AS
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
CREATE OR REPLACE VIEW DBE_PERF.session_stat AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_session_stat AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_stat();

CREATE OR REPLACE VIEW DBE_PERF.session_time AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_session_time AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_time();

CREATE OR REPLACE VIEW DBE_PERF.session_memory AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_session_memory AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_memory();


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
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.session_memory_detail AS
  SELECT * FROM pv_session_memory_detail_tp();

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

CREATE OR REPLACE VIEW DBE_PERF.global_session_memory_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_session_memory_detail();

CREATE OR REPLACE VIEW DBE_PERF.session_cpu_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.session_memory_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.thread_wait_status AS
  SELECT * FROM pg_stat_get_status(NULL);

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

/* WLM */
CREATE OR REPLACE VIEW DBE_PERF.wlm_controlgroup_config AS
  SELECT DISTINCT * FROM gs_all_control_group_info();

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

CREATE OR REPLACE VIEW DBE_PERF.wlm_cluster_resource_runtime AS
  SELECT * FROM pg_stat_get_wlm_node_resource_info(0);

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

CREATE OR REPLACE VIEW DBE_PERF.wlm_user_resource_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.wlm_cgroup_config AS
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

CREATE OR REPLACE VIEW DBE_PERF.wlm_user_resource_config AS
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

CREATE OR REPLACE VIEW DBE_PERF.wlm_resourcepool_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.wlm_workload_runtime AS
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
  FROM pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U, gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
          S.usesysid = U.oid;

CREATE OR REPLACE VIEW DBE_PERF.wlm_workload_history_info AS
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

/* Query-AP Operator */
--ec history operator-level view for DM in single CN
CREATE OR REPLACE VIEW DBE_PERF.operator_ec_history AS
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

CREATE OR REPLACE VIEW DBE_PERF.operator_ec_history_table AS
  SELECT * FROM gs_wlm_ec_operator_info;

--real time ec operator-level view in single CN
CREATE OR REPLACE VIEW DBE_PERF.operator_ec_runtime AS
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
  FROM DBE_PERF.session_stat_activity AS s, pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
    where s.query_id = t.queryid and t.ec_operator > 0;

CREATE OR REPLACE VIEW DBE_PERF.operator_history_table AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_operator_history_table AS
  SELECT * FROM DBE_PERF.get_global_operator_history_table();

--history operator-level view for DM in single CN
CREATE OR REPLACE VIEW DBE_PERF.operator_history AS
  SELECT * FROM pg_stat_get_wlm_operator_info(0);

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

CREATE OR REPLACE VIEW DBE_PERF.global_operator_history AS
    SELECT * FROM DBE_PERF.get_global_operator_history();

--real time operator-level view in single CN
CREATE OR REPLACE VIEW DBE_PERF.operator_runtime AS
  SELECT t.*
  FROM DBE_PERF.session_stat_activity AS s, pg_stat_get_wlm_realtime_operator_info(NULL) as t
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

CREATE OR REPLACE VIEW DBE_PERF.global_operator_runtime AS
  SELECT * FROM DBE_PERF.get_global_operator_runtime();

/* Query-AP */
CREATE OR REPLACE VIEW DBE_PERF.statement_complex_history AS
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
FROM pg_stat_get_wlm_session_info(0) S;


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

CREATE OR REPLACE VIEW DBE_PERF.global_statement_complex_history AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history();

CREATE OR REPLACE VIEW DBE_PERF.statement_complex_history_table AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statement_complex_history_table AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_history_table();

CREATE OR REPLACE VIEW DBE_PERF.statement_user_complex_history AS
  SELECT * FROM DBE_PERF.statement_complex_history_table
    WHERE username = current_user::text;

CREATE OR REPLACE VIEW DBE_PERF.statement_complex_runtime AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statement_complex_runtime AS
  SELECT * FROM DBE_PERF.get_global_statement_complex_runtime();

CREATE OR REPLACE VIEW DBE_PERF.statement_iostat_complex_runtime AS
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
CREATE OR REPLACE VIEW DBE_PERF.memory_node_detail AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_memory_node_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_memory_node_detail();

CREATE OR REPLACE VIEW DBE_PERF.memory_node_ng_detail AS
  SELECT * FROM gs_total_nodegroup_memory_detail();

CREATE OR REPLACE VIEW DBE_PERF.shared_memory_detail AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_shared_memory_detail AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_shared_memory_detail();

/* comm */
CREATE OR REPLACE VIEW DBE_PERF.comm_delay AS
  SELECT DISTINCT * FROM pg_comm_delay();

CREATE OR REPLACE VIEW DBE_PERF.comm_recv_stream AS
  SELECT
    S.node_name,
    S.local_tid,
    S.remote_name,
    S.remote_tid,
    S.idx,
    S.sid,
    S.tcp_sock,
    S.state,
    S.query_id,
    S.pn_id,
    S.send_smp,
    S.recv_smp,
    S.recv_bytes,
    S.time,
    S.speed,
    S.quota,
    S.buff_usize
  FROM pg_comm_recv_stream() AS S;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_comm_recv_stream()
RETURNS setof DBE_PERF.comm_recv_stream
AS $$
DECLARE
    row_data record;
    local_query text;
    other_query text;
    BEGIN
        local_query := 'SELECT * FROM DBE_PERF.comm_recv_stream;';
        FOR row_data IN EXECUTE(local_query) LOOP
            return next row_data;
        END LOOP;
        other_query := 'SELECT * FROM global_comm_get_recv_stream() order by node_name;';
        FOR row_data IN EXECUTE(other_query) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_comm_recv_stream AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_comm_recv_stream();

CREATE OR REPLACE VIEW DBE_PERF.comm_send_stream AS
  SELECT
    S.node_name,
    S.local_tid,
    S.remote_name,
    S.remote_tid,
    S.idx,
    S.sid,
    S.tcp_sock,
    S.state,
    S.query_id,
    S.pn_id,
    S.send_smp,
    S.recv_smp,
    S.send_bytes,
    S.time,
    S.speed,
    S.quota,
    S.wait_quota
  FROM pg_comm_send_stream() AS S;

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_comm_send_stream()
RETURNS setof DBE_PERF.comm_send_stream
AS $$
DECLARE
    row_data record;
    local_query text;
    other_query text;
    BEGIN
        local_query := 'SELECT * FROM DBE_PERF.comm_send_stream;';
        FOR row_data IN EXECUTE(local_query) LOOP
            return next row_data;
        END LOOP;
        other_query := 'SELECT * FROM global_comm_get_send_stream() order by node_name;';
        FOR row_data IN EXECUTE(other_query) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_comm_send_stream AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_comm_send_stream();

CREATE OR REPLACE VIEW DBE_PERF.comm_status AS
  SELECT * FROM pg_comm_status();

CREATE OR REPLACE FUNCTION DBE_PERF.get_global_comm_status()
RETURNS setof DBE_PERF.comm_status
AS $$
DECLARE
    row_data record;
    local_query text;
    other_query text;
    BEGIN
        local_query := 'SELECT * FROM pg_comm_status;';
        FOR row_data IN EXECUTE(local_query) LOOP
            return next row_data;
        END LOOP;
        other_query := 'SELECT * FROM global_comm_get_status() order by node_name;';
        FOR row_data IN EXECUTE(other_query) LOOP
            return next row_data;
        END LOOP;
            return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_comm_status AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_comm_status();

/* cache I/O */
CREATE OR REPLACE VIEW DBE_PERF.statio_all_indexes AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_all_indexes AS
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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit
    FROM DBE_PERF.get_summary_statio_all_indexes() as Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.statio_all_sequences AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_all_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_all_sequences();

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_all_sequences AS
  SELECT schemaname, relname,
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
   FROM DBE_PERF.get_global_statio_all_sequences()
   GROUP BY (schemaname, relname);

CREATE OR REPLACE VIEW DBE_PERF.statio_all_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_all_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_all_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_all_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname,
         SUM(Ti.heap_blks_read) heap_blks_read, SUM(Ti.heap_blks_hit) heap_blks_hit,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit,
         SUM(Ti.toast_blks_read) toast_blks_read, SUM(Ti.toast_blks_hit) toast_blks_hit,
         SUM(Ti.tidx_blks_read) tidx_blks_read, SUM(Ti.tidx_blks_hit) tidx_blks_hit
    FROM DBE_PERF.get_summary_statio_all_tables() Ti left join DBE_PERF.get_local_toast_relation() Tn on Tn.shemaname = Ti.toastrelschemaname and Tn.relname = Ti.toastrelname
  GROUP BY (1, 2);

CREATE OR REPLACE VIEW DBE_PERF.statio_sys_indexes AS
  SELECT * FROM DBE_PERF.statio_all_indexes
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_sys_indexes AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_indexes();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_sys_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit
  FROM DBE_PERF.get_summary_statio_sys_indexes() AS Ti
      LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.statio_sys_sequences AS
  SELECT * FROM DBE_PERF.statio_all_sequences
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_sys_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_sequences();

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_sys_sequences AS
  SELECT schemaname, relname,
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
    FROM DBE_PERF.get_global_statio_sys_sequences()
  GROUP BY (schemaname, relname);

CREATE OR REPLACE VIEW DBE_PERF.statio_sys_tables AS
  SELECT * FROM DBE_PERF.statio_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_sys_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_sys_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.statio_user_indexes AS
  SELECT * FROM DBE_PERF.statio_all_indexes
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_user_indexes AS
  SELECT *  FROM DBE_PERF.get_global_statio_user_indexes();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_user_indexes AS
  SELECT
      Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
      COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
      SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit
  FROM DBE_PERF.get_summary_statio_user_indexes() AS Ti
      LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
      ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
  GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.statio_user_sequences AS
  SELECT * FROM DBE_PERF.statio_all_sequences
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_user_sequences AS
  SELECT * FROM DBE_PERF.get_global_statio_user_sequences();

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_user_sequences AS
  SELECT schemaname, relname,
         SUM(blks_read) blks_read, SUM(blks_hit) blks_hit
    FROM DBE_PERF.get_global_statio_user_sequences()
  GROUP BY (schemaname, relname);

CREATE OR REPLACE VIEW DBE_PERF.statio_user_tables AS
  SELECT * FROM DBE_PERF.statio_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_statio_user_tables AS
  SELECT * FROM DBE_PERF.get_global_statio_user_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_statio_user_tables AS
  SELECT Ti.schemaname as schemaname, COALESCE(Ti.relname, Tn.toastname) as relname,
         SUM(Ti.heap_blks_read) heap_blks_read, SUM(Ti.heap_blks_hit) heap_blks_hit,
         SUM(Ti.idx_blks_read) idx_blks_read, SUM(Ti.idx_blks_hit) idx_blks_hit,
         SUM(Ti.toast_blks_read) toast_blks_read, SUM(Ti.toast_blks_hit) toast_blks_hit,
         SUM(Ti.tidx_blks_read) tidx_blks_read, SUM(Ti.tidx_blks_hit) tidx_blks_hit
  FROM DBE_PERF.get_summary_statio_user_tables() AS Ti LEFT JOIN DBE_PERF.get_local_toast_relation() Tn
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_db_cu AS
  SELECT * FROM DBE_PERF.get_stat_db_cu();

/* Object */
CREATE OR REPLACE VIEW DBE_PERF.stat_all_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.stat_all_indexes AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_all_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_all_indexes();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_all_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_all_indexes() AS Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.stat_sys_tables AS
  SELECT * FROM DBE_PERF.stat_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_sys_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_sys_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.stat_sys_indexes AS
  SELECT * FROM DBE_PERF.stat_all_indexes
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_sys_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_sys_indexes();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_sys_indexes AS
  SELECT Ti.schemaname AS schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_sys_indexes() AS Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.stat_user_tables AS
  SELECT * FROM DBE_PERF.stat_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_user_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_user_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_user_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.stat_user_indexes AS
  SELECT * FROM DBE_PERF.stat_all_indexes
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_user_indexes AS
  SELECT * FROM DBE_PERF.get_global_stat_user_indexes();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_user_indexes AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
         SUM(Ti.idx_scan) idx_scan, SUM(Ti.idx_tup_read) idx_tup_read, SUM(Ti.idx_tup_fetch) idx_tup_fetch
    FROM DBE_PERF.get_summary_stat_user_indexes() as Ti
         LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2, 3);

CREATE OR REPLACE VIEW DBE_PERF.stat_database AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_database AS
  SELECT * FROM DBE_PERF.get_global_stat_database();

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_database AS
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

CREATE OR REPLACE VIEW DBE_PERF.stat_database_conflicts AS
  SELECT
    D.oid AS datid,
    D.datname AS datname,
    pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
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


CREATE OR REPLACE VIEW DBE_PERF.global_stat_database_conflicts AS
  SELECT * FROM DBE_PERF.get_global_stat_database_conflicts();

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_database_conflicts AS
  SELECT
    D.datname AS datname,
    pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
    pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
    pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
    pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
    pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
  FROM pg_database D;

CREATE OR REPLACE VIEW DBE_PERF.stat_xact_all_tables AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_xact_all_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_all_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_xact_all_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname, NULL) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_all_tables() as Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE OR REPLACE VIEW DBE_PERF.stat_xact_sys_tables AS
  SELECT * FROM DBE_PERF.stat_xact_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_xact_sys_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_sys_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_xact_sys_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_sys_tables() as Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE OR REPLACE VIEW DBE_PERF.stat_xact_user_tables AS
  SELECT * FROM DBE_PERF.stat_xact_all_tables
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_xact_user_tables AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_user_tables();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_xact_user_tables AS
  SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
         SUM(Ti.seq_scan) seq_scan, SUM(Ti.seq_tup_read) seq_tup_read, SUM(Ti.idx_scan) idx_scan,
         SUM(Ti.idx_tup_fetch) idx_tup_fetch, SUM(Ti.n_tup_ins) n_tup_ins, SUM(Ti.n_tup_upd) n_tup_upd,
         SUM(Ti.n_tup_del) n_tup_del, SUM(Ti.n_tup_hot_upd) n_tup_hot_upd
    FROM DBE_PERF.get_summary_stat_xact_user_tables() AS Ti LEFT JOIN DBE_PERF.get_local_toast_relation() AS Tn
         ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
    GROUP BY (1, 2);

CREATE OR REPLACE VIEW DBE_PERF.stat_user_functions AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_user_functions AS
  SELECT * FROM DBE_PERF.get_global_stat_user_functions();

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_user_functions AS
  SELECT schemaname, funcname,
         SUM(calls) calls, SUM(total_time) total_time, SUM(self_time) self_time
    FROM DBE_PERF.get_global_stat_user_functions()
    GROUP BY (schemaname, funcname);

CREATE OR REPLACE VIEW DBE_PERF.stat_xact_user_functions AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_xact_user_functions AS
  SELECT * FROM DBE_PERF.get_global_stat_xact_user_functions();

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_xact_user_functions AS
  SELECT schemaname, funcname,
         SUM(calls) calls, SUM(total_time) total_time, SUM(self_time) self_time
    FROM DBE_PERF.get_global_stat_xact_user_functions()
    GROUP BY (schemaname, funcname);

CREATE OR REPLACE VIEW DBE_PERF.stat_bad_block AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_stat_bad_block AS
  SELECT DISTINCT * from DBE_PERF.get_global_stat_bad_block();

CREATE OR REPLACE VIEW DBE_PERF.summary_stat_bad_block AS
  SELECT databaseid, tablespaceid, relfilenode,
         SUM(forknum) forknum, SUM(error_count) error_count,
         MIN(first_time) first_time, MAX(last_time) last_time
    FROM DBE_PERF.get_global_stat_bad_block()
    GROUP BY (databaseid, tablespaceid, relfilenode);

/* File */
CREATE OR REPLACE VIEW DBE_PERF.file_redo_iostat AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_file_redo_iostat AS
  SELECT * FROM DBE_PERF.get_global_file_redo_iostat();

CREATE OR REPLACE VIEW DBE_PERF.summary_file_redo_iostat AS
  SELECT
    sum(phywrts) AS phywrts,
    sum(phyblkwrt) AS phyblkwrt,
    sum(writetim) AS writetim,
    ((sum(writetim) / greatest(sum(phywrts), 1))::bigint) AS avgiotim,
    max(lstiotim) AS lstiotim,
    min(miniotim) AS miniotim,
    max(maxiowtm) AS maxiowtm
    FROM DBE_PERF.get_global_file_redo_iostat();

CREATE OR REPLACE VIEW DBE_PERF.local_rel_iostat AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_rel_iostat AS
  SELECT * FROM DBE_PERF.get_global_rel_iostat();

CREATE OR REPLACE VIEW DBE_PERF.summary_rel_iostat AS
  SELECT
    sum(phyrds) AS phyrds, sum(phywrts) AS phywrts, sum(phyblkrd) AS phyblkrd,
    sum(phyblkwrt) AS phyblkwrt
    FROM DBE_PERF.get_global_rel_iostat();


CREATE OR REPLACE VIEW DBE_PERF.file_iostat AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_file_iostat AS
  SELECT * FROM DBE_PERF.get_global_file_iostat();

CREATE OR REPLACE VIEW DBE_PERF.summary_file_iostat AS
  SELECT
    filenum, dbid, spcid,
    sum(phyrds) AS phyrds, sum(phywrts) AS phywrts, sum(phyblkrd) AS phyblkrd,
    sum(phyblkwrt) AS phyblkwrt, sum(readtim) AS readtim, sum(writetim) AS writetim,
    ((sum(readtim + writetim) / greatest(sum(phyrds + phywrts), 1))::bigint) AS avgiotim,
    max(lstiotim) AS lstiotim, min(miniotim) AS miniotim, max(maxiowtm) AS maxiowtm
    FROM DBE_PERF.get_global_file_iostat()
    GROUP by (filenum, dbid, spcid);


/* Lock*/
CREATE OR REPLACE VIEW DBE_PERF.locks AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_locks AS
  SELECT * FROM DBE_PERF.get_global_locks();

/* utility */
CREATE OR REPLACE VIEW DBE_PERF.replication_slots AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_replication_slots AS
  SELECT * FROM DBE_PERF.get_global_replication_slots();

CREATE OR REPLACE VIEW DBE_PERF.bgwriter_stat AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_bgwriter_stat AS
  SELECT * FROM DBE_PERF.get_global_bgwriter_stat();

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
          S.pid = W.sender_pid;

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

CREATE OR REPLACE VIEW DBE_PERF.global_replication_stat AS
  SELECT * FROM DBE_PERF.get_global_replication_stat();

/* transaction */
CREATE OR REPLACE VIEW DBE_PERF.transactions_running_xacts AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_transactions_running_xacts AS
  SELECT DISTINCT * from DBE_PERF.get_global_transactions_running_xacts();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_transactions_running_xacts AS
  SELECT DISTINCT * from DBE_PERF.get_summary_transactions_running_xacts();

CREATE OR REPLACE VIEW DBE_PERF.transactions_prepared_xacts AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM DBE_PERF.get_global_transactions_prepared_xacts();

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

CREATE OR REPLACE VIEW DBE_PERF.summary_transactions_prepared_xacts AS
  SELECT DISTINCT * FROM DBE_PERF.get_summary_transactions_prepared_xacts();

/* Query unique SQL*/
CREATE OR REPLACE VIEW DBE_PERF.statement AS
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

CREATE OR REPLACE VIEW DBE_PERF.summary_statement AS
  SELECT * FROM DBE_PERF.get_summary_statement();

CREATE OR REPLACE VIEW DBE_PERF.statement_count AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_statement_count AS
  SELECT * FROM DBE_PERF.get_global_statement_count();

CREATE OR REPLACE VIEW DBE_PERF.summary_statement_count AS
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
CREATE OR REPLACE VIEW DBE_PERF.config_settings AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_config_settings AS
  SELECT * FROM DBE_PERF.get_global_config_settings();

/* waits*/
CREATE OR REPLACE VIEW DBE_PERF.wait_events AS
  SELECT * FROM get_instr_wait_event(NULL);

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

CREATE OR REPLACE VIEW DBE_PERF.global_wait_events AS
  SELECT * FROM DBE_PERF.get_global_wait_events();

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
CREATE OR REPLACE VIEW DBE_PERF.statement_responsetime_percentile AS
  select * from DBE_PERF.get_statement_responsetime_percentile();

CREATE OR REPLACE VIEW DBE_PERF.user_login AS
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

CREATE OR REPLACE VIEW DBE_PERF.summary_user_login AS
  SELECT * FROM DBE_PERF.get_summary_user_login();

CREATE OR REPLACE VIEW DBE_PERF.class_vital_info AS
  SELECT
    C.oid AS relid,
    N.nspname AS schemaname,
    C.relname AS relname,
    C.relkind AS relkind
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i');

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

CREATE OR REPLACE  VIEW dbe_perf.global_ckpt_status AS
        SELECT node_name,ckpt_redo_point,ckpt_clog_flush_num,ckpt_csnlog_flush_num,ckpt_multixact_flush_num,ckpt_predicate_flush_num,ckpt_twophase_flush_num
        FROM pg_catalog.local_ckpt_stat();

CREATE OR REPLACE VIEW dbe_perf.global_double_write_status AS
    SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num,
           total_writes, low_threshold_writes, high_threshold_writes,
           total_pages, low_threshold_pages, high_threshold_pages
    FROM pg_catalog.local_double_write_stat();

CREATE OR REPLACE  VIEW dbe_perf.global_pagewriter_status AS
        SELECT node_name,pgwr_actual_flush_total_num,pgwr_last_flush_num,remain_dirty_page_num,queue_head_page_rec_lsn,queue_rec_lsn,current_xlog_insert_lsn,ckpt_redo_point
        FROM pg_catalog.local_pagewriter_stat();

CREATE OR REPLACE VIEW DBE_PERF.global_record_reset_time AS
  SELECT * FROM DBE_PERF.get_global_record_reset_time();

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

CREATE OR REPLACE VIEW dbe_perf.global_recovery_status AS
SELECT node_name, standby_node_name, source_ip, source_port, dest_ip, dest_port, current_rto, target_rto, current_sleep_time
FROM pg_catalog.local_recovery_status();

CREATE OR REPLACE VIEW DBE_PERF.local_threadpool_status AS
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

CREATE OR REPLACE VIEW DBE_PERF.global_threadpool_status AS
  SELECT * FROM DBE_PERF.global_threadpool_status();

CREATE OR REPLACE VIEW DBE_PERF.local_plancache_status AS
  SELECT * FROM plancache_status();

CREATE VIEW DBE_PERF.global_plancache_status AS
  SELECT * FROM pg_catalog.plancache_status();

CREATE OR REPLACE VIEW DBE_PERF.local_prepare_statement_status AS
  SELECT * FROM prepare_statement_status();

CREATE OR REPLACE VIEW DBE_PERF.local_plancache_clean AS
  SELECT * FROM plancache_clean();

CREATE OR REPLACE VIEW DBE_PERF.gs_slow_query_info AS
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
		S.data_io_time
FROM gs_wlm_session_query_info_all S where S.is_slow_query = 1;

CREATE OR REPLACE VIEW DBE_PERF.gs_slow_query_history AS
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
		S.data_io_time
FROM pg_stat_get_wlm_session_info(0) S where S.is_slow_query = 1;

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

CREATE OR REPLACE FUNCTION DBE_PERF.global_slow_query_info()
RETURNS setof DBE_PERF.gs_slow_query_info
AS $$
DECLARE
		row_data DBE_PERF.gs_slow_query_info%rowtype;
		row_name record;
		query_str text;
		query_str_nodes text;
		BEGIN
				--Get all the node names
				query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type=''C'' AND nodeis_active = true';
				FOR row_name IN EXECUTE(query_str_nodes) LOOP
						query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM DBE_PERF.gs_slow_query_info''';
						FOR row_data IN EXECUTE(query_str) LOOP
								return next row_data;
						END LOOP;
				END LOOP;
				return;
		END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION DBE_PERF.global_slow_query_info_bytime(text, TIMESTAMP, TIMESTAMP, int)
RETURNS setof DBE_PERF.gs_slow_query_info
AS $$
DECLARE
		row_data DBE_PERF.gs_slow_query_info%rowtype;
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
				query_str_cn := 'SELECT * FROM DBE_PERF.gs_slow_query_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
				FOR row_name IN EXECUTE(query_str_nodes) LOOP
						query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_cn||''';';
						FOR row_data IN EXECUTE(query_str) LOOP
								return next row_data;
						END LOOP;
				END LOOP;
				return;
		END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE VIEW DBE_PERF.global_slow_query_history AS
SELECT * FROM DBE_PERF.global_slow_query_history();

CREATE OR REPLACE VIEW DBE_PERF.global_slow_query_info AS
SELECT * FROM DBE_PERF.global_slow_query_info();

CREATE OR REPLACE VIEW DBE_PERF.local_active_session AS
  SELECT * FROM pg_catalog.get_local_active_session();

CREATE OR REPLACE VIEW DBE_PERF.wait_event_info AS
  SELECT * FROM get_wait_event_info();

grant select on all tables in schema DBE_PERF to public;

DROP INDEX IF EXISTS pg_catalog.pg_asp_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_asp;
DROP TABLE IF EXISTS pg_catalog.pg_asp;

CREATE EXTENSION security_plugin;

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_activity_timeout() cascade;
DROP FUNCTION IF EXISTS pg_catalog.global_stat_activity_timeout() cascade;

DROP FUNCTION IF EXISTS remove_create_partition_policy(name, name);
DROP FUNCTION IF EXISTS remove_drop_partition_policy(name, name);
DROP FUNCTION IF EXISTS is_super_user_or_sysadm(name);

DROP INDEX IF EXISTS pg_catalog.gs_asp_sample_time_index;

DO $$
DECLARE
  ans boolean;
  user_name text;
  query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    drop view if exists dbe_perf.wlm_user_resource_runtime cascade;
    drop function if exists dbe_perf.get_wlm_user_resource_runtime(OUT oid, OUT integer, OUT integer, OUT integer, OUT integer, OUT bigint, OUT bigint, OUT bigint, OUT bigint, OUT bigint, OUT bigint) cascade;

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
      FROM (select usename, (gs_wlm_user_resource_info(usename::cstring)).* from pg_user) T;

    select session_user into user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.wlm_user_resource_runtime TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON DBE_PERF.wlm_user_resource_runtime TO PUBLIC;
  end if;

END$$;

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

drop function if exists pg_catalog.login_audit_messages;
drop function if exists pg_catalog.login_audit_messages_pid;
drop function if exists pg_catalog.pg_query_audit(timestamptz, timestamptz) cascade;
drop function if exists pg_catalog.pg_query_audit(timestamptz, timestamptz, text) cascade;
drop function if exists pg_catalog.pgxc_query_audit(timestamptz, timestamptz) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3780;
create function pg_catalog.pg_query_audit(TIMESTAMPTZ, TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT type TEXT, OUT result TEXT, OUT userid TEXT, OUT username TEXT, OUT database TEXT, OUT client_conninfo TEXT, OUT object_name TEXT, OUT detail_info TEXT, OUT node_name TEXT, OUT thread_id TEXT, OUT local_port TEXT, OUT remote_port TEXT) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 100 STRICT as 'pg_query_audit';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3782;
create function pg_catalog.pg_query_audit(TIMESTAMPTZ, TIMESTAMPTZ, TEXT, OUT "time" TIMESTAMPTZ, OUT type TEXT, OUT result TEXT, OUT userid TEXT, OUT username TEXT, OUT database TEXT, OUT client_conninfo TEXT, OUT object_name TEXT, OUT detail_info TEXT, OUT node_name TEXT, OUT thread_id TEXT, OUT local_port TEXT, OUT remote_port TEXT) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE ROWS 100 STRICT as 'pg_query_audit';

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

--get audit log from all CNs
DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then

    DROP FUNCTION IF EXISTS dbe_perf.gs_stat_activity_timeout() cascade;
    DROP FUNCTION IF EXISTS dbe_perf.global_stat_activity_timeout() cascade;

    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4520;
    CREATE OR REPLACE FUNCTION dbe_perf.gs_stat_activity_timeout(IN timeout_threshold int4, OUT database name, OUT pid INT8, OUT sessionid INT8, OUT usesysid oid, OUT application_name text, OUT query text, OUT xact_start timestamptz, OUT query_start timestamptz, OUT query_id INT8)
    RETURNS SETOF RECORD LANGUAGE INTERNAL as 'gs_stat_activity_timeout';
  end if;
END$DO$;

DROP FUNCTION IF EXISTS pkg_service.job_submit(bigint, text, timestamp, text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4802;
CREATE OR REPLACE FUNCTION pkg_service.job_submit(
    in id bigint,
    in content text,
    in next_date timestamp without time zone default sysdate,
    in interval_time text default 'null',
    out job integer
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_submit';

    DROP VIEW IF EXISTS DBE_PERF.local_prepare_statement_status CASCADE;
    DROP FUNCTION IF EXISTS pg_catalog.prepare_statement_status() CASCADE;
    set local inplace_upgrade_next_system_object_oids = IUO_PROC,3959;
    CREATE OR REPLACE FUNCTION pg_catalog.prepare_statement_status(OUT nodename text, OUT cn_sess_id int8, OUT cn_node_id int4, OUT cn_time_line int4, OUT statement_name text, OUT refcount int4, OUT is_shared bool) RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_prepare_status';

    CREATE VIEW DBE_PERF.local_prepare_statement_status AS
        SELECT * FROM prepare_statement_status();

    DECLARE
    	    user_name text;
	    query_str text;
    BEGIN
	SELECT SESSION_USER INTO user_name;

	query_str := 'GRANT ALL ON TABLE DBE_PERF.local_prepare_statement_status TO ' || quote_ident(user_name) || ';';
	EXECUTE IMMEDIATE query_str;

    END;
    /

    GRANT SELECT ON TABLE DBE_PERF.local_prepare_statement_status TO PUBLIC;

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
    FROM pg_database D, pg_catalog.pg_stat_get_activity_with_conninfo(NULL) AS S,
            pg_authid U, pg_catalog.gs_wlm_session_respool(0) AS T
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
    FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_catalog.pg_stat_get_activity_ng(NULL) AS N,
            pg_authid U, pg_catalog.gs_wlm_session_respool(0) AS T
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
    FROM pg_database D, pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S,
            pg_authid AS U, pg_catalog.gs_wlm_session_respool(0) AS T
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
    FROM pg_catalog.pg_stat_get_wlm_statistics(NULL);

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
FROM pg_stat_activity_ng AS S, pg_catalog.pg_stat_get_wlm_session_iostat_info(0) AS T
WHERE S.pid = T.threadid;

CREATE OR REPLACE VIEW pg_catalog.gs_cluster_resource_info AS SELECT * FROM pg_catalog.pg_stat_get_wlm_node_resource_info(0);

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

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_session_info_all AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(0);

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
    FROM pg_catalog.pg_stat_get_cgroup_info(NULL);

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
FROM pg_catalog.gs_wlm_get_resource_pool_info(0) AS T, pg_resource_pool AS R
WHERE T.respool_oid = R.oid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_rebuild_user_resource_pool AS
        SELECT * FROM pg_catalog.gs_wlm_rebuild_user_resource_pool(0);

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
            S.pid = W.sender_pid;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_database AS
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

CREATE OR REPLACE VIEW pg_catalog.pg_stat_database_conflicts AS
    SELECT
            D.oid AS datid,
            D.datname AS datname,
            pg_catalog.pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
            pg_catalog.pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
            pg_catalog.pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
            pg_catalog.pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
            pg_catalog.pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock
    FROM pg_database D;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_user_functions AS
    SELECT
            P.oid AS funcid,
            N.nspname AS schemaname,
            P.proname AS funcname,
            pg_catalog.pg_stat_get_function_calls(P.oid) AS calls,
            pg_catalog.pg_stat_get_function_total_time(P.oid) AS total_time,
            pg_catalog.pg_stat_get_function_self_time(P.oid) AS self_time
    FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
    WHERE P.prolang != 12  -- fast check to eliminate built-in functions
          AND pg_catalog.pg_stat_get_function_calls(P.oid) IS NOT NULL;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_xact_user_functions AS
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

CREATE OR REPLACE VIEW pg_catalog.DV_SESSIONS AS
        SELECT
                sa.sessionid AS SID,
                0::integer AS SERIAL#,
                sa.usesysid AS USER#,
                ad.rolname AS USERNAME
        FROM pg_catalog.pg_stat_get_activity(NULL) AS sa
        LEFT JOIN pg_authid ad ON(sa.usesysid = ad.oid)
        WHERE sa.application_name <> 'JobScheduler';

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
    SELECT text(oid) FROM pg_authid WHERE rolname=SESSION_USER INTO user_id;
    SELECT SESSION_USER INTO user_name;
    SELECT CURRENT_DATABASE() INTO db_name;
    SELECT pg_backend_pid() INTO mybackendid;
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
LANGUAGE plpgsql NOT FENCED;

CREATE OR REPLACE VIEW pg_catalog.pg_thread_wait_status AS
    SELECT * FROM pg_catalog.pg_stat_get_status(NULL);

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

CREATE OR REPLACE VIEW pg_catalog.pg_get_invalid_backends AS
    SELECT
            C.pid,
            C.node_name,
            S.datname AS dbname,
            S.backend_start,
            S.query
    FROM pg_catalog.pg_pool_validate(false, ' ') AS C LEFT JOIN pg_stat_activity AS S
        ON (C.pid = S.sessionid);

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_statistics AS
SELECT t.*
FROM pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_operator_info(NULL) as t
where s.query_id = t.queryid;

CREATE OR REPLACE VIEW pg_catalog.gs_wlm_operator_history AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_operator_info(0);

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
                        FROM pg_catalog.pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0';

        query_str := 'SELECT * FROM pg_catalog.pg_stat_get_wlm_operator_info(1)';

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
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''installation'')';
            ELSE
                query_str := 'SELECT *,CAST(''' || row_name.group_name || ''' AS TEXT) AS nodegroup,CAST(''' || row_name.group_kind || ''' AS TEXT) AS group_kind FROM pg_catalog.gs_all_nodegroup_control_group_info(''' ||row_name.group_name||''')';
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
FROM pg_stat_activity AS s, pg_catalog.pg_stat_get_wlm_realtime_ec_operator_info(NULL) as t
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
FROM pg_catalog.pg_stat_get_wlm_ec_operator_info(0) where ec_operator > 0;

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
            lock_str = format('SELECT * FROM pg_catalog.pgxc_lock_for_sp_database(''%s'')', databse_name.datname);
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
            unlock_str = format('SELECT * FROM pg_catalog.pgxc_unlock_for_sp_database(''%s'')', databse_name.datname);
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
                        AS NUMERIC(5,2)) dirty_page_rate FROM pg_catalog.pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||') GROUP BY (relname,schemaname)) s
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
            EXECUTE 'select count(1) from ' || $1 into tolal_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''H'') as seqNum from ' || $1 ||
                            ') group by seqNum order by num DESC';
        else
            tolal_num = row_num;
            execute_query = 'select seqNum, count(1) as num
                            from (select pg_catalog.table_data_skewness(row(' || $2 ||'), ''H'') as seqNum from ' || $1 ||
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
WHERE S.sessionid = T.threadid;

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
WHERE S.sessionid = T.threadid;

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
WHERE S.sessionid = T.threadid;

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
        query_info_str := 'SELECT C.oid,C.reldeltarelid,C.parttype FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)  WHERE C.relname = $1 and N.nspname = $2';
        FOR row_info_data IN EXECUTE query_info_str USING rel, schema_name LOOP
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
            query_part_str := 'SELECT relname,reldeltarelid from pg_partition where parentid = '||row_info_data.oid|| 'and relname <> $1';
            FOR row_part_info IN EXECUTE query_part_str USING rel LOOP
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

CREATE OR REPLACE FUNCTION pg_catalog.get_delta_info(IN rel TEXT, OUT part_name TEXT, OUT total_live_tuple INT8, OUT total_data_size INT8, OUT max_blockNum INT8)
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    BEGIN
        query_str := 'select part_name, sum(live_tuple) total_live_tuple, sum(data_size) total_data_size, max(blocknum) max_blocknum from (select * from pg_catalog.pgxc_get_delta_info(%L)) group by part_name order by max_blocknum desc';
        FOR row_data IN EXECUTE format(query_str, rel) LOOP
            part_name := row_data.part_name;
            total_live_tuple := row_data.total_live_tuple;
            total_data_size := row_data.total_data_size;
            max_blockNum := row_data.max_blocknum;
            return next;
        END LOOP;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP EXTENSION IF EXISTS security_plugin CASCADE;

CREATE EXTENSION IF NOT EXISTS security_plugin;

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
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE pg_authid.rolname = current_user
    OR (SELECT rolcreaterole FROM pg_authid WHERE pg_authid.rolname = current_user)
    OR (SELECT rolsystemadmin FROM pg_authid WHERE pg_authid.rolname = current_user);

GRANT SELECT ON TABLE pg_catalog.pg_roles TO PUBLIC;

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
                    FROM pg_catalog.pg_authid
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
    LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relowner = (SELECT oid FROM pg_authid WHERE rolname=current_user)
    OR (SELECT rolsystemadmin FROM pg_authid WHERE rolname=current_user);

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
    FROM pg_authid AS S, gs_wlm_get_user_info(NULL) AS T, pg_resource_pool AS R
      WHERE S.oid = T.userid AND T.rpoid = R.oid;

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE dbe_perf.wlm_user_resource_config TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE dbe_perf.wlm_user_resource_config TO PUBLIC;
  end if;
END $DO$;

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
            S.pid = W.pid;

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
              S.pid = W.pid;
   end if;
END$DO$;

create extension if not exists gsredistribute;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_stat_dirty_tables(in dirty_percent int4, in n_tuples int4,in schema text, out relid oid, out relname name, out schemaname name, out n_tup_ins int8, out n_tup_upd int8, out n_tup_del int8, out n_live_tup int8, out n_dead_tup int8, out dirty_page_rate numeric(5,2))
RETURNS setof record
AS $$
DECLARE
    query_str text;
    row_data record;
    flag boolean;
    special text := '[;|-]';
    BEGIN
        EXECUTE IMMEDIATE 'SELECT regexp_like(:1,:2);' INTO flag USING IN $3, IN special;
        IF flag = true THEN
            raise WARNING 'illegal character entered for function';
            RETURN;
        END IF;
        query_str := 'SELECT oid relid, s.relname,s.schemaname,s.n_tup_ins,s.n_tup_upd,s.n_tup_del,s.n_live_tup,s.n_dead_tup,s.dirty_page_rate
                        FROM pg_class p,
                        (SELECT  relname, schemaname, SUM(n_tup_ins) n_tup_ins, SUM(n_tup_upd) n_tup_upd, SUM(n_tup_del) n_tup_del, SUM(n_live_tup) n_live_tup, SUM(n_dead_tup) n_dead_tup, CAST((SUM(n_dead_tup) / SUM(n_dead_tup + n_live_tup + 0.00001) * 100)
                        AS NUMERIC(5,2)) dirty_page_rate FROM pg_catalog.pgxc_stat_dirty_tables('||dirty_percent||','||n_tuples||',$1) GROUP BY (relname,schemaname)) s
                        WHERE p.relname = s.relname AND p.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname) ORDER BY dirty_page_rate DESC';
        FOR row_data IN EXECUTE(query_str) using schema LOOP
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

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
CREATE FUNCTION pg_catalog.pg_stat_get_activity(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT srespool NAME) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT srespool NAME, OUT connection_info TEXT) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity_with_conninfo';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_session_wlmstat(INT4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3502;
CREATE FUNCTION pg_catalog.pg_stat_get_session_wlmstat(INT4, OUT datid OID, OUT threadid INT8, OUT sessionid INT8, OUT threadpid INT4, OUT usesysid OID, OUT appname TEXT, OUT query TEXT,
OUT priority INT8, OUT block_time INT8, OUT elapsed_time INT8, OUT total_cpu_time INT8, OUT skew_percent INT4, OUT statement_mem INT4, OUT active_points INT4, OUT dop_value INT4, OUT current_cgroup TEXT,
OUT current_status TEXT, OUT enqueue_state TEXT, OUT attribute TEXT, OUT is_plana BOOL, OUT node_group TEXT, OUT srespool NAME) RETURNS
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
            WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
            ELSE S.srespool
            END AS resource_pool,
        S.query_id,
        S.query,
        S.connection_info
FROM pg_database D, pg_catalog.pg_stat_get_activity_with_conninfo(NULL) AS S,
        pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;

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
FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_catalog.pg_stat_get_activity_ng(NULL) AS N,
        pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
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
            WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
            ELSE S.srespool
            END AS resource_pool,
        S.query,
        S.is_plana,
        S.node_group
FROM pg_database D, pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S,
        pg_authid AS U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;

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
                WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                ELSE S.srespool
                END AS resource_pool,
            S.query_id,
            S.query
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;


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

    CREATE OR REPLACE VIEW DBE_PERF.wlm_workload_runtime AS
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
    FROM pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U, gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
          S.usesysid = U.oid;
  end if;
END$DO$;

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

DO $DO$
DECLARE
  ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    DROP FUNCTION IF EXISTS dbe_perf.track_memory_context(IN contexts text) cascade;
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3988;
    CREATE OR REPLACE FUNCTION dbe_perf.track_memory_context(IN contexts text)
    RETURNS BOOLEAN LANGUAGE INTERNAL as 'track_memory_context';

    DROP FUNCTION IF EXISTS dbe_perf.track_memory_context_detail(OUT context_name text, OUT file text, OUT line int, OUT size bigint) cascade;
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3990;
    CREATE OR REPLACE FUNCTION dbe_perf.track_memory_context_detail(OUT context_name text, OUT file text, OUT line int, OUT size bigint)
    RETURNS SETOF RECORD LANGUAGE INTERNAL as 'track_memory_context_detail';

    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;
    CREATE OR REPLACE VIEW dbe_perf.track_memory_context_detail AS
      SELECT * FROM dbe_perf.track_memory_context_detail();
  end if;
END$DO$;

REVOKE ALL on table pg_catalog.gs_wlm_session_query_info_all FROM public;
REVOKE ALL on table pg_catalog.gs_wlm_operator_info FROM public;
REVOKE ALL on table pg_catalog.gs_wlm_ec_operator_info FROM public;
revoke all on pg_catalog.gs_wlm_session_info from public;
revoke all on pg_catalog.gs_wlm_user_session_info from public;
revoke all on pg_catalog.gs_wlm_workload_records from public;

DROP FUNCTION IF EXISTS pg_catalog.local_bgwriter_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4373;
CREATE FUNCTION pg_catalog.local_bgwriter_stat
(
OUT node_name pg_catalog.text,
OUT bgwr_actual_flush_total_num pg_catalog.int8,
OUT bgwr_last_flush_num pg_catalog.int4,
OUT candidate_slots pg_catalog.int4,
OUT get_buffer_from_list pg_catalog.int8,
OUT get_buf_clock_sweep pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_bgwriter_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_bgwriter_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4374;
CREATE FUNCTION pg_catalog.remote_bgwriter_stat
(
OUT node_name pg_catalog.text,
OUT bgwr_actual_flush_total_num pg_catalog.int8,
OUT bgwr_last_flush_num pg_catalog.int4,
OUT candidate_slots pg_catalog.int4,
OUT get_buffer_from_list pg_catalog.int8,
OUT get_buf_clock_sweep pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_bgwriter_stat';

CREATE OR REPLACE VIEW DBE_PERF.global_get_bgwriter_status AS
SELECT node_name,bgwr_actual_flush_total_num,bgwr_last_flush_num,candidate_slots,get_buffer_from_list,get_buf_clock_sweep
FROM pg_catalog.remote_bgwriter_stat()
UNION ALL
SELECT node_name,bgwr_actual_flush_total_num,bgwr_last_flush_num,candidate_slots,get_buffer_from_list,get_buf_clock_sweep
FROM pg_catalog.local_bgwriter_stat();

REVOKE ALL on DBE_PERF.global_get_bgwriter_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_get_bgwriter_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_get_bgwriter_status TO PUBLIC;

-- 1. add system views
CREATE VIEW pg_catalog.gs_matviews AS
    SELECT
        N.nspname AS schemaname,
        C.relname AS matviewname,
        pg_get_userbyid(C.relowner) AS matviewowner,
        T.spcname AS tablespace,
        C.relhasindex AS hasindexes,
        pg_get_viewdef(C.oid) AS definition
    FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
    WHERE C.relkind = 'm';

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
        OUT net_send_time bigint,
        OUT data_io_time bigint
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
    ADD COLUMN snap_net_send_info text,
    ADD COLUMN snap_net_recv_info text,
    ADD COLUMN snap_net_stream_send_info text,
    ADD COLUMN snap_net_stream_recv_info text;
  end if;
END$DO$;

-- ----------------------------------------------------------------
-- upgrade pg_pooler_status
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3955;
CREATE FUNCTION pg_catalog.pg_stat_get_pooler_status(OUT database_name text, OUT user_name text, OUT tid int8, OUT pgoptions text, OUT node_oid int8, OUT in_use boolean, OUT session_params text, OUT fdsock int8, OUT remote_pid int8, OUT used_count int8) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'pg_stat_get_pooler_status';

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_gs_asp() CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.global_active_session CASCADE;
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

set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5731;
CREATE OR REPLACE FUNCTION pg_catalog.working_version_num
  ()
RETURNS int4 LANGUAGE INTERNAL VOLATILE NOT FENCED as 'working_version_num';

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
  OUT fastpath pg_catalog.bool,
  OUT locktag pg_catalog.text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1000 VOLATILE STRICT as 'pg_lock_status';
CREATE OR REPLACE VIEW pg_catalog.pg_locks AS
    SELECT * FROM pg_catalog.pg_lock_status() AS L;

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
  OUT wait_event pg_catalog.text,
  OUT locktag pg_catalog.text,
  OUT lockmode pg_catalog.text,
  OUT block_sessionid pg_catalog.int8)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1000 VOLATILE STRICT as 'pg_stat_get_status';
CREATE OR REPLACE VIEW pg_catalog.pg_thread_wait_status AS
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
  OUT wait_event pg_catalog.text,
  OUT locktag pg_catalog.text,
  OUT lockmode pg_catalog.text,
  OUT block_sessionid pg_catalog.int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL COST 100 ROWS 1000 AS 'pgxc_stat_get_status';
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
   OUT unique_query text,
   OUT locktag text,
   OUT lockmode text,
   OUT block_sessionid bigint,
   OUT wait_status text)
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

      CREATE VIEW DBE_PERF.global_thread_wait_status AS
        SELECT * FROM DBE_PERF.get_global_thread_wait_status();

      CREATE VIEW DBE_PERF.local_active_session AS
        WITH RECURSIVE
        las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid, tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id, user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status) as (select t.* from get_local_active_session() as t),
        tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid, tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id, user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, final_block_sessionid, level, head) AS(
            SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, array_append('{}', las.sessionid) AS head FROM las
          union all
            SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid, tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id, tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, las.block_sessionid AS final_block_sessionid, tt.level + 1 AS level, array_append(tt.head, las.sessionid) AS head
            FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
            WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0) AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
        SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid, tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id, user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status FROM tt
        WHERE level = (SELECT MAX(level) FROM tt t1 WHERE t1.sessionid = tt.sessionid);

      SELECT SESSION_USER INTO user_name;
      global_query_str := 'GRANT ALL ON TABLE DBE_PERF.locks TO ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE global_query_str;
      global_query_str := 'GRANT ALL ON TABLE DBE_PERF.global_thread_wait_status TO ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE global_query_str;
      global_query_str := 'GRANT ALL ON TABLE DBE_PERF.thread_wait_status TO ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE global_query_str;
      global_query_str := 'GRANT ALL ON TABLE DBE_PERF.local_active_session TO ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE global_query_str;

      GRANT SELECT ON DBE_PERF.locks TO public;
      GRANT SELECT ON DBE_PERF.global_thread_wait_status TO public;
      GRANT SELECT ON DBE_PERF.thread_wait_status TO public;
      GRANT SELECT ON DBE_PERF.local_active_session TO public;

    end if;
END $DO$;

set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5730;
CREATE OR REPLACE FUNCTION pg_catalog.locktag_decode
  (IN text)
RETURNS text LANGUAGE INTERNAL VOLATILE NOT FENCED as 'locktag_decode';

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
    ADD COLUMN snap_locktag text,
    ADD COLUMN snap_lockmode text,
    ADD COLUMN snap_block_sessionid int8;
  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.pg_get_flush_lsn();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3316;
CREATE FUNCTION pg_catalog.pg_get_flush_lsn
(
OUT flush_lsn pg_catalog.text
) RETURNS pg_catalog.text LANGUAGE INTERNAL STABLE as 'pg_get_flush_lsn';

DROP FUNCTION IF EXISTS pg_catalog.pg_get_sync_flush_lsn();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3317;
CREATE FUNCTION pg_catalog.pg_get_sync_flush_lsn
(
OUT sync_flush_lsn pg_catalog.text
) RETURNS pg_catalog.text LANGUAGE INTERNAL STABLE as 'pg_get_sync_flush_lsn';

DROP EXTENSION IF EXISTS gsredistribute CASCADE;

CREATE EXTENSION gsredistribute;

DROP FUNCTION IF EXISTS pg_catalog.gs_get_control_group_info() CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_control_group_info
(
out group_name text,
out group_type text,
out gid bigint,
out classgid bigint,
out class text,
out group_workload text,
out shares bigint,
out limits bigint,
out wdlevel bigint,
out cpucores text,
out nodegroup text,
out group_kind text
)
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
            nodegroup := row_name.group_name;
            group_kind := row_name.group_kind;
            FOR row_data IN EXECUTE(query_str) LOOP
                group_name := row_data.name;
                group_type := row_data."type";
                gid := row_data.gid;
                classgid := row_data.classgid;
                class := row_data.class;
                group_workload := row_data.workload;
                shares := row_data.shares;
                limits := row_data.limits;
                wdlevel:= row_data.wdlevel;
                cpucores := row_data.cpucores;
                return next;
            END LOOP;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

-- the view for function gs_get_control_group_info.
DROP VIEW IF EXISTS pg_catalog.gs_get_control_group_info CASCADE;
CREATE VIEW pg_catalog.gs_get_control_group_info AS
  SELECT * from gs_get_control_group_info();


DO $DO$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_wlm_controlgroup_ng_config() CASCADE;
        DROP VIEW IF EXISTS DBE_PERF.wlm_controlgroup_ng_config;

        CREATE OR REPLACE FUNCTION DBE_PERF.get_wlm_controlgroup_ng_config
        (
        out group_name text,
        out group_type text,
        out gid bigint,
        out classgid bigint,
        out class text,
        out group_workload text,
        out shares bigint,
        out limits bigint,
        out wdlevel bigint,
        out cpucores text,
        out nodegroup text,
        out group_kind text
        )
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
                    nodegroup := row_name.group_name;
                    group_kind := row_name.group_kind;
                    FOR row_data IN EXECUTE(query_str) LOOP
                        group_name := row_data.name;
                        group_type := row_data."type";
                        gid := row_data.gid;
                        classgid := row_data.classgid;
                        class := row_data.class;
                        group_workload := row_data.workload;
                        shares := row_data.shares;
                        limits := row_data.limits;
                        wdlevel:= row_data.wdlevel;
                        cpucores := row_data.cpucores;
                        return next;
                    END LOOP;
                END LOOP;
                return;
            END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        -- the view for function gs_get_control_group_info.
        CREATE OR REPLACE VIEW DBE_PERF.wlm_controlgroup_ng_config AS
            SELECT * FROM DBE_PERF.get_wlm_controlgroup_ng_config();
    end if;
END $DO$;

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_user_info(input integer, OUT userid oid, OUT sysadmin boolean, OUT rpoid oid, OUT parentid oid, OUT totalspace bigint, OUT spacelimit bigint, OUT childcount integer, OUT childlist text) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_wlm_user_info';

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_resource_pool_info(input integer, OUT respool_oid oid, OUT ref_count integer, OUT active_points integer, OUT running_count integer, OUT waiting_count integer, OUT iops_limits integer, OUT io_priority integer) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_resource_pool_info';

CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_get_workload_records(input integer, OUT node_idx oid, OUT query_pid bigint, OUT start_time bigint, OUT memory integer, OUT actpts integer, OUT maxpts integer, OUT priority integer, OUT resource_pool text, OUT node_name text, OUT queue_type text, OUT node_group text) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_workload_records';

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
                            c.relfilenode = ' || quote_literal(relfile_node) || '
                        )
                        , ''null''
                    ) relname';
    FOR relname IN EXECUTE(query_str) LOOP
        return relname;
    END LOOP;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP EXTENSION IF EXISTS security_plugin CASCADE;

CREATE EXTENSION IF NOT EXISTS security_plugin;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4805;

DROP FUNCTION IF EXISTS pg_catalog.delta(Numeric) CASCADE;

CREATE FUNCTION pg_catalog.delta(Numeric)
RETURNS Numeric
LANGUAGE INTERNAL NOT FENCED STRICT as 'window_delta';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2022;
CREATE FUNCTION pg_catalog.pg_stat_get_activity(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT srespool NAME) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_activity_with_conninfo(INT8) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4212;
CREATE FUNCTION pg_catalog.pg_stat_get_activity_with_conninfo(INT8, OUT datid OID, OUT pid INT8, OUT sessionid INT8, OUT usesysid OID, OUT application_name TEXT, OUT state TEXT,
OUT query TEXT, OUT waiting BOOL, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT state_change timestamptz, OUT client_addr INET,
OUT client_hostname TEXT, OUT client_port INT4, OUT enqueue TEXT, OUT query_id INT8, OUT connection_info TEXT, OUT srespool NAME) RETURNS
SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_activity_with_conninfo';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_session_wlmstat(INT4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3502;
CREATE FUNCTION pg_catalog.pg_stat_get_session_wlmstat(INT4, OUT datid OID, OUT threadid INT8, OUT sessionid INT8, OUT threadpid INT4, OUT usesysid OID, OUT appname TEXT, OUT query TEXT,
OUT priority INT8, OUT block_time INT8, OUT elapsed_time INT8, OUT total_cpu_time INT8, OUT skew_percent INT4, OUT statement_mem INT4, OUT active_points INT4, OUT dop_value INT4, OUT current_cgroup TEXT,
OUT current_status TEXT, OUT enqueue_state TEXT, OUT attribute TEXT, OUT is_plana BOOL, OUT node_group TEXT, OUT srespool NAME) RETURNS
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
            WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
            ELSE S.srespool
            END AS resource_pool,
        S.query_id,
        S.query,
        S.connection_info
FROM pg_database D, pg_catalog.pg_stat_get_activity_with_conninfo(NULL) AS S,
        pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;

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
FROM pg_database D, pg_catalog.pg_stat_get_activity(NULL) AS S, pg_catalog.pg_stat_get_activity_ng(NULL) AS N,
        pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid AND
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
            WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
            ELSE S.srespool
            END AS resource_pool,
        S.query,
        S.is_plana,
        S.node_group
FROM pg_database D, pg_catalog.pg_stat_get_session_wlmstat(NULL) AS S,
        pg_authid AS U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;

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
                WHEN S.srespool = 'unknown' THEN (U.rolrespool) :: name
                ELSE S.srespool
                END AS resource_pool,
            S.query_id,
            S.query
    FROM pg_database D, pg_stat_get_activity(NULL) AS S, pg_authid U
    WHERE S.datid = D.oid AND
          S.usesysid = U.oid;


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

    CREATE OR REPLACE VIEW DBE_PERF.wlm_workload_runtime AS
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
    FROM pg_stat_get_session_wlmstat(NULL) AS S, pg_authid U, gs_wlm_get_workload_records(0) P
    WHERE P.query_pid = S.threadpid AND
          S.usesysid = U.oid;
  end if;
END$DO$;

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

DROP FUNCTION IF EXISTS pg_catalog.pg_cbm_rotate_file(in rotate_lsn text);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4660;
CREATE FUNCTION pg_catalog.pg_cbm_rotate_file
(
rotate_lsn pg_catalog.text
) RETURNS void LANGUAGE INTERNAL STABLE as 'pg_cbm_rotate_file';

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

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_session_respool() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5021;
CREATE OR REPLACE FUNCTION pg_catalog.gs_wlm_session_respool(input bigint, OUT datid oid, OUT threadid bigint, OUT sessionid bigint, OUT threadpid integer, OUT usesysid oid, OUT cgroup text, OUT session_respool name) RETURNS SETOF RECORD LANGUAGE INTERNAL as 'pg_stat_get_session_respool';

DROP INDEX IF EXISTS pg_catalog.gs_matview_matviewid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_matviewid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_relid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_mlogid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_bothid_index;

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

DO $DO$
DECLARE
    ans boolean;
    user_name text;
    global_query_str text;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
      DROP VIEW IF EXISTS DBE_PERF.local_active_session CASCADE;
      CREATE VIEW DBE_PERF.local_active_session AS
        WITH RECURSIVE
          las(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
              tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
              user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status)
            AS (select t.* from get_local_active_session() as t),
          tt(sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
             tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
             user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, wait_status, final_block_sessionid, level, head)
            AS(SELECT las.*, las.block_sessionid AS final_block_sessionid, 1 AS level, array_append('{}', las.sessionid) AS head FROM las
              UNION ALL
               SELECT tt.sampleid, tt.sample_time, tt.need_flush_sample, tt.databaseid, tt.thread_id, tt.sessionid, tt.start_time, tt.event, tt.lwtid, tt.psessionid,
                      tt.tlevel, tt.smpid, tt.userid, tt.application_name, tt.client_addr, tt.client_hostname, tt.client_port, tt.query_id, tt.unique_query_id,
                      tt.user_id, tt.cn_id, tt.unique_query, tt.locktag, tt.lockmode, tt.block_sessionid, tt.wait_status, las.block_sessionid AS final_block_sessionid,
                      tt.level + 1 AS level, array_append(tt.head, las.sessionid) AS head
               FROM tt INNER JOIN las ON tt.final_block_sessionid = las.sessionid
               WHERE las.sampleid = tt.sampleid AND (las.block_sessionid IS NOT NULL OR las.block_sessionid != 0)
                 AND las.sessionid != all(head) AND las.sessionid != las.block_sessionid)
        SELECT sampleid, sample_time, need_flush_sample, databaseid, thread_id, sessionid, start_time, event, lwtid, psessionid,
               tlevel, smpid, userid, application_name, client_addr, client_hostname, client_port, query_id, unique_query_id,
               user_id, cn_id, unique_query, locktag, lockmode, block_sessionid, final_block_sessionid, wait_status FROM tt
          WHERE level = (SELECT MAX(level) FROM tt t1 WHERE t1.sampleid = tt.sampleid AND t1.sessionid = tt.sessionid);

      SELECT SESSION_USER INTO user_name;
      global_query_str := 'GRANT ALL ON TABLE DBE_PERF.local_active_session TO ' || quote_ident(user_name) || ';';
      EXECUTE IMMEDIATE global_query_str;

      GRANT SELECT ON DBE_PERF.local_active_session TO public;

    end if;
END $DO$;

--clob cast text
DROP CAST IF EXISTS (TEXT AS CLOB) CASCADE;
DROP CAST IF EXISTS (CLOB AS TEXT) CASCADE;
CREATE CAST (TEXT AS CLOB) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (CLOB AS TEXT) WITHOUT FUNCTION AS IMPLICIT;

 /* text to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (TEXT) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(TEXT)
RETURNS CLOB
AS $$ select $1 $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* char to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (CHAR) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(CHAR)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* varchar to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (VARCHAR) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(VARCHAR)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* nvarchar2 to clob */
DROP FUNCTION IF EXISTS pg_catalog.to_clob (NVARCHAR2) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.to_clob(NVARCHAR2)
RETURNS CLOB
AS $$ select CAST($1 AS TEXT) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

/* clob to regclass */
DROP CAST IF EXISTS (clob AS regclass) CASCADE;
CREATE CAST (clob AS regclass) WITH FUNCTION pg_catalog.regclass(text) AS IMPLICIT;
/* clob to bpchar */
DROP CAST IF EXISTS (clob AS bpchar) CASCADE;
CREATE CAST (clob AS bpchar) WITHOUT FUNCTION AS IMPLICIT;
/* clob to varchar */
DROP CAST IF EXISTS (clob AS varchar) CASCADE;
CREATE CAST (clob AS varchar) WITHOUT FUNCTION AS IMPLICIT;
/* clob to nvarchar2 */
DROP CAST IF EXISTS (clob AS nvarchar2) CASCADE;
CREATE CAST (clob AS nvarchar2) WITHOUT FUNCTION AS IMPLICIT;
/* bpchar to clob */
DROP CAST IF EXISTS (bpchar AS clob) CASCADE;
CREATE CAST (bpchar AS clob) WITH FUNCTION pg_catalog.text(bpchar) AS IMPLICIT;
/* varchar to clob */
DROP CAST IF EXISTS (varchar AS clob) CASCADE;
CREATE CAST (varchar AS clob) WITHOUT FUNCTION AS IMPLICIT;
/* nvarchar2 to clob */
DROP CAST IF EXISTS (nvarchar2 AS clob) CASCADE;
CREATE CAST (nvarchar2 AS clob) WITHOUT FUNCTION AS IMPLICIT;

/* char to clob */
DROP CAST IF EXISTS ("char" AS clob) CASCADE;
CREATE CAST ("char" AS clob) WITH FUNCTION pg_catalog.text("char") AS IMPLICIT;
/* name to clob */
DROP CAST IF EXISTS (name AS clob) CASCADE;
CREATE CAST (name AS clob) WITH FUNCTION pg_catalog.text(name) AS IMPLICIT;
/* clob to char */
DROP CAST IF EXISTS (clob AS "char") CASCADE;
CREATE CAST (clob AS "char") WITH FUNCTION pg_catalog.char(text) AS ASSIGNMENT;
/* clob to name */
DROP CAST IF EXISTS (clob AS name) CASCADE;
CREATE CAST (clob AS name) WITH FUNCTION pg_catalog.name(text) AS IMPLICIT;

/* pg_node_tree to clob */
DROP CAST IF EXISTS (pg_node_tree AS clob) CASCADE;
CREATE CAST (pg_node_tree AS clob) WITHOUT FUNCTION AS IMPLICIT;

/* clob to timestamp */
DROP CAST IF EXISTS (clob AS timestamp) CASCADE;
CREATE CAST (clob AS timestamp) WITH FUNCTION pg_catalog.text_timestamp(text) AS IMPLICIT;

/* cidr to clob*/
DROP CAST IF EXISTS (cidr AS clob) CASCADE;
CREATE CAST (cidr AS clob) WITH FUNCTION pg_catalog.text(inet) AS ASSIGNMENT;
/* inet to clob */
DROP CAST IF EXISTS (inet AS clob) CASCADE;
CREATE CAST (inet AS clob) WITH FUNCTION pg_catalog.text(inet) AS ASSIGNMENT;
/* bool to clob */
DROP CAST IF EXISTS (bool AS clob) CASCADE;
CREATE CAST (bool AS clob) WITH FUNCTION pg_catalog.text(bool) AS ASSIGNMENT;
/* xml to clob */
DROP CAST IF EXISTS (xml AS clob) CASCADE;
CREATE CAST (xml AS clob) WITHOUT FUNCTION AS ASSIGNMENT;
/* clob to xml */
DROP CAST IF EXISTS (clob AS xml) CASCADE;
CREATE CAST (clob AS xml) WITH FUNCTION pg_catalog.xml(text);

/* date to clob */
--DROP CAST IF EXISTS (date AS clob) CASCADE;
--CREATE CAST (date AS clob) WITH FUNCTION pg_catalog.date_text("date") AS IMPLICIT;

/* clob to date */
--DROP CAST IF EXISTS (clob AS date) CASCADE;
--CREATE CAST (clob AS date) WITH FUNCTION pg_catalog.text_date(text) AS IMPLICIT;

/* int1 to clob */
DROP CAST IF EXISTS (int1 AS clob) CASCADE;
CREATE CAST (int1 AS clob) WITH FUNCTION pg_catalog.int1_text(int1) AS IMPLICIT;
/* int2 to clob */
DROP CAST IF EXISTS (int2 AS clob) CASCADE;
CREATE CAST (int2 AS clob) WITH FUNCTION pg_catalog.int2_text(int2) AS IMPLICIT;
/* int4 to clob */
DROP CAST IF EXISTS (int4 AS clob) CASCADE;
CREATE CAST (int4 AS clob) WITH FUNCTION pg_catalog.int4_text(int4) AS IMPLICIT;
/* int8 to clob */
DROP CAST IF EXISTS (int8 AS clob) CASCADE;
CREATE CAST (int8 AS clob) WITH FUNCTION pg_catalog.int8_text(int8) AS IMPLICIT;

/* float4 to clob */
DROP CAST IF EXISTS (float4 AS clob) CASCADE;
CREATE CAST (float4 AS clob) WITH FUNCTION pg_catalog.float4_text(float4) AS IMPLICIT;
/* float8 to clob */
DROP CAST IF EXISTS (float8 AS clob) CASCADE;
CREATE CAST (float8 AS clob) WITH FUNCTION pg_catalog.float8_text(float8) AS IMPLICIT;
/* numeric to clob */
DROP CAST IF EXISTS (numeric AS clob) CASCADE;
CREATE CAST (numeric AS clob) WITH FUNCTION pg_catalog.numeric_text(numeric) AS IMPLICIT;

/* timestamptz to clob */
DROP CAST IF EXISTS (timestamptz AS clob) CASCADE;
CREATE CAST (timestamptz AS clob) WITH FUNCTION pg_catalog.timestampzone_text(timestamptz) AS IMPLICIT;
/* timestamp to clob */
DROP CAST IF EXISTS (timestamp AS clob) CASCADE;
CREATE CAST (timestamp AS clob) WITH FUNCTION pg_catalog.timestamp_text(timestamp) AS IMPLICIT;

/* clob to int1 */
DROP CAST IF EXISTS (clob AS int1) CASCADE;
CREATE CAST (clob AS int1) WITH FUNCTION pg_catalog.text_int1(text) AS IMPLICIT;
/* clob to int2*/
DROP CAST IF EXISTS (clob AS int2) CASCADE;
CREATE CAST (clob AS int2) WITH FUNCTION pg_catalog.text_int2(text) AS IMPLICIT;
/* clob to int4*/
DROP CAST IF EXISTS (clob AS int4) CASCADE;
CREATE CAST (clob AS int4) WITH FUNCTION pg_catalog.text_int4(text) AS IMPLICIT;
/* clob to int8*/
DROP CAST IF EXISTS (clob AS int8) CASCADE;
CREATE CAST (clob AS int8) WITH FUNCTION pg_catalog.text_int8(text) AS IMPLICIT;
/* clob to float4*/
DROP CAST IF EXISTS (clob AS float4) CASCADE;
CREATE CAST (clob AS float4) WITH FUNCTION pg_catalog.text_float4(text) AS IMPLICIT;
/* clob to float8*/
DROP CAST IF EXISTS (clob AS float8) CASCADE;
CREATE CAST (clob AS float8) WITH FUNCTION pg_catalog.text_float8(text) AS IMPLICIT;
/* clob to numeric*/
DROP CAST IF EXISTS (clob AS numeric) CASCADE;
CREATE CAST (clob AS numeric) WITH FUNCTION pg_catalog.text_numeric(text) AS IMPLICIT;

DROP FUNCTION IF EXISTS pg_catalog.median_float8_finalfn(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5557;
CREATE FUNCTION pg_catalog.median_float8_finalfn(internal)
RETURNS float8 LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE as 'median_float8_finalfn';

DROP FUNCTION IF EXISTS pg_catalog.median_interval_finalfn(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5558;
CREATE FUNCTION pg_catalog.median_interval_finalfn(internal)
RETURNS interval LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE as 'median_interval_finalfn';

DROP FUNCTION IF EXISTS pg_catalog.median_transfn(internal, "any") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5559;
CREATE FUNCTION pg_catalog.median_transfn(internal, "any")
RETURNS internal LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE as 'median_transfn';

drop aggregate if exists pg_catalog.median(float8);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5555;
create aggregate pg_catalog.median(float8) (SFUNC=median_transfn, STYPE= internal, finalfunc = median_float8_finalfn);

drop aggregate if exists pg_catalog.median(interval);
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5556;
create aggregate pg_catalog.median(interval) (SFUNC=median_transfn, STYPE= internal, finalfunc = median_interval_finalfn);

DROP FUNCTION IF EXISTS pg_catalog.pg_get_gtt_relstats() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3596;
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_gtt_relstats(relid oid, OUT relfilenode oid, OUT relpages int4, OUT reltuples float4, OUT relallvisible int4, OUT relfrozenxid xid, OUT relminmxid xid)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_get_gtt_relstats';

DROP FUNCTION IF EXISTS pg_catalog.pg_get_gtt_statistics() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3597;
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_gtt_statistics(relid oid, att int4, x anyelement, OUT starelid oid, OUT starelkind "char", OUT staattnum int2, OUT stainherit bool, OUT stanullfrac float4, OUT stawidth int4, OUT stadistinct float4, OUT stakind1 int2, OUT stakind2 int2, OUT stakind3 int2, OUT stakind4 int2, OUT stakind5 int2, OUT staop1 oid, OUT staop2 oid, OUT staop3 oid, OUT staop4 oid, OUT staop5 oid, OUT stanumbers1 _float4, OUT stanumbers2 _float4, OUT stanumbers3 _float4, OUT stanumbers4 _float4, OUT stanumbers5 _float4, OUT stavalues1 anyarray, OUT stavalues2 anyarray, OUT stavalues3 anyarray, OUT stavalues4 anyarray, OUT stavalues5 anyarray, OUT stadndistinct float4, OUT staextinfo text)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_get_gtt_statistics';

DROP FUNCTION IF EXISTS pg_catalog.pg_gtt_attached_pid() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3598;
CREATE OR REPLACE FUNCTION pg_catalog.pg_gtt_attached_pid(relid oid, OUT relid oid, OUT pid int8)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_gtt_attached_pid';

DROP FUNCTION IF EXISTS pg_catalog.pg_list_gtt_relfrozenxids() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3599;
CREATE OR REPLACE FUNCTION pg_catalog.pg_list_gtt_relfrozenxids(OUT pid int8, OUT relfrozenxid xid)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_list_gtt_relfrozenxids';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 0;

DROP VIEW IF EXISTS pg_catalog.pg_gtt_relstats cascade;
CREATE VIEW pg_catalog.pg_gtt_relstats WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    (select relfilenode from pg_get_gtt_relstats(c.oid)),
    (select relpages from pg_get_gtt_relstats(c.oid)),
    (select reltuples from pg_get_gtt_relstats(c.oid)),
    (select relallvisible from pg_get_gtt_relstats(c.oid)),
    (select relfrozenxid from pg_get_gtt_relstats(c.oid)),
    (select relminmxid from pg_get_gtt_relstats(c.oid))
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r','p','i','t');
GRANT SELECT ON TABLE pg_catalog.pg_gtt_relstats TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_gtt_attached_pids cascade;
CREATE VIEW pg_catalog.pg_gtt_attached_pids WITH (security_barrier) AS
 SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    c.oid AS relid,
    array(select pid from pg_gtt_attached_pid(c.oid)) AS pids
 FROM
     pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relpersistence='g' AND c.relkind in('r','S');
GRANT SELECT ON TABLE pg_catalog.pg_gtt_attached_pids TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_gtt_stats cascade;
CREATE VIEW pg_catalog.pg_gtt_stats WITH (security_barrier) AS
SELECT s.nspname AS schemaname,
    s.relname AS tablename,
    s.attname,
    s.stainherit AS inherited,
    s.stanullfrac AS null_frac,
    s.stawidth AS avg_width,
    s.stadistinct AS n_distinct,
        CASE
            WHEN s.stakind1 = 1 THEN s.stavalues1
            WHEN s.stakind2 = 1 THEN s.stavalues2
            WHEN s.stakind3 = 1 THEN s.stavalues3
            WHEN s.stakind4 = 1 THEN s.stavalues4
            WHEN s.stakind5 = 1 THEN s.stavalues5
        END AS most_common_vals,
        CASE
            WHEN s.stakind1 = 1 THEN s.stanumbers1
            WHEN s.stakind2 = 1 THEN s.stanumbers2
            WHEN s.stakind3 = 1 THEN s.stanumbers3
            WHEN s.stakind4 = 1 THEN s.stanumbers4
            WHEN s.stakind5 = 1 THEN s.stanumbers5
        END AS most_common_freqs,
        CASE
            WHEN s.stakind1 = 2 THEN s.stavalues1
            WHEN s.stakind2 = 2 THEN s.stavalues2
            WHEN s.stakind3 = 2 THEN s.stavalues3
            WHEN s.stakind4 = 2 THEN s.stavalues4
            WHEN s.stakind5 = 2 THEN s.stavalues5
        END AS histogram_bounds,
        CASE
            WHEN s.stakind1 = 3 THEN s.stanumbers1[1]
            WHEN s.stakind2 = 3 THEN s.stanumbers2[1]
            WHEN s.stakind3 = 3 THEN s.stanumbers3[1]
            WHEN s.stakind4 = 3 THEN s.stanumbers4[1]
            WHEN s.stakind5 = 3 THEN s.stanumbers5[1]
        END AS correlation,
        CASE
            WHEN s.stakind1 = 4 THEN s.stavalues1
            WHEN s.stakind2 = 4 THEN s.stavalues2
            WHEN s.stakind3 = 4 THEN s.stavalues3
            WHEN s.stakind4 = 4 THEN s.stavalues4
            WHEN s.stakind5 = 4 THEN s.stavalues5
        END AS most_common_elems,
        CASE
            WHEN s.stakind1 = 4 THEN s.stanumbers1
            WHEN s.stakind2 = 4 THEN s.stanumbers2
            WHEN s.stakind3 = 4 THEN s.stanumbers3
            WHEN s.stakind4 = 4 THEN s.stanumbers4
            WHEN s.stakind5 = 4 THEN s.stanumbers5
        END AS most_common_elem_freqs,
        CASE
            WHEN s.stakind1 = 5 THEN s.stanumbers1
            WHEN s.stakind2 = 5 THEN s.stanumbers2
            WHEN s.stakind3 = 5 THEN s.stanumbers3
            WHEN s.stakind4 = 5 THEN s.stanumbers4
            WHEN s.stakind5 = 5 THEN s.stanumbers5
        END AS elem_count_histogram
   FROM
    (SELECT n.nspname,
        c.relname,
        a.attname,
        (select stainherit from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stainherit,
        (select stanullfrac from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanullfrac,
        (select stawidth from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stawidth,
        (select stadistinct from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stadistinct,
        (select stakind1 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind1,
        (select stakind2 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind2,
        (select stakind3 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind3,
        (select stakind4 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind4,
        (select stakind5 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stakind5,
        (select stanumbers1 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers1,
        (select stanumbers2 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers2,
        (select stanumbers3 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers3,
        (select stanumbers4 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers4,
        (select stanumbers5 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stanumbers5,
        (select stavalues1 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues1,
        (select stavalues2 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues2,
        (select stavalues3 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues3,
        (select stavalues4 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues4,
        (select stavalues5 from pg_get_gtt_statistics(c.oid, a.attnum, ''::text)) as stavalues5
       FROM
         pg_class c
         JOIN pg_attribute a ON c.oid = a.attrelid
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relpersistence='g' AND c.relkind in('r','p','i','t') and a.attnum > 0 and NOT a.attisdropped AND has_column_privilege(c.oid, a.attnum, 'select'::text)) s;
GRANT SELECT ON TABLE pg_catalog.pg_gtt_stats TO PUBLIC;

DROP FUNCTION IF EXISTS pg_catalog.pg_partition_filepath() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3220;
CREATE OR REPLACE FUNCTION pg_catalog.pg_partition_filepath(partitionoid oid, OUT filepath text)
RETURNS text LANGUAGE INTERNAL STABLE as 'pg_partition_filepath';

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
                            (relname varchar, begintime timestamptz, filename varchar, lineno int8, rawrecord text, detail text)';
        EXECUTE query_str_create_table;

        query_str_create_index := 'CREATE INDEX copy_error_log_relname_idx ON public.pgxc_copy_error_log(relname)';
        EXECUTE query_str_create_index;

        query_str_do_revoke := 'REVOKE ALL on public.pgxc_copy_error_log FROM public';
        EXECUTE query_str_do_revoke;

        return true;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
REVOKE ALL on FUNCTION pg_catalog.copy_error_log_create() FROM public;

DO $DO$
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
      WHERE C.relkind IN ('r', 't', 'i', 'I');
  end if;
END $DO$;

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
    WHERE C.relkind IN ('r','m') AND I.relkind IN ('i','I');

DROP TABLE IF EXISTS snapshot.snap_global_operator_runtime;
DROP TABLE IF EXISTS snapshot.snap_global_operator_history;
DROP TABLE IF EXISTS snapshot.snap_global_operator_history_table;

DROP TABLE IF EXISTS snapshot.snap_global_operator_ec_history_table;
DROP TABLE IF EXISTS snapshot.snap_global_operator_ec_runtime;
DROP TABLE IF EXISTS snapshot.snap_global_operator_ec_history;

DROP TABLE IF EXISTS snapshot.snap_global_statement_complex_history_table;
DROP TABLE IF EXISTS snapshot.snap_global_statement_complex_runtime;
DROP TABLE IF EXISTS snapshot.snap_global_statement_complex_history;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3790;
CREATE OR REPLACE FUNCTION pg_catalog.pg_create_physical_replication_slot_extern
(  in slotname name,
   in dummy_standby boolean,
   in extra_content text default null,
   OUT slotname text,
   OUT xlog_position text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'pg_create_physical_replication_slot_extern';

drop extension if exists gsredistribute cascade;
create extension gsredistribute;

drop function if exists pg_catalog.series(anyelement) cascade;
drop function if exists pg_catalog.top_key(anyelement, int8) cascade;
drop function if exists pg_catalog.series(name) cascade;
drop function if exists pg_catalog.top_key(name, int8) cascade;
drop function if exists pg_catalog.top_key(name, int) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6006;
create or replace function pg_catalog.series(anyelement) returns anyelement LANGUAGE INTERNAL as 'series_internal';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6008;
create or replace function pg_catalog.top_key(anyelement, int8) returns anyelement LANGUAGE INTERNAL as 'top_key_internal';

DROP FUNCTION IF EXISTS pg_catalog.mode_final(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4462;
CREATE FUNCTION pg_catalog.mode_final(internal)
RETURNS anyelement LANGUAGE INTERNAL IMMUTABLE NOT FENCED as 'mode_final';

DROP AGGREGATE IF EXISTS pg_catalog.mode(order by anyelement);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4461;
CREATE AGGREGATE pg_catalog.mode(order by anyelement) (SFUNC=ordered_set_transition, STYPE=internal, finalfunc=mode_final);

DROP FUNCTION IF EXISTS pg_catalog.gs_index_advise(cstring);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4888;
CREATE OR REPLACE FUNCTION pg_catalog.gs_index_advise(cstring) returns table ("table" text, "column" text) language INTERNAL NOT FENCED as 'gs_index_advise';

DROP FUNCTION IF EXISTS pg_catalog.local_single_flush_dw_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4375;
CREATE FUNCTION pg_catalog.local_single_flush_dw_stat
(
OUT node_name pg_catalog.text,
OUT curr_dwn pg_catalog.int4,
OUT curr_start_page pg_catalog.int4,
OUT total_writes pg_catalog.int8,
OUT file_trunc_num pg_catalog.int8,
OUT file_reset_num pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'local_single_flush_dw_stat';

DROP FUNCTION IF EXISTS pg_catalog.remote_single_flush_dw_stat();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4376;
CREATE FUNCTION pg_catalog.remote_single_flush_dw_stat
(
OUT node_name pg_catalog.text,
OUT curr_dwn pg_catalog.int4,
OUT curr_start_page pg_catalog.int4,
OUT total_writes pg_catalog.int8,
OUT file_trunc_num pg_catalog.int8,
OUT file_reset_num pg_catalog.int8
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'remote_single_flush_dw_stat';

CREATE OR REPLACE VIEW DBE_PERF.global_single_flush_dw_status AS
    SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num
    FROM pg_catalog.remote_single_flush_dw_stat()
    UNION ALL
    SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num
    FROM pg_catalog.local_single_flush_dw_stat();

REVOKE ALL on DBE_PERF.global_single_flush_dw_status FROM PUBLIC;

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.global_single_flush_dw_status TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
END;
/

GRANT SELECT ON TABLE DBE_PERF.global_single_flush_dw_status TO PUBLIC;

DROP FUNCTION IF EXISTS pg_catalog.gs_set_obs_delete_location(text) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9031;
CREATE OR REPLACE FUNCTION pg_catalog.gs_set_obs_delete_location
(  in delete_location text,
   OUT xlog_file_name text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_set_obs_delete_location';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.get_local_cont_query_stat(cq_id oid)
        RETURNS TABLE (
            cq oid,
            w_in_rows int8,
            w_in_bytes int8,
            w_out_rows int8,
            w_out_bytes int8,
            w_pendings int8,
            w_errors int8,
            r_in_rows int8,
            r_in_bytes int8,
            r_out_rows int8,
            r_out_bytes int8,
            r_errors int8,
            c_in_rows int8,
            c_in_bytes int8,
            c_out_rows int8,
            c_out_bytes int8,
            c_pendings int8,
            c_errors int8
        )
        AS '$libdir/streaming', 'get_local_cont_query_stat'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;
    else
        EXECUTE 'CREATE OR REPLACE FUNCTION pg_catalog.get_local_cont_query_stat(cq_id oid)
        RETURNS TABLE (
            cq oid,
            w_in_rows int8,
            w_in_bytes int8,
            w_out_rows int8,
            w_out_bytes int8,
            w_pendings int8,
            w_errors int8,
            r_in_rows int8,
            r_in_bytes int8,
            r_out_rows int8,
            r_out_bytes int8,
            r_errors int8,
            c_in_rows int8,
            c_in_bytes int8,
            c_out_rows int8,
            c_out_bytes int8,
            c_pendings int8,
            c_errors int8
        ) AS
        $func$
        BEGIN
            w_in_rows = 0;
            w_in_bytes = 0;
            w_out_rows = 0;
            w_out_bytes = 0;
            w_pendings = 0;
            w_errors = 0;
            r_in_rows = 0;
            r_in_bytes = 0;
            r_out_rows = 0;
            r_out_bytes = 0;
            r_errors = 0;
            c_in_rows = 0;
            c_in_bytes = 0;
            c_out_rows = 0;
            c_out_bytes = 0;
            c_pendings = 0;
            c_errors = 0;
        END
        $func$
        LANGUAGE ''plpgsql'' NOT FENCED';
    end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.reset_local_cont_query_stat(cq_id oid)
        RETURNS bool
        AS '$libdir/streaming', 'reset_local_cont_query_stat'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;
    else
        EXECUTE 'CREATE OR REPLACE FUNCTION pg_catalog.reset_local_cont_query_stat(cq_id oid)
        RETURNS bool AS
        $func$
        BEGIN
            return false;
        END
        $func$
        LANGUAGE ''plpgsql'' NOT FENCED';
    end if;
END$$;

CREATE OR REPLACE FUNCTION pg_catalog.check_cont_query_schema_changed(cq_id oid)
RETURNS bool
AS $$
DECLARE
    query_stats text;
    stats record;
BEGIN
    query_stats := 'SELECT * FROM  pg_catalog.get_local_cont_query_stat(' || cq_id || ')';
    FOR stats IN EXECUTE(query_stats) LOOP
        IF (stats.r_in_rows != stats.r_out_rows OR stats.r_out_rows != stats.c_in_rows OR stats.w_pendings > 0 OR stats.c_pendings > 0)
        THEN
            RAISE EXCEPTION 'cont query schema change in progress, id[%] r_in_rows[%] r_out_rows[%] c_in_rows[%]', stats.cq,
            stats.r_in_rows, stats.r_out_rows, stats.c_in_rows;
        END IF;
    END LOOP;
    return true;
END; $$
LANGUAGE 'plpgsql' NOT FENCED;

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
        SELECT pg_catalog.streaming_cont_query_matrelid_index_local_set_unique(false) INTO ans;
        SELECT pg_catalog.streaming_cont_query_matrelid_index_set_unique(false) INTO ans;
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

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.gather_sfunc_dummy(anyelement, anyelement)
        RETURNS anyelement
        AS '$libdir/streaming', 'gather_sfunc_dummy'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;

        CREATE AGGREGATE gather(anyelement) (
            sfunc = gather_sfunc_dummy,
            stype = anyelement,
            initcond = 'gather_dummy'
        );

        /* avg */
        CREATE AGGREGATE streaming_int8_avg_gather(int8[]) (
                sfunc = int8_avg_collect,
                stype = int8[],
                cfunc = int8_avg_collect,
                finalfunc = int8_avg,
                initcond = '{0,0}'
        );

        CREATE AGGREGATE streaming_numeric_avg_gather(numeric[]) (
                sfunc = numeric_avg_collect,
                stype = numeric[],
                cfunc = numeric_avg_collect,
                finalfunc = numeric_avg,
                initcond = '{0,0}'
        );

        CREATE AGGREGATE streaming_float8_avg_gather(float8[]) (
                sfunc = float8_collect,
                stype = float8[],
                cfunc = float8_collect,
                finalfunc = float8_avg,
                initcond = '{0,0,0}'
        );

        CREATE AGGREGATE streaming_interval_avg_gather(interval[]) (
                sfunc = interval_collect,
                stype = interval[],
                cfunc = interval_collect,
                finalfunc = interval_avg,
                initcond = '{0 second,0 second}'
        );

        /* sum */
        CREATE AGGREGATE streaming_int8_sum_gather(numeric) (
                sfunc = numeric_add,
                stype = numeric,
                cfunc = numeric_add
        );

        CREATE AGGREGATE streaming_int2_int4_sum_gather(int8) (
                sfunc = int8_sum_to_int8,
                stype = int8,
                cfunc = int8_sum_to_int8
        );
    end if;
END$$;

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
    OUT params_num int4,
    OUT func_id oid)
RETURNS SETOF RECORD LANGUAGE INTERNAL AS 'gs_globalplancache_status';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3959;
CREATE OR REPLACE FUNCTION pg_catalog.prepare_statement_status(
    OUT nodename text,
    OUT cn_sess_id int8,
    OUT cn_node_id int4,
    OUT cn_time_line int4,
    OUT statement_name text,
    OUT refcount int4,
    OUT is_shared bool,
    OUT query text)
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
    DROP FUNCTION IF EXISTS DBE_PERF.global_plancache_clean() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.local_plancache_clean CASCADE;

    CREATE VIEW DBE_PERF.local_plancache_status AS
      SELECT * FROM pg_catalog.plancache_status();

    CREATE VIEW DBE_PERF.local_prepare_statement_status AS
      SELECT * FROM pg_catalog.prepare_statement_status();

    CREATE VIEW DBE_PERF.local_plancache_clean AS
      SELECT * FROM pg_catalog.plancache_clean();

    CREATE VIEW DBE_PERF.global_plancache_clean AS
      SELECT * FROM pg_catalog.plancache_clean();

  end if;
END$DO$;

DROP FUNCTION IF EXISTS pg_catalog.hypopg_create_index(text);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4889;
CREATE OR REPLACE FUNCTION pg_catalog.hypopg_create_index(text) returns table ("indexrelid" int, "indexname" text) language INTERNAL NOT FENCED as 'hypopg_create_index';

DROP FUNCTION IF EXISTS pg_catalog.hypopg_display_index();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4890;
CREATE OR REPLACE FUNCTION pg_catalog.hypopg_display_index() returns table ("indexname" text, "indexrelid" int, "table" text, "column" text) language INTERNAL NOT FENCED as 'hypopg_display_index';

DROP FUNCTION IF EXISTS pg_catalog.hypopg_drop_index(int4);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4891;
CREATE OR REPLACE FUNCTION pg_catalog.hypopg_drop_index(int4) returns boolean language INTERNAL NOT FENCED as 'hypopg_drop_index';

DROP FUNCTION IF EXISTS pg_catalog.hypopg_estimate_size(int4);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4892;
CREATE OR REPLACE FUNCTION pg_catalog.hypopg_estimate_size(int4) returns int8 language INTERNAL NOT FENCED as 'hypopg_estimate_size';

DROP FUNCTION IF EXISTS pg_catalog.hypopg_reset_index();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,4893;
CREATE OR REPLACE FUNCTION pg_catalog.hypopg_reset_index() returns void language INTERNAL NOT FENCED as 'hypopg_reset_index';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_global_barrier_status() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9032;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_global_barrier_status
(  OUT global_barrier_id text,
   OUT global_achive_barrier_id text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_global_barrier_status';

DROP FUNCTION IF EXISTS pg_catalog.gs_get_local_barrier_status() cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9033;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_local_barrier_status
(  OUT barrier_id text,
   OUT barrier_lsn text,
   OUT archive_lsn text,
   OUT flush_lsn text)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_local_barrier_status';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3900;
CREATE FUNCTION pg_catalog.global_stat_clean_hotkeys() RETURNS BOOL LANGUAGE INTERNAL as 'global_stat_clean_hotkeys';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3903;
CREATE FUNCTION pg_catalog.global_stat_get_hotkeys_info(OUT database_name TEXT, OUT schema_name TEXT,
OUT table_name TEXT, OUT key_value TEXT, OUT hash_value INT8, OUT count INT8) RETURNS SETOF RECORD
LANGUAGE INTERNAL as 'global_stat_get_hotkeys_info';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3805;
CREATE FUNCTION pg_catalog.gs_stat_clean_hotkeys() RETURNS BOOL LANGUAGE INTERNAL as 'gs_stat_clean_hotkeys';

set local inplace_upgrade_next_system_object_oids = IUO_PROC,3803;
CREATE FUNCTION pg_catalog.gs_stat_get_hotkeys_info(OUT database_name TEXT, OUT schema_name TEXT,
OUT table_name TEXT, OUT key_value TEXT, OUT hash_value INT8, OUT count INT8) RETURNS SETOF RECORD
LANGUAGE INTERNAL as 'gs_stat_get_hotkeys_info';

CREATE VIEW global_stat_hotkeys_info AS
SELECT
    database_name, schema_name, table_name, key_value, hash_value, sum(count) as count
FROM
    global_stat_get_hotkeys_info()
group by
    database_name, schema_name, table_name, key_value, hash_value
order by
    count
DESC;

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

CREATE VIEW DBE_PERF.statement_history AS
  select * from pg_catalog.statement_history;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
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
       OUT is_slow_sql bool)
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
   OUT is_slow_sql bool)
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
              return next;
           END LOOP;
        END LOOP;
        return;
      END; $$
    LANGUAGE 'plpgsql' NOT FENCED;
  end if;
END$DO$;


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
    OUT data_io_time bigint,
    OUT net_send_info text,
    Out net_recv_info text,
    OUT net_stream_send_info text,
    OUT net_stream_recv_info text,
    OUT last_updated timestamp with time zone
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_unique_sql';

DROP FUNCTION IF EXISTS pg_catalog.statement_detail_decode() CASCADE;
set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5732;
CREATE OR REPLACE FUNCTION pg_catalog.statement_detail_decode
(  IN text,
   IN text,
   IN boolean)
RETURNS text LANGUAGE INTERNAL NOT FENCED as 'statement_detail_decode';

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

    CREATE VIEW DBE_PERF.summary_statement AS
      SELECT * FROM DBE_PERF.get_summary_statement();

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.STATEMENT TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.summary_statement TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;

  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_summary_statement' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_summary_statement
    ADD COLUMN snap_last_updated timestamp with time zone;
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
DROP FUNCTION IF EXISTS pg_catalog.get_instr_wait_event(IN param int4, OUT nodename text, OUT type text, OUT event text, OUT wait bigint, OUT failed_wait bigint, OUT total_wait_time bigint, OUT avg_wait_time bigint, OUT max_wait_time bigint, OUT min_wait_time bigint) cascade;

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
    OUT min_wait_time bigint,
    OUT last_updated timestamp with time zone
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

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_global_wait_events' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_global_wait_events
    ADD COLUMN snap_last_updated timestamp with time zone;
  end if;
END$DO$;

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
    OUT net_recv_info text,
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

    CREATE VIEW DBE_PERF.summary_statement AS
      SELECT * FROM DBE_PERF.get_summary_statement();

    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.STATEMENT TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.summary_statement TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE DBE_PERF.statement_history TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT ALL ON TABLE pg_catalog.statement_history TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;

    GRANT SELECT ON TABLE DBE_PERF.STATEMENT TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_statement TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;

  end if;
END$DO$;

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select * from pg_tables where tablename = 'snap_summary_statement' and schemaname = 'snapshot' limit 1) into ans;
  if ans = true then
    alter table snapshot.snap_summary_statement
    ADD COLUMN snap_sort_count bigint,
    ADD COLUMN snap_sort_time bigint,
    ADD COLUMN snap_sort_mem_used bigint,
    ADD COLUMN snap_sort_spill_count bigint,
    ADD COLUMN snap_sort_spill_size bigint,
    ADD COLUMN snap_hash_count bigint,
    ADD COLUMN snap_hash_time bigint,
    ADD COLUMN snap_hash_mem_used bigint,
    ADD COLUMN snap_hash_spill_count bigint,
    ADD COLUMN snap_hash_spill_size bigint;
  end if;
END$DO$;

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
          AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES', 'COMMENT')
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
           CAST(p.prtype AS character_data) AS privilege_type,
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
          AND p.prtype IN ('EXECUTE', 'ALTER', 'DROP', 'COMMENT')
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
           CAST(t.prtype AS character_data) AS privilege_type, -- sic
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
          AND t.prtype IN ('USAGE', 'ALTER', 'DROP', 'COMMENT')
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
          AND c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER' ,
                           'ALTER', 'DROP', 'COMMENT', 'INDEX', 'VACUUM')
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

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_statistics()
RETURNS setof record
AS $$
DECLARE
	row_data record;
	query_str text;
	BEGIN
		query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_session_statistics''';
		FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_history()
RETURNS setof record
AS $$
DECLARE
        row_data record;
        query_str text;
        BEGIN
                --Get all the node names
                query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_session_history''';
                FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_info()
RETURNS setof record
AS $$
DECLARE
        row_data record;
        query_str text;
        BEGIN
                --Get all the node names
                query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_session_info''';
                FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
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
        query_str text;
        query_str_cn text;
        BEGIN
        		IF $1 IN ('start_time', 'finish_time') THEN

	        	ELSE
	        		 raise WARNING 'Illegal character entered for function, colname must be start_time or finish_time';
	        		return;
	        	END IF;

                --Get all the node names
                query_str_cn := 'SELECT * FROM gs_wlm_session_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
                query_str := 'EXECUTE DIRECT ON COORDINATORS ''' || query_str_cn||''';';
                FOR row_data IN EXECUTE(query_str) LOOP
                        return next row_data;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_wlm_rebuild_user_resource_pool()
RETURNS setof gs_wlm_rebuild_user_resource_pool
AS $$
DECLARE
	row_data gs_wlm_rebuild_user_resource_pool%rowtype;
	query_str text;
	BEGIN
		--Get all the node names
		query_str := 'EXECUTE DIRECT ON ALL ''SELECT * FROM gs_wlm_rebuild_user_resource_pool''';
		FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_wlm_get_workload_records()
RETURNS setof record
AS $$
DECLARE
	row_data record;
	query_str text;
	BEGIN
		--Get all the node names
		query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_workload_records''';
		FOR row_data IN EXECUTE(query_str) LOOP
			return next row_data;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_sql_count()
RETURNS setof gs_sql_count
AS $$
DECLARE
    row_data gs_sql_count%rowtype;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON ALL ''SELECT * FROM gs_sql_count''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_os_threads()
RETURNS setof pg_os_threads
AS $$
DECLARE
    row_data pg_os_threads%rowtype;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON ALL ''SELECT * FROM pg_os_threads''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_running_xacts()
RETURNS setof pg_running_xacts
AS $$
DECLARE
    row_data pg_running_xacts%rowtype;
    query_str text;
    BEGIN
        --Get all the coordinator node names
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM pg_running_xacts''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_variable_info()
RETURNS setof pg_variable_info
AS $$
DECLARE
    row_data pg_variable_info%rowtype;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON ALL ''SELECT * FROM pg_variable_info''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_comm_delay()
RETURNS setof pg_comm_delay
AS $$
DECLARE
        row_data pg_comm_delay%rowtype;
        query_str text;
        BEGIN
                query_str := 'EXECUTE DIRECT ON DATANODES ''SELECT * from pg_comm_delay''';
                 FOR row_data IN EXECUTE(query_str) LOOP
                         RETURN NEXT row_data;
                 END LOOP;
            return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_statistics()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    query_str text;
    BEGIN
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_operator_statistics''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_history()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_operator_history''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_operator_info()
RETURNS setof gs_wlm_operator_info
AS $$
DECLARE
    row_data gs_wlm_operator_info%rowtype;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_operator_info''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_statistics()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    query_str text;
    BEGIN
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_ec_operator_statistics''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_history()
RETURNS setof record
AS $$
DECLARE
    row_data record;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_ec_operator_history''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_ec_operator_info()
RETURNS setof gs_wlm_ec_operator_info
AS $$
DECLARE
    row_data gs_wlm_ec_operator_info%rowtype;
    query_str text;
    BEGIN
        --Get all the node names
        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM gs_wlm_ec_operator_info''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

DROP FUNCTION IF EXISTS  pg_catalog.pgxc_query_audit(IN starttime TIMESTAMPTZ, IN endtime TIMESTAMPTZ, OUT "time" TIMESTAMPTZ, OUT "type" text, OUT "result" text, OUT userid text, OUT username text, OUT database text, OUT client_conninfo text, OUT "object_name" text, OUT detail_info text, OUT node_name text, OUT thread_id text, OUT local_port text, OUT remote_port text)
 CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pgxc_stat_all_tables()
 CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_stat_all_tables()
RETURNS setof pg_stat_all_tables
AS $$
DECLARE
	row_data record;
	query_str text;
	BEGIN
		--Get all the node names
        query_str := 'EXECUTE DIRECT ON DATANODES ''SELECT * FROM pg_stat_all_tables''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
        END LOOP;

        query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM pg_stat_all_tables where schemaname = ''''pg_catalog'''' or schemaname =''''pg_toast'''' ''';
        FOR row_data IN EXECUTE(query_str) LOOP
            return next row_data;
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

CREATE OR REPLACE FUNCTION pg_catalog.reload_active_coordinator()
RETURNS boolean
AS $$
DECLARE
	row_data record;
	query_str text;
	return_result  bool = true;
	BEGIN
		query_str := 'EXECUTE DIRECT ON COORDINATORS ''SELECT * FROM pgxc_pool_reload()''';
		begin
		    FOR row_data IN EXECUTE(query_str) LOOP
		        if row_data.pgxc_pool_reload = 'f' then
				return_result = false;
				return return_result;
			    end if;
            END LOOP;
		end;
		return return_result;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;

GRANT SELECT ON pg_catalog.pgxc_get_stat_all_tables TO public;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2200;
CREATE OR REPLACE FUNCTION pg_catalog.gs_roach_stop_backup
(  in slotname text,
   OUT backup_stop_lsn text)
RETURNS text LANGUAGE INTERNAL STRICT as 'gs_roach_stop_backup';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2201;
CREATE OR REPLACE FUNCTION pg_catalog.gs_roach_enable_delay_ddl_recycle
(  in slotname name,
   OUT ddl_delay_start_lsn text)
RETURNS text LANGUAGE INTERNAL STRICT as 'gs_roach_enable_delay_ddl_recycle';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2202;
CREATE OR REPLACE FUNCTION pg_catalog.gs_roach_disable_delay_ddl_recycle
(  in slotname name,
   OUT ddl_delay_start_lsn text,
   OUT ddl_delay_end_lsn text)
RETURNS record LANGUAGE INTERNAL STRICT as 'gs_roach_disable_delay_ddl_recycle';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2203;
CREATE OR REPLACE FUNCTION pg_catalog.gs_roach_switch_xlog
(bool)
RETURNS text LANGUAGE INTERNAL STRICT as 'gs_roach_switch_xlog';

DROP FUNCTION IF EXISTS pg_catalog.pg_resume_bkp_flag();
DROP FUNCTION IF EXISTS pg_catalog.pg_resume_bkp_flag(name);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4445;
CREATE OR REPLACE FUNCTION pg_catalog.pg_resume_bkp_flag
(in slot_name name,
out start_backup_flag bool,
out to_delay bool,
out ddl_delay_recycle_ptr text,
out rewind_time text)
RETURNS record LANGUAGE INTERNAL STRICT as 'pg_resume_bkp_flag';

create or replace function pg_catalog.pgxc_cgroup_map_ng_conf(IN ngname text)
returns bool
AS $$
declare
    query_string text;
    flag boolean;
    special text := '[^A-z0-9_]';
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

--Test distribute situation
create or replace function pg_catalog.table_skewness(table_name text, column_name text,
                        OUT seqNum text, OUT Num text, OUT Ratio text, row_num text default '0')
RETURNS setof record
AS $$
DECLARE
    tolal_num text;
    row_data record;
    execute_query text;
    dist_type text;
    num_check int :=0;
    sqltmp text;
    BEGIN
        sqltmp := 'select count(*) from pg_attribute join pg_class on pg_attribute.attrelid = pg_class.oid where pg_class.relname =' || quote_literal(table_name) ||' and pg_attribute.attname = ' ||quote_literal($2);
        EXECUTE immediate sqltmp into num_check;
        if num_check = 0 then
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

CREATE OR REPLACE PROCEDURE proc_matview()
AS
    BEGIN
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matview') THEN
            GRANT SELECT ON TABLE pg_catalog.gs_matview TO PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matview_dependency') THEN
            GRANT SELECT ON TABLE pg_catalog.gs_matview_dependency TO PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='gs_matviews') THEN
            GRANT SELECT ON TABLE pg_catalog.gs_matviews TO PUBLIC;
        END IF;
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='track_memory_context_detail') THEN
            GRANT SELECT ON TABLE dbe_perf.track_memory_context_detail TO PUBLIC;
        END IF;
    END;
/

CALL proc_matview();
DROP PROCEDURE IF EXISTS proc_matview;

CREATE OR REPLACE PROCEDURE proc_hotkeyview()
AS
    BEGIN
        IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='global_stat_hotkeys_info') THEN
            GRANT SELECT ON TABLE global_stat_hotkeys_info TO PUBLIC;
        END IF;
    END;
/
CALL proc_hotkeyview();
DROP PROCEDURE IF EXISTS proc_hotkeyview;

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
    details bytea,
    is_slow_sql bool
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
         OUT is_slow_sql bool)
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
         OUT is_slow_sql bool)
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
                    return next;
                END LOOP;
            END LOOP;
            return;
          END; $$
        LANGUAGE 'plpgsql' NOT FENCED;

        DROP FUNCTION IF EXISTS pg_catalog.statement_detail_decode() CASCADE;
        set local inplace_upgrade_next_system_object_oids = IUO_PROC, 5732;
        CREATE OR REPLACE FUNCTION pg_catalog.statement_detail_decode
        (  IN bytea,
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

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;

    IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='track_memory_context_detail') THEN
        query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.track_memory_context_detail TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        GRANT SELECT ON TABLE dbe_perf.track_memory_context_detail TO PUBLIC;
    END IF;
END;
/

drop function if exists pg_catalog.sys_context(text, text) cascade;

DROP SCHEMA IF EXISTS pkg_service cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 3988;
CREATE SCHEMA pkg_service;
COMMENT ON schema pkg_service IS 'pkg_service schema';


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4800;
CREATE OR REPLACE FUNCTION pkg_service.job_cancel(
    in id bigint
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
 as 'job_cancel';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4801;
CREATE OR REPLACE FUNCTION pkg_service.job_finish(
    in id bigint,
    in finished boolean,
    in next_time timestamp without time zone default sysdate
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_finish';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4802;
CREATE OR REPLACE FUNCTION pkg_service.job_submit(
    in id bigint,
    in content text,
    in next_date timestamp without time zone default sysdate,
    in interval_time text default 'null',
    out job integer
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_submit';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4803;
CREATE OR REPLACE FUNCTION pkg_service.job_update(
    in id bigint,
    in next_date timestamp without time zone,
    in interval_time text,
    in content text
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'job_update';

DECLARE
    user_name text;
    query_str text;
BEGIN
    SELECT SESSION_USER INTO user_name;
    IF EXISTS (SELECT oid FROM pg_catalog.pg_class WHERE relname='statement_history') THEN
        query_str := 'REVOKE ALL ON TABLE dbe_perf.statement_history FROM ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        query_str := 'REVOKE ALL ON TABLE pg_catalog.statement_history FROM ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        query_str := 'REVOKE SELECT on table dbe_perf.statement_history FROM public;';
        EXECUTE IMMEDIATE query_str;
        query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE dbe_perf.statement_history TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        query_str := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON TABLE pg_catalog.statement_history TO ' || quote_ident(user_name) || ';';
        EXECUTE IMMEDIATE query_str;
        GRANT SELECT ON TABLE DBE_PERF.statement_history TO PUBLIC;
    END IF;
END;
/

CREATE OR REPLACE VIEW pg_catalog.pg_seclabels AS
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN rel.relkind = 'r' THEN 'table'::text
		 WHEN rel.relkind = 'v' THEN 'view'::text
		 WHEN rel.relkind = 'm' THEN 'materialized view'::text
		 WHEN rel.relkind = 'S' THEN 'sequence'::text
		 WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
	     END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'column'::text AS objtype,
	rel.relnamespace AS objnamespace,
	CASE WHEN pg_table_is_visible(rel.oid)
	     THEN quote_ident(rel.relname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
	     END || '.' || att.attname AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
	JOIN pg_attribute att
	     ON rel.oid = att.attrelid AND l.objsubid = att.attnum
	JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
WHERE
	l.objsubid != 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN pro.proisagg = true THEN 'aggregate'::text
	     WHEN pro.proisagg = false THEN 'function'::text
	END AS objtype,
	pro.pronamespace AS objnamespace,
	CASE WHEN pg_function_is_visible(pro.oid)
	     THEN quote_ident(pro.proname)
	     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(pro.proname)
	END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_proc pro ON l.classoid = pro.tableoid AND l.objoid = pro.oid
	JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	CASE WHEN typ.typtype = 'd' THEN 'domain'::text
	ELSE 'type'::text END AS objtype,
	typ.typnamespace AS objnamespace,
	CASE WHEN pg_type_is_visible(typ.oid)
	THEN quote_ident(typ.typname)
	ELSE quote_ident(nsp.nspname) || '.' || quote_ident(typ.typname)
	END AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_type typ ON l.classoid = typ.tableoid AND l.objoid = typ.oid
	JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'large object'::text AS objtype,
	NULL::oid AS objnamespace,
	l.objoid::text AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
WHERE
	l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'language'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(lan.lanname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_language lan ON l.classoid = lan.tableoid AND l.objoid = lan.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, l.objsubid,
	'schema'::text AS objtype,
	nsp.oid AS objnamespace,
	quote_ident(nsp.nspname) AS objname,
	l.provider, l.label
FROM
	pg_seclabel l
	JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid = nsp.oid
WHERE
	l.objsubid = 0
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'database'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(dat.datname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid = dat.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'tablespace'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(spc.spcname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid = spc.oid
UNION ALL
SELECT
	l.objoid, l.classoid, 0::int4 AS objsubid,
	'role'::text AS objtype,
	NULL::oid AS objnamespace,
	quote_ident(rol.rolname) AS objname,
	l.provider, l.label
FROM
	pg_shseclabel l
	JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid = rol.oid;

CREATE OR REPLACE VIEW pg_catalog.gs_session_memory_context AS SELECT * FROM pv_session_memory_detail();
CREATE OR REPLACE VIEW pg_catalog.gs_thread_memory_context AS SELECT * FROM pv_thread_memory_detail();
