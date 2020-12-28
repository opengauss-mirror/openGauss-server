DECLARE
sql text;
BEGIN
    if exists(select extname from pg_extension where extname = 'packages') then
        sql := 'DROP EXTENSION IF EXISTS packages CASCADE;';
        execute sql;
        sql := 'CREATE EXTENSION packages;';
        execute sql;
    end if;
END
/

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='pkg_service' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS pkg_service.job_cancel(bigint) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_finish(bigint, boolean, timestamp) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_submit(bigint, text, timestamp, text) cascade;
            DROP FUNCTION IF EXISTS pkg_service.job_update(bigint, timestamp, text, text) cascade;
        end if;
        exit;
    END LOOP;
END$$;

DECLARE
sql text;
BEGIN
    if exists(select extname from pg_extension where extname = 'gsredistribute') then
        sql := 'DROP EXTENSION IF EXISTS gsredistribute CASCADE;';
        execute sql;
        sql := 'CREATE EXTENSION gsredistribute;';
        execute sql;
    end if;
END
/

DROP FUNCTION IF EXISTS pg_catalog.pg_control_checkpoint() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_control_system() CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.track_model_train_opt(text, text);

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_get_wlm_plan_operator_info(oid);

DROP FUNCTION IF EXISTS pg_catalog.model_train_opt(text, text);

DROP FUNCTION IF EXISTS pg_catalog.encode_plan_node(text, text, text, text, int8, text, text);

DROP FUNCTION IF EXISTS pg_catalog.check_engine_status(text, text);


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
    INNER JOIN pg_catalog.pgxc_class x ON c.oid = x.pcrelid AND x.pclocatortype = 'H'
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

create or replace function table_skewness(table_name text, column_name text,
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

DROP FUNCTION IF EXISTS pg_catalog.gs_roach_stop_backup(text);

DROP FUNCTION IF EXISTS pg_catalog.gs_roach_enable_delay_ddl_recycle(name);

DROP FUNCTION IF EXISTS pg_catalog.gs_roach_disable_delay_ddl_recycle(name);

DROP FUNCTION IF EXISTS pg_catalog.gs_roach_switch_xlog(bool);

DROP FUNCTION IF EXISTS pg_catalog.pg_resume_bkp_flag(name);
DROP FUNCTION IF EXISTS pg_catalog.pg_resume_bkp_flag();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4445;
CREATE OR REPLACE FUNCTION pg_catalog.pg_resume_bkp_flag
(out start_backup_flag bool,
out to_delay bool,
out ddl_delay_recycle_ptr text,
out rewind_time text)
RETURNS record LANGUAGE INTERNAL STRICT as 'pg_resume_bkp_flag';

DROP INDEX IF EXISTS pg_catalog.pgxc_slice_relid_index;
DROP INDEX IF EXISTS pg_catalog.pgxc_slice_order_index;
DROP TYPE IF EXISTS pg_catalog.pgxc_slice;
DROP TABLE IF EXISTS pg_catalog.pgxc_slice;

DROP INDEX IF EXISTS pg_catalog.streaming_gather_agg_index;
DROP INDEX IF EXISTS pg_catalog.streaming_reaper_status_id_index;
DROP INDEX IF EXISTS pg_catalog.streaming_reaper_status_oid_index;
DROP TYPE IF EXISTS pg_catalog.streaming_reaper_status;
DROP TABLE IF EXISTS pg_catalog.streaming_reaper_status;

DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_schema_change_index;
UPDATE pg_index SET indisunique = true WHERE indexrelid = 3234;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'byteawithoutorderwithequalcol' limit 1) into ans;
    if ans = true then
        DROP OPERATOR FAMILY IF EXISTS byteawithoutorderwithequalcol_ops USING BTREE;
        DROP OPERATOR FAMILY IF EXISTS byteawithoutorderwithequalcol_ops USING HASH;
        DROP OPERATOR CLASS IF EXISTS byteawithoutorderwithequalcol_ops USING BTREE;
        DROP OPERATOR CLASS IF EXISTS byteawithoutorderwithequalcol_ops USING HASH;

        DROP OPERATOR IF EXISTS pg_catalog.= (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.= (byteawithoutorderwithequalcol, bytea) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.= (bytea, byteawithoutorderwithequalcol) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<> (byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<> (byteawithoutorderwithequalcol, bytea) cascade;
        DROP OPERATOR IF EXISTS pg_catalog.<> (bytea, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmp(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmpbytear(byteawithoutorderwithequalcol, bytea) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolcmpbyteal(bytea, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleq(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleqbyteal(bytea, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoleqbytear(byteawithoutorderwithequalcol, bytea) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolne(byteawithoutorderwithequalcol, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolnebyteal(bytea, byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolnebytear(byteawithoutorderwithequalcol, bytea) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.hll_hash_byteawithoutorderwithequalcol(byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolout(byteawithoutorderwithequalcol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolsend(byteawithoutorderwithequalcol) cascade;
    end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'byteawithoutordercol' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolout(byteawithoutordercol) cascade;
        DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolsend(byteawithoutordercol) cascade;
    end if;
END$$;
DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolin(cstring) cascade;
DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolin(cstring) cascade;
DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcolrecv(internal) cascade;
DROP FUNCTION IF EXISTS pg_catalog.byteawithoutordercolrecv(internal) cascade;

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoltypmodin(_cstring) cascade;

DROP FUNCTION IF EXISTS pg_catalog.byteawithoutorderwithequalcoltypmodout(int4) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(name, text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(name, oid, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, oid, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cek_privilege(oid, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(name, text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(name, oid, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, oid, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(text, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.has_cmk_privilege(oid, text) cascade;

DROP TYPE IF EXISTS pg_catalog.byteawithoutorderwithequalcol;
DROP TYPE IF EXISTS pg_catalog.byteawithoutordercol;

DROP INDEX IF EXISTS pg_catalog.gs_column_keys_args_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_column_keys_args;
DROP TABLE IF EXISTS pg_catalog.gs_column_keys_args;

DROP INDEX IF EXISTS pg_catalog.gs_client_global_keys_args_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_client_global_keys_args;
DROP TABLE IF EXISTS pg_catalog.gs_client_global_keys_args;

DROP INDEX IF EXISTS pg_catalog.gs_column_keys_distributed_id_index;
DROP INDEX IF EXISTS pg_catalog.gs_column_keys_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_column_keys_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_column_keys;
DROP TABLE IF EXISTS pg_catalog.gs_column_keys;

DROP INDEX IF EXISTS pg_catalog.gs_client_global_keys_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_client_global_keys_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_client_global_keys;
DROP TABLE IF EXISTS pg_catalog.gs_client_global_keys;

DROP INDEX IF EXISTS pg_catalog.gs_encrypted_columns_rel_id_column_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_encrypted_columns_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_encrypted_columns;
DROP TABLE IF EXISTS pg_catalog.gs_encrypted_columns;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'packages' limit 1) into ans;
    if ans = true then
        DROP EXTENSION IF EXISTS packages CASCADE;
        CREATE EXTENSION IF NOT EXISTS packages;
        ALTER EXTENSION packages UPDATE TO '1.1';
    end if;
END$$;

DECLARE
sql text;
BEGIN
    if exists(select extname from pg_extension where extname = 'gsredistribute') then
        sql := 'DROP EXTENSION IF EXISTS gsredistribute CASCADE;';
        execute sql;
        sql := 'CREATE EXTENSION gsredistribute;';
        execute sql;
    end if;
END
/

DROP FUNCTION IF EXISTS pg_catalog.delta(NUMERIC) CASCADE;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'security_plugin' limit 1) into ans;
    if ans = true then
        drop extension if exists security_plugin cascade;
        create extension security_plugin;
    end if;
END$$;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.hash_group(VARIADIC "any")
        RETURNS integer
        AS '$libdir/streaming', 'hash_group'
        LANGUAGE C IMMUTABLE NOT FENCED;

        CREATE OR REPLACE FUNCTION pg_catalog.ls_hash_group(VARIADIC "any")
        RETURNS bigint
        AS '$libdir/streaming', 'ls_hash_group'
        LANGUAGE C IMMUTABLE NOT FENCED;

        CREATE OR REPLACE FUNCTION pg_catalog.time_floor(timestamp with time zone, interval)
        RETURNS timestamp with time zone
        AS '$libdir/streaming', 'timestamp_floor'
        LANGUAGE C IMMUTABLE STRICT NOT FENCED;
    end if;
END$$;

DECLARE
sql text;
BEGIN
    if exists(select extname from pg_extension where extname = 'gsredistribute') then
        sql := 'DROP EXTENSION IF EXISTS gsredistribute CASCADE;';
        execute sql;
        sql := 'CREATE EXTENSION gsredistribute;;';
        execute sql;
    end if;
END
/

-- 1. drop system relations and indexes
--    gs_matview
--    gs_matview_dependency

DROP INDEX IF EXISTS pg_catalog.gs_matview_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_matviewid_index;
DROP TYPE  IF EXISTS pg_catalog.gs_matview;
DROP TABLE IF EXISTS pg_catalog.gs_matview;

DROP INDEX IF EXISTS pg_catalog.gs_matviewdep_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_matviewid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_relid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_mlogid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matview_dependency_bothid_index;
DROP TYPE  IF EXISTS pg_catalog.gs_matview_dependency;
DROP TABLE IF EXISTS pg_catalog.gs_matview_dependency;

DROP FUNCTION IF EXISTS pg_catalog.top_key(name, int8) cascade;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'streaming' limit 1) into ans;
    if ans = true then
        CREATE OR REPLACE FUNCTION pg_catalog.hash_group(VARIADIC "any")
        RETURNS integer
        AS '$libdir/streaming', 'hash_group'
        LANGUAGE C IMMUTABLE NOT FENCED;

        CREATE OR REPLACE FUNCTION pg_catalog.ls_hash_group(VARIADIC "any")
        RETURNS bigint
        AS '$libdir/streaming', 'ls_hash_group'
        LANGUAGE C IMMUTABLE NOT FENCED;
    end if;
END$$;

drop function if exists pg_catalog.report_fatal() cascade;
drop function if exists pg_catalog.signal_backend(bigint, int) cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2537;
create function pg_catalog.report_fatal()
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'report_fatal';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2539;
create function pg_catalog.signal_backend(bigint, integer)
 RETURNS boolean
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE
AS 'signal_backend';

DO $$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (select relname from pg_class where relname='gs_asp_sampletime_index' limit 1) into ans;
  if ans = true then
    DROP INDEX IF EXISTS pg_catalog.gs_asp_sampletime_index;
  end if;
END$$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_policy') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_policy_access') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_access TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_policy_filters') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_filters TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_policy_privileges') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_policy_privileges TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_policy_label') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_policy_label TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_masking_policy') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_masking_policy TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_masking_policy_actions') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_masking_policy_actions TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_masking_policy_filters') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_masking_policy_filters TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_labels') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_labels TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_masking') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_masking TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_access') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_access TO public;
    end if;
END$DO$;

DO $DO$
  DECLARE
  ans boolean;
  BEGIN
    select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_privilege') into ans;
    if ans = true then
       GRANT SELECT ON TABLE pg_catalog.gs_auditing_privilege TO public;
    end if;
END$DO$;

DO $DO$
DECLARE
 user_name text;
 query_str text;
 ans boolean;
BEGIN
  select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_labels') into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'REVOKE ALL ON TABLE pg_catalog.gs_labels FROM ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
  end if;
END$DO$;

DO $DO$
DECLARE
  user_name text;
  query_str text;
  ans boolean;
BEGIN
  select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_masking') into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'REVOKE ALL ON TABLE pg_catalog.gs_masking FROM ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
  end if;
END$DO$;

DO $DO$
DECLARE
  user_name text;
  query_str text;
  ans boolean;
BEGIN
  select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing') into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'REVOKE ALL ON TABLE pg_catalog.gs_auditing FROM ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
  end if;
END$DO$;

DO $DO$
DECLARE
  user_name text;
  query_str text;
  ans boolean;
BEGIN
  select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_access') into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'REVOKE ALL ON TABLE pg_catalog.gs_auditing_access FROM ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
  end if;
END$DO$;

DO $DO$
DECLARE
  user_name text;
  query_str text;
  ans boolean;
BEGIN
  select case when count(*)>0 then true else false end as ans from (select relname from pg_class where relname='gs_auditing_privilege') into ans;
  if ans = true then
    SELECT SESSION_USER INTO user_name;
    query_str := 'REVOKE ALL ON TABLE pg_catalog.gs_auditing_privilege FROM ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
  end if;
END$DO$;

drop function if exists pg_catalog.get_large_table_name;

DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS DBE_PERF.get_global_gs_asp() cascade;
    end if;
END$$;
DROP INDEX IF EXISTS pg_catalog.gs_asp_sample_time_index;
DROP TYPE IF EXISTS pg_catalog.gs_asp;
DROP TABLE IF EXISTS pg_catalog.gs_asp;

CREATE OR REPLACE FUNCTION  pg_catalog.proc_add_partition(
                  IN        relationname         name,
                  IN        boundaries_interval  INTERVAL
)
RETURNS void
AS
$$
DECLARE
    pgclass_rec         pg_class%rowtype;
    row_name            record;
    sql                 text;
    part_relname        text;
    check_count         int;
    rel_oid             int;
    job_id              int;
    time_interval       int;
    part_boundarie      timestamptz;
    partition_tmp       timestamptz;
    username            name;
    database            name;
    now_timestamp       timestamptz;
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

    /* skip for cluster unstable */
    IF (SELECT count(*) FROM pg_settings WHERE name='enable_prevent_job_task_startup' AND setting='on') > 0
    THEN
       RAISE WARNING 'skip proc_add_partition() when enable_prevent_job_task_startup = on';
       RETURN;
    END IF;
    IF (SELECT count(*) FROM pg_settings WHERE name='enable_online_ddl_waitlock' AND setting='on') > 0
    THEN
       RAISE WARNING 'skip proc_add_partition() when enable_online_ddl_waitlock = on';
       RETURN;
    END IF;
    /* skip for cluster unstable */

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
    IF pos != 0 THEN
        namespace = substring(relationname from 1 for pos -1);
        table_name = substring(relationname from pos+1 for char_length(relationname) - pos);
    END IF;
    /* check parameter end */
    sql = 'select current_database();';
    EXECUTE sql INTO database;
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

    /* check partition table */
    IF pos = 0 THEN
        sql := 'SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || relationname || '''
            AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    ELSE
        sql := 'SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = ''' || table_name || '''
            AND n.nspname = ''' || namespace || ''' and pg_catalog.pg_get_userbyid(c.relowner) = ''' || username || ''';';
    END IF;
    EXECUTE sql into rel_oid;
    sql := 'SELECT relkind,parttype FROM pg_class WHERE oid = ''' || rel_oid ||'''';
    EXECUTE sql into pgclass_rec.relkind, pgclass_rec.parttype;
    IF pgclass_rec.relkind != 'r' THEN
        RAISE EXCEPTION  ' % is not a table !',relationname;
    END IF;
    IF pgclass_rec.parttype != 'p' THEN
        RAISE EXCEPTION  ' % does not have partition !',relationname;
    END IF;
    /* check partition table end */

    /* iteratively checking time range for every partition*/
    sql := 'SELECT boundaries[1] FROM pg_partition WHERE parentid = ' || rel_oid ||' AND boundaries IS NOT NULL ORDER BY
    EXTRACT(epoch FROM CAST(boundaries[1] as timestamptz)) DESC LIMIT 1';
    EXECUTE sql INTO part_boundarie;

    /* reinforce the job failed to throw error when inserting data */
    sql := 'select current_timestamp(0)';
    EXECUTE(sql) into now_timestamp;
    WHILE  part_boundarie - 20 * boundaries_interval < now_timestamp  LOOP
        part_boundarie = part_boundarie + boundaries_interval;
        sql = 'select proc_add_partition_by_boundary(''' || relationname || ''', ' ||  '''' || part_boundarie || ''');';
        EXECUTE(sql);
    END LOOP;

    part_boundarie := part_boundarie + boundaries_interval;
    partition_tmp = date_trunc('second', part_boundarie);
    sql :='ALTER TABLE '||relationname||' ADD PARTITION p'||EXTRACT(epoch FROM CAST(partition_tmp AS TIMESTAMPTZ))||' values less than ('''||part_boundarie||''');';
    EXECUTE (sql);

    /* Add regarding dependencies into pg_depend */
    sql := 'SELECT node_name,node_host,node_port FROM pgxc_node where node_type = ''C'' and nodeis_active = ''t'' ';
    FOR row_name IN EXECUTE(sql) LOOP
        sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''select c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = ''''' || relationname || '''''  AND pg_catalog.pg_table_is_visible(c.oid) and pg_catalog.pg_get_userbyid(c.relowner) = ''''' || username || ''''';  ''';
        EXECUTE sql INTO rel_oid;
        sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''  SELECT a.job_id from pg_job a , pg_job_proc b where
                b.what like  ''''%proc_add_partition('''''''''|| relationname ||'''''''''%''''  and a.job_id = b.job_id
                and a.priv_user='''''|| username ||''''' and a.dbname='''''|| database ||'''''  ''';
        EXECUTE sql INTO job_id;
        sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') '' SELECT count(*) from pg_depend where classid = 9022 and objid =' || job_id || '''';
        EXECUTE sql INTO check_count;
        /* Add regarding dependencies of proc_add_partition job */
        IF check_count = 0 THEN
            sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') '' SELECT pg_catalog.add_job_class_depend(' || job_id || ',' || rel_oid || ')''';
            EXECUTE sql INTO job_id;
        END IF;
        sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''  SELECT count(*) from pg_job a , pg_job_proc b where
               b.what like  ''''%proc_drop_partition('''''''''|| relationname ||'''''''''%''''  and a.job_id = b.job_id and a.dbname='''''|| database ||'''''  ''';
        /* Add regarding dependencies of proc_drop_partition job */
        EXECUTE sql INTO check_count;
        IF check_count = 1 THEN
            sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''  SELECT a.job_id from pg_job a , pg_job_proc b where
                   b.what like  ''''%proc_drop_partition('''''''''|| relationname ||'''''''''%''''  and a.job_id = b.job_id and a.dbname='''''|| database ||'''''  ''';
            EXECUTE sql INTO job_id;
            sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') '' SELECT count(*) from pg_depend where classid = 9022 and objid =' || job_id || '''';
            RAISE NOTICE '%',sql;
            EXECUTE sql INTO check_count;
            IF check_count = 0 THEN
                sql := 'EXECUTE DIRECT ON (' || row_name.node_name || ') '' SELECT pg_catalog.add_job_class_depend(' || job_id || ',' || rel_oid || ')''';
                EXECUTE sql INTO job_id;
            END IF;
        END IF;

        RAISE NOTICE 'succeess';
    END LOOP;
END;
$$
LANGUAGE plpgsql;

drop function if exists pg_catalog.report_application_error(text, integer) cascade;
CREATE OR REPLACE FUNCTION pg_catalog.raise_application_error(errcode integer,
												errmsg text,
												flag boolean DEFAULT false)
RETURNS integer
as '$libdir/plpgsql','raise_application_error'
LANGUAGE C STRICT VOLATILE NOT FENCED;

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbms_job' limit 1)
    LOOP
        if ans = true then
			DROP FUNCTION IF EXISTS dbms_job.remove(bigint) cascade;
			DROP FUNCTION IF EXISTS dbms_job.change(bigint, text, timestamp, text) cascade;
			DROP FUNCTION IF EXISTS dbms_job.what(bigint, text) cascade;
			DROP FUNCTION IF EXISTS dbms_job.next_date(bigint, timestamp) cascade;
			DROP FUNCTION IF EXISTS dbms_job.interval(bigint, text) cascade;
			DROP FUNCTION IF EXISTS dbms_job.broken(bigint, boolean, timestamp) cascade;
			DROP FUNCTION IF EXISTS dbms_job.isubmit(bigint, text, timestamp, text) cascade;
			DROP FUNCTION IF EXISTS dbms_job.submit(text, timestamp, text, out integer) cascade;
			DROP FUNCTION IF EXISTS dbms_job.isubmit_on_nodes(bigint, name, name, text, timestamp without time zone, text) cascade;
			DROP FUNCTION IF EXISTS dbms_job.submit_on_nodes(name, name, text, timestamp without time zone, text, OUT integer) cascade;
			DROP FUNCTION IF EXISTS dbms_job.isubmit_on_nodes_internal(bigint, name, name, text, timestamp without time zone, text) cascade;
		end if;
        exit;
    END LOOP;
END$$;

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
DROP SCHEMA IF EXISTS dbms_job cascade;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 3988;
CREATE SCHEMA dbms_job;
COMMENT ON schema dbms_job IS 'dbms_job schema';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3990;
CREATE OR REPLACE FUNCTION dbms_job.isubmit(
    in job bigint,
    in what text,
    in next_date timestamp without time zone default sysdate,
    in interval_time text default 'null'
)RETURNS void LANGUAGE INTERNAL
IMMUTABLE NOT SHIPPABLE
as 'isubmit_job';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5717;
CREATE FUNCTION dbms_job.submit_on_nodes(node_name name, database name, what text, next_date timestamp without time zone, job_interval text, OUT job integer)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'submit_job_on_nodes';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5718;
CREATE FUNCTION dbms_job.isubmit_on_nodes(job bigint, node_name name, database name, what text, next_date timestamp without time zone, job_interval text)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'isubmit_job_on_nodes';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6007;
CREATE FUNCTION dbms_job.isubmit_on_nodes_internal(job bigint, node_name name, database name, what text, next_date timestamp without time zone, job_interval text)
 RETURNS integer
 LANGUAGE internal
 NOT FENCED NOT SHIPPABLE
AS 'isubmit_job_on_nodes_internal';

CREATE OR REPLACE VIEW PG_CATALOG.DUAL AS (SELECT 'X'::TEXT AS DUMMY);
GRANT SELECT ON TABLE DUAL TO PUBLIC;

CREATE OR REPLACE VIEW pg_catalog.user_jobs AS
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

GRANT SELECT ON pg_catalog.user_jobs TO public;

CREATE OR REPLACE VIEW pg_catalog.DBA_PART_TABLES AS

	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
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
		CASE
			WHEN c.reltablespace = 0 THEN ('DEFAULT TABLESPACE'::TEXT)::NAME
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE c.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,

	----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
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

CREATE OR REPLACE VIEW pg_catalog.USER_PART_TABLES AS
	SELECT
		*
	FROM DBA_PART_TABLES
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.DBA_TAB_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS TABLE_OWNER,
	----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
	----------------------------------------------------------------------
		CAST(c.relname AS varchar2(64))  AS TABLE_NAME,
	----------------------------------------------------------------------
		CAST(p.relname AS varchar2(64)) AS PARTITION_NAME,
	----------------------------------------------------------------------
		array_to_string(p.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
	----------------------------------------------------------------------
		CASE
			WHEN p.reltablespace = 0 THEN ('DEFAULT TABLESPACE'::TEXT)::NAME
			ELSE (SELECT spc.spcname FROM pg_tablespace spc WHERE p.reltablespace = spc.oid)
		END
		AS TABLESPACE_NAME
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

CREATE OR REPLACE VIEW pg_catalog.USER_TAB_PARTITIONS AS
	SELECT
		*
	FROM DBA_TAB_PARTITIONS
	WHERE TABLE_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.DBA_PART_INDEXES AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME                 ,
		----------------------------------------------------------------------
		CAST(ct.relname AS varchar2(64)) AS TABLE_NAME                 ,
		----------------------------------------------------------------------
		CASE
			WHEN p.partstrategy = 'r' THEN 'RANGE'::TEXT
			WHEN p.partstrategy = 'i' THEN 'INTERVAL'::TEXT
			ELSE p.partstrategy
		END AS PARTITIONING_TYPE      ,
		----------------------------------------------------------------------
		(SELECT count(*) FROM pg_partition ps WHERE ps.parentid = ct.oid AND ps.parttype = 'p')
		AS PARTITION_COUNT    ,
		----------------------------------------------------------------------
		CASE
			WHEN ci.reltablespace = 0 THEN ('DEFAULT TABLESPACE'::TEXT)::NAME
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE ci.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME,
		----------------------------------------------------------------------
		array_length(p.partkey,1)as PARTITIONING_KEY_COUNT
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

CREATE OR REPLACE VIEW pg_catalog.USER_PART_INDEXES AS
	SELECT
		*
	FROM DBA_PART_INDEXES
	WHERE INDEX_OWNER	= CURRENT_USER;

CREATE OR REPLACE VIEW pg_catalog.DBA_IND_PARTITIONS AS
	SELECT
		CAST(a.rolname AS VARCHAR2(64)) AS INDEX_OWNER,
		----------------------------------------------------------------------
		CAST(n.nspname AS VARCHAR2(64)) AS SCHEMA,
		----------------------------------------------------------------------
		CAST(ci.relname AS varchar2(64)) AS INDEX_NAME,
		----------------------------------------------------------------------
		CAST(pi.relname AS varchar2(64)) AS PARTITION_NAME,
		----------------------------------------------------------------------
		pi.indisusable AS INDEX_PARTITION_USABLE,
		----------------------------------------------------------------------
		array_to_string(pt.BOUNDARIES, ',' , 'MAXVALUE') AS HIGH_VALUE,
		----------------------------------------------------------------------
		CASE
			WHEN pi.reltablespace = 0 THEN ('DEFAULT TABLESPACE'::TEXT)::NAME
			ELSE
			(SELECT spc.spcname FROM pg_tablespace spc WHERE pi.reltablespace = spc.oid)
		END
			AS DEF_TABLESPACE_NAME
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

CREATE OR REPLACE VIEW pg_catalog.USER_IND_PARTITIONS AS
	SELECT
		*
	FROM DBA_IND_PARTITIONS
	WHERE INDEX_OWNER	= CURRENT_USER;


DROP VIEW IF EXISTS PG_CATALOG.SYS_DUMMY CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_PART_TABLES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_PART_TABLES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TAB_PARTITIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_TAB_PARTITIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_PART_INDEXES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_PART_INDEXES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_IND_PARTITIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_IND_PARTITIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_JOBS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_OBJECTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_OBJECTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_PROCEDURES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TABLES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_TABLES CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_TABLES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TAB_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_TAB_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_VIEWS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_VIEWS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_VIEWS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TRIGGERS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_TRIGGERS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_TAB_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_SOURCE CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_SOURCE CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_DEPENDENCIES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_SOURCE CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_SEQUENCES CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_SEQUENCES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_SEQUENCES CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_PROCEDURES CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_INDEXES CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_INDEXES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_USERS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_USERS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_DATA_FILES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TABLESPACES CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_OBJECTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_IND_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_IND_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_IND_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_IND_EXPRESSIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_IND_EXPRESSIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_IND_EXPRESSIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_CONSTRAINTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_CONSTRAINTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_CONSTRAINTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_CONS_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_CONS_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_CONS_COLUMNS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_TAB_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_TAB_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_TAB_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DV_SESSIONS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DV_SESSION_LONGOPS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_COL_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_COL_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_COL_COMMENTS CASCADE;
DROP VIEW IF EXISTS pg_catalog.ADM_SYNONYMS CASCADE;
DROP VIEW IF EXISTS pg_catalog.MY_SYNONYMS CASCADE;
DROP VIEW IF EXISTS pg_catalog.DB_SYNONYMS CASCADE;


--because tsdb functions use job views and job system function, so we should reload the extension
DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(int, name, name,text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(name, name, text) cascade;
DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(name, name) cascade;

DROP FUNCTION IF EXISTS pg_catalog.pg_lock_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 1371;
CREATE FUNCTION pg_catalog.pg_lock_status
(
OUT locktype pg_catalog.text,
OUT database pg_catalog.oid,
OUT relation pg_catalog.oid,
OUT page pg_catalog.int4,
OUT tuple pg_catalog.int2,
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


DROP FUNCTION IF EXISTS dbms_job.isubmit_on_nodes_interval(bigint, name, name, text, timestamp without time zone, text) cascade;

DROP FUNCTION IF EXISTS pg_catalog.proc_add_depend(name, name);

-- 1. drop streaming_stream and its indexes
DROP INDEX IF EXISTS pg_catalog.streaming_stream_oid_index;
DROP INDEX IF EXISTS pg_catalog.streaming_stream_relid_index;
DROP TYPE IF EXISTS pg_catalog.streaming_stream;
DROP TABLE IF EXISTS pg_catalog.streaming_stream;

-- 2. drop streaming_cont_query and its indexes
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_relid_index;
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_defrelid_index;
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_id_index;
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_oid_index;
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_matrelid_index;
DROP INDEX IF EXISTS pg_catalog.streaming_cont_query_lookupidxid_index;
DROP TYPE IF EXISTS pg_catalog.streaming_cont_query;
DROP TABLE IF EXISTS pg_catalog.streaming_cont_query;

--93
DROP VIEW IF EXISTS pg_catalog.gs_masking;

DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_filters_row_index;
DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_filters_oid_index;

DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_actions_policy_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_actions_row_index;
DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_actions_oid_index;

DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_masking_policy_oid_index;

DROP TYPE IF EXISTS pg_catalog.gs_masking_policy_filters;
DROP TABLE IF EXISTS pg_catalog.gs_masking_policy_filters;

DROP TYPE IF EXISTS pg_catalog.gs_masking_policy_actions;
DROP TABLE IF EXISTS pg_catalog.gs_masking_policy_actions;

DROP TYPE IF EXISTS pg_catalog.gs_masking_policy;
DROP TABLE IF EXISTS pg_catalog.gs_masking_policy;

DROP FUNCTION IF EXISTS dbms_job.isubmit_on_nodes(bigint, name, name, text, timestamp without time zone, text) cascade;
DROP FUNCTION IF EXISTS dbms_job.submit_on_nodes(name, name, text, timestamp without time zone, text, OUT integer) cascade;
DROP FUNCTION IF EXISTS pg_catalog.capture_view_to_json(text, integer) cascade;

-- 1. drop new system view gs_auditing(_xxx)
DROP VIEW IF EXISTS pg_catalog.gs_auditing_access CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_auditing_privilege CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_auditing CASCADE;

-- 2. drop system table and its index.
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_oid_index;
DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy;
DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_access_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_access_row_index;
DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_access;
DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_access;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_filters_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_filters_row_index;
DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_filters;
DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_filters;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_privileges_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_auditing_policy_privileges_row_index;
DROP TYPE  IF EXISTS pg_catalog.gs_auditing_policy_privileges;
DROP TABLE IF EXISTS pg_catalog.gs_auditing_policy_privileges;

DROP FUNCTION IF EXISTS pg_catalog.get_local_active_session() cascade;


DROP INDEX IF EXISTS pg_catalog.pg_asp_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_asp;
DROP TABLE IF EXISTS pg_catalog.pg_asp;

DROP FUNCTION IF EXISTS pg_catalog.get_wait_event_info() cascade;

DROP VIEW IF EXISTS pg_catalog.pgxc_wlm_session_history CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_history() CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_history CASCADE;

DROP VIEW IF EXISTS pg_catalog.pgxc_wlm_session_info CASCADE;
DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info_all CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_info() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgxc_get_wlm_session_info_bytime(text, timestamp without time zone, timestamp without time zone, integer) CASCADE;


DROP TABLE IF EXISTS pg_catalog.gs_wlm_session_query_info_all CASCADE;

DO $$
BEGIN
    IF EXISTS(
        SELECT tablename FROM pg_tables where tablename='gs_wlm_session_info')
    THEN
        DROP TABLE IF EXISTS pg_catalog.gs_wlm_session_info CASCADE;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS(
        SELECT viewname FROM pg_views where viewname='gs_wlm_session_info')
    THEN
        DROP VIEW IF EXISTS pg_catalog.gs_wlm_session_info CASCADE;
    END IF;
END $$;

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
 OUT top_cpu_dn text) RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'pg_stat_get_wlm_session_info';

 create table pg_catalog.gs_wlm_session_info
(
	datid	     		Oid,
	dbname	     		text,
	schemaname    		text,
	nodename    		text,
	username    		text,
	application_name	text,
	client_addr    		inet,
	client_hostname 	text,
	client_port 		int,
	query_band    		text,
	block_time    		bigint,
	start_time   		timestamp with time zone,
	finish_time   		timestamp with time zone,
	duration      		bigint,
	estimate_total_time	bigint,
	status      		text,
	abort_info  		text,
	resource_pool 		text,
	control_group 		text,
	estimate_memory		int,
	min_peak_memory		int,
	max_peak_memory		int,
	average_peak_memory	int,
	memory_skew_percent	int,
	spill_info  		text,
	min_spill_size		int,
	max_spill_size		int,
	average_spill_size	int,
	spill_skew_percent	int,
	min_dn_time	    	bigint,
	max_dn_time	    	bigint,
	average_dn_time		bigint,
	dntime_skew_percent	int,
	min_cpu_time		bigint,
	max_cpu_time		bigint,
	total_cpu_time  	bigint,
	cpu_skew_percent	int,
	min_peak_iops		int,
	max_peak_iops		int,
	average_peak_iops	int,
	iops_skew_percent	int,
	warning	    		text,
	queryid      		bigint NOT NULL,
	query       		text,
	query_plan	    	text,
	node_group		text,
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
	top_cpu_dn text
);

CREATE VIEW pg_catalog.gs_wlm_session_info_all AS
SELECT * FROM pg_catalog.pg_stat_get_wlm_session_info(0);

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
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_catalog.gs_wlm_session_history''';
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
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''SELECT * FROM pg_catalog.gs_wlm_session_info''';
                        FOR row_data IN EXECUTE(query_str) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.pgxc_get_wlm_session_info_bytime(text, TIMESTAMP, TIMESTAMP, int)
RETURNS setof pg_catalog.gs_wlm_session_info
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
                query_str_cn := 'SELECT * FROM pg_catalog.gs_wlm_session_info where '||$1||'>'''''||$2||''''' and '||$1||'<'''''||$3||''''' limit '||$4;
                FOR row_name IN EXECUTE(query_str_nodes) LOOP
                        query_str := 'EXECUTE DIRECT ON (' || row_name.node_name || ') ''' || query_str_cn||''';';
                        FOR row_data IN EXECUTE(query_str) LOOP
                                return next row_data;
                        END LOOP;
                END LOOP;
                return;
        END; $$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE VIEW pg_catalog.pgxc_wlm_session_history AS
SELECT * FROM pg_catalog.pgxc_get_wlm_session_history() AS
(
datid  Oid,
dbname text,
schemaname text,
nodename text,
username text,
application_name text,
client_addr inet,
client_hostname text,
client_port int,
query_band text,
block_time bigint,
start_time timestamp with time zone,
finish_time timestamp with time zone,
duration bigint,
estimate_total_time bigint,
status text,
abort_info text,
resource_pool text,
control_group text,
estimate_memory int,
min_peak_memory int,
max_peak_memory int,
average_peak_memory int,
memory_skew_percent int,
spill_info text,
min_spill_size int,
max_spill_size int,
average_spill_size int,
spill_skew_percent int,
min_dn_time bigint,
max_dn_time bigint,
average_dn_time bigint,
dntime_skew_percent int,
min_cpu_time bigint,
max_cpu_time bigint,
total_cpu_time bigint,
cpu_skew_percent int,
min_peak_iops int,
max_peak_iops int,
average_peak_iops int,
iops_skew_percent int,
warning text,
queryid bigint,
query text,
query_plan text,
node_group text,
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
top_cpu_dn text
);

CREATE VIEW pg_catalog.pgxc_wlm_session_info AS
SELECT * FROM pg_catalog.pgxc_get_wlm_session_info() AS
(
	datid	     		Oid,
	dbname	     		text,
	schemaname    		text,
	nodename    		text,
	username    		text,
	application_name	text,
	client_addr    		inet,
	client_hostname 	text,
	client_port 		int,
	query_band    		text,
	block_time    		bigint,
	start_time   		timestamp with time zone,
	finish_time   		timestamp with time zone,
	duration      		bigint,
	estimate_total_time	bigint,
	status      		text,
	abort_info  		text,
	resource_pool 		text,
	control_group 		text,
	estimate_memory		int,
	min_peak_memory		int,
	max_peak_memory		int,
	average_peak_memory	int,
	memory_skew_percent	int,
	spill_info  		text,
	min_spill_size		int,
	max_spill_size		int,
	average_spill_size	int,
	spill_skew_percent	int,
	min_dn_time	    	bigint,
	max_dn_time	    	bigint,
	average_dn_time		bigint,
	dntime_skew_percent	int,
	min_cpu_time		bigint,
	max_cpu_time		bigint,
	total_cpu_time  	bigint,
	cpu_skew_percent	int,
	min_peak_iops		int,
	max_peak_iops		int,
	average_peak_iops	int,
	iops_skew_percent	int,
	warning	    		text,
	queryid      		bigint,
	query       		text,
	query_plan	    	text,
	node_group		text,
	cpu_top1_node_name	text,
	cpu_top2_node_name	text,
	cpu_top3_node_name	text,
	cpu_top4_node_name	text,
	cpu_top5_node_name	text,
	mem_top1_node_name	text,
	mem_top2_node_name	text,
	mem_top3_node_name	text,
	mem_top4_node_name	text,
	mem_top5_node_name	text,
	cpu_top1_value		bigint,
	cpu_top2_value		bigint,
	cpu_top3_value		bigint,
	cpu_top4_value		bigint,
	cpu_top5_value		bigint,
	mem_top1_value		bigint,
	mem_top2_value		bigint,
	mem_top3_value		bigint,
	mem_top4_value		bigint,
	mem_top5_value		bigint,
	top_mem_dn		text,
	top_cpu_dn		text
);


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
			EXECUTE 'INSERT INTO pg_catalog.gs_wlm_session_info ' || query_str;
		ELSE
			EXECUTE query_str;
		END IF;

		RETURN record_cnt;
	END; $$
LANGUAGE plpgsql NOT FENCED;


GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_history TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_wlm_session_info_all TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.pgxc_wlm_session_history TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.pgxc_wlm_session_info TO PUBLIC;

DROP FUNCTION IF EXISTS pg_catalog.gs_stat_activity_timeout(IN timeout_threshold int4, OUT datid oid, OUT pid INT8, OUT sessionid INT8, OUT usesysid oid, OUT application_name text, OUT query text, OUT xact_start timestamptz, OUT query_start timestamptz, OUT query_id INT8) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.global_stat_activity_timeout(in execute_time int4, out nodename text, out datid oid, out pid int8, out sessionid int8, out usesysid oid, out application_name text, out query text, out xact_start timestamptz, out query_start timestamptz, out query_id int8) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.series(name);

DROP FUNCTION IF EXISTS pg_catalog.labels(name);

DROP FUNCTION IF EXISTS pg_catalog.labels_count(name);

drop aggregate if exists pg_catalog.spread(real);

drop function if exists pg_catalog.spread_collect(real[], real[]);

drop function if exists pg_catalog.spread_final(real[]);

drop function if exists pg_catalog.spread_internal(real[], real);

drop aggregate if exists pg_catalog.delta(anyelement);

drop function if exists pg_catalog.delta_final(anyarray);

drop function if exists pg_catalog.delta_internal(anyarray, anyelement);

DROP VIEW IF EXISTS pg_catalog.pg_roles_v1 cascade;

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
        rolspillspace
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid);

GRANT SELECT ON TABLE pg_catalog.pg_roles TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_user_v1 cascade;

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
        rolspillspace AS spillspacelimit
    FROM pg_authid LEFT JOIN pg_db_role_setting s
    ON (pg_authid.oid = setrole AND setdatabase = 0)
    LEFT JOIN pgxc_group
    ON (pg_authid.rolnodegroup = pgxc_group.oid)
    WHERE rolcanlogin;

GRANT SELECT ON TABLE pg_catalog.pg_user TO PUBLIC;

DROP VIEW IF EXISTS pg_catalog.pg_shadow_v1 cascade;

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
        rolspillspace AS spillspacelimit
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

DROP VIEW IF EXISTS pg_catalog.pg_rlspolicies_v1 cascade;

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

DROP FUNCTION IF EXISTS pg_catalog.gs_wlm_user_resource_info(CSTRING);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,5012;
CREATE FUNCTION pg_catalog.gs_wlm_user_resource_info(cstring, OUT userid oid, OUT used_memory int4, OUT total_memory int4, OUT used_cpu float, OUT total_cpu int4, OUT used_space int8, OUT total_space int8, OUT used_temp_space int8, OUT total_temp_space int8, OUT used_spill_space int8, OUT total_spill_space int8, OUT read_kbytes int8, OUT write_kbytes int8, OUT read_counts int8, OUT write_counts int8, OUT read_speed float, OUT write_speed float) returns setof record language internal stable rows 100 as 'gs_wlm_user_resource_info';

DROP VIEW IF EXISTS pg_catalog.pg_total_user_resource_info_oid CASCADE;
CREATE OR REPLACE VIEW pg_catalog.pg_total_user_resource_info_oid AS
SELECT * FROM gs_wlm_get_all_user_resource_info() AS
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

GRANT SELECT ON pg_catalog.pg_total_user_resource_info_oid TO public;

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

DROP FUNCTION IF EXISTS pg_catalog.get_instr_workload_info(int4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5000;
CREATE FUNCTION pg_catalog.get_instr_workload_info
(
int4,
OUT resourcepool_oid bigint,
OUT commit_counter integer,
OUT rollback_counter integer,
OUT resp_min bigint,
OUT resp_max bigint,
OUT resp_avg bigint,
OUT resp_total bigint,
OUT bg_commit_counter integer,
OUT bg_rollback_counter integer,
OUT bg_resp_min bigint,
OUT bg_resp_max bigint,
OUT bg_resp_avg bigint,
OUT bg_resp_total bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_workload_info';


DROP FUNCTION IF EXISTS pg_catalog.get_instr_workload_info(int4) cascade;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5000;
CREATE FUNCTION pg_catalog.get_instr_workload_info
(
int4,
OUT user_oid bigint,
OUT commit_counter bigint,
OUT rollback_counter bigint,
OUT resp_min bigint,
OUT resp_max bigint,
OUT resp_avg bigint,
OUT resp_total bigint,
OUT bg_commit_counter bigint,
OUT bg_rollback_counter bigint,
OUT bg_resp_min bigint,
OUT bg_resp_max bigint,
OUT bg_resp_avg bigint,
OUT bg_resp_total bigint
)
RETURNS setof record LANGUAGE INTERNAL VOLATILE NOT FENCED as 'get_instr_workload_info';

-- roll back 3956
DROP FUNCTION IF EXISTS pg_catalog.threadpool_status;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,3956;
CREATE OR REPLACE FUNCTION pg_catalog.threadpool_status(
    OUT node_name TEXT,
    OUT group_id INT,
    OUT bind_numa_id INT,
    OUT bind_cpu_number INT,
    OUT listener INT,
    OUT worker_info TEXT,
    OUT session_info TEXT)
RETURNS RECORD LANGUAGE INTERNAL as 'gs_threadpool_status';


DROP FUNCTION IF EXISTS pg_catalog.table_skewness(text, OUT DNName text, OUT Num text, OUT Ratio text);
DROP FUNCTION IF EXISTS pg_catalog.distributed_count(IN _table_name text, OUT DNName text, OUT Num text, OUT Ratio text);

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
		query_str_nodes := 'SELECT node_name FROM pgxc_node where node_type IN (''C'', ''D'') AND nodeis_active = true AND oid::text in ' ||
		'(SELECT regexp_split_to_table(nodeoids::text, '' '') FROM pgxc_class WHERE pcrelid=''' || _table_name || '''::regclass::oid)';

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
			RETURN QUERY EXECUTE 'SELECT * FROM distributed_count(''' || $1 || ''') ORDER BY num DESC, dnname';
		END IF;
	END; $$
LANGUAGE plpgsql NOT FENCED;

DROP FUNCTION if exists pg_catalog.pgxc_lock_for_transfer(OUT pgxc_lock_for_transfer bool, IN schemaName Name) cascade;
DROP FUNCTION if exists pg_catalog.pgxc_unlock_for_transfer(OUT pgxc_lock_for_transfer bool, IN schemaName Name) cascade;

DROP VIEW IF EXISTS pg_catalog.gs_labels;

DROP INDEX IF EXISTS pg_catalog.gs_policy_label_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_policy_label_oid_index;

DROP TYPE IF EXISTS pg_catalog.gs_policy_label;
DROP TABLE IF EXISTS pg_catalog.gs_policy_label;

drop type if exists pg_catalog.gs_global_config cascade;
DROP TABLE IF EXISTS pg_catalog.gs_global_config cascade;

-- ----------------------------------------------------------------
-- rollback comm_check_connection_status
-- ----------------------------------------------------------------

DROP FUNCTION IF EXISTS pg_catalog.comm_check_connection_status(OUT node_name text, OUT remote_name text, OUT remote_host text, OUT remote_port int4, OUT is_connected bool) CASCADE;
