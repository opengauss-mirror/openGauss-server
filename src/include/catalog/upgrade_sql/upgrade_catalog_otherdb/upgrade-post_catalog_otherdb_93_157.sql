-- WDR enhance: pv_instance_time.n_calls + database_sql_stat
-- and snap_global_instance_time.snap_n_calls for existing snapshot schema.
-- NOTE: Must run under inplace upgrade (IsInplaceUpgrade=on). Builtin OIDs
-- 3969/5740 cannot be DROP'd in a normal session.

-- 1) Existing snap table: add snap_n_calls if missing (user table, safe first)
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans from (
    select c.oid
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'snapshot'
      and c.relname = 'snap_global_instance_time'
      and c.relkind = 'r'
      and not exists (
        select 1 from pg_catalog.pg_attribute a
        where a.attrelid = c.oid
          and a.attname = 'snap_n_calls'
          and a.attnum > 0
          and not a.attisdropped
      )
    limit 1
  ) into ans;
  if ans = true then
    alter table snapshot.snap_global_instance_time ADD COLUMN snap_n_calls bigint;
  end if;
END$DO$;

-- 2) Recreate pv_instance_time with n_calls (OID 3969)
--    CASCADE drops dependent dbe_perf.instance_time / global_instance_time.
DROP FUNCTION IF EXISTS pg_catalog.pv_instance_time() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3969;
CREATE FUNCTION pg_catalog.pv_instance_time(
    OUT stat_id integer,
    OUT stat_name text,
    OUT value bigint,
    OUT n_calls bigint
) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 1000 AS 'pv_instance_time';

-- 2b) Recreate gs_instance_time view (SELECT * expansion is frozen at creation time)
DROP VIEW IF EXISTS pg_catalog.gs_instance_time CASCADE;
CREATE VIEW pg_catalog.gs_instance_time AS SELECT * FROM pg_catalog.pv_instance_time();

-- 3) Recreate dbe_perf instance_time / global_instance_time with n_calls
DO $DO$
DECLARE
ans boolean;
user_name text;
query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_instance_time() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_instance_time CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.instance_time CASCADE;

    CREATE VIEW DBE_PERF.instance_time AS
      SELECT * FROM pg_catalog.pv_instance_time();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_instance_time
      (OUT node_name name, OUT stat_id integer, OUT stat_name text, OUT value bigint, OUT n_calls bigint)
    RETURNS setof record
    AS $$
    DECLARE
      row_data DBE_PERF.instance_time%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.instance_time';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          stat_id := row_data.stat_id;
          stat_name := row_data.stat_name;
          value := row_data.value;
          n_calls := row_data.n_calls;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.global_instance_time AS
      SELECT DISTINCT * FROM DBE_PERF.get_global_instance_time();

    REVOKE ALL ON DBE_PERF.instance_time FROM PUBLIC;
    REVOKE ALL ON DBE_PERF.global_instance_time FROM PUBLIC;
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.instance_time TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_instance_time TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.instance_time TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_instance_time TO PUBLIC;
  END IF;
END $DO$;

-- 4) Add get_database_sql_stat (OID 5740) and dbe_perf views
DROP FUNCTION IF EXISTS pg_catalog.get_database_sql_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5740;
CREATE FUNCTION pg_catalog.get_database_sql_stat(
    OUT node_name text,
    OUT datid oid,
    OUT datname name,
    OUT n_calls bigint,
    OUT total_elapse_time bigint
) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 1000 AS 'get_database_sql_stat';

DO $DO$
DECLARE
ans boolean;
user_name text;
query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    DROP VIEW IF EXISTS DBE_PERF.summary_database_sql_stat CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_database_sql_stat() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.database_sql_stat CASCADE;

    CREATE VIEW DBE_PERF.database_sql_stat AS
      SELECT * FROM pg_catalog.get_database_sql_stat();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_database_sql_stat()
    RETURNS SETOF DBE_PERF.database_sql_stat
    AS $$
    DECLARE
      ROW_DATA DBE_PERF.database_sql_stat%ROWTYPE;
      ROW_NAME RECORD;
      QUERY_STR TEXT;
      QUERY_STR_NODES TEXT;
    BEGIN
      QUERY_STR_NODES := 'select * from dbe_perf.node_name';
      FOR ROW_NAME IN EXECUTE(QUERY_STR_NODES) LOOP
        QUERY_STR := 'SELECT * FROM dbe_perf.database_sql_stat';
        FOR ROW_DATA IN EXECUTE(QUERY_STR) LOOP
          RETURN NEXT ROW_DATA;
        END LOOP;
      END LOOP;
      RETURN;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_database_sql_stat AS
      SELECT * FROM DBE_PERF.get_summary_database_sql_stat();

    REVOKE ALL ON DBE_PERF.database_sql_stat FROM PUBLIC;
    REVOKE ALL ON DBE_PERF.summary_database_sql_stat FROM PUBLIC;
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.database_sql_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.summary_database_sql_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.database_sql_stat TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_database_sql_stat TO PUBLIC;
  END IF;
END $DO$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
