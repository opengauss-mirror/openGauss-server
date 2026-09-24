-- Thread IO statistics: add pg_thread_io_stat builtin function (OID 6983),
-- gs_thread_io_stat view, and dbe_perf.thread_io_stat /
-- get_global_thread_io_stat / global_thread_io_stat for existing clusters.
-- NOTE: Must run under inplace upgrade (IsInplaceUpgrade=on). Builtin OID
-- 6983 cannot be DROP'd in a normal session.

-- 1) Add pg_thread_io_stat builtin function (OID 6983)
DROP FUNCTION IF EXISTS pg_catalog.pg_thread_io_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6983;
CREATE FUNCTION pg_catalog.pg_thread_io_stat(
    OUT io_role_id integer,
    OUT role_name text,
    OUT object_name text,
    OUT context_name text,
    OUT num_reads bigint,
    OUT num_writes bigint,
    OUT bytes_read bigint,
    OUT bytes_written bigint,
    OUT read_time_ms double precision,
    OUT write_time_ms double precision,
    OUT writebacks bigint,
    OUT writeback_time_ms double precision,
    OUT max_writeback_time_ms double precision,
    OUT extend_bytes bigint,
    OUT extend_time_ms double precision,
    OUT max_extend_time_ms double precision,
    OUT hits bigint,
    OUT evictions bigint,
    OUT reuses bigint,
    OUT fsyncs bigint,
    OUT total_fsync_time_ms double precision,
    OUT max_read_time_ms double precision,
    OUT max_write_time_ms double precision
) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 1000 AS 'pg_thread_io_stat';

-- 2) Add gs_thread_io_stat view
DROP VIEW IF EXISTS pg_catalog.gs_thread_io_stat CASCADE;
CREATE VIEW pg_catalog.gs_thread_io_stat AS SELECT * FROM pg_catalog.pg_thread_io_stat();

-- 3) Add dbe_perf thread IO views and function (dependency order:
--    thread_io_stat view -> get_global_thread_io_stat function ->
--    global_thread_io_stat view)
DO $DO$
DECLARE
ans boolean;
user_name text;
query_str text;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_thread_io_stat() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_thread_io_stat CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.thread_io_stat CASCADE;

    CREATE VIEW DBE_PERF.thread_io_stat AS
      SELECT * FROM pg_catalog.pg_thread_io_stat();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_thread_io_stat
      (OUT node_name name, OUT io_role_id integer, OUT role_name text,
       OUT object_name text, OUT context_name text,
       OUT num_reads bigint, OUT num_writes bigint, OUT bytes_read bigint, OUT bytes_written bigint,
       OUT read_time_ms double precision, OUT write_time_ms double precision, OUT writebacks bigint,
       OUT writeback_time_ms double precision, OUT max_writeback_time_ms double precision, OUT extend_bytes bigint,
       OUT extend_time_ms double precision, OUT max_extend_time_ms double precision, OUT hits bigint,
       OUT evictions bigint, OUT reuses bigint, OUT fsyncs bigint, OUT total_fsync_time_ms double precision,
       OUT max_read_time_ms double precision, OUT max_write_time_ms double precision)
    RETURNS setof record
    AS $$
    DECLARE
      row_data DBE_PERF.thread_io_stat%rowtype;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.thread_io_stat';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          io_role_id := row_data.io_role_id;
          role_name := row_data.role_name;
          object_name := row_data.object_name;
          context_name := row_data.context_name;
          num_reads := row_data.num_reads;
          num_writes := row_data.num_writes;
          bytes_read := row_data.bytes_read;
          bytes_written := row_data.bytes_written;
          read_time_ms := row_data.read_time_ms;
          write_time_ms := row_data.write_time_ms;
          writebacks := row_data.writebacks;
          writeback_time_ms := row_data.writeback_time_ms;
          max_writeback_time_ms := row_data.max_writeback_time_ms;
          extend_bytes := row_data.extend_bytes;
          extend_time_ms := row_data.extend_time_ms;
          max_extend_time_ms := row_data.max_extend_time_ms;
          hits := row_data.hits;
          evictions := row_data.evictions;
          reuses := row_data.reuses;
          fsyncs := row_data.fsyncs;
          total_fsync_time_ms := row_data.total_fsync_time_ms;
          max_read_time_ms := row_data.max_read_time_ms;
          max_write_time_ms := row_data.max_write_time_ms;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.global_thread_io_stat AS
      SELECT DISTINCT * FROM DBE_PERF.get_global_thread_io_stat();

    REVOKE ALL ON DBE_PERF.thread_io_stat FROM PUBLIC;
    REVOKE ALL ON DBE_PERF.global_thread_io_stat FROM PUBLIC;
    SELECT SESSION_USER INTO user_name;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.thread_io_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    query_str := 'GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE DBE_PERF.global_thread_io_stat TO ' || quote_ident(user_name) || ';';
    EXECUTE IMMEDIATE query_str;
    GRANT SELECT ON TABLE DBE_PERF.thread_io_stat TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_thread_io_stat TO PUBLIC;
  END IF;
END $DO$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
