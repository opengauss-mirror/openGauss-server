DO $upgrade$
BEGIN
IF working_version_num() < 92608 then
DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info()
 RETURNS text
 LANGUAGE internal
 STABLE STRICT NOT FENCED SHIPPABLE
AS $function$get_client_info$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'security_plugin' limit 1) into ans;
    if ans = true then
        drop extension if exists security_plugin cascade;
        create extension security_plugin;
    end if;
END;

------------------------------------------------------------------------------------------------------------------------------------
DECLARE
  ans boolean;
  user_name text;
  grant_query text;
BEGIN
select case when count(*)=1 then true else false end as ans from (select nspname from pg_namespace where nspname='dbe_perf' limit 1) into ans;

-----------------------------------------------------------------------------
-- DROP: pg_catalog.pg_replication_slots
DROP VIEW IF EXISTS pg_catalog.pg_replication_slots CASCADE;

-- DROP: pg_get_replication_slots()
DROP FUNCTION IF EXISTS pg_catalog.pg_get_replication_slots(OUT slot_name text, OUT plugin text, OUT slot_type text, OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn text, OUT dummy_standby boolean, OUT confirmed_flush text) CASCADE;

-- DROP: gs_get_parallel_decode_status()
DROP FUNCTION IF EXISTS pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text, OUT reader_lsn text, OUT working_txn_cnt int8, OUT working_txn_memory int8) CASCADE;

-----------------------------------------------------------------------------
-- CREATE: pg_get_replication_slots
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3784;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_replication_slots(OUT slot_name text, OUT plugin text, OUT slot_type text, OUT datoid oid, OUT active boolean, OUT xmin xid, OUT catalog_xmin xid, OUT restart_lsn text, OUT dummy_standby boolean) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED as 'pg_get_replication_slots';

COMMENT ON FUNCTION pg_catalog.pg_get_replication_slots() is 'information about replication slots currently in use';

-- CREATE: pg_catalog.pg_replication_slots
CREATE VIEW pg_catalog.pg_replication_slots AS
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
    FROM pg_catalog.pg_get_replication_slots() AS L
            LEFT JOIN pg_database D ON (L.datoid = D.oid);

-- CREATE: dbe_perf.replication_slots
IF ans = true THEN
 
CREATE OR REPLACE VIEW dbe_perf.replication_slots AS
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
 
END IF;
-- CREATE: gs_get_parallel_decode_status
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9377;
CREATE OR REPLACE FUNCTION pg_catalog.gs_get_parallel_decode_status(OUT slot_name text, OUT parallel_decode_num int4, OUT read_change_queue_length text, OUT decode_change_queue_length text)
 RETURNS SETOF RECORD
 LANGUAGE internal
AS $function$gs_get_parallel_decode_status$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

-----------------------------------------------------------------------------
-- privileges
SELECT SESSION_USER INTO user_name;
 
-- dbe_perf
IF ans = true THEN
grant_query := 'GRANT INSERT,SELECT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON dbe_perf.replication_slots TO ' || quote_ident(user_name) || ';';
EXECUTE IMMEDIATE grant_query;
 
GRANT SELECT ON dbe_perf.replication_slots TO PUBLIC;
END IF;

-- pg_catalog
GRANT SELECT ON pg_catalog.pg_replication_slots TO PUBLIC;

-----------------------------------------------------------------------------
END;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_zone(int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_spaces(int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta_dump_slot(int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, int4) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_slot(int4, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot_dump_xid(xid, boolean) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_record(bigint) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_xid(xid) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_dump_parsepage_mv(text, bigint, text, boolean) CASCADE;
DECLARE
query_str text;
ans boolean;
old_version boolean;
has_version_proc boolean;
BEGIN
  FOR ans in select case when count(*) = 1 then true else false end as ans from (select 1 from pg_catalog.pg_extension where extname = 'hdfs_fdw' limit 1) LOOP
    IF ans = false then
      select case when count(*)=1 then true else false end as has_version_proc from (select * from pg_proc where proname = 'working_version_num' limit 1) into has_version_proc;
      IF has_version_proc = true then
        select working_version_num < 92608 as old_version from working_version_num() into old_version;
        IF old_version = true then
          raise info 'Processing hdfs extension';
          query_str := 'CREATE EXTENSION IF NOT EXISTS hdfs_fdw;';
          EXECUTE query_str;
        END IF;
      END IF;
    END IF;
  END LOOP;
END;
END IF;
END $upgrade$;
