-- Rollback thread IO statistics catalog changes from upgrade-post_catalog_*_93_164.sql
-- NOTE: Must run under inplace upgrade (IsInplaceUpgrade=on).
-- NOTE: No snapshot table operations here: snapshot tables only exist in the
-- initial database, non-initial databases must not touch them.

-- 1) Drop dbe_perf thread IO objects
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    DROP VIEW IF EXISTS DBE_PERF.global_thread_io_stat CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_thread_io_stat() CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.thread_io_stat CASCADE;
  END IF;
END $DO$;

-- 2) Drop gs_thread_io_stat view
DROP VIEW IF EXISTS pg_catalog.gs_thread_io_stat CASCADE;

-- 3) Drop pg_thread_io_stat builtin function (OID 6983)
DROP FUNCTION IF EXISTS pg_catalog.pg_thread_io_stat() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
