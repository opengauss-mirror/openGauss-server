-- ============================================================
-- WDR Instance IO object mapping - rollback script (version: 93170)
-- ============================================================

-- 1. restore dbe_perf.class_vital_info definition (remove relfilenode)
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    -- CREATE OR REPLACE VIEW cannot drop existing columns, so rebuild the view
    DROP VIEW IF EXISTS DBE_PERF.class_vital_info;
    CREATE VIEW DBE_PERF.class_vital_info AS
      SELECT
        C.oid AS relid,
        N.nspname AS schemaname,
        C.relname AS relname,
        C.relkind AS relkind
      FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i', 'I');
  end if;
END $DO$;

-- 2. drop snap_relfilenode from the runtime snapshot table if present
--    (no-op when the snapshot table does not exist in this database)
DO $DO$
DECLARE
v_table_exist boolean;
BEGIN
    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid
       AND n.nspname = 'snapshot'
       AND c.relname = 'snap_class_vital_info';

    IF v_table_exist THEN
        EXECUTE 'ALTER TABLE snapshot.snap_class_vital_info DROP COLUMN IF EXISTS snap_relfilenode';
    END IF;
END $DO$;
