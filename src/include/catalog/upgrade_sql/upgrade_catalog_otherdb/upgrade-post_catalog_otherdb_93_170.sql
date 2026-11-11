-- ============================================================
-- WDR Instance IO object mapping - upgrade script (version number: 93170)
-- extend view: dbe_perf.class_vital_info (add relfilenode column)
-- extend snapshot table: snap_class_vital_info (add snap_relfilenode column)
-- ============================================================

-- 1. recreate dbe_perf.class_vital_info with the relfilenode column
DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  if ans = true then
    CREATE OR REPLACE VIEW DBE_PERF.class_vital_info AS
      SELECT
        C.oid AS relid,
        N.nspname AS schemaname,
        C.relname AS relname,
        C.relkind AS relkind,
        C.relfilenode AS relfilenode
      FROM pg_catalog.pg_class C LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'i', 'I');
  end if;
END $DO$;

-- 2. add snap_relfilenode to the runtime snapshot table if it exists and
--    lacks the column (no-op when snapshots were never taken in this database)
DO $DO$
DECLARE
v_table_exist boolean;
v_col_exist boolean;
BEGIN
    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid
       AND n.nspname = 'snapshot'
       AND c.relname = 'snap_class_vital_info';

    IF v_table_exist THEN
        SELECT count(*) = 0 INTO v_col_exist
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'snapshot.snap_class_vital_info'::regclass
           AND a.attnum > 0
           AND NOT a.attisdropped
           AND a.attname = 'snap_relfilenode';

        IF v_col_exist THEN
            EXECUTE 'ALTER TABLE snapshot.snap_class_vital_info ADD COLUMN snap_relfilenode oid';
        END IF;
    END IF;
END $DO$;
