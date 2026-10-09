-- ============================================================
-- LWLock/lock activity statistics - upgrade script (version number: 93160)
-- modify function: get_instr_wait_event (column number extended, need to be recreated)
-- extend snapshot table: snap_global_wait_events (no-wait statistic columns)
-- ============================================================

-- 1. get_instr_wait_event (OID: 5705) - column number extended, need to be recreated
--    Note: This function is an existing function, upgrade requires replacement of definition
DROP FUNCTION IF EXISTS pg_catalog.get_instr_wait_event(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5705;
CREATE FUNCTION pg_catalog.get_instr_wait_event(
    IN param integer,
    OUT nodename text,
    OUT type text,
    OUT event text,
    OUT wait bigint,
    OUT failed_wait bigint,
    OUT total_wait_time bigint,
    OUT avg_wait_time bigint,
    OUT max_wait_time bigint,
    OUT min_wait_time bigint,
    OUT request_count bigint,
    OUT nw_acquired bigint,
    OUT nw_not_acquired bigint,
    OUT last_updated timestamp with time zone
)
RETURNS SETOF RECORD STABLE ROWS 100 LANGUAGE INTERNAL NOT FENCED NOT SHIPPABLE AS 'get_instr_wait_event';

-- 2. rebuild wait events objects
DROP VIEW IF EXISTS dbe_perf.global_wait_events CASCADE;
DROP FUNCTION IF EXISTS dbe_perf.get_global_wait_events() CASCADE;
DROP VIEW IF EXISTS dbe_perf.wait_events CASCADE;
CREATE VIEW dbe_perf.wait_events AS SELECT * FROM pg_catalog.get_instr_wait_event(NULL);

-- the function body must be kept byte-identical with performance_views.sql,
-- otherwise pg_proc.prosrc differs from a fresh install and the upgrade
-- metadata check (gs_upgradechk) fails
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
CREATE OR REPLACE VIEW dbe_perf.global_wait_events AS SELECT * FROM dbe_perf.get_global_wait_events();
GRANT SELECT ON dbe_perf.wait_events TO PUBLIC;
GRANT SELECT ON dbe_perf.global_wait_events TO PUBLIC;

-- 3. alter snapshot table for global wait events
DO $$
DECLARE
    v_table_exist boolean;
    v_col_exist   boolean;
    v_col         record;
BEGIN
    -- check whether snapshot.snap_global_wait_events exists
    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid
       AND n.nspname = 'snapshot'
       AND c.relname = 'snap_global_wait_events';

    IF v_table_exist THEN
        FOR v_col IN
            SELECT col_name, col_type, col_comment
              FROM (VALUES
                ('snap_request_count',   'bigint DEFAULT 0', 'lock/lwlock total request times'),
                ('snap_nw_acquired',     'bigint DEFAULT 0', 'non-wait lock acquired times'),
                ('snap_nw_not_acquired', 'bigint DEFAULT 0', 'non-wait lock not acquired times')
              ) AS t(col_name, col_type, col_comment)
        LOOP
            -- check whether the column already exists
            SELECT count(*) = 0 INTO v_col_exist
              FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = 'snapshot.snap_global_wait_events'::regclass
               AND a.attnum > 0
               AND NOT a.attisdropped
               AND a.attname = v_col.col_name;

            IF v_col_exist THEN
                EXECUTE 'ALTER TABLE snapshot.snap_global_wait_events ADD COLUMN ' ||
                        v_col.col_name || ' ' || v_col.col_type;
                EXECUTE 'COMMENT ON COLUMN snapshot.snap_global_wait_events.' || v_col.col_name ||
                        ' IS ''' || v_col.col_comment || '''';
            END IF;
        END LOOP;
    END IF;
END$$;
