-- ============================================================
-- LWLock/lock activity statistics - rollback script (version: 93160)
-- ============================================================

-- restore get_instr_wait_event to the old definition
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
    OUT last_updated timestamp with time zone
)
RETURNS SETOF RECORD STABLE ROWS 100 LANGUAGE INTERNAL NOT FENCED NOT SHIPPABLE AS 'get_instr_wait_event';

DROP VIEW IF EXISTS dbe_perf.global_wait_events CASCADE;
DROP FUNCTION IF EXISTS dbe_perf.get_global_wait_events() CASCADE;
DROP VIEW IF EXISTS dbe_perf.wait_events CASCADE;
CREATE VIEW dbe_perf.wait_events AS SELECT * FROM pg_catalog.get_instr_wait_event(NULL);

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

-- remove added columns from snapshot.snap_global_wait_events
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
            SELECT col_name
              FROM (VALUES
                ('snap_request_count'),
                ('snap_nw_acquired'),
                ('snap_nw_not_acquired')
              ) AS t(col_name)
        LOOP
            -- check whether the column exists before dropping
            SELECT count(*) > 0 INTO v_col_exist
              FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = 'snapshot.snap_global_wait_events'::regclass
               AND a.attnum > 0
               AND NOT a.attisdropped
               AND a.attname = v_col.col_name;

            IF v_col_exist THEN
                EXECUTE 'ALTER TABLE snapshot.snap_global_wait_events DROP COLUMN ' ||
                        v_col.col_name;
            END IF;
        END LOOP;
    END IF;
END$$;
