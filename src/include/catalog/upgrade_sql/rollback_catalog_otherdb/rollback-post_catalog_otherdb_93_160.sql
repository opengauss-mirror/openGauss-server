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
-- Dolphin owns a view that depends on wait_events. Preserve that view and
-- its extension membership while rebuilding wait_events in this transaction.
DO $$
DECLARE
    dolphin_view_definition text;
    dolphin_view_stub text;
BEGIN
    -- Return one NULL row when this database has no Dolphin member view.
    SELECT pg_catalog.max(pg_catalog.pg_get_viewdef(c.oid))
      INTO dolphin_view_definition
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_depend d
        ON d.classid = 'pg_catalog.pg_class'::regclass
       AND d.objid = c.oid
       AND d.deptype = 'e'
      JOIN pg_catalog.pg_extension e
        ON d.refclassid = 'pg_catalog.pg_extension'::regclass
       AND d.refobjid = e.oid
     WHERE n.nspname = 'performance_schema'
       AND c.relname = 'events_waits_summary_global_by_event_name'
       AND c.relkind = 'v'
       AND e.extname = 'dolphin';

    IF dolphin_view_definition IS NOT NULL THEN
        -- Follow 93_097: preserve the existing column names, types and order.
        SELECT 'CREATE OR REPLACE VIEW performance_schema.events_waits_summary_global_by_event_name AS SELECT ' ||
               pg_catalog.string_agg(
                   'NULL::' || pg_catalog.format_type(a.atttypid, a.atttypmod) ||
                   ' AS ' || pg_catalog.quote_ident(a.attname),
                   ', ' ORDER BY a.attnum) || ' WHERE false'
          INTO dolphin_view_stub
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'performance_schema.events_waits_summary_global_by_event_name'::regclass
           AND a.attnum > 0
           AND NOT a.attisdropped;

        -- Remove the query dependency without dropping the extension member.
        EXECUTE dolphin_view_stub;
    END IF;

    DROP VIEW IF EXISTS dbe_perf.wait_events CASCADE;
    CREATE VIEW dbe_perf.wait_events AS SELECT * FROM pg_catalog.get_instr_wait_event(NULL);

    IF dolphin_view_definition IS NOT NULL THEN
        EXECUTE 'CREATE OR REPLACE VIEW performance_schema.events_waits_summary_global_by_event_name AS '
                || dolphin_view_definition;
    END IF;
END$$;

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
