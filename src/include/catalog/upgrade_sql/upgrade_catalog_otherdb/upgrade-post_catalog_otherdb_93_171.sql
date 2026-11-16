-- ============================================================
-- WDR Unused Indexes: persist indisunique/indisprimary (version: 93171)
-- Extend dbe_perf.*stat_*_indexes views/functions and snapshot tables so
-- Unused Indexes can exclude primary/unique indexes (idx_scan stays 0 for
-- uniqueness checks).
--
-- OID-safe: use CREATE OR REPLACE for base/filter views to preserve OIDs.
-- Functions that gain OUT columns must be DROP + CREATE (signature change);
-- dependent global/summary views are recreated after that.
-- ============================================================

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    -- Preserve OID of base view while appending columns (same pattern as 93_170).
    CREATE OR REPLACE VIEW DBE_PERF.stat_all_indexes AS
      SELECT
        C.oid AS relid,
        I.oid AS indexrelid,
        N.nspname AS schemaname,
        C.relname AS relname,
        I.relname AS indexrelname,
        pg_catalog.pg_stat_get_numscans(I.oid) AS idx_scan,
        pg_catalog.pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
        pg_catalog.pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch,
        X.indisunique AS indisunique,
        X.indisprimary AS indisprimary
      FROM pg_catalog.pg_class C
           JOIN pg_catalog.pg_index X ON C.oid = X.indrelid
           JOIN pg_catalog.pg_class I ON I.oid = X.indexrelid
           LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't');

    -- OUT-parameter list changed: CREATE OR REPLACE cannot alter return type.
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_stat_all_indexes() CASCADE;
    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_all_indexes
      (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
       OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint,
       OUT indisunique boolean, OUT indisprimary boolean)
    RETURNS setof record
    AS $$
    DECLARE
      row_data record;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.stat_all_indexes';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          relid := row_data.relid;
          indexrelid := row_data.indexrelid;
          schemaname := row_data.schemaname;
          relname := row_data.relname;
          indexrelname := row_data.indexrelname;
          idx_scan := row_data.idx_scan;
          idx_tup_read := row_data.idx_tup_read;
          idx_tup_fetch := row_data.idx_tup_fetch;
          indisunique := row_data.indisunique;
          indisprimary := row_data.indisprimary;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE OR REPLACE VIEW DBE_PERF.global_stat_all_indexes AS
      SELECT * FROM DBE_PERF.get_global_stat_all_indexes();

    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_stat_all_indexes() CASCADE;
    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_all_indexes
      (OUT schemaname name, OUT relname name, OUT indexrelname name,
       OUT toastrelschemaname name, OUT toastrelname name,
       OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint,
       OUT indisunique boolean, OUT indisprimary boolean)
    RETURNS setof record
    AS $$
    DECLARE
      row_data record;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := '
        SELECT
          T.relname AS relname,
          T.schemaname AS schemaname,
          C.relname AS toastrelname,
          N.nspname AS toastrelschemaname,
          T.indexrelname AS indexrelname,
          T.idx_scan AS idx_scan,
          T.idx_tup_read AS idx_tup_read,
          T.idx_tup_fetch AS idx_tup_fetch,
          T.indisunique AS indisunique,
          T.indisprimary AS indisprimary
        FROM dbe_perf.stat_all_indexes T
          LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
          LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
        FOR row_data IN EXECUTE(query_str) LOOP
          schemaname := row_data.schemaname;
          IF row_data.toastrelname IS NULL THEN
              relname := row_data.relname;
              indexrelname := row_data.indexrelname;
          ELSE
              relname := NULL;
              indexrelname := NULL;
          END IF;
          toastrelschemaname := row_data.toastrelschemaname;
          toastrelname := row_data.toastrelname;
          idx_scan := row_data.idx_scan;
          idx_tup_read := row_data.idx_tup_read;
          idx_tup_fetch := row_data.idx_tup_fetch;
          indisunique := row_data.indisunique;
          indisprimary := row_data.indisprimary;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE OR REPLACE VIEW DBE_PERF.summary_stat_all_indexes AS
      SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
             COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
             pg_catalog.SUM(Ti.idx_scan) idx_scan,
             pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read,
             pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch,
             pg_catalog.bool_or(Ti.indisunique) AS indisunique,
             pg_catalog.bool_or(Ti.indisprimary) AS indisprimary
        FROM DBE_PERF.get_summary_stat_all_indexes() AS Ti
             LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
             ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
        GROUP BY (1, 2, 3);

    -- Refresh SELECT * projections so new columns are visible; preserve OIDs.
    CREATE OR REPLACE VIEW DBE_PERF.stat_user_indexes AS
      SELECT * FROM DBE_PERF.stat_all_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
              schemaname !~ '^pg_toast';

    DROP FUNCTION IF EXISTS DBE_PERF.get_global_stat_user_indexes() CASCADE;
    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_user_indexes
      (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
       OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint,
       OUT indisunique boolean, OUT indisprimary boolean)
    RETURNS setof record
    AS $$
    DECLARE
      row_data record;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := 'SELECT * FROM dbe_perf.stat_user_indexes';
        FOR row_data IN EXECUTE(query_str) LOOP
          node_name := row_name.node_name;
          relid := row_data.relid;
          indexrelid := row_data.indexrelid;
          schemaname := row_data.schemaname;
          relname := row_data.relname;
          indexrelname := row_data.indexrelname;
          idx_scan := row_data.idx_scan;
          idx_tup_read := row_data.idx_tup_read;
          idx_tup_fetch := row_data.idx_tup_fetch;
          indisunique := row_data.indisunique;
          indisprimary := row_data.indisprimary;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE OR REPLACE VIEW DBE_PERF.global_stat_user_indexes AS
      SELECT * FROM DBE_PERF.get_global_stat_user_indexes();

    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_stat_user_indexes() CASCADE;
    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_user_indexes
      (OUT schemaname name, OUT relname name, OUT indexrelname name,
       OUT toastrelschemaname name, OUT toastrelname name,
       OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint,
       OUT indisunique boolean, OUT indisprimary boolean)
    RETURNS setof record
    AS $$
    DECLARE
      row_data record;
      row_name record;
      query_str text;
      query_str_nodes text;
    BEGIN
      query_str_nodes := 'select * from dbe_perf.node_name';
      FOR row_name IN EXECUTE(query_str_nodes) LOOP
        query_str := '
        SELECT
          T.relname AS relname,
          T.schemaname AS schemaname,
          C.relname AS toastrelname,
          N.nspname AS toastrelschemaname,
          T.indexrelname AS indexrelname,
          T.idx_scan AS idx_scan,
          T.idx_tup_read AS idx_tup_read,
          T.idx_tup_fetch AS idx_tup_fetch,
          T.indisunique AS indisunique,
          T.indisprimary AS indisprimary
        FROM dbe_perf.stat_user_indexes T
          LEFT JOIN pg_class C ON T.relid = C.reltoastrelid
          LEFT JOIN pg_namespace N ON C.relnamespace = N.oid';
        FOR row_data IN EXECUTE(query_str) LOOP
          schemaname := row_data.schemaname;
          IF row_data.toastrelname IS NULL THEN
              relname := row_data.relname;
              indexrelname := row_data.indexrelname;
          ELSE
              relname := NULL;
              indexrelname := NULL;
          END IF;
          toastrelschemaname := row_data.toastrelschemaname;
          toastrelname := row_data.toastrelname;
          idx_scan := row_data.idx_scan;
          idx_tup_read := row_data.idx_tup_read;
          idx_tup_fetch := row_data.idx_tup_fetch;
          indisunique := row_data.indisunique;
          indisprimary := row_data.indisprimary;
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE OR REPLACE VIEW DBE_PERF.summary_stat_user_indexes AS
      SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
             COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
             pg_catalog.SUM(Ti.idx_scan) idx_scan,
             pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read,
             pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch,
             pg_catalog.bool_or(Ti.indisunique) AS indisunique,
             pg_catalog.bool_or(Ti.indisprimary) AS indisprimary
        FROM DBE_PERF.get_summary_stat_user_indexes() as Ti
             LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
             ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
        GROUP BY (1, 2, 3);

    CREATE OR REPLACE VIEW DBE_PERF.stat_sys_indexes AS
      SELECT * FROM DBE_PERF.stat_all_indexes
        WHERE schemaname IN ('pg_catalog', 'information_schema') OR
              schemaname ~ '^pg_toast';

    GRANT SELECT ON TABLE DBE_PERF.stat_all_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_stat_all_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_stat_all_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.stat_user_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.global_stat_user_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.summary_stat_user_indexes TO PUBLIC;
    GRANT SELECT ON TABLE DBE_PERF.stat_sys_indexes TO PUBLIC;
  END IF;
END $DO$;

-- Add columns on existing snapshot tables (no-op if tables were never created).
-- New clusters create snap_* from current view attrs on first WDR snapshot.
DO $DO$
DECLARE
v_table_exist boolean;
v_col_exist boolean;
BEGIN
    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid
       AND n.nspname = 'snapshot'
       AND c.relname = 'snap_global_stat_all_indexes';
    IF v_table_exist THEN
        SELECT count(*) = 0 INTO v_col_exist
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'snapshot.snap_global_stat_all_indexes'::regclass
           AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attname = 'snap_indisunique';
        IF v_col_exist THEN
            EXECUTE 'ALTER TABLE snapshot.snap_global_stat_all_indexes ADD COLUMN snap_indisunique boolean';
        END IF;
        SELECT count(*) = 0 INTO v_col_exist
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'snapshot.snap_global_stat_all_indexes'::regclass
           AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attname = 'snap_indisprimary';
        IF v_col_exist THEN
            EXECUTE 'ALTER TABLE snapshot.snap_global_stat_all_indexes ADD COLUMN snap_indisprimary boolean';
        END IF;
    END IF;

    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid
       AND n.nspname = 'snapshot'
       AND c.relname = 'snap_summary_stat_all_indexes';
    IF v_table_exist THEN
        SELECT count(*) = 0 INTO v_col_exist
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'snapshot.snap_summary_stat_all_indexes'::regclass
           AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attname = 'snap_indisunique';
        IF v_col_exist THEN
            EXECUTE 'ALTER TABLE snapshot.snap_summary_stat_all_indexes ADD COLUMN snap_indisunique boolean';
        END IF;
        SELECT count(*) = 0 INTO v_col_exist
          FROM pg_catalog.pg_attribute a
         WHERE a.attrelid = 'snapshot.snap_summary_stat_all_indexes'::regclass
           AND a.attnum > 0 AND NOT a.attisdropped
           AND a.attname = 'snap_indisprimary';
        IF v_col_exist THEN
            EXECUTE 'ALTER TABLE snapshot.snap_summary_stat_all_indexes ADD COLUMN snap_indisprimary boolean';
        END IF;
    END IF;
END $DO$;
