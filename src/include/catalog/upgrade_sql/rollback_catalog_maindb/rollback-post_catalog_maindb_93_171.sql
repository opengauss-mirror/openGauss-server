-- ============================================================
-- WDR Unused Indexes attr rollback (version: 93171)
-- Restore dbe_perf.*stat_*_indexes without indisunique/indisprimary
-- and drop the corresponding snapshot columns.
--
-- CREATE OR REPLACE VIEW cannot drop columns, so dependent views and
-- the base view must be DROP + CREATE (same pattern as 93_170).
-- ============================================================

DO $DO$
DECLARE
ans boolean;
BEGIN
  select case when count(*)=1 then true else false end as ans
    from (select nspname from pg_catalog.pg_namespace where nspname='dbe_perf' limit 1) into ans;
  IF ans = true THEN
    DROP VIEW IF EXISTS DBE_PERF.summary_stat_user_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_stat_user_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.summary_stat_all_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.global_stat_all_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.stat_user_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.stat_sys_indexes CASCADE;
    DROP VIEW IF EXISTS DBE_PERF.stat_all_indexes CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_stat_user_indexes() CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_stat_user_indexes() CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_summary_stat_all_indexes() CASCADE;
    DROP FUNCTION IF EXISTS DBE_PERF.get_global_stat_all_indexes() CASCADE;

    CREATE VIEW DBE_PERF.stat_all_indexes AS
      SELECT
        C.oid AS relid,
        I.oid AS indexrelid,
        N.nspname AS schemaname,
        C.relname AS relname,
        I.relname AS indexrelname,
        pg_catalog.pg_stat_get_numscans(I.oid) AS idx_scan,
        pg_catalog.pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
        pg_catalog.pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
      FROM pg_catalog.pg_class C
           JOIN pg_catalog.pg_index X ON C.oid = X.indrelid
           JOIN pg_catalog.pg_class I ON I.oid = X.indexrelid
           LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't');

    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_all_indexes
      (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
       OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
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
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.global_stat_all_indexes AS
      SELECT * FROM DBE_PERF.get_global_stat_all_indexes();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_all_indexes
      (OUT schemaname name, OUT relname name, OUT indexrelname name,
       OUT toastrelschemaname name, OUT toastrelname name,
       OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
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
          T.idx_tup_fetch AS idx_tup_fetch
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
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_stat_all_indexes AS
      SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
             COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
             pg_catalog.SUM(Ti.idx_scan) idx_scan,
             pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read,
             pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch
        FROM DBE_PERF.get_summary_stat_all_indexes() AS Ti
             LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
             ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
        GROUP BY (1, 2, 3);

    CREATE VIEW DBE_PERF.stat_user_indexes AS
      SELECT * FROM DBE_PERF.stat_all_indexes
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
              schemaname !~ '^pg_toast';

    CREATE OR REPLACE FUNCTION DBE_PERF.get_global_stat_user_indexes
      (OUT node_name name, OUT relid oid, OUT indexrelid oid, OUT schemaname name, OUT relname name,
       OUT indexrelname name, OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
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
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.global_stat_user_indexes AS
      SELECT * FROM DBE_PERF.get_global_stat_user_indexes();

    CREATE OR REPLACE FUNCTION DBE_PERF.get_summary_stat_user_indexes
      (OUT schemaname name, OUT relname name, OUT indexrelname name,
       OUT toastrelschemaname name, OUT toastrelname name,
       OUT idx_scan bigint, OUT idx_tup_read bigint, OUT idx_tup_fetch bigint)
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
          T.idx_tup_fetch AS idx_tup_fetch
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
          return next;
        END LOOP;
      END LOOP;
      return;
    END; $$
    LANGUAGE 'plpgsql' NOT FENCED;

    CREATE VIEW DBE_PERF.summary_stat_user_indexes AS
      SELECT Ti.schemaname, COALESCE(Ti.relname, Tn.toastname) AS relname,
             COALESCE(Ti.indexrelname, Tn.toastindexname) AS indexrelname,
             pg_catalog.SUM(Ti.idx_scan) idx_scan,
             pg_catalog.SUM(Ti.idx_tup_read) idx_tup_read,
             pg_catalog.SUM(Ti.idx_tup_fetch) idx_tup_fetch
        FROM DBE_PERF.get_summary_stat_user_indexes() as Ti
             LEFT JOIN DBE_PERF.get_local_toastname_and_toastindexname() AS Tn
             ON (Tn.shemaname = Ti.toastrelschemaname AND Tn.relname = Ti.toastrelname)
        GROUP BY (1, 2, 3);

    CREATE VIEW DBE_PERF.stat_sys_indexes AS
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

DO $DO$
DECLARE
v_table_exist boolean;
BEGIN
    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid AND n.nspname = 'snapshot'
       AND c.relname = 'snap_global_stat_all_indexes';
    IF v_table_exist THEN
        EXECUTE 'ALTER TABLE snapshot.snap_global_stat_all_indexes DROP COLUMN IF EXISTS snap_indisunique';
        EXECUTE 'ALTER TABLE snapshot.snap_global_stat_all_indexes DROP COLUMN IF EXISTS snap_indisprimary';
    END IF;

    SELECT count(*) > 0 INTO v_table_exist
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relnamespace = n.oid AND n.nspname = 'snapshot'
       AND c.relname = 'snap_summary_stat_all_indexes';
    IF v_table_exist THEN
        EXECUTE 'ALTER TABLE snapshot.snap_summary_stat_all_indexes DROP COLUMN IF EXISTS snap_indisunique';
        EXECUTE 'ALTER TABLE snapshot.snap_summary_stat_all_indexes DROP COLUMN IF EXISTS snap_indisprimary';
    END IF;
END $DO$;
