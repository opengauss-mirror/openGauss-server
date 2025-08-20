-- pg_stat_force_next_flush()
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_force_next_flush() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8395;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_force_next_flush()
  RETURNS void
  LANGUAGE internal
  NOT FENCED NOT SHIPPABLE
 AS $function$pg_stat_force_next_flush$function$;
COMMENT ON FUNCTION pg_catalog.pg_stat_force_next_flush() IS 'statistics: force stats to be flushed after the next commit';

 -- pg_stat_get_lastscan()
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_lastscan(oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8396;
CREATE OR REPLACE FUNCTION pg_catalog.pg_stat_get_lastscan(oid)
  RETURNS timestamp with time zone
  LANGUAGE internal
  STABLE STRICT NOT FENCED NOT SHIPPABLE
 AS $function$pg_stat_get_lastscan$function$;
COMMENT ON FUNCTION pg_catalog.pg_stat_get_lastscan(oid) IS 'statistics: time of the last scan for table/index';


-- pg_stat_all_tables
DROP VIEW IF EXISTS pg_catalog.pg_stat_sys_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_user_tables CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_all_tables CASCADE;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_all_tables AS
    SELECT
            C.oid AS relid,
            N.nspname AS schemaname,
            C.relname AS relname,
            pg_catalog.pg_stat_get_numscans(C.oid) AS seq_scan,
            pg_catalog.pg_stat_get_lastscan(C.oid) AS last_seq_scan,
            pg_catalog.pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
            pg_catalog.sum(pg_catalog.pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
            pg_catalog.max(pg_catalog.pg_stat_get_lastscan(I.indexrelid)) AS last_idx_scan,
            pg_catalog.sum(pg_catalog.pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
            pg_catalog.pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
            pg_catalog.pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
            pg_catalog.pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
            pg_catalog.pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
            pg_catalog.pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
            pg_catalog.pg_stat_get_live_tuples(C.oid) AS n_live_tup,
            pg_catalog.pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
            pg_catalog.pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
            pg_catalog.pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
            pg_catalog.pg_stat_get_last_analyze_time(C.oid) as last_analyze,
            pg_catalog.pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
            pg_catalog.pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
            pg_catalog.pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
            pg_catalog.pg_stat_get_analyze_count(C.oid) AS analyze_count,
            pg_catalog.pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count,
            pg_catalog.pg_stat_get_last_data_changed_time(C.oid) AS last_data_changed
    FROM pg_class C LEFT JOIN
         pg_index I ON C.oid = I.indrelid
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm')
    GROUP BY C.oid, N.nspname, C.relname;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_sys_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE VIEW pg_catalog.pg_stat_user_tables AS
    SELECT * FROM pg_stat_all_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

GRANT SELECT ON pg_catalog.pg_stat_all_tables TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_sys_tables TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_user_tables TO PUBLIC;

-- pg_stat_all_indexes
DROP VIEW IF EXISTS pg_catalog.pg_stat_sys_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_user_indexes CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_all_indexes CASCADE;

CREATE OR REPLACE VIEW pg_catalog.pg_stat_all_indexes AS
    SELECT
            C.oid AS relid,
            I.oid AS indexrelid,
            N.nspname AS schemaname,
            C.relname AS relname,
            I.relname AS indexrelname,
            pg_catalog.pg_stat_get_numscans(I.oid) AS idx_scan,
            pg_catalog.pg_stat_get_lastscan(I.oid) AS last_idx_scan,
            pg_catalog.pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
            pg_catalog.pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
    FROM pg_class C JOIN
            pg_index X ON C.oid = X.indrelid JOIN
            pg_class I ON I.oid = X.indexrelid
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.relkind IN ('r', 't', 'm');

CREATE OR REPLACE VIEW pg_catalog.pg_stat_sys_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname IN ('pg_catalog', 'information_schema') OR
          schemaname ~ '^pg_toast';

CREATE OR REPLACE VIEW pg_catalog.pg_stat_user_indexes AS
    SELECT * FROM pg_stat_all_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
          schemaname !~ '^pg_toast';

GRANT SELECT ON pg_catalog.pg_stat_all_indexes TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_sys_indexes TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_user_indexes TO PUBLIC;
