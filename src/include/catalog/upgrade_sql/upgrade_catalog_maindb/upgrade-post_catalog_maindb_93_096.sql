
DROP VIEW IF EXISTS pg_catalog.pg_stat_progress_copy CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_progress_info(
    IN  cmdtype text,
    OUT pid int8,
    OUT datid oid,
    OUT relid oid,
    OUT param1 int8,
    OUT param2 int8,
    OUT param3 int8,
    OUT param4 int8,
    OUT param5 int8,
    OUT param6 int8,
    OUT param7 int8,
    OUT param8 int8,
    OUT param9 int8,
    OUT param10 int8,
    OUT param11 int8,
    OUT param12 int8,
    OUT param13 int8,
    OUT param14 int8,
    OUT param15 int8,
    OUT param16 int8,
    OUT param17 int8,
    OUT param18 int8,
    OUT param19 int8,
    OUT param20 int8
) CASCADE;

/* pg_stat_get_progress_info */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3360;
CREATE FUNCTION pg_catalog.pg_stat_get_progress_info (
    IN  cmdtype text,
    OUT pid int8,
    OUT datid oid,
    OUT relid oid,
    OUT param1 int8,
    OUT param2 int8,
    OUT param3 int8,
    OUT param4 int8,
    OUT param5 int8,
    OUT param6 int8,
    OUT param7 int8,
    OUT param8 int8,
    OUT param9 int8,
    OUT param10 int8,
    OUT param11 int8,
    OUT param12 int8,
    OUT param13 int8,
    OUT param14 int8,
    OUT param15 int8,
    OUT param16 int8,
    OUT param17 int8,
    OUT param18 int8,
    OUT param19 int8,
    OUT param20 int8
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED STRICT COST 1 ROWS 100 as 'pg_stat_get_progress_info';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_progress_info(
    IN  cmdtype text,
    OUT pid int8,
    OUT datid oid,
    OUT relid oid,
    OUT param1 int8,
    OUT param2 int8,
    OUT param3 int8,
    OUT param4 int8,
    OUT param5 int8,
    OUT param6 int8,
    OUT param7 int8,
    OUT param8 int8,
    OUT param9 int8,
    OUT param10 int8,
    OUT param11 int8,
    OUT param12 int8,
    OUT param13 int8,
    OUT param14 int8,
    OUT param15 int8,
    OUT param16 int8,
    OUT param17 int8,
    OUT param18 int8,
    OUT param19 int8,
    OUT param20 int8
) IS 'command progress statistics info';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

/* pg_stat_progress_copy */
CREATE VIEW pg_catalog.pg_stat_progress_copy AS
    SELECT
        S.pid AS pid, S.datid AS datid, D.datname AS datname,
        S.relid AS relid,
        CASE S.param5 WHEN 1 THEN 'COPY FROM'
                      WHEN 2 THEN 'COPY TO'
                      END AS command,
        CASE S.param6 WHEN 1 THEN 'FILE'
                      WHEN 2 THEN 'PIPE'
                      WHEN 3 THEN 'CALLBACK'
                      END AS "type",
        S.param1 AS bytes_processed,
        S.param2 AS bytes_total,
        S.param3 AS tuples_processed,
        S.param4 AS tuples_excluded
    FROM pg_stat_get_progress_info('COPY') AS S
        LEFT JOIN pg_database D ON S.datid = D.oid;

REVOKE ALL ON pg_catalog.pg_stat_progress_copy FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_progress_copy TO PUBLIC;
