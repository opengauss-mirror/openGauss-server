/* pg_stat_progress_copy */
DROP VIEW IF EXISTS pg_catalog.pg_stat_progress_copy CASCADE;
/* pg_stat_get_progress_info */
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
