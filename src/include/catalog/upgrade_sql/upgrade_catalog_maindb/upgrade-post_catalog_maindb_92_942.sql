DROP FUNCTION IF EXISTS pg_catalog.gs_get_preparse_location();
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2874;
CREATE FUNCTION pg_catalog.gs_get_preparse_location(
    OUT preparse_start_location text,
    OUT preparse_end_location text,
    OUT last_valid_record text
)
RETURNS SETOF record 
LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_get_preparse_location';