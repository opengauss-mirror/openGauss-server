DROP FUNCTION IF EXISTS pg_catalog.row_count() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9994;
CREATE FUNCTION pg_catalog.row_count (
) RETURNS int8 LANGUAGE INTERNAL VOLATILE STRICT as 'b_database_row_count';