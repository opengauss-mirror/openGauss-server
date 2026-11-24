DROP FUNCTION IF EXISTS pg_catalog.array_position(anyarray, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8553;
CREATE FUNCTION pg_catalog.array_position(anyarray, anyelement)
RETURNS int4
AS 'array_position'
LANGUAGE INTERNAL
IMMUTABLE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8559;
CREATE FUNCTION pg_catalog.array_position(anyarray, anyelement, int4)
RETURNS int4
AS 'array_position'
LANGUAGE INTERNAL
IMMUTABLE;

COMMENT ON FUNCTION pg_catalog.array_position(anyarray, anyelement) IS 'return the offset of a value in an array';
COMMENT ON FUNCTION pg_catalog.array_position(anyarray, anyelement, int4) IS 'returns an offset of value in array with start index';
