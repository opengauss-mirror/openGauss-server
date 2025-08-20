DROP FUNCTION IF EXISTS pg_catalog.scale(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8551;
CREATE FUNCTION pg_catalog.scale(numeric)
RETURNS int4
AS 'numeric_scale'
LANGUAGE INTERNAL
IMMUTABLE;

DROP FUNCTION IF EXISTS pg_catalog.make_timestamp(int4, int4, int4, int4, int4, float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8552;
CREATE FUNCTION pg_catalog.make_timestamp(int4, int4, int4, int4, int4, float8)
RETURNS timestamp
AS 'make_timestamp'
LANGUAGE INTERNAL
IMMUTABLE;

DROP FUNCTION IF EXISTS pg_catalog.array_position(anyarray, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8553;
CREATE FUNCTION pg_catalog.array_position(anyarray, int4)
RETURNS int4
AS 'array_position'
LANGUAGE INTERNAL
IMMUTABLE;

COMMENT ON FUNCTION pg_catalog.scale(numeric) IS 'return the count of decimal digits in the fractional part';
COMMENT ON FUNCTION pg_catalog.make_timestamp(int4, int4, int4, int4, int4, float8) IS 'construct a timestamp';
COMMENT ON FUNCTION pg_catalog.array_position(anyarray, int4) IS 'return the offset of a value in an array';