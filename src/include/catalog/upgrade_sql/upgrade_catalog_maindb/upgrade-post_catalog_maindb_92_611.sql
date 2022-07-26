DROP FUNCTION IF EXISTS pg_catalog.to_timestamp(float8);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1158;
CREATE FUNCTION pg_catalog.to_timestamp(float8) RETURNS timestamptz LANGUAGE INTERNAL STABLE AS 'float8_timestamptz';