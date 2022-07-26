DROP FUNCTION IF EXISTS pg_catalog.to_timestamp(float8);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1158;
CREATE FUNCTION pg_catalog.to_timestamp(float8) RETURNS timestamptz LANGUAGE SQL AS 'select (''epoch''::pg_catalog.timestamptz + $1 * ''1 second''::pg_catalog.interval)';