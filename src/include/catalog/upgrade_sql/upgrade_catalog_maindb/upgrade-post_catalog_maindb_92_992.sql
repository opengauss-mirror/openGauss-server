DROP FUNCTION IF EXISTS pg_catalog.l2_norm(vector) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9085;
CREATE FUNCTION pg_catalog.l2_norm (
    vector
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'vector_norm';
COMMENT ON FUNCTION pg_catalog.l2_norm(vector) IS 'NULL';

DROP FUNCTION IF EXISTS pg_catalog.l2_norm(unknown) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9086;
CREATE FUNCTION pg_catalog.l2_norm (
    unknown
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'l2_norm_unknown_compat';
COMMENT ON FUNCTION pg_catalog.l2_norm(unknown) IS 'NULL';
