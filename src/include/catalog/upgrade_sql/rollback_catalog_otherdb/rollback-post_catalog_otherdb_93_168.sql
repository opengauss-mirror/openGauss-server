/*------ Rollback new array_agg(anyarray) ------*/
DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anyarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_array_finalfn(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_array_transfn(internal, anyarray) CASCADE;

/*------ Rollback array_agg -> anyelement ------*/
DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anynonarray) CASCADE;
DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anyelement) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_transfn(internal, anynonarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_transfn(internal, anyelement) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2333;
CREATE FUNCTION pg_catalog.array_agg_transfn(internal, anyelement)
RETURNS internal LANGUAGE INTERNAL IMMUTABLE as 'array_agg_transfn';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2335;
CREATE AGGREGATE pg_catalog.array_agg(anyelement) (
    SFUNC = array_agg_transfn,
    STYPE = internal,
    FINALFUNC = array_agg_finalfn
);