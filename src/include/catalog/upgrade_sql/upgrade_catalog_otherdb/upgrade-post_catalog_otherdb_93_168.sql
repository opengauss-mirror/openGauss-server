/*------ Step 1: Modify existing array_agg(anyelement) -> array_agg(anynonarray) ------*/
DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anynonarray) CASCADE;
DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anyelement) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_transfn(internal, anynonarray) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.array_agg_transfn(internal, anyelement) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2333;
CREATE FUNCTION pg_catalog.array_agg_transfn(internal, anynonarray)
RETURNS internal LANGUAGE INTERNAL IMMUTABLE as 'array_agg_transfn';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2335;
CREATE AGGREGATE pg_catalog.array_agg(anynonarray) (
    SFUNC = array_agg_transfn,
    STYPE = internal,
    FINALFUNC = array_agg_finalfn
);
COMMENT ON FUNCTION pg_catalog.array_agg_transfn(internal, anynonarray)                                                                                                                                                                            
         IS 'aggregate transition function';                                                                                                                                                                                                            
COMMENT ON AGGREGATE pg_catalog.array_agg(anynonarray)                                                                                                                                                                                             
    IS 'concatenate aggregate input into an array';   

/*------ Step 2: Add new array_agg(anyarray) support ------*/
DROP FUNCTION IF EXISTS pg_catalog.array_agg_array_transfn(internal, anyarray) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4060;
CREATE FUNCTION pg_catalog.array_agg_array_transfn(internal, anyarray)
RETURNS internal LANGUAGE INTERNAL IMMUTABLE as 'array_agg_array_transfn';

DROP FUNCTION IF EXISTS pg_catalog.array_agg_array_finalfn(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4061;
CREATE FUNCTION pg_catalog.array_agg_array_finalfn(internal)
RETURNS anyarray LANGUAGE INTERNAL IMMUTABLE as 'array_agg_array_finalfn';

DROP AGGREGATE IF EXISTS pg_catalog.array_agg(anyarray) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4062;
CREATE AGGREGATE pg_catalog.array_agg(anyarray) (
    SFUNC = array_agg_array_transfn,
    STYPE = internal,
    FINALFUNC = array_agg_array_finalfn
);

COMMENT ON FUNCTION pg_catalog.array_agg_array_transfn(internal, anyarray)                                                                                                                                                                         
    IS 'aggregate transition function';                                                                                                                                                                                                            
COMMENT ON FUNCTION pg_catalog.array_agg_array_finalfn(internal)                                                                                                                                                                                   
    IS 'aggregate final function';                                                                                                                                                                                                                 
COMMENT ON AGGREGATE pg_catalog.array_agg(anyarray)                                                                                                                                                                                                
    IS 'input arrays concatenated into array of one higher dimension';  