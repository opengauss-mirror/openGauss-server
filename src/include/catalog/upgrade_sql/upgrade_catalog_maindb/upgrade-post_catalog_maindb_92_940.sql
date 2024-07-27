DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(text, text, bool, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7012;
CREATE FUNCTION pg_catalog.to_binary_float(text, text, bool, bool)
RETURNS float4
as 'to_binary_float_text'
LANGUAGE INTERNAL
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(text, text, bool, bool) IS 'convert text to a single precision floating-point number, with default return expr on convert error';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7013;
CREATE FUNCTION pg_catalog.to_binary_float(text)
RETURNS float4 AS 
$$
BEGIN
    RETURN (select pg_catalog.to_binary_float($1, ' ', false, false));
END;
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION pg_catalog.to_binary_float(text) IS 'convert text to a single precision floating-point number';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(float8, float8, bool, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7014;
CREATE FUNCTION pg_catalog.to_binary_float(float8, float8, bool, bool)
RETURNS float4
as 'to_binary_float_number'
LANGUAGE INTERNAL
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(float8, float8, bool, bool) IS 'convert float8 to a single precision floating-point number, with default return expr on convert error';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7015;
CREATE FUNCTION pg_catalog.to_binary_float(float8)
RETURNS float4 AS 
$$
BEGIN
    RETURN (select pg_catalog.to_binary_float($1, 0, false, false));
END;
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION pg_catalog.to_binary_float(float8) IS 'convert float8 to a single precision floating-point number';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(float8, text, bool, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7016;
CREATE FUNCTION pg_catalog.to_binary_float(float8, text, bool, bool)
RETURNS float4 AS 
$$
BEGIN
    RETURN (select pg_catalog.to_binary_float($1, 0, false, false));
END;
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION pg_catalog.to_binary_float(float8, float8, bool, bool) IS 'convert float8 to a single precision floating-point number, with default return expr on convert error';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(text, float8, bool, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7017;
CREATE FUNCTION pg_catalog.to_binary_float(text, float8, bool, bool)
RETURNS float4
as 'to_binary_float_text_number'
LANGUAGE INTERNAL
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(text, text, bool, bool) IS 'convert text to a single precision floating-point number, with default return expr on convert error';
