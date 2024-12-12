DROP FUNCTION IF EXISTS pg_catalog.to_number(text, numeric, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5258;
CREATE FUNCTION pg_catalog.to_number(text, numeric, bool, bool, text, text)
RETURNS NUMERIC
COST 1
as 'numeric_to_text_number'
LANGUAGE INTERNAL
NOT SHIPPABLE
stable;
COMMENT ON FUNCTION pg_catalog.to_number(text, numeric, bool, bool, text, text) IS 'convert text to numeric with default return value';


DROP FUNCTION IF EXISTS pg_catalog.to_number(text, text, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5260;
CREATE FUNCTION pg_catalog.to_number(text, text, bool, bool, text, text)
RETURNS NUMERIC
COST 1
as 'numeric_to_default_without_defaultval'
LANGUAGE INTERNAL
NOT SHIPPABLE
stable;
COMMENT ON FUNCTION pg_catalog.to_number(text, text, bool, bool, text, text) IS 'convert text to numeric without default';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(text, text, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7018;
CREATE FUNCTION pg_catalog.to_binary_float(text, text, bool, bool, text, text)
RETURNS float4
COST 1
AS 'select pg_catalog.to_binary_float($1, $2, $3, $4)'
LANGUAGE sql
NOT SHIPPABLE
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(text, text, bool, bool, text, text) IS 'convert text to a single precision floating-point number, with default return expr on convert error';


DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(float8, float8, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7019;
CREATE FUNCTION pg_catalog.to_binary_float(float8, float8, bool, bool, text, text)
RETURNS float4
COST 1
AS 'select pg_catalog.to_binary_float($1, $2, $3, $4)'
LANGUAGE sql
NOT SHIPPABLE
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(float8, float8, bool, bool, text, text) IS 'convert float8 to a single precision floating-point number, with default return expr on convert error';

DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(float8, text, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7020;
CREATE FUNCTION pg_catalog.to_binary_float(float8, text, bool, bool, text, text)
RETURNS float4
COST 1
AS 'select pg_catalog.to_binary_float($1, $2, $3, $4)'
LANGUAGE sql
NOT SHIPPABLE
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(float8, text, bool, bool, text, text) IS 'convert float8 to a single precision floating-point number, with default return expr on convert error';

DROP FUNCTION IF EXISTS pg_catalog.to_binary_float(text, float8, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7021;
CREATE FUNCTION pg_catalog.to_binary_float(text, float8, bool, bool, text, text)
RETURNS float4
COST 1
AS 'select pg_catalog.to_binary_float($1, $2, $3, $4)'
LANGUAGE sql
NOT SHIPPABLE
IMMUTABLE;
COMMENT ON FUNCTION pg_catalog.to_binary_float(text, float8, bool, bool, text, text) IS 'convert text to a single precision floating-point number, with default return expr on convert error';
