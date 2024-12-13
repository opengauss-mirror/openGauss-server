-- float4 and float8 to money
DROP CAST IF EXISTS (float4 AS money);
DROP CAST IF EXISTS (float8 AS money);

DROP FUNCTION IF EXISTS pg_catalog.money(float4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3910;
CREATE FUNCTION pg_catalog.money(float4) RETURNS money LANGUAGE INTERNAL IMMUTABLE STRICT AS 'float4_cash';
COMMENT ON FUNCTION pg_catalog.money(float4) is 'convert float4 to money';

DROP FUNCTION IF EXISTS pg_catalog.money(float8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3911;
CREATE FUNCTION pg_catalog.money(float8) RETURNS money LANGUAGE INTERNAL IMMUTABLE STRICT AS 'float8_cash';
COMMENT ON FUNCTION pg_catalog.money(float8) is 'convert float8 to money';

CREATE CAST (float4 AS money) WITH FUNCTION pg_catalog.money(float4) AS ASSIGNMENT;
CREATE CAST (float8 AS money) WITH FUNCTION pg_catalog.money(float8) AS ASSIGNMENT;
