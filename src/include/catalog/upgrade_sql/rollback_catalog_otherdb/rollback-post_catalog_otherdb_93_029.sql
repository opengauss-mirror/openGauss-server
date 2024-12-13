DROP CAST IF EXISTS (float8 AS money) cascade;
DROP CAST IF EXISTS (float4 AS money) cascade;

DROP FUNCTION IF EXISTS pg_catalog.money(float8);
DROP FUNCTION IF EXISTS pg_catalog.money(float4);

