DROP AGGREGATE IF EXISTS pg_catalog.max(inet) CASCADE;
DROP AGGREGATE IF EXISTS pg_catalog.min(inet) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.network_larger(inet, inet) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.network_smaller(inet, inet) CASCADE;