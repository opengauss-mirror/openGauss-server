DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text, text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25_shard_stat(regclass, text) CASCADE;

-- Restore text_date(text) volatility for rollback.
ALTER FUNCTION pg_catalog.text_date(text) IMMUTABLE;
