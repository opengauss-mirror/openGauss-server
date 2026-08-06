DROP FUNCTION IF EXISTS pg_catalog.bm25_shard_stat(regclass, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8655;
CREATE FUNCTION pg_catalog.bm25_shard_stat(regclass, text)
RETURNS text
AS 'bm25_shard_stat'
LANGUAGE INTERNAL
VOLATILE NOT FENCED NOT SHIPPABLE;
COMMENT ON FUNCTION pg_catalog.bm25_shard_stat(regclass, text) IS 'NULL';

DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8658;
CREATE FUNCTION pg_catalog.bm25_table_stat(text, text)
RETURNS text
AS 'bm25_table_stat_2'
LANGUAGE INTERNAL
VOLATILE NOT FENCED NOT SHIPPABLE;
COMMENT ON FUNCTION pg_catalog.bm25_table_stat(text, text) IS 'NULL';

DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8659;
CREATE FUNCTION pg_catalog.bm25_table_stat(text, text, text)
RETURNS text
AS 'bm25_table_stat_3'
LANGUAGE INTERNAL
VOLATILE NOT FENCED NOT SHIPPABLE;
COMMENT ON FUNCTION pg_catalog.bm25_table_stat(text, text, text) IS 'NULL';

DROP FUNCTION IF EXISTS pg_catalog.bm25_table_stat(text, text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8660;
CREATE FUNCTION pg_catalog.bm25_table_stat(text, text, text, text)
RETURNS text
AS 'bm25_table_stat'
LANGUAGE INTERNAL
VOLATILE NOT FENCED NOT SHIPPABLE;
COMMENT ON FUNCTION pg_catalog.bm25_table_stat(text, text, text, text) IS 'NULL';

-- Mark text_date(text) as stable because text_date('now') depends on statement start timestamp.
ALTER FUNCTION pg_catalog.text_date(text) STABLE;
