SELECT
   'EXPLAIN SELECT * FROM(' || rtrim(pg_catalog.pg_get_viewdef(c.oid), ';') || ') AS "' || n.nspname || '.' || c.relname || '";'
FROM pg_class c
LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace AND n.nspname NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore'))
WHERE c.relkind = 'v'::"char" and c.oid > 16384;