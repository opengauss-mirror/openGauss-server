SELECT 
	relname, 
	nspname, 
	relpages, 
	reltuples, 
	relallvisible 
FROM pg_class c 
JOIN pg_namespace n ON(c.relnamespace = n.oid AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema'))
