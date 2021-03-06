SELECT 
	relname,
	nspname, 
	pgroup,
	relallvisible
FROM pg_class c , pg_namespace n, pgxc_class x  
WHERE (x.pcrelid=c.oid AND c.relnamespace = n.oid AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema'))