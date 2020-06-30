SELECT 
	n.nspname AS schemaname, 
	c.relname AS tablename
FROM pg_class c
INNER JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'::"char" and c.oid > 16384 and relhasoids = true;
