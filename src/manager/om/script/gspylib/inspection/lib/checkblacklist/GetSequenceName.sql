SELECT 
	n.nspname AS schemaname, 
	c.relname AS sequencename,
	pg_get_userbyid(c.relowner) as sequenceowner
FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
where c.relkind = 'S'::"char";