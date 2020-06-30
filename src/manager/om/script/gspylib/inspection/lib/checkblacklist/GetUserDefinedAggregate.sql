SELECT 
	n.nspname AS schemaname, 
	p.proname AS proname
FROM pg_catalog.pg_proc p 
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
where p.oid > 16384 and proisagg;