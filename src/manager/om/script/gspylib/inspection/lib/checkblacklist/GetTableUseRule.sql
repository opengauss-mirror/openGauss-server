with oid2relname AS
(
	SELECT 
		n.nspname AS schemaname, 
		c.relname AS tablename,
		c.oid AS relid
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
)
select 
	rulename,
	schemaname,
	tablename
FROM
	pg_rewrite r inner join oid2relname on (ev_class = relid)
WHERE r.oid > 16384 and rulename != '_RETURN';