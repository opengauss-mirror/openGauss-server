with oid2relname AS
(
	SELECT 
		n.nspname AS schemaname, 
		c.relname AS tablename,
		c.oid AS relid
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
)

select
	schemaname,
	tablename,
	pgroup as nodegroup
from pgxc_class
inner join oid2relname on (relid = pcrelid)
where pgroup is null;