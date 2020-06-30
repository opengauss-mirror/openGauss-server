with oid2relname AS
(
	SELECT 
		n.nspname AS schemaname, 
		c.relname AS tablename,
		c.oid AS relid
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
)

select
	n.schemaname,
	n.tablename,
	case when contype = 'f' then 'FOREIGN KEY CONSTRAINT'
		 when contype = 'x' then 'EXCLUSION CONSTRAINT'
		 when contype = 't' then 'TRIGGER CONSTRAINT'
	end as contype
from pg_constraint c
inner join oid2relname n on (n.relid = c.confrelid);