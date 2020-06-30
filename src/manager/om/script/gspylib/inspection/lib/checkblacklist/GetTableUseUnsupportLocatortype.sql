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
	case when pclocatortype = 'G' then 'RANGE'
		 when pclocatortype = 'N' then 'RROBIN'
		 when pclocatortype = 'N' then 'RROBIN'
		 when pclocatortype = 'C' then 'CUSTOM'
		 when pclocatortype = 'M' then 'MODULO'
		 when pclocatortype = 'O' then 'NONE'
		 when pclocatortype = 'D' then 'DISTRIBUTED'
	end as locatortype
from pgxc_class 
inner join oid2relname on (pcrelid = relid)
where pclocatortype not in ('R', 'H') and pcrelid not in (select oid from pg_class where relkind='f') and pcrelid not in  (select oid from pg_class where reloptions::text  like '%internal_mask=33029%')
;