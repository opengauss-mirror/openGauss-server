with oid2relname AS
(
	SELECT 
		n.nspname AS schemaname, 
		c.relname AS tablename,
		pg_get_userbyid(c.relowner) as relowner,
		c.oid AS relid
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
)
select 
	a.schemaname as inh_schemaname,
	a.tablename  as inh_tablename,
	a.relowner   as inh_owner,
	b.schemaname as parent_schemaname,
	b.tablename  as parent_tablename,
	b.relowner   as parent_owner
from pg_inherits h
inner join oid2relname a on a.relid = h.inhrelid
inner join oid2relname b on b.relid = h.inhparent
;