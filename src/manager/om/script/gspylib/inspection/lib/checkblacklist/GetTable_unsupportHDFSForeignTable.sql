with f_table as
(
	select 
		ftrelid 
	from pg_foreign_table t
	inner join pg_foreign_server s on( t.ftserver = s.oid and s. srvoptions is not null)
)

select 
	b.nspname, 
	a.relname,
	pg_get_userbyid(a.relowner) as relowner
from pg_class a
inner join pg_namespace b on a. relnamespace= b.oid 
inner join f_table c on a.oid = c.ftrelid
;