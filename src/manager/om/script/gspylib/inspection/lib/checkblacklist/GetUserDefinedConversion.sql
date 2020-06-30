select
	n.nspname,
	c.conname,
	pg_get_userbyid(conowner) as conowner
from pg_conversion c
left join pg_namespace n on (c.connamespace = n.oid)
where c.oid > 16384;