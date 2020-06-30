select
	n.nspname as nspname,
	extname as extensionname,
	pg_get_userbyid(extowner)
FROM pg_extension e
left join pg_namespace n on (n.oid = e.extnamespace)
where e.oid > 16384