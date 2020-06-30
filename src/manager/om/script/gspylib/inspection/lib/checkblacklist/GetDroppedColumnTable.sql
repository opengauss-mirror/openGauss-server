with relinfo as
(
    select 
		n.nspname as nspname,
		c.relname as tablename,
        pg_get_userbyid(c.relowner) as relowner,
		(c.xmin::text::bigint) as rel_xmin,
        c.oid as relid
    from pg_class c
    left join pg_namespace n on (c.relnamespace = n.oid)
),

groupinfo as
(
	select
		c.pcrelid as relid,
		c.pgroup as group_name,
		(g.xmin::text::bigint) as group_xmin
	from pgxc_class c
	left join pgxc_group g on (c.pgroup = g.group_name)
)

select
    t.relid,
    nspname,
    tablename,
    relowner,
    attname,
    attnum,
    attisdropped,
	t.rel_xmin,
	g.group_xmin,
	case when rel_xmin > group_xmin then 'unable to do dilatation' else 'already broken by dilatation' end as notice,
	group_name
from pg_attribute a
left join relinfo t on a.attrelid = t.relid
left join groupinfo g on a.attrelid = g.relid
where a.attisdropped = true
order by notice, nspname, tablename
;
