with aclinfo as
(
    select
        oid as relid,
        relname,
        relnamespace,
        pg_get_userbyid(relowner) as relowner,
        relacl,
        trim(trim(relacl::text, '}'), '{') as acl
    from pg_class where relacl is not null
),

acl2tab as
(
    select
        c.relid,
        nspname,
        relname,
        relowner,
        relacl,
        regexp_split_to_table(acl, ',') as record
    from aclinfo c
    inner join pg_namespace n on c.relnamespace = n.oid
    where relacl is not null
),

split_acl as
(
    select
        relid,
        nspname,
        relname,
        relowner,
        relacl,
        split_part(record, '/', 2) as grantUser,
        split_part(record, '=', 1) as grantedToUser,
        split_part(split_part(record, '=', 2), '/', 1) as privilege
    from acl2tab
    where relowner != grantUser
)

select 
	*
from split_acl
order by nspname, relname
;