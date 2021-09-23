DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'hash16' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.hash16_eq(hash16, hash16);
        DROP FUNCTION IF EXISTS pg_catalog.hash16_add(hash16, hash16);
    end if;
END$$;