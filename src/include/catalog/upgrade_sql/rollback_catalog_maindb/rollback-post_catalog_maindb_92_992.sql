do $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * FROM pg_catalog.pg_type where typname = 'vector' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.l2_norm(vector) CASCADE;
    end if;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.l2_norm(unknown) CASCADE;
