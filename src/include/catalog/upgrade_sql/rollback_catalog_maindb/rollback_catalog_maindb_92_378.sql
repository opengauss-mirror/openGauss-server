DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'packages' limit 1) into ans;
    if ans = true then
        DROP EXTENSION IF EXISTS packages CASCADE;
        CREATE EXTENSION IF NOT EXISTS packages;
        ALTER EXTENSION packages UPDATE TO '1.1';
    end if;
END$$;
