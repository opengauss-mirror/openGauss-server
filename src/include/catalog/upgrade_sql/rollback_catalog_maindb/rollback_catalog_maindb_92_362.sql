DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_extension where extname = 'security_plugin' limit 1) into ans;
    if ans = true then
        drop extension if exists security_plugin cascade;
        create extension security_plugin;
    end if;
END$$;