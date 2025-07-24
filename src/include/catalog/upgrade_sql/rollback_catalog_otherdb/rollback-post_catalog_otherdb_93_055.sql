do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = false then
            DROP FUNCTION IF EXISTS pg_catalog.to_char(time, text) CASCADE;
        end if;
        exit;
    END LOOP;
END$$;

DROP FUNCTION IF EXISTS pg_catalog.to_char(timetz, text) CASCADE;
