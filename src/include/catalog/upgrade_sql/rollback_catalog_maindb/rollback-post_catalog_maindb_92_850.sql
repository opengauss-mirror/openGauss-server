do $$
DECLARE
ans boolean;
lite boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select extname from pg_extension where extname='dolphin')
    LOOP
        if ans = true then
            select version() like '%openGauss-lite%' into lite;
            if lite = false then
                ALTER EXTENSION dolphin UPDATE TO '1.0';
            else
                DROP EXTENSION if exists dolphin;
            end if;
        end if;
        exit;
    END LOOP;
END$$;