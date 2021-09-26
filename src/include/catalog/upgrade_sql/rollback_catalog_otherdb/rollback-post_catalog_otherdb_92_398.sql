do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldebugger' limit 1)
    LOOP
        if ans = true then
            REVOKE USAGE ON SCHEMA dbe_pldebugger FROM PUBLIC;
        end if;
        exit;
    END LOOP;
END$$;

