do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldebugger' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS dbe_pldebugger.backtrace;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1510;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.backtrace(OUT frameno integer, OUT func_name text)
            RETURNS SETOF record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
            AS 'debug_client_backtrace';
            end if;
        exit;
    END LOOP;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;