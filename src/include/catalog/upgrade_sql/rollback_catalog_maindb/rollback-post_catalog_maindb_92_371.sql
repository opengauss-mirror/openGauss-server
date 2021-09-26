
do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldebugger' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS dbe_pldebugger.abort() cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.attach(text, integer) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.next(OUT lineno integer, OUT query text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.info_locals(OUT varname text, OUT vartype text, OUT value text, OUT package_name text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.continue() cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.turn_off(oid) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.turn_on(func_oid oid, OUT nodename text, OUT port integer) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.delete_breakpoint(integer) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.add_breakpoint(lineno integer, OUT breakpointno integer) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT lineno integer, OUT query text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.info_code(OUT lineno integer, OUT query text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.backtrace(OUT frameno integer, OUT func_name text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.step(OUT func_oid oid, OUT funcname text, OUT lineno integer, OUT query text) cascade;
            DROP FUNCTION IF EXISTS dbe_pldebugger.local_debug_server_info(OUT nodename text, OUT port int8, OUT funcoid oid);
        end if;
        exit;
    END LOOP;
END$$;

DROP SCHEMA IF EXISTS dbe_pldebugger cascade;
