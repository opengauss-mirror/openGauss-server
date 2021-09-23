do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldebugger' limit 1)
    LOOP
        if ans = true then
            DROP FUNCTION IF EXISTS dbe_pldebugger.attach;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1504;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.attach(text, integer)
            RETURNS boolean
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE
            AS 'debug_client_attatch';

            DROP FUNCTION IF EXISTS dbe_pldebugger.continue;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1506;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.continue()
            RETURNS boolean
            LANGUAGE internal
            STABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
            AS 'debug_client_continue';

            DROP FUNCTION IF EXISTS dbe_pldebugger.info_code;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1511;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.info_code(OUT lineno integer, OUT query text)
            RETURNS SETOF record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
            AS 'debug_client_info_code';

            DROP FUNCTION IF EXISTS dbe_pldebugger.next;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1502;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.next(OUT lineno integer, OUT query text)
            RETURNS SETOF record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
            AS 'debug_client_next';

            DROP FUNCTION IF EXISTS dbe_pldebugger.step;
            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1512;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.step(OUT func_oid oid, OUT funcname text, OUT lineno integer, OUT query text)
            RETURNS SETOF record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
            AS 'debug_client_info_step';

            DROP FUNCTION IF EXISTS dbe_pldebugger.turn_on;
            DROP FUNCTION IF EXISTS dbe_pldebugger.turn_off;

            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1500;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_off(oid)
            RETURNS boolean
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE
            AS $function$debug_server_turn_off$function$;

            SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1501;
            CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_on(func_oid oid, OUT nodename text, OUT port integer)
            RETURNS SETOF record
            LANGUAGE internal
            STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
            AS 'debug_server_turn_on';
        end if;
        exit;
    END LOOP;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;


