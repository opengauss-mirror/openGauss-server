DROP FUNCTION IF EXISTS dbe_pldebugger.attach;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1504;
CREATE OR REPLACE FUNCTION dbe_pldebugger.attach(nodename text, port integer, OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_attatch$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.continue;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1506;
CREATE OR REPLACE FUNCTION dbe_pldebugger.continue(OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS record
 LANGUAGE internal
 STABLE STRICT LEAKPROOF NOT FENCED NOT SHIPPABLE
AS $function$debug_client_continue$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.info_code;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1511;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_code(funcoid oid, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_info_code$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.next;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1502;
CREATE OR REPLACE FUNCTION dbe_pldebugger.next(OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_next$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.step;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1512;
CREATE OR REPLACE FUNCTION dbe_pldebugger.step(OUT funcoid oid, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_info_step$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.print_var;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1515;
CREATE OR REPLACE FUNCTION dbe_pldebugger.print_var(var_name text, OUT varname text, OUT vartype text, OUT value text, OUT package_name text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_client_print_variables$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.turn_on;
DROP FUNCTION IF EXISTS dbe_pldebugger.turn_off;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1501;
CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_on(funcoid oid, OUT nodename text, OUT port integer)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 1
AS $function$debug_server_turn_on$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1500;
CREATE OR REPLACE FUNCTION dbe_pldebugger.turn_off(oid)
 RETURNS boolean
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'debug_server_turn_off';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;