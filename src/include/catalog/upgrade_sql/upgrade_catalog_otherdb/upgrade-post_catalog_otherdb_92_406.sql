DROP FUNCTION IF EXISTS dbe_pldebugger.add_breakpoint;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1507;
CREATE OR REPLACE FUNCTION dbe_pldebugger.add_breakpoint(funcoid oid, lineno integer, OUT breakpointno integer)
 RETURNS integer
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$debug_client_add_breakpoint$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.info_breakpoints;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1509;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_breakpoints(OUT breakpointno integer, OUT funcoid oid, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_info_breakpoints$function$;

DROP FUNCTION IF EXISTS dbe_pldebugger.info_code;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1511;
CREATE OR REPLACE FUNCTION dbe_pldebugger.info_code(funcoid oid, OUT lineno integer, OUT query text, OUT canbreak boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_info_code$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;