DROP FUNCTION IF EXISTS dbe_pldebugger.backtrace;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1510;
CREATE OR REPLACE FUNCTION dbe_pldebugger.backtrace(OUT frameno integer, OUT funcname text, OUT lineno integer, OUT query text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE ROWS 100
AS $function$debug_client_backtrace$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;