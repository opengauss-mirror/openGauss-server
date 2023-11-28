DROP FUNCTION IF EXISTS pg_catalog.pg_prepared_statement(in_sessionid bigint,OUT sessionid bigint, OUT username text,OUT name text, OUT statement text, OUT prepare_time timestamp with time zone, OUT parameter_types regtype[], OUT from_sql boolean) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3702;

CREATE OR REPLACE FUNCTION pg_catalog.pg_prepared_statement(in_sessionid bigint,OUT sessionid bigint, OUT username text,OUT name text, OUT statement text, OUT prepare_time timestamp with time zone, OUT parameter_types regtype[], OUT from_sql boolean)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$pg_prepared_statement_global$function$;
