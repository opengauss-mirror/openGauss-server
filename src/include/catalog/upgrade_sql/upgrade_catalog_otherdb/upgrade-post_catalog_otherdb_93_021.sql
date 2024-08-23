DROP FUNCTION IF EXISTS pg_catalog.nls_initcap(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 9537;
CREATE OR REPLACE FUNCTION pg_catalog.nls_initcap(str text, nlsparam text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$nls_initcap$function$;

