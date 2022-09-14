DROP FUNCTION IF EXISTS pg_catalog.textanycat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2003;
CREATE OR REPLACE FUNCTION pg_catalog.textanycat(text, anynonarray)
RETURNS text LANGUAGE sql  STABLE SHIPPABLE COST 1 STRICT as $function$select $1 || $2::pg_catalog.text$function$;
COMMENT ON FUNCTION  textanycat(text, anynonarray) IS 'implementation of || operator';
