--create internal function
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC,7816;
CREATE OR REPLACE FUNCTION pg_catalog.value(record)
RETURNS record
LANGUAGE internal
IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$object_table_value$function$;