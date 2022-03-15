DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info()
 RETURNS text
 LANGUAGE internal
 STABLE STRICT NOT FENCED SHIPPABLE
AS $function$get_client_info$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
