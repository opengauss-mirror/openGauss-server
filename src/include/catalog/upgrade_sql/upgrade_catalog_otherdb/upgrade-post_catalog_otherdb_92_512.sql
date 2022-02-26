DROP FUNCTION IF EXISTS pg_catalog.get_client_info;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7732;
CREATE OR REPLACE FUNCTION pg_catalog.get_client_info(OUT sid bigint, OUT client_info text)
 RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED SHIPPABLE ROWS 100
AS $function$get_client_info$function$;
comment on function PG_CATALOG.get_client_info() is 'read current client';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
