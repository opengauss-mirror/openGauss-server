SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8890;
CREATE FUNCTION pg_catalog.array_deleteidx_db_a(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8891;
CREATE FUNCTION pg_catalog.array_deleteidx_db_a(anyarray, integer, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_multi_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8892;
CREATE FUNCTION pg_catalog.array_integer_deleteidx_db_a(anyarray, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8893;
CREATE FUNCTION pg_catalog.array_integer_deleteidx_db_a(anyarray, integer, integer)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_integer_multi_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8894;
CREATE FUNCTION pg_catalog.array_varchar_deleteidx_db_a(anyarray, character varying)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8895;
CREATE FUNCTION pg_catalog.array_varchar_deleteidx_db_a(anyarray, character varying, character varying)
 RETURNS anyarray
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$array_varchar_multi_deleteidx_db_a$function$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;