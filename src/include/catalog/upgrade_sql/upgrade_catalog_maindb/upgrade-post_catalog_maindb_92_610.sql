-- regexp_count
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 385;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 386;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 387;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_count(text, text, int, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_count_matchopt$function$;

-- regexp_instr
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 630;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 631;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 632;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_occurren$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 633;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int, int)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_returnopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 634;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_instr(text, text, int, int, int, text)
 RETURNS integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_instr_matchopt$function$;

-- regexp_replace
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1116;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_noopt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1117;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1118;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_occur$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1119;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_replace(text, text, text, int, int, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_replace_matchopt$function$;

-- regexp_substr
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1566;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_position$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1567;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int, int)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_occur$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1568;
CREATE OR REPLACE FUNCTION pg_catalog.regexp_substr(text, text, int, int, text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$regexp_substr_with_opt$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;