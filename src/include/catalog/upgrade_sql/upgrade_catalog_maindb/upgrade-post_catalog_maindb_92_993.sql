DROP FUNCTION IF EXISTS pg_catalog.ss_ub_link_available() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6226;
CREATE OR REPLACE FUNCTION pg_catalog.ss_ub_link_available()
RETURNS boolean
LANGUAGE internal
STABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$ss_ub_link_available$function$;

COMMENT ON FUNCTION pg_catalog.ss_ub_link_available() IS 'whether the UB cache link is currently available';
