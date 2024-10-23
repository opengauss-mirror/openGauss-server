
DROP FUNCTION IF EXISTS pg_catalog.gs_lwlock_status() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8888;

CREATE FUNCTION pg_catalog.gs_lwlock_status(
    OUT node_name text,
    OUT lock_name text,
    OUT lock_unique_id bigint,
    OUT pid bigint,
    OUT sessionid bigint,
    OUT global_sessionid text,
    OUT mode text,
    OUT granted boolean,
    OUT start_time timestamp with time zone,
    OUT tag text
) RETURNS SETOF record
 LANGUAGE internal
 STABLE NOT FENCED NOT SHIPPABLE
AS $function$gs_lwlock_status$function$;

comment on function pg_catalog.gs_lwlock_status() is 'View system lwlock information';

