
DROP FUNCTION IF EXISTS pg_catalog.gs_lwlock_status() CASCADE;

DO
$do$
DECLARE
    from_3_0_5 boolean;
BEGIN
    select (working_version_num() >= 92608 and working_version_num() <= 92655) into from_3_0_5;

    IF from_3_0_5 = true then

        SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8888;
        CREATE FUNCTION pg_catalog.gs_lwlock_status
        (
            OUT node_name pg_catalog.text,
            OUT lock_name pg_catalog.text,
            OUT lock_unique_id pg_catalog.int8,
            OUT pid pg_catalog.int8,
            OUT sessionid pg_catalog.int8,
            OUT global_sessionid pg_catalog.text,
            OUT mode pg_catalog.text,
            OUT granted pg_catalog.bool,
            OUT start_time pg_catalog.timestamptz
        ) RETURNS SETOF record
          LANGUAGE INTERNAL
          STABLE
          as 'gs_lwlock_status';

        comment on function pg_catalog.gs_lwlock_status() is 'View system lwlock information';

    END IF;
END
$do$;


