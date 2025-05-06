DROP FUNCTION IF EXISTS pg_catalog.dispatch_stat_detail();
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4395;
CREATE FUNCTION pg_catalog.dispatch_stat_detail
(
    OUT thread_name pg_catalog.text,
    OUT pid pg_catalog.int8,
    OUT pending_count pg_catalog.int4,
    OUT ratio pg_catalog.float4,
    OUT detail pg_catalog.text
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'dispatch_stat_detail';

DROP FUNCTION IF EXISTS pg_catalog.gs_lwlock_status() CASCADE;
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
) RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'gs_lwlock_status';
comment on function pg_catalog.gs_lwlock_status() is 'View system lwlock information';
