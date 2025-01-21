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