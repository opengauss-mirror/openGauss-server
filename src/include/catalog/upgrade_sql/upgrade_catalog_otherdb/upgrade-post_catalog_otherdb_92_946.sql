DROP FUNCTION IF EXISTS pg_catalog.pg_query_audit(timestamptz, timestamptz) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3780;
CREATE FUNCTION pg_catalog.pg_query_audit(
    IN "begin" TIMESTAMPTZ,
    IN "end" TIMESTAMPTZ,
    OUT "time" TIMESTAMPTZ,
    OUT type TEXT,
    OUT result TEXT,
    OUT userid TEXT,
    OUT username TEXT,
    OUT database TEXT,
    OUT client_conninfo TEXT,
    OUT object_name TEXT,
    OUT detail_info TEXT,
    OUT node_name TEXT,
    OUT thread_id TEXT,
    OUT local_port TEXT,
    OUT remote_port TEXT,
    OUT sha_code TEXT,
    OUT verify_result BOOLEAN
) RETURNS SETOF RECORD LANGUAGE INTERNAL VOLATILE ROWS 10 as 'pg_query_audit';
DROP FUNCTION IF EXISTS pg_catalog.pg_query_audit(timestamptz, timestamptz, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 3782;
CREATE FUNCTION pg_catalog.pg_query_audit(
    IN "begin" TIMESTAMPTZ,
    IN "end" TIMESTAMPTZ,
    IN directory TEXT,
    OUT "time" TIMESTAMPTZ,
    OUT type TEXT,
    OUT result TEXT,
    OUT userid TEXT,
    OUT username TEXT,
    OUT database TEXT,
    OUT client_conninfo TEXT,
    OUT object_name TEXT,
    OUT detail_info TEXT,
    OUT node_name TEXT,
    OUT thread_id TEXT,
    OUT local_port TEXT,
    OUT remote_port TEXT,
    OUT sha_code TEXT,
    OUT verify_result BOOLEAN
) RETURNS SETOF RECORD LANGUAGE INTERNAL VOLATILE ROWS 10 as 'pg_query_audit';