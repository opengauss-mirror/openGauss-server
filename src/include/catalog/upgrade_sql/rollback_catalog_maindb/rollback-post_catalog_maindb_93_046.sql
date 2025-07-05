DROP FUNCTION IF EXISTS pg_catalog.gs_create_sql_limit
(
    limit_name name,
    limit_type text,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_update_sql_limit
(
    limit_id int8,
    limit_name name,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_select_sql_limit
(
    IN limit_id int8,
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_select_sql_limit
(
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.gs_delete_sql_limit
(
    limit_id int8
) CASCADE;


DROP INDEX IF EXISTS pg_catalog.gs_sql_limit_id_index;
DROP TYPE IF EXISTS pg_catalog.gs_sql_limit;
DROP TABLE IF EXISTS pg_catalog.gs_sql_limit;

DROP FUNCTION IF EXISTS pg_catalog.realtime_build_queue_status() CASCADE;

DROP FUNCTION IF EXISTS dbe_perf.get_statement_history(
    IN start_time_point timestamp with time zone,
    IN finish_time_point timestamp with time zone) CASCADE;