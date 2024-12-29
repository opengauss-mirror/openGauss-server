CREATE OR REPLACE VIEW pg_stat_activity_extension AS
SELECT
    pg_stat_get_backend_pid(s.backendid) AS pid,
    s.query,
    get_query_execution_time(pg_stat_get_backend_pid(s.backendid)) AS exec_duration
FROM
    pg_stat_get_activity(NULL) s;
