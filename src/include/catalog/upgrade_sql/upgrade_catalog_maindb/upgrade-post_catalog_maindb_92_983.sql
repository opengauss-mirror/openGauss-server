DROP FUNCTION IF EXISTS pg_catalog.realtime_build_log_ctrl_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6992;
CREATE FUNCTION pg_catalog.realtime_build_log_ctrl_status(
    out ins_id                int4,
    out current_rto           int8,
    out prev_reply_time       timestamp with time zone,
    out prev_calculate_time   timestamp with time zone,
    out reply_time            timestamp with time zone,
    out period_total_build    int8,
    out build_rate            int8,
    out prev_build_ptr        text,
    out realtime_build_ptr    text,
    out current_insert_lsn    text,
    out sleepTime             int4
)
RETURNS SETOF record LANGUAGE INTERNAL as 'realtime_build_log_ctrl_status' stable;