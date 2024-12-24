-- create gs_stat_walsender
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walsender(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2864;
CREATE FUNCTION pg_catalog.gs_stat_walsender(
    in  operation                   int4 default 2,
    out is_enable_stat              boolean,
    out channel                     text,
    out cur_time                    timestamptz,
    out send_times                  xid,
    out first_send_time             timestamptz,
    out last_send_time              timestamptz,
    out last_reset_time             timestamptz,
    out avg_send_interval           xid,
    out since_last_send_interval    xid
)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_stat_walsender' stable;

-- create gs_stat_walreceiver
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walreceiver(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2865;
CREATE FUNCTION pg_catalog.gs_stat_walreceiver(
    in  operation                 int4 default 2,
    out is_enable_stat            boolean,
    out buffer_current_size       xid,
    out buffer_full_times         xid,
    out wake_writer_times         xid,
    out avg_wake_interval         xid,
    out since_last_wake_interval  xid,
    out first_wake_time           timestamptz,
    out last_wake_time            timestamptz,
    out last_reset_time           timestamptz,
    out cur_time                  timestamptz
)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_stat_walreceiver' stable;

-- create gs_stat_walrecvwriter
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_walrecvwriter(int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2868;
CREATE FUNCTION pg_catalog.gs_stat_walrecvwriter(
    in  operation           int4 default 2,
    out is_enable_stat      boolean,
    out total_write_bytes   xid,
    out write_times         xid,
    out total_write_time    xid,
    out avg_write_time      xid,
    out avg_write_bytes     xid,
    out total_sync_bytes    xid,
    out sync_times          xid,
    out total_sync_time     xid,
    out avg_sync_time       xid,
    out avg_sync_bytes      xid,
    out current_xlog_segno  xid,
    out inited_xlog_segno   xid,
    out last_reset_time     timestamptz,
    out cur_time            timestamptz
)
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_stat_walrecvwriter' stable;