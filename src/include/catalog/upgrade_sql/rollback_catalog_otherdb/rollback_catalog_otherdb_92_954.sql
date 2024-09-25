DROP FUNCTION IF EXISTS pg_catalog.ondemand_recovery_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6991;
CREATE FUNCTION pg_catalog.ondemand_recovery_status(
    out primary_checkpoint_redo_lsn text,
    out realtime_build_replayed_lsn text,
    out hashmap_used_blocks         oid,
    out hashmap_total_blocks        oid,
    out trxn_queue_blocks           oid,
    out seg_queue_blocks            oid,
    out in_ondemand_recovery        boolean,
    out ondemand_recovery_status    text,
    out realtime_build_status       text,
    out recovery_pause_status       text
)
RETURNS SETOF record LANGUAGE INTERNAL as 'ondemand_recovery_status' stable;