DROP FUNCTION IF EXISTS pg_catalog.pg_create_logical_replication_slot(name, name, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4227;
CREATE FUNCTION pg_catalog.pg_create_logical_replication_slot(
    in slotname            name,
    in plugin              name,
    in restart_lsn         text,
    in confirmed_flush     text,
    out slotname           text,
    out xlog_position      text
)
RETURNS record LANGUAGE INTERNAL as 'pg_create_logical_replication_slot_with_lsn' volatile;
comment on function pg_catalog.pg_create_logical_replication_slot(name, name, text, text) is 'set up a logical replication slot with restart_lsn and confirmed_flush';
