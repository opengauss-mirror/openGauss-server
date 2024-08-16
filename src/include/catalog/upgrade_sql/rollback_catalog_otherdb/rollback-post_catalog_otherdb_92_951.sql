DROP FUNCTION IF EXISTS pg_catalog.gs_get_recv_locations() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2872;

CREATE FUNCTION pg_catalog.gs_get_recv_locations(
 out received_lsn text,
 out write_lsn text,
 out flush_lsn text,
 out replay_lsn text)
RETURNS record LANGUAGE INTERNAL VOLATILE STRICT as 'gs_get_recv_locations';
comment on function pg_catalog.gs_get_recv_locations() is 'statistics: information about currently wal locations';