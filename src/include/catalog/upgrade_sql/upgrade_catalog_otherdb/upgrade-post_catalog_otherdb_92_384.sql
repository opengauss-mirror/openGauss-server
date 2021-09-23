-- ----------------------------------------------------------------
-- upgrade get_paxos_replication_info
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.get_paxos_replication_info(OUT paxos_write_location text, OUT paxos_commit_location text, OUT local_write_location text, OUT local_flush_location text, OUT local_replay_location text, OUT dcf_replication_info text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8001;
CREATE FUNCTION pg_catalog.get_paxos_replication_info(OUT paxos_write_location text, OUT paxos_commit_location text, OUT local_write_location text, OUT local_flush_location text, OUT local_replay_location text, OUT dcf_replication_info text) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 as 'get_paxos_replication_info';
