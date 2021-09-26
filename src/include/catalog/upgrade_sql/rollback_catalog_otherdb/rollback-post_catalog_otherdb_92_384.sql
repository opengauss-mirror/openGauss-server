-- ----------------------------------------------------------------
-- rollback get_paxos_replication_info
-- ----------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.get_paxos_replication_info(OUT paxos_write_location text, OUT paxos_commit_location text, OUT local_write_location text, OUT local_flush_location text, OUT local_replay_location text, OUT dcf_replication_info text) CASCADE;
