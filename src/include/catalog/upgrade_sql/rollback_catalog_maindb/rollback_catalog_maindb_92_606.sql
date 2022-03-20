-- for any x group
DO $$
DECLARE
ans boolean;
BEGIN
    select case when working_version_num()=92301 then true else false end as ans into ans;
    if ans = false then
        DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_wal_senders() CASCADE;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3099;
        CREATE FUNCTION pg_catalog.pg_stat_get_wal_senders(
            OUT pid bigint,
            OUT sender_pid integer,
            OUT local_role text,
            OUT peer_role text,
            OUT peer_state text,
            OUT state text,
            OUT catchup_start timestamp with time zone,
            OUT catchup_end timestamp with time zone,
            OUT sender_sent_location text,
            OUT sender_write_location text,
            OUT sender_flush_location text,
            OUT sender_replay_location text,
            OUT receiver_received_location text,
            OUT receiver_write_location text,
            OUT receiver_flush_location text,
            OUT receiver_replay_location text,
            OUT sync_percent text,
            OUT sync_state text,
            OUT sync_priority integer,
            OUT sync_most_available text,
            OUT channel text
        ) RETURNS SETOF record
        STABLE NOT FENCED NOT SHIPPABLE ROWS 10
        LANGUAGE internal AS 'pg_stat_get_wal_senders';
    end if;
END$$;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;
