-- deleting system table pg_subscription_rel
DROP INDEX IF EXISTS pg_catalog.pg_subscription_rel_srrelid_srsubid_index;
DROP TYPE IF EXISTS pg_catalog.pg_subscription_rel;
DROP TABLE IF EXISTS pg_catalog.pg_subscription_rel;

DROP FUNCTION IF EXISTS pg_catalog.pg_get_replica_identity_index(regclass);

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT relid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2802;

CREATE FUNCTION pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) RETURNS record LANGUAGE INTERNAL STABLE AS 'pg_stat_get_subscription';

DROP VIEW IF EXISTS pg_catalog.pg_stat_subscription CASCADE;

do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select relname from pg_class where relname='pg_subscription')
    LOOP
        if ans = true then
            CREATE VIEW pg_catalog.pg_stat_subscription AS
                SELECT
                        su.oid AS subid,
                        su.subname,
                        st.pid,
                        st.received_lsn,
                        st.last_msg_send_time,
                        st.last_msg_receipt_time,
                        st.latest_end_lsn,
                        st.latest_end_time
                FROM pg_subscription su
                        LEFT JOIN pg_stat_get_subscription(NULL) st
                                ON (st.subid = su.oid);
        end if;
        exit;
    END LOOP;
END$$;
