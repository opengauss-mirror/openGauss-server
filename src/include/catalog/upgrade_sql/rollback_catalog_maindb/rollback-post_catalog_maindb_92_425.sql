-- deleting system view
DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
DROP VIEW IF EXISTS pg_catalog.pg_stat_subscription;
DROP VIEW IF EXISTS pg_catalog.pg_replication_origin_status;

-- deleting function pg_replication_origin_create
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_drop
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_oid
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) CASCADE;

-- deleting function pg_replication_origin_session_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_setup(IN node_name text) CASCADE;

-- deleting function pg_replication_origin_session_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_reset() CASCADE;

-- deleting function pg_replication_origin_session_is_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_is_setup() CASCADE;

-- deleting function pg_replication_origin_session_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_progress(IN flush boolean) CASCADE;

-- deleting function pg_replication_origin_xact_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) CASCADE;

-- deleting function pg_replication_origin_xact_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_reset() CASCADE;

-- deleting function pg_replication_origin_advance
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) CASCADE;

-- deleting function pg_replication_origin_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) CASCADE;

-- deleting function pg_show_replication_origin_status
DROP FUNCTION IF EXISTS pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) CASCADE;

-- deleting function pg_get_publication_tables
DROP FUNCTION IF EXISTS pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) CASCADE;

-- deleting function pg_stat_get_subscription
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) CASCADE;

-- deleting system table pg_subscription

DROP INDEX IF EXISTS pg_catalog.pg_subscription_oid_index;
DROP INDEX IF EXISTS pg_catalog.pg_subscription_subname_index;
DROP TYPE IF EXISTS pg_catalog.pg_subscription;
DROP TABLE IF EXISTS pg_catalog.pg_subscription;

-- deleting system table pg_replication_origin

DROP INDEX IF EXISTS pg_catalog.pg_replication_origin_roident_index;
DROP INDEX IF EXISTS pg_catalog.pg_replication_origin_roname_index;
DROP TYPE IF EXISTS pg_catalog.pg_replication_origin;
DROP TABLE IF EXISTS pg_catalog.pg_replication_origin;