-- adding system table pg_subscription

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 6126, 6128, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_subscription
(
    subdbid oid NOCOMPRESS NOT NULL,
    subname name NOCOMPRESS,
    subowner oid NOCOMPRESS,
    subenabled bool NOCOMPRESS,
    subconninfo text NOCOMPRESS,
    subslotname name NOCOMPRESS,
    subsynccommit bool NOCOMPRESS,
    subpublications text[] NOCOMPRESS
) WITH OIDS TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6124;
CREATE UNIQUE INDEX pg_subscription_oid_index ON pg_catalog.pg_subscription USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6125;
CREATE UNIQUE INDEX pg_subscription_subname_index ON pg_catalog.pg_subscription USING BTREE(subdbid, subname);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_subscription TO PUBLIC;

-- adding system table pg_replication_origin

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 6134, 6143, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_replication_origin
(
    roident oid NOCOMPRESS NOT NULL,
    roname text NOCOMPRESS
) TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6136;
CREATE UNIQUE INDEX pg_replication_origin_roident_index ON pg_catalog.pg_replication_origin USING BTREE(roident OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 6137;
CREATE UNIQUE INDEX pg_replication_origin_roname_index ON pg_catalog.pg_replication_origin USING BTREE(roname TEXT_PATTERN_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_replication_origin TO PUBLIC;

-- adding function pg_replication_origin_create
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2635;
CREATE FUNCTION pg_catalog.pg_replication_origin_create(IN node_name text, OUT replication_origin_oid oid) RETURNS oid LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_create';

-- adding function pg_replication_origin_drop
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2636;
CREATE FUNCTION pg_catalog.pg_replication_origin_drop(IN node_name text, OUT replication_origin_oid oid) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_drop';

-- adding function pg_replication_origin_oid
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2637;
CREATE FUNCTION pg_catalog.pg_replication_origin_oid(IN node_name text, OUT replication_origin_oid oid) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_oid';

-- adding function pg_replication_origin_session_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_setup(IN node_name text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2751;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_setup(IN node_name text) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_setup';

-- adding function pg_replication_origin_session_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_reset() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2750;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_reset() RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_reset';

-- adding function pg_replication_origin_session_is_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_is_setup() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2639;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_is_setup() RETURNS boolean LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_is_setup';

-- adding function pg_replication_origin_session_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_session_progress(IN flush boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2640;
CREATE FUNCTION pg_catalog.pg_replication_origin_session_progress(IN flush boolean) RETURNS record LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_session_progress';

-- adding function pg_replication_origin_xact_setup
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2799;
CREATE FUNCTION pg_catalog.pg_replication_origin_xact_setup(IN origin_lsn text, IN origin_timestamp timestamp with time zone) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_xact_setup';

-- adding function pg_replication_origin_xact_reset
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_xact_reset() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2752;
CREATE FUNCTION pg_catalog.pg_replication_origin_xact_reset() RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_xact_reset';

-- adding function pg_replication_origin_advance
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2634;
CREATE FUNCTION pg_catalog.pg_replication_origin_advance(IN node_name text, IN lsn text) RETURNS void LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_advance';

-- adding function pg_replication_origin_progress
DROP FUNCTION IF EXISTS pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2638;
CREATE FUNCTION pg_catalog.pg_replication_origin_progress(IN node_name text, IN flush boolean) RETURNS record LANGUAGE INTERNAL STRICT AS 'pg_replication_origin_progress';

-- adding function pg_show_replication_origin_status
DROP FUNCTION IF EXISTS pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2800;
CREATE FUNCTION pg_catalog.pg_show_replication_origin_status(OUT local_id oid, OUT external_id text, OUT remote_lsn text, OUT local_lsn text) RETURNS SETOF record LANGUAGE INTERNAL STABLE ROWS 100 AS 'pg_show_replication_origin_status';

-- adding function pg_get_publication_tables
DROP FUNCTION IF EXISTS pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2801;
CREATE FUNCTION pg_catalog.pg_get_publication_tables(IN pubname text, OUT relid oid) RETURNS SETOF oid LANGUAGE INTERNAL STABLE STRICT AS 'pg_get_publication_tables';

-- adding function pg_stat_get_subscription
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2802;
CREATE FUNCTION pg_catalog.pg_stat_get_subscription(IN subid oid, OUT subid oid, OUT pid integer, OUT received_lsn text, OUT last_msg_send_time timestamp with time zone, OUT last_msg_receipt_time timestamp with time zone, OUT latest_end_lsn text, OUT latest_end_time timestamp with time zone) RETURNS record LANGUAGE INTERNAL STABLE AS 'pg_stat_get_subscription';

-- adding system view
DROP VIEW IF EXISTS pg_catalog.pg_publication_tables CASCADE;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        P.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM pg_publication P, pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid IN (SELECT relid FROM pg_get_publication_tables(P.pubname));

DROP VIEW IF EXISTS pg_catalog.pg_stat_subscription CASCADE;
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

DROP VIEW IF EXISTS pg_catalog.pg_replication_origin_status CASCADE;
CREATE VIEW pg_catalog.pg_replication_origin_status AS
    SELECT *
    FROM pg_show_replication_origin_status();

REVOKE ALL ON pg_catalog.pg_replication_origin_status FROM public;