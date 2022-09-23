DROP FUNCTION IF EXISTS pg_catalog.pg_export_snapshot_and_csn() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4396;
CREATE FUNCTION pg_catalog.pg_export_snapshot_and_csn(OUT snapshot text, OUT csn text) RETURNS record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_export_snapshot_and_csn';

DROP FUNCTION IF EXISTS pg_catalog.pg_get_xidlimit() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2000;
CREATE FUNCTION pg_catalog.pg_get_xidlimit(OUT next_xid xid, OUT oldest_xid xid, OUT xid_vac_limit xid, OUT xid_warn_limit xid, OUT xid_stop_limit xid, OUT xid_wrap_limit xid, OUT oldest_xid_db oid)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_get_xidlimit';

DROP FUNCTION IF EXISTS pg_catalog.pg_get_variable_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2097;
CREATE FUNCTION pg_catalog.pg_get_variable_info(OUT node_name text, OUT next_oid oid, OUT next_xid xid, OUT oldest_xid xid, OUT xid_vac_limit xid, OUT oldest_xid_db oid, OUT last_extend_csn_logpage xid, OUT start_extend_csn_logpage xid, OUT next_commit_seqno xid, OUT latest_completed_xid xid, OUT startup_max_xid xid)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE STRICT as 'pg_get_variable_info';

DROP VIEW IF EXISTS pg_catalog.pg_variable_info CASCADE;
CREATE VIEW pg_catalog.pg_variable_info AS
    SELECT * FROM pg_catalog.pg_get_variable_info();
GRANT SELECT ON pg_catalog.pg_variable_info TO PUBLIC;
