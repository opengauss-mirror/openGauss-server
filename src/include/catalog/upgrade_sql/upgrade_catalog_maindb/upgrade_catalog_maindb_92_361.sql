CREATE FUNCTION pg_catalog.insert_pg_authid_temp(default_role_name text) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'INSERT INTO pg_catalog.pg_authid VALUES (''' || default_role_name || ''', false, true, false, false, false, false, false, false, false, -1, null, null, null, ''default_pool'', false, 0, null, ''n'', 0, null, null, null, false, false, false)';
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION pg_catalog.insert_pg_shdepend_temp(default_role_id oid) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'INSERT INTO pg_catalog.pg_shdepend VALUES (0,0,0,0, 1260,' || default_role_id || ', ''p'')';
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1044;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_copy_files');
SELECT pg_catalog.insert_pg_shdepend_temp(1044);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1045;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_signal_backend');
SELECT pg_catalog.insert_pg_shdepend_temp(1045);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1046;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_tablespace');
SELECT pg_catalog.insert_pg_shdepend_temp(1046);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1047;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_replication');
SELECT pg_catalog.insert_pg_shdepend_temp(1047);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1048;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_account_lock');
SELECT pg_catalog.insert_pg_shdepend_temp(1048);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION pg_catalog.insert_pg_authid_temp();
DROP FUNCTION pg_catalog.insert_pg_shdepend_temp();
