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

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1055;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_pldebugger');
SELECT pg_catalog.insert_pg_shdepend_temp(1055);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION pg_catalog.insert_pg_authid_temp();
DROP FUNCTION pg_catalog.insert_pg_shdepend_temp();

