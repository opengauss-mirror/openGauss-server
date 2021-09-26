CREATE FUNCTION pg_catalog.delete_pg_authid_temp(default_role_oid oid) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'delete from pg_catalog.pg_authid where oid= '|| default_role_oid ||' ';
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION pg_catalog.delete_pg_shdepend_temp(default_role_id oid) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'delete from pg_catalog.pg_shdepend where refobjid= '|| default_role_id ||'' ;
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

SELECT pg_catalog.delete_pg_authid_temp(1055);
SELECT pg_catalog.delete_pg_shdepend_temp(1055);

DROP FUNCTION pg_catalog.delete_pg_authid_temp();
DROP FUNCTION pg_catalog.delete_pg_shdepend_temp();
