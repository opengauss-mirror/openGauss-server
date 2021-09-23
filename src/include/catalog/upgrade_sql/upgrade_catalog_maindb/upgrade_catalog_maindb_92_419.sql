SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5035;
CREATE OR REPLACE FUNCTION pg_catalog.pg_get_indexdef(oid, bool) RETURNS text
    LANGUAGE INTERNAL STABLE STRICT NOT FENCED AS 'pg_get_indexdef_for_dump';
