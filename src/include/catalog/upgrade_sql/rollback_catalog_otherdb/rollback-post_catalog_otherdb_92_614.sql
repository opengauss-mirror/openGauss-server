DROP FUNCTION IF EXISTS pg_catalog.age(xid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 1181;
CREATE OR REPLACE FUNCTION pg_catalog.age(xid)
RETURNS int LANGUAGE internal STRICT STABLE NOT FENCED as 'xid_age';
COMMENT ON FUNCTION pg_catalog.age(xid) IS 'age of a transaction ID, in transactions before current transaction';