DROP FUNCTION IF EXISTS pg_catalog.pubddl_decode(pubddl bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4648;
CREATE FUNCTION pg_catalog.pubddl_decode (pubddl bigint) 
RETURNS text 
LANGUAGE INTERNAL IMMUTABLE STRICT NOT FENCED
AS 'pubddl_decode';