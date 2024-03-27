DO $upgrade$
BEGIN
IF working_version_num() < 92780 then
DROP FUNCTION IF EXISTS pg_catalog.sha(text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.sha1(text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.sha2(text, bigint) CASCADE;
END IF;
END $upgrade$;