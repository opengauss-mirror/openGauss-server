DROP FUNCTION IF EXISTS pg_catalog.gen_random_uuid() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8025;
CREATE OR REPLACE FUNCTION pg_catalog.gen_random_uuid() RETURNS uuid
LANGUAGE INTERNAL VOLATILE STRICT SHIPPABLE NOT FENCED as 'gen_random_uuid';

COMMENT ON FUNCTION pg_catalog.gen_random_uuid() IS 'generate random UUID';
