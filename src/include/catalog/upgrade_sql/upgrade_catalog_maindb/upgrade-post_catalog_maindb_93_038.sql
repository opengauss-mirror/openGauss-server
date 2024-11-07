DROP FUNCTION IF EXISTS pg_catalog.to_char(unknown, unknown) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9756; 
CREATE FUNCTION pg_catalog.to_char ( 
unknown,
unknown
) RETURNS text LANGUAGE INTERNAL STABLE SHIPPABLE STRICT as 'unknown_to_char2';

COMMENT ON FUNCTION pg_catalog.to_char(unknown, unknown) is 'unknown,unknown to char for compatibility';