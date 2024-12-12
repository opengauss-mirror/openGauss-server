DROP FUNCTION IF EXISTS pg_catalog.to_timestamp(text, text, bool, bool, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5263;
CREATE FUNCTION pg_catalog.to_timestamp(text, text, bool, bool, text, text)
RETURNS TIMESTAMP
COST 1
as 'to_timestamp_with_default_val'
LANGUAGE INTERNAL
NOT SHIPPABLE
stable;
COMMENT ON FUNCTION pg_catalog.to_timestamp(text, text, bool, bool, text, text)  IS 'convert text to timestamp with default and format';