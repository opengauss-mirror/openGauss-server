DROP FUNCTION IF EXISTS pg_catalog.to_timestamp(text, text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5265;
CREATE FUNCTION pg_catalog.to_timestamp(text, text, text)
RETURNS timestamp
AS 'to_timestamp_with_fmt_nls'
LANGUAGE INTERNAL
stable;
COMMENT ON FUNCTION pg_catalog.to_timestamp(text, text, text)  IS 'convert text to timestamp with format str and nls language';
