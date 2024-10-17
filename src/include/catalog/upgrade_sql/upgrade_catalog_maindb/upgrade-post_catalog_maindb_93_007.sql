SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 970;
CREATE FUNCTION pg_catalog.new_time(timestamp, text, text)
RETURNS record LANGUAGE INTERNAL IMMUTABLE STRICT as 'new_time';
COMMENT ON FUNCTION pg_catalog.new_time(timestamp, text, text) IS 'convert date and time in timezone1 to timezone2';