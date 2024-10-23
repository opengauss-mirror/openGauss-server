DROP FUNCTION IF EXISTS pg_catalog.timezone_extract(text, timestamp without time zone);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2060;
CREATE OR REPLACE FUNCTION pg_catalog.timezone_extract(text, timestamp without time zone)
RETURNS text
LANGUAGE internal
VOLATILE STRICT
AS 'timestamp_extract_zone';
COMMENT ON FUNCTION pg_catalog.timezone_extract(text, timestamp without time zone) IS 'extract timezone information from timestamp without time zone';

DROP FUNCTION IF EXISTS pg_catalog.timezone_extract(text, timestamp with time zone);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2061;
CREATE OR REPLACE FUNCTION pg_catalog.timezone_extract(text, timestamp with time zone)
RETURNS text
LANGUAGE internal
VOLATILE STRICT
AS 'timestamptz_extract_zone';
COMMENT ON FUNCTION pg_catalog.timezone_extract(text, timestamp with time zone) IS 'extract timezone information from timestamp with time zone';

DROP FUNCTION IF EXISTS pg_catalog.timezone_extract(text, interval);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2062;
CREATE OR REPLACE FUNCTION pg_catalog.timezone_extract(text, interval)
RETURNS text
LANGUAGE internal
VOLATILE STRICT
AS 'interval_extract_zone';
COMMENT ON FUNCTION pg_catalog.timezone_extract(text, interval) IS 'extract timezone information from interval';

DROP FUNCTION IF EXISTS pg_catalog.timezone_extract(text, time);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2063;
CREATE OR REPLACE FUNCTION pg_catalog.timezone_extract(text, time)
RETURNS text
LANGUAGE internal
VOLATILE STRICT
AS 'time_extract_zone';
COMMENT ON FUNCTION pg_catalog.timezone_extract(text, time) IS 'extract timezone information from time';

DROP FUNCTION IF EXISTS pg_catalog.timezone_extract(text, timetz);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 2064;
CREATE OR REPLACE FUNCTION pg_catalog.timezone_extract(text, timetz)
RETURNS text
LANGUAGE internal
VOLATILE STRICT
AS 'timetz_extract_zone';
COMMENT ON FUNCTION pg_catalog.timezone_extract(text, timetz) IS 'extract timezone information from timetz';