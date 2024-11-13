DROP FUNCTION IF EXISTS pg_catalog.to_char(timestamp, text, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6551; 
CREATE FUNCTION pg_catalog.to_char ( 
timestamp,
text,
text
) RETURNS text LANGUAGE INTERNAL VOLATILE STRICT as 'to_char';

DROP FUNCTION IF EXISTS pg_catalog.to_char(timestamp with time zone, text, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6552; 
CREATE FUNCTION pg_catalog.to_char ( 
timestamp with time zone,
text,
text
) RETURNS text LANGUAGE INTERNAL VOLATILE STRICT as 'to_char';

DROP FUNCTION IF EXISTS pg_catalog.to_char(interval, text, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6553; 
CREATE FUNCTION pg_catalog.to_char ( 
interval,
text,
text
) RETURNS text LANGUAGE INTERNAL VOLATILE STRICT as 'to_char';

DROP FUNCTION IF EXISTS pg_catalog.to_char(blob, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9753; 
CREATE FUNCTION pg_catalog.to_char ( 
blob,
text
) RETURNS text LANGUAGE INTERNAL VOLATILE STRICT as 'to_char';

DROP FUNCTION IF EXISTS pg_catalog.to_char(blob) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9754; 
CREATE FUNCTION pg_catalog.to_char ( 
blob
) RETURNS text LANGUAGE INTERNAL VOLATILE STRICT as 'to_char';

COMMENT ON FUNCTION pg_catalog.to_char(timestamp, text, text) is 'format timestamp to text with nls param';
COMMENT ON FUNCTION pg_catalog.to_char(timestamp with time zone, text, text) is 'format timestamp with time zone to text with nls param';
COMMENT ON FUNCTION pg_catalog.to_char(interval, text, text) is 'format interval to text with nls param';
COMMENT ON FUNCTION pg_catalog.to_char(blob, text) is 'blob to text';
COMMENT ON FUNCTION pg_catalog.to_char(blob) is 'blob to text database encoding';