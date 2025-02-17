DROP FUNCTION IF EXISTS pg_catalog.nls_lower(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9538;
CREATE FUNCTION pg_catalog.nls_lower(text)
RETURNS text COST 1
AS 'nls_lower_plain'
LANGUAGE INTERNAL NOT SHIPPABLE IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.nls_lower(bytea) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9539;
CREATE FUNCTION pg_catalog.nls_lower(bytea)
RETURNS bytea COST 1
AS 'nls_lower_byte'
LANGUAGE INTERNAL NOT SHIPPABLE IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.nls_lower(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9540;
CREATE FUNCTION pg_catalog.nls_lower(text, text)
RETURNS text COST 1
AS 'nls_lower_fmt'
LANGUAGE INTERNAL NOT SHIPPABLE IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS pg_catalog.nls_lower(bytea, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9541;
CREATE FUNCTION pg_catalog.nls_lower(bytea, text)
RETURNS bytea COST 1
AS 'nls_lower_fmt_byte'
LANGUAGE INTERNAL NOT SHIPPABLE STRICT IMMUTABLE;