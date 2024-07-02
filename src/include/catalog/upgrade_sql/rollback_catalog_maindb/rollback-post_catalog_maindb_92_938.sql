DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int1, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4223;
CREATE FUNCTION pg_catalog.num_to_interval(int1, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE  AS 'int1_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int2, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4224;
CREATE FUNCTION pg_catalog.num_to_interval(int2, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'int2_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int4, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4225;
CREATE FUNCTION pg_catalog.num_to_interval(int4, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'int4_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.float8_to_interval(float8, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4230;
CREATE FUNCTION pg_catalog.float8_to_interval(float8, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE  AS 'float8_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(numeric, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4226;
CREATE FUNCTION pg_catalog.num_to_interval(numeric, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE  AS 'numeric_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.text_interval(text, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4211;
CREATE FUNCTION pg_catalog.text_interval(text, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE  AS 'text_interval';
