DROP CAST IF EXISTS (INT1 AS INTERVAL);
DROP CAST IF EXISTS (INT2 AS INTERVAL);
DROP CAST IF EXISTS (INT4 AS INTERVAL);
DROP CAST IF EXISTS (FLOAT8 AS INTERVAL);
DROP CAST IF EXISTS (NUMERIC AS INTERVAL);
DROP CAST IF EXISTS (BPCHAR AS INTERVAL);
DROP CAST IF EXISTS (VARCHAR2 AS INTERVAL);
DROP CAST IF EXISTS (TEXT AS INTERVAL);

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int1, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4223;
CREATE FUNCTION pg_catalog.num_to_interval(int1, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'int1_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int2, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4224;
CREATE FUNCTION pg_catalog.num_to_interval(int2, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'int2_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(int4, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4225;
CREATE FUNCTION pg_catalog.num_to_interval(int4, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'int4_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.float8_to_interval(float8, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4230;
CREATE FUNCTION pg_catalog.float8_to_interval(float8, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'float8_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.num_to_interval(numeric, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4226;
CREATE FUNCTION pg_catalog.num_to_interval(numeric, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'numeric_to_interval';

DROP FUNCTION IF EXISTS pg_catalog.text_interval(text, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4211;
CREATE FUNCTION pg_catalog.text_interval(text, int4) RETURNS INTERVAL LANGUAGE INTERNAL IMMUTABLE AS 'text_interval';

DROP FUNCTION IF EXISTS pg_catalog.TO_INTERVAL(BPCHAR);
CREATE OR REPLACE FUNCTION pg_catalog.TO_INTERVAL(BPCHAR, int)
RETURNS INTERVAL
AS $$  select pg_catalog.interval_in(pg_catalog.bpcharout($1), 0::Oid, $2) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

DROP FUNCTION IF EXISTS pg_catalog.TO_INTERVAL(VARCHAR2);
CREATE OR REPLACE FUNCTION pg_catalog.TO_INTERVAL(VARCHAR2, int)
RETURNS INTERVAL
AS $$  select pg_catalog.interval_in(pg_catalog.varcharout($1), 0::Oid, $2) $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE CAST (INT1 AS INTERVAL) WITH FUNCTION pg_catalog.num_to_interval(INT1, int) AS IMPLICIT;
CREATE CAST (INT2 AS INTERVAL) WITH FUNCTION pg_catalog.num_to_interval(INT2, int) AS IMPLICIT;
CREATE CAST (INT4 AS INTERVAL) WITH FUNCTION pg_catalog.num_to_interval(INT4, int) AS IMPLICIT;
CREATE CAST (FLOAT8 AS INTERVAL) WITH FUNCTION pg_catalog.float8_to_interval(FLOAT8, int) AS IMPLICIT;
CREATE CAST (NUMERIC AS INTERVAL) WITH FUNCTION pg_catalog.num_to_interval(NUMERIC, int) AS IMPLICIT;
CREATE CAST (TEXT AS INTERVAL) WITH FUNCTION pg_catalog.TEXT_INTERVAL(TEXT, int) AS IMPLICIT;
CREATE CAST (BPCHAR AS INTERVAL) WITH FUNCTION pg_catalog.TO_INTERVAL(BPCHAR, int) AS IMPLICIT;
CREATE CAST (VARCHAR2 AS INTERVAL) WITH FUNCTION pg_catalog.TO_INTERVAL(VARCHAR2, int) AS IMPLICIT;
