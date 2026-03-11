DROP TYPE IF EXISTS pg_catalog._pg_lsn CASCADE;
DROP TYPE IF EXISTS pg_catalog.pg_lsn CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3222, 3223, b;
CREATE TYPE pg_catalog.pg_lsn;

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9081;
CREATE FUNCTION pg_catalog.pg_lsn_in (
    cstring
) RETURNS pg_lsn LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_in';
COMMENT ON FUNCTION pg_catalog.pg_lsn_in(cstring) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_out(pg_lsn) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9082;
CREATE FUNCTION pg_catalog.pg_lsn_out (
    pg_lsn
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_out';
COMMENT ON FUNCTION pg_catalog.pg_lsn_out(pg_lsn) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9083;
CREATE FUNCTION pg_catalog.pg_lsn_recv (
    internal
) RETURNS pg_lsn LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_recv';
COMMENT ON FUNCTION pg_catalog.pg_lsn_recv(internal) IS 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_send(pg_lsn) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 9084;
CREATE FUNCTION pg_catalog.pg_lsn_send (
    pg_lsn
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'pg_lsn_send';
COMMENT ON FUNCTION pg_catalog.pg_lsn_send(pg_lsn) IS 'I/O';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3222, 3223, b;
CREATE TYPE pg_catalog.pg_lsn (
    INPUT = pg_lsn_in,
    OUTPUT = pg_lsn_out,
    RECEIVE = pg_lsn_recv,
    SEND = pg_lsn_send,
    INTERNALLENGTH = 8,
    PASSEDBYVALUE,
    ALIGNMENT = double,
    STORAGE = plain,
    CATEGORY = 'U'
);
COMMENT ON TYPE pg_catalog.pg_lsn IS 'PostgreSQL LSN datatype';

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.timestamp_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.interval_out($1), 0::Oid, -1)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT pg_catalog.nvarchar2in(pg_catalog.numeric_out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.int2out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select pg_catalog.nvarchar2in(pg_catalog.int4out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.int8out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.float4out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select pg_catalog.nvarchar2in(pg_catalog.float8out($1), 0::Oid, -1) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;
