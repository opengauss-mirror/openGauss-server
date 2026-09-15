-- Rollback pg_lsn type and related I/O functions
-- This rollback script removes the pg_lsn type and its I/O functions

DO $$
DECLARE
ans boolean;
BEGIN
    -- Check if pg_lsn type exists
    select case when count(*)=1 then true else false end as ans 
    from (select * from pg_type where typname = 'pg_lsn' limit 1) into ans;
    
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_send(pg_lsn) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_recv(internal) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_out(pg_lsn) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.pg_lsn_in(cstring) CASCADE;
    end if;
END$$;

DROP TYPE IF EXISTS pg_catalog._pg_lsn CASCADE;
DROP TYPE IF EXISTS pg_catalog.pg_lsn CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(TIMESTAMP WITHOUT TIME ZONE)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.timestamp_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INTERVAL)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.interval_out($1) AS NVARCHAR2)  $$
LANGUAGE SQL IMMUTABLE STRICT NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(NUMERIC)
RETURNS NVARCHAR2
AS $$ SELECT CAST(pg_catalog.numeric_out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT2)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int2out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT4)
RETURNS NVARCHAR2
AS $$  select CAST(pg_catalog.int4out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(INT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.int8out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT4)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float4out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;

CREATE OR REPLACE FUNCTION pg_catalog.TO_NVARCHAR2(FLOAT8)
RETURNS NVARCHAR2
AS $$ select CAST(pg_catalog.float8out($1) AS NVARCHAR2) $$
LANGUAGE SQL STRICT IMMUTABLE NOT FENCED;
