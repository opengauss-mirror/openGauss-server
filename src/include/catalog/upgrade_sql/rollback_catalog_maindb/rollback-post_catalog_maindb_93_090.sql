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

-- 12. 回滚 coverage.proc_coverage：将新版本 relkind='z' 的序列重建为 'S'
DO $$
DECLARE
    ans boolean;
BEGIN
    SELECT CASE WHEN count(*)=1 THEN true ELSE false END
      FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n
     WHERE c.relname='proc_coverage_coverage_id_seq'
       AND n.nspname='coverage'
       AND c.relnamespace=n.oid
       AND c.relkind='z' INTO ans;
    IF ans = true THEN
        DROP TABLE IF EXISTS coverage.proc_coverage;
        DROP SEQUENCE IF EXISTS coverage.proc_coverage_coverage_id_seq;
        CREATE SEQUENCE coverage.proc_coverage_coverage_id_seq START 1;
        CREATE UNLOGGED TABLE coverage.proc_coverage(
            coverage_id bigint NOT NULL DEFAULT nextval('coverage.proc_coverage_coverage_id_seq'::regclass),
            pro_oid oid NOT NULL,
            pro_name text NOT NULL,
            db_name text NOT NULL,
            pro_querys text NOT NULL,
            pro_canbreak bool[] NOT NULL,
            coverage int[] NOT NULL
        ) WITH (orientation=row, compression=no);
        REVOKE ALL on table coverage.proc_coverage FROM public;
    END IF;
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
