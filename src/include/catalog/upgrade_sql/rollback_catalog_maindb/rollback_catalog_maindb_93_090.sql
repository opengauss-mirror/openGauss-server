DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='proc_coverage_coverage_id_seq' and n.nspname='coverage' and c.relkind ='z') into ans;
    if ans = true THEN
        UPDATE pg_catalog.pg_class SET relkind = 'S' WHERE relname='proc_coverage_coverage_id_seq';
        DROP table IF EXISTS coverage.proc_coverage;
        DROP SEQUENCE IF EXISTS coverage.proc_coverage_coverage_id_seq;
        DROP SCHEMA IF EXISTS coverage cascade;
    end if;
END$$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4994;
DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='proc_coverage_coverage_id_seq' and n.nspname='coverage') into ans;
    if ans = false THEN
        CREATE SCHEMA IF NOT EXISTS coverage;
        COMMENT ON schema coverage IS 'coverage schema';

        CREATE SEQUENCE IF NOT EXISTS coverage.proc_coverage_coverage_id_seq START 1;
        CREATE unlogged table IF NOT EXISTS coverage.proc_coverage(
            coverage_id bigint NOT NULL DEFAULT nextval('coverage.proc_coverage_coverage_id_seq'::regclass),
            pro_oid oid NOT NULL,
            pro_name text NOT NULL,
            db_name text NOT NULL,
            pro_querys text NOT NULL,
            pro_canbreak bool[] NOT NULL,
            coverage int[] NOT NULL
        ) WITH (orientation=row, compression=no);
        REVOKE ALL on table coverage.proc_coverage FROM public;
    end if;
END$$;


DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='snapshot_sequence' and n.nspname='db4ai' and c.relkind ='z') into ans;
    if ans = true THEN
        UPDATE pg_catalog.pg_class SET relkind = 'S' WHERE relname='snapshot_sequence';
        DROP SEQUENCE IF EXISTS db4ai.snapshot_sequence;
    end if;
END$$;

DO $$
DECLARE
    ans boolean;
BEGIN
select case when count(*)=1 then true else false end from (select c.relname,c.relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where c.relname='snapshot_sequence' and n.nspname='db4ai') into ans;
    if ans = false THEN
        CREATE SEQUENCE IF NOT EXISTS db4ai.snapshot_sequence;
        GRANT UPDATE ON db4ai.snapshot_sequence TO PUBLIC;
    end if;
END$$;
