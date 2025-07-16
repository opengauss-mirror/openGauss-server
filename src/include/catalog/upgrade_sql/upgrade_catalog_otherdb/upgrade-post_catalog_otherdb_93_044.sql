DROP FUNCTION IF EXISTS pg_catalog.query_parameterization_views
    (
    out reloid oid,
    out query_type text,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query text
    ) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6809;

CREATE FUNCTION pg_catalog.query_parameterization_views
    (
    out reloid oid,
    out query_type text,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query text
    ) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE as 'query_parameterization_views';
comment on function pg_catalog.query_parameterization_views(
    out reloid oid,
    out query_type text,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query text
) is 'query_parameterization_views';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;

ALTER FUNCTION db4ai.create_snapshot(
    IN i_schema NAME,
    IN i_name NAME,
    IN i_commands TEXT[],
    IN i_vers NAME,
    IN i_comment TEXT
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.prepare_snapshot(
    IN i_schema NAME,
    IN i_parent NAME,
    IN i_commands TEXT[],
    IN i_vers NAME,
    IN i_comment TEXT
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.manage_snapshot_internal(
    IN i_schema NAME,
    IN i_name NAME,
    IN publish BOOLEAN
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.archive_snapshot(
    IN i_schema NAME,
    IN i_name NAME
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.publish_snapshot(
    IN i_schema NAME,
    IN i_name NAME
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.purge_snapshot(
    IN i_schema NAME,
    IN i_name NAME
) SET client_min_messages TO ERROR;

ALTER FUNCTION db4ai.sample_snapshot(
    IN i_schema NAME,
    IN i_parent NAME,
    IN i_sample_infixes NAME[],
    IN i_sample_ratios NUMBER[],
    IN i_stratify NAME[],
    IN i_sample_comments TEXT[]
) SET client_min_messages TO ERROR;

do $$
DECLARE
ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans from (select extname from pg_extension where extname='shark')
    LOOP
        if ans = true then
            ALTER EXTENSION shark UPDATE TO '2.0';
        end if;
        exit;
    END LOOP;
END$$;
