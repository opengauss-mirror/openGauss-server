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
