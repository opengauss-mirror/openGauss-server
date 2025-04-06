DROP FUNCTION IF EXISTS pg_catalog.query_parameterization_views
    (
    out reloid oid,
    out query_type name,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query name
    ) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6809;

CREATE FUNCTION pg_catalog.query_parameterization_views
    (
    out reloid oid,
    out query_type name,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query name
    ) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE as 'query_parameterization_views';
comment on function pg_catalog.query_parameterization_views(
    out reloid oid,
    out query_type name,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query name
) is 'query_parameterization_views';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;
