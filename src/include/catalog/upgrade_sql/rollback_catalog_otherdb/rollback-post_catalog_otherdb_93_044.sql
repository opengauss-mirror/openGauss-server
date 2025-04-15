DROP FUNCTION IF EXISTS pg_catalog.query_parameterization_views
    (
    out reloid oid,
    out query_type name,
    out is_bypass bool,
    out param_types int2vector,
    out param_nums smallint,
    out parameterized_query text
    ) CASCADE;