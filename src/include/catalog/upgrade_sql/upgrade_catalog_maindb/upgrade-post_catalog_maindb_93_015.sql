DROP FUNCTION IF EXISTS pg_catalog.query_imcstore_views
    (
    out reloid oid,
    out relname name,
    out imcs_attrs int2vector,
    out imcs_nattrs smallint,
    out imcs_status name,
    out is_partition bool,
    out parent_oid oid,
    out cu_size_in_mem bigint,
    out cu_num_in_mem bigint,
    out cu_size_in_disk bigint,
    out cu_num_in_disk bigint
    ) CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 6808;

CREATE FUNCTION pg_catalog.query_imcstore_views
    (
    out reloid oid,
    out relname name,
    out imcs_attrs int2vector,
    out imcs_nattrs smallint,
    out imcs_status name,
    out is_partition bool,
    out parent_oid oid,
    out cu_size_in_mem bigint,
    out cu_num_in_mem bigint,
    out cu_size_in_disk bigint,
    out cu_num_in_disk bigint
    ) RETURNS SETOF RECORD LANGUAGE INTERNAL STABLE as 'query_imcstore_views';
comment on function pg_catalog.query_imcstore_views(
    out reloid oid,
    out relname name,
    out imcs_attrs int2vector,
    out imcs_nattrs smallint,
    out imcs_status name,
    out is_partition bool,
    out parent_oid oid,
    out cu_size_in_mem bigint,
    out cu_num_in_mem bigint,
    out cu_size_in_disk bigint,
    out cu_num_in_disk bigint
) is 'query_imcstore_views';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 0;