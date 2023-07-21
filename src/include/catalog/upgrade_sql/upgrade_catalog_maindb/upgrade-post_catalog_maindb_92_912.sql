DROP FUNCTION IF EXISTS pg_catalog.query_page_distribution_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2866;
CREATE FUNCTION pg_catalog.query_page_distribution_info
(
    text,
    int4,
    int4,
    OUT master_id   int4,
    OUT is_master   boolean,
    OUT is_owner    boolean,
    OUT is_copy     boolean,
    OUT lock_mode   text,
    OUT mem_lsn     bigint,
    OUT disk_lsn    bigint,
    OUT is_dirty    boolean
)
RETURNS SETOF record LANGUAGE INTERNAL as 'query_page_distribution_info';