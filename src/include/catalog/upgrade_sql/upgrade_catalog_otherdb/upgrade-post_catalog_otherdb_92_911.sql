DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2867;
CREATE FUNCTION pg_catalog.query_node_reform_info
(
    int4,
    OUT node_id                 int4,
    OUT reform_type             text,
    OUT reform_start_time       text,
    OUT reform_end_time         text,
    OUT is_reform_success       boolean,
    OUT redo_start_time         text,
    OUT redo_end_time           text,
    OUT xlog_total_bytes        int4,
    OUT hashmap_construct_time  text
)
RETURNS record LANGUAGE INTERNAL as 'query_node_reform_info';