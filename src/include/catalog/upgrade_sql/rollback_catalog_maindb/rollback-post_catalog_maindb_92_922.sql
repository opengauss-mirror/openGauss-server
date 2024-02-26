DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2867;
CREATE OR REPLACE FUNCTION pg_catalog.query_node_reform_info
(
    OUT reform_node_id          integer, 
    OUT reform_type             text, 
    OUT reform_start_time       text, 
    OUT reform_end_time         text, 
    OUT is_reform_success       boolean, 
    OUT redo_start_time         text, 
    OUT redo_end_time           text, 
    OUT xlog_total_bytes        integer, 
    OUT hashmap_construct_time  text, 
    OUT action                  text
)
 RETURNS SETOF record
 LANGUAGE internal
 STRICT NOT FENCED NOT SHIPPABLE ROWS 64
AS $function$query_node_reform_info$function$;