SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 3240, 3241, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_sql_limit
(
   limit_id int8 NOCOMPRESS,
   limit_name name NOCOMPRESS,
   is_valid bool NOCOMPRESS,
   work_node int1 NOCOMPRESS,
   max_concurrency int8 NOCOMPRESS,
   start_time timestamptz NOCOMPRESS,
   end_time timestamptz NOCOMPRESS,
   limit_type text NOCOMPRESS,
   databases name[] NOCOMPRESS,
   users name[] NOCOMPRESS,
   limit_opt text[] NOCOMPRESS
) WITHOUT OIDS TABLESPACE pg_global;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, true, true, 0, 0, 0, 3242;

CREATE INDEX IF NOT EXISTS gs_sql_limit_id_index ON pg_catalog.gs_sql_limit USING btree (limit_id int8_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8231;

CREATE OR REPLACE FUNCTION pg_catalog.gs_create_sql_limit
(
    limit_name name,
    limit_type text,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
)
RETURNS int8 NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_create_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8232;

CREATE OR REPLACE FUNCTION pg_catalog.gs_update_sql_limit
(
    limit_id int8,
    limit_name name,
    work_node int1,
    max_concurrency int8,
    start_time timestamptz,
    end_time timestamptz,
    limit_opt text[],
    databases name[],
    users name[]
)
RETURNS boolean NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_update_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8233;

CREATE OR REPLACE FUNCTION pg_catalog.gs_select_sql_limit
(
    IN limit_id int8,
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$gs_select_sql_limit$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8234;

CREATE OR REPLACE FUNCTION pg_catalog.gs_select_sql_limit
(
    OUT limit_id int8,
    OUT is_valid boolean,
    OUT work_node int1,
    OUT max_concurrency int8,
    OUT hit_count int8,
    OUT reject_count int8
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$gs_select_sql_limit_all$function$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8235;

CREATE OR REPLACE FUNCTION pg_catalog.gs_delete_sql_limit
(
    limit_id int8
)
RETURNS boolean NOT FENCED NOT SHIPPABLE STABLE
LANGUAGE internal AS $function$gs_delete_sql_limit$function$;

DROP FUNCTION IF EXISTS pg_catalog.realtime_build_queue_status() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6993;
CREATE OR REPLACE FUNCTION pg_catalog.realtime_build_queue_status(
    OUT batch_num oid,
    OUT readline_queue_size oid,
    OUT trxn_manager_queue_size oid,
    OUT trxn_worker_queue_size oid,
    OUT segworker_queue_size oid,
    OUT batchredo_queue_size1 oid,
    OUT batchredo_queue_size2 oid,
    OUT batchredo_queue_size3 oid,
    OUT batchredo_queue_size4 oid,
    OUT redomanager_queue_size1 oid,
    OUT redomanager_queue_size2 oid,
    OUT redomanager_queue_size3 oid,
    OUT redomanager_queue_size4 oid,
    OUT hashmap_manager_queue_size1 oid,
    OUT hashmap_manager_queue_size2 oid,
    OUT hashmap_manager_queue_size3 oid,
    OUT hashmap_manager_queue_size4 oid
)
RETURNS SETOF record NOT FENCED NOT SHIPPABLE ROWS 1 STABLE
LANGUAGE internal AS $function$get_realtime_build_queue_status$function$;;