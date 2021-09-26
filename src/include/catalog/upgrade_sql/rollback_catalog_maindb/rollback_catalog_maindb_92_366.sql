DROP SCHEMA IF EXISTS db4ai cascade;

--delete system relation pg_catalog.gs_model_warehouse and its indexes
DROP INDEX IF EXISTS pg_toast.pg_toast_3991_index;
DROP TYPE IF EXISTS pg_toast.pg_toast_3991;
DROP TABLE IF EXISTS pg_toast.pg_toast_3991;
DROP INDEX IF EXISTS pg_catalog.gs_model_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_model_name_index;
DROP TYPE IF EXISTS pg_catalog.gs_model_warehouse;
DROP TABLE IF EXISTS pg_catalog.gs_model_warehouse;
