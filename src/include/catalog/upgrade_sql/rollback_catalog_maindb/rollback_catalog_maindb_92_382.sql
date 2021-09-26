DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_args_nsp_pkg_index;
DROP INDEX IF EXISTS pg_catalog.gs_package_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_package_oid_index;
DROP TYPE IF EXISTS pg_catalog.gs_package;
DROP TABLE IF EXISTS pg_catalog.gs_package;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
