SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9753, 9756, 0, 0;
DROP INDEX IF EXISTS pg_catalog.gs_matviewlog_mlogid_index;
DROP INDEX IF EXISTS pg_catalog.gs_matviewlog_relid_index;
DROP TABLE IF EXISTS pg_catalog.gs_matview_log;
DROP TYPE IF EXISTS pg_catalog.gs_matview_log;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_matview_log
(
    mlogid Oid NOCOMPRESS NOT NULL,
    relid Oid NOCOMPRESS NOT NULL
);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9754;
CREATE UNIQUE INDEX gs_matviewlog_mlogid_index ON pg_catalog.gs_matview_log USING BTREE(mlogid oid_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9755;
CREATE UNIQUE INDEX gs_matviewlog_relid_index ON pg_catalog.gs_matview_log USING BTREE(relid oid_ops);
GRANT SELECT ON TABLE pg_catalog.gs_matview_log TO PUBLIC;

INSERT INTO pg_catalog.gs_matview_log SELECT DISTINCT mlogid, relid FROM pg_catalog.gs_matview_dependency;
