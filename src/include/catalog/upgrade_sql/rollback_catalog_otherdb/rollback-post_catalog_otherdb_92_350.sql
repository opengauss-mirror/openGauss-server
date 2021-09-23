--delete systable
DROP INDEX IF EXISTS pg_catalog.gs_txn_snapshot_csn_xmin_index;
DROP INDEX IF EXISTS pg_catalog.gs_txn_snapshot_time_csn_index;
DROP INDEX IF EXISTS pg_catalog.gs_txn_snapshot_xmin_index;
DROP TYPE IF EXISTS pg_catalog.gs_txn_snapshot;
DROP TABLE IF EXISTS pg_catalog.gs_txn_snapshot;

DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_id_index;
DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_baseid_index;
DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_dbid_nsp_oriname_index;
DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_dbid_relid_index;
DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_dbid_spcid_rcycsn_index;
DROP INDEX IF EXISTS pg_catalog.gs_recyclebin_name_index;
DROP TYPE IF EXISTS pg_catalog.gs_recyclebin;
DROP TABLE IF EXISTS pg_catalog.gs_recyclebin;