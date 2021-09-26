/*------ add shared table gs_txn_snapshot ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 8645, 8646, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_txn_snapshot
(
    snptime timestamptz NOCOMPRESS NOT NULL,
    snpxmin int8 NOCOMPRESS NOT NULL,
    snpcsn int8 NOCOMPRESS NOT NULL,
    snpsnapshot text
)TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8653;
CREATE INDEX gs_txn_snapshot_time_csn_index ON pg_catalog.gs_txn_snapshot USING BTREE(snptime timestamptz_ops desc, snpcsn int8_ops asc);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8654;
CREATE INDEX gs_txn_snapshot_csn_xmin_index ON pg_catalog.gs_txn_snapshot USING BTREE(snpcsn int8_ops desc, snpxmin int8_ops asc);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8655;
CREATE INDEX gs_txn_snapshot_xmin_index ON pg_catalog.gs_txn_snapshot USING BTREE(snpxmin int8_ops);

/* add shared table gs_recyclebin */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 8643, 8644, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_recyclebin
(
    rcybaseid Oid NOCOMPRESS NOT NULL,
    rcydbid Oid NOCOMPRESS NOT NULL,
    rcyrelid Oid NOCOMPRESS NOT NULL,
    rcyname name NOCOMPRESS NOT NULL,
    rcyoriginname name NOCOMPRESS NOT NULL,
    rcyoperation "char" NOCOMPRESS NOT NULL,
    rcytype integer NOCOMPRESS NOT NULL,
    rcyrecyclecsn bigint NOCOMPRESS NOT NULL,
    rcyrecycletime timestamp with time zone NOCOMPRESS NOT NULL,
    rcycreatecsn bigint NOCOMPRESS NOT NULL,
    rcychangecsn bigint NOCOMPRESS NOT NULL,
    rcynamespace Oid NOCOMPRESS NOT NULL,
    rcyowner Oid NOCOMPRESS NOT NULL,
    rcytablespace Oid NOCOMPRESS NOT NULL, 
    rcyrelfilenode Oid NOCOMPRESS NOT NULL,
    rcycanrestore boolean NOCOMPRESS NOT NULL,
    rcycanpurge boolean NOCOMPRESS NOT NULL,
    rcyfrozenxid xid32 NOCOMPRESS NOT NULL,
    rcyfrozenxid64 xid NOCOMPRESS NOT NULL
)WITH OIDS TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8647;
CREATE UNIQUE INDEX gs_recyclebin_id_index ON pg_catalog.gs_recyclebin USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8648;
CREATE INDEX gs_recyclebin_baseid_index ON pg_catalog.gs_recyclebin USING BTREE(rcybaseid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8650;
CREATE INDEX gs_recyclebin_dbid_nsp_oriname_index ON pg_catalog.gs_recyclebin USING BTREE(rcynamespace OID_OPS, rcydbid OID_OPS, rcyoriginname name_ops, rcyrecyclecsn int8_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8652;
CREATE INDEX gs_recyclebin_dbid_relid_index ON pg_catalog.gs_recyclebin USING BTREE(rcydbid OID_OPS, rcyrelid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8651;
CREATE INDEX gs_recyclebin_dbid_spcid_rcycsn_index ON pg_catalog.gs_recyclebin USING BTREE(rcytablespace OID_OPS, rcydbid OID_OPS, rcyrecyclecsn int8_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8649;
CREATE INDEX gs_recyclebin_name_index ON pg_catalog.gs_recyclebin USING BTREE(rcyname name_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;