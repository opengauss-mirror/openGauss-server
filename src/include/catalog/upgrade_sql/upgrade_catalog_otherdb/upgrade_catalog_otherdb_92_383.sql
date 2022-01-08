--add gs_encrypted_proc
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9750, 9753, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_encrypted_proc
(
    func_id Oid NOCOMPRESS NOT NULL,
    prorettype_orig int4 NOCOMPRESS,
    proargcachedcol oidvector NOCOMPRESS,
    proallargtypes_orig Oid[1] NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9751;
CREATE UNIQUE INDEX gs_encrypted_proc_oid ON pg_catalog.gs_encrypted_proc USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9752;
CREATE UNIQUE INDEX gs_encrypted_proc_func_id_index ON pg_catalog.gs_encrypted_proc USING BTREE(func_id OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON pg_catalog.gs_encrypted_proc TO PUBLIC;