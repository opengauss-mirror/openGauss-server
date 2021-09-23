SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9996;
CREATE UNIQUE INDEX pg_catalog.pg_partition_indextblid_parentoid_reloid_index ON pg_catalog.pg_partition USING BTREE(indextblid OID_OPS, parentid OID_OPS, oid OID_OPS);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;