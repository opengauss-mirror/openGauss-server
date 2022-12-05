SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 2700;
CREATE INDEX pg_catalog.pg_trigger_tgname_index ON pg_catalog.pg_trigger USING BTREE(tgname name_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
