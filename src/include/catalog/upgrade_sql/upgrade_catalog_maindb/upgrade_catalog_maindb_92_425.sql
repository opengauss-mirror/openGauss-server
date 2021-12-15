-- adding system table pg_publication

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 6130, 6141, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_publication
(
    pubname name NOCOMPRESS,
    pubowner oid NOCOMPRESS,
    puballtables bool NOCOMPRESS,
    pubinsert bool NOCOMPRESS,
    pubupdate bool NOCOMPRESS,
    pubdelete bool NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6120;
CREATE UNIQUE INDEX pg_publication_oid_index ON pg_catalog.pg_publication USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6121;
CREATE UNIQUE INDEX pg_publication_pubname_index ON pg_catalog.pg_publication USING BTREE(pubname);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_publication TO PUBLIC;

-- adding system table pg_publication_rel

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 6132, 6142, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_publication_rel
(
    prpubid oid NOCOMPRESS NOT NULL,
    prrelid oid NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6122;
CREATE UNIQUE INDEX pg_publication_rel_oid_index ON pg_catalog.pg_publication_rel USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6123;
CREATE UNIQUE INDEX pg_publication_rel_map_index ON pg_catalog.pg_publication_rel USING BTREE(prrelid, prpubid);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_publication_rel TO PUBLIC;