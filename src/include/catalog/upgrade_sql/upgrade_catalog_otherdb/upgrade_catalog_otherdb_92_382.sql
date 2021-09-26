DROP TYPE IF EXISTS pg_catalog.gs_package;
DROP TABLE IF EXISTS pg_catalog.gs_package;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG,false,true,7815,9745,0,0;

CREATE TABLE IF NOT EXISTS pg_catalog.gs_package
(
    pkgnamespace Oid NOCOMPRESS NOT NULL,
    pkgowner Oid NOCOMPRESS NOT NULL,
    pkgname name NOCOMPRESS NOT NULL,
    pkgspecsrc text NOCOMPRESS,
    pkgbodydeclsrc text NOCOMPRESS,
    pkgbodyinitsrc text NOCOMPRESS,
    pkgacl aclitem[] NOCOMPRESS
)WITH OIDS TABLESPACE pg_default;


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9993;
CREATE UNIQUE INDEX gs_package_oid_index ON pg_catalog.gs_package USING BTREE(oid oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9736;
CREATE UNIQUE INDEX gs_package_name_index ON pg_catalog.gs_package USING BTREE(pkgname name_ops, pkgnamespace oid_ops);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
