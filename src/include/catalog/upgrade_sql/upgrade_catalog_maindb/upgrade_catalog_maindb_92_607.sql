-- adding system table pg_subscription_rel

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 6135, 6139, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_subscription_rel
(
    srsubid oid NOCOMPRESS NOT NULL,
    srrelid oid NOCOMPRESS NOT NULL,
    srsubstate "char" NOCOMPRESS NOT NULL,
    srcsn bigint NOCOMPRESS NOT NULL,
    srsublsn text NOCOMPRESS NOT NULL
) WITHOUT OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6138;
CREATE UNIQUE INDEX pg_subscription_rel_srrelid_srsubid_index ON pg_catalog.pg_subscription_rel USING BTREE(srrelid OID_OPS, srsubid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_subscription_rel TO PUBLIC;

-- adding builtin function pg_get_replica_identity_index

DROP FUNCTION IF EXISTS pg_catalog.pg_get_replica_identity_index(regclass);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6120;

CREATE OR REPLACE FUNCTION pg_catalog.pg_get_replica_identity_index(regclass)
returns oid
LANGUAGE internal
STABLE STRICT NOT FENCED
AS $function$pg_get_replica_identity_index$function$;
