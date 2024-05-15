DROP FUNCTION IF EXISTS pg_catalog.gs_catalog_attribute_records(oid);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8010;
CREATE OR REPLACE FUNCTION pg_catalog.gs_catalog_attribute_records(
        IN relid oid,
        OUT attrelid oid,
        OUT attname name,
        OUT atttypid oid,
        OUT attstattarget integer,
        OUT attlen smallint,
        OUT attnum smallint,
        OUT attndims integer,
        OUT attcacheoff integer,
        OUT atttypmod integer,
        OUT attbyval boolean,
        OUT attstorage "char",
        OUT attalign "char",
        OUT attnotnull boolean,
        OUT atthasdef boolean,
        OUT attisdropped boolean,
        OUT attislocal boolean,
        OUT attcmprmode tinyint,
        OUT attinhcount integer,
        OUT attcollation oid,
        OUT attacl aclitem [],
        OUT attoptions text [],
        OUT attfdwoptions text [],
        OUT attinitdefval bytea,
        OUT attkvtype tinyint,
        OUT attdroppedname name
    ) RETURNS SETOF RECORD STRICT STABLE ROWS 1000 LANGUAGE INTERNAL AS 'gs_catalog_attribute_records';
COMMENT ON FUNCTION pg_catalog.gs_catalog_attribute_records(oid) IS 'attribute description for catalog relation';
