-- --------------------------------------------------------------
-- upgrade pg_catalog.pg_buffercache_pages
-- --------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4130;
CREATE OR REPLACE FUNCTION pg_catalog.pg_buffercache_pages
(out bufferid pg_catalog.int4,
out relfilenode pg_catalog.oid,
out bucketid pg_catalog.int4,
out storage_type pg_catalog.int2,
out reltablespace pg_catalog.oid,
out reldatabase pg_catalog.oid,
out relforknumber pg_catalog.int2,
out relblocknumber pg_catalog.int8,
out isdirty pg_catalog.bool,
out isvalid pg_catalog.bool,
out usage_count pg_catalog.int2,
out pinning_backends pg_catalog.int4)
RETURNS SETOF record LANGUAGE INTERNAL STABLE STRICT as 'pg_buffercache_pages';