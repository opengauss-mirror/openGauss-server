DROP FUNCTION IF EXISTS pg_catalog.pg_stat_segment_extent_usage(int4,int4,int4,int4);
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_segment_space_info(int4,int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_space_shrink(int4,int4,int4,int4);
DROP FUNCTION IF EXISTS pg_catalog.pg_free_remain_segment(int4,int4,int4);
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_remain_segment_info();
DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4130;
CREATE FUNCTION pg_catalog.pg_buffercache_pages
(
OUT bufferid pg_catalog.int4,
OUT relfilenode pg_catalog.oid,
OUT bucketid pg_catalog.int2,
OUT reltablespace pg_catalog.oid,
OUT reldatabase pg_catalog.oid,
OUT relforknumber pg_catalog.int4,
OUT relblocknumber pg_catalog.oid,
OUT isdirty pg_catalog.bool,
OUT usage_count pg_catalog.int2
)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_buffercache_pages';
