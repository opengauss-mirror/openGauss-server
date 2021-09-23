ALTER TABLE IF EXISTS pg_catalog.pg_hashbucket_tmp_9027 rename to pg_hashbucket;
ALTER INDEX IF EXISTS pg_hashbucket_tmp_9027_oid_index rename to pg_hashbucket_oid_index;
ALTER INDEX IF EXISTS pg_hashbucket_tmp_9027_bid_index rename to pg_hashbucket_bid_index;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_segment_extent_usage() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7001;
CREATE FUNCTION pg_catalog.pg_stat_segment_extent_usage
(
int4,
int4,
int4,
int4,
OUT start_block pg_catalog.oid,
OUT extent_size pg_catalog.oid,
OUT usage_type pg_catalog.text,
OUT ower_location pg_catalog.oid,
OUT special_data pg_catalog.oid
)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE ROWS 100 COST 1000 STRICT as 'pg_stat_segment_extent_usage';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_segment_space_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7000;
CREATE FUNCTION pg_catalog.pg_stat_segment_space_info
(
int4,
int4,
OUT extent_size pg_catalog.oid,
OUT forknum pg_catalog.int4,
OUT total_blocks pg_catalog.oid,
OUT meta_data_blocks pg_catalog.oid,
OUT used_data_blocks pg_catalog.oid,
OUT utilization pg_catalog.float4,
OUT high_water_mark pg_catalog.oid
)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE ROWS 4 COST 100 STRICT as 'pg_stat_segment_space_info';

DROP FUNCTION IF EXISTS pg_catalog.gs_space_shrink() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7002;
CREATE FUNCTION pg_catalog.gs_space_shrink(int4,int4,int4,int4)
RETURNS int8 LANGUAGE INTERNAL IMMUTABLE COST 10000 STRICT as 'gs_space_shrink';

DROP FUNCTION IF EXISTS pg_catalog.pg_free_remain_segment() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7004;
CREATE FUNCTION pg_catalog.pg_free_remain_segment(int4,int4,int4)
RETURNS int8 LANGUAGE INTERNAL VOLATILE COST 10000 STRICT as 'pg_free_remain_segment';

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_remain_segment_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7003;
CREATE FUNCTION pg_catalog.pg_stat_remain_segment_info(
OUT space_id pg_catalog.oid,
OUT db_id pg_catalog.oid,
OUT block_id pg_catalog.oid,
OUT type pg_catalog.text
)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE COST 10000 STRICT as 'pg_stat_remain_segment_info';

DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4130;
CREATE FUNCTION pg_catalog.pg_buffercache_pages
(
OUT bufferid pg_catalog.int4,
OUT relfilenode pg_catalog.oid,
OUT bucketid pg_catalog.int2,
OUT storage_type pg_catalog.int8,
OUT reltablespace pg_catalog.oid,
OUT reldatabase pg_catalog.oid,
OUT relforknumber pg_catalog.int4,
OUT relblocknumber pg_catalog.oid,
OUT isdirty pg_catalog.bool,
OUT usage_count pg_catalog.int2
)
RETURNS SETOF record LANGUAGE INTERNAL STABLE as 'pg_buffercache_pages';
