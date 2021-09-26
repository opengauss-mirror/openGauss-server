DROP FUNCTION IF EXISTS pg_catalog.local_segment_space_info() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.local_space_shrink() CASCADE;

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