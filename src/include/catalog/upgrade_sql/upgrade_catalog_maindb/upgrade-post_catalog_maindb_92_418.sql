DROP FUNCTION IF EXISTS pg_catalog.pg_stat_segment_space_info() CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.local_segment_space_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7005;
CREATE FUNCTION pg_catalog.local_segment_space_info
(
text,
text,
OUT node_name pg_catalog.text,
OUT extent_size pg_catalog.oid,
OUT forknum pg_catalog.int4,
OUT total_blocks pg_catalog.oid,
OUT meta_data_blocks pg_catalog.oid,
OUT used_data_blocks pg_catalog.oid,
OUT utilization pg_catalog.float4,
OUT high_water_mark pg_catalog.oid
)
RETURNS SETOF record LANGUAGE INTERNAL VOLATILE ROWS 4 COST 100 STRICT as 'local_segment_space_info';

DROP FUNCTION IF EXISTS pg_catalog.local_space_shrink() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 7006;
CREATE FUNCTION pg_catalog.local_space_shrink(text, text)
    RETURNS int8 LANGUAGE INTERNAL IMMUTABLE COST 10000 STRICT as 'local_space_shrink';
