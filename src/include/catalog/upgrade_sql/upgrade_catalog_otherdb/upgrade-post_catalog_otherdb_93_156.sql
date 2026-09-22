DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_hit_rate CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_hash_chain CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_chunk CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_pool CASCADE;
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer_hit_rate(
    OUT lookups int8,
    OUT hits int8,
    OUT misses int8,
    OUT hit_rate numeric
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer(
    OUT capacity_bytes int8,
    OUT configured_capacity_bytes int8,
    OUT used_bytes int8,
    OUT chunk_size int4,
    OUT chunk_count int4,
    OUT max_vbps int4,
    OUT min_payload int4,
    OUT entries int4,
    OUT n_free_total int4,
    OUT active_pools int4,
    OUT evict_pending int4,
    OUT installs int8,
    OUT fallbacks int8,
    OUT evictions int8,
    OUT pool_full_requests int8,
    OUT reclaim_attempts int8,
    OUT reclaim_victims int8,
    OUT invalidations int8,
    OUT invalidated_entries int8
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer_pool(
    OUT vbp_id int4,
    OUT generation int4,
    OUT state text,
    OUT spcnode oid,
    OUT dbnode oid,
    OUT relfilenode oid,
    OUT payload_len int4,
    OUT slot_size int4,
    OUT live_entries int4,
    OUT n_free_total int4,
    OUT n_occupied_total int4,
    OUT n_chunks_used int4,
    OUT scan_refs int4,
    OUT evict_requested boolean
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer_chunk(
    OUT chunk_index int4,
    OUT vbp_id int4,
    OUT vbp_generation int4,
    OUT state text,
    OUT n_free int4,
    OUT n_reserved int4,
    OUT n_cached int4,
    OUT n_quarantined int4,
    OUT slot_count int4,
    OUT slot_stride int4,
    OUT in_cl boolean,
    OUT in_freelist boolean
) CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer_hash_chain(
    OUT vbp_id int4,
    OUT generation int4,
    OUT live_entries int4,
    OUT bucket_count int4,
    OUT n0 int4,
    OUT n1 int4,
    OUT n2 int4,
    OUT n3 int4,
    OUT n_ge4 int4,
    OUT max_chain int4,
    OUT chained_nodes int4,
    OUT truncated int4,
    OUT rehashing boolean,
    OUT migrated_buckets int4,
    OUT candidate_bucket_count int4
) CASCADE;

/* pg_stat_get_vector_buffer */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8932;
CREATE FUNCTION pg_catalog.pg_stat_get_vector_buffer (
    OUT capacity_bytes int8,
    OUT configured_capacity_bytes int8,
    OUT used_bytes int8,
    OUT chunk_size int4,
    OUT chunk_count int4,
    OUT max_vbps int4,
    OUT min_payload int4,
    OUT entries int4,
    OUT n_free_total int4,
    OUT active_pools int4,
    OUT evict_pending int4,
    OUT installs int8,
    OUT fallbacks int8,
    OUT evictions int8,
    OUT pool_full_requests int8,
    OUT reclaim_attempts int8,
    OUT reclaim_victims int8,
    OUT invalidations int8,
    OUT invalidated_entries int8
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED COST 1 ROWS 1 as 'pg_stat_get_vector_buffer';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_vector_buffer(
    OUT capacity_bytes int8,
    OUT configured_capacity_bytes int8,
    OUT used_bytes int8,
    OUT chunk_size int4,
    OUT chunk_count int4,
    OUT max_vbps int4,
    OUT min_payload int4,
    OUT entries int4,
    OUT n_free_total int4,
    OUT active_pools int4,
    OUT evict_pending int4,
    OUT installs int8,
    OUT fallbacks int8,
    OUT evictions int8,
    OUT pool_full_requests int8,
    OUT reclaim_attempts int8,
    OUT reclaim_victims int8,
    OUT invalidations int8,
    OUT invalidated_entries int8
) IS 'statistics: vector buffer pool occupancy and counters';

/* pg_stat_get_vector_buffer_pool */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8933;
CREATE FUNCTION pg_catalog.pg_stat_get_vector_buffer_pool (
    OUT vbp_id int4,
    OUT generation int4,
    OUT state text,
    OUT spcnode oid,
    OUT dbnode oid,
    OUT relfilenode oid,
    OUT payload_len int4,
    OUT slot_size int4,
    OUT live_entries int4,
    OUT n_free_total int4,
    OUT n_occupied_total int4,
    OUT n_chunks_used int4,
    OUT scan_refs int4,
    OUT evict_requested boolean
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED COST 1 ROWS 10 as 'pg_stat_get_vector_buffer_pool';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_vector_buffer_pool(
    OUT vbp_id int4,
    OUT generation int4,
    OUT state text,
    OUT spcnode oid,
    OUT dbnode oid,
    OUT relfilenode oid,
    OUT payload_len int4,
    OUT slot_size int4,
    OUT live_entries int4,
    OUT n_free_total int4,
    OUT n_occupied_total int4,
    OUT n_chunks_used int4,
    OUT scan_refs int4,
    OUT evict_requested boolean
) IS 'statistics: per-pool occupancy for vector buffer';

/* pg_stat_get_vector_buffer_chunk */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8934;
CREATE FUNCTION pg_catalog.pg_stat_get_vector_buffer_chunk (
    OUT chunk_index int4,
    OUT vbp_id int4,
    OUT vbp_generation int4,
    OUT state text,
    OUT n_free int4,
    OUT n_reserved int4,
    OUT n_cached int4,
    OUT n_quarantined int4,
    OUT slot_count int4,
    OUT slot_stride int4,
    OUT in_cl boolean,
    OUT in_freelist boolean
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED COST 1 ROWS 100 as 'pg_stat_get_vector_buffer_chunk';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_vector_buffer_chunk(
    OUT chunk_index int4,
    OUT vbp_id int4,
    OUT vbp_generation int4,
    OUT state text,
    OUT n_free int4,
    OUT n_reserved int4,
    OUT n_cached int4,
    OUT n_quarantined int4,
    OUT slot_count int4,
    OUT slot_stride int4,
    OUT in_cl boolean,
    OUT in_freelist boolean
) IS 'statistics: per-chunk four-state occupancy for vector buffer';

/* pg_stat_get_vector_buffer_hash_chain */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8935;
CREATE FUNCTION pg_catalog.pg_stat_get_vector_buffer_hash_chain (
    OUT vbp_id int4,
    OUT generation int4,
    OUT live_entries int4,
    OUT bucket_count int4,
    OUT n0 int4,
    OUT n1 int4,
    OUT n2 int4,
    OUT n3 int4,
    OUT n_ge4 int4,
    OUT max_chain int4,
    OUT chained_nodes int4,
    OUT truncated int4,
    OUT rehashing boolean,
    OUT migrated_buckets int4,
    OUT candidate_bucket_count int4
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED COST 1 ROWS 10 as 'pg_stat_get_vector_buffer_hash_chain';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_vector_buffer_hash_chain(
    OUT vbp_id int4,
    OUT generation int4,
    OUT live_entries int4,
    OUT bucket_count int4,
    OUT n0 int4,
    OUT n1 int4,
    OUT n2 int4,
    OUT n3 int4,
    OUT n_ge4 int4,
    OUT max_chain int4,
    OUT chained_nodes int4,
    OUT truncated int4,
    OUT rehashing boolean,
    OUT migrated_buckets int4,
    OUT candidate_bucket_count int4
) IS 'statistics: unlocked hash-chain length histogram for vector buffer';
/* pg_stat_get_vector_buffer_hit_rate */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8936;
CREATE FUNCTION pg_catalog.pg_stat_get_vector_buffer_hit_rate (
    OUT lookups int8,
    OUT hits int8,
    OUT misses int8,
    OUT hit_rate numeric
) RETURNS setof record LANGUAGE INTERNAL STABLE NOT FENCED COST 1 ROWS 1 as 'pg_stat_get_vector_buffer_hit_rate';
COMMENT ON FUNCTION pg_catalog.pg_stat_get_vector_buffer_hit_rate(
    OUT lookups int8,
    OUT hits int8,
    OUT misses int8,
    OUT hit_rate numeric
) IS 'statistics: cumulative shared-hash vector buffer hit rate';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

/* pg_stat_vector_buffer_hit_rate */
CREATE VIEW pg_catalog.pg_stat_vector_buffer_hit_rate AS
    SELECT * FROM pg_catalog.pg_stat_get_vector_buffer_hit_rate();

REVOKE ALL ON pg_catalog.pg_stat_vector_buffer_hit_rate FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_vector_buffer_hit_rate TO PUBLIC;

/* pg_stat_vector_buffer */
CREATE VIEW pg_catalog.pg_stat_vector_buffer AS
    SELECT * FROM pg_catalog.pg_stat_get_vector_buffer();

REVOKE ALL ON pg_catalog.pg_stat_vector_buffer FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_vector_buffer TO PUBLIC;

/* pg_stat_vector_buffer_pool */
CREATE VIEW pg_catalog.pg_stat_vector_buffer_pool AS
    SELECT * FROM pg_catalog.pg_stat_get_vector_buffer_pool();

REVOKE ALL ON pg_catalog.pg_stat_vector_buffer_pool FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_vector_buffer_pool TO PUBLIC;

/* pg_stat_vector_buffer_chunk */
CREATE VIEW pg_catalog.pg_stat_vector_buffer_chunk AS
    SELECT * FROM pg_catalog.pg_stat_get_vector_buffer_chunk();

REVOKE ALL ON pg_catalog.pg_stat_vector_buffer_chunk FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_vector_buffer_chunk TO PUBLIC;

/* pg_stat_vector_buffer_hash_chain */
CREATE VIEW pg_catalog.pg_stat_vector_buffer_hash_chain AS
    SELECT * FROM pg_catalog.pg_stat_get_vector_buffer_hash_chain();

REVOKE ALL ON pg_catalog.pg_stat_vector_buffer_hash_chain FROM PUBLIC;
GRANT SELECT ON pg_catalog.pg_stat_vector_buffer_hash_chain TO PUBLIC;
