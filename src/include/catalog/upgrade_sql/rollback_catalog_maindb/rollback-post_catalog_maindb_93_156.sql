/* pg_stat_vector_buffer_hit_rate */
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_hit_rate CASCADE;
/* pg_stat_vector_buffer_hash_chain */
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_hash_chain CASCADE;
/* pg_stat_vector_buffer_chunk */
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_chunk CASCADE;
/* pg_stat_vector_buffer_pool */
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer_pool CASCADE;
/* pg_stat_vector_buffer */
DROP VIEW IF EXISTS pg_catalog.pg_stat_vector_buffer CASCADE;

/* pg_stat_get_vector_buffer_hit_rate */
DROP FUNCTION IF EXISTS pg_catalog.pg_stat_get_vector_buffer_hit_rate(
    OUT lookups int8,
    OUT hits int8,
    OUT misses int8,
    OUT hit_rate numeric
) CASCADE;

/* pg_stat_get_vector_buffer_hash_chain */
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

/* pg_stat_get_vector_buffer_chunk */
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

/* pg_stat_get_vector_buffer_pool */
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

/* pg_stat_get_vector_buffer */
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
