DROP FUNCTION IF EXISTS pg_catalog.ss_txnstatus_cache_stat() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8888;
CREATE FUNCTION pg_catalog.ss_txnstatus_cache_stat(
    OUT vcache_gets bigint,
    OUT hcache_gets bigint,
    OUT nio_gets bigint,
    OUT avg_hcache_gettime_us float8,
    OUT avg_nio_gettime_us float8,
    OUT cache_hit_rate float8,
    OUT hcache_eviction bigint,
    OUT avg_eviction_refcnt float8
) 
RETURNS SETOF record 
LANGUAGE internal STABLE NOT FENCED NOT SHIPPABLE ROWS 100 
AS 'ss_txnstatus_cache_stat';
