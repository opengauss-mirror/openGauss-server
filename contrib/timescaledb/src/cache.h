/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CACHE_H
#define TIMESCALEDB_CACHE_H

#include <postgres.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>

#include "tsdb_head.h"
#include "export.h"
#include "commands/extension.h"


typedef enum TelemetryLevel
{
	TELEMETRY_OFF,
	TELEMETRY_BASIC,
} TelemetryLevel;

typedef enum CacheQueryFlags
{
	CACHE_FLAG_NONE = 0,
	CACHE_FLAG_MISSING_OK = 1 << 0,
	CACHE_FLAG_NOCREATE = 1 << 1,
} CacheQueryFlags;

enum ExtensionState
{
    /*
     * NOT_INSTALLED means that this backend knows that the extension is not
     * present.  In this state we know that the proxy table is not present.
     * Thus, the only way to get out of this state is a RelCacheInvalidation
     * indicating that the proxy table was added. This is the state returned
     * during DROP EXTENSION.
     */
    EXTENSION_STATE_NOT_INSTALLED,

    /*
     * UNKNOWN state is used only if we cannot be sure what the state is. This
     * can happen in two cases: 1) at the start of a backend or 2) We got a
     * relcache event outside of a transaction and thus could not check the
     * cache for the presence/absence of the proxy table or extension.
     */
    EXTENSION_STATE_UNKNOWN,

    /*
     * TRANSITIONING only occurs in the middle of a CREATE EXTENSION or ALTER
     * EXTENSION UPDATE
     */
    EXTENSION_STATE_TRANSITIONING,

    /*
     * CREATED means we know the extension is loaded, metadata is up-to-date,
     * and we therefore do not need a full check until a RelCacheInvalidation
     * on the proxy table.
     */
    EXTENSION_STATE_CREATED,
};

#define CACHE_FLAG_CHECK (CACHE_FLAG_MISSING_OK | CACHE_FLAG_NOCREATE)

typedef struct CacheQuery
{
	/* CacheQueryFlags as defined above */
	const unsigned int flags;
	void *result;
	void *data;
} CacheQuery;

typedef struct CacheStats
{
	long numelements;
	uint64 hits;
	uint64 misses;
} CacheStats;

typedef struct Cache
{
	HASHCTL hctl;
	HTAB *htab;
	int refcount;
	const char *name;
	long numelements;
	int flags;
	CacheStats stats;
	void *(*get_key)(struct CacheQuery *);
	void *(*create_entry)(struct Cache *, CacheQuery *);
	void *(*update_entry)(struct Cache *, CacheQuery *);
	void (*missing_error)(const struct Cache *, const CacheQuery *);
	bool (*valid_result)(const void *);
	void (*pre_destroy_hook)(struct Cache *);
	bool release_on_commit; /* This should be false if doing
							 * cross-commit operations like CLUSTER or
							 * VACUUM */
} Cache;

typedef struct tsdb_session_context {
    List *tsdb_pinned_caches;
    MemoryContext tsdb_pinned_caches_mctx;
	Cache *tsdb_hypertable_cache_current;
	List *tsdb_planner_hcaches;
	const char *tsdb_TS_CTE_EXPAND;
	ExtensiblePathMethods tsdb_constraint_aware_append_path_methods;
	ExtensiblePlanMethods tsdb_constraint_aware_append_plan_methods;
	ExtensibleExecMethods tsdb_constraint_aware_append_state_methods;
	ExtensiblePlanMethods tsdb_chunk_append_plan_methods;
	bool tsdb_expect_chunk_modification;

	
	struct config_enum_entry tsdb_telemetry_level_options[3];
	TelemetryLevel tsdb_on_level;
	bool tsdb_first_start;

	char *tsdb_ts_guc_license_key;
	bool tsdb_loaded;
	bool tsdb_loader_present;

	char tsdb_base64[65];

	int64 tsdb_fixed_memory_cache_size;
	FmgrInfo tsdb_ddl_commands_fmgrinfo;
	FmgrInfo tsdb_dropped_objects_fmgrinfo;

	Oid tsdb_extension_proxy_oid;
	HTAB *tsdb_func_hash;
	bool tsdb_downgrade_to_apache_enabled;
	void *tsdb_tsl_handle;
	PGFunction tsdb_tsl_validate_license_fn;
	PGFunction tsdb_tsl_startup_fn;
	bool tsdb_can_load;
	GucSource tsdb_load_source;

	bool tsdb_dsm_init_done;

	dsm_handle tsdb_dsm_control_handle;
	
	Size tsdb_dsm_control_mapped_size;
	void *tsdb_dsm_control_impl_private;
	int	tsdb_NamedLWLockTrancheRequestsAllocated;
	bool tsdb_lock_named_request_allowed;
	BackgroundWorkerArray *tsdb_BackgroundWorkerData;

	bool tsdb_ts_guc_disable_optimizations;
	bool tsdb_ts_guc_optimize_non_hypertables;
	bool tsdb_ts_guc_restoring;
	bool tsdb_ts_guc_constraint_aware_append;
	bool tsdb_ts_guc_enable_ordered_append;
	bool tsdb_ts_guc_enable_chunk_append;
	bool tsdb_ts_guc_enable_parallel_chunk_append;
	bool tsdb_ts_guc_enable_runtime_exclusion;
	bool tsdb_ts_guc_enable_constraint_exclusion;
	bool tsdb_ts_guc_enable_cagg_reorder_groupby;
	bool tsdb_ts_guc_enable_transparent_decompression;
	int tsdb_ts_guc_max_open_chunks_per_insert;
	int tsdb_ts_guc_max_cached_chunks_per_hypertable;
	int tsdb_ts_guc_telemetry_level;

	char *tsdb_ts_last_tune_time;
	char *tsdb_ts_last_tune_version;
	char *tsdb_ts_telemetry_cloud;
    enum ExtensionState tsdb_global_extstate;
	#ifdef TS_DEBUG
	bool tsdb_ts_shutdown_bgw;
	char *tsdb_ts_current_timestamp_mock;
	#endif
} tsdb_session_context; 


extern void ts_cache_init(Cache *cache);
extern void ts_cache_invalidate(Cache *cache);
extern void *ts_cache_fetch(Cache *cache, CacheQuery *query);
extern bool ts_cache_remove(Cache *cache, void *key);

extern MemoryContext ts_cache_memory_ctx(Cache *cache);

extern Cache *ts_cache_pin(Cache *cache);
extern TSDLLEXPORT int ts_cache_release(Cache *cache);



extern void _cache_init(void);
extern void _cache_fini(void);



#endif /* TIMESCALEDB_CACHE_H */
