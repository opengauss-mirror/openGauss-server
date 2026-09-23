/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * vector_buffer_stat.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/vector_buffer_stat.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/datavec/vector_buffer.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

typedef enum VbpPoolStatState {
    VBP_POOL_STAT_FREE = 0,
    VBP_POOL_STAT_ACTIVE = 1,
    VBP_POOL_STAT_DETACHED = 2,
    VBP_POOL_STAT_CORRUPT = 3,
    VBP_POOL_STAT_LEGACY_DESTROYING = 4
} VbpPoolStatState;

typedef enum VbpChunkStatState {
    VBP_CHUNK_STAT_FREE = 0,
    VBP_CHUNK_STAT_ACTIVE = 1,
    VBP_CHUNK_STAT_DRAINING = 2,
    VBP_CHUNK_STAT_CLAIMED = 3
} VbpChunkStatState;

static const char *VbpPoolStateName(uint32 state)
{
    switch (state) {
        case VBP_POOL_STAT_FREE:
            return "FREE";
        case VBP_POOL_STAT_ACTIVE:
            return "ACTIVE";
        case VBP_POOL_STAT_DETACHED:
            return "DETACHED";
        /* Preserve legacy SQL labels for compatibility. */
        case VBP_POOL_STAT_CORRUPT:
            return "DESTROY_QUEUED";
        case VBP_POOL_STAT_LEGACY_DESTROYING:
            return "DESTROYING";
        default:
            return "UNKNOWN";
    }
}

static const char *VbpChunkStateName(uint32 state)
{
    switch (state) {
        case VBP_CHUNK_STAT_FREE:
            return "FREE";
        case VBP_CHUNK_STAT_ACTIVE:
            return "ACTIVE";
        case VBP_CHUNK_STAT_DRAINING:
            return "DRAINING";
        case VBP_CHUNK_STAT_CLAIMED:
            return "CLAIMED";
        default:
            return "UNKNOWN";
    }
}

typedef enum VbpGlobalStatColumn {
    VBP_GLOBAL_COL_CAPACITY_BYTES = 0,
    VBP_GLOBAL_COL_CONFIGURED_CAPACITY_BYTES,
    VBP_GLOBAL_COL_USED_BYTES,
    VBP_GLOBAL_COL_CHUNK_SIZE,
    VBP_GLOBAL_COL_CHUNK_COUNT,
    VBP_GLOBAL_COL_MAX_VBPS,
    VBP_GLOBAL_COL_MIN_PAYLOAD,
    VBP_GLOBAL_COL_ENTRIES,
    VBP_GLOBAL_COL_N_FREE_TOTAL,
    VBP_GLOBAL_COL_ACTIVE_POOLS,
    VBP_GLOBAL_COL_EVICT_PENDING,
    VBP_GLOBAL_COL_INSTALLS,
    VBP_GLOBAL_COL_FALLBACKS,
    VBP_GLOBAL_COL_EVICTIONS,
    VBP_GLOBAL_COL_POOL_FULL_REQUESTS,
    VBP_GLOBAL_COL_RECLAIM_ATTEMPTS,
    VBP_GLOBAL_COL_RECLAIM_VICTIMS,
    VBP_GLOBAL_COL_INVALIDATIONS,
    VBP_GLOBAL_COL_INVALIDATED_ENTRIES,
    VBP_GLOBAL_COL_COUNT
} VbpGlobalStatColumn;

typedef enum VbpHitRateColumn {
    VBP_HIT_RATE_COL_LOOKUPS = 0,
    VBP_HIT_RATE_COL_HITS,
    VBP_HIT_RATE_COL_MISSES,
    VBP_HIT_RATE_COL_HIT_RATE,
    VBP_HIT_RATE_COL_COUNT
} VbpHitRateColumn;

typedef enum VbpPoolStatColumn {
    VBP_POOL_COL_VBP_ID = 0,
    VBP_POOL_COL_GENERATION,
    VBP_POOL_COL_STATE,
    VBP_POOL_COL_SPCNODE,
    VBP_POOL_COL_DBNODE,
    VBP_POOL_COL_RELFILENODE,
    VBP_POOL_COL_PAYLOAD_LEN,
    VBP_POOL_COL_SLOT_SIZE,
    VBP_POOL_COL_LIVE_ENTRIES,
    VBP_POOL_COL_N_FREE_TOTAL,
    VBP_POOL_COL_N_OCCUPIED_TOTAL,
    VBP_POOL_COL_N_CHUNKS_USED,
    VBP_POOL_COL_SCAN_REFS,
    VBP_POOL_COL_EVICT_REQUESTED,
    VBP_POOL_COL_COUNT
} VbpPoolStatColumn;

typedef enum VbpChunkStatColumn {
    VBP_CHUNK_COL_CHUNK_INDEX = 0,
    VBP_CHUNK_COL_VBP_ID,
    VBP_CHUNK_COL_VBP_GENERATION,
    VBP_CHUNK_COL_STATE,
    VBP_CHUNK_COL_N_FREE,
    VBP_CHUNK_COL_N_RESERVED,
    VBP_CHUNK_COL_N_CACHED,
    VBP_CHUNK_COL_N_QUARANTINED,
    VBP_CHUNK_COL_SLOT_COUNT,
    VBP_CHUNK_COL_SLOT_STRIDE,
    VBP_CHUNK_COL_IN_CL,
    VBP_CHUNK_COL_IN_FREELIST,
    VBP_CHUNK_COL_COUNT
} VbpChunkStatColumn;

typedef enum VbpHashChainStatColumn {
    VBP_HASH_CHAIN_COL_VBP_ID = 0,
    VBP_HASH_CHAIN_COL_GENERATION,
    VBP_HASH_CHAIN_COL_LIVE_ENTRIES,
    VBP_HASH_CHAIN_COL_BUCKET_COUNT,
    VBP_HASH_CHAIN_COL_N0,
    VBP_HASH_CHAIN_COL_N1,
    VBP_HASH_CHAIN_COL_N2,
    VBP_HASH_CHAIN_COL_N3,
    VBP_HASH_CHAIN_COL_N_GE4,
    VBP_HASH_CHAIN_COL_MAX_CHAIN,
    VBP_HASH_CHAIN_COL_CHAINED_NODES,
    VBP_HASH_CHAIN_COL_TRUNCATED,
    VBP_HASH_CHAIN_COL_REHASHING,
    VBP_HASH_CHAIN_COL_MIGRATED_BUCKETS,
    VBP_HASH_CHAIN_COL_CANDIDATE_BUCKET_COUNT,
    VBP_HASH_CHAIN_COL_COUNT
} VbpHashChainStatColumn;

static TupleDesc VbpCreateGlobalStatTupleDesc(void)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(VBP_GLOBAL_COL_COUNT, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_CAPACITY_BYTES + 1), "capacity_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_CONFIGURED_CAPACITY_BYTES + 1),
        "configured_capacity_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_USED_BYTES + 1), "used_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_CHUNK_SIZE + 1), "chunk_size", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_CHUNK_COUNT + 1), "chunk_count", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_MAX_VBPS + 1), "max_vbps", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_MIN_PAYLOAD + 1), "min_payload", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_ENTRIES + 1), "entries", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_N_FREE_TOTAL + 1), "n_free_total", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_ACTIVE_POOLS + 1), "active_pools", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_EVICT_PENDING + 1), "evict_pending", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_INSTALLS + 1), "installs", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_FALLBACKS + 1), "fallbacks", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_EVICTIONS + 1), "evictions", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_POOL_FULL_REQUESTS + 1),
        "pool_full_requests", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_RECLAIM_ATTEMPTS + 1),
        "reclaim_attempts", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_RECLAIM_VICTIMS + 1),
        "reclaim_victims", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_INVALIDATIONS + 1), "invalidations", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_GLOBAL_COL_INVALIDATED_ENTRIES + 1),
        "invalidated_entries", INT8OID, -1, 0);

    return BlessTupleDesc(tupdesc);
}
static TupleDesc VbpCreatePoolStatTupleDesc(void)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(VBP_POOL_COL_COUNT, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_VBP_ID + 1), "vbp_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_GENERATION + 1), "generation", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_STATE + 1), "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_SPCNODE + 1), "spcnode", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_DBNODE + 1), "dbnode", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_RELFILENODE + 1), "relfilenode", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_PAYLOAD_LEN + 1), "payload_len", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_SLOT_SIZE + 1), "slot_size", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_LIVE_ENTRIES + 1), "live_entries", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_N_FREE_TOTAL + 1), "n_free_total", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_N_OCCUPIED_TOTAL + 1),
        "n_occupied_total", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_N_CHUNKS_USED + 1), "n_chunks_used", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_SCAN_REFS + 1), "scan_refs", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_POOL_COL_EVICT_REQUESTED + 1), "evict_requested", BOOLOID, -1, 0);

    return BlessTupleDesc(tupdesc);
}
static TupleDesc VbpCreateChunkStatTupleDesc(void)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(VBP_CHUNK_COL_COUNT, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_CHUNK_INDEX + 1), "chunk_index", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_VBP_ID + 1), "vbp_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_VBP_GENERATION + 1), "vbp_generation", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_STATE + 1), "state", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_N_FREE + 1), "n_free", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_N_RESERVED + 1), "n_reserved", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_N_CACHED + 1), "n_cached", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_N_QUARANTINED + 1), "n_quarantined", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_SLOT_COUNT + 1), "slot_count", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_SLOT_STRIDE + 1), "slot_stride", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_IN_CL + 1), "in_cl", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_CHUNK_COL_IN_FREELIST + 1), "in_freelist", BOOLOID, -1, 0);

    return BlessTupleDesc(tupdesc);
}
static TupleDesc VbpCreateHashChainStatTupleDesc(void)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(VBP_HASH_CHAIN_COL_COUNT, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_VBP_ID + 1), "vbp_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_GENERATION + 1), "generation", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_LIVE_ENTRIES + 1), "live_entries", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_BUCKET_COUNT + 1), "bucket_count", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_N0 + 1), "n0", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_N1 + 1), "n1", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_N2 + 1), "n2", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_N3 + 1), "n3", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_N_GE4 + 1), "n_ge4", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_MAX_CHAIN + 1), "max_chain", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_CHAINED_NODES + 1),
        "chained_nodes", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_TRUNCATED + 1), "truncated", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_REHASHING + 1), "rehashing", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_MIGRATED_BUCKETS + 1),
        "migrated_buckets", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HASH_CHAIN_COL_CANDIDATE_BUCKET_COUNT + 1),
        "candidate_bucket_count", INT4OID, -1, 0);

    return BlessTupleDesc(tupdesc);
}
Datum pg_stat_get_vector_buffer(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    VectorBufferGlobalStat *row = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = VbpCreateGlobalStatTupleDesc();
        funcctx->tuple_desc = tupdesc;
        row = (VectorBufferGlobalStat *)palloc0(sizeof(VectorBufferGlobalStat));
        VectorBufferGetGlobalStat(row);
        funcctx->user_fctx = row;
        funcctx->max_calls = 1;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[VBP_GLOBAL_COL_COUNT];
        bool nulls[VBP_GLOBAL_COL_COUNT] = {false};
        HeapTuple tuple;

        row = (VectorBufferGlobalStat *)funcctx->user_fctx;
        values[VBP_GLOBAL_COL_CAPACITY_BYTES] = Int64GetDatum((int64)row->stats.capacityBytes);
        values[VBP_GLOBAL_COL_CONFIGURED_CAPACITY_BYTES] = Int64GetDatum((int64)row->configuredCapacityBytes);
        values[VBP_GLOBAL_COL_USED_BYTES] = Int64GetDatum((int64)row->stats.usedBytes);
        values[VBP_GLOBAL_COL_CHUNK_SIZE] = Int32GetDatum((int32)row->chunkSize);
        values[VBP_GLOBAL_COL_CHUNK_COUNT] = Int32GetDatum((int32)row->chunkCount);
        values[VBP_GLOBAL_COL_MAX_VBPS] = Int32GetDatum((int32)row->maxVbps);
        values[VBP_GLOBAL_COL_MIN_PAYLOAD] = Int32GetDatum((int32)row->minPayload);
        values[VBP_GLOBAL_COL_ENTRIES] = Int32GetDatum((int32)row->stats.entries);
        values[VBP_GLOBAL_COL_N_FREE_TOTAL] = Int32GetDatum((int32)row->stats.nFreeTotal);
        values[VBP_GLOBAL_COL_ACTIVE_POOLS] = Int32GetDatum((int32)row->activePools);
        values[VBP_GLOBAL_COL_EVICT_PENDING] = Int32GetDatum((int32)row->stats.evictPending);
        values[VBP_GLOBAL_COL_INSTALLS] = Int64GetDatum((int64)row->stats.installs);
        values[VBP_GLOBAL_COL_FALLBACKS] = Int64GetDatum((int64)row->stats.fallbacks);
        values[VBP_GLOBAL_COL_EVICTIONS] = Int64GetDatum((int64)row->stats.evictions);
        values[VBP_GLOBAL_COL_POOL_FULL_REQUESTS] = Int64GetDatum((int64)row->stats.poolFullRequests);
        values[VBP_GLOBAL_COL_RECLAIM_ATTEMPTS] = Int64GetDatum((int64)row->stats.reclaimAttempts);
        values[VBP_GLOBAL_COL_RECLAIM_VICTIMS] = Int64GetDatum((int64)row->stats.reclaimVictims);
        values[VBP_GLOBAL_COL_INVALIDATIONS] = Int64GetDatum((int64)row->stats.invalidations);
        values[VBP_GLOBAL_COL_INVALIDATED_ENTRIES] = Int64GetDatum((int64)row->stats.invalidatedEntries);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}
Datum pg_stat_get_vector_buffer_hit_rate(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    VectorBufferStats *row = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = CreateTemplateTupleDesc(VBP_HIT_RATE_COL_COUNT, false, TableAmHeap);
        TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HIT_RATE_COL_LOOKUPS + 1), "lookups", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HIT_RATE_COL_HITS + 1), "hits", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HIT_RATE_COL_MISSES + 1), "misses", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)(VBP_HIT_RATE_COL_HIT_RATE + 1), "hit_rate", NUMERICOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        row = (VectorBufferStats *)palloc0(sizeof(VectorBufferStats));
        VectorBufferGetStats(row);
        funcctx->user_fctx = row;
        funcctx->max_calls = 1;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[VBP_HIT_RATE_COL_COUNT];
        bool nulls[VBP_HIT_RATE_COL_COUNT] = {false};
        uint64 lookups;
        HeapTuple tuple;

        row = (VectorBufferStats *)funcctx->user_fctx;
        lookups = row->hits + row->misses;
        values[VBP_HIT_RATE_COL_LOOKUPS] = Int64GetDatum((int64)lookups);
        values[VBP_HIT_RATE_COL_HITS] = Int64GetDatum((int64)row->hits);
        values[VBP_HIT_RATE_COL_MISSES] = Int64GetDatum((int64)row->misses);
        if (lookups == 0) {
            nulls[VBP_HIT_RATE_COL_HIT_RATE] = true;
            values[VBP_HIT_RATE_COL_HIT_RATE] = (Datum)0;
        } else {
            values[VBP_HIT_RATE_COL_HIT_RATE] = DirectFunctionCall2(numeric_div,
                DirectFunctionCall1(int8_numeric, Int64GetDatum((int64)row->hits)),
                DirectFunctionCall1(int8_numeric, Int64GetDatum((int64)lookups)));
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

Datum pg_stat_get_vector_buffer_pool(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        VectorBufferPoolStat *rows;
        uint32 cap;
        VectorBufferGlobalStat global = {};

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = VbpCreatePoolStatTupleDesc();
        funcctx->tuple_desc = tupdesc;
        VectorBufferGetGlobalStat(&global);
        cap = global.maxVbps == 0 ? 1 : global.maxVbps;
        rows = (VectorBufferPoolStat *)palloc0(sizeof(VectorBufferPoolStat) * cap);
        funcctx->max_calls = VectorBufferCopyPoolStats(rows, cap);
        funcctx->user_fctx = rows;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        VectorBufferPoolStat *rows = (VectorBufferPoolStat *)funcctx->user_fctx;
        VectorBufferPoolStat *row = &rows[funcctx->call_cntr];
        Datum values[VBP_POOL_COL_COUNT];
        bool nulls[VBP_POOL_COL_COUNT] = {false};
        HeapTuple tuple;
        values[VBP_POOL_COL_VBP_ID] = Int32GetDatum((int32)row->vbpId);
        values[VBP_POOL_COL_GENERATION] = Int32GetDatum((int32)row->generation);
        values[VBP_POOL_COL_STATE] = CStringGetTextDatum(VbpPoolStateName(row->state));
        values[VBP_POOL_COL_SPCNODE] = ObjectIdGetDatum(row->spcNode);
        values[VBP_POOL_COL_DBNODE] = ObjectIdGetDatum(row->dbNode);
        values[VBP_POOL_COL_RELFILENODE] = ObjectIdGetDatum(row->relNode);
        values[VBP_POOL_COL_PAYLOAD_LEN] = Int32GetDatum((int32)row->payloadLen);
        values[VBP_POOL_COL_SLOT_SIZE] = Int32GetDatum((int32)row->slotSize);
        values[VBP_POOL_COL_LIVE_ENTRIES] = Int32GetDatum((int32)row->liveEntries);
        values[VBP_POOL_COL_N_FREE_TOTAL] = Int32GetDatum((int32)row->nFreeTotal);
        values[VBP_POOL_COL_N_OCCUPIED_TOTAL] = Int32GetDatum((int32)row->nOccupiedTotal);
        values[VBP_POOL_COL_N_CHUNKS_USED] = Int32GetDatum((int32)row->nChunksUsed);
        values[VBP_POOL_COL_SCAN_REFS] = Int32GetDatum((int32)row->scanRefs);
        values[VBP_POOL_COL_EVICT_REQUESTED] = BoolGetDatum(row->evictRequested);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

Datum pg_stat_get_vector_buffer_chunk(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        VectorBufferChunkStat *rows;
        uint32 cap;
        VectorBufferGlobalStat global = {};

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = VbpCreateChunkStatTupleDesc();
        funcctx->tuple_desc = tupdesc;
        VectorBufferGetGlobalStat(&global);
        cap = global.chunkCount == 0 ? 1 : global.chunkCount;
        rows = (VectorBufferChunkStat *)palloc0(sizeof(VectorBufferChunkStat) * cap);
        funcctx->max_calls = VectorBufferCopyChunkStats(rows, cap);
        funcctx->user_fctx = rows;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        VectorBufferChunkStat *rows = (VectorBufferChunkStat *)funcctx->user_fctx;
        VectorBufferChunkStat *row = &rows[funcctx->call_cntr];
        Datum values[VBP_CHUNK_COL_COUNT];
        bool nulls[VBP_CHUNK_COL_COUNT] = {false};
        HeapTuple tuple;
        values[VBP_CHUNK_COL_CHUNK_INDEX] = Int32GetDatum((int32)row->chunkIndex);
        values[VBP_CHUNK_COL_VBP_ID] = Int32GetDatum((int32)row->vbpId);
        values[VBP_CHUNK_COL_VBP_GENERATION] = Int32GetDatum((int32)row->vbpGeneration);
        values[VBP_CHUNK_COL_STATE] = CStringGetTextDatum(VbpChunkStateName(row->state));
        values[VBP_CHUNK_COL_N_FREE] = Int32GetDatum((int32)row->nFree);
        values[VBP_CHUNK_COL_N_RESERVED] = Int32GetDatum((int32)row->nReserved);
        values[VBP_CHUNK_COL_N_CACHED] = Int32GetDatum((int32)row->nCached);
        values[VBP_CHUNK_COL_N_QUARANTINED] = Int32GetDatum((int32)row->nQuarantined);
        values[VBP_CHUNK_COL_SLOT_COUNT] = Int32GetDatum((int32)row->slotCount);
        values[VBP_CHUNK_COL_SLOT_STRIDE] = Int32GetDatum((int32)row->slotStride);
        values[VBP_CHUNK_COL_IN_CL] = BoolGetDatum(row->inCl);
        values[VBP_CHUNK_COL_IN_FREELIST] = BoolGetDatum(row->inFreelist);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

Datum pg_stat_get_vector_buffer_hash_chain(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        VectorBufferHashChainStat *rows;
        uint32 cap;
        VectorBufferGlobalStat global = {};

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        tupdesc = VbpCreateHashChainStatTupleDesc();
        funcctx->tuple_desc = tupdesc;
        VectorBufferGetGlobalStat(&global);
        cap = global.maxVbps == 0 ? 1 : global.maxVbps;
        rows = (VectorBufferHashChainStat *)palloc0(sizeof(VectorBufferHashChainStat) * cap);
        funcctx->max_calls = VectorBufferCopyHashChainStats(rows, cap);
        funcctx->user_fctx = rows;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        VectorBufferHashChainStat *rows = (VectorBufferHashChainStat *)funcctx->user_fctx;
        VectorBufferHashChainStat *row = &rows[funcctx->call_cntr];
        Datum values[VBP_HASH_CHAIN_COL_COUNT];
        bool nulls[VBP_HASH_CHAIN_COL_COUNT] = {false};
        HeapTuple tuple;
        values[VBP_HASH_CHAIN_COL_VBP_ID] = Int32GetDatum((int32)row->vbpId);
        values[VBP_HASH_CHAIN_COL_GENERATION] = Int32GetDatum((int32)row->generation);
        values[VBP_HASH_CHAIN_COL_LIVE_ENTRIES] = Int32GetDatum((int32)row->liveEntries);
        values[VBP_HASH_CHAIN_COL_BUCKET_COUNT] = Int32GetDatum((int32)row->bucketCount);
        values[VBP_HASH_CHAIN_COL_N0] = Int32GetDatum((int32)row->n0);
        values[VBP_HASH_CHAIN_COL_N1] = Int32GetDatum((int32)row->n1);
        values[VBP_HASH_CHAIN_COL_N2] = Int32GetDatum((int32)row->n2);
        values[VBP_HASH_CHAIN_COL_N3] = Int32GetDatum((int32)row->n3);
        values[VBP_HASH_CHAIN_COL_N_GE4] = Int32GetDatum((int32)row->nGe4);
        values[VBP_HASH_CHAIN_COL_MAX_CHAIN] = Int32GetDatum((int32)row->maxChain);
        values[VBP_HASH_CHAIN_COL_CHAINED_NODES] = Int32GetDatum((int32)row->chainedNodes);
        values[VBP_HASH_CHAIN_COL_TRUNCATED] = Int32GetDatum((int32)row->truncated);
        values[VBP_HASH_CHAIN_COL_REHASHING] = BoolGetDatum(row->rehashing);
        values[VBP_HASH_CHAIN_COL_MIGRATED_BUCKETS] = Int32GetDatum((int32)row->migratedBuckets);
        values[VBP_HASH_CHAIN_COL_CANDIDATE_BUCKET_COUNT] = Int32GetDatum((int32)row->candidateBucketCount);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}
