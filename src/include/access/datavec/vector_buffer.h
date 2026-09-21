/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/include/access/datavec/vector_buffer.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VECTOR_BUFFER_H
#define VECTOR_BUFFER_H

#include "postgres.h"
#include "storage/item/itemptr.h"
#include "storage/smgr/relfilenode.h"
#include "utils/resowner.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef VECTOR_BUFFER_ACCESS_TYPEDEF
#define VECTOR_BUFFER_ACCESS_TYPEDEF
typedef struct VectorBufferAccess VectorBufferAccess;
#endif

#define VECTOR_BUFFER_DEFAULT_CHUNK_SIZE_KB 2048
#define VECTOR_BUFFER_MIN_CHUNK_SIZE_KB 1024
#define VECTOR_BUFFER_DEFAULT_HASH_PARTITIONS 128
#define VECTOR_BUFFER_DEFAULT_MIN_PAYLOAD 128
#define VECTOR_BUFFER_DEFAULT_RECLAIM_SCAN_LIMIT 1024
#define VECTOR_BUFFER_DEFAULT_RECLAIM_BATCH_SIZE 32
#define VECTOR_BUFFER_DEFAULT_RECLAIM_INTERVAL_MS 1000

/*
 * On-disk payload kind. This phase only pins/stores VEC_PAYLOAD_RAW_VECTOR.
 * PQ / RabitQ / OTHER keep their numeric values as reserved format slots.
 */
typedef enum VecPayloadKind {
    VEC_PAYLOAD_RAW_VECTOR = 0,
    VEC_PAYLOAD_PQ_CODE,
    VEC_PAYLOAD_RABITQ_CODE,
    VEC_PAYLOAD_OTHER
} VecPayloadKind;

typedef struct VecPayloadRef {
    RelFileNode rnode;
    ItemPointerData payloadTid; /* payload-page ItemPointer, not the graph tid */
    uint32 payloadLen;
    uint8 kind;
    uint8 reserved[3];
} VecPayloadRef;

typedef struct VectorBufferLoadGuard {
    const char *data;
    uintptr_t opaque[2];
    bool active;
} VectorBufferLoadGuard;

typedef bool (*VectorBufferLoadBeginFn)(const ItemPointerData *payloadTid, uint32 payloadLen,
    void *loaderCtx, VectorBufferLoadGuard *guard);
/* On abort, LWLockReleaseAll has already released the loader's content lock. */
typedef void (*VectorBufferLoadEndFn)(VectorBufferLoadGuard *guard, bool isCommit);

typedef struct VectorBufferLoadOps {
    VectorBufferLoadBeginFn begin;
    VectorBufferLoadEndFn end;
} VectorBufferLoadOps;

typedef struct VectorBufferHandle {
    const char *data;
    uint32 len;
    uint64 pinCookie;
    bool cached;
    VectorBufferAccess *access;
} VectorBufferHandle;

typedef enum VectorBufferInvalidateResult {
    VECTOR_BUFFER_INVALIDATE_NOT_FOUND = 0,
    VECTOR_BUFFER_INVALIDATE_DONE,
    VECTOR_BUFFER_INVALIDATE_DEFERRED
} VectorBufferInvalidateResult;

typedef struct VectorBufferStats {
    uint64 hits;
    uint64 misses;
    uint64 installs;
    uint64 fallbacks;
    uint64 invalidations;
    uint64 invalidatedEntries;
    uint64 evictions;
    uint64 poolFullRequests;
    uint64 reclaimAttempts;
    uint64 reclaimVictims;
    uint64 usedBytes;
    uint64 capacityBytes;
    uint32 entries;
    uint32 nFreeTotal;
    uint32 evictPending;
} VectorBufferStats;

typedef struct VectorBufferGlobalStat {
    VectorBufferStats stats;
    uint64 configuredCapacityBytes;
    uint32 chunkSize;
    uint32 chunkCount;
    uint32 maxVbps;
    uint32 minPayload;
    uint32 activePools;
} VectorBufferGlobalStat;

typedef struct VectorBufferPoolStat {
    uint32 vbpId;
    uint32 generation;
    uint32 state;
    Oid spcNode;
    Oid dbNode;
    Oid relNode;
    uint32 payloadLen;
    uint32 slotSize;
    uint32 liveEntries;
    uint32 nFreeTotal;
    uint32 nOccupiedTotal;
    uint32 nChunksUsed;
    uint32 scanRefs;
    bool evictRequested;
} VectorBufferPoolStat;

typedef struct VectorBufferChunkStat {
    uint32 chunkIndex;
    uint32 vbpId;
    uint32 vbpGeneration;
    uint32 state;
    uint32 nFree;
    uint32 nReserved;
    uint32 nCached;
    uint32 nQuarantined;
    uint32 slotCount;
    uint32 slotStride;
    bool inCl;
    bool inFreelist;
} VectorBufferChunkStat;

/* Unlocked snapshot of hash-chain lengths; nK = buckets whose chain has exactly K nodes. */
typedef struct VectorBufferHashChainStat {
    uint32 vbpId;
    uint32 generation;
    uint32 liveEntries;
    uint32 bucketCount;
    uint32 n0;
    uint32 n1;
    uint32 n2;
    uint32 n3;
    uint32 nGe4;
    uint32 maxChain;
    uint32 chainedNodes;
    uint32 truncated;
    bool rehashing;
    uint32 migratedBuckets;
    uint32 candidateBucketCount;
} VectorBufferHashChainStat;

extern Size VectorBufferShmemSize(void);
extern void VectorBufferShmemInit(void);
/* True once shared memory is attached and usable; cheap enough for redo-path gating. */
extern bool VectorBufferIsActive(void);
/* Dedicated reclaim util-thread entry (postmaster VBP_RECLAIM). */
extern void VectorBufferReclaimMain(void);
extern bool VectorBufferBeginAccess(const RelFileNode *rnode, uint32 payloadLen, VectorBufferAccess **access);
extern bool VectorBufferAccessIsIdle(const VectorBufferAccess *access);
extern void VectorBufferEndAccess(VectorBufferAccess **access);
extern void VectorBufferReleaseOwnerAccess(VectorBufferAccess *access, bool isCommit);
/* The caller must remove access from its ResourceOwner array before calling this cleanup entry. */
extern void VectorBufferReleaseOwnerAccessNoForget(VectorBufferAccess *access, bool isCommit);
extern void VectorBufferReassignOwnerAccess(VectorBufferAccess *access, ResourceOwner owner);
extern bool VectorBufferPinFast(VectorBufferAccess *access, const ItemPointerData *payloadTid,
    const VectorBufferLoadOps *loadOps, void *loaderCtx, VectorBufferHandle *handle);
extern void VectorBufferRelease(VectorBufferHandle *handle);
extern VectorBufferInvalidateResult VectorBufferInvalidatePayload(
    const RelFileNode *rnode, const ItemPointerData *payloadTid);
extern void VectorBufferInvalidateRelation(const RelFileNode *rnode);
extern void VectorBufferGetStats(VectorBufferStats *stats);
extern void VectorBufferGetGlobalStat(VectorBufferGlobalStat *stats);
extern uint32 VectorBufferCopyPoolStats(VectorBufferPoolStat *out, uint32 capacity);
extern uint32 VectorBufferCopyChunkStats(VectorBufferChunkStat *out, uint32 capacity);
extern uint32 VectorBufferCopyHashChainStats(VectorBufferHashChainStat *out, uint32 capacity);

#ifdef __cplusplus
}
#endif

#endif /* VECTOR_BUFFER_H */
