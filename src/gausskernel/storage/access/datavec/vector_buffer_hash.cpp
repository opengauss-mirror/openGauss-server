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
 *        src/gausskernel/storage/access/datavec/vector_buffer_hash.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "utils/atomic.h"
#include "vector_buffer_internal.h"

#define VBP_HASH_CHAIN_WALK_LIMIT 64U
#define VBP_HASH_CHAIN_INTERRUPT_MASK 8191U
/*
 * Hit-path rehash assist stride.
 * sc2 c=20 ≈ 1e5 Pin/s (QPS ~700 × ef_search 200). Worst remaining work is
 * one old table (524288 buckets). assists/s = 1e5 / STRIDE; 524288 / (1e5/8)
 * ≈ 42s, inside the 60s Hit-only budget. Steady Hit pays one local decrement
 * per Pin and a hashControl read only every STRIDE-th Pin.
 */
#define VBP_MIGRATE_ASSIST_STRIDE 8U

static VbpHashRuntime *g_vbpHashRuntime = NULL;
static THR_LOCAL uint64 g_vbpPinCookie = 0;


/* Atomic load/store for bucket heads and hashNext (Step B lock-free hit). */
static inline VbpEntryRef VbpHashRefLoad(const volatile VbpEntryRef *slot)
{
    return (VbpEntryRef)pg_atomic_barrier_read_u64((volatile uint64 *)slot);
}

static inline void VbpHashRefStore(volatile VbpEntryRef *slot, VbpEntryRef value)
{
    pg_atomic_write_u64((volatile uint64 *)slot, (uint64)value);
}
static THR_LOCAL uint32 g_vbpMigrateAssistTick = 1;

#define VBP_HASH_CHAIN_TWO 2U
#define VBP_HASH_CHAIN_THREE 3U

typedef enum VbpHashFindResult {
    VBP_HASH_NOT_FOUND = 0,
    VBP_HASH_FOUND,
    VBP_HASH_CORRUPT
} VbpHashFindResult;

typedef struct VbpHashFindMatch {
    VbpEntryRef ref;
    VbpEntry *entry;
    VbpEntry *previous;
} VbpHashFindMatch;

typedef struct VbpHashEvictResult {
    uint32 entryChecks;
    VbpEntryRef evicted;
    uint32 incarnation;
} VbpHashEvictResult;

typedef struct VbpHashMigrationContext {
    VbpHashRuntime *runtime;
    VbpInstance *vbp;
    const VbpHashView *oldView;
    const VbpHashView *candidate;
    VbpEntryRef *oldHead;
    VbpEntryRef *lowHead;
    VbpEntryRef *highHead;
    VbpEntryRef low;
    VbpEntryRef high;
    uint64 control;
    uint32 oldBucket;
} VbpHashMigrationContext;

typedef enum VbpHashPinAttemptResult {
    VBP_HASH_PIN_MISS = 0,
    VBP_HASH_PIN_RETRY,
    VBP_HASH_PIN_HIT
} VbpHashPinAttemptResult;

typedef struct VbpEntrySnapshotRequest {
    const VbpHashRuntime *runtime;
    const VbpInstance *vbp;
    uint32 slotStride;
    uint32 slotsPerChunk;
    uint32 vbpGeneration;
} VbpEntrySnapshotRequest;

typedef struct VbpHashPinRequest {
    VbpInstance *vbp;
    const ItemPointerData *tid;
    VectorBufferAccess *access;
    VbpEntryRef *entryRef;
    const char **payload;
    uint32 hash;
    uint32 slotStride;
    uint32 slotsPerChunk;
    uint32 vbpGeneration;
} VbpHashPinRequest;

typedef struct VbpHashPublishContext {
    VbpInstance *vbp;
    const ItemPointerData *tid;
    VectorBufferAccess *access;
    VbpHashPublishResult *result;
    VbpEntry *reservedEntry;
    VbpHashPartition *partition;
    VbpEntryRef *bucketHead;
    VbpEntryRef reserved;
    uint64 control;
    uint32 reservedIncarnation;
    uint32 hash;
} VbpHashPublishContext;

typedef enum VbpHashDrainStepResult {
    VBP_HASH_DRAIN_ERROR = 0,
    VBP_HASH_DRAIN_RESTART,
    VBP_HASH_DRAIN_REMOVED,
    VBP_HASH_DRAIN_EMPTY
} VbpHashDrainStepResult;

typedef struct VbpHashDrainState {
    VbpHashRuntime *runtime;
    VbpInstance *vbp;
    uint64 control;
    Size removedCount;
    Size removalLimit;
    uint32 generation;
} VbpHashDrainState;

typedef struct VbpHashDrainResult {
    VbpEntryRef removed;
    uint32 incarnation;
    bool bucketEmpty;
    bool recycleOwned;
} VbpHashDrainResult;

typedef struct VbpHashEvictBucketContext {
    VbpHashRuntime *runtime;
    VbpInstance *vbp;
    const VbpHashView *oldView;
    VbpHashEvictResult *result;
    uint64 control;
    uint32 oldBucket;
    uint32 maxEntryChecks;
} VbpHashEvictBucketContext;

typedef enum VbpHashEvictProbeResult {
    VBP_HASH_EVICT_PROBE_ERROR = 0,
    VBP_HASH_EVICT_PROBE_MISS,
    VBP_HASH_EVICT_PROBE_HIT
} VbpHashEvictProbeResult;

static inline uint64 VbpHashControlRead(const VbpInstance *vbp)
{
    return pg_atomic_barrier_read_u64((volatile uint64 *)&vbp->hashControl);
}

static inline bool VbpHashControlIsRehashing(uint64 control)
{
    return (uint32)(control >> VBP_HASH_CONTROL_CANDIDATE_SHIFT) != 0;
}

static uint64 VbpHashPackControl(uint32 activeDesc, uint32 candidateDesc)
{
    if (activeDesc == VBP_INVALID_INDEX) {
        return 0;
    }
    return (uint64)(activeDesc + 1) |
        ((candidateDesc == VBP_INVALID_INDEX ? 0 : (uint64)(candidateDesc + 1)) <<
            VBP_HASH_CONTROL_CANDIDATE_SHIFT);
}

static bool VbpHashRuntimeIsValid(const VbpHashRuntime *runtime)
{
    return runtime != NULL && runtime->partitionArena != NULL && runtime->partitionCapacity != 0 &&
           TYPEALIGN(alignof(VbpHashPartition), (uintptr_t)runtime->partitionArena) ==
               (uintptr_t)runtime->partitionArena &&
           runtime->viewDescArena != NULL && runtime->viewDescCapacity != 0 &&
           TYPEALIGN(alignof(VbpHashViewDesc), (uintptr_t)runtime->viewDescArena) ==
               (uintptr_t)runtime->viewDescArena &&
           runtime->chunkArena != NULL && runtime->chunkCount != 0 &&
           runtime->chunkSize >= sizeof(VbpEntry) && runtime->entryArena != NULL &&
           TYPEALIGN(alignof(VbpMemChunk), (uintptr_t)runtime->chunkArena) ==
               (uintptr_t)runtime->chunkArena &&
           runtime->entryArenaSize == (Size)runtime->chunkCount * runtime->chunkSize &&
           TYPEALIGN(alignof(VbpEntry), (uintptr_t)runtime->entryArena) ==
               (uintptr_t)runtime->entryArena;
}

uint64 VbpNextPinCookie(void)
{
    g_vbpPinCookie++;
    if (g_vbpPinCookie == 0) {
        g_vbpPinCookie = 1;
    }
    return g_vbpPinCookie;
}

static inline bool VbpHashViewBucketsAreValid(const VbpHashView *view)
{
    return view != NULL && view->buckets != NULL &&
           PointerIsAligned(view->buckets, VbpEntryRef) && view->bucketCount != 0 &&
           (view->bucketCount & (view->bucketCount - 1)) == 0;
}

static inline bool VbpHashMigrationBitmapIsValid(const VbpHashView *view)
{
    return view != NULL && view->migratedBitmap != NULL &&
        PointerIsAligned(view->migratedBitmap, uint64);
}

static inline VbpHashViewDesc *VbpHashResolveViewDesc(const VbpHashRuntime *runtime, uint32 descIndex)
{
    VbpHashViewDesc *desc;

    if (runtime == NULL || descIndex == VBP_INVALID_INDEX || descIndex >= runtime->viewDescCapacity) {
        return NULL;
    }
    desc = &runtime->viewDescArena[descIndex];
    return desc->view.buckets == NULL ? NULL : desc;
}

static inline VbpHashViewDesc *VbpHashPublishedViewDesc(const VbpHashRuntime *runtime,
    uint32 descIndex)
{
    Assert(runtime != NULL);
    Assert(descIndex != VBP_INVALID_INDEX && descIndex < runtime->viewDescCapacity);
    VbpHashViewDesc *desc = &runtime->viewDescArena[descIndex];

    Assert(desc->view.buckets != NULL);
    return desc;
}

static bool VbpHashControlIsValid(const VbpHashRuntime *runtime, uint64 control)
{
    uint32 activeDesc;
    uint32 candidateDesc;

    if (runtime == NULL || control == 0) {
        return false;
    }
    activeDesc = VbpHashControlActiveDesc(control);
    candidateDesc = VbpHashControlCandidateDesc(control);
    if (VbpHashResolveViewDesc(runtime, activeDesc) == NULL) {
        return false;
    }
    return candidateDesc == VBP_INVALID_INDEX ||
        (candidateDesc != activeDesc && VbpHashResolveViewDesc(runtime, candidateDesc) != NULL);
}

static bool VbpHashInstanceLayoutIsValid(const VbpHashRuntime *runtime, const VbpInstance *vbp,
    uint64 control)
{
    VbpHashViewDesc *activeDesc;
    const VbpHashView *active;

    if (runtime == NULL || vbp == NULL || vbp->partitionCount == 0 ||
        (vbp->partitionCount & (vbp->partitionCount - 1)) != 0 ||
        vbp->partitionBase > runtime->partitionCapacity ||
        vbp->partitionCount > runtime->partitionCapacity - vbp->partitionBase ||
        !VbpHashControlIsValid(runtime, control)) {
        return false;
    }

    activeDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    active = &activeDesc->view;
    if (!VbpHashViewBucketsAreValid(active) || vbp->partitionCount > active->bucketCount) {
        return false;
    }

    if (VbpHashControlIsRehashing(control)) {
        VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime,
            VbpHashControlCandidateDesc(control));
        const VbpHashView *candidate = &candidateDesc->view;

        if (active->bucketCount > PG_UINT32_MAX / VBP_HASH_GROWTH_FACTOR ||
            candidate->bucketCount != active->bucketCount * VBP_HASH_GROWTH_FACTOR ||
            !VbpHashViewBucketsAreValid(candidate) ||
            !VbpHashMigrationBitmapIsValid(candidate)) {
            return false;
        }
    }
    return true;
}

static VbpEntry *VbpResolveEntrySnapshot(const VbpEntrySnapshotRequest *request, VbpEntryRef ref,
    VbpMemChunk **resolvedChunk, uint64 *resolvedEpoch)
{
    uint32 encodedChunk = (uint32)(ref >> VBP_PACKED_WORD_SHIFT);
    uint32 encodedSlot = (uint32)ref;
    uint32 chunkIndex;
    uint32 slotIndex;
    VbpMemChunk *chunk;
    uint32 control;
    uint64 publicationEpoch;
    Size offset;

    if (resolvedChunk != NULL) {
        *resolvedChunk = NULL;
    }
    if (resolvedEpoch != NULL) {
        *resolvedEpoch = 0;
    }
    if (request == NULL || request->runtime == NULL || request->vbp == NULL ||
        resolvedChunk == NULL || resolvedEpoch == NULL ||
        encodedChunk == 0 || encodedSlot == 0) {
        return NULL;
    }
    chunkIndex = encodedChunk - 1;
    slotIndex = encodedSlot - 1;
    if (chunkIndex >= request->runtime->chunkCount || slotIndex >= request->slotsPerChunk) {
        return NULL;
    }

    chunk = &request->runtime->chunkArena[chunkIndex];
    control = VbpChunkControlRead(chunk);
    publicationEpoch = VbpChunkPublicationEpochRead(chunk);
    if (((control & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_ACTIVE &&
            (control & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_DRAINING) ||
        chunk->vbpId != request->vbp->id || chunk->vbpGeneration != request->vbpGeneration) {
        return NULL;
    }

    Assert(chunk->entryBase == (uint64)chunkIndex * request->runtime->chunkSize);
    offset = (Size)chunkIndex * request->runtime->chunkSize + (Size)slotIndex * request->slotStride;
    Assert(offset <= request->runtime->entryArenaSize &&
        request->runtime->entryArenaSize - offset >= sizeof(VbpEntry));

    *resolvedChunk = chunk;
    *resolvedEpoch = publicationEpoch;
    return (VbpEntry *)(request->runtime->entryArena + offset);
}

static inline bool VbpEntrySnapshotIsValid(const VbpInstance *vbp,
    const VbpMemChunk *chunk, uint64 publicationEpoch, uint32 vbpGeneration)
{
    uint64 finalEpoch = VbpChunkPublicationEpochRead(chunk);
    uint32 finalControl = VbpChunkControlRead(chunk);
    if (publicationEpoch != finalEpoch ||
        ((finalControl & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_ACTIVE &&
            (finalControl & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_DRAINING) ||
        chunk->vbpId != vbp->id || chunk->vbpGeneration != vbpGeneration) {
        return false;
    }
    return true;
}

VbpEntry *VbpResolveEntry(VbpInstance *vbp, VbpEntryRef ref, VbpMemChunk **resolvedChunk)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpMemChunk *chunk;
    VbpEntry *entry;
    uint64 publicationEpoch;
    uint32 slotStride;
    uint32 slotsPerChunk;
    uint32 vbpGeneration;

    if (resolvedChunk != NULL) {
        *resolvedChunk = NULL;
    }
    if (runtime == NULL || vbp == NULL) {
        return NULL;
    }
    vbpGeneration = VbpInstanceGeneration(vbp);
    slotStride = vbp->payloadLayout.slotSize;
    if (slotStride < sizeof(VbpEntry) || slotStride > runtime->chunkSize ||
        slotStride % alignof(VbpEntry) != 0) {
        return NULL;
    }
    slotsPerChunk = vbp->payloadLayout.slotsPerChunk;
    VbpEntrySnapshotRequest request = {runtime, vbp, slotStride, slotsPerChunk, vbpGeneration};
    entry = VbpResolveEntrySnapshot(&request, ref, &chunk, &publicationEpoch);
    if (entry == NULL || !VbpEntrySnapshotIsValid(vbp, chunk, publicationEpoch, vbpGeneration)) {
        return NULL;
    }
    if (resolvedChunk != NULL) {
        *resolvedChunk = chunk;
    }
    return entry;
}

static Size VbpHashTraversalLimit(const VbpHashRuntime *runtime)
{
    Size limit = runtime->entryArenaSize / sizeof(VbpEntry);

    return limit == (Size)-1 ? limit : limit + 1;
}

static VbpHashFindResult VbpHashFindEntry(VbpInstance *vbp, VbpEntryRef head, const ItemPointerData *tid,
    VbpHashFindMatch *match)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashFindMatch emptyMatch = {VBP_INVALID_ENTRY_REF, NULL, NULL};
    VbpEntry *previous = NULL;
    VbpEntryRef current = head;
    Size visited = 0;

    if (runtime == NULL || match == NULL) {
        return VBP_HASH_CORRUPT;
    }
    *match = emptyMatch;
    Size limit = VbpHashTraversalLimit(runtime);

    while (VbpEntryRefIsValid(current)) {
        VbpEntry *entry;
        uint64 entryControl;
        VbpEntryRef next;

        if (visited++ >= limit) {
            return VBP_HASH_CORRUPT;
        }
        entry = VbpResolveEntry(vbp, current, NULL);
        if (entry == NULL) {
            return VBP_HASH_CORRUPT;
        }
        entryControl = pg_atomic_barrier_read_u64(&entry->control);
        next = VbpHashRefLoad(&entry->hashNext);
        if (!VbpEntryControlCanPin(entryControl) ||
            VbpEntryControlIncarnation(entryControl) == 0) {
            return VBP_HASH_CORRUPT;
        }
        if (ItemPointerEqualsNoCheck((ItemPointer)&entry->payloadTid, (ItemPointer)tid)) {
            match->ref = current;
            match->entry = entry;
            match->previous = previous;
            return VBP_HASH_FOUND;
        }
        previous = entry;
        current = next;
    }

    if (current != VBP_INVALID_ENTRY_REF) {
        return VBP_HASH_CORRUPT;
    }
    return VBP_HASH_NOT_FOUND;
}

static bool VbpHashTryPinEntryFromControl(VbpEntry *entry, uint64 raw, uint32 *incarnation)
{
    uint32 entryIncarnation;

    if (entry == NULL || incarnation == NULL) {
        return false;
    }
    entryIncarnation = VbpEntryControlIncarnation(raw);
    if (entryIncarnation == 0) {
        return false;
    }

    while (VbpEntryControlIncarnation(raw) == entryIncarnation &&
        VbpEntryControlCanPin(raw) && VbpEntryControlReaders(raw) != VBP_ENTRY_READER_MASK) {
        uint32 readers = VbpEntryControlReaders(raw);
        uint64 expected;
        uint64 desired;

        expected = raw;
        desired = VbpEntryControlWithUsed(
            VbpEntryControlPack(entryIncarnation, VBP_ENTRY_CACHED, readers + 1));
        if (pg_atomic_compare_exchange_u64(&entry->control, &expected, desired)) {
            *incarnation = entryIncarnation;
            return true;
        }
        raw = expected;
    }
    return false;
}

static bool VbpHashTryPinEntry(VbpEntry *entry, uint32 *incarnation)
{
    if (entry == NULL) {
        return false;
    }
    return VbpHashTryPinEntryFromControl(entry, pg_atomic_read_u64(&entry->control), incarnation);
}

/*
 * Drop a pin when the VBP becomes inactive after the entry CAS.
 * Must not go through VbpReleaseActivePin: the VBP generation can already
 * disagree with access, and that path would clear locals while leaking readers.
 */
static void VbpHashUndoPinEntry(VbpInstance *vbp, VbpEntryRef ref, VbpEntry *entry, uint32 incarnation)
{
    uint64 control;

    if (entry == NULL || incarnation == 0) {
        return;
    }
    control = pg_atomic_read_u64(&entry->control);
    while (VbpEntryControlIncarnation(control) == incarnation) {
        VbpEntryState state;
        uint32 readers;
        uint64 expected;
        uint64 desired;

        state = VbpEntryControlState(control);
        readers = VbpEntryControlReaders(control);
        if (readers == 0 || (state != VBP_ENTRY_CACHED && state != VBP_ENTRY_CLOSED)) {
            return;
        }
        desired = VbpEntryControlPack(incarnation, state, readers - 1);
        if (VbpEntryControlUsed(control) &&
            (state == VBP_ENTRY_CACHED || readers > 1)) {
            desired = VbpEntryControlWithUsed(desired);
        }
        expected = control;
        if (pg_atomic_compare_exchange_u64(&entry->control, &expected, desired)) {
            if (state == VBP_ENTRY_CLOSED && readers == 1) {
                VbpRecycleEntryOwned(vbp, ref, incarnation, NULL);
            }
            return;
        }
        control = expected;
    }
}

static bool VbpHashBitmapIsMigrated(const VbpHashView *candidate, uint32 oldBucketCount,
    uint32 oldBucket, bool *migrated)
{
    uint32 wordIndex;
    uint32 bitIndex;
    uint64 word;

    if (migrated == NULL || oldBucket >= oldBucketCount ||
        !VbpHashMigrationBitmapIsValid(candidate)) {
        return false;
    }
    wordIndex = oldBucket / VBP_HASH_BITMAP_BITS;
    bitIndex = oldBucket % VBP_HASH_BITMAP_BITS;
    word = pg_atomic_barrier_read_u64(&candidate->migratedBitmap[wordIndex]);
    *migrated = (word & (UINT64CONST(1) << bitIndex)) != 0;
    return true;
}

static bool VbpHashPartitionForControl(const VbpHashRuntime *runtime, VbpInstance *vbp, uint64 control,
    uint32 hash, VbpHashPartition **partition)
{
    VbpHashViewDesc *activeDesc;
    const VbpHashView *active;
    uint32 bucket;
    uint32 relativePartition;

    if (partition == NULL || !VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
        return false;
    }
    activeDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    if (activeDesc == NULL) {
        return false;
    }
    active = &activeDesc->view;
    bucket = hash & (active->bucketCount - 1);
    relativePartition = VbpHashPartitionForBucket(bucket, vbp->partitionCount);
    if (relativePartition == VBP_INVALID_INDEX) {
        return false;
    }
    *partition = &runtime->partitionArena[vbp->partitionBase + relativePartition];
    return true;
}

static bool VbpHashLockPartition(VbpInstance *vbp, uint32 generation, uint32 hash,
    uint64 *lockedControl, VbpHashPartition **lockedPartition)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint64 control;
    VbpHashPartition *partition;
    VbpHashPartition *latestPartition;

    if (runtime == NULL || lockedControl == NULL || lockedPartition == NULL ||
        !VbpInstanceLifecycleMatches(vbp, generation, VBP_ACTIVE)) {
        return false;
    }
    control = VbpHashControlRead(vbp);
    if (!VbpHashPartitionForControl(runtime, vbp, control, hash, &partition)) {
        return false;
    }

    SpinLockAcquire(&partition->lock);
    if (!VbpInstanceLifecycleMatches(vbp, generation, VBP_ACTIVE)) {
        SpinLockRelease(&partition->lock);
        return false;
    }
    control = VbpHashControlRead(vbp);
    if (!VbpHashPartitionForControl(runtime, vbp, control, hash, &latestPartition)) {
        SpinLockRelease(&partition->lock);
        return false;
    }
    if (latestPartition != partition) {
        SpinLockRelease(&partition->lock);
        return false;
    }

    *lockedControl = control;
    *lockedPartition = partition;
    return true;
}

static inline bool VbpHashStateAllowsPayloadInvalidation(uint32 state)
{
    return state == VBP_ACTIVE || state == VBP_DETACHED;
}

static bool VbpHashLockPayloadInvalidationPartition(VbpInstance *vbp, uint32 generation,
    uint32 hash, uint64 *lockedControl, VbpHashPartition **lockedPartition)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint64 control;
    VbpHashPartition *partition;
    VbpHashPartition *latestPartition;

    if (runtime == NULL || vbp == NULL || generation == 0 || lockedControl == NULL ||
        lockedPartition == NULL || VbpInstanceGeneration(vbp) != generation ||
        !VbpHashStateAllowsPayloadInvalidation(VbpInstanceState(vbp))) {
        return false;
    }
    control = VbpHashControlRead(vbp);
    if (!VbpHashPartitionForControl(runtime, vbp, control, hash, &partition)) {
        return false;
    }

    SpinLockAcquire(&partition->lock);
    control = VbpHashControlRead(vbp);
    if (!VbpHashStateAllowsPayloadInvalidation(VbpInstanceState(vbp))) {
        SpinLockRelease(&partition->lock);
        return false;
    }
    if (VbpInstanceGeneration(vbp) != generation ||
        !VbpHashPartitionForControl(runtime, vbp, control, hash, &latestPartition) ||
        latestPartition != partition) {
        SpinLockRelease(&partition->lock);
        return false;
    }

    *lockedControl = control;
    *lockedPartition = partition;
    return true;
}

static VbpEntryRef *VbpHashBucketForControl(uint64 control, uint32 hash)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashViewDesc *activeDesc = VbpHashPublishedViewDesc(runtime,
        VbpHashControlActiveDesc(control));
    const VbpHashView *selected;
    uint32 selectedBucket;

    selected = &activeDesc->view;
    Assert(VbpHashViewBucketsAreValid(selected));
    selectedBucket = hash & (selected->bucketCount - 1);

    if (VbpHashControlIsRehashing(control)) {
        VbpHashViewDesc *candidateDesc = VbpHashPublishedViewDesc(runtime,
            VbpHashControlCandidateDesc(control));
        const VbpHashView *candidate;
        uint32 wordIndex;
        uint32 bitIndex;
        uint64 word;

        candidate = &candidateDesc->view;
        Assert(VbpHashViewBucketsAreValid(candidate));
        Assert(VbpHashMigrationBitmapIsValid(candidate));
        wordIndex = selectedBucket / VBP_HASH_BITMAP_BITS;
        bitIndex = selectedBucket % VBP_HASH_BITMAP_BITS;
        word = pg_atomic_barrier_read_u64(&candidate->migratedBitmap[wordIndex]);
        if ((word & (UINT64CONST(1) << bitIndex)) != 0) {
            selected = candidate;
            selectedBucket = hash & (candidate->bucketCount - 1);
        }
    }

    return &selected->buckets[selectedBucket];
}

static bool VbpHashValidateMigrationChain(VbpInstance *vbp, VbpEntryRef head, uint32 oldBucket,
    uint32 oldBucketCount)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpEntryRef current = head;
    Size visited = 0;
    Size limit = VbpHashTraversalLimit(runtime);

    while (VbpEntryRefIsValid(current)) {
        VbpEntry *entry;

        if (visited++ >= limit) {
            return false;
        }
        entry = VbpResolveEntry(vbp, current, NULL);
        if (entry == NULL ||
            VbpEntryControlState(pg_atomic_barrier_read_u64(&entry->control)) != VBP_ENTRY_CACHED ||
            (VbpHashTid(&entry->payloadTid) & (oldBucketCount - 1)) != oldBucket) {
            return false;
        }
        current = VbpHashRefLoad(&entry->hashNext);
    }
    return current == VBP_INVALID_ENTRY_REF;
}

static bool VbpHashFinishResizeLocked(VbpInstance *vbp, uint64 control)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint32 oldDescIndex = VbpHashControlActiveDesc(control);
    uint32 candidateDescIndex = VbpHashControlCandidateDesc(control);
    VbpHashViewDesc *oldDesc = VbpHashResolveViewDesc(runtime, oldDescIndex);
    VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime, candidateDescIndex);
    uint64 expected = control;
    uint64 desired = VbpHashPackControl(candidateDescIndex, VBP_INVALID_INDEX);
    if (oldDesc == NULL || candidateDesc == NULL || desired == 0 ||
        candidateDesc->listNext != VBP_INVALID_INDEX) {
        return false;
    }
    candidateDesc->listNext = oldDescIndex;
    pg_memory_barrier();
    if (pg_atomic_compare_exchange_u64(&vbp->hashControl, &expected, desired)) {
        return true;
    }

    return expected == desired;
}

static void VbpHashSplitMigrationChain(VbpHashMigrationContext *context)
{
    VbpEntryRef current = VbpHashRefLoad(context->oldHead);

    while (VbpEntryRefIsValid(current)) {
        VbpEntry *entry = VbpResolveEntry(context->vbp, current, NULL);
        VbpEntryRef next = VbpHashRefLoad(&entry->hashNext);
        uint32 newBucket = VbpHashTid(&entry->payloadTid) & (context->candidate->bucketCount - 1);
        if (newBucket == context->oldBucket) {
            VbpHashRefStore(&entry->hashNext, context->low);
            context->low = current;
        } else {
            Assert(newBucket == context->oldBucket + context->oldView->bucketCount);
            VbpHashRefStore(&entry->hashNext, context->high);
            context->high = current;
        }
        current = next;
    }
}

static bool VbpHashCompleteBucketMigration(VbpHashMigrationContext *context)
{
    uint32 wordIndex;
    uint32 bitIndex;
    uint32 migratedCount;

    VbpHashRefStore(context->lowHead, context->low);
    VbpHashRefStore(context->highHead, context->high);
    VbpHashRefStore(context->oldHead, VBP_INVALID_ENTRY_REF);
    pg_memory_barrier();
    wordIndex = context->oldBucket / VBP_HASH_BITMAP_BITS;
    bitIndex = context->oldBucket % VBP_HASH_BITMAP_BITS;
    (void)pg_atomic_fetch_or_u64(&context->candidate->migratedBitmap[wordIndex], UINT64CONST(1) << bitIndex);
    migratedCount = pg_atomic_add_fetch_u32(&context->vbp->migratedBuckets, 1);
    Assert(migratedCount <= context->oldView->bucketCount);
    if (migratedCount == context->oldView->bucketCount) {
        return VbpHashFinishResizeLocked(context->vbp, context->control);
    }
    return migratedCount < context->oldView->bucketCount;
}

static bool VbpHashMigrateBucketLocked(VbpInstance *vbp, uint64 control, uint32 oldBucket)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashViewDesc *oldDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime,
        VbpHashControlCandidateDesc(control));
    const VbpHashView *oldView;
    const VbpHashView *candidate;
    VbpHashMigrationContext context = {};
    bool migrated;

    if (!VbpHashControlIsRehashing(control) || oldDesc == NULL || candidateDesc == NULL) {
        return false;
    }
    oldView = &oldDesc->view;
    candidate = &candidateDesc->view;
    if (oldBucket >= oldView->bucketCount ||
        !VbpHashBitmapIsMigrated(candidate, oldView->bucketCount, oldBucket, &migrated)) {
        return false;
    }
    if (migrated) {
        return true;
    }

    context.runtime = runtime;
    context.vbp = vbp;
    context.oldView = oldView;
    context.candidate = candidate;
    context.oldHead = &oldView->buckets[oldBucket];
    context.lowHead = &candidate->buckets[oldBucket];
    context.highHead = &candidate->buckets[oldBucket + oldView->bucketCount];
    context.low = VBP_INVALID_ENTRY_REF;
    context.high = VBP_INVALID_ENTRY_REF;
    context.control = control;
    context.oldBucket = oldBucket;
    if (VbpHashRefLoad(context.lowHead) != VBP_INVALID_ENTRY_REF ||
        VbpHashRefLoad(context.highHead) != VBP_INVALID_ENTRY_REF ||
        !VbpHashValidateMigrationChain(vbp, VbpHashRefLoad(context.oldHead), oldBucket, oldView->bucketCount)) {
        return false;
    }
    VbpHashSplitMigrationChain(&context);
    return VbpHashCompleteBucketMigration(&context);
}

static bool VbpHashCandidateIsNewView(const VbpHashRuntime *runtime, uint32 activeDescIndex,
    uint32 candidateDescIndex)
{
    VbpHashViewDesc *activeDesc = VbpHashResolveViewDesc(runtime, activeDescIndex);
    VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime, candidateDescIndex);
    uint32 current;
    uint32 visited = 0;

    if (activeDesc == NULL || candidateDesc == NULL || activeDescIndex == candidateDescIndex ||
        activeDesc->view.buckets == candidateDesc->view.buckets ||
        (activeDesc->view.migratedBitmap != NULL &&
            activeDesc->view.migratedBitmap == candidateDesc->view.migratedBitmap) ||
        candidateDesc->listNext != VBP_INVALID_INDEX) {
        return false;
    }

    current = activeDesc->listNext;
    pg_read_barrier();
    while (current != VBP_INVALID_INDEX) {
        VbpHashViewDesc *retiredDesc;

        if (visited++ >= runtime->viewDescCapacity || current == candidateDescIndex) {
            return false;
        }
        retiredDesc = VbpHashResolveViewDesc(runtime, current);
        if (retiredDesc == NULL || retiredDesc->view.buckets == candidateDesc->view.buckets ||
            (retiredDesc->view.migratedBitmap != NULL &&
                retiredDesc->view.migratedBitmap == candidateDesc->view.migratedBitmap)) {
            return false;
        }
        current = retiredDesc->listNext;
        pg_read_barrier();
    }
    return true;
}

void VbpHashPartitionArenaInit(VbpHashPartition *partitionArena, uint32 partitionCapacity)
{
    Assert(partitionArena != NULL || partitionCapacity == 0);
    if (partitionArena == NULL) {
        return;
    }

    for (uint32 partition = 0; partition < partitionCapacity; ++partition) {
        SpinLockInit(&partitionArena[partition].lock);
    }
}

bool VbpHashRuntimeInit(VbpHashRuntime *runtime, const VbpHashRuntime *config)
{
    if (runtime == NULL || !VbpHashRuntimeIsValid(config)) {
        return false;
    }

    *runtime = *config;
    return true;
}

void VbpHashRuntimeAttach(VbpHashRuntime *runtime)
{
    g_vbpHashRuntime = VbpHashRuntimeIsValid(runtime) ? runtime : NULL;
}

void VbpHashRuntimeDetach(VbpHashRuntime *runtime)
{
    if (g_vbpHashRuntime == runtime) {
        g_vbpHashRuntime = NULL;
    }
}

bool VbpHashInit(VbpInstance *vbp, const VbpHashInitConfig *config)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashViewDesc *initialDesc;
    const VbpHashView *initial;
    uint64 initialControl;

    if (runtime == NULL || vbp == NULL || config == NULL ||
        VbpLifecycleRead(vbp) != VbpLifecycleControlPack(config->generation, VBP_FREE, 0)) {
        return false;
    }
    initialDesc = VbpHashResolveViewDesc(runtime, config->initialDesc);
    if (initialDesc == NULL ||
        initialDesc->listNext != VBP_INVALID_INDEX) {
        return false;
    }
    initial = &initialDesc->view;
    initialControl = VbpHashPackControl(config->initialDesc, VBP_INVALID_INDEX);
    if (initialControl == 0 || !VbpHashViewBucketsAreValid(initial) ||
        config->partitionCount == 0 || (config->partitionCount & (config->partitionCount - 1)) != 0 ||
        config->partitionCount > initial->bucketCount ||
        config->partitionBase > runtime->partitionCapacity ||
        config->partitionCount > runtime->partitionCapacity - config->partitionBase) {
        return false;
    }
    vbp->partitionBase = config->partitionBase;
    vbp->partitionCount = config->partitionCount;
    vbp->id = config->id;
    pg_atomic_init_u32(&vbp->liveEntries, 0);
    pg_atomic_init_u32(&vbp->migratedBuckets, 0);
    pg_atomic_init_u32(&vbp->resizeClaim, VBP_RESIZE_NONE);
    pg_atomic_init_u32(&vbp->reclaimBucketCursor, 0);
    pg_atomic_init_u32(&vbp->migrateCursor, 0);
    pg_memory_barrier();
    pg_atomic_init_u64(&vbp->hashControl, initialControl);
    return true;
}

bool VbpHashNeedsResize(const VbpInstance *vbp)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint64 control;
    VbpHashViewDesc *activeDesc;
    uint32 liveEntries;

    if (runtime == NULL || vbp == NULL || VbpInstanceState(vbp) != VBP_ACTIVE) {
        return false;
    }
    control = VbpHashControlRead(vbp);
    if (VbpHashControlIsRehashing(control) || !VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
        return false;
    }
    activeDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    liveEntries = pg_atomic_read_u32((volatile uint32 *)&vbp->liveEntries);
    /*
     * Grow when live exceeds the current table so steady load stays in (0.5, 1].
     * The previous 2x threshold left typical Hit walking 2–3 chain nodes.
     */
    return (uint64)liveEntries > (uint64)activeDesc->view.bucketCount;
}

static void VbpHashReleaseResizeOwner(VbpInstance *vbp)
{
    Assert(vbp != NULL);
    Assert(pg_atomic_read_u32(&vbp->resizeClaim) == VBP_RESIZE_OWNER);
    pg_memory_barrier();
    pg_atomic_write_u32(&vbp->resizeClaim, VBP_RESIZE_NONE);
}

bool VbpHashStartResize(VbpInstance *vbp, uint32 candidateDescIndex)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint32 expectedClaim = VBP_RESIZE_NONE;
    uint32 expectedGeneration;
    uint64 lifecycle;
    uint64 expected;
    uint64 desired;
    uint32 activeDescIndex;
    VbpHashViewDesc *activeDesc;
    VbpHashViewDesc *candidateDesc;

    if (runtime == NULL || vbp == NULL || candidateDescIndex == VBP_INVALID_INDEX) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE) {
        return false;
    }
    expectedGeneration = VbpLifecycleGeneration(lifecycle);
    if (expectedGeneration == 0 ||
        !pg_atomic_compare_exchange_u32(&vbp->resizeClaim, &expectedClaim, VBP_RESIZE_OWNER)) {
        return false;
    }
    pg_read_barrier();
    if (!VbpInstanceLifecycleMatches(vbp, expectedGeneration, VBP_ACTIVE)) {
        VbpHashReleaseResizeOwner(vbp);
        return false;
    }
    expected = VbpHashControlRead(vbp);
    activeDescIndex = VbpHashControlActiveDesc(expected);
    activeDesc = VbpHashResolveViewDesc(runtime, activeDescIndex);
    candidateDesc = VbpHashResolveViewDesc(runtime, candidateDescIndex);
    if (VbpHashControlIsRehashing(expected) || !VbpHashInstanceLayoutIsValid(runtime, vbp, expected) ||
        activeDesc == NULL || candidateDesc == NULL ||
        activeDesc->view.bucketCount > PG_UINT32_MAX / VBP_HASH_GROWTH_FACTOR ||
        candidateDesc->view.bucketCount != activeDesc->view.bucketCount * VBP_HASH_GROWTH_FACTOR ||
        !VbpHashViewBucketsAreValid(&candidateDesc->view) ||
        !VbpHashMigrationBitmapIsValid(&candidateDesc->view) ||
        !VbpHashCandidateIsNewView(runtime, activeDescIndex, candidateDescIndex)) {
        VbpHashReleaseResizeOwner(vbp);
        return false;
    }

    pg_atomic_write_u32(&vbp->migratedBuckets, 0);
    pg_memory_barrier();
    desired = VbpHashPackControl(activeDescIndex, candidateDescIndex);
    if (!pg_atomic_compare_exchange_u64(&vbp->hashControl, &expected, desired)) {
        VbpHashReleaseResizeOwner(vbp);
        return false;
    }
    VbpHashReleaseResizeOwner(vbp);
    return true;
}

static VbpHashPinAttemptResult VbpHashTryPinChain(const VbpHashPinRequest *request, VbpEntryRef head,
    VbpEntryRef *pinnedRef, VbpEntry **pinnedEntry, uint32 *pinnedIncarnation)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpEntryRef current = head;
    Size visited = 0;
    Size limit;
    VbpEntrySnapshotRequest snapshotRequest;

    if (runtime == NULL || request == NULL || pinnedRef == NULL || pinnedEntry == NULL ||
        pinnedIncarnation == NULL) {
        return VBP_HASH_PIN_RETRY;
    }
    snapshotRequest = {runtime, request->vbp, request->slotStride,
        request->slotsPerChunk, request->vbpGeneration};
    limit = VbpHashTraversalLimit(runtime);
    while (VbpEntryRefIsValid(current)) {
        VbpMemChunk *chunk;
        uint64 publicationEpoch;
        VbpEntry *entry;
        uint64 entryControl;
        bool tidMatches;

        if (visited++ >= limit) {
            return VBP_HASH_PIN_RETRY;
        }
        entry = VbpResolveEntrySnapshot(&snapshotRequest, current, &chunk, &publicationEpoch);
        if (entry == NULL) {
            return VBP_HASH_PIN_RETRY;
        }
        entryControl = pg_atomic_barrier_read_u64(&entry->control);
        tidMatches = VbpEntryControlCanPin(entryControl) &&
            VbpEntryControlIncarnation(entryControl) != 0 &&
            ItemPointerEqualsNoCheck((ItemPointer)&entry->payloadTid, (ItemPointer)request->tid);
        if (tidMatches) {
            if (!VbpEntrySnapshotIsValid(request->vbp, chunk, publicationEpoch,
                    request->vbpGeneration)) {
                return VBP_HASH_PIN_RETRY;
            }
            if (!VbpHashTryPinEntryFromControl(entry, entryControl, pinnedIncarnation)) {
                return VBP_HASH_PIN_RETRY;
            }
            *pinnedRef = current;
            *pinnedEntry = entry;
            return VBP_HASH_PIN_HIT;
        }
        current = VbpHashRefLoad(&entry->hashNext);
        if (!VbpEntrySnapshotIsValid(request->vbp, chunk, publicationEpoch,
                request->vbpGeneration)) {
            return VBP_HASH_PIN_RETRY;
        }
    }
    return current == VBP_INVALID_ENTRY_REF ? VBP_HASH_PIN_MISS : VBP_HASH_PIN_RETRY;
}

static VbpHashPinAttemptResult VbpHashTryPinLocked(const VbpHashPinRequest *request,
    VbpEntryRef *pinnedRef, VbpEntry **pinnedEntry, uint32 *pinnedIncarnation)
{
    uint64 control;
    VbpHashPartition *partition;
    VbpEntryRef *bucketHead;
    VbpHashFindMatch match;
    VbpHashFindResult result;

    if (request == NULL || pinnedRef == NULL || pinnedEntry == NULL || pinnedIncarnation == NULL ||
        !VbpHashLockPartition(request->vbp, request->access->vbpGeneration, request->hash,
            &control, &partition)) {
        return VBP_HASH_PIN_RETRY;
    }
    bucketHead = VbpHashBucketForControl(control, request->hash);
    result = VbpHashFindEntry(request->vbp, VbpHashRefLoad(bucketHead), request->tid, &match);
    if (result == VBP_HASH_FOUND && !VbpHashTryPinEntry(match.entry, pinnedIncarnation)) {
        result = VBP_HASH_CORRUPT;
    }
    SpinLockRelease(&partition->lock);
    if (result == VBP_HASH_FOUND) {
        *pinnedRef = match.ref;
        *pinnedEntry = match.entry;
        return VBP_HASH_PIN_HIT;
    }
    return result == VBP_HASH_NOT_FOUND ? VBP_HASH_PIN_MISS : VBP_HASH_PIN_RETRY;
}

static VbpHashPinAttemptResult VbpHashTryPinOnce(const VbpHashPinRequest *request)
{
    VbpEntryRef *bucketHead = NULL;
    VbpEntryRef pinnedRef = VBP_INVALID_ENTRY_REF;
    VbpEntry *pinnedEntry = NULL;
    uint32 incarnation = 0;
    uint64 control;
    uint64 lifecycle;
    VbpHashPinAttemptResult result;

    control = VbpHashControlRead(request->vbp);
    bucketHead = VbpHashBucketForControl(control, request->hash);
    result = VbpHashTryPinChain(request, VbpHashRefLoad(bucketHead),
        &pinnedRef, &pinnedEntry, &incarnation);
    if (result == VBP_HASH_PIN_MISS) {
        uint64 latestControl = VbpHashControlRead(request->vbp);
        if (VbpHashControlIsRehashing(control) || latestControl != control) {
            result = VbpHashTryPinLocked(request, &pinnedRef, &pinnedEntry, &incarnation);
        }
    }
    if (result != VBP_HASH_PIN_HIT) {
        return result;
    }
    lifecycle = VbpLifecycleRead(request->vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE ||
        VbpLifecycleGeneration(lifecycle) != request->vbpGeneration) {
        VbpHashUndoPinEntry(request->vbp, pinnedRef, pinnedEntry, incarnation);
        return VBP_HASH_PIN_MISS;
    }
    VbpAccessSetEntry(request->access, pinnedRef, pinnedEntry, incarnation, VBP_ACCESS_CACHED_PIN);
    request->access->pinCookie = VbpNextPinCookie();
    *request->entryRef = pinnedRef;
    *request->payload = VbpEntryPayload(pinnedEntry);
    return VBP_HASH_PIN_HIT;
}

bool VbpHashTryPin(VbpInstance *vbp, const ItemPointerData *tid, VectorBufferAccess *access,
    VbpEntryRef *entryRef, const char **payload)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashPinRequest request = {};
    const uint32 maxAttempts = 4U;
    uint64 lifecycle;

    if (entryRef != NULL) {
        *entryRef = VBP_INVALID_ENTRY_REF;
    }
    if (payload != NULL) {
        *payload = NULL;
    }
    if (runtime == NULL || vbp == NULL || tid == NULL || access == NULL || entryRef == NULL || payload == NULL ||
        access->mode != VBP_ACCESS_IDLE || access->pinCookie != 0 || access->vbp != vbp) {
        return false;
    }
    request.vbp = vbp;
    request.tid = tid;
    request.access = access;
    request.entryRef = entryRef;
    request.payload = payload;
    request.hash = VbpHashTid(tid);
    request.slotStride = vbp->payloadLayout.slotSize;
    request.vbpGeneration = access->vbpGeneration;
    lifecycle = VbpLifecycleRead(vbp);
    if (request.vbpGeneration == 0 || VbpLifecycleState(lifecycle) != VBP_ACTIVE ||
        VbpLifecycleGeneration(lifecycle) != request.vbpGeneration ||
        request.slotStride < sizeof(VbpEntry) ||
        request.slotStride > runtime->chunkSize ||
        request.slotStride % alignof(VbpEntry) != 0) {
        return false;
    }
    request.slotsPerChunk = vbp->payloadLayout.slotsPerChunk;
    Assert(request.slotsPerChunk != 0);
    for (uint32 attempt = 0; attempt < maxAttempts; ++attempt) {
        VbpHashPinAttemptResult result = VbpHashTryPinOnce(&request);
        if (result != VBP_HASH_PIN_RETRY) {
            return result == VBP_HASH_PIN_HIT;
        }
    }
    return false;
}

static bool VbpHashLockPublishBucket(VbpHashPublishContext *context)
{
    if (!VbpHashLockPartition(context->vbp, context->access->vbpGeneration, context->hash,
            &context->control, &context->partition)) {
        return false;
    }
    if (VbpHashControlIsRehashing(context->control)) {
        VbpHashViewDesc *oldDesc = VbpHashResolveViewDesc(g_vbpHashRuntime,
            VbpHashControlActiveDesc(context->control));
        VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(g_vbpHashRuntime,
            VbpHashControlCandidateDesc(context->control));
        const VbpHashView *oldView;
        const VbpHashView *candidate;
        uint32 oldBucket;

        if (oldDesc == NULL || candidateDesc == NULL) {
            SpinLockRelease(&context->partition->lock);
            return false;
        }
        oldView = &oldDesc->view;
        candidate = &candidateDesc->view;
        oldBucket = context->hash & (oldView->bucketCount - 1);
        if (!VbpHashMigrateBucketLocked(context->vbp, context->control, oldBucket)) {
            SpinLockRelease(&context->partition->lock);
            return false;
        }
        context->bucketHead = &candidate->buckets[context->hash & (candidate->bucketCount - 1)];
    } else {
        context->bucketHead = VbpHashBucketForControl(context->control, context->hash);
    }
    return true;
}

static bool VbpHashPublishExistingAndUnlock(VbpHashPublishContext *context,
    const VbpHashFindMatch *match)
{
    uint32 foundIncarnation = 0;
    const char *foundPayload;

    if (!VbpHashTryPinEntry(match->entry, &foundIncarnation)) {
        SpinLockRelease(&context->partition->lock);
        return false;
    }
    foundPayload = VbpEntryPayload(match->entry);
    SpinLockRelease(&context->partition->lock);
    VbpReleaseReservation(context->access);
    if (context->access->mode != VBP_ACCESS_IDLE) {
        VbpHashUndoPinEntry(context->vbp, match->ref, match->entry, foundIncarnation);
        return false;
    }
    VbpAccessSetEntry(context->access, match->ref, match->entry, foundIncarnation, VBP_ACCESS_CACHED_PIN);
    context->access->pinCookie = VbpNextPinCookie();
    context->result->winner = match->ref;
    context->result->payload = foundPayload;
    return true;
}

static bool VbpHashPublishReservedAndUnlock(VbpHashPublishContext *context)
{
    uint64 reservedControl = VbpEntryControlPack(context->reservedIncarnation, VBP_ENTRY_CLOSED, 0);
    if (!VbpInstanceLifecycleMatches(context->vbp,
            context->access->vbpGeneration, VBP_ACTIVE) ||
        pg_atomic_read_u64(&context->reservedEntry->control) != reservedControl ||
        !ItemPointerEqualsNoCheck((ItemPointer)&context->reservedEntry->payloadTid,
            (ItemPointer)context->tid)) {
        SpinLockRelease(&context->partition->lock);
        return false;
    }
    VbpHashRefStore(&context->reservedEntry->hashNext, VbpHashRefLoad(context->bucketHead));
    if (!pg_atomic_compare_exchange_u64(&context->reservedEntry->control, &reservedControl,
            VbpEntryControlWithUsed(
                VbpEntryControlPack(context->reservedIncarnation, VBP_ENTRY_CACHED, 1)))) {
        SpinLockRelease(&context->partition->lock);
        return false;
    }
    pg_write_barrier();
    VbpHashRefStore(context->bucketHead, context->reserved);
    (void)pg_atomic_fetch_add_u32(&context->vbp->liveEntries, 1);
    VbpAccessSetEntry(context->access, context->reserved, context->reservedEntry, context->reservedIncarnation,
        VBP_ACCESS_CACHED_PIN);
    context->access->pinCookie = VbpNextPinCookie();
    context->result->winner = context->reserved;
    context->result->payload = VbpEntryPayload(context->reservedEntry);
    context->result->published = true;
    SpinLockRelease(&context->partition->lock);
    return true;
}

bool VbpHashPublishReserved(VbpInstance *vbp, const ItemPointerData *tid, VbpEntryRef reserved,
    VectorBufferAccess *access, VbpHashPublishResult *publishResult)
{
    VbpHashPublishContext context = {};
    VbpHashFindMatch match;
    VbpHashFindResult findResult;

    if (publishResult != NULL) {
        publishResult->winner = VBP_INVALID_ENTRY_REF;
        publishResult->payload = NULL;
        publishResult->published = false;
    }
    if (vbp == NULL || tid == NULL || access == NULL || publishResult == NULL ||
        access->mode != VBP_ACCESS_RESERVED || access->pinCookie != 0 || access->vbp != vbp ||
        !VbpEntryRefIsValid(reserved) || access->state.entry.ref != reserved ||
        access->state.entry.incarnation == 0) {
        return false;
    }
    context.vbp = vbp;
    context.tid = tid;
    context.access = access;
    context.result = publishResult;
    context.reserved = reserved;
    context.reservedIncarnation = access->state.entry.incarnation;
    if (access->vbpGeneration == 0 || access->vbpGeneration != VbpInstanceGeneration(vbp)) {
        return false;
    }
    context.reservedEntry = VbpResolveEntry(vbp, reserved, NULL);
    if (context.reservedEntry == NULL ||
        !ItemPointerEqualsNoCheck((ItemPointer)&context.reservedEntry->payloadTid, (ItemPointer)tid)) {
        return false;
    }
    context.hash = VbpHashTid(tid);
    if (!VbpHashLockPublishBucket(&context)) {
        return false;
    }
    findResult = VbpHashFindEntry(vbp, VbpHashRefLoad(context.bucketHead), tid, &match);
    if (findResult == VBP_HASH_CORRUPT) {
        SpinLockRelease(&context.partition->lock);
        return false;
    }
    return findResult == VBP_HASH_FOUND ?
        VbpHashPublishExistingAndUnlock(&context, &match) : VbpHashPublishReservedAndUnlock(&context);
}

static bool VbpHashInvalidateControlLocked(VbpEntry *entry, uint64 raw,
    uint32 *incarnation, bool *recycleOwned)
{
    uint32 expectedIncarnation;

    if (entry == NULL || incarnation == NULL || recycleOwned == NULL ||
        VbpEntryControlState(raw) != VBP_ENTRY_CACHED) {
        return false;
    }
    expectedIncarnation = VbpEntryControlIncarnation(raw);
    if (expectedIncarnation == 0) {
        return false;
    }

    while (VbpEntryControlIncarnation(raw) == expectedIncarnation &&
        VbpEntryControlState(raw) == VBP_ENTRY_CACHED) {
        uint32 readers;
        bool owned;
        uint64 expected;
        uint64 desired;

        readers = VbpEntryControlReaders(raw);
        owned = readers == 0;
        desired = VbpEntryControlPack(expectedIncarnation, VBP_ENTRY_CLOSED, readers);
        expected = raw;
        if (pg_atomic_compare_exchange_u64(&entry->control, &expected, desired)) {
            *incarnation = expectedIncarnation;
            *recycleOwned = owned;
            return true;
        }
        raw = expected;
    }
    return false;
}

static bool VbpHashInvalidateMatchLocked(VbpInstance *vbp, VbpEntryRef *bucketHead,
    const VbpHashFindMatch *match, VbpHashInvalidateResult *result)
{
    uint64 raw = pg_atomic_read_u64(&match->entry->control);
    if (VbpEntryControlState(raw) != VBP_ENTRY_CACHED ||
        VbpEntryControlIncarnation(raw) == 0 || pg_atomic_read_u32(&vbp->liveEntries) == 0) {
        return false;
    }
    if (match->previous == NULL) {
        VbpHashRefStore(bucketHead, VbpHashRefLoad(&match->entry->hashNext));
    } else {
        VbpHashRefStore(&match->previous->hashNext, VbpHashRefLoad(&match->entry->hashNext));
    }
    VbpHashRefStore(&match->entry->hashNext, VBP_INVALID_ENTRY_REF);
    (void)pg_atomic_fetch_sub_u32(&vbp->liveEntries, 1);
    if (!VbpHashInvalidateControlLocked(match->entry, raw, &result->incarnation,
            &result->recycleOwned)) {
        return false;
    }
    result->removed = match->ref;
    return true;
}

bool VbpHashInvalidatePayload(VbpInstance *vbp, uint32 generation, const ItemPointerData *tid,
    VbpHashInvalidateResult *invalidateResult)
{
    uint32 hash;
    uint64 control;
    VbpHashPartition *partition;
    VbpEntryRef *bucketHead;
    VbpHashFindMatch match;
    VbpHashFindResult result;
    bool invalidated;

    if (invalidateResult != NULL) {
        invalidateResult->removed = VBP_INVALID_ENTRY_REF;
        invalidateResult->incarnation = 0;
        invalidateResult->recycleOwned = false;
    }
    if (vbp == NULL || generation == 0 || tid == NULL || invalidateResult == NULL) {
        return false;
    }

    hash = VbpHashTid(tid);
    if (!VbpHashLockPayloadInvalidationPartition(vbp, generation, hash, &control, &partition)) {
        return false;
    }
    bucketHead = VbpHashBucketForControl(control, hash);
    result = VbpHashFindEntry(vbp, VbpHashRefLoad(bucketHead), tid, &match);
    if (result != VBP_HASH_FOUND) {
        SpinLockRelease(&partition->lock);
        return false;
    }
    invalidated = VbpHashInvalidateMatchLocked(vbp, bucketHead, &match, invalidateResult);
    SpinLockRelease(&partition->lock);
    return invalidated;
}

static bool VbpHashSelectDrainHead(VbpHashRuntime *runtime, VbpInstance *vbp, uint64 control,
    uint32 oldBucket, VbpEntryRef **head)
{
    VbpHashViewDesc *activeDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    const VbpHashView *active = activeDesc == NULL ? NULL : &activeDesc->view;

    *head = NULL;
    if (active == NULL || oldBucket >= active->bucketCount) {
        return false;
    }
    if (!VbpHashControlIsRehashing(control)) {
        *head = &active->buckets[oldBucket];
        return true;
    }

    VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime,
        VbpHashControlCandidateDesc(control));
    bool migrated = false;

    if (candidateDesc == NULL || !VbpHashBitmapIsMigrated(&candidateDesc->view,
            active->bucketCount, oldBucket, &migrated)) {
        return false;
    }
    if (!migrated) {
        *head = &active->buckets[oldBucket];
    } else {
        VbpEntryRef *low = &candidateDesc->view.buckets[oldBucket];
        VbpEntryRef *high = &candidateDesc->view.buckets[oldBucket + active->bucketCount];
        VbpEntryRef lowHead = VbpHashRefLoad(low);
        if (lowHead != VBP_INVALID_ENTRY_REF && !VbpEntryRefIsValid(lowHead)) {
            return false;
        }
        *head = lowHead != VBP_INVALID_ENTRY_REF ? low : high;
    }
    return true;
}

static bool VbpHashDrainBucketLocked(VbpInstance *vbp, uint64 control, uint32 oldBucket,
    VbpHashDrainResult *result)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpEntryRef *head = NULL;
    VbpEntryRef current;
    VbpEntry *entry;
    VbpEntryRef next;
    uint64 raw;

    result->bucketEmpty = false;
    result->removed = VBP_INVALID_ENTRY_REF;
    result->incarnation = 0;
    result->recycleOwned = false;
    if (!VbpHashSelectDrainHead(runtime, vbp, control, oldBucket, &head)) {
        return false;
    }
    current = VbpHashRefLoad(head);
    if (current == VBP_INVALID_ENTRY_REF) {
        result->bucketEmpty = true;
        return true;
    }
    if (!VbpEntryRefIsValid(current)) {
        return false;
    }
    entry = VbpResolveEntry(vbp, current, NULL);
    if (entry == NULL) {
        return false;
    }
    raw = pg_atomic_read_u64(&entry->control);
    if (VbpEntryControlState(raw) != VBP_ENTRY_CACHED ||
        VbpEntryControlIncarnation(raw) == 0 || pg_atomic_read_u32(&vbp->liveEntries) == 0) {
        return false;
    }
    next = VbpHashRefLoad(&entry->hashNext);
    if (next != VBP_INVALID_ENTRY_REF && !VbpEntryRefIsValid(next)) {
        return false;
    }

    VbpHashRefStore(head, next);
    VbpHashRefStore(&entry->hashNext, VBP_INVALID_ENTRY_REF);
    (void)pg_atomic_fetch_sub_u32(&vbp->liveEntries, 1);
    if (!VbpHashInvalidateControlLocked(entry, raw, &result->incarnation, &result->recycleOwned)) {
        return false;
    }
    result->removed = current;
    return true;
}

static VbpHashDrainStepResult VbpHashDrainBucketStep(VbpHashDrainState *state, uint32 oldBucket)
{
    uint32 relativePartition = VbpHashPartitionForBucket(oldBucket, state->vbp->partitionCount);
    VbpHashPartition *partition;
    uint64 latestControl;
    VbpHashDrainResult result = {};
    bool drained;

    if (relativePartition == VBP_INVALID_INDEX ||
        state->vbp->partitionBase + relativePartition >= state->runtime->partitionCapacity) {
        return VBP_HASH_DRAIN_ERROR;
    }
    partition = &state->runtime->partitionArena[state->vbp->partitionBase + relativePartition];
    SpinLockAcquire(&partition->lock);
    latestControl = VbpHashControlRead(state->vbp);
    if (!VbpInstanceLifecycleMatches(state->vbp, state->generation, VBP_DETACHED)) {
        SpinLockRelease(&partition->lock);
        return VBP_HASH_DRAIN_ERROR;
    }
    if (latestControl != state->control) {
        SpinLockRelease(&partition->lock);
        state->control = latestControl;
        return VBP_HASH_DRAIN_RESTART;
    }
    drained = VbpHashDrainBucketLocked(state->vbp, state->control, oldBucket, &result);
    SpinLockRelease(&partition->lock);
    if (!drained) {
        return VBP_HASH_DRAIN_ERROR;
    }
    if (!VbpEntryRefIsValid(result.removed)) {
        return result.bucketEmpty ? VBP_HASH_DRAIN_EMPTY : VBP_HASH_DRAIN_ERROR;
    }
    if (++state->removedCount > state->removalLimit) {
        return VBP_HASH_DRAIN_ERROR;
    }
    if (result.recycleOwned) {
        VbpRecycleEntryOwned(state->vbp, result.removed, result.incarnation, NULL);
    }
    return VBP_HASH_DRAIN_REMOVED;
}

bool VbpHashDrainDetached(VbpInstance *vbp, uint32 generation)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashDrainState state = {};
    uint32 controlRetries = 0;

    if (runtime == NULL ||
        !VbpInstanceLifecycleMatches(vbp, generation, VBP_DETACHED)) {
        return false;
    }
    state.runtime = runtime;
    state.vbp = vbp;
    state.control = VbpHashControlRead(vbp);
    state.removalLimit = VbpHashTraversalLimit(runtime);
    state.generation = generation;

    while (controlRetries <= runtime->viewDescCapacity + 1) {
        VbpHashViewDesc *activeDesc = VbpHashResolveViewDesc(runtime,
            VbpHashControlActiveDesc(state.control));
        uint32 bucketCount;
        bool restart = false;

        if (!VbpInstanceLifecycleMatches(vbp, generation, VBP_DETACHED) ||
            !VbpHashInstanceLayoutIsValid(runtime, vbp, state.control) || activeDesc == NULL) {
            return false;
        }
        bucketCount = activeDesc->view.bucketCount;
        for (uint32 oldBucket = 0; oldBucket < bucketCount;) {
            VbpHashDrainStepResult result = VbpHashDrainBucketStep(&state, oldBucket);
            if (result == VBP_HASH_DRAIN_ERROR) {
                return false;
            }
            if (result == VBP_HASH_DRAIN_RESTART) {
                restart = true;
                break;
            }
            if (result == VBP_HASH_DRAIN_EMPTY) {
                ++oldBucket;
            }
        }
        uint64 latestControl = VbpHashControlRead(vbp);
        if (restart || latestControl != state.control) {
            state.control = latestControl;
            if (++controlRetries > runtime->viewDescCapacity + 1) {
                return false;
            }
            continue;
        }
        return pg_atomic_read_u32(&vbp->liveEntries) == 0;
    }
    return false;
}

static bool VbpHashEvictChainLocked(VbpInstance *vbp, volatile VbpEntryRef *head, uint32 maxEntryChecks,
    VbpHashEvictResult *result)
{
    volatile VbpEntryRef *link = head;
    VbpEntryRef current = VbpHashRefLoad(head);

    while (VbpEntryRefIsValid(current) && result->entryChecks < maxEntryChecks) {
        VbpEntry *entry = VbpResolveEntry(vbp, current, NULL);
        uint64 control;
        VbpEntryRef next;

        if (entry == NULL) {
            return false;
        }
        control = pg_atomic_read_u64(&entry->control);
        if (VbpEntryControlState(control) != VBP_ENTRY_CACHED ||
            VbpEntryControlIncarnation(control) == 0) {
            return false;
        }
        next = VbpHashRefLoad(&entry->hashNext);
        result->entryChecks++;
        if (VbpEntryControlUsed(control)) {
            uint64 expected = control;

            (void)pg_atomic_compare_exchange_u64(&entry->control, &expected,
                control & ~VBP_ENTRY_USED_BIT);
        } else if (VbpEntryControlReaders(control) == 0) {
            uint32 entryIncarnation = VbpEntryControlIncarnation(control);
            uint64 expected = control;

            if (pg_atomic_compare_exchange_u64(&entry->control, &expected,
                    VbpEntryControlPack(entryIncarnation, VBP_ENTRY_CLOSED, 0))) {
                VbpHashRefStore(link, next);
                VbpHashRefStore(&entry->hashNext, VBP_INVALID_ENTRY_REF);
                (void)pg_atomic_fetch_sub_u32(&vbp->liveEntries, 1);
                result->evicted = current;
                result->incarnation = entryIncarnation;
                return true;
            }
        }
        link = &entry->hashNext;
        current = next;
    }
    return false;
}

static bool VbpHashEvictBucketLocked(const VbpHashEvictBucketContext *context)
{
    if (!VbpHashControlIsRehashing(context->control)) {
        VbpEntryRef *head = &context->oldView->buckets[context->oldBucket];

        return VbpHashEvictChainLocked(context->vbp, head, context->maxEntryChecks, context->result);
    }

    VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(context->runtime,
        VbpHashControlCandidateDesc(context->control));
    bool migrated = false;

    if (candidateDesc == NULL || !VbpHashBitmapIsMigrated(&candidateDesc->view,
            context->oldView->bucketCount, context->oldBucket, &migrated)) {
        return false;
    }
    if (!migrated) {
        VbpEntryRef *head = &context->oldView->buckets[context->oldBucket];

        return VbpHashEvictChainLocked(context->vbp, head, context->maxEntryChecks, context->result);
    }

    VbpEntryRef *low = &candidateDesc->view.buckets[context->oldBucket];
    VbpEntryRef *high = &candidateDesc->view.buckets[
        context->oldBucket + context->oldView->bucketCount];
    bool evicted = VbpHashEvictChainLocked(context->vbp, low, context->maxEntryChecks, context->result);
    if (!evicted && context->result->entryChecks < context->maxEntryChecks) {
        evicted = VbpHashEvictChainLocked(context->vbp, high, context->maxEntryChecks, context->result);
    }
    return evicted;
}

static VbpHashEvictProbeResult VbpHashEvictProbe(VbpHashRuntime *runtime, VbpInstance *vbp,
    uint32 maxEntryChecks, VbpHashEvictResult *result)
{
    uint64 control = VbpHashControlRead(vbp);
    VbpHashViewDesc *oldDesc;
    const VbpHashView *oldView;
    uint32 oldBucket;
    uint32 relativePartition;
    VbpHashPartition *partition;
    VbpHashEvictBucketContext context = {};
    bool evicted;

    if (VbpInstanceState(vbp) != VBP_ACTIVE ||
        !VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
        return VBP_HASH_EVICT_PROBE_ERROR;
    }
    oldDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    if (oldDesc == NULL) {
        return VBP_HASH_EVICT_PROBE_ERROR;
    }
    oldView = &oldDesc->view;
    oldBucket = pg_atomic_fetch_add_u32(&vbp->reclaimBucketCursor, 1) & (oldView->bucketCount - 1);
    relativePartition = VbpHashPartitionForBucket(oldBucket, vbp->partitionCount);
    if (relativePartition == VBP_INVALID_INDEX) {
        return VBP_HASH_EVICT_PROBE_ERROR;
    }
    partition = &runtime->partitionArena[vbp->partitionBase + relativePartition];
    SpinLockAcquire(&partition->lock);
    if (VbpHashControlRead(vbp) != control || VbpInstanceState(vbp) != VBP_ACTIVE) {
        SpinLockRelease(&partition->lock);
        return VBP_HASH_EVICT_PROBE_MISS;
    }
    context.runtime = runtime;
    context.vbp = vbp;
    context.oldView = oldView;
    context.result = result;
    context.control = control;
    context.oldBucket = oldBucket;
    context.maxEntryChecks = maxEntryChecks;
    evicted = VbpHashEvictBucketLocked(&context);
    SpinLockRelease(&partition->lock);
    return evicted ? VBP_HASH_EVICT_PROBE_HIT : VBP_HASH_EVICT_PROBE_MISS;
}

bool VbpHashEvictOne(VbpInstance *vbp, uint32 maxBucketProbes, uint32 maxEntryChecks,
    VbpEntryRef *evicted, uint32 *incarnation)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    VbpHashEvictResult result = {0, VBP_INVALID_ENTRY_REF, 0};

    if (evicted != NULL) {
        *evicted = VBP_INVALID_ENTRY_REF;
    }
    if (incarnation != NULL) {
        *incarnation = 0;
    }
    if (runtime == NULL || vbp == NULL || evicted == NULL || incarnation == NULL ||
        maxBucketProbes == 0 || maxEntryChecks == 0) {
        return false;
    }

    for (uint32 probe = 0; probe < maxBucketProbes && result.entryChecks < maxEntryChecks; ++probe) {
        VbpHashEvictProbeResult probeResult = VbpHashEvictProbe(runtime, vbp, maxEntryChecks, &result);
        if (probeResult == VBP_HASH_EVICT_PROBE_ERROR) {
            return false;
        }
        if (probeResult == VBP_HASH_EVICT_PROBE_HIT) {
            *evicted = result.evicted;
            *incarnation = result.incarnation;
            return true;
        }
    }
    return false;
}

static bool VbpHashMigrateOne(VbpInstance *vbp, uint32 oldBucket)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;

    if (runtime == NULL || vbp == NULL) {
        return false;
    }
    uint64 lifecycle = VbpLifecycleRead(vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE) {
        return false;
    }
    uint32 expectedGeneration = VbpLifecycleGeneration(lifecycle);
    uint64 control = VbpHashControlRead(vbp);

    while (VbpHashControlIsRehashing(control)) {
        VbpHashViewDesc *oldDesc;
        const VbpHashView *oldView;
        uint32 relativePartition;
        VbpHashPartition *partition;

        if (!VbpInstanceLifecycleMatches(vbp, expectedGeneration, VBP_ACTIVE) ||
            !VbpHashControlIsRehashing(control) ||
            !VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
            return false;
        }
        oldDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
        if (oldDesc == NULL) {
            return false;
        }
        oldView = &oldDesc->view;
        if (oldBucket >= oldView->bucketCount) {
            return false;
        }
        relativePartition = VbpHashPartitionForBucket(oldBucket, vbp->partitionCount);
        if (relativePartition == VBP_INVALID_INDEX) {
            return false;
        }
        partition = &runtime->partitionArena[vbp->partitionBase + relativePartition];

        SpinLockAcquire(&partition->lock);
        if (!VbpInstanceLifecycleMatches(vbp, expectedGeneration, VBP_ACTIVE)) {
            SpinLockRelease(&partition->lock);
            return false;
        }
        if (VbpHashControlRead(vbp) != control) {
            SpinLockRelease(&partition->lock);
            control = VbpHashControlRead(vbp);
            continue;
        }
        bool result = VbpHashMigrateBucketLocked(vbp, control, oldBucket);
        SpinLockRelease(&partition->lock);
        return result;
    }
    return false;
}

bool VbpHashMigrateNext(VbpInstance *vbp)
{
    VbpHashRuntime *runtime = g_vbpHashRuntime;
    uint64 lifecycle;
    uint64 control;
    VbpHashViewDesc *oldDesc;
    uint32 oldBucket;

    if (runtime == NULL || vbp == NULL) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE) {
        return false;
    }
    uint32 generation = VbpLifecycleGeneration(lifecycle);
    control = VbpHashControlRead(vbp);
    if (!VbpInstanceLifecycleMatches(vbp, generation, VBP_ACTIVE) ||
        !VbpHashControlIsRehashing(control) || !VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
        return false;
    }
    oldDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    if (oldDesc == NULL) {
        return false;
    }
    oldBucket = pg_atomic_fetch_add_u32(&vbp->migrateCursor, 1) &
        (oldDesc->view.bucketCount - 1);
    return VbpHashMigrateOne(vbp, oldBucket);
}

void VbpHashMigrateAssist(VbpInstance *vbp)
{
    if (vbp == NULL) {
        return;
    }
    if (--g_vbpMigrateAssistTick > 0) {
        return;
    }
    g_vbpMigrateAssistTick = VBP_MIGRATE_ASSIST_STRIDE;
    if (!VbpHashControlIsRehashing(VbpHashControlRead(vbp))) {
        return;
    }
    (void)VbpHashMigrateNext(vbp);
}

static uint32 VbpHashWalkChainLength(VbpInstance *vbp, VbpEntryRef head, bool *truncated)
{
    VbpEntryRef current = head;
    uint32 len = 0;

    *truncated = false;
    while (VbpEntryRefIsValid(current)) {
        VbpEntry *entry;

        if (len >= VBP_HASH_CHAIN_WALK_LIMIT) {
            *truncated = true;
            break;
        }
        entry = VbpResolveEntry(vbp, current, NULL);
        if (entry == NULL) {
            break;
        }
        len++;
        current = VbpHashRefLoad(&entry->hashNext);
    }
    return len;
}

static void VbpHashAccountChainLength(VectorBufferHashChainStat *out, uint32 len, bool truncated)
{
    if (truncated) {
        out->truncated++;
    }
    out->chainedNodes += len;
    if (len > out->maxChain) {
        out->maxChain = len;
    }
    if (len == 0) {
        out->n0++;
    } else if (len == 1) {
        out->n1++;
    } else if (len == VBP_HASH_CHAIN_TWO) {
        out->n2++;
    } else if (len == VBP_HASH_CHAIN_THREE) {
        out->n3++;
    } else {
        out->nGe4++;
    }
}

/*
 * Unlocked sequential scan of the active view. Follows hashNext only and
 * caps each chain at VBP_HASH_CHAIN_WALK_LIMIT. During rehash, migrated
 * buckets have empty active heads, so chainedNodes may lag liveEntries;
 * the histogram is not a performance signal until rehashing=false.
 * migratedBuckets / candidateBucketCount report resize progress (0 when
 * not rehashing).
 */
bool VbpHashCollectChainStat(VbpInstance *vbp, VectorBufferHashChainStat *out)
{
    VbpHashRuntime *runtime;
    uint64 control;
    VbpHashViewDesc *activeDesc;
    const VbpHashView *active;
    if (out == NULL) {
        return false;
    }
    securec_check(memset_s(out, sizeof(*out), 0, sizeof(*out)), "\0", "\0");
    if (vbp == NULL) {
        return false;
    }
    runtime = g_vbpHashRuntime;
    if (runtime == NULL) {
        return false;
    }
    control = VbpHashControlRead(vbp);
    if (!VbpHashInstanceLayoutIsValid(runtime, vbp, control)) {
        return false;
    }
    activeDesc = VbpHashResolveViewDesc(runtime, VbpHashControlActiveDesc(control));
    if (activeDesc == NULL || !VbpHashViewBucketsAreValid(&activeDesc->view)) {
        return false;
    }
    active = &activeDesc->view;
    out->vbpId = vbp->id;
    out->generation = VbpInstanceGeneration(vbp);
    out->liveEntries = pg_atomic_read_u32(&vbp->liveEntries);
    out->bucketCount = active->bucketCount;
    out->rehashing = VbpHashControlIsRehashing(control);
    if (out->rehashing) {
        VbpHashViewDesc *candidateDesc = VbpHashResolveViewDesc(runtime,
            VbpHashControlCandidateDesc(control));

        out->migratedBuckets = pg_atomic_read_u32(&vbp->migratedBuckets);
        out->candidateBucketCount = (candidateDesc == NULL) ? 0 : candidateDesc->view.bucketCount;
    }

    for (uint32 bucket = 0; bucket < active->bucketCount; ++bucket) {
        VbpEntryRef head;
        uint32 len;
        bool truncated = false;

        if ((bucket & VBP_HASH_CHAIN_INTERRUPT_MASK) == 0) {
            CHECK_FOR_INTERRUPTS();
        }
        head = VbpHashRefLoad(&active->buckets[bucket]);
        len = VbpHashWalkChainLength(vbp, head, &truncated);
        VbpHashAccountChainLength(out, len, truncated);
    }
    return true;
}
