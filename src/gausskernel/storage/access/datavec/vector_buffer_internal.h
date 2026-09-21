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
 *        src/gausskernel/storage/access/datavec/vector_buffer_internal.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VECTOR_BUFFER_INTERNAL_H
#define VECTOR_BUFFER_INTERNAL_H

#include "postgres.h"
#include "access/datavec/vector_buffer.h"
#include "storage/item/itemptr.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/relfilenode.h"
#include "storage/spin.h"
#include "utils/hashutils.h"
#include "utils/resowner.h"

#ifdef __cplusplus
template<typename T>
class MpmcBoundedQueue;

extern "C" {
#endif

#define VBP_INVALID_INDEX PG_UINT32_MAX
#define VBP_INVALID_ENTRY_REF UINT64CONST(0)
#define VBP_ENTRY_READER_MASK UINT64CONST(0x1FFFFFFF)
#define VBP_ENTRY_USED_BIT UINT64CONST(0x20000000)
#define VBP_ENTRY_FREE_NEXT_MASK UINT64CONST(0x3FFFFFFF)
#define VBP_ENTRY_STATE_SHIFT 30
#define VBP_ENTRY_STATE_MASK UINT64CONST(0xC0000000)
#define VBP_ENTRY_INCARNATION_SHIFT 32
#define VBP_LIFECYCLE_REF_MASK UINT64CONST(0x3FFFFFFF)
#define VBP_LIFECYCLE_STATE_SHIFT 30U
#define VBP_LIFECYCLE_STATE_MASK UINT64CONST(0xC0000000)
#define VBP_LIFECYCLE_GENERATION_SHIFT 32U
#define VBP_CHUNK_STATE_MASK 0x00000003U
#define VBP_CHUNK_IN_FREELIST_BIT 0x00000004U
#define VBP_CHUNK_ALLOC_REF_ONE 0x00000008U
#define VBP_CHUNK_ALLOC_REF_MASK 0xFFFFFFF8U
#define VBP_HASH_BITMAP_BITS 64
#define VBP_HASH_CONTROL_CANDIDATE_SHIFT 32U
#define VBP_HASH_DESC_CAPACITY_MAX PG_UINT32_MAX
#define VBP_CHUNK_FREELIST_CAPACITY 64U
#define VBP_WORK_RECLAIM_REQUESTED 0x00000001U
#define VBP_WORK_AVAILABLE_OVERFLOW 0x00000002U
#define VBP_PACKED_WORD_SHIFT 32U
#define VBP_HASH_GROWTH_FACTOR 2U

typedef uint32 VbpId;
typedef uint64 VbpEntryRef;

typedef struct VbpRelationKey {
    Oid spcNode;
    Oid dbNode;
    Oid relNode;
} VbpRelationKey;

typedef struct VbpPayloadLayout {
    uint32 payloadLen;
    uint32 slotSize;
    uint32 slotsPerChunk;
} VbpPayloadLayout;

static inline VbpRelationKey VbpMakeRelationKey(const RelFileNode *rnode)
{
    VbpRelationKey key;

    key.spcNode = rnode->spcNode;
    key.dbNode = rnode->dbNode;
    key.relNode = rnode->relNode;
    return key;
}

static inline uint32 VbpHashTid(const ItemPointerData *tid)
{
    uint32 hash = murmurhash32(ItemPointerGetBlockNumberNoCheck(tid));
    return hash_combine(hash,
        murmurhash32((uint32)ItemPointerGetOffsetNumberNoCheck(tid)));
}

static inline uint32 VbpHashPartitionForBucket(uint32 bucket, uint32 partitionCount)
{
    Assert(partitionCount != 0 && (partitionCount & (partitionCount - 1)) == 0);
    if (partitionCount == 0 || (partitionCount & (partitionCount - 1)) != 0) {
        return VBP_INVALID_INDEX;
    }
    return bucket & (partitionCount - 1);
}

static inline bool VbpEntryRefIsValid(VbpEntryRef ref)
{
    return ref != VBP_INVALID_ENTRY_REF && (uint32)(ref >> VBP_PACKED_WORD_SHIFT) != 0 && (uint32)ref != 0;
}

static inline VbpEntryRef VbpMakeEntryRef(uint32 chunkIndex, uint32 slotIndex)
{
    Assert(chunkIndex != VBP_INVALID_INDEX);
    Assert(slotIndex != VBP_INVALID_INDEX);
    if (chunkIndex == VBP_INVALID_INDEX || slotIndex == VBP_INVALID_INDEX) {
        return VBP_INVALID_ENTRY_REF;
    }
    return ((uint64)(chunkIndex + 1) << VBP_PACKED_WORD_SHIFT) | (uint64)(slotIndex + 1);
}

static inline uint32 VbpEntryRefChunk(VbpEntryRef ref)
{
    Assert(VbpEntryRefIsValid(ref));
    if (!VbpEntryRefIsValid(ref)) {
        return VBP_INVALID_INDEX;
    }
    return (uint32)(ref >> VBP_PACKED_WORD_SHIFT) - 1;
}

static inline uint32 VbpEntryRefSlot(VbpEntryRef ref)
{
    Assert(VbpEntryRefIsValid(ref));
    if (!VbpEntryRefIsValid(ref)) {
        return VBP_INVALID_INDEX;
    }
    return (uint32)ref - 1;
}

static inline uint64 VbpMakeChunkToken(uint32 publicationEpochLow, uint32 chunkIndex)
{
    Assert(chunkIndex != VBP_INVALID_INDEX);
    if (chunkIndex == VBP_INVALID_INDEX) {
        return 0;
    }
    return ((uint64)publicationEpochLow << VBP_PACKED_WORD_SHIFT) | (uint64)(chunkIndex + 1);
}

static inline uint32 VbpChunkTokenEpoch(uint64 token)
{
    return (uint32)(token >> VBP_PACKED_WORD_SHIFT);
}

static inline uint32 VbpChunkTokenIndex(uint64 token)
{
    uint32 encoded = (uint32)token;

    return encoded == 0 ? VBP_INVALID_INDEX : encoded - 1;
}

typedef enum VbpState {
    VBP_FREE = 0,
    VBP_ACTIVE,
    VBP_DETACHED,
    VBP_CORRUPT
} VbpState;

static inline uint64 VbpLifecycleControlPack(uint32 generation, VbpState state, uint32 refs)
{
    Assert(generation != 0);
    Assert(state <= VBP_CORRUPT);
    Assert(refs <= VBP_LIFECYCLE_REF_MASK);
    return ((uint64)generation << VBP_LIFECYCLE_GENERATION_SHIFT) |
        ((uint64)state << VBP_LIFECYCLE_STATE_SHIFT) | (uint64)refs;
}

static inline uint32 VbpLifecycleGeneration(uint64 control)
{
    return (uint32)(control >> VBP_LIFECYCLE_GENERATION_SHIFT);
}

static inline VbpState VbpLifecycleState(uint64 control)
{
    return (VbpState)((control & VBP_LIFECYCLE_STATE_MASK) >> VBP_LIFECYCLE_STATE_SHIFT);
}

static inline uint32 VbpLifecycleRefs(uint64 control)
{
    return (uint32)(control & VBP_LIFECYCLE_REF_MASK);
}

typedef enum VbpResizeClaim {
    VBP_RESIZE_NONE = 0,
    VBP_RESIZE_OWNER,
    VBP_RESIZE_DETACH_GATE
} VbpResizeClaim;

/*
 * FREE: allocator-reusable; the low 30 bits store free-stack next, not readers
 * CLOSED: externally owned when readers == 0; retired/draining when readers > 0
 * CACHED: hash-visible and open to new pins, readers >= 0
 * QUARANTINED: isolated and not allocator-reusable, readers == 0
 */
typedef enum VbpEntryState {
    VBP_ENTRY_FREE = 0,
    VBP_ENTRY_CLOSED,
    VBP_ENTRY_CACHED,
    VBP_ENTRY_QUARANTINED
} VbpEntryState;

typedef enum VbpChunkState {
    VBP_CHUNK_FREE = 0,
    VBP_CHUNK_ACTIVE,
    VBP_CHUNK_DRAINING,
    VBP_CHUNK_CLAIMED
} VbpChunkState;

static inline uint64 VbpEntryControlPack(uint32 incarnation, VbpEntryState state, uint32 readers)
{
    Assert(readers <= VBP_ENTRY_READER_MASK);
    Assert(state <= VBP_ENTRY_QUARANTINED);
    return ((uint64)incarnation << VBP_ENTRY_INCARNATION_SHIFT) |
        ((uint64)state << VBP_ENTRY_STATE_SHIFT) | (uint64)readers;
}

static inline uint64 VbpEntryFreeControlPack(uint32 incarnation, uint32 nextIndexPlusOne)
{
    Assert(nextIndexPlusOne <= VBP_ENTRY_FREE_NEXT_MASK);
    return ((uint64)incarnation << VBP_ENTRY_INCARNATION_SHIFT) | (uint64)nextIndexPlusOne;
}

static inline uint32 VbpEntryControlIncarnation(uint64 control)
{
    return (uint32)(control >> VBP_ENTRY_INCARNATION_SHIFT);
}

static inline VbpEntryState VbpEntryControlState(uint64 control)
{
    return (VbpEntryState)((control & VBP_ENTRY_STATE_MASK) >> VBP_ENTRY_STATE_SHIFT);
}

static inline uint32 VbpEntryControlReaders(uint64 control)
{
    return (uint32)(control & VBP_ENTRY_READER_MASK);
}

static inline bool VbpEntryControlUsed(uint64 control)
{
    return (control & VBP_ENTRY_USED_BIT) != 0;
}

static inline uint32 VbpEntryControlFreeNext(uint64 control)
{
    return (uint32)(control & VBP_ENTRY_FREE_NEXT_MASK);
}

static inline uint64 VbpEntryControlWithUsed(uint64 control)
{
    return control | VBP_ENTRY_USED_BIT;
}

static inline bool VbpEntryControlIsFree(uint64 control)
{
    return VbpEntryControlState(control) == VBP_ENTRY_FREE;
}

static inline bool VbpEntryControlIsOwned(uint64 control)
{
    return VbpEntryControlState(control) == VBP_ENTRY_CLOSED && VbpEntryControlReaders(control) == 0;
}

static inline bool VbpEntryControlCanPin(uint64 control)
{
    return VbpEntryControlState(control) == VBP_ENTRY_CACHED;
}

static inline uint32 VbpNextEntryIncarnation(uint32 incarnation)
{
    incarnation++;
    return incarnation == 0 ? 1 : incarnation;
}

typedef struct VbpTaggedHead {
    volatile uint64 value;
} VbpTaggedHead;

typedef struct VbpMemChunk {
    volatile uint32 control;
    volatile uint64 publicationEpoch;
    VbpId vbpId;
    uint64 entryBase;
    uint32 vbpGeneration;
    VbpTaggedHead freeEntries;
    volatile uint32 nFree;
    /* Owned-chunk list links; read/write only under the owning VBP's chunkLock. */
    uint32 clPrev;
    uint32 clNext;
    /* Atomic link used only while the FREE chunk is on the global freelist stack. */
    volatile uint32 globalFreeNext;
} VbpMemChunk;

typedef struct VbpEntry {
    volatile uint64 control;
    ItemPointerData payloadTid; /* lookup key: payload-page ItemPointer, not the graph tid */
    /* Read/written with VbpHashRefLoad/Store; must stay naturally aligned uint64. */
    volatile VbpEntryRef hashNext;
} VbpEntry;

static inline char *VbpEntryPayload(VbpEntry *entry)
{
    return entry == NULL ? NULL : (char *)entry + MAXALIGN(sizeof(VbpEntry));
}

typedef struct VbpHashView {
    VbpEntryRef *buckets;
    volatile uint64 *migratedBitmap;
    uint32 bucketCount;
} VbpHashView;

/* listNext links the free list while empty, or older views after publication. */
typedef struct VbpHashViewDesc {
    VbpHashView view;
    uint32 listNext;
} VbpHashViewDesc;

typedef struct VbpHashPartition {
    alignas(PG_CACHE_LINE_SIZE) slock_t lock;
} VbpHashPartition;

typedef struct VbpInstance {
    volatile uint64 lifecycleControl;
    volatile uint64 hashControl;
    volatile uint32 liveEntries;
    volatile uint32 migratedBuckets;
    volatile uint32 resizeClaim;
    uint32 partitionBase;
    uint32 partitionCount;
    VbpId id;
    VbpRelationKey relationKey;
    VbpPayloadLayout payloadLayout;
    uint32 directoryNext;
    uint32 freeNext;
    LWLock chunkLock;
    MpmcBoundedQueue<uint64> *chunkFreelist;
    volatile uint32 reclaimBucketCursor;
    volatile uint32 migrateCursor;
    /* Reclaim and exceptional available-queue repair requests. */
    volatile uint32 workFlags;
    /* All chunks owned by this VBP; read/write clHead only under chunkLock. */
    uint32 clHead;
    volatile uint32 deferredNext;
} VbpInstance;

static inline uint64 VbpLifecycleRead(const VbpInstance *vbp)
{
    Assert(vbp != NULL);
    return vbp == NULL ? 0 : pg_atomic_read_u64((volatile uint64 *)&vbp->lifecycleControl);
}

static inline uint32 VbpInstanceGeneration(const VbpInstance *vbp)
{
    return VbpLifecycleGeneration(VbpLifecycleRead(vbp));
}

static inline VbpState VbpInstanceState(const VbpInstance *vbp)
{
    return VbpLifecycleState(VbpLifecycleRead(vbp));
}

static inline bool VbpInstanceLifecycleMatches(const VbpInstance *vbp, uint32 generation, VbpState state)
{
    uint64 lifecycle;

    if (vbp == NULL || generation == 0 || state > VBP_CORRUPT) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    return VbpLifecycleState(lifecycle) == state &&
        VbpLifecycleGeneration(lifecycle) == generation;
}

/* Backend-local attachment only; never place these pointers in shared memory. */
typedef struct VbpHashRuntime {
    VbpHashPartition *partitionArena;
    uint32 partitionCapacity;
    VbpHashViewDesc *viewDescArena;
    uint32 viewDescCapacity;
    VbpMemChunk *chunkArena;
    uint32 chunkCount;
    uint32 chunkSize;
    char *entryArena;
    Size entryArenaSize;
} VbpHashRuntime;

typedef struct VbpHashInitConfig {
    VbpId id;
    uint32 generation;
    uint32 partitionBase;
    uint32 partitionCount;
    uint32 initialDesc;
} VbpHashInitConfig;

typedef struct VbpHashPublishResult {
    VbpEntryRef winner;
    const char *payload;
    bool published;
} VbpHashPublishResult;

typedef struct VbpHashInvalidateResult {
    VbpEntryRef removed;
    uint32 incarnation;
    bool recycleOwned;
} VbpHashInvalidateResult;

typedef enum VbpAccessMode {
    VBP_ACCESS_IDLE = 0,
    VBP_ACCESS_RESERVED,
    VBP_ACCESS_CACHED_PIN,
    VBP_ACCESS_BORROWED_PIN
} VbpAccessMode;

typedef struct VbpAccessEntryOwnership {
    VbpEntryRef ref;
    VbpEntry *entry;
    uint32 incarnation;
} VbpAccessEntryOwnership;

typedef struct VbpAccessBorrowedPin {
    VectorBufferLoadGuard guard;
    VectorBufferLoadEndFn end;
} VbpAccessBorrowedPin;

typedef union VbpAccessState {
    VbpAccessEntryOwnership entry;
    VbpAccessBorrowedPin borrowed;
} VbpAccessState;

/* The public header will keep this type opaque; these fields are backend-local. */
struct VectorBufferAccess {
    VbpAccessState state;
    uint64 pinCookie;
    uint64 statHits;
    uint64 statMisses;
    uint64 statInstalls;
    uint64 statFallbacks;
    uint64 statEvictions;
    ResourceOwner owner;
    MemoryContext accessContext;
    VbpInstance *vbp;
    uint32 vbpGeneration;
    uint32 payloadLen;
    VbpAccessMode mode;
};

static inline void VbpAccessSetIdle(VectorBufferAccess *access)
{
    access->state.entry.ref = VBP_INVALID_ENTRY_REF;
    access->state.entry.entry = NULL;
    access->state.entry.incarnation = 0;
    access->mode = VBP_ACCESS_IDLE;
    access->pinCookie = 0;
}

static inline void VbpAccessSetEntry(VectorBufferAccess *access, VbpEntryRef ref, VbpEntry *entry,
    uint32 incarnation, VbpAccessMode mode)
{
    Assert(mode == VBP_ACCESS_RESERVED || mode == VBP_ACCESS_CACHED_PIN);
    Assert(entry != NULL);
    access->state.entry.ref = ref;
    access->state.entry.entry = entry;
    access->state.entry.incarnation = incarnation;
    access->mode = mode;
}

static inline uint64 VbpPackTaggedIndex(uint32 indexPlusOne, uint32 version)
{
    return ((uint64)version << VBP_PACKED_WORD_SHIFT) | (uint64)indexPlusOne;
}

static inline uint32 VbpTaggedIndex(uint64 value)
{
    return (uint32)value;
}

static inline uint32 VbpTaggedVersion(uint64 value)
{
    return (uint32)(value >> VBP_PACKED_WORD_SHIFT);
}

static inline uint64 VbpTaggedHeadRead(const VbpTaggedHead *head)
{
    Assert(head != NULL);
    if (head == NULL) {
        return VbpPackTaggedIndex(0, 0);
    }
    return pg_atomic_barrier_read_u64((volatile uint64 *)&head->value);
}

static inline bool VbpTaggedHeadCompareExchange(VbpTaggedHead *head, uint64 *expected, uint64 desired)
{
    Assert(head != NULL);
    Assert(expected != NULL);
    if (head == NULL || expected == NULL) {
        return false;
    }
    /* The openGauss CAS is a full barrier and updates expected on failure. */
    return pg_atomic_compare_exchange_u64(&head->value, expected, desired);
}

static inline uint32 VbpChunkControlRead(const VbpMemChunk *chunk)
{
    Assert(chunk != NULL);
    if (chunk == NULL) {
        return VBP_CHUNK_FREE;
    }
    return __atomic_load_n((volatile uint32 *)&chunk->control, __ATOMIC_ACQUIRE);
}

static inline uint64 VbpChunkPublicationEpochRead(const VbpMemChunk *chunk)
{
    if (chunk == NULL) {
        return 0;
    }
    return pg_atomic_barrier_read_u64((volatile uint64 *)&chunk->publicationEpoch);
}

static inline bool VbpChunkTokenMatches(const VbpMemChunk *chunk, uint32 chunkIndex, uint64 token)
{
    return chunk != NULL && chunkIndex != VBP_INVALID_INDEX &&
        VbpChunkTokenIndex(token) == chunkIndex &&
        (uint32)VbpChunkPublicationEpochRead(chunk) == VbpChunkTokenEpoch(token);
}

static inline void VbpChunkReleaseAllocatorRef(VbpMemChunk *chunk)
{
    uint32 oldControl;

    Assert(chunk != NULL);
    if (chunk == NULL) {
        return;
    }
    oldControl = pg_atomic_fetch_sub_u32(&chunk->control, VBP_CHUNK_ALLOC_REF_ONE);
    Assert((oldControl & VBP_CHUNK_ALLOC_REF_MASK) >= VBP_CHUNK_ALLOC_REF_ONE);
    if ((oldControl & VBP_CHUNK_ALLOC_REF_MASK) < VBP_CHUNK_ALLOC_REF_ONE) {
        (void)pg_atomic_fetch_add_u32(&chunk->control, VBP_CHUNK_ALLOC_REF_ONE);
    }
}

static inline uint32 VbpHashControlActiveDesc(uint64 control)
{
    uint32 encoded = (uint32)control;

    return encoded == 0 ? VBP_INVALID_INDEX : encoded - 1;
}

static inline uint32 VbpHashControlCandidateDesc(uint64 control)
{
    uint32 encoded = (uint32)(control >> VBP_HASH_CONTROL_CANDIDATE_SHIFT);

    return encoded == 0 ? VBP_INVALID_INDEX : encoded - 1;
}

extern void VbpTaggedHeadInit(VbpTaggedHead *head);
extern void VbpChunkInit(VbpMemChunk *chunk, VbpChunkState state);
extern bool VbpChunkTryAcquireAllocatorRef(VbpMemChunk *chunk);
extern bool VbpChunkTryBeginDrain(VbpMemChunk *chunk);
extern bool VbpChunkAllocatorRefsDrained(const VbpMemChunk *chunk);
extern bool VbpFreeEntryStackPush(VbpTaggedHead *head, char *slotBase, Size slotSize, uint32 slotCount,
    uint32 slotIndex);
extern bool VbpFreeEntryStackPop(VbpTaggedHead *head, char *slotBase, Size slotSize, uint32 slotCount,
    uint32 *slotIndex);

/* Called only while creating a new shared partition arena, before it is published. */
extern void VbpHashPartitionArenaInit(VbpHashPartition *partitionArena, uint32 partitionCapacity);
extern bool VbpHashRuntimeInit(VbpHashRuntime *runtime, const VbpHashRuntime *config);
extern void VbpHashRuntimeAttach(VbpHashRuntime *runtime);
extern void VbpHashRuntimeDetach(VbpHashRuntime *runtime);
extern bool VbpHashInit(VbpInstance *vbp, const VbpHashInitConfig *config);
extern bool VbpHashNeedsResize(const VbpInstance *vbp);
extern bool VbpHashStartResize(VbpInstance *vbp, uint32 candidateDesc);
extern bool VbpHashMigrateNext(VbpInstance *vbp);
/* Hit-path assist: stride-gated, at most one bucket. No-op when not rehashing. */
extern void VbpHashMigrateAssist(VbpInstance *vbp);
/* Allocate before publishing resize state; release a loser only after leaving VBP/hash locks. */
extern bool VbpHashAllocateResizeCandidate(VbpInstance *vbp, uint32 *candidateDesc);
extern bool VbpHashReleaseUnpublishedView(VbpInstance *vbp, uint32 descIndex);

/*
 * VBP lock contract for these helpers and their vector_buffer.cpp callers:
 * 1. A directory LWLock never nests with another VBP lock.  The sole outer-lock
 *    exception is payload-page exclusive -> directory shared while invalidation
 *    takes a transient scan ref; release directory before taking a bucket lock.
 * 2. Ownership/hash-arena LWLocks and resize ownership never overlap a bucket
 *    spinlock or payload-page content lock.
 * 3. A payload-page content lock may precede at most one bucket spinlock;
 *    bucket -> payload-page is forbidden.
 * 4. A bucket spinlock body performs only bounded shared-memory metadata work
 *    and atomics: no I/O, allocation/free, payload copy, distance work,
 *    interrupt checks, or ERROR-capable helpers.
 * 5. ResourceOwner no-forget shared cleanup uses only CAS and lock-free
 *    stack/queue operations; it takes no directory, ownership, bucket, or
 *    payload-page lock.
 *
 * Pin/publish require a live access scan ref and exact VBP pointer/id/generation.
 * Hit (TryPin) validates identity without holding the partition spinlock; Publish/
 * Evict/Invalidate still revalidate under the spinlock while mutating bucket topology.
 * Bucket heads and entry->hashNext are accessed with atomic 64-bit load/store.
 */
extern bool VbpHashTryPin(VbpInstance *vbp, const ItemPointerData *tid,
    VectorBufferAccess *access, VbpEntryRef *entryRef, const char **payload);
extern bool VbpHashPublishReserved(VbpInstance *vbp, const ItemPointerData *tid,
    VbpEntryRef reserved, VectorBufferAccess *access, VbpHashPublishResult *result);
extern bool VbpHashInvalidatePayload(VbpInstance *vbp, uint32 generation,
    const ItemPointerData *tid, VbpHashInvalidateResult *result);
extern bool VbpHashDrainDetached(VbpInstance *vbp, uint32 generation);
extern bool VbpHashEvictOne(VbpInstance *vbp, uint32 maxBucketProbes, uint32 maxEntryChecks,
    VbpEntryRef *evicted, uint32 *incarnation);
extern bool VbpHashCollectChainStat(VbpInstance *vbp, VectorBufferHashChainStat *out);
extern VbpEntry *VbpResolveEntry(VbpInstance *vbp, VbpEntryRef ref, VbpMemChunk **resolvedChunk);
extern uint64 VbpNextPinCookie(void);
/*
 * Recycle a CLOSED entry back to FREE (or QUARANTINED on push failure).
 */
extern void VbpRecycleEntryOwned(VbpInstance *vbp, VbpEntryRef ref, uint32 incarnation,
    VectorBufferAccess *access);
extern void VbpReleaseReservation(VectorBufferAccess *access);

#ifdef __cplusplus
}
#endif

#endif /* VECTOR_BUFFER_INTERNAL_H */
