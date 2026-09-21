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
 *        src/gausskernel/storage/access/datavec/vector_buffer.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <new>

#include "component/thread/mpmcqueue.h"
#include "gssignal/gs_signal.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/atomic.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "vector_buffer_internal.h"
#include "access/datavec/vector_buffer.h"

#define VBP_SHMEM_NAME "Vector Buffer"
#define VBP_MAGIC 0x56425032U
#define VBP_DEFERRED_STACK_POP_CAS_LIMIT 1024U
#define VBP_DEFERRED_SAFE_POINT_LIMIT 1U
#define VBP_DESTROY_ALLOCATOR_WAIT_LIMIT 1024U
#define VBP_DETACH_RESIZE_WAIT_LIMIT 1024U
#define VBP_BYTES_PER_KB 1024U
#define VBP_MIN_BUCKET_COUNT 16U
#define VBP_RECLAIM_ATTEMPT_MULTIPLIER 2U
#define VBP_RECLAIM_BUCKET_PROBES 8U
#define VBP_MEMORY_CONTEXT_SLACK (8U * 1024U * 1024U)
#define VBP_HASH_VIEW_CHAIN_MAX 32U
#define VBP_MAX_POWER_OF_TWO_32 ((Size)(PG_UINT32_MAX >> 1) + 1)

typedef struct VbpSharedLayout {
    Size totalSize;
    Size chunksOffset;
    Size payloadDataOffset;
} VbpSharedLayout;

typedef struct VectorBufferCtl {
    uint32 magic;
    uint32 chunkSize;
    uint32 chunkCount;
    uint32 maxVbps;
    uint32 minPayload;
    uint32 reclaimScanLimit;
    uint32 directoryLockCount;
    uint32 directoryBucketCount;
    uint32 hashPartitionStride;
    uint32 hashPartitionCapacity;
    uint32 hashBucketCapacity;
    uint32 hashViewDescCapacity;
    uint32 hashViewFreeHead;
    uint32 vbpFreeHead;
    VbpTaggedHead globalFreeChunkHead;
    VbpTaggedHead deferredDestroyHead;
    uint64 configuredCapacityBytes;
    uint64 capacityBytes;
    LWLock hashViewLock;
    VectorBufferStats stats;
    /* Cold reclaim: a miss sets the latch; the VBP_RECLAIM thread waits on it. */
    Latch reclaimLatch;
    volatile bool reclaimLatchInited;
    /* True only while the reclaim util thread owns the latch (SetLatch-safe). */
    volatile bool reclaimLatchOwned;
} VectorBufferCtl;

typedef struct VbpSharedConfig {
    Size configuredCapacityBytes;
    Size capacityBytes;
    uint32 chunkSize;
    uint32 chunkCount;
    uint32 maxVbps;
    uint32 maxSlotsPerChunk;
    uint32 maxSlotCount;
    uint32 minPayload;
    uint32 reclaimScanLimit;
    uint32 directoryLockCount;
    uint32 directoryBucketCount;
    uint32 hashPartitionStride;
    uint32 hashPartitionCapacity;
    uint32 hashBucketCapacity;
    uint32 hashViewDescCapacity;
    VbpSharedLayout layout;
    Size metadataSize;
} VbpSharedConfig;

typedef struct VbpSharedConfigInput {
    Size capacityBytes;
    Size chunkBytes;
    uint32 hashPartitions;
    uint32 minPayload;
    uint32 reclaimScanLimit;
} VbpSharedConfigInput;

typedef enum VbpDirectoryResult {
    VBP_DIRECTORY_MISSING = 0,
    VBP_DIRECTORY_ACQUIRED,
    VBP_DIRECTORY_LAYOUT_MISMATCH,
    VBP_DIRECTORY_REFCOUNT_EXHAUSTED
} VbpDirectoryResult;

typedef enum VbpDetachResult {
    VBP_DETACH_MISSING = 0,
    VBP_DETACH_DONE,
    VBP_DETACH_REFCOUNT_EXHAUSTED,
    VBP_DETACH_RESIZE_BUSY,
    VBP_DETACH_CORRUPT
} VbpDetachResult;

typedef enum VbpDetachGateResult {
    VBP_DETACH_GATE_ACQUIRED = 0,
    VBP_DETACH_GATE_BUSY,
    VBP_DETACH_GATE_CORRUPT
} VbpDetachGateResult;

typedef struct VbpDirectoryLookup {
    VbpId id;
    uint32 existingPayloadLen;
} VbpDirectoryLookup;

typedef struct VbpPoppedSlot {
    uint64 publicationEpoch;
    uint32 chunkIndex;
    char *slotBase;
    uint32 slotIndex;
    VbpEntry *entry;
    uint64 originalFreeControl;
} VbpPoppedSlot;

typedef struct VbpMissRequest {
    VectorBufferAccess *access;
    const ItemPointerData *payloadTid;
    const VectorBufferLoadOps *loadOps;
    void *loaderCtx;
    VectorBufferHandle *handle;
} VbpMissRequest;

typedef struct VbpReservedMissState {
    VectorBufferLoadGuard guard;
    VbpHashPublishResult publish;
    bool guardActive;
    bool guardEndCalled;
} VbpReservedMissState;

typedef MpmcBoundedQueue<uint64> VbpChunkFreelist;

static VectorBufferCtl *g_vbpCtl = NULL;
static MemoryContext g_vbpMemoryContext = NULL;
static LWLock *g_vbpDirectoryLocks = NULL;
static uint32 *g_vbpDirectoryBuckets = NULL;
static VbpInstance *g_vbpInstances = NULL;
static VbpHashPartition *g_vbpPartitions = NULL;
static VbpHashViewDesc *g_vbpHashViewDescs = NULL;
static VbpMemChunk *g_vbpChunks = NULL;
static char *g_vbpPayloadData = NULL;

static VbpHashRuntime g_vbpHashRuntime;
static bool g_vbpHashRuntimeReady = false;

static uint32 VectorBufferProcessDeferredDestroys(uint32 limit);
static void VbpReleaseBorrowedPin(VectorBufferAccess *access, bool isCommit = true);

static void VbpChunkFreelistCleanup(int code, Datum arg)
{
    (void)code;
    (void)arg;

    if (g_vbpCtl == NULL || g_vbpInstances == NULL) {
        return;
    }
    for (uint32 index = 0; index < g_vbpCtl->maxVbps; ++index) {
        VbpChunkFreelist *freelist = g_vbpInstances[index].chunkFreelist;

        if (freelist == NULL) {
            continue;
        }
        freelist->~VbpChunkFreelist();
        pfree(freelist);
        g_vbpInstances[index].chunkFreelist = NULL;
    }
}

static bool VbpPayloadSlotSize(uint32 payloadLen, uint32 *slotSize)
{
    Size headerSize;
    Size payloadSize;
    Size total;

    if (payloadLen == 0 || slotSize == NULL) {
        return false;
    }
    headerSize = MAXALIGN(sizeof(VbpEntry));
    payloadSize = MAXALIGN((Size)payloadLen);
    total = add_size(headerSize, payloadSize);
    if (total > PG_UINT32_MAX) {
        return false;
    }
    *slotSize = (uint32)total;
    return true;
}

static Size VbpLayoutAppendArray(Size *size, Size count, Size elementSize)
{
    Size offset;
    Size bytes;

    *size = MAXALIGN(*size);
    offset = *size;
    bytes = mul_size(count, elementSize);
    *size = add_size(*size, bytes);
    return offset;
}

static void VbpComputeSharedLayout(const VbpSharedConfig *config, VbpSharedLayout *layout)
{
    Size size = 0;

    layout->chunksOffset = VbpLayoutAppendArray(&size, config->chunkCount, sizeof(VbpMemChunk));
    size = TYPEALIGN(config->chunkSize, size);
    layout->payloadDataOffset = size;
    size = add_size(size, config->capacityBytes);
    layout->totalSize = size;
}

static Size VbpComputeMetadataSize(const VbpSharedConfig *config)
{
    Size size = MAXALIGN(sizeof(VectorBufferCtl));

    (void)VbpLayoutAppendArray(&size, config->directoryLockCount, sizeof(LWLock));
    (void)VbpLayoutAppendArray(&size, config->directoryBucketCount, sizeof(uint32));
    (void)VbpLayoutAppendArray(&size, config->maxVbps, sizeof(VbpInstance));
    size = TYPEALIGN(alignof(VbpHashPartition), size);
    size = add_size(size, mul_size((Size)config->hashPartitionCapacity, sizeof(VbpHashPartition)));
    (void)VbpLayoutAppendArray(&size, config->hashViewDescCapacity, sizeof(VbpHashViewDesc));
    return size;
}

static bool VbpConfigurePayloadArena(Size capacityBytes, Size chunkBytes, uint32 minPayload,
    uint32 reclaimScanLimit, VbpSharedConfig *config)
{
    Size normalizedChunk;
    Size normalizedMinPayload;
    Size chunkCount;
    Size maxSlotCount;
    uint32 minSlotSize;

    if (capacityBytes == 0) {
        return false;
    }

    normalizedChunk = chunkBytes == 0 ?
        mul_size((Size)VECTOR_BUFFER_DEFAULT_CHUNK_SIZE_KB, (Size)VBP_BYTES_PER_KB) : chunkBytes;
    normalizedChunk = MAXALIGN(normalizedChunk);
    if (normalizedChunk == 0 || normalizedChunk > PG_UINT32_MAX ||
        (normalizedChunk & (normalizedChunk - 1)) != 0) {
        return false;
    }

    normalizedMinPayload = MAXALIGN(
        minPayload == 0 ? (Size)VECTOR_BUFFER_DEFAULT_MIN_PAYLOAD : (Size)minPayload);
    if (normalizedMinPayload == 0 || normalizedMinPayload > PG_UINT32_MAX ||
        !VbpPayloadSlotSize((uint32)normalizedMinPayload, &minSlotSize) || minSlotSize == 0) {
        return false;
    }

    chunkCount = capacityBytes / normalizedChunk;
    if (chunkCount == 0 || chunkCount > PG_UINT32_MAX) {
        return false;
    }
    config->configuredCapacityBytes = capacityBytes;
    config->capacityBytes = mul_size(chunkCount, normalizedChunk);
    config->chunkSize = (uint32)normalizedChunk;
    config->chunkCount = (uint32)chunkCount;
    config->maxVbps = config->chunkCount;
    config->minPayload = (uint32)normalizedMinPayload;
    config->reclaimScanLimit = reclaimScanLimit == 0 ?
        VECTOR_BUFFER_DEFAULT_RECLAIM_SCAN_LIMIT : reclaimScanLimit;
    config->maxSlotsPerChunk = config->chunkSize / minSlotSize;
    if (config->maxSlotsPerChunk == 0) {
        return false;
    }
    maxSlotCount = mul_size((Size)config->chunkCount, (Size)config->maxSlotsPerChunk);
    if (maxSlotCount > PG_UINT32_MAX) {
        return false;
    }
    config->maxSlotCount = (uint32)maxSlotCount;
    return true;
}

static bool VbpConfigureHashBuckets(uint32 hashPartitions, VbpSharedConfig *config)
{
    Size partitionCount = hashPartitions == 0 ?
        (Size)VECTOR_BUFFER_DEFAULT_HASH_PARTITIONS : (Size)hashPartitions;
    Size desired;

    if (partitionCount == 0 || partitionCount > VBP_MAX_POWER_OF_TWO_32) {
        return false;
    }
    config->hashPartitionStride = pg_nextpower2_32((uint32)partitionCount);

    desired = mul_size((Size)config->maxVbps, (Size)VBP_HASH_GROWTH_FACTOR);
    if (desired < VBP_MIN_BUCKET_COUNT) {
        desired = VBP_MIN_BUCKET_COUNT;
    }
    if (desired > VBP_MAX_POWER_OF_TWO_32) {
        return false;
    }
    config->directoryBucketCount = pg_nextpower2_32((uint32)desired);
    config->directoryLockCount = Min(config->hashPartitionStride, config->directoryBucketCount);

    desired = mul_size((Size)config->maxSlotCount, (Size)VBP_HASH_GROWTH_FACTOR);
    if (desired < VBP_MIN_BUCKET_COUNT) {
        desired = VBP_MIN_BUCKET_COUNT;
    }
    if (desired > VBP_MAX_POWER_OF_TWO_32) {
        return false;
    }
    config->hashBucketCapacity = pg_nextpower2_32((uint32)desired);
    return true;
}

static bool VbpConfigureHashViews(VbpSharedConfig *config)
{
    Size viewDescChainBound;
    uint32 minimumBucketCount = Min(config->hashPartitionStride, config->hashBucketCapacity);
    uint32 viewCountPerVbp;

    if (minimumBucketCount == 0) {
        return false;
    }
    viewCountPerVbp = (uint32)pg_leftmost_one_pos32(
        config->hashBucketCapacity / minimumBucketCount) + 1;
    viewDescChainBound = mul_size((Size)config->maxVbps, (Size)viewCountPerVbp);
    if (viewDescChainBound == 0 || viewDescChainBound > VBP_HASH_DESC_CAPACITY_MAX) {
        return false;
    }
    config->hashViewDescCapacity = (uint32)viewDescChainBound;
    return true;
}

static bool VbpConfigureHashPartitions(VbpSharedConfig *config)
{
    Size partitionCapacity;

    partitionCapacity = mul_size((Size)config->maxVbps, (Size)config->hashPartitionStride);
    if (partitionCapacity > PG_UINT32_MAX) {
        return false;
    }
    config->hashPartitionCapacity = (uint32)partitionCapacity;
    return true;
}

static bool VbpBuildSharedConfig(const VbpSharedConfigInput *input, VbpSharedConfig *config)
{
    if (input == NULL || config == NULL) {
        return false;
    }
    securec_check(memset_s(config, sizeof(VbpSharedConfig), 0, sizeof(VbpSharedConfig)), "\0", "\0");
    if (!VbpConfigurePayloadArena(input->capacityBytes, input->chunkBytes, input->minPayload,
            input->reclaimScanLimit, config) ||
        !VbpConfigureHashBuckets(input->hashPartitions, config) || !VbpConfigureHashViews(config) ||
        !VbpConfigureHashPartitions(config)) {
        return false;
    }

    VbpComputeSharedLayout(config, &config->layout);
    config->metadataSize = VbpComputeMetadataSize(config);
    return true;
}

static void *VbpMetadataAlloc(Size count, Size elementSize, Size alignment)
{
    Size bytes = mul_size(count, elementSize);
    Size allocBytes = add_size(bytes, alignment - 1);
    char *raw = (char *)MemoryContextAllocExtended(g_vbpMemoryContext, allocBytes,
        MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    uintptr_t aligned;

    if (raw == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
            errmsg("could not allocate vector buffer metadata"),
            errdetail("Failed to allocate %zu bytes from VectorBufferMemoryContext.", bytes)));
    }
    Assert(alignment != 0 && (alignment & (alignment - 1)) == 0);
    aligned = TYPEALIGN(alignment, (uintptr_t)raw);
    return (void *)aligned;
}

static VectorBufferCtl *VbpCreateMetadata(const VbpSharedConfig *config)
{
    MemoryContext parent = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    Size contextLimit = add_size(config->metadataSize,
        add_size(config->capacityBytes / 4, (Size)VBP_MEMORY_CONTEXT_SLACK));
    VectorBufferCtl *ctl;

    g_vbpMemoryContext = AllocSetContextCreate(parent, "VectorBufferMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT, contextLimit);
    ctl = (VectorBufferCtl *)VbpMetadataAlloc(1, sizeof(VectorBufferCtl), MAXIMUM_ALIGNOF);
    g_vbpDirectoryLocks = (LWLock *)VbpMetadataAlloc(
        config->directoryLockCount, sizeof(LWLock), alignof(LWLock));
    g_vbpDirectoryBuckets = (uint32 *)VbpMetadataAlloc(
        config->directoryBucketCount, sizeof(uint32), alignof(uint32));
    g_vbpInstances = (VbpInstance *)VbpMetadataAlloc(
        config->maxVbps, sizeof(VbpInstance), alignof(VbpInstance));
    g_vbpPartitions = (VbpHashPartition *)VbpMetadataAlloc(
        config->hashPartitionCapacity, sizeof(VbpHashPartition), alignof(VbpHashPartition));
    g_vbpHashViewDescs = (VbpHashViewDesc *)VbpMetadataAlloc(
        config->hashViewDescCapacity, sizeof(VbpHashViewDesc), alignof(VbpHashViewDesc));
    return ctl;
}

static inline bool VbpGlobalChunkIndexIsValid(uint32 index, uint32 chunkCount)
{
    return index == VBP_INVALID_INDEX || index < chunkCount;
}

static void VbpPushGlobalFreeChunk(VbpTaggedHead *head, VbpMemChunk *chunks, uint32 chunkCount,
    uint32 chunkIndex)
{
    VbpMemChunk *chunk;
    uint64 expected;

    Assert(head != NULL && chunks != NULL && chunkIndex < chunkCount);
    if (head == NULL || chunks == NULL || chunkIndex >= chunkCount) {
        return;
    }
    chunk = &chunks[chunkIndex];
    Assert(VbpChunkControlRead(chunk) == VBP_CHUNK_FREE);
    Assert(chunk->vbpId == VBP_INVALID_INDEX && chunk->vbpGeneration == 0);
    Assert(pg_atomic_read_u32(&chunk->globalFreeNext) == VBP_INVALID_INDEX);
    expected = VbpTaggedHeadRead(head);
    while (VbpGlobalChunkIndexIsValid(
        VbpTaggedIndex(expected) == 0 ? VBP_INVALID_INDEX : VbpTaggedIndex(expected) - 1, chunkCount)) {
        uint32 encoded = VbpTaggedIndex(expected);
        uint32 next = encoded == 0 ? VBP_INVALID_INDEX : encoded - 1;
        uint64 desired;

        pg_atomic_write_u32(&chunk->globalFreeNext, next);
        pg_write_barrier();
        desired = VbpPackTaggedIndex(chunkIndex + 1, VbpTaggedVersion(expected) + 1);
        if (VbpTaggedHeadCompareExchange(head, &expected, desired)) {
            return;
        }
    }
    Assert(false);
}

static bool VbpPopGlobalFreeChunk(VbpTaggedHead *head, VbpMemChunk *chunks, uint32 chunkCount,
    uint32 *chunkIndex)
{
    uint64 expected;

    if (head == NULL || chunks == NULL || chunkCount == 0 || chunkIndex == NULL) {
        return false;
    }
    *chunkIndex = VBP_INVALID_INDEX;
    expected = VbpTaggedHeadRead(head);
    while (VbpTaggedIndex(expected) != 0) {
        uint32 encoded = VbpTaggedIndex(expected);
        uint32 current;
        VbpMemChunk *chunk;
        uint32 next;
        uint64 desired;

        current = encoded - 1;
        if (current >= chunkCount) {
            return false;
        }
        chunk = &chunks[current];
        next = pg_atomic_read_u32(&chunk->globalFreeNext);
        if (!VbpGlobalChunkIndexIsValid(next, chunkCount)) {
            return false;
        }
        desired = VbpPackTaggedIndex(next == VBP_INVALID_INDEX ? 0 : next + 1,
            VbpTaggedVersion(expected) + 1);
        if (!VbpTaggedHeadCompareExchange(head, &expected, desired)) {
            continue;
        }

        pg_atomic_write_u32(&chunk->globalFreeNext, VBP_INVALID_INDEX);
        if (VbpChunkControlRead(chunk) != VBP_CHUNK_FREE ||
            chunk->vbpId != VBP_INVALID_INDEX || chunk->vbpGeneration != 0) {
            return false;
        }
        {
            uint32 expectedControl = VBP_CHUNK_FREE;

            if (!pg_atomic_compare_exchange_u32(&chunk->control, &expectedControl,
                    VBP_CHUNK_CLAIMED)) {
                return false;
            }
        }
        *chunkIndex = current;
        return true;
    }
    return false;
}

static bool VbpEnsureHashRuntimeAttached(void)
{
    VbpHashRuntime config;

    if (g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC) {
        return false;
    }
    if (g_vbpHashRuntimeReady) {
        return true;
    }
    config.partitionArena = g_vbpPartitions;
    config.partitionCapacity = g_vbpCtl->hashPartitionCapacity;
    config.viewDescArena = g_vbpHashViewDescs;
    config.viewDescCapacity = g_vbpCtl->hashViewDescCapacity;
    config.chunkArena = g_vbpChunks;
    config.chunkCount = g_vbpCtl->chunkCount;
    config.chunkSize = g_vbpCtl->chunkSize;
    config.entryArena = g_vbpPayloadData;
    config.entryArenaSize = (Size)g_vbpCtl->capacityBytes;
    if (!VbpHashRuntimeInit(&g_vbpHashRuntime, &config)) {
        return false;
    }
    VbpHashRuntimeAttach(&g_vbpHashRuntime);
    g_vbpHashRuntimeReady = true;
    return true;
}

static void VbpResetHashViewDesc(VbpHashViewDesc *desc, uint32 next)
{
    desc->view.buckets = NULL;
    desc->view.migratedBitmap = NULL;
    desc->view.bucketCount = 0;
    desc->listNext = next;
}

static void VbpInitializeVbpDescriptor(VbpInstance *vbp, VbpId id, uint32 maxVbps,
    uint32 partitionBase)
{
    void *freelistStorage = MemoryContextAlloc(g_vbpMemoryContext, sizeof(VbpChunkFreelist));

    vbp->id = id;
    pg_atomic_init_u64(&vbp->lifecycleControl, VbpLifecycleControlPack(1, VBP_FREE, 0));
    vbp->partitionBase = partitionBase;
    vbp->partitionCount = 0;
    vbp->chunkFreelist = new (freelistStorage) VbpChunkFreelist(VBP_CHUNK_FREELIST_CAPACITY);
    vbp->directoryNext = VBP_INVALID_INDEX;
    vbp->freeNext = id + 1 < maxVbps ? id + 1 : VBP_INVALID_INDEX;
    vbp->deferredNext = VBP_INVALID_INDEX;
    pg_atomic_init_u64(&vbp->hashControl, 0);
    pg_atomic_init_u32(&vbp->liveEntries, 0);
    pg_atomic_init_u32(&vbp->migratedBuckets, 0);
    pg_atomic_init_u32(&vbp->resizeClaim, VBP_RESIZE_NONE);
    pg_atomic_init_u32(&vbp->reclaimBucketCursor, 0);
    pg_atomic_init_u32(&vbp->migrateCursor, 0);
    pg_atomic_init_u32(&vbp->workFlags, 0);
    vbp->clHead = VBP_INVALID_INDEX;
    LWLockInitialize(&vbp->chunkLock, (int)LWTRANCHE_EXTEND, 0);
}

static void VbpInitializeControl(VectorBufferCtl *ctl, const VbpSharedConfig *config)
{
    ctl->chunkSize = config->chunkSize;
    ctl->chunkCount = config->chunkCount;
    ctl->maxVbps = config->maxVbps;
    ctl->minPayload = config->minPayload;
    ctl->reclaimScanLimit = config->reclaimScanLimit;
    ctl->directoryLockCount = config->directoryLockCount;
    ctl->directoryBucketCount = config->directoryBucketCount;
    ctl->hashPartitionStride = config->hashPartitionStride;
    ctl->hashPartitionCapacity = config->hashPartitionCapacity;
    ctl->hashBucketCapacity = config->hashBucketCapacity;
    ctl->hashViewDescCapacity = config->hashViewDescCapacity;
    ctl->hashViewFreeHead = config->hashViewDescCapacity == 0 ? VBP_INVALID_INDEX : 0;
    ctl->vbpFreeHead = 0;
    VbpTaggedHeadInit(&ctl->globalFreeChunkHead);
    VbpTaggedHeadInit(&ctl->deferredDestroyHead);
    ctl->configuredCapacityBytes = config->configuredCapacityBytes;
    ctl->capacityBytes = config->capacityBytes;
    ctl->stats.capacityBytes = config->capacityBytes;
    LWLockInitialize(&ctl->hashViewLock, (int)LWTRANCHE_EXTEND, 0);
    InitSharedLatch(&ctl->reclaimLatch);
    ctl->reclaimLatchInited = true;
    ctl->reclaimLatchOwned = false;
}

static void VbpInitializeDirectoryAndInstances(VectorBufferCtl *ctl)
{
    for (uint32 index = 0; index < ctl->directoryLockCount; index++) {
        LWLockInitialize(&g_vbpDirectoryLocks[index], (int)LWTRANCHE_EXTEND, 0);
    }
    for (uint32 index = 0; index < ctl->directoryBucketCount; index++) {
        g_vbpDirectoryBuckets[index] = VBP_INVALID_INDEX;
    }
    for (uint32 index = 0; index < ctl->maxVbps; index++) {
        Size partitionBase = mul_size((Size)index, (Size)ctl->hashPartitionStride);

        VbpInitializeVbpDescriptor(&g_vbpInstances[index], index, ctl->maxVbps, (uint32)partitionBase);
    }
}

static void VbpInitializeHashViewDescs(VectorBufferCtl *ctl)
{
    for (uint32 index = 0; index < ctl->hashViewDescCapacity; index++) {
        VbpHashViewDesc *desc = &g_vbpHashViewDescs[index];

        desc->view.buckets = NULL;
        desc->view.migratedBitmap = NULL;
        desc->view.bucketCount = 0;
        desc->listNext = index + 1 < ctl->hashViewDescCapacity ? index + 1 : VBP_INVALID_INDEX;
    }
}

static void VbpInitializeChunks(VectorBufferCtl *ctl)
{
    for (uint32 index = 0; index < ctl->chunkCount; index++) {
        VbpMemChunk *chunk = &g_vbpChunks[index];
        Size entryBase = mul_size((Size)index, (Size)ctl->chunkSize);

        VbpChunkInit(chunk, VBP_CHUNK_FREE);
        pg_atomic_init_u64(&chunk->publicationEpoch, 0);
        chunk->vbpId = VBP_INVALID_INDEX;
        chunk->vbpGeneration = 0;
        chunk->entryBase = (uint64)entryBase;
        VbpTaggedHeadInit(&chunk->freeEntries);
        pg_atomic_init_u32(&chunk->nFree, 0);
        chunk->clPrev = VBP_INVALID_INDEX;
        chunk->clNext = VBP_INVALID_INDEX;
        pg_atomic_write_u32(&chunk->globalFreeNext,
            index == 0 ? VBP_INVALID_INDEX : index - 1);
    }
    pg_write_barrier();
    pg_atomic_write_u64(&ctl->globalFreeChunkHead.value,
        VbpPackTaggedIndex(ctl->chunkCount, 1));
}

static void VbpInitializeSharedMemory(char *sharedBase, const VbpSharedConfig *config)
{
    VectorBufferCtl *ctl;

    ctl = VbpCreateMetadata(config);
    VbpInitializeControl(ctl, config);
    g_vbpChunks = (VbpMemChunk *)(sharedBase + config->layout.chunksOffset);
    g_vbpPayloadData = sharedBase + config->layout.payloadDataOffset;
    VbpHashPartitionArenaInit(g_vbpPartitions, ctl->hashPartitionCapacity);
    VbpInitializeDirectoryAndInstances(ctl);
    VbpInitializeHashViewDescs(ctl);
    VbpInitializeChunks(ctl);

    pg_write_barrier();
    ctl->magic = VBP_MAGIC;
    pg_write_barrier();
    g_vbpCtl = ctl;
}

static void VbpFreeHashViewStorage(VbpHashView *view)
{
    Assert(g_vbpCtl == NULL || !LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    if (view == NULL) {
        return;
    }
    if (view->migratedBitmap != NULL) {
        pfree((void *)view->migratedBitmap);
    }
    if (view->buckets != NULL) {
        pfree(view->buckets);
    }
    view->buckets = NULL;
    view->migratedBitmap = NULL;
    view->bucketCount = 0;
}

static bool VbpAllocateHashViewStorage(uint32 bucketCount, uint32 migratedBucketCount,
    VbpHashView *view)
{
    const int allocFlags = MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO;
    Size bitmapWords = 0;

    Assert(g_vbpCtl == NULL || !LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    if (view == NULL || g_vbpCtl == NULL || g_vbpMemoryContext == NULL || !g_vbpHashRuntimeReady ||
        bucketCount == 0 || (bucketCount & (bucketCount - 1)) != 0 ||
        bucketCount > g_vbpCtl->hashBucketCapacity ||
        (migratedBucketCount != 0 &&
            (bucketCount < VBP_HASH_GROWTH_FACTOR ||
                migratedBucketCount != bucketCount / VBP_HASH_GROWTH_FACTOR))) {
        return false;
    }
    view->buckets = NULL;
    view->migratedBitmap = NULL;
    view->bucketCount = bucketCount;
    view->buckets = (VbpEntryRef *)MemoryContextAllocExtended(g_vbpMemoryContext,
        mul_size((Size)bucketCount, sizeof(VbpEntryRef)), allocFlags);
    if (view->buckets == NULL) {
        return false;
    }
    if (migratedBucketCount != 0) {
        bitmapWords = add_size((Size)migratedBucketCount, VBP_HASH_BITMAP_BITS - 1) /
            VBP_HASH_BITMAP_BITS;
        view->migratedBitmap = (volatile uint64 *)MemoryContextAllocExtended(g_vbpMemoryContext,
            mul_size(bitmapWords, sizeof(uint64)), allocFlags);
        if (view->migratedBitmap == NULL) {
            VbpFreeHashViewStorage(view);
            return false;
        }
    }
    return true;
}

static bool VbpInstallHashViewLocked(const VbpHashView *view, uint32 *descIndex)
{
    uint32 reservedDesc;
    VbpHashViewDesc *desc;
    uint32 next;

    Assert(g_vbpCtl != NULL && LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    if (view == NULL || descIndex == NULL || view->buckets == NULL || view->bucketCount == 0 ||
        (view->bucketCount & (view->bucketCount - 1)) != 0 ||
        view->bucketCount > g_vbpCtl->hashBucketCapacity ||
        !PointerIsAligned(view->buckets, VbpEntryRef) ||
        (view->migratedBitmap != NULL && !PointerIsAligned(view->migratedBitmap, uint64))) {
        return false;
    }
    reservedDesc = g_vbpCtl->hashViewFreeHead;
    if (reservedDesc == VBP_INVALID_INDEX || reservedDesc >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    desc = &g_vbpHashViewDescs[reservedDesc];
    next = desc->listNext;
    if (desc->view.buckets != NULL || desc->view.migratedBitmap != NULL || desc->view.bucketCount != 0 ||
        (next != VBP_INVALID_INDEX && next >= g_vbpCtl->hashViewDescCapacity)) {
        return false;
    }
    g_vbpCtl->hashViewFreeHead = next;
    desc->view = *view;
    desc->listNext = VBP_INVALID_INDEX;
    *descIndex = reservedDesc;
    return true;
}

static bool VbpDetachHashViewLocked(uint32 descIndex, VbpHashView *view)
{
    VbpHashViewDesc *desc;

    Assert(g_vbpCtl != NULL && LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    if (view == NULL || descIndex >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    desc = &g_vbpHashViewDescs[descIndex];
    if (desc->view.buckets == NULL || desc->view.bucketCount == 0 ||
        (desc->view.bucketCount & (desc->view.bucketCount - 1)) != 0 ||
        desc->view.bucketCount > g_vbpCtl->hashBucketCapacity ||
        !MemoryContextContains(g_vbpMemoryContext, desc->view.buckets) ||
        (desc->view.migratedBitmap != NULL &&
            !MemoryContextContains(g_vbpMemoryContext, (void *)desc->view.migratedBitmap))) {
        return false;
    }
    *view = desc->view;
    VbpResetHashViewDesc(desc, g_vbpCtl->hashViewFreeHead);
    g_vbpCtl->hashViewFreeHead = descIndex;
    return true;
}

bool VbpHashAllocateResizeCandidate(VbpInstance *vbp, uint32 *candidateDesc)
{
    VbpHashView candidate = {};
    uint64 control;
    uint32 activeDescIndex;
    uint32 oldBucketCount;
    uint32 newBucketCount;
    uint32 expectedGeneration;
    bool allocated = false;

    if (vbp == NULL || candidateDesc == NULL || !VbpEnsureHashRuntimeAttached() ||
        vbp->id >= g_vbpCtl->maxVbps || vbp != &g_vbpInstances[vbp->id] ||
        VbpInstanceState(vbp) != VBP_ACTIVE) {
        return false;
    }
    *candidateDesc = VBP_INVALID_INDEX;
    expectedGeneration = VbpInstanceGeneration(vbp);
    control = pg_atomic_read_u64(&vbp->hashControl);
    if (VbpHashControlCandidateDesc(control) != VBP_INVALID_INDEX) {
        return false;
    }
    activeDescIndex = VbpHashControlActiveDesc(control);
    if (activeDescIndex >= g_vbpCtl->hashViewDescCapacity ||
        g_vbpHashViewDescs[activeDescIndex].view.buckets == NULL) {
        return false;
    }
    oldBucketCount = g_vbpHashViewDescs[activeDescIndex].view.bucketCount;
    if (oldBucketCount == 0 || oldBucketCount > PG_UINT32_MAX / VBP_HASH_GROWTH_FACTOR) {
        return false;
    }
    newBucketCount = oldBucketCount * VBP_HASH_GROWTH_FACTOR;
    if (!VbpAllocateHashViewStorage(newBucketCount, oldBucketCount, &candidate)) {
        return false;
    }

    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    if (pg_atomic_read_u64(&vbp->hashControl) == control &&
        VbpInstanceLifecycleMatches(vbp, expectedGeneration, VBP_ACTIVE)) {
        allocated = VbpInstallHashViewLocked(&candidate, candidateDesc);
    }
    LWLockRelease(&g_vbpCtl->hashViewLock);
    if (!allocated) {
        VbpFreeHashViewStorage(&candidate);
    }
    return allocated;
}

static bool VbpHashViewIsRetired(uint64 control, uint32 descIndex)
{
    uint32 active = VbpHashControlActiveDesc(control);
    uint32 current;

    if (active == VBP_INVALID_INDEX || active >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    current = g_vbpHashViewDescs[active].listNext;

    for (uint32 visited = 0; current != VBP_INVALID_INDEX &&
            visited < g_vbpCtl->hashViewDescCapacity; visited++) {
        if (current == descIndex) {
            return true;
        }
        if (current >= g_vbpCtl->hashViewDescCapacity) {
            return true;
        }
        current = g_vbpHashViewDescs[current].listNext;
    }
    return false;
}

bool VbpHashReleaseUnpublishedView(VbpInstance *vbp, uint32 descIndex)
{
    VbpHashView releasedView = {};
    uint64 control;
    bool released = false;

    if (g_vbpCtl == NULL || vbp == NULL || vbp->id >= g_vbpCtl->maxVbps ||
        vbp != &g_vbpInstances[vbp->id] || descIndex >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    control = pg_atomic_barrier_read_u64(&vbp->hashControl);
    if (VbpHashControlActiveDesc(control) != descIndex &&
        VbpHashControlCandidateDesc(control) != descIndex &&
        !VbpHashViewIsRetired(control, descIndex)) {
        released = VbpDetachHashViewLocked(descIndex, &releasedView);
    }
    LWLockRelease(&g_vbpCtl->hashViewLock);
    if (released) {
        VbpFreeHashViewStorage(&releasedView);
    }
    return released;
}

static bool VbpInitialHashLayout(uint32 payloadLen, uint32 *slotSize, uint32 *slotsPerChunk,
    uint32 *bucketCount)
{
    Size halfSlots;
    Size desired;

    if (payloadLen < g_vbpCtl->minPayload || !VbpPayloadSlotSize(payloadLen, slotSize) ||
        *slotSize > g_vbpCtl->chunkSize) {
        return false;
    }
    *slotsPerChunk = g_vbpCtl->chunkSize / *slotSize;
    if (*slotsPerChunk == 0) {
        return false;
    }
    halfSlots = add_size((Size)*slotsPerChunk, (Size)1) / VBP_HASH_GROWTH_FACTOR;
    desired = halfSlots > g_vbpCtl->hashPartitionStride ?
        halfSlots : g_vbpCtl->hashPartitionStride;
    if (desired > VBP_MAX_POWER_OF_TWO_32) {
        return false;
    }
    *bucketCount = pg_nextpower2_32((uint32)desired);
    return *bucketCount <= g_vbpCtl->hashBucketCapacity;
}

static VbpId VbpPopDescriptorLocked(void)
{
    VbpId id = g_vbpCtl->vbpFreeHead;

    if (id == VBP_INVALID_INDEX) {
        return VBP_INVALID_INDEX;
    }
    Assert(id < g_vbpCtl->maxVbps);
    g_vbpCtl->vbpFreeHead = g_vbpInstances[id].freeNext;
    g_vbpInstances[id].freeNext = VBP_INVALID_INDEX;
    return id;
}

static void VbpPushDescriptorLocked(VbpInstance *vbp, uint32 generation)
{
    pg_atomic_write_u64(&vbp->hashControl, 0);
    pg_atomic_write_u32(&vbp->liveEntries, 0);
    pg_atomic_write_u32(&vbp->migratedBuckets, 0);
    pg_atomic_write_u32(&vbp->resizeClaim, VBP_RESIZE_NONE);
    pg_atomic_write_u32(&vbp->reclaimBucketCursor, 0);
    pg_atomic_write_u32(&vbp->migrateCursor, 0);
    pg_atomic_write_u32(&vbp->workFlags, 0);
    vbp->clHead = VBP_INVALID_INDEX;
    vbp->partitionCount = 0;
    vbp->relationKey.spcNode = InvalidOid;
    vbp->relationKey.dbNode = InvalidOid;
    vbp->relationKey.relNode = InvalidOid;
    vbp->payloadLayout.payloadLen = 0;
    vbp->payloadLayout.slotSize = 0;
    vbp->payloadLayout.slotsPerChunk = 0;
    vbp->directoryNext = VBP_INVALID_INDEX;
    vbp->deferredNext = VBP_INVALID_INDEX;
    pg_write_barrier();
    pg_atomic_write_u64(&vbp->lifecycleControl, VbpLifecycleControlPack(generation, VBP_FREE, 0));
    vbp->freeNext = g_vbpCtl->vbpFreeHead;
    g_vbpCtl->vbpFreeHead = vbp->id;
}

typedef struct VbpCandidateReservation {
    VbpId id;
    uint32 generation;
    uint32 partitionBase;
    uint32 partitionCount;
    uint32 descIndex;
    VbpHashView view;
} VbpCandidateReservation;

static bool VbpCandidateReservationMatchesLocked(const VbpCandidateReservation *reservation)
{
    VbpInstance *vbp;
    VbpHashViewDesc *desc;

    Assert(g_vbpCtl != NULL && LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    if (reservation == NULL || reservation->id >= g_vbpCtl->maxVbps ||
        reservation->descIndex >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    vbp = &g_vbpInstances[reservation->id];
    desc = &g_vbpHashViewDescs[reservation->descIndex];
    uint64 lifecycle = VbpLifecycleRead(vbp);

    return vbp->id == reservation->id &&
           lifecycle == VbpLifecycleControlPack(reservation->generation, VBP_FREE, 0) &&
           vbp->partitionBase == reservation->partitionBase && vbp->partitionCount == 0 &&
           vbp->freeNext == VBP_INVALID_INDEX && pg_atomic_read_u64(&vbp->hashControl) == 0 &&
           vbp->directoryNext == VBP_INVALID_INDEX &&
           desc->view.buckets == reservation->view.buckets &&
           desc->view.bucketCount == reservation->view.bucketCount &&
           desc->view.migratedBitmap == reservation->view.migratedBitmap &&
           desc->listNext == VBP_INVALID_INDEX;
}

static bool VbpRollbackCandidateLocked(const VbpCandidateReservation *reservation, VbpHashView *releasedView)
{
    VbpInstance *vbp;

    if (!VbpCandidateReservationMatchesLocked(reservation)) {
        return false;
    }
    vbp = &g_vbpInstances[reservation->id];
    if (!VbpDetachHashViewLocked(reservation->descIndex, releasedView)) {
        return false;
    }
    VbpPushDescriptorLocked(vbp, reservation->generation);
    return true;
}

static bool VbpReserveCandidateLocked(const VbpHashView *view, VbpCandidateReservation *reservation,
    VbpInstance **candidate)
{
    VbpId id;

    if (view == NULL || reservation == NULL || candidate == NULL) {
        return false;
    }
    id = VbpPopDescriptorLocked();
    if (id == VBP_INVALID_INDEX) {
        return false;
    }
    *candidate = &g_vbpInstances[id];
    reservation->id = id;
    reservation->generation = VbpInstanceGeneration(*candidate);
    reservation->partitionBase = (*candidate)->partitionBase;
    reservation->partitionCount = Min(g_vbpCtl->hashPartitionStride, view->bucketCount);
    reservation->descIndex = VBP_INVALID_INDEX;
    if (!VbpInstallHashViewLocked(view, &reservation->descIndex)) {
        VbpPushDescriptorLocked(*candidate, reservation->generation);
        return false;
    }
    reservation->view = *view;
    return true;
}

static void VbpInitializeCandidate(VbpInstance *vbp, const VbpRelationKey *key,
    uint32 payloadLen, uint32 slotSize, uint32 slotsPerChunk)
{
    vbp->relationKey = *key;
    vbp->payloadLayout.payloadLen = payloadLen;
    vbp->payloadLayout.slotSize = slotSize;
    vbp->payloadLayout.slotsPerChunk = slotsPerChunk;
    vbp->directoryNext = VBP_INVALID_INDEX;
    vbp->deferredNext = VBP_INVALID_INDEX;
    pg_atomic_write_u32(&vbp->reclaimBucketCursor, 0);
    pg_atomic_write_u32(&vbp->migrateCursor, 0);
    pg_atomic_write_u32(&vbp->workFlags, 0);
    vbp->clHead = VBP_INVALID_INDEX;
    for (uint32 index = 0; vbp->chunkFreelist != NULL && index < VBP_CHUNK_FREELIST_CAPACITY; ++index) {
        uint64 staleToken;

        if (!vbp->chunkFreelist->Dequeue(staleToken)) {
            break;
        }
    }
}

static VbpId VbpPrepareCandidate(const VbpRelationKey *key, uint32 payloadLen)
{
    VbpCandidateReservation reservation = {};
    VbpHashView allocatedView = {};
    VbpHashView releasedView = {};
    VbpInstance *vbp;
    uint32 slotSize;
    uint32 slotsPerChunk;
    uint32 bucketCount;
    bool rolledBack;

    if (!VbpEnsureHashRuntimeAttached() ||
        !VbpInitialHashLayout(payloadLen, &slotSize, &slotsPerChunk, &bucketCount)) {
        return VBP_INVALID_INDEX;
    }
    if (!VbpAllocateHashViewStorage(bucketCount, 0, &allocatedView)) {
        return VBP_INVALID_INDEX;
    }

    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    if (!VbpReserveCandidateLocked(&allocatedView, &reservation, &vbp)) {
        LWLockRelease(&g_vbpCtl->hashViewLock);
        VbpFreeHashViewStorage(&allocatedView);
        return VBP_INVALID_INDEX;
    }
    VbpInitializeCandidate(vbp, key, payloadLen, slotSize, slotsPerChunk);
    LWLockRelease(&g_vbpCtl->hashViewLock);

    Assert(!LWLockHeldByMe(&g_vbpCtl->hashViewLock));
    {
        VbpHashInitConfig hashConfig = {
            reservation.id,
            reservation.generation,
            reservation.partitionBase,
            reservation.partitionCount,
            reservation.descIndex
        };

        if (VbpHashInit(vbp, &hashConfig)) {
            return reservation.id;
        }
    }

    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    rolledBack = VbpRollbackCandidateLocked(&reservation, &releasedView);
    LWLockRelease(&g_vbpCtl->hashViewLock);
    Assert(rolledBack);
    if (rolledBack) {
        VbpFreeHashViewStorage(&releasedView);
    }
    return VBP_INVALID_INDEX;
}

static void VbpDiscardCandidate(VbpId id)
{
    VbpHashView releasedView = {};
    VbpInstance *vbp;
    uint32 descIndex;
    bool detached = false;

    if (id == VBP_INVALID_INDEX || id >= g_vbpCtl->maxVbps) {
        return;
    }
    vbp = &g_vbpInstances[id];
    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    descIndex = VbpHashControlActiveDesc(pg_atomic_read_u64(&vbp->hashControl));
    pg_atomic_write_u64(&vbp->hashControl, 0);
    if (descIndex != VBP_INVALID_INDEX) {
        detached = VbpDetachHashViewLocked(descIndex, &releasedView);
    }
    if (detached) {
        VbpPushDescriptorLocked(vbp, VbpInstanceGeneration(vbp));
    }
    LWLockRelease(&g_vbpCtl->hashViewLock);
    if (detached) {
        VbpFreeHashViewStorage(&releasedView);
    }
}

static inline bool VbpRelationKeyEquals(const VbpRelationKey *left, const VbpRelationKey *right)
{
    return left->spcNode == right->spcNode && left->dbNode == right->dbNode &&
        left->relNode == right->relNode;
}

static uint32 VbpHashRelationKey(const VbpRelationKey *key)
{
    uint32 hash = murmurhash32((uint32)key->spcNode);

    hash = hash_combine(hash, murmurhash32((uint32)key->dbNode));
    return hash_combine(hash, murmurhash32((uint32)key->relNode));
}

static inline uint32 VbpDirectoryBucket(uint32 hash)
{
    return hash & (g_vbpCtl->directoryBucketCount - 1);
}

static inline uint32 VbpDirectoryPartition(uint32 bucket)
{
    return bucket & (g_vbpCtl->directoryLockCount - 1);
}

static inline void VbpDirectoryLockAcquire(uint32 partition, LWLockMode mode)
{
    LWLockAcquire(&g_vbpDirectoryLocks[partition], mode);
}

static inline void VbpDirectoryLockRelease(uint32 partition)
{
    LWLockRelease(&g_vbpDirectoryLocks[partition]);
}

static VbpId VbpDirectoryFindLocked(uint32 bucket, const VbpRelationKey *key)
{
    VbpId current = g_vbpDirectoryBuckets[bucket];

    for (uint32 visited = 0; current != VBP_INVALID_INDEX && visited < g_vbpCtl->maxVbps; visited++) {
        VbpInstance *vbp;

        if (current >= g_vbpCtl->maxVbps) {
            return VBP_INVALID_INDEX;
        }
        vbp = &g_vbpInstances[current];
        if (VbpInstanceState(vbp) == VBP_ACTIVE &&
            VbpRelationKeyEquals(&vbp->relationKey, key)) {
            return current;
        }
        current = vbp->directoryNext;
    }
    return VBP_INVALID_INDEX;
}

static bool VbpTryAcquireScanRef(VbpInstance *vbp)
{
    uint64 expected;

    if (vbp == NULL) {
        return false;
    }
    expected = VbpLifecycleRead(vbp);
    while (VbpLifecycleState(expected) == VBP_ACTIVE &&
        VbpLifecycleRefs(expected) < VBP_LIFECYCLE_REF_MASK) {
        uint64 desired = expected + 1;

        if (pg_atomic_compare_exchange_u64(&vbp->lifecycleControl, &expected, desired)) {
            return true;
        }
    }
    return false;
}

static bool VbpMarkLifecycleCorrupt(VbpInstance *vbp, uint32 generation)
{
    uint64 expected;

    if (vbp == NULL || generation == 0) {
        return false;
    }
    expected = VbpLifecycleRead(vbp);
    while (VbpLifecycleGeneration(expected) == generation &&
        VbpLifecycleState(expected) != VBP_FREE) {
        uint64 desired = VbpLifecycleControlPack(generation, VBP_CORRUPT,
            VbpLifecycleRefs(expected));
        if (expected == desired ||
            pg_atomic_compare_exchange_u64(&vbp->lifecycleControl, &expected, desired)) {
            return true;
        }
    }
    return false;
}

static bool VbpPushDeferredDestroy(VbpInstance *vbp, uint32 generation)
{
    uint64 expected;
    uint64 lifecycle;

    if (g_vbpCtl == NULL || vbp == NULL || vbp->id >= g_vbpCtl->maxVbps ||
        vbp != &g_vbpInstances[vbp->id] || generation == 0) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    if (lifecycle != VbpLifecycleControlPack(generation, VBP_DETACHED, 0)) {
        return false;
    }
    expected = VbpTaggedHeadRead(&g_vbpCtl->deferredDestroyHead);
    while (VbpTaggedIndex(expected) == 0 || VbpTaggedIndex(expected) - 1 < g_vbpCtl->maxVbps) {
        uint32 encoded = VbpTaggedIndex(expected);
        uint32 nextVersion = VbpTaggedVersion(expected) + 1;
        uint64 desired;

        vbp->deferredNext = encoded == 0 ? VBP_INVALID_INDEX : encoded - 1;
        pg_write_barrier();
        desired = VbpPackTaggedIndex(vbp->id + 1, nextVersion);
        if (VbpTaggedHeadCompareExchange(&g_vbpCtl->deferredDestroyHead, &expected, desired)) {
            return true;
        }
    }
    (void)VbpMarkLifecycleCorrupt(vbp, generation);
    return false;
}

static bool VbpPopDeferredDestroy(VbpId *id, uint32 *generation)
{
    uint64 expected;

    if (g_vbpCtl == NULL || id == NULL || generation == NULL) {
        return false;
    }
    expected = VbpTaggedHeadRead(&g_vbpCtl->deferredDestroyHead);
    for (uint32 attempt = 0; attempt < VBP_DEFERRED_STACK_POP_CAS_LIMIT; ++attempt) {
        uint32 encoded = VbpTaggedIndex(expected);
        VbpInstance *vbp;
        uint32 next;
        uint64 desired;

        if (encoded == 0) {
            return false;
        }
        if (encoded - 1 >= g_vbpCtl->maxVbps) {
            return false;
        }
        vbp = &g_vbpInstances[encoded - 1];
        next = vbp->deferredNext;
        if (next != VBP_INVALID_INDEX && next >= g_vbpCtl->maxVbps) {
            return false;
        }
        desired = VbpPackTaggedIndex(next == VBP_INVALID_INDEX ? 0 : next + 1,
            VbpTaggedVersion(expected) + 1);
        if (VbpTaggedHeadCompareExchange(&g_vbpCtl->deferredDestroyHead, &expected, desired)) {
            uint64 lifecycle = VbpLifecycleRead(vbp);

            *id = vbp->id;
            *generation = VbpLifecycleGeneration(lifecycle);
            vbp->deferredNext = VBP_INVALID_INDEX;
            return true;
        }
    }
    return false;
}

static void VbpReleaseScanRef(VbpInstance *vbp, uint32 generation)
{
    uint64 expected;

    if (vbp == NULL || generation == 0) {
        return;
    }
    expected = VbpLifecycleRead(vbp);
    while (VbpLifecycleGeneration(expected) == generation && VbpLifecycleRefs(expected) > 0 &&
        VbpLifecycleState(expected) != VBP_FREE) {
        uint64 desired = expected - 1;

        if (pg_atomic_compare_exchange_u64(&vbp->lifecycleControl, &expected, desired)) {
            if (VbpLifecycleState(desired) == VBP_DETACHED && VbpLifecycleRefs(desired) == 0) {
                (void)VbpPushDeferredDestroy(vbp, generation);
            }
            return;
        }
    }
    Assert(VbpLifecycleGeneration(expected) != generation || VbpLifecycleRefs(expected) > 0 ||
        VbpLifecycleState(expected) == VBP_FREE);
}

static VbpDirectoryResult VbpDirectoryAcquireExisting(const VbpRelationKey *key, uint32 payloadLen,
    VbpDirectoryLookup *lookup)
{
    uint32 hash = VbpHashRelationKey(key);
    uint32 bucket = VbpDirectoryBucket(hash);
    uint32 partition = VbpDirectoryPartition(bucket);
    VbpId id;
    VbpDirectoryResult result;
    VbpDirectoryLockAcquire(partition, LW_SHARED);
    id = VbpDirectoryFindLocked(bucket, key);
    if (id == VBP_INVALID_INDEX) {
        result = VBP_DIRECTORY_MISSING;
    } else if (g_vbpInstances[id].payloadLayout.payloadLen != payloadLen) {
        lookup->existingPayloadLen = g_vbpInstances[id].payloadLayout.payloadLen;
        result = VBP_DIRECTORY_LAYOUT_MISMATCH;
    } else if (!VbpTryAcquireScanRef(&g_vbpInstances[id])) {
        result = VBP_DIRECTORY_REFCOUNT_EXHAUSTED;
    } else {
        lookup->id = id;
        result = VBP_DIRECTORY_ACQUIRED;
    }
    VbpDirectoryLockRelease(partition);
    return result;
}

static VbpDirectoryResult VbpDirectoryPublishCandidate(const VbpRelationKey *key, uint32 payloadLen,
    VbpId candidate, VbpDirectoryLookup *lookup)
{
    uint32 hash = VbpHashRelationKey(key);
    uint32 bucket = VbpDirectoryBucket(hash);
    uint32 partition = VbpDirectoryPartition(bucket);
    VbpId existing;
    VbpDirectoryResult result;
    VbpDirectoryLockAcquire(partition, LW_EXCLUSIVE);
    existing = VbpDirectoryFindLocked(bucket, key);
    if (existing != VBP_INVALID_INDEX) {
        if (g_vbpInstances[existing].payloadLayout.payloadLen != payloadLen) {
            lookup->existingPayloadLen = g_vbpInstances[existing].payloadLayout.payloadLen;
            result = VBP_DIRECTORY_LAYOUT_MISMATCH;
        } else if (!VbpTryAcquireScanRef(&g_vbpInstances[existing])) {
            result = VBP_DIRECTORY_REFCOUNT_EXHAUSTED;
        } else {
            lookup->id = existing;
            result = VBP_DIRECTORY_ACQUIRED;
        }
    } else {
        VbpInstance *vbp = &g_vbpInstances[candidate];
        uint32 generation = VbpInstanceGeneration(vbp);

        vbp->directoryNext = g_vbpDirectoryBuckets[bucket];
        pg_write_barrier();
        pg_atomic_write_u64(&vbp->lifecycleControl,
            VbpLifecycleControlPack(generation, VBP_ACTIVE, 1));
        g_vbpDirectoryBuckets[bucket] = candidate;
        lookup->id = candidate;
        result = VBP_DIRECTORY_ACQUIRED;
    }
    VbpDirectoryLockRelease(partition);
    return result;
}

static bool VbpDirectoryAcquirePayloadInvalidationRef(const VbpRelationKey *key,
    VbpInstance **acquired, uint32 *generation)
{
    uint32 hash;
    uint32 bucket;
    uint32 partition;
    VbpId id;
    bool result = false;

    if (key == NULL || acquired == NULL || generation == NULL) {
        return false;
    }
    *acquired = NULL;
    *generation = 0;
    hash = VbpHashRelationKey(key);
    bucket = VbpDirectoryBucket(hash);
    partition = VbpDirectoryPartition(bucket);
    VbpDirectoryLockAcquire(partition, LW_SHARED);
    id = VbpDirectoryFindLocked(bucket, key);
    if (id != VBP_INVALID_INDEX && VbpTryAcquireScanRef(&g_vbpInstances[id])) {
        *acquired = &g_vbpInstances[id];
        *generation = VbpInstanceGeneration(&g_vbpInstances[id]);
        result = true;
    }
    VbpDirectoryLockRelease(partition);
    return result;
}

static VbpDetachGateResult VbpAcquireDetachGate(VbpInstance *vbp)
{
    uint32 observed;

    if (vbp == NULL) {
        return VBP_DETACH_GATE_CORRUPT;
    }
    observed = pg_atomic_read_u32(&vbp->resizeClaim);
    for (uint32 attempt = 0; attempt < VBP_DETACH_RESIZE_WAIT_LIMIT; ++attempt) {
        if (observed == VBP_RESIZE_NONE) {
            uint32 expected = VBP_RESIZE_NONE;

            if (pg_atomic_compare_exchange_u32(&vbp->resizeClaim, &expected,
                    VBP_RESIZE_DETACH_GATE)) {
                return VBP_DETACH_GATE_ACQUIRED;
            }
            observed = expected;
            continue;
        }
        if (observed != VBP_RESIZE_OWNER) {
            return VBP_DETACH_GATE_CORRUPT;
        }
        pg_read_barrier();
        observed = pg_atomic_read_u32(&vbp->resizeClaim);
    }
    return VBP_DETACH_GATE_BUSY;
}

static void VbpReleaseDetachGate(VbpInstance *vbp)
{
    Assert(vbp != NULL);
    Assert(pg_atomic_read_u32(&vbp->resizeClaim) == VBP_RESIZE_DETACH_GATE);
    pg_memory_barrier();
    pg_atomic_write_u32(&vbp->resizeClaim, VBP_RESIZE_NONE);
}

static bool VbpTryDetachLifecycle(VbpInstance *vbp, uint32 generation)
{
    uint64 expected = VbpLifecycleRead(vbp);

    while (VbpLifecycleGeneration(expected) == generation &&
        VbpLifecycleState(expected) == VBP_ACTIVE && VbpLifecycleRefs(expected) > 0) {
        uint64 desired = VbpLifecycleControlPack(generation, VBP_DETACHED,
            VbpLifecycleRefs(expected));
        if (pg_atomic_compare_exchange_u64(&vbp->lifecycleControl, &expected, desired)) {
            return true;
        }
    }
    return false;
}

static VbpDetachResult VbpDetachMatchedInstance(VbpInstance *vbp, uint32 *link,
    VbpInstance **detached, uint32 *generation)
{
    VbpDetachGateResult gateResult;
    if (!VbpTryAcquireScanRef(vbp)) {
        return VBP_DETACH_REFCOUNT_EXHAUSTED;
    }
    *generation = VbpInstanceGeneration(vbp);
    gateResult = VbpAcquireDetachGate(vbp);
    if (gateResult != VBP_DETACH_GATE_ACQUIRED) {
        VbpReleaseScanRef(vbp, *generation);
        *generation = 0;
        return gateResult == VBP_DETACH_GATE_BUSY ? VBP_DETACH_RESIZE_BUSY : VBP_DETACH_CORRUPT;
    }
    if (!VbpTryDetachLifecycle(vbp, *generation)) {
        VbpReleaseDetachGate(vbp);
        VbpReleaseScanRef(vbp, *generation);
        *generation = 0;
        return VBP_DETACH_CORRUPT;
    }
    *link = vbp->directoryNext;
    vbp->directoryNext = VBP_INVALID_INDEX;
    pg_write_barrier();
    *detached = vbp;
    return VBP_DETACH_DONE;
}

static VbpDetachResult VbpDirectoryDetach(const VbpRelationKey *key, VbpInstance **detached,
    uint32 *generation)
{
    uint32 hash;
    uint32 bucket;
    uint32 partition;
    uint32 *link;
    VbpDetachResult result = VBP_DETACH_MISSING;

    if (key == NULL || detached == NULL || generation == NULL) {
        return VBP_DETACH_CORRUPT;
    }
    *detached = NULL;
    *generation = 0;
    hash = VbpHashRelationKey(key);
    bucket = VbpDirectoryBucket(hash);
    partition = VbpDirectoryPartition(bucket);
    VbpDirectoryLockAcquire(partition, LW_EXCLUSIVE);
    link = &g_vbpDirectoryBuckets[bucket];
    for (uint32 visited = 0; *link != VBP_INVALID_INDEX && visited < g_vbpCtl->maxVbps; ++visited) {
        VbpId id = *link;
        VbpInstance *vbp;

        if (id >= g_vbpCtl->maxVbps) {
            result = VBP_DETACH_CORRUPT;
            break;
        }
        vbp = &g_vbpInstances[id];
        if (VbpInstanceState(vbp) != VBP_ACTIVE ||
            !VbpRelationKeyEquals(&vbp->relationKey, key)) {
            link = &vbp->directoryNext;
            continue;
        }
        result = VbpDetachMatchedInstance(vbp, link, detached, generation);
        break;
    }
    VbpDirectoryLockRelease(partition);
    return result;
}

static void VbpSetPrivateAccess(VectorBufferAccess *access, ResourceOwner owner,
    MemoryContext accessContext, uint32 payloadLen)
{
    VbpAccessSetIdle(access);
    access->owner = owner;
    access->accessContext = accessContext;
    access->payloadLen = payloadLen;
}

static void VbpSetSharedAccess(VectorBufferAccess *access, VbpId id)
{
    VbpInstance *vbp = &g_vbpInstances[id];

    access->vbp = vbp;
    access->vbpGeneration = VbpInstanceGeneration(vbp);
}

static void VbpDeleteAccessStorage(VectorBufferAccess *access)
{
    MemoryContext accessContext;

    Assert(access != NULL);
    accessContext = access->accessContext;
    if (accessContext != NULL) {
        MemoryContextDelete(accessContext);
    } else {
        pfree(access);
    }
}

static bool VbpFinishBeginAccess(VectorBufferAccess *local, VectorBufferAccess **access)
{
    ResourceOwnerRememberVectorBufferAccess(local->owner, local);
    *access = local;
    return true;
}

static bool VbpSharedAccessEnabled(void)
{
    return g_vbpCtl != NULL && g_vbpCtl->magic == VBP_MAGIC && u_sess != NULL &&
        u_sess->datavec_ctx.enable_vector_buffer_cache;
}

static bool VbpPayloadFitsShared(uint32 payloadLen)
{
    uint32 slotSize;

    return payloadLen >= g_vbpCtl->minPayload &&
        VbpPayloadSlotSize(payloadLen, &slotSize) && slotSize <= g_vbpCtl->chunkSize;
}

static void VbpReleaseWriteU32(volatile uint32 *target, uint32 value)
{
    pg_memory_barrier();
    pg_atomic_write_u32(target, value);
}

static void VbpReleaseWriteU64(volatile uint64 *target, uint64 value)
{
    pg_memory_barrier();
    pg_atomic_write_u64(target, value);
}

static inline void VbpChunkLockAcquire(VbpInstance *vbp, LWLockMode mode)
{
    LWLockAcquire(&vbp->chunkLock, mode);
}

static inline void VbpChunkLockRelease(VbpInstance *vbp)
{
    LWLockRelease(&vbp->chunkLock);
}

static bool VbpChunkInCl(const VbpInstance *vbp, const VbpMemChunk *chunk, uint32 chunkIndex)
{
    if (vbp == NULL || chunk == NULL) {
        return false;
    }
    return chunk->clPrev != VBP_INVALID_INDEX || chunk->clNext != VBP_INVALID_INDEX ||
        vbp->clHead == chunkIndex;
}

/*
 * Caller must already hold chunkLock EXCLUSIVE. Head-insert into CL.
 */
static void VbpChunkListAttachLocked(VbpInstance *vbp, VbpMemChunk *chunk, uint32 chunkIndex)
{
    uint32 head;

    Assert(vbp != NULL && chunk != NULL && LWLockHeldByMe(&vbp->chunkLock));
    Assert(!VbpChunkInCl(vbp, chunk, chunkIndex));
    head = vbp->clHead;
    chunk->clPrev = VBP_INVALID_INDEX;
    chunk->clNext = head;
    if (head != VBP_INVALID_INDEX) {
        g_vbpChunks[head].clPrev = chunkIndex;
    }
    vbp->clHead = chunkIndex;
}

/*
 * Caller must already hold chunkLock EXCLUSIVE.
 */
static void VbpChunkListDetachLocked(VbpInstance *vbp, VbpMemChunk *chunk, uint32 chunkIndex)
{
    uint32 prev;
    uint32 next;

    Assert(vbp != NULL && chunk != NULL && LWLockHeldByMe(&vbp->chunkLock));
    if (!VbpChunkInCl(vbp, chunk, chunkIndex)) {
        return;
    }
    prev = chunk->clPrev;
    next = chunk->clNext;
    if (prev == VBP_INVALID_INDEX) {
        Assert(vbp->clHead == chunkIndex);
        vbp->clHead = next;
    } else {
        g_vbpChunks[prev].clNext = next;
    }
    if (next != VBP_INVALID_INDEX) {
        g_vbpChunks[next].clPrev = prev;
    }
    chunk->clPrev = VBP_INVALID_INDEX;
    chunk->clNext = VBP_INVALID_INDEX;
}

static char *VbpChunkSlotBase(const VbpMemChunk *chunk)
{
    if (g_vbpCtl == NULL || g_vbpPayloadData == NULL || chunk == NULL ||
        chunk->entryBase > g_vbpCtl->capacityBytes ||
        g_vbpCtl->chunkSize > g_vbpCtl->capacityBytes - chunk->entryBase) {
        return NULL;
    }
    return g_vbpPayloadData + (Size)chunk->entryBase;
}

/*
 * Identity check for new freelist work (enqueue / reserve). ACTIVE only —
 * DRAINING must not accept new slot occupancy (destroy × freelist crossover).
 * In-flight pins still resolve DRAINING chunks via the hash path.
 */
static bool VbpChunkMatchesVbp(VbpInstance *vbp, VbpMemChunk *chunk, uint64 *publicationEpoch)
{
    uint64 firstEpoch;
    uint64 finalEpoch;
    uint64 lifecycle;
    uint32 control;
    uint32 generation;

    if (g_vbpCtl == NULL || vbp == NULL || chunk == NULL || vbp->payloadLayout.slotSize == 0) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE) {
        return false;
    }
    generation = VbpLifecycleGeneration(lifecycle);
    firstEpoch = VbpChunkPublicationEpochRead(chunk);
    control = VbpChunkControlRead(chunk);
    if ((control & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_ACTIVE) {
        return false;
    }
    if (chunk->vbpId != vbp->id || chunk->vbpGeneration != generation ||
        vbp->payloadLayout.slotsPerChunk == 0 ||
        VbpChunkSlotBase(chunk) == NULL) {
        return false;
    }
    finalEpoch = VbpChunkPublicationEpochRead(chunk);
    control = VbpChunkControlRead(chunk);
    lifecycle = VbpLifecycleRead(vbp);
    if (firstEpoch != finalEpoch || (control & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_ACTIVE ||
        VbpLifecycleState(lifecycle) != VBP_ACTIVE ||
        VbpLifecycleGeneration(lifecycle) != generation) {
        return false;
    }
    if (publicationEpoch != NULL) {
        *publicationEpoch = firstEpoch;
    }
    return true;
}

static bool VbpChunkStillActiveForEpoch(const VbpMemChunk *chunk, uint64 publicationEpoch)
{
    return chunk != NULL && VbpChunkPublicationEpochRead(chunk) == publicationEpoch &&
        (VbpChunkControlRead(chunk) & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE;
}

static void VbpClearQueuedForEpoch(VbpMemChunk *chunk, uint64 publicationEpoch)
{
    uint32 expected;

    if (chunk == NULL || VbpChunkPublicationEpochRead(chunk) != publicationEpoch) {
        return;
    }
    expected = VbpChunkControlRead(chunk);
    while (VbpChunkPublicationEpochRead(chunk) == publicationEpoch &&
        ((expected & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE ||
            (expected & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_DRAINING) &&
        (expected & VBP_CHUNK_IN_FREELIST_BIT) != 0) {
        if (pg_atomic_compare_exchange_u32(&chunk->control, &expected,
                expected & ~VBP_CHUNK_IN_FREELIST_BIT)) {
            return;
        }
    }
}

static void VbpChunkFreelistEnqueue(VbpInstance *vbp, VbpMemChunk *chunk, uint32 chunkIndex)
{
    uint64 publicationEpoch;
    uint32 expected;

    if (g_vbpCtl == NULL || vbp == NULL || vbp->chunkFreelist == NULL || chunk == NULL ||
        chunkIndex >= g_vbpCtl->chunkCount || VbpInstanceState(vbp) != VBP_ACTIVE ||
        !VbpChunkMatchesVbp(vbp, chunk, &publicationEpoch) ||
        pg_atomic_read_u32(&chunk->nFree) == 0) {
        return;
    }
    expected = VbpChunkControlRead(chunk);
    bool marked = false;
    while (!marked) {
        if (VbpChunkPublicationEpochRead(chunk) != publicationEpoch ||
            (expected & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_ACTIVE ||
            (expected & VBP_CHUNK_IN_FREELIST_BIT) != 0) {
            return;
        }
        if (pg_atomic_compare_exchange_u32(&chunk->control, &expected,
                expected | VBP_CHUNK_IN_FREELIST_BIT)) {
            marked = true;
        }
    }
    if (!vbp->chunkFreelist->Enqueue(VbpMakeChunkToken((uint32)publicationEpoch, chunkIndex))) {
        VbpClearQueuedForEpoch(chunk, publicationEpoch);
        (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_AVAILABLE_OVERFLOW);
    }
}

static void VbpFinishAvailableLease(VbpInstance *vbp, VbpMemChunk *chunk,
    uint32 chunkIndex, uint64 publicationEpoch)
{
    bool shouldRequeue;

    if (vbp == NULL || vbp->chunkFreelist == NULL || chunk == NULL) {
        return;
    }
    shouldRequeue = VbpChunkStillActiveForEpoch(chunk, publicationEpoch) &&
        pg_atomic_read_u32(&chunk->nFree) > 0;
    if (shouldRequeue &&
        vbp->chunkFreelist->Enqueue(VbpMakeChunkToken((uint32)publicationEpoch, chunkIndex))) {
        return;
    }
    if (shouldRequeue) {
        (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_AVAILABLE_OVERFLOW);
    }
    VbpClearQueuedForEpoch(chunk, publicationEpoch);
    if (!shouldRequeue && VbpChunkStillActiveForEpoch(chunk, publicationEpoch) &&
        pg_atomic_read_u32(&chunk->nFree) > 0) {
        VbpChunkFreelistEnqueue(vbp, chunk, chunkIndex);
    }
}

static bool VbpInitializeChunkSlots(VbpMemChunk *chunk, char *slotBase,
    uint32 slotStride, uint32 slotCount)
{
    if (chunk == NULL || slotBase == NULL || slotCount == 0 ||
        slotCount > VBP_ENTRY_FREE_NEXT_MASK) {
        return false;
    }
    for (uint32 slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
        VbpEntry *entry = (VbpEntry *)(slotBase + (Size)slotIndex * slotStride);

        ItemPointerSetInvalid(&entry->payloadTid);
        pg_atomic_write_u64((volatile uint64 *)&entry->hashNext, VBP_INVALID_ENTRY_REF);
        pg_atomic_write_u64(&entry->control,
            VbpEntryFreeControlPack(0, slotIndex == 0 ? 0 : slotIndex));
    }
    pg_write_barrier();
    pg_atomic_write_u64(&chunk->freeEntries.value, VbpPackTaggedIndex(slotCount, 1));
    pg_atomic_write_u32(&chunk->nFree, slotCount);
    return true;
}

static bool VbpActivateChunk(VbpInstance *vbp, uint32 chunkIndex)
{
    VbpMemChunk *chunk;
    char *slotBase;
    uint32 slotCount;
    uint32 generation;
    uint64 publicationEpoch;

    if (g_vbpCtl == NULL || vbp == NULL || chunkIndex >= g_vbpCtl->chunkCount ||
        vbp->payloadLayout.slotSize == 0 ||
        (VbpChunkControlRead(&g_vbpChunks[chunkIndex]) & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_CLAIMED) {
        return false;
    }
    chunk = &g_vbpChunks[chunkIndex];
    slotBase = VbpChunkSlotBase(chunk);
    slotCount = vbp->payloadLayout.slotsPerChunk;
    generation = VbpInstanceGeneration(vbp);
    if (slotBase == NULL || slotCount == 0 ||
        pg_atomic_read_u32(&chunk->globalFreeNext) != VBP_INVALID_INDEX) {
        return false;
    }

    chunk->vbpId = vbp->id;
    chunk->vbpGeneration = generation;
    if (!VbpInitializeChunkSlots(chunk, slotBase, vbp->payloadLayout.slotSize, slotCount)) {
        return false;
    }

    VbpChunkLockAcquire(vbp, LW_EXCLUSIVE);
    if (!VbpInstanceLifecycleMatches(vbp, generation, VBP_ACTIVE) ||
        (VbpChunkControlRead(chunk) & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_CLAIMED ||
        pg_atomic_read_u32(&chunk->globalFreeNext) != VBP_INVALID_INDEX) {
        VbpChunkLockRelease(vbp);
        return false;
    }
    publicationEpoch = VbpChunkPublicationEpochRead(chunk) + 1;
    VbpChunkListAttachLocked(vbp, chunk, chunkIndex);
    VbpReleaseWriteU64(&chunk->publicationEpoch, publicationEpoch);
    VbpReleaseWriteU32(&chunk->control, VBP_CHUNK_ACTIVE);
    VbpChunkLockRelease(vbp);
    VbpChunkFreelistEnqueue(vbp, chunk, chunkIndex);
    return true;
}

static void VbpReturnClaimedChunk(VbpMemChunk *chunk, uint32 chunkIndex)
{
    Assert(g_vbpCtl != NULL && chunk != NULL && chunkIndex < g_vbpCtl->chunkCount);
    chunk->vbpId = VBP_INVALID_INDEX;
    chunk->vbpGeneration = 0;
    pg_atomic_write_u32(&chunk->globalFreeNext, VBP_INVALID_INDEX);
    pg_atomic_write_u32(&chunk->nFree, 0);
    chunk->clPrev = VBP_INVALID_INDEX;
    chunk->clNext = VBP_INVALID_INDEX;
    VbpTaggedHeadInit(&chunk->freeEntries);
    VbpReleaseWriteU32(&chunk->control, VBP_CHUNK_FREE);
    VbpPushGlobalFreeChunk(&g_vbpCtl->globalFreeChunkHead, g_vbpChunks,
        g_vbpCtl->chunkCount, chunkIndex);
}

static bool VbpAcquireAndAttachGlobalChunk(VbpInstance *vbp)
{
    uint32 chunkIndex;
    VbpMemChunk *chunk;

    if (g_vbpCtl == NULL || vbp == NULL) {
        return false;
    }
    if (!VbpPopGlobalFreeChunk(&g_vbpCtl->globalFreeChunkHead, g_vbpChunks,
            g_vbpCtl->chunkCount, &chunkIndex)) {
        return false;
    }
    chunk = &g_vbpChunks[chunkIndex];
    if (VbpActivateChunk(vbp, chunkIndex)) {
        return true;
    }
    VbpReturnClaimedChunk(chunk, chunkIndex);
    return false;
}

static void VbpClearEntryOwnership(VectorBufferAccess *access, VbpEntryRef ref, uint32 incarnation)
{
    if (access == NULL || (access->mode != VBP_ACCESS_RESERVED && access->mode != VBP_ACCESS_CACHED_PIN) ||
        access->state.entry.ref != ref || access->state.entry.incarnation != incarnation) {
        return;
    }
    VbpAccessSetIdle(access);
}

static void VbpQuarantinePoppedFreeSlot(VbpMemChunk *chunk, VbpEntry *entry, uint64 control)
{
    uint64 expected = control;

    /*
     * Caller has already removed this slot from the free count (or it was never
     * counted). Quarantine occupies the slot: FREE→QUARANTINED.
     */
    if (VbpEntryControlIsFree(control) &&
        pg_atomic_compare_exchange_u64(&entry->control, &expected,
            VbpEntryControlPack(VbpEntryControlIncarnation(control), VBP_ENTRY_QUARANTINED, 0))) {
        uint32 oldFree = pg_atomic_fetch_sub_u32(&chunk->nFree, 1);

        Assert(oldFree != 0);
        if (oldFree == 0) {
            (void)pg_atomic_fetch_add_u32(&chunk->nFree, 1);
            return;
        }
    }
}

static void VbpReturnPoppedSlotOrQuarantine(VbpInstance *vbp, VbpMemChunk *chunk,
    const VbpPoppedSlot *slot)
{
    uint64 currentControl = pg_atomic_read_u64(&slot->entry->control);
    if (!VbpEntryControlIsFree(slot->originalFreeControl) || currentControl != slot->originalFreeControl) {
        if (VbpEntryControlIsFree(currentControl)) {
            VbpQuarantinePoppedFreeSlot(chunk, slot->entry, currentControl);
        } else {
            uint32 oldFree = pg_atomic_fetch_sub_u32(&chunk->nFree, 1);

            Assert(oldFree != 0);
            if (oldFree == 0) {
                (void)pg_atomic_fetch_add_u32(&chunk->nFree, 1);
                return;
            }
        }
        return;
    }
    if (!VbpFreeEntryStackPush(&chunk->freeEntries, slot->slotBase,
            vbp->payloadLayout.slotSize, vbp->payloadLayout.slotsPerChunk, slot->slotIndex)) {
        currentControl = pg_atomic_read_u64(&slot->entry->control);
        VbpQuarantinePoppedFreeSlot(chunk, slot->entry, currentControl);
    }
}

static bool VbpClaimPoppedSlot(VbpInstance *vbp, VbpMemChunk *chunk,
    const VbpPoppedSlot *slot, VectorBufferAccess *access)
{
    uint64 expectedControl;
    uint32 incarnation;

    if (!VbpChunkStillActiveForEpoch(chunk, slot->publicationEpoch)) {
        VbpReturnPoppedSlotOrQuarantine(vbp, chunk, slot);
        return false;
    }
    if (!VbpEntryControlIsFree(slot->originalFreeControl)) {
        VbpQuarantinePoppedFreeSlot(chunk, slot->entry, slot->originalFreeControl);
        return false;
    }
    incarnation = VbpNextEntryIncarnation(VbpEntryControlIncarnation(slot->originalFreeControl));
    expectedControl = slot->originalFreeControl;
    if (!pg_atomic_compare_exchange_u64(&slot->entry->control, &expectedControl,
            VbpEntryControlPack(incarnation, VBP_ENTRY_CLOSED, 0))) {
        VbpReturnPoppedSlotOrQuarantine(vbp, chunk, slot);
        return false;
    }
    uint32 oldFree = pg_atomic_fetch_sub_u32(&chunk->nFree, 1);
    if (oldFree == 0) {
        (void)pg_atomic_fetch_add_u32(&chunk->nFree, 1);
        expectedControl = VbpEntryControlPack(incarnation, VBP_ENTRY_CLOSED, 0);
        (void)pg_atomic_compare_exchange_u64(&slot->entry->control, &expectedControl,
            VbpEntryControlPack(incarnation, VBP_ENTRY_QUARANTINED, 0));
        return false;
    }
    ItemPointerSetInvalid(&slot->entry->payloadTid);
    pg_atomic_write_u64((volatile uint64 *)&slot->entry->hashNext, VBP_INVALID_ENTRY_REF);
    VbpAccessSetEntry(access, VbpMakeEntryRef(slot->chunkIndex, slot->slotIndex), slot->entry, incarnation,
        VBP_ACCESS_RESERVED);
    return true;
}

static bool VbpTryReserveWithAllocatorRef(VbpInstance *vbp, VectorBufferAccess *access,
    VbpMemChunk *chunk, uint32 chunkIndex, const uint64 *availableToken)
{
    uint64 publicationEpoch;
    char *slotBase;
    uint32 slotIndex = VBP_INVALID_INDEX;
    VbpEntry *entry;
    uint64 freeControl;
    VbpPoppedSlot popped;

    Assert(!LWLockHeldByMe(&vbp->chunkLock));
    if (!VbpChunkMatchesVbp(vbp, chunk, &publicationEpoch) ||
        (availableToken != NULL && !VbpChunkTokenMatches(chunk, chunkIndex, *availableToken))) {
        if (availableToken != NULL && VbpChunkTokenMatches(chunk, chunkIndex, *availableToken)) {
            VbpClearQueuedForEpoch(chunk, VbpChunkPublicationEpochRead(chunk));
        }
        VbpChunkReleaseAllocatorRef(chunk);
        return false;
    }
    /*
     * Destroy may have flipped ACTIVE→DRAINING while we held the allocator ref.
     * Do not occupy a new slot or re-enqueue a draining chunk.
     */
    if (!VbpChunkStillActiveForEpoch(chunk, publicationEpoch)) {
        if (availableToken != NULL) {
            VbpFinishAvailableLease(vbp, chunk, chunkIndex, publicationEpoch);
        }
        VbpChunkReleaseAllocatorRef(chunk);
        return false;
    }
    slotBase = VbpChunkSlotBase(chunk);
    if (slotBase == NULL || !VbpFreeEntryStackPop(&chunk->freeEntries, slotBase,
            vbp->payloadLayout.slotSize, vbp->payloadLayout.slotsPerChunk, &slotIndex)) {
        if (availableToken != NULL) {
            VbpFinishAvailableLease(vbp, chunk, chunkIndex, publicationEpoch);
        }
        VbpChunkReleaseAllocatorRef(chunk);
        return false;
    }
    entry = (VbpEntry *)(slotBase + (Size)slotIndex * vbp->payloadLayout.slotSize);
    freeControl = pg_atomic_read_u64(&entry->control);
    popped.publicationEpoch = publicationEpoch;
    popped.chunkIndex = chunkIndex;
    popped.slotBase = slotBase;
    popped.slotIndex = slotIndex;
    popped.entry = entry;
    popped.originalFreeControl = freeControl;
    bool reserved = VbpClaimPoppedSlot(vbp, chunk, &popped, access);
    if (availableToken != NULL) {
        VbpFinishAvailableLease(vbp, chunk, chunkIndex, publicationEpoch);
    }
    VbpChunkReleaseAllocatorRef(chunk);
    return reserved;
}

static bool VbpRepairAvailableQueue(VbpInstance *vbp)
{
    uint64 lifecycle;
    uint32 generation;
    uint32 chunkIndex;
    uint32 visited = 0;

    if (vbp == NULL ||
        (pg_atomic_fetch_and_u32(&vbp->workFlags, ~VBP_WORK_AVAILABLE_OVERFLOW) &
            VBP_WORK_AVAILABLE_OVERFLOW) == 0) {
        return false;
    }
    lifecycle = VbpLifecycleRead(vbp);
    if (VbpLifecycleState(lifecycle) != VBP_ACTIVE) {
        return false;
    }
    generation = VbpLifecycleGeneration(lifecycle);

    VbpChunkLockAcquire(vbp, LW_SHARED);
    chunkIndex = vbp->clHead;
    while (chunkIndex != VBP_INVALID_INDEX && visited++ < g_vbpCtl->chunkCount) {
        if (chunkIndex >= g_vbpCtl->chunkCount) {
            (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_AVAILABLE_OVERFLOW);
            break;
        }
        VbpMemChunk *chunk = &g_vbpChunks[chunkIndex];
        uint32 next = chunk->clNext;
        uint32 control = VbpChunkControlRead(chunk);
        if ((control & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE &&
            (control & VBP_CHUNK_IN_FREELIST_BIT) == 0 &&
            chunk->vbpId == vbp->id && chunk->vbpGeneration == generation &&
            pg_atomic_read_u32(&chunk->nFree) > 0) {
            VbpChunkFreelistEnqueue(vbp, chunk, chunkIndex);
        }
        chunkIndex = next;
    }
    if (chunkIndex != VBP_INVALID_INDEX) {
        (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_AVAILABLE_OVERFLOW);
    }
    VbpChunkLockRelease(vbp);
    return true;
}

static bool VbpTryReserveFromFreelist(VbpInstance *vbp, VectorBufferAccess *access)
{
    for (uint32 probe = 0; probe < VBP_CHUNK_FREELIST_CAPACITY; ++probe) {
        uint64 token;
        uint32 chunkIndex;
        VbpMemChunk *chunk;

        if (vbp->chunkFreelist == NULL) {
            return false;
        }
        if (!vbp->chunkFreelist->Dequeue(token)) {
            if (VbpRepairAvailableQueue(vbp)) {
                continue;
            }
            return false;
        }
        chunkIndex = VbpChunkTokenIndex(token);
        if (chunkIndex == VBP_INVALID_INDEX || chunkIndex >= g_vbpCtl->chunkCount) {
            continue;
        }
        chunk = &g_vbpChunks[chunkIndex];
        if (!VbpChunkTryAcquireAllocatorRef(chunk)) {
            uint32 control = VbpChunkControlRead(chunk);
            uint64 publicationEpoch = VbpChunkPublicationEpochRead(chunk);

            if ((control & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE &&
                chunk->vbpId == vbp->id &&
                chunk->vbpGeneration == access->vbpGeneration &&
                VbpChunkTokenMatches(chunk, chunkIndex, token)) {
                VbpClearQueuedForEpoch(chunk, publicationEpoch);
                (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_AVAILABLE_OVERFLOW);
            }
            continue;
        }
        if (VbpTryReserveWithAllocatorRef(vbp, access, chunk, chunkIndex, &token)) {
            return true;
        }
    }
    return false;
}

static inline bool VbpColdEvictEnabled(void)
{
    return g_instance.attr.attr_storage.vbp_cold_evict;
}

static void VbpRequestColdEvict(VbpInstance *vbp)
{
    if (vbp == NULL || VbpInstanceState(vbp) != VBP_ACTIVE) {
        return;
    }
    if (g_vbpCtl != NULL) {
        (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.poolFullRequests, 1);
    }
    /* Idempotent request bit for the dedicated VBP reclaim thread. */
    (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_RECLAIM_REQUESTED);
    /*
     * SetLatch only when the reclaim util thread owns the latch. UT and
     * pre-start backends still set pending; ReclaimPass/Batch drain it.
     */
    if (VbpColdEvictEnabled() && g_vbpCtl != NULL && g_vbpCtl->reclaimLatchInited &&
        g_vbpCtl->reclaimLatchOwned) {
        SetLatch(&g_vbpCtl->reclaimLatch);
    }
}

static bool VbpHasFreeSlot(VbpInstance *vbp)
{
    uint32 chunkIndex;
    uint32 visited = 0;
    bool hasFreeSlot = false;

    VbpChunkLockAcquire(vbp, LW_SHARED);
    chunkIndex = vbp->clHead;
    while (chunkIndex != VBP_INVALID_INDEX && visited++ < g_vbpCtl->chunkCount) {
        if (chunkIndex >= g_vbpCtl->chunkCount) {
            break;
        }
        VbpMemChunk *chunk = &g_vbpChunks[chunkIndex];

        if (pg_atomic_read_u32(&chunk->nFree) > 0) {
            hasFreeSlot = true;
            break;
        }
        chunkIndex = chunk->clNext;
    }
    VbpChunkLockRelease(vbp);
    return hasFreeSlot;
}

static uint32 VbpColdEvictBatch(VbpInstance *vbp, uint32 budget)
{
    uint32 evictedCount = 0;
    uint32 scanLimit;
    uint32 maxAttempts;

    if (budget == 0 || VbpInstanceState(vbp) != VBP_ACTIVE) {
        return 0;
    }
    scanLimit = g_vbpCtl->reclaimScanLimit;
    if (scanLimit == 0) {
        return 0;
    }
    (void)pg_atomic_fetch_and_u32(&vbp->workFlags, ~VBP_WORK_RECLAIM_REQUESTED);

    /*
     * budget caps successful recycles. Allow extra EvictOne attempts so the
     * clock hand can clear USED before a victim becomes eligible.
     */
    maxAttempts = budget * VBP_RECLAIM_ATTEMPT_MULTIPLIER;
    for (uint32 attempt = 0; attempt < maxAttempts && evictedCount < budget; ++attempt) {
        VbpEntryRef evicted = VBP_INVALID_ENTRY_REF;
        uint32 incarnation = 0;

        (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.reclaimAttempts, 1);
        if (!VbpHashEvictOne(vbp, Min(scanLimit, VBP_RECLAIM_BUCKET_PROBES), scanLimit,
                &evicted, &incarnation)) {
            continue;
        }
        VbpRecycleEntryOwned(vbp, evicted, incarnation, NULL);
        (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.evictions, 1);
        (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.reclaimVictims, 1);
        evictedCount++;
    }
    if (VbpInstanceState(vbp) == VBP_ACTIVE && !VbpHasFreeSlot(vbp)) {
        (void)pg_atomic_fetch_or_u32(&vbp->workFlags, VBP_WORK_RECLAIM_REQUESTED);
    }
    return evictedCount;
}

static uint32 VectorBufferReclaimPass(void)
{
    uint32 totalEvicted = 0;
    uint32 budget;

    if (!VbpColdEvictEnabled() || g_vbpCtl == NULL || g_vbpInstances == NULL) {
        return 0;
    }
    budget = (uint32)g_instance.attr.attr_storage.vectorBufferReclaimBatchSize;
    if (budget == 0) {
        budget = VECTOR_BUFFER_DEFAULT_RECLAIM_BATCH_SIZE;
    }

    for (uint32 id = 0; id < g_vbpCtl->maxVbps; ++id) {
        VbpInstance *vbp = &g_vbpInstances[id];
        uint32 generation;

        if ((pg_atomic_read_u32(&vbp->workFlags) & VBP_WORK_RECLAIM_REQUESTED) == 0 ||
            !VbpTryAcquireScanRef(vbp)) {
            continue;
        }
        generation = VbpInstanceGeneration(vbp);
        totalEvicted += VbpColdEvictBatch(vbp, budget);
        VbpReleaseScanRef(vbp, generation);
    }
    return totalEvicted;
}

static void VbpReclaimSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGHUP = true;
    if (g_vbpCtl != NULL && g_vbpCtl->reclaimLatchInited) {
        SetLatch(&g_vbpCtl->reclaimLatch);
    }
    errno = saveErrno;
}

static void VbpReclaimSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.worker_sig_flags.got_SIGTERM = true;
    if (g_vbpCtl != NULL && g_vbpCtl->reclaimLatchInited) {
        SetLatch(&g_vbpCtl->reclaimLatch);
    }
    errno = saveErrno;
}

static void VbpReclaimDisownLatch(int code, Datum arg)
{
    (void)code;
    (void)arg;
    if (g_vbpCtl != NULL && g_vbpCtl->reclaimLatchInited) {
        g_vbpCtl->reclaimLatchOwned = false;
        DisownLatch(&g_vbpCtl->reclaimLatch);
    }
}

static void VbpReclaimConfigureSignals(void)
{
    gspqsignal(SIGHUP, VbpReclaimSighupHandler);
    gspqsignal(SIGINT, SIG_IGN);
    gspqsignal(SIGTERM, VbpReclaimSigtermHandler);
    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, SIG_IGN);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);
}

static void VbpReclaimRecoverFromError(void)
{
    t_thrd.log_cxt.error_context_stack = NULL;
    HOLD_INTERRUPTS();
    EmitErrorReport();
    LWLockReleaseAll();
    FlushErrorState();
    RESUME_INTERRUPTS();
    if (t_thrd.worker_sig_flags.got_SIGTERM) {
        proc_exit(0);
    }
    pg_usleep(1000000L);
}

static void VbpReclaimRunLoop(long naptimeMs)
{
    bool postmasterAlive = true;

    while (!t_thrd.worker_sig_flags.got_SIGTERM && postmasterAlive) {
        ResetLatch(&g_vbpCtl->reclaimLatch);
        if (t_thrd.worker_sig_flags.got_SIGHUP) {
            t_thrd.worker_sig_flags.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            if (g_instance.attr.attr_storage.vectorBufferReclaimInterval > 0) {
                naptimeMs = (long)g_instance.attr.attr_storage.vectorBufferReclaimInterval;
            }
        }
        if (VbpColdEvictEnabled()) {
            (void)VectorBufferReclaimPass();
        }
        (void)WaitLatch(&g_vbpCtl->reclaimLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            naptimeMs);
        postmasterAlive = PostmasterIsAlive();
    }
}

void VectorBufferReclaimMain(void)
{
    sigjmp_buf localSigjmpBuf;

    SetProcessingMode(InitProcessing);
    VbpReclaimConfigureSignals();
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    VectorBufferShmemInit();
    if (g_vbpCtl == NULL || !g_vbpCtl->reclaimLatchInited) {
        ereport(LOG, (errmsg("VBP reclaim: vector buffer not configured; exiting")));
        proc_exit(0);
    }
    OwnLatch(&g_vbpCtl->reclaimLatch);
    g_vbpCtl->reclaimLatchOwned = true;
    on_shmem_exit(VbpReclaimDisownLatch, 0);

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("vbp reclaim");
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        VbpReclaimRecoverFromError();
    }
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;
    VbpReclaimRunLoop((long)g_instance.attr.attr_storage.vectorBufferReclaimInterval);
    proc_exit(0);
}

static bool VbpReserveEntry(VbpInstance *vbp, VectorBufferAccess *access)
{
    if (g_vbpCtl == NULL || vbp == NULL || access == NULL || access->mode != VBP_ACCESS_IDLE ||
        access->pinCookie != 0 || access->vbp != vbp ||
        !VbpInstanceLifecycleMatches(vbp, access->vbpGeneration, VBP_ACTIVE)) {
        return false;
    }
    /* Candidate layer: lock-free chunk freelist. */
    if (VbpTryReserveFromFreelist(vbp, access)) {
        return true;
    }
    /* Expand from the global FREE chunk stack when freelist is empty. */
    if (VbpAcquireAndAttachGlobalChunk(vbp) && VbpTryReserveFromFreelist(vbp, access)) {
        return true;
    }
    /* Let the background worker reclaim space while this miss falls back. */
    VbpRequestColdEvict(vbp);
    return false;
}

void VbpRecycleEntryOwned(VbpInstance *vbp, VbpEntryRef ref, uint32 incarnation,
    VectorBufferAccess *access)
{
    VbpMemChunk *chunk;
    VbpEntry *entry;
    uint32 slotIndex;
    uint32 chunkIndex;
    char *slotBase;
    uint64 ownedControl;
    uint64 expected;

    if (vbp == NULL || !VbpEntryRefIsValid(ref) || incarnation == 0) {
        VbpClearEntryOwnership(access, ref, incarnation);
        return;
    }
    entry = VbpResolveEntry(vbp, ref, &chunk);
    ownedControl = VbpEntryControlPack(incarnation, VBP_ENTRY_CLOSED, 0);
    if (entry == NULL || chunk == NULL || pg_atomic_read_u64(&entry->control) != ownedControl) {
        VbpClearEntryOwnership(access, ref, incarnation);
        return;
    }
    slotIndex = VbpEntryRefSlot(ref);
    chunkIndex = VbpEntryRefChunk(ref);
    slotBase = (char *)entry - (Size)slotIndex * vbp->payloadLayout.slotSize;
    ItemPointerSetInvalid(&entry->payloadTid);
    pg_atomic_write_u64((volatile uint64 *)&entry->hashNext, VBP_INVALID_ENTRY_REF);
    expected = ownedControl;
    if (!pg_atomic_compare_exchange_u64(&entry->control, &expected,
            VbpEntryControlPack(incarnation, VBP_ENTRY_FREE, 0))) {
        VbpClearEntryOwnership(access, ref, incarnation);
        return;
    }
    if (!VbpFreeEntryStackPush(&chunk->freeEntries, slotBase,
            vbp->payloadLayout.slotSize, vbp->payloadLayout.slotsPerChunk, slotIndex)) {
        expected = pg_atomic_read_u64(&entry->control);
        while (VbpEntryControlIsFree(expected) &&
            VbpEntryControlIncarnation(expected) == incarnation) {
            if (pg_atomic_compare_exchange_u64(&entry->control, &expected,
                    VbpEntryControlPack(incarnation, VBP_ENTRY_QUARANTINED, 0))) {
                break;
            }
        }
        VbpClearEntryOwnership(access, ref, incarnation);
        return;
    }
    uint32 oldFree = pg_atomic_fetch_add_u32(&chunk->nFree, 1);

    Assert(oldFree < vbp->payloadLayout.slotsPerChunk);
    if (oldFree == 0) {
        VbpChunkFreelistEnqueue(vbp, chunk, chunkIndex);
    }
    VbpClearEntryOwnership(access, ref, incarnation);
}

static bool VbpReleaseActivePin(VectorBufferAccess *access)
{
    VbpEntryRef ref;
    uint32 incarnation;
    VbpEntry *entry;
    uint64 control;

    if (access == NULL || access->mode != VBP_ACCESS_CACHED_PIN) {
        return false;
    }
    ref = access->state.entry.ref;
    incarnation = access->state.entry.incarnation;
    if (!VbpEntryRefIsValid(ref) || incarnation == 0 || access->vbp == NULL ||
        access->vbpGeneration != VbpInstanceGeneration(access->vbp)) {
        VbpAccessSetIdle(access);
        return false;
    }
    entry = access->state.entry.entry;
    if (entry == NULL) {
        VbpAccessSetIdle(access);
        return false;
    }
    control = pg_atomic_read_u64(&entry->control);
    while (VbpEntryControlIncarnation(control) == incarnation) {
        VbpEntryState state = VbpEntryControlState(control);
        uint32 readers = VbpEntryControlReaders(control);
        uint64 desired;
        uint64 expected;

        if (readers == 0 || (state != VBP_ENTRY_CACHED && state != VBP_ENTRY_CLOSED)) {
            VbpAccessSetIdle(access);
            return false;
        }
        desired = state == VBP_ENTRY_CLOSED && readers == 1 ?
            VbpEntryControlPack(incarnation, VBP_ENTRY_CLOSED, 0) :
            VbpEntryControlPack(incarnation, state, readers - 1);
        if (VbpEntryControlUsed(control) &&
            (state == VBP_ENTRY_CACHED || readers > 1)) {
            desired = VbpEntryControlWithUsed(desired);
        }
        expected = control;
        if (pg_atomic_compare_exchange_u64(&entry->control, &expected, desired)) {
            if (state == VBP_ENTRY_CLOSED && readers == 1) {
                VbpRecycleEntryOwned(access->vbp, ref, incarnation, access);
            } else {
                VbpAccessSetIdle(access);
            }
            return true;
        }
        control = expected;
    }
    VbpAccessSetIdle(access);
    return false;
}

void VbpReleaseReservation(VectorBufferAccess *access)
{
    VbpEntryRef ref;
    uint32 incarnation;
    VbpEntry *entry;
    uint64 expected;

    if (access == NULL || access->mode != VBP_ACCESS_RESERVED) {
        return;
    }
    ref = access->state.entry.ref;
    incarnation = access->state.entry.incarnation;
    if (!VbpEntryRefIsValid(ref) || incarnation == 0 || access->vbp == NULL ||
        access->vbpGeneration != VbpInstanceGeneration(access->vbp)) {
        VbpAccessSetIdle(access);
        return;
    }
    entry = access->state.entry.entry;
    expected = VbpEntryControlPack(incarnation, VBP_ENTRY_CLOSED, 0);
    if (entry != NULL && pg_atomic_read_u64(&entry->control) == expected) {
        VbpRecycleEntryOwned(access->vbp, ref, incarnation, access);
        return;
    }
    VbpAccessSetIdle(access);
}

static void VbpReportPayloadLayoutMismatch(const VbpRelationKey *key, uint32 expected, uint32 requested)
{
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("vector buffer payload length does not match the active relation layout"),
        errdetail("Relation (%u,%u,%u) uses payload length %u, but %u was requested.",
            key->spcNode, key->dbNode, key->relNode, expected, requested)));
}

static void VbpReportScanRefExhausted(const VbpRelationKey *key)
{
    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        errmsg("vector buffer access reference count is exhausted"),
        errdetail("Relation (%u,%u,%u) already has the maximum number of active accesses.",
            key->spcNode, key->dbNode, key->relNode)));
}

static void VbpReportDetachResizeBusy(const VbpRelationKey *key)
{
    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        errmsg("vector buffer relation invalidation could not acquire the resize gate"),
        errdetail("Relation (%u,%u,%u) still has an in-progress vector buffer resize.",
            key->spcNode, key->dbNode, key->relNode)));
}

typedef enum VbpDestroyChunkResult {
    VBP_DESTROY_CHUNK_NONE = 0,
    VBP_DESTROY_CHUNK_SELECTED,
    VBP_DESTROY_CHUNK_RETURNED,
    VBP_DESTROY_CHUNK_RETRY,
    VBP_DESTROY_CHUNK_CORRUPT
} VbpDestroyChunkResult;

typedef enum VbpDestroyResult {
    VBP_DESTROY_DONE = 0,
    VBP_DESTROY_RETRY,
    VBP_DESTROY_CORRUPT
} VbpDestroyResult;

typedef struct VbpDestroyChunkTarget {
    VbpMemChunk *chunk;
    uint32 chunkIndex;
    uint64 publicationEpoch;
} VbpDestroyChunkTarget;

static bool VbpDestroyChunkSlotsAreSafe(VbpInstance *vbp, VbpMemChunk *chunk)
{
    char *slotBase;
    uint32 freeCount = 0;
    uint32 quarantinedCount = 0;

    if (vbp == NULL || chunk == NULL || vbp->payloadLayout.slotsPerChunk == 0) {
        return false;
    }
    slotBase = VbpChunkSlotBase(chunk);
    if (slotBase == NULL) {
        return false;
    }
    for (uint32 slot = 0; slot < vbp->payloadLayout.slotsPerChunk; ++slot) {
        VbpEntry *entry = (VbpEntry *)(slotBase + (Size)slot * vbp->payloadLayout.slotSize);
        uint64 control = pg_atomic_read_u64(&entry->control);
        VbpEntryState state = VbpEntryControlState(control);
        if (state == VBP_ENTRY_FREE) {
            ++freeCount;
        } else if (state == VBP_ENTRY_QUARANTINED && VbpEntryControlReaders(control) == 0) {
            ++quarantinedCount;
        } else {
            return false;
        }
    }
    return freeCount + quarantinedCount == vbp->payloadLayout.slotsPerChunk &&
        pg_atomic_read_u32(&chunk->nFree) == freeCount;
}

static VbpDestroyChunkResult VbpSelectDestroyChunkLocked(VbpInstance *vbp, uint32 generation,
    VbpDestroyChunkTarget *target)
{
    target->chunk = NULL;
    target->chunkIndex = VBP_INVALID_INDEX;
    target->publicationEpoch = 0;
    if (VbpLifecycleRead(vbp) != VbpLifecycleControlPack(generation, VBP_DETACHED, 0)) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    if (vbp->clHead == VBP_INVALID_INDEX) {
        return VBP_DESTROY_CHUNK_NONE;
    }
    if (vbp->clHead >= g_vbpCtl->chunkCount) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    target->chunkIndex = vbp->clHead;
    target->chunk = &g_vbpChunks[target->chunkIndex];
    target->publicationEpoch = VbpChunkPublicationEpochRead(target->chunk);
    uint32 state = VbpChunkControlRead(target->chunk) & VBP_CHUNK_STATE_MASK;
    if (target->chunk->clPrev != VBP_INVALID_INDEX || target->chunk->vbpId != vbp->id ||
        target->chunk->vbpGeneration != generation ||
        (state != VBP_CHUNK_ACTIVE && state != VBP_CHUNK_DRAINING)) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    if (pg_atomic_read_u32(&target->chunk->globalFreeNext) != VBP_INVALID_INDEX) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    if (state == VBP_CHUNK_ACTIVE &&
        !VbpChunkTryBeginDrain(target->chunk)) {
        return VBP_DESTROY_CHUNK_RETRY;
    }
    return VBP_DESTROY_CHUNK_SELECTED;
}

static bool VbpWaitForAllocatorRefs(VbpMemChunk *chunk)
{
    for (uint32 wait = 0; wait < VBP_DESTROY_ALLOCATOR_WAIT_LIMIT; ++wait) {
        if (VbpChunkAllocatorRefsDrained(chunk)) {
            return true;
        }
        pg_read_barrier();
    }
    return VbpChunkAllocatorRefsDrained(chunk);
}

static VbpDestroyChunkResult VbpClaimDestroyChunkLocked(VbpInstance *vbp, uint32 generation,
    const VbpDestroyChunkTarget *target)
{
    VbpMemChunk *chunk = target->chunk;
    uint32 control = VbpChunkControlRead(chunk);

    if (VbpLifecycleRead(vbp) != VbpLifecycleControlPack(generation, VBP_DETACHED, 0) ||
        chunk->vbpId != vbp->id || chunk->vbpGeneration != generation ||
        pg_atomic_read_u32(&chunk->globalFreeNext) != VBP_INVALID_INDEX ||
        VbpChunkPublicationEpochRead(chunk) != target->publicationEpoch ||
        !VbpChunkInCl(vbp, chunk, target->chunkIndex)) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    if ((control & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE ||
        (control & VBP_CHUNK_ALLOC_REF_MASK) != 0) {
        return VBP_DESTROY_CHUNK_RETRY;
    }
    if ((control & VBP_CHUNK_STATE_MASK) != VBP_CHUNK_DRAINING ||
        !VbpDestroyChunkSlotsAreSafe(vbp, chunk)) {
        return VBP_DESTROY_CHUNK_CORRUPT;
    }
    uint32 expected = control;

    if (!pg_atomic_compare_exchange_u32(&chunk->control, &expected, VBP_CHUNK_CLAIMED)) {
        return VBP_DESTROY_CHUNK_RETRY;
    }
    VbpChunkListDetachLocked(vbp, chunk, target->chunkIndex);
    return VBP_DESTROY_CHUNK_RETURNED;
}

static VbpDestroyChunkResult VbpDestroyOneChunk(VbpInstance *vbp, uint32 generation)
{
    VbpDestroyChunkTarget target;
    VbpDestroyChunkResult result;

    VbpChunkLockAcquire(vbp, LW_EXCLUSIVE);
    result = VbpSelectDestroyChunkLocked(vbp, generation, &target);
    VbpChunkLockRelease(vbp);
    if (result != VBP_DESTROY_CHUNK_SELECTED) {
        return result;
    }
    if (!VbpWaitForAllocatorRefs(target.chunk)) {
        return VBP_DESTROY_CHUNK_RETRY;
    }
    VbpChunkLockAcquire(vbp, LW_EXCLUSIVE);
    result = VbpClaimDestroyChunkLocked(vbp, generation, &target);
    VbpChunkLockRelease(vbp);
    if (result == VBP_DESTROY_CHUNK_RETURNED) {
        VbpReturnClaimedChunk(target.chunk, target.chunkIndex);
    }
    return result;
}

static bool VbpHashViewIsReleasableLocked(uint32 descIndex)
{
    VbpHashViewDesc *desc;

    if (descIndex >= g_vbpCtl->hashViewDescCapacity) {
        return false;
    }
    desc = &g_vbpHashViewDescs[descIndex];
    if (desc->view.buckets == NULL || desc->view.bucketCount == 0 ||
        (desc->view.bucketCount & (desc->view.bucketCount - 1)) != 0 ||
        desc->view.bucketCount > g_vbpCtl->hashBucketCapacity ||
        !MemoryContextContains(g_vbpMemoryContext, desc->view.buckets) ||
        (desc->view.migratedBitmap != NULL &&
            !MemoryContextContains(g_vbpMemoryContext, (void *)desc->view.migratedBitmap))) {
        return false;
    }
    return true;
}

static bool VbpDestroyHashViewsLocked(VbpInstance *vbp, VbpHashView *releasedViews,
    uint32 *releasedCount)
{
    uint64 control = pg_atomic_barrier_read_u64(&vbp->hashControl);
    uint32 active = VbpHashControlActiveDesc(control);
    uint32 candidate = VbpHashControlCandidateDesc(control);
    uint32 retired;
    uint32 retiredCount = 0;
    uint32 viewCount = 1;

    if (releasedViews == NULL || releasedCount == NULL) {
        return false;
    }
    *releasedCount = 0;

    if (!VbpHashViewIsReleasableLocked(active)) {
        return false;
    }
    retired = g_vbpHashViewDescs[active].listNext;
    if (candidate != VBP_INVALID_INDEX) {
        if (candidate == active || !VbpHashViewIsReleasableLocked(candidate)) {
            return false;
        }
        viewCount++;
    }
    for (uint32 current = retired; current != VBP_INVALID_INDEX;) {
        if (current == active || current == candidate || !VbpHashViewIsReleasableLocked(current)) {
            return false;
        }
        retiredCount++;
        if (retiredCount > g_vbpCtl->hashViewDescCapacity ||
            viewCount + retiredCount > VBP_HASH_VIEW_CHAIN_MAX) {
            return false;
        }
        current = g_vbpHashViewDescs[current].listNext;
    }

    if (candidate != VBP_INVALID_INDEX) {
        if (!VbpDetachHashViewLocked(candidate, &releasedViews[*releasedCount])) {
            return false;
        }
        (*releasedCount)++;
    }
    if (!VbpDetachHashViewLocked(active, &releasedViews[*releasedCount])) {
        return false;
    }
    (*releasedCount)++;
    while (retired != VBP_INVALID_INDEX) {
        uint32 next = g_vbpHashViewDescs[retired].listNext;

        if (!VbpDetachHashViewLocked(retired, &releasedViews[*releasedCount])) {
            return false;
        }
        (*releasedCount)++;
        retired = next;
    }
    pg_atomic_write_u64(&vbp->hashControl, 0);
    return true;
}

static VbpDestroyResult VbpDestroyInstance(VbpInstance *vbp, uint32 generation)
{
    VbpHashView releasedViews[VBP_HASH_VIEW_CHAIN_MAX] = {};
    uint32 releasedCount = 0;
    VbpDestroyChunkResult chunkResult = VBP_DESTROY_CHUNK_CORRUPT;

    if (vbp == NULL || generation == 0 ||
        VbpLifecycleRead(vbp) != VbpLifecycleControlPack(generation, VBP_DETACHED, 0) ||
        pg_atomic_read_u32(&vbp->liveEntries) != 0 ||
        pg_atomic_read_u32(&vbp->resizeClaim) != VBP_RESIZE_NONE) {
        return VBP_DESTROY_CORRUPT;
    }

    for (uint32 chunkAttempt = 0; chunkAttempt <= g_vbpCtl->chunkCount; ++chunkAttempt) {
        chunkResult = VbpDestroyOneChunk(vbp, generation);
        if (chunkResult == VBP_DESTROY_CHUNK_RETURNED) {
            continue;
        }
        if (chunkResult == VBP_DESTROY_CHUNK_RETRY) {
            return VBP_DESTROY_RETRY;
        }
        if (chunkResult != VBP_DESTROY_CHUNK_NONE) {
            return VBP_DESTROY_CORRUPT;
        }
        break;
    }
    if (chunkResult != VBP_DESTROY_CHUNK_NONE) {
        return VBP_DESTROY_CORRUPT;
    }

    LWLockAcquire(&g_vbpCtl->hashViewLock, LW_EXCLUSIVE);
    if (VbpLifecycleRead(vbp) != VbpLifecycleControlPack(generation, VBP_DETACHED, 0) ||
        pg_atomic_read_u32(&vbp->liveEntries) != 0 ||
        pg_atomic_read_u32(&vbp->resizeClaim) != VBP_RESIZE_NONE ||
        !VbpDestroyHashViewsLocked(vbp, releasedViews, &releasedCount)) {
        LWLockRelease(&g_vbpCtl->hashViewLock);
        for (uint32 index = 0; index < releasedCount; ++index) {
            VbpFreeHashViewStorage(&releasedViews[index]);
        }
        return VBP_DESTROY_CORRUPT;
    } else {
        uint32 nextGeneration = generation == PG_UINT32_MAX ? 1 : generation + 1;

        VbpPushDescriptorLocked(vbp, nextGeneration);
    }
    LWLockRelease(&g_vbpCtl->hashViewLock);
    for (uint32 index = 0; index < releasedCount; ++index) {
        VbpFreeHashViewStorage(&releasedViews[index]);
    }
    return VBP_DESTROY_DONE;
}

static void VbpProcessDeferredDestroySafePoint(void)
{
    (void)VectorBufferProcessDeferredDestroys(VBP_DEFERRED_SAFE_POINT_LIMIT);
}

static void VbpResetHandle(VectorBufferHandle *handle)
{
    if (handle == NULL) {
        return;
    }
    handle->data = NULL;
    handle->len = 0;
    handle->pinCookie = 0;
    handle->cached = false;
    handle->access = NULL;
}

static Size VbpShmemSizeFromParams(const VbpSharedConfigInput *input)
{
    VbpSharedConfig config;

    if (!VbpBuildSharedConfig(input, &config)) {
        return 0;
    }
    return config.layout.totalSize;
}

Size VectorBufferShmemSize(void)
{
    Size capacityBytes;
    Size chunkBytes;
    Size size;

    if (g_instance.attr.attr_storage.vectorBuffers <= 0) {
        return 0;
    }
    capacityBytes = mul_size((Size)g_instance.attr.attr_storage.vectorBuffers, (Size)VBP_BYTES_PER_KB);
    chunkBytes = mul_size((Size)g_instance.attr.attr_storage.vbpChunkSize, (Size)VBP_BYTES_PER_KB);
    VbpSharedConfigInput input = {
        capacityBytes,
        chunkBytes,
        (uint32)g_instance.attr.attr_storage.vbpHashPartitions,
        (uint32)g_instance.attr.attr_storage.vbpMinPayload,
        (uint32)g_instance.attr.attr_storage.vbpReclaimScanLimit
    };
    size = VbpShmemSizeFromParams(&input);
    if (size == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid vector buffer shared memory configuration"),
            errdetail("vector_buffers is %d kB, vbp_chunk_size is %d kB, and vbp_min_payload is %d bytes; "
                "the configuration must fit at least one complete aligned entry per chunk.",
                g_instance.attr.attr_storage.vectorBuffers,
                g_instance.attr.attr_storage.vbpChunkSize,
                g_instance.attr.attr_storage.vbpMinPayload)));
    }
    return size;
}

void VectorBufferShmemInit(void)
{
    VbpSharedConfig config;
    Size capacityBytes;
    Size chunkBytes;
    Size size;
    bool found = false;
    char *sharedBase;

    if (g_vbpCtl != NULL) {
        if (!VbpEnsureHashRuntimeAttached()) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("could not attach vector buffer hash runtime")));
        }
        return;
    }
    if (g_instance.attr.attr_storage.vectorBuffers <= 0) {
        return;
    }

    capacityBytes = mul_size((Size)g_instance.attr.attr_storage.vectorBuffers, (Size)VBP_BYTES_PER_KB);
    chunkBytes = mul_size((Size)g_instance.attr.attr_storage.vbpChunkSize, (Size)VBP_BYTES_PER_KB);
    VbpSharedConfigInput input = {
        capacityBytes,
        chunkBytes,
        (uint32)g_instance.attr.attr_storage.vbpHashPartitions,
        (uint32)g_instance.attr.attr_storage.vbpMinPayload,
        (uint32)g_instance.attr.attr_storage.vbpReclaimScanLimit
    };
    if (!VbpBuildSharedConfig(&input, &config)) {
        (void)VectorBufferShmemSize();
        return;
    }
    size = config.layout.totalSize;
    sharedBase = (char *)ShmemInitStruct(VBP_SHMEM_NAME, size, &found);
    if (found) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("vector buffer chunk arena was initialized without its metadata context")));
    }
    VbpInitializeSharedMemory(sharedBase, &config);
    on_proc_exit(VbpChunkFreelistCleanup, (Datum)0);
    if (!VbpEnsureHashRuntimeAttached()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("could not attach vector buffer hash runtime")));
    }
}

static VectorBufferAccess *VbpCreateAccess(uint32 payloadLen)
{
    ResourceOwner owner = t_thrd.utils_cxt.CurrentResourceOwner;
    MemoryContext accessContext;
    VectorBufferAccess *access;

    if (owner == NULL) {
        access = (VectorBufferAccess *)palloc0(sizeof(VectorBufferAccess));
        VbpSetPrivateAccess(access, NULL, NULL, payloadLen);
        return access;
    }
    ResourceOwnerEnlargeVectorBufferAccesses(owner);
    accessContext = AllocSetContextCreate(ResourceOwnerGetMemCxt(owner),
        "VectorBufferAccess",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
    access = (VectorBufferAccess *)MemoryContextAllocZero(accessContext, sizeof(VectorBufferAccess));
    VbpSetPrivateAccess(access, owner, accessContext, payloadLen);
    return access;
}

static bool VbpHandleDirectoryFailure(VectorBufferAccess *access, const VbpRelationKey *key,
    uint32 payloadLen, VbpDirectoryResult result, const VbpDirectoryLookup *lookup)
{
    VbpDeleteAccessStorage(access);
    if (result == VBP_DIRECTORY_LAYOUT_MISMATCH) {
        VbpReportPayloadLayoutMismatch(key, lookup->existingPayloadLen, payloadLen);
    } else if (result == VBP_DIRECTORY_REFCOUNT_EXHAUSTED) {
        VbpReportScanRefExhausted(key);
    }
    return false;
}

static void VbpDiscardUnusedCandidate(VectorBufferAccess *access, VbpId candidate)
{
    PG_TRY();
    {
        VbpDiscardCandidate(candidate);
    }
    PG_CATCH();
    {
        VbpReleaseScanRef(access->vbp, access->vbpGeneration);
        access->vbp = NULL;
        access->vbpGeneration = 0;
        VbpDeleteAccessStorage(access);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static bool VbpPublishCandidateAccess(VectorBufferAccess *local, const VbpRelationKey *key,
    uint32 payloadLen, VectorBufferAccess **access)
{
    VbpDirectoryLookup lookup = {VBP_INVALID_INDEX, 0};
    VbpId candidate = VbpPrepareCandidate(key, payloadLen);
    VbpDirectoryResult result;

    if (candidate == VBP_INVALID_INDEX) {
        return VbpFinishBeginAccess(local, access);
    }
    result = VbpDirectoryPublishCandidate(key, payloadLen, candidate, &lookup);
    if (result == VBP_DIRECTORY_ACQUIRED) {
        VbpSetSharedAccess(local, lookup.id);
        if (lookup.id != candidate) {
            VbpDiscardUnusedCandidate(local, candidate);
        }
        return VbpFinishBeginAccess(local, access);
    }
    VbpDiscardCandidate(candidate);
    return VbpHandleDirectoryFailure(local, key, payloadLen, result, &lookup);
}

bool VectorBufferBeginAccess(const RelFileNode *rnode, uint32 payloadLen, VectorBufferAccess **access)
{
    VectorBufferAccess *local;
    VbpRelationKey key;
    VbpDirectoryResult result;
    VbpDirectoryLookup lookup = {VBP_INVALID_INDEX, 0};

    if (access == NULL) {
        return false;
    }
    *access = NULL;
    if (rnode == NULL || payloadLen == 0) {
        return false;
    }
    local = VbpCreateAccess(payloadLen);
    if (local->owner == NULL) {
        *access = local;
        return true;
    }
    /* The shared directory is keyed by relation, not by bucket. Bucket files
     * can have identical payload TIDs, so retain the borrowed-buffer path. */
    if (IsBucketFileNode(*rnode) || !VbpSharedAccessEnabled() || !VbpEnsureHashRuntimeAttached()) {
        return VbpFinishBeginAccess(local, access);
    }
    VbpProcessDeferredDestroySafePoint();

    key = VbpMakeRelationKey(rnode);
    result = VbpDirectoryAcquireExisting(&key, payloadLen, &lookup);
    if (result == VBP_DIRECTORY_ACQUIRED) {
        VbpSetSharedAccess(local, lookup.id);
        return VbpFinishBeginAccess(local, access);
    }
    if (result != VBP_DIRECTORY_MISSING) {
        return VbpHandleDirectoryFailure(local, &key, payloadLen, result, &lookup);
    }
    if (!VbpPayloadFitsShared(payloadLen)) {
        return VbpFinishBeginAccess(local, access);
    }
    return VbpPublishCandidateAccess(local, &key, payloadLen, access);
}

bool VectorBufferAccessIsIdle(const VectorBufferAccess *access)
{
    return access != NULL && access->mode == VBP_ACCESS_IDLE && access->pinCookie == 0;
}

void VectorBufferReleaseOwnerAccessNoForget(VectorBufferAccess *access, bool isCommit)
{
    if (access == NULL) {
        return;
    }
    switch (access->mode) {
        case VBP_ACCESS_CACHED_PIN:
            (void)VbpReleaseActivePin(access);
            break;
        case VBP_ACCESS_RESERVED:
            VbpReleaseReservation(access);
            break;
        case VBP_ACCESS_BORROWED_PIN:
            VbpReleaseBorrowedPin(access, isCommit);
            break;
        case VBP_ACCESS_IDLE:
        default:
            VbpAccessSetIdle(access);
            break;
    }
    if (access->vbp != NULL) {
        VbpInstance *vbp = access->vbp;
        uint32 generation = access->vbpGeneration;
        access->vbp = NULL;
        access->vbpGeneration = 0;
        VbpReleaseScanRef(vbp, generation);
    }
    access->owner = NULL;
    if (g_vbpCtl != NULL) {
        if (access->statHits != 0) {
            (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.hits, access->statHits);
        }
        if (access->statMisses != 0) {
            (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.misses, access->statMisses);
        }
        if (access->statInstalls != 0) {
            (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.installs, access->statInstalls);
        }
        if (access->statFallbacks != 0) {
            (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.fallbacks, access->statFallbacks);
        }
        if (access->statEvictions != 0) {
            (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.evictions, access->statEvictions);
        }
    }
    VbpDeleteAccessStorage(access);
}

void VectorBufferReleaseOwnerAccess(VectorBufferAccess *access, bool isCommit)
{
    if (access == NULL) {
        return;
    }
    ResourceOwnerForgetVectorBufferAccess(access->owner, access);
    VectorBufferReleaseOwnerAccessNoForget(access, isCommit);
}

void VectorBufferEndAccess(VectorBufferAccess **access)
{
    VectorBufferAccess *local;

    if (access == NULL || *access == NULL) {
        return;
    }
    local = *access;
    ResourceOwnerForgetVectorBufferAccess(local->owner, local);
    *access = NULL;
    VectorBufferReleaseOwnerAccessNoForget(local, true);
    VbpProcessDeferredDestroySafePoint();
}

void VectorBufferReassignOwnerAccess(VectorBufferAccess *access, ResourceOwner owner)
{
    if (access == NULL) {
        return;
    }
    Assert(owner != NULL);
    Assert(access->accessContext != NULL);
    if (owner == NULL || access->accessContext == NULL) {
        return;
    }
    MemoryContextSetParent(access->accessContext, ResourceOwnerGetMemCxt(owner));
    access->owner = owner;
}

static bool VbpSetCachedHandle(VectorBufferAccess *access, VbpEntryRef ref,
    const char *payload, VectorBufferHandle *handle)
{
    if (access == NULL || handle == NULL || access->mode != VBP_ACCESS_CACHED_PIN ||
        access->state.entry.ref != ref || payload == NULL || access->pinCookie == 0) {
        return false;
    }
    handle->data = payload;
    handle->len = access->payloadLen;
    handle->pinCookie = access->pinCookie;
    handle->cached = true;
    handle->access = access;
    return true;
}

static void VbpReleaseBorrowedPin(VectorBufferAccess *access, bool isCommit)
{
    VbpAccessBorrowedPin borrowed;

    Assert(access != NULL && access->mode == VBP_ACCESS_BORROWED_PIN);
    borrowed = access->state.borrowed;
    VbpAccessSetIdle(access);
    if (borrowed.end != NULL && borrowed.guard.active) {
        borrowed.end(&borrowed.guard, isCommit);
    }
}

static void VbpSetBorrowedHandle(VectorBufferAccess *access, const VectorBufferLoadGuard *guard,
    const VectorBufferLoadOps *loadOps, VectorBufferHandle *handle)
{
    Assert(access != NULL && guard != NULL && guard->active && guard->data != NULL);
    Assert(loadOps != NULL && loadOps->end != NULL && handle != NULL);

    access->state.borrowed.guard = *guard;
    access->state.borrowed.end = loadOps->end;
    access->mode = VBP_ACCESS_BORROWED_PIN;
    access->pinCookie = VbpNextPinCookie();
    access->statFallbacks++;
    handle->data = guard->data;
    handle->len = access->payloadLen;
    handle->pinCookie = access->pinCookie;
    handle->cached = false;
    handle->access = access;
}

static bool VbpLoadBorrowed(VectorBufferAccess *access, const ItemPointerData *payloadTid,
    const VectorBufferLoadOps *loadOps, void *loaderCtx, VectorBufferHandle *handle)
{
    volatile VectorBufferLoadGuard guard;
    volatile bool guardActive = false;
    volatile bool guardEndCalled = false;
    volatile bool guardTransferred = false;
    volatile bool loaded = false;
    VectorBufferLoadGuard *mutableGuard = (VectorBufferLoadGuard *)&guard;

    mutableGuard->data = NULL;
    mutableGuard->opaque[0] = 0;
    mutableGuard->opaque[1] = 0;
    mutableGuard->active = false;
    PG_TRY();
    {
        bool began = loadOps->begin(payloadTid, access->payloadLen, loaderCtx, mutableGuard);

        guardActive = mutableGuard->active;
        if (began && guardActive && mutableGuard->data != NULL) {
            VbpSetBorrowedHandle(access, mutableGuard, loadOps, handle);
            guardTransferred = true;
            guardActive = false;
            loaded = true;
        } else if (guardActive) {
            guardActive = false;
            guardEndCalled = true;
            loadOps->end(mutableGuard, true);
        }
    }
    PG_CATCH();
    {
        if (!guardTransferred && !guardEndCalled && (guardActive || guard.active)) {
            guardActive = false;
            guardEndCalled = true;
            loadOps->end(mutableGuard, true);
        }
        if (guardTransferred && access->mode == VBP_ACCESS_BORROWED_PIN) {
            VbpReleaseBorrowedPin(access);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!loaded) {
        return false;
    }
    return true;
}

static void VbpLoadReservedMiss(const VbpMissRequest *request, VbpReservedMissState *state)
{
    VectorBufferAccess *access = request->access;
    VectorBufferLoadGuard *guard = &state->guard;
    bool began = request->loadOps->begin(request->payloadTid, access->payloadLen,
        request->loaderCtx, guard);

    state->guardActive = guard->active;
    if (!began || !state->guardActive || guard->data == NULL) {
        VbpReleaseReservation(access);
        if (state->guardActive) {
            state->guardActive = false;
            state->guardEndCalled = true;
            request->loadOps->end(guard, true);
        }
        return;
    }

    Assert(access->mode == VBP_ACCESS_RESERVED);
    VbpEntry *reservedEntry = access->mode == VBP_ACCESS_RESERVED ? access->state.entry.entry : NULL;
    bool publishSucceeded = false;

    if (reservedEntry != NULL) {
        ItemPointerSet(&reservedEntry->payloadTid,
            ItemPointerGetBlockNumberNoCheck(request->payloadTid),
            ItemPointerGetOffsetNumberNoCheck(request->payloadTid));
        securec_check(memcpy_s(VbpEntryPayload(reservedEntry), access->payloadLen,
            guard->data, access->payloadLen), "\0", "\0");
        publishSucceeded = VbpHashPublishReserved(access->vbp, request->payloadTid,
            access->state.entry.ref, access, &state->publish);
    }
    if (publishSucceeded) {
        state->guardActive = false;
        state->guardEndCalled = true;
        request->loadOps->end(guard, true);
        return;
    }
    VbpReleaseReservation(access);
    VbpSetBorrowedHandle(access, guard, request->loadOps, request->handle);
    state->guardActive = false;
}

static void VbpCleanupReservedMissError(const VbpMissRequest *request, VbpReservedMissState *state)
{
    (void)VbpReleaseActivePin(request->access);
    VbpReleaseReservation(request->access);
    if (request->access->mode == VBP_ACCESS_BORROWED_PIN) {
        VbpReleaseBorrowedPin(request->access);
    } else if (!state->guardEndCalled && (state->guardActive || state->guard.active)) {
        state->guardActive = false;
        state->guardEndCalled = true;
        request->loadOps->end(&state->guard, true);
    }
}

static bool VbpFinishReservedMiss(const VbpMissRequest *request, const VbpReservedMissState *state)
{
    if (request->access->mode == VBP_ACCESS_BORROWED_PIN) {
        return true;
    }
    if (request->access->mode != VBP_ACCESS_CACHED_PIN) {
        return false;
    }
    if (!VbpSetCachedHandle(request->access, state->publish.winner,
            state->publish.payload, request->handle)) {
        (void)VbpReleaseActivePin(request->access);
        return false;
    }
    if (state->publish.published) {
        request->access->statInstalls++;
    }
    return true;
}

static bool VbpPinReservedMiss(VectorBufferAccess *access, const ItemPointerData *payloadTid,
    const VectorBufferLoadOps *loadOps, void *loaderCtx, VectorBufferHandle *handle)
{
    VbpMissRequest request = {access, payloadTid, loadOps, loaderCtx, handle};
    volatile VbpReservedMissState state = {};
    VbpReservedMissState *mutableState = (VbpReservedMissState *)&state;

    PG_TRY();
    {
        VbpLoadReservedMiss(&request, mutableState);
    }
    PG_CATCH();
    {
        VbpCleanupReservedMissError(&request, mutableState);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return VbpFinishReservedMiss(&request, mutableState);
}

static void VbpMaybeResizeAfterMiss(VbpInstance *vbp)
{
    if (vbp == NULL || VbpInstanceState(vbp) != VBP_ACTIVE) {
        return;
    }
    if (VbpHashNeedsResize(vbp)) {
        uint32 candidateDesc = VBP_INVALID_INDEX;

        if (VbpHashAllocateResizeCandidate(vbp, &candidateDesc)) {
            if (!VbpHashStartResize(vbp, candidateDesc)) {
                (void)VbpHashReleaseUnpublishedView(vbp, candidateDesc);
            }
        }
    }
    (void)VbpHashMigrateNext(vbp);
}

bool VectorBufferPinFast(VectorBufferAccess *access, const ItemPointerData *payloadTid,
    const VectorBufferLoadOps *loadOps, void *loaderCtx, VectorBufferHandle *handle)
{
    VbpEntryRef entryRef = VBP_INVALID_ENTRY_REF;
    const char *payload = NULL;

    /* An uncached handle borrows loadOps' guard until Release or EndAccess. */
    VbpResetHandle(handle);
    if (access == NULL || payloadTid == NULL || loadOps == NULL || loadOps->begin == NULL ||
        loadOps->end == NULL || handle == NULL || access->payloadLen == 0 ||
        !ItemPointerIsValid(payloadTid) || access->mode != VBP_ACCESS_IDLE || access->pinCookie != 0) {
        return false;
    }
    if (access->vbp != NULL && VbpSharedAccessEnabled() &&
        VbpInstanceLifecycleMatches(access->vbp, access->vbpGeneration, VBP_ACTIVE)) {
        if (VbpHashTryPin(access->vbp, payloadTid, access, &entryRef, &payload)) {
            access->statHits++;
            if (VbpSetCachedHandle(access, entryRef, payload, handle)) {
                /*
                 * Pin is a refcount, not a lock; MigrateOne takes one partition
                 * spinlock and does not nest other VBP locks.
                 */
                VbpHashMigrateAssist(access->vbp);
                return true;
            }
            (void)VbpReleaseActivePin(access);
            return false;
        }
        access->statMisses++;
        if (VbpReserveEntry(access->vbp, access)) {
            bool pinned = VbpPinReservedMiss(access, payloadTid, loadOps, loaderCtx, handle);
            if (pinned && handle->cached) {
                VbpMaybeResizeAfterMiss(access->vbp);
            }
            return pinned;
        }
    }
    return VbpLoadBorrowed(access, payloadTid, loadOps, loaderCtx, handle);
}

void VectorBufferRelease(VectorBufferHandle *handle)
{
    VectorBufferAccess *access;

    if (handle == NULL) {
        return;
    }
    access = handle->access;
    if (access != NULL && handle->pinCookie != 0 && access->pinCookie == handle->pinCookie) {
        if (handle->cached && access->mode == VBP_ACCESS_CACHED_PIN) {
            (void)VbpReleaseActivePin(access);
        } else if (!handle->cached && access->mode == VBP_ACCESS_BORROWED_PIN &&
            access->state.borrowed.guard.data == handle->data) {
            VbpReleaseBorrowedPin(access);
        }
    }
    VbpResetHandle(handle);
}

VectorBufferInvalidateResult VectorBufferInvalidatePayload(const RelFileNode *rnode,
    const ItemPointerData *payloadTid)
{
    VbpRelationKey key;
    VbpInstance *vbp = NULL;
    uint32 generation = 0;
    VbpHashInvalidateResult result = {VBP_INVALID_ENTRY_REF, 0, false};

    if (rnode == NULL || IsBucketFileNode(*rnode) || payloadTid == NULL || !ItemPointerIsValid(payloadTid) ||
        g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC || !VbpEnsureHashRuntimeAttached()) {
        return VECTOR_BUFFER_INVALIDATE_NOT_FOUND;
    }
    (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.invalidations, 1);
    key = VbpMakeRelationKey(rnode);
    if (!VbpDirectoryAcquirePayloadInvalidationRef(&key, &vbp, &generation)) {
        return VECTOR_BUFFER_INVALIDATE_NOT_FOUND;
    }
    if (!VbpHashInvalidatePayload(vbp, generation, payloadTid, &result)) {
        VbpReleaseScanRef(vbp, generation);
        return VECTOR_BUFFER_INVALIDATE_NOT_FOUND;
    }

    if (result.recycleOwned) {
        VbpRecycleEntryOwned(vbp, result.removed, result.incarnation, NULL);
    }
    (void)pg_atomic_fetch_add_u64((volatile uint64 *)&g_vbpCtl->stats.invalidatedEntries, 1);
    VbpReleaseScanRef(vbp, generation);
    return result.recycleOwned ? VECTOR_BUFFER_INVALIDATE_DONE : VECTOR_BUFFER_INVALIDATE_DEFERRED;
}

bool VectorBufferIsActive(void)
{
    return g_vbpCtl != NULL && g_vbpCtl->magic == VBP_MAGIC;
}

void VectorBufferInvalidateRelation(const RelFileNode *rnode)
{
    VbpRelationKey key;
    VbpInstance *vbp = NULL;
    uint32 generation = 0;
    VbpDetachResult result;

    if (rnode == NULL || IsBucketFileNode(*rnode) || g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC ||
        !VbpEnsureHashRuntimeAttached()) {
        return;
    }
    key = VbpMakeRelationKey(rnode);
    result = VbpDirectoryDetach(&key, &vbp, &generation);
    if (result == VBP_DETACH_REFCOUNT_EXHAUSTED) {
        VbpReportScanRefExhausted(&key);
        return;
    }
    if (result == VBP_DETACH_RESIZE_BUSY) {
        VbpReportDetachResizeBusy(&key);
        return;
    }
    if (result != VBP_DETACH_DONE) {
        return;
    }

    if (!VbpHashDrainDetached(vbp, generation)) {
        (void)VbpMarkLifecycleCorrupt(vbp, generation);
    }
    VbpReleaseDetachGate(vbp);
    VbpReleaseScanRef(vbp, generation);
    VbpProcessDeferredDestroySafePoint();
}

static uint32 VectorBufferProcessDeferredDestroys(uint32 limit)
{
    uint32 destroyed = 0;
    uint32 attempts = 0;
    VbpId failedHead = VBP_INVALID_INDEX;

    if (limit == 0 || g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC ||
        !VbpEnsureHashRuntimeAttached()) {
        return 0;
    }

    while (attempts < limit) {
        VbpId id;
        uint32 generation;
        VbpInstance *vbp;

        if (!VbpPopDeferredDestroy(&id, &generation)) {
            break;
        }
        ++attempts;
        if (id >= g_vbpCtl->maxVbps) {
            continue;
        }
        vbp = &g_vbpInstances[id];
        if (VbpLifecycleRead(vbp) != VbpLifecycleControlPack(generation, VBP_DETACHED, 0)) {
            continue;
        }
        VbpDestroyResult result = VbpDestroyInstance(vbp, generation);
        if (result == VBP_DESTROY_DONE) {
            ++destroyed;
            continue;
        }
        if (result == VBP_DESTROY_RETRY &&
            VbpLifecycleRead(vbp) == VbpLifecycleControlPack(generation, VBP_DETACHED, 0)) {
            vbp->deferredNext = failedHead;
            failedHead = id;
        } else {
            (void)VbpMarkLifecycleCorrupt(vbp, generation);
        }
    }

    while (failedHead != VBP_INVALID_INDEX) {
        VbpInstance *vbp;
        VbpId next;
        uint32 generation;

        if (failedHead >= g_vbpCtl->maxVbps) {
            break;
        }
        vbp = &g_vbpInstances[failedHead];
        next = vbp->deferredNext;
        generation = VbpInstanceGeneration(vbp);
        vbp->deferredNext = VBP_INVALID_INDEX;
        (void)VbpPushDeferredDestroy(vbp, generation);
        failedHead = next;
    }
    return destroyed;
}

static void VbpCollectPoolUsage(VbpInstance *vbp, uint32 generation,
    uint32 *nFree, uint32 *nOccupied, uint32 *nNonEmpty)
{
    uint32 chunkIndex;
    uint32 visited = 0;
    uint32 slotCount;

    Assert(vbp != NULL && nFree != NULL && nOccupied != NULL);
    if (vbp == NULL || nFree == NULL || nOccupied == NULL) {
        return;
    }
    slotCount = vbp->payloadLayout.slotsPerChunk;
    VbpChunkLockAcquire(vbp, LW_SHARED);
    chunkIndex = vbp->clHead;
    while (chunkIndex != VBP_INVALID_INDEX && visited++ < g_vbpCtl->chunkCount) {
        if (chunkIndex >= g_vbpCtl->chunkCount) {
            break;
        }
        VbpMemChunk *chunk = &g_vbpChunks[chunkIndex];
        uint32 next = chunk->clNext;
        uint32 freeCount = pg_atomic_read_u32(&chunk->nFree);
        if ((VbpChunkControlRead(chunk) & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE &&
            chunk->vbpId == vbp->id && chunk->vbpGeneration == generation &&
            freeCount <= slotCount) {
            *nFree += freeCount;
            *nOccupied += slotCount - freeCount;
            if (nNonEmpty != NULL && freeCount != slotCount) {
                (*nNonEmpty)++;
            }
        }
        chunkIndex = next;
    }
    VbpChunkLockRelease(vbp);
}

void VectorBufferGetStats(VectorBufferStats *stats)
{
    if (stats == NULL) {
        return;
    }
    securec_check(memset_s(stats, sizeof(VectorBufferStats), 0, sizeof(VectorBufferStats)), "\0", "\0");
    if (g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC) {
        return;
    }
    stats->hits = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.hits);
    stats->misses = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.misses);
    stats->installs = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.installs);
    stats->fallbacks = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.fallbacks);
    stats->invalidations = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.invalidations);
    stats->invalidatedEntries =
        pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.invalidatedEntries);
    stats->evictions = pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.evictions);
    stats->poolFullRequests =
        pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.poolFullRequests);
    stats->reclaimAttempts =
        pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.reclaimAttempts);
    stats->reclaimVictims =
        pg_atomic_read_u64((volatile uint64 *)&g_vbpCtl->stats.reclaimVictims);
    stats->capacityBytes = g_vbpCtl->capacityBytes;
    for (uint32 id = 0; id < g_vbpCtl->maxVbps; ++id) {
        VbpInstance *vbp = &g_vbpInstances[id];
        uint32 generation;
        uint32 nFree = 0;
        uint32 nOccupied = 0;

        if (!VbpTryAcquireScanRef(vbp)) {
            continue;
        }
        generation = VbpInstanceGeneration(vbp);
        stats->entries += pg_atomic_read_u32(&vbp->liveEntries);
        if ((pg_atomic_read_u32(&vbp->workFlags) & VBP_WORK_RECLAIM_REQUESTED) != 0) {
            stats->evictPending++;
        }
        VbpCollectPoolUsage(vbp, generation, &nFree, &nOccupied, NULL);
        stats->nFreeTotal += nFree;
        stats->usedBytes += (uint64)nOccupied * vbp->payloadLayout.slotSize;
        VbpReleaseScanRef(vbp, generation);
    }
}

void VectorBufferGetGlobalStat(VectorBufferGlobalStat *stats)
{
    if (stats == NULL) {
        return;
    }
    securec_check(memset_s(stats, sizeof(VectorBufferGlobalStat), 0, sizeof(VectorBufferGlobalStat)), "\0", "\0");
    VectorBufferGetStats(&stats->stats);
    if (g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC || g_vbpInstances == NULL) {
        return;
    }
    stats->configuredCapacityBytes = g_vbpCtl->configuredCapacityBytes;
    stats->chunkSize = g_vbpCtl->chunkSize;
    stats->chunkCount = g_vbpCtl->chunkCount;
    stats->maxVbps = g_vbpCtl->maxVbps;
    stats->minPayload = g_vbpCtl->minPayload;
    for (uint32 id = 0; id < g_vbpCtl->maxVbps; ++id) {
        if (VbpInstanceState(&g_vbpInstances[id]) == VBP_ACTIVE) {
            stats->activePools++;
        }
    }
}

uint32 VectorBufferCopyPoolStats(VectorBufferPoolStat *out, uint32 capacity)
{
    uint32 n = 0;

    if (out == NULL || capacity == 0 || g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC ||
        g_vbpInstances == NULL) {
        return 0;
    }
    for (uint32 id = 0; id < g_vbpCtl->maxVbps && n < capacity; ++id) {
        VbpInstance *vbp = &g_vbpInstances[id];
        VectorBufferPoolStat *row;
        uint64 lifecycle;
        uint32 generation;
        if (!VbpTryAcquireScanRef(vbp)) {
            continue;
        }
        lifecycle = VbpLifecycleRead(vbp);
        generation = VbpLifecycleGeneration(lifecycle);
        row = &out[n++];
        row->vbpId = vbp->id;
        row->generation = generation;
        row->state = VbpLifecycleState(lifecycle);
        row->spcNode = vbp->relationKey.spcNode;
        row->dbNode = vbp->relationKey.dbNode;
        row->relNode = vbp->relationKey.relNode;
        row->payloadLen = vbp->payloadLayout.payloadLen;
        row->slotSize = vbp->payloadLayout.slotSize;
        row->liveEntries = pg_atomic_read_u32(&vbp->liveEntries);
        row->nFreeTotal = 0;
        row->nOccupiedTotal = 0;
        row->nChunksUsed = 0;
        VbpCollectPoolUsage(vbp, generation, &row->nFreeTotal,
            &row->nOccupiedTotal, &row->nChunksUsed);
        row->scanRefs = VbpLifecycleRefs(VbpLifecycleRead(vbp)) - 1;
        row->evictRequested =
            (pg_atomic_read_u32(&vbp->workFlags) & VBP_WORK_RECLAIM_REQUESTED) != 0;
        VbpReleaseScanRef(vbp, generation);
    }
    return n;
}

static void VbpCollectChunkEntryStats(VbpInstance *vbp, VbpMemChunk *chunk,
    VectorBufferChunkStat *row)
{
    char *slotBase = VbpChunkSlotBase(chunk);

    if (slotBase == NULL) {
        return;
    }
    for (uint32 slot = 0; slot < vbp->payloadLayout.slotsPerChunk; ++slot) {
        VbpEntry *entry = (VbpEntry *)(slotBase + (Size)slot * vbp->payloadLayout.slotSize);

        switch (VbpEntryControlState(pg_atomic_read_u64(&entry->control))) {
            case VBP_ENTRY_CLOSED:
                row->nReserved++;
                break;
            case VBP_ENTRY_CACHED:
                row->nCached++;
                break;
            case VBP_ENTRY_QUARANTINED:
                row->nQuarantined++;
                break;
            default:
                break;
        }
    }
}

uint32 VectorBufferCopyChunkStats(VectorBufferChunkStat *out, uint32 capacity)
{
    uint32 n = 0;

    if (out == NULL || capacity == 0 || g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC ||
        g_vbpChunks == NULL) {
        return 0;
    }
    for (uint32 chunkIndex = 0; chunkIndex < g_vbpCtl->chunkCount && n < capacity; ++chunkIndex) {
        VbpMemChunk *chunk = &g_vbpChunks[chunkIndex];
        VectorBufferChunkStat *row;
        uint32 control = VbpChunkControlRead(chunk);

        row = &out[n++];
        row->vbpId = VBP_INVALID_INDEX;
        row->vbpGeneration = 0;
        row->chunkIndex = chunkIndex;
        row->state = control & VBP_CHUNK_STATE_MASK;
        row->nFree = 0;
        row->nReserved = 0;
        row->nCached = 0;
        row->nQuarantined = 0;
        row->slotCount = 0;
        row->slotStride = 0;
        row->inCl = false;
        row->inFreelist = (control & VBP_CHUNK_IN_FREELIST_BIT) != 0;
        if (VbpChunkTryAcquireAllocatorRef(chunk)) {
            VbpId vbpId = chunk->vbpId;

            control = VbpChunkControlRead(chunk);
            if ((control & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE &&
                vbpId < g_vbpCtl->maxVbps) {
                VbpInstance *vbp = &g_vbpInstances[vbpId];

                row->vbpId = vbpId;
                row->vbpGeneration = chunk->vbpGeneration;
                row->nFree = pg_atomic_read_u32(&chunk->nFree);
                row->slotCount = vbp->payloadLayout.slotsPerChunk;
                row->slotStride = vbp->payloadLayout.slotSize;
                row->inFreelist = (control & VBP_CHUNK_IN_FREELIST_BIT) != 0;
                VbpCollectChunkEntryStats(vbp, chunk, row);
                VbpChunkLockAcquire(vbp, LW_SHARED);
                row->inCl = VbpChunkInCl(vbp, chunk, chunkIndex);
                VbpChunkLockRelease(vbp);
            }
            VbpChunkReleaseAllocatorRef(chunk);
        }
    }
    return n;
}

uint32 VectorBufferCopyHashChainStats(VectorBufferHashChainStat *out, uint32 capacity)
{
    uint32 n = 0;

    if (out == NULL || capacity == 0 || g_vbpCtl == NULL || g_vbpCtl->magic != VBP_MAGIC ||
        g_vbpInstances == NULL || !VbpEnsureHashRuntimeAttached()) {
        return 0;
    }
    for (uint32 id = 0; id < g_vbpCtl->maxVbps && n < capacity; ++id) {
        VbpInstance *vbp = &g_vbpInstances[id];
        uint32 generation;

        if (!VbpTryAcquireScanRef(vbp)) {
            continue;
        }
        generation = VbpInstanceGeneration(vbp);
        if (VbpHashCollectChainStat(vbp, &out[n])) {
            n++;
        }
        VbpReleaseScanRef(vbp, generation);
    }
    return n;
}
