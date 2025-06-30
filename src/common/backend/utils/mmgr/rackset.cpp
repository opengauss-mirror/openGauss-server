/* -------------------------------------------------------------------------
 *
 * rackset.c
 *	  Allocation rack memory set definitions.
 *
 * AllocSet is our rackset implementation of the abstract MemoryContext
 * type.
 *
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Mulan Permissive Software License，Version 2
 * Mulan Permissive Software License，Version 2 (Mulan PSL v2)
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/rackset.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/mman.h>
#include <ctime>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "port/pg_bitutils.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/mmpool.h"
#include "utils/aset.h"
#include "gssignal/gs_signal.h"
#include "gs_register/gs_malloc.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "utils/memprot.h"
#include "utils/memtrack.h"
#include "storage/procarray.h"
#include "storage/matrix_mem.h"
#include "postmaster/rack_mem_cleaner.h"

#define MAX_RACK_MEMORY_CHUNK_SIZE MIN_RACK_ALLOC_SIZE
#define MAX_RACK_MEMORY_ALLOC_SIZE (MAX_RACK_MEMORY_CHUNK_SIZE - sizeof(RackPrefix))

/* Define this to detail debug alloc information .  HAVE_ALLOCINFO */

/* --------------------
 * Chunk freelist k holds chunks of size 1 << (k + ALLOC_MINBITS),
 * for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
 *
 * Note that all chunks in the freelists have power-of-2 sizes.  This
 * improves recyclability: we may waste some space, but the wasted space
 * should stay pretty constant as requests are made and released.
 *
 * A request too large for the last freelist is handled by allocating a
 * dedicated block from malloc().  The block still has a block header and
 * chunk header, but when the chunk is freed we'll return the whole block
 * to malloc(), not put it on our freelists.
 *
 * CAUTION: ALLOC_MINBITS must be large enough so that
 * 1<<ALLOC_MINBITS is at least MAXALIGN,
 * or we may fail to align the smallest chunks adequately.
 * 8-byte alignment is enough on all currently known machines.
 *
 * With the current parameters, request sizes up to 8K are treated as chunks,
 * larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
 * to adjust the boundary point.  (But in contexts with small maxBlockSize,
 * we may set the allocChunkLimit to less than 8K, so as to avoid space
 * wastage.)
 * --------------------
 */

#define ALLOC_MINBITS 3 /* smallest chunk size is 8 bytes */
#define ALLOCSET_NUM_FREELISTS 11
/* We allow chunks to be at most 1/4 of maxBlockSize (less overhead) */

/* --------------------
 * The first block allocated for an allocset has size initBlockSize.
 * Each time we have to allocate another block, we double the block size
 * (if possible, and without exceeding maxBlockSize), so as to reduce
 * the bookkeeping load on malloc().
 *
 * Blocks allocated to hold oversize chunks do not follow this rule, however;
 * they are just however big they need to be to hold that single chunk.
 * --------------------
 */

#ifdef MEMORY_CONTEXT_CHECKING
#define ALLOC_MAGICHDRSZ MAXALIGN(sizeof(AllocMagicData))
#else
#define ALLOC_MAGICHDRSZ 0
#endif

#ifdef MEMORY_CONTEXT_CHECKING
const uint32 PosmagicNum = 0xDCDCDCDC;
typedef struct AllocMagicData {
    void* aset;
    uint32 size;  // chunk size
    uint32 posnum;
} AllocMagicData;
#endif

/*
 * AllocSetIsValid
 *		True iff set is valid allocation set.
 */
#define AllocSetIsValid(set) PointerIsValid(set)

#define AllocPointerGetChunk(ptr) ((AllocChunk)(((char*)(ptr)) - ALLOC_CHUNKHDRSZ))
#define AllocChunkGetPointer(chk) ((AllocPointer)(((char*)(chk)) + ALLOC_CHUNKHDRSZ))

/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define AllocFreeInfo(_cxt, _chunk) \
    fprintf(stderr, "AllocFree: %s: %p, %d\n", (_cxt)->header.name, (_chunk), (_chunk)->size)
#define AllocAllocInfo(_cxt, _chunk) \
    fprintf(stderr, "AllocAlloc: %s: %p, %d\n", (_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define AllocFreeInfo(_cxt, _chunk)
#define AllocAllocInfo(_cxt, _chunk)
#endif

#ifdef MEMORY_CONTEXT_CHECKING
#define CHECK_CONTEXT_OWNER(context) \
    Assert((context->thread_id == gs_thread_self() || (context->session_id == u_sess->session_id)))
#else
#define CHECK_CONTEXT_OWNER(context) ((void)0)
#endif

constexpr int RACKALLOCSET_NUM_FREELISTS = 26;
constexpr float RACKMEM_LIMIT_DENOMINATOR = 2000;
constexpr float RACKMEM_LIMIT_BASE_VALUE = 200;

void check_pointer_valid(AllocBlock block, bool is_shared, MemoryContext context, AllocChunk chunk);

struct RackAllocSetContext : AllocSetContext {
    AllocChunk extern_freelist[RACKALLOCSET_NUM_FREELISTS - ALLOCSET_NUM_FREELISTS];
    MemoryContext local_context;
};

using RackAllocSet = RackAllocSetContext*;

struct RackPrefix {
    Size size;
};

pg_atomic_uint64 rackUsedSize = 0;

bool EnableBorrowWorkMemory()
{
    return g_matrixMemFunc.matrix_mem_inited &&
           g_instance.attr.attr_memory.enable_borrow_memory &&
           u_sess->attr.attr_memory.borrow_work_mem > 0 &&
           g_instance.attr.attr_memory.avail_borrow_mem > 0 &&
           g_instance.rackMemCleanerCxt.rack_available == 1;
}

Size GetAvailRackMemory(int dop)
{
    if (!EnableBorrowWorkMemory()) {
        return 0;
    }
    if (dop <= 0) {
        dop = 1;
    }
    return TYPEALIGN_DOWN(MAX_RACK_MEMORY_ALLOC_SIZE / 1024L, u_sess->attr.attr_memory.borrow_work_mem / dop);
}

static bool TryRackMemFree(void* ptr)
{
    if (u_sess->enable_rack_memory_free_test) {
        RegisterFailedFreeMemory(ptr);
        return false;
    }

    int ret = RackMemFree(ptr);
    if (ret != 0) {
        RegisterFailedFreeMemory(ptr);
        ereport(LOG, (errmsg("RackMemFree failed")));
        return false;
    }
    return true;
}

bool RackMemoryBusy(int64 used)
{
    if (!EnableBorrowWorkMemory()) {
        return true;
    }

    uint64 totalUsed = pg_atomic_read_u64(&rackUsedSize);
    int64 percent = (totalUsed / 1024 * 100) / g_instance.attr.attr_memory.avail_borrow_mem;
    uint64 nodeUsed = used >> BITS_IN_KB;

    if (percent > HIGH_PROCMEM_MARK) {
        return true;
    }

    if (percent >= LOW_PROCMEM_MARK) {
        if (nodeUsed > (double)g_instance.attr.attr_memory.avail_borrow_mem / RACKMEM_LIMIT_DENOMINATOR *
                ((HIGH_PROCMEM_MARK - percent) * (HIGH_PROCMEM_MARK - percent) + RACKMEM_LIMIT_BASE_VALUE)) {
            return true;
        }
    }

    return false;
}

static inline void* RackMallocConverter(Size size)
{
    size += sizeof(RackPrefix);
    size = TYPEALIGN(MAX_RACK_MEMORY_CHUNK_SIZE, size);
    Assert(size % MAX_RACK_MEMORY_CHUNK_SIZE == 0);
    pg_atomic_add_fetch_u64(&rackUsedSize, size);

    RackPrefix* ptr;
    if (u_sess->attr.attr_common.log_min_messages <= DEBUG1) {
        clock_t start;
        clock_t finish;
        start = clock();
        ptr = (RackPrefix*)RackMemMalloc(size, RackMemPerfLevel::L0, 0);
        finish = clock();
        double timeused = static_cast<double>(finish - start) / CLOCKS_PER_SEC;
        ereport(LOG, (errmsg("RackMallocConverter: RackMemMalloc used %fs to alloc memory from remote", timeused)));
    } else {
        ptr = (RackPrefix*)RackMemMalloc(size, RackMemPerfLevel::L0, 0);
    }

    if (!ptr) {
        pg_atomic_sub_fetch_u64(&rackUsedSize, size);
        ereport(LOG, (errmsg("RackMallocConverter: try alloc from rack failed")));
        return nullptr;
    }
    ptr->size = size;
    return reinterpret_cast<void*>(ptr + 1);
}

static inline void RackFreeConverter(void* ptr)
{
    Assert(ptr);
    RackPrefix* prefix = (RackPrefix*)(ptr - sizeof(RackPrefix));
    Size size = prefix->size;
    TryRackMemFree((void*)prefix);

    pg_atomic_sub_fetch_u64(&rackUsedSize, size);
}

static inline void* RackReallocConverter(void* ptr, Size ptrsize, Size newSize)
{
    void* newptr = RackMallocConverter(newSize);
    errno_t rc = memcpy_s(newptr, newSize, ptr, newSize > ptrsize ? ptrsize : newSize);
    securec_check_ss(rc, "", "");
    if (newSize > ptrsize) {
        rc = memset_s(newptr + newSize, newSize - ptrsize, 0, newSize - ptrsize);
        securec_check_ss(rc, "", "");
    }
    RackFreeConverter(ptr);

    return newptr;
}

/* ----------
 * AllocSetFreeIndex -
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= ALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int AllocSetFreeIndex(Size size)
{
    int idx;

    if (size > (1 << ALLOC_MINBITS)) {
        /*----------
         * At this point we must compute ceil(log2(size >> ALLOC_MINBITS)).
         *----------
         */
        return pg_leftmost_one_pos32((size - 1) >> ALLOC_MINBITS) + 1;
    } else {
        idx = 0;
    }

    return idx;
}

#ifdef MEMORY_CONTEXT_CHECKING
static void set_sentinel(void* base, Size offset)
{
    char* ptr = (char*)base + offset;

    *ptr = 0x7E;
}

static bool sentinel_ok(const void* base, Size offset)
{
    const char* ptr = (const char*)base + offset;
    bool ret = false;

    ret = *ptr == 0x7E;

    return ret;
}

#endif

/*
 * AllocSetMethodDefinition
 *      Define the method functions based on the templated value
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void RackMemoryAllocator::AllocSetMethodDefinition(MemoryContextMethods* method)
{
    method->alloc = &RackMemoryAllocator::AllocSetAlloc<enable_memoryprotect, is_shared, is_tracked>;
    method->free_p = &RackMemoryAllocator::AllocSetFree<enable_memoryprotect, is_shared, is_tracked>;
    method->realloc = &RackMemoryAllocator::AllocSetRealloc<enable_memoryprotect, is_shared, is_tracked>;
    method->init = &GenericMemoryAllocator::AllocSetInit;
    method->reset = &RackMemoryAllocator::AllocSetReset<enable_memoryprotect, is_shared, is_tracked>;
    method->delete_context = &RackMemoryAllocator::AllocSetDelete<enable_memoryprotect, is_shared, is_tracked>;
    method->get_chunk_space = &GenericMemoryAllocator::AllocSetGetChunkSpace;
    method->is_empty = &GenericMemoryAllocator::AllocSetIsEmpty;
    method->stats = &RackMemoryAllocator::AllocSetStats;
#ifdef MEMORY_CONTEXT_CHECKING
    method->check = &GenericMemoryAllocator::AllocSetCheck;
#endif
}

/*
 * AllocSetContextSetMethods
 *		set the method functions
 *
 * Notes: unsupprot to track the memory usage of shared context
 */
void RackMemoryAllocator::AllocSetContextSetMethods(unsigned long value, MemoryContextMethods* method)
{
    bool isProt = (value & IS_PROTECT) ? true : false;
    bool isShared = (value & IS_SHARED) ? true : false;
    bool isTracked = (value & IS_TRACKED) ? true : false;

    if (isProt) {
        if (isShared)
            AllocSetMethodDefinition<true, true, false>(method);
        else {
            if (isTracked)
                AllocSetMethodDefinition<true, false, true>(method);
            else
                AllocSetMethodDefinition<true, false, false>(method);
        }
    } else {
        if (isShared)
            AllocSetMethodDefinition<false, true, false>(method);
        else {
            if (isTracked)
                AllocSetMethodDefinition<false, false, true>(method);
            else
                AllocSetMethodDefinition<false, false, false>(method);
        }
    }
}

/*
 * AllocSetContextCreate
 *		Create a new AllocSet context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * minContextSize: minimum context size
 * initBlockSize: initial allocation block size
 * maxBlockSize: maximum allocation block size
 */
MemoryContext RackMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    RackAllocSet context;
    NodeTag type = isShared ? T_RackSharedAllocSetContext : T_RackAllocSetContext;
    bool isTracked = false;
    unsigned long value = isShared ? IS_SHARED : 0;
    MemoryProtectFuncDef* func = NULL;
    Size localInitBlock = initBlockSize;
    Size localMaxBlock = maxBlockSize;
    Size localMax = maxSize;

    if (isShared)
        func = &SharedFunctions;
    else if (!isSession && (parent == NULL || parent->session_id == 0))
        func = &GenericFunctions;
    else
        func = &SessionFunctions;

    /* we want to be sure ErrorContext still has some memory even if we've run out elsewhere!
     * Don't limit the memory allocation for ErrorContext. And skip memory tracking memory allocation.
     */
    if ((0 != strcmp(name, "ErrorContext")) && (0 != strcmp(name, "MemoryTrackMemoryContext")) &&
        (strcmp(name, "Track MemoryInfo hash") != 0) && (0 != strcmp(name, "DolphinErrorData")))
        value |= IS_PROTECT;

    /* only track the unshared context after t_thrd.mem_cxt.mem_track_mem_cxt is created */
    if (func == &GenericFunctions && parent && (MEMORY_TRACKING_MODE > MEMORY_TRACKING_PEAKMEMORY) &&
        t_thrd.mem_cxt.mem_track_mem_cxt &&
        (t_thrd.utils_cxt.ExecutorMemoryTrack == NULL || ((RackAllocSet)parent)->track)) {
        isTracked = true;
        value |= IS_TRACKED;
    }

    /* Do the type-independent part of context creation */
    context = (RackAllocSet)MemoryContextCreate(type, sizeof(RackAllocSetContext), parent, name, __FILE__, __LINE__);

    if (isShared && maxSize == DEFAULT_MEMORY_CONTEXT_MAX_SIZE) {
        // default maxSize of shared memory context.
        context->maxSpaceSize = SHARED_MEMORY_CONTEXT_MAX_SIZE;
    } else {
        // set by user (shared or generic),default generic max size(eg. 10M),
        // these three types run following scope:
        context->maxSpaceSize = maxSize + SELF_GENRIC_MEMCTX_LIMITATION;
    }

    /* assign the method function with specified templated to the context */
    AllocSetContextSetMethods(value, ((MemoryContext)context)->methods);

    /*
     * Make sure alloc parameters are reasonable, and save them.
     *
     * We somewhat arbitrarily enforce a minimum 1K block size.
     */
    initBlockSize = MAXALIGN(initBlockSize);
    if (initBlockSize < MAX_RACK_MEMORY_ALLOC_SIZE)
        initBlockSize = MAX_RACK_MEMORY_ALLOC_SIZE;
    maxBlockSize = MAXALIGN(maxBlockSize);
    if (maxBlockSize < initBlockSize)
        maxBlockSize = initBlockSize;
    context->initBlockSize = initBlockSize;
    context->maxBlockSize = maxBlockSize;
    context->nextBlockSize = initBlockSize;

    /* initialize statistic */
    context->totalSpace = 0;
    context->freeSpace = 0;

    /* create the memory track structure */
    if (isTracked)
        MemoryTrackingCreate((MemoryContext)context, parent);

    /*
     * Compute the allocation chunk size limit for this context.  It can't be
     * more than ALLOC_CHUNK_LIMIT because of the fixed number of freelists.
     * If maxBlockSize is small then requests exceeding the maxBlockSize, or
     * even a significant fraction of it, should be treated as large chunks
     * too.  For the typical case of maxBlockSize a power of 2, the chunk size
     * limit will be at most 1/8th maxBlockSize, so that given a stream of
     * requests that are all the maximum chunk size we will waste at most
     * 1/8th of the allocated space.
     *
     * We have to have allocChunkLimit a power of two, because the requested
     * and actually-allocated sizes of any chunk must be on the same side of
     * the limit, else we get confused about whether the chunk is "big".
     */
    context->allocChunkLimit = 1 << (AllocSetFreeIndex(MAX_RACK_MEMORY_ALLOC_SIZE) - 1);

    /*
     * Grab always-allocated space, if requested
     */
    if (minContextSize > ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ) {
        Size blksize = MAXALIGN(minContextSize);
        AllocBlock block;

        block = (AllocBlock)RackMallocConverter(blksize);
        if (block == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                    errmsg("memory is temporarily unavailable"),
                    errdetail("Failed while creating memory context \"%s\".", name)));
        }
        block->aset = context;
        block->freeptr = ((char*)block) + ALLOC_BLOCKHDRSZ;
        block->endptr = ((char*)block) + blksize;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = BlkMagicNum;
#endif

        context->totalSpace += blksize;
        context->freeSpace += block->endptr - block->freeptr;

        /* update the memory tracking information when allocating memory */
        if (isTracked)
            MemoryTrackingAllocInfo((MemoryContext)context, blksize);

        block->prev = NULL;
        block->next = NULL;
        /* Remember block as part of block list */
        context->blocks = block;
        /* Mark block as not to be released at reset time */
        context->keeper = block;
    }

    context->header.is_shared = isShared;
    if (isShared)
        (void)pthread_rwlock_init(&(context->header.lock), NULL);

    context->local_context = ::AllocSetContextCreate(
        (MemoryContext)context,
        "rack context local",
        localInitBlock,
        localMaxBlock,
        localMax);

    return (MemoryContext)context;
}

/*
 * AllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not necessarily
 * give back all the resources the set owns.  Our actual implementation is
 * that we hang onto any "keeper" block specified for the set.	In this way,
 * we don't thrash malloc() when a context is repeatedly reset after small
 * allocations, which is typical behavior for per-tuple contexts.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void RackMemoryAllocator::AllocSetReset(MemoryContext context)
{
    RackAllocSet set = (RackAllocSet)context;
    AllocBlock block;
    MemoryProtectFuncDef* func = NULL;
    MemoryContextReset(set->local_context);

    AssertArg(AllocSetIsValid(set));

    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        CHECK_CONTEXT_OWNER(context);
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    GenericMemoryAllocator::AllocSetCheck(context);
#endif

    /* Clear chunk freelists */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    MemSetAligned(set->extern_freelist, 0, sizeof(set->extern_freelist));

    block = set->blocks;

    /* New blocks list is either empty or just the keeper block */
    set->blocks = set->keeper;
    while (block != NULL) {
        AllocBlock next = block->next;
        Size tempSize = block->allocSize;

        if (block == set->keeper) {
            /* Reset the block, but don't return it to malloc */
            char* datastart = ((char*)block) + ALLOC_BLOCKHDRSZ;

            block->freeptr = datastart;
            block->allocSize = tempSize;
            block->next = NULL;
            block->prev = NULL;
        } else {
            if (is_tracked)
                MemoryTrackingFreeInfo(context, tempSize);

            RackFreeConverter(block);
        }
        block = next;
    }
    /* Reset block size allocation sequence, too */
    set->nextBlockSize = set->initBlockSize;

    if (set->blocks != NULL) {
        /* calculate memory statisic after reset. */
        block = set->blocks;

        set->freeSpace = block->endptr - block->freeptr;
        set->totalSpace = block->endptr - (char*)block;
    } else {
        set->freeSpace = 0;
        set->totalSpace = 0;
    }

    if (is_shared)
        MemoryContextUnlock(context);
}

/*
 * AllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike AllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void RackMemoryAllocator::AllocSetDelete(MemoryContext context)
{
    RackAllocSet set = (RackAllocSet)context;
    AssertArg(AllocSetIsValid(set));

    AllocBlock block = set->blocks;
    MemoryProtectFuncDef* func = NULL;

    if (set->blocks == NULL) {
        return;
    }

    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        CHECK_CONTEXT_OWNER(context);
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    GenericMemoryAllocator::AllocSetCheck(context);
#endif

    /* Make it look empty, just in case... */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    MemSetAligned(set->extern_freelist, 0, sizeof(set->extern_freelist));
    set->blocks = NULL;
    set->keeper = NULL;

    while (block != NULL) {
        AllocBlock next = block->next;
        Size tempSize = block->allocSize;

        if (is_tracked)
            MemoryTrackingFreeInfo(context, tempSize);

        RackFreeConverter(block);
        block = next;
    }

    /* reset to 0 after deletion. */
    set->freeSpace = 0;
    set->totalSpace = 0;

    if (is_shared) {
        MemoryContextUnlock(context);
    }
}

/*
 * AllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void* RackMemoryAllocator::AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
{
    Assert(file != NULL);
    Assert(line != 0);
    RackAllocSet set = (RackAllocSet)context;
    AllocBlock block;
    AllocChunk chunk;
#ifndef ENABLE_MEMORY_CHECK
    unsigned int fidx;
#endif
    Size chunk_size;
    Size blksize;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AllocSetIsValid(set));
    AssertArg(align == 0);

#ifdef MEMORY_CONTEXT_CHECKING
    /* memory enjection */
    if (gs_memory_enjection() && 0 != strcmp(context->name, "ErrorContext"))
        return NULL;
#endif

    /* if local memory not exhaust, will try alloc memory from local context */
    if (!u_sess->local_memory_exhaust) {
#ifndef MEMORY_CONTEXT_CHECKING
        void* res = set->local_context->alloc_methods->alloc_from_context(set->local_context, size, file, line);
#else
        void* res = set->local_context->alloc_methods->alloc_from_context_debug(set->local_context, size, file, line);
#endif
        if (res) {
            return res;
        }
    }

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    size += ALLOC_MAGICHDRSZ;

    /*
     * If requested size exceeds maximum for chunks, allocate an entire block
     * for this request.
     */
#ifndef ENABLE_MEMORY_CHECK
    if (size > set->allocChunkLimit) {
#endif
        chunk_size = MAXALIGN(size);
        blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

        block = (AllocBlock)RackMallocConverter(blksize);
        if (block == NULL) {
            if (is_shared)
                MemoryContextUnlock(context);
            return NULL;
        }
        block->aset = set;
        block->freeptr = block->endptr = ((char*)block) + blksize;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = BlkMagicNum;
#endif

        /* enlarge total space only. */
        set->totalSpace += blksize;

        /* update the memory tracking information when allocating memory */
        if (is_tracked)
            MemoryTrackingAllocInfo(context, blksize);

        chunk = (AllocChunk)(((char*)block) + ALLOC_BLOCKHDRSZ);
        chunk->aset = set;
        chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < chunk_size)
            set_sentinel(AllocChunkGetPointer(chunk), size - ALLOC_MAGICHDRSZ);
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
        magic->aset = set;
        magic->size = chunk->size;
        magic->posnum = PosmagicNum;

        /* track the detail allocation information */
        MemoryTrackingDetailInfo(context, size, chunk->size, file, line);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
        /* fill the allocated space with junk */
        randomize_mem((char*)AllocChunkGetPointer(chunk), size);
#endif

        /*
         * Stick the new block underneath the active allocation block, so that
         * we don't lose the use of the space remaining therein.
         */
        if (set->blocks != NULL) {
            block->prev = set->blocks;
            block->next = set->blocks->next;
            if (block->next)
                block->next->prev = block;
            set->blocks->next = block;
        } else {
            block->prev = NULL;
            block->next = NULL;
            set->blocks = block;
        }

        AllocAllocInfo(set, chunk);

        if (is_shared)
            MemoryContextUnlock(context);

        return AllocChunkGetPointer(chunk);
#ifndef ENABLE_MEMORY_CHECK
    }

    /*
     * Request is small enough to be treated as a chunk.  Look in the
     * corresponding free list to see if there is a free chunk we could reuse.
     * If one is found, remove it from the free list, make it again a member
     * of the alloc set and return its data address.
     */
    fidx = AllocSetFreeIndex(size);
    if (fidx < ALLOCSET_NUM_FREELISTS) {
        chunk = set->freelist[fidx];
    } else {
        chunk = set->extern_freelist[fidx - ALLOCSET_NUM_FREELISTS];
    }
    if (chunk != NULL) {
        Assert(chunk->size >= size);
        Assert(chunk->aset != set);
        Assert((int)fidx == AllocSetFreeIndex(chunk->size));

        if (fidx < ALLOCSET_NUM_FREELISTS) {
            set->freelist[fidx] = (AllocChunk)chunk->aset;
        } else {
            set->extern_freelist[fidx - ALLOCSET_NUM_FREELISTS] = (AllocChunk)chunk->aset;
        }

        chunk->aset = (void*)set;

        set->freeSpace -= (chunk->size + ALLOC_CHUNKHDRSZ);
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < chunk->size)
            set_sentinel(AllocChunkGetPointer(chunk), size - ALLOC_MAGICHDRSZ);
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
        magic->aset = set;
        magic->size = chunk->size;
        magic->posnum = PosmagicNum;

        /* track the detail allocation information */
        MemoryTrackingDetailInfo(context, size, chunk->size, file, line);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
        /* fill the allocated space with junk */
        randomize_mem((char*)AllocChunkGetPointer(chunk), size);
#endif

        AllocAllocInfo(set, chunk);

        if (is_shared)
            MemoryContextUnlock(context);

        return AllocChunkGetPointer(chunk);
    }

    /*
     * Choose the actual chunk size to allocate.
     */
    chunk_size = ((unsigned long)1 << ALLOC_MINBITS) << fidx;
    Assert(chunk_size >= size);

    /*
     * If there is enough room in the active allocation block, we will put the
     * chunk into that block.  Else must start a new one.
     */
    if ((block = set->blocks) != NULL) {
        Size availspace = block->endptr - block->freeptr;

        if (availspace < (chunk_size + ALLOC_CHUNKHDRSZ)) {
            /*
             * The existing active (top) block does not have enough room for
             * the requested allocation, but it might still have a useful
             * amount of space in it.  Once we push it down in the block list,
             * we'll never try to allocate more space from it. So, before we
             * do that, carve up its free space into chunks that we can put on
             * the set's freelists.
             *
             * Because we can only get here when there's less than
             * ALLOC_CHUNK_LIMIT left in the block, this loop cannot iterate
             * more than ALLOCSET_NUM_FREELISTS-1 times.
             */
            while (availspace >= ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ)) {
                Size availchunk = availspace - ALLOC_CHUNKHDRSZ;
                int a_fidx = AllocSetFreeIndex(availchunk);

                /*
                 * In most cases, we'll get back the index of the next larger
                 * freelist than the one we need to put this chunk on.	The
                 * exception is when availchunk is exactly a power of 2.
                 */
                if (availchunk != ((Size)1 << ((unsigned int)a_fidx + ALLOC_MINBITS))) {
                    a_fidx--;
                    Assert(a_fidx >= 0);
                    availchunk = ((Size)1 << ((unsigned int)a_fidx + ALLOC_MINBITS));
                }

                chunk = (AllocChunk)(block->freeptr);

                block->freeptr += (availchunk + ALLOC_CHUNKHDRSZ);
                availspace -= (availchunk + ALLOC_CHUNKHDRSZ);

                chunk->size = availchunk;
#ifdef MEMORY_CONTEXT_TRACK
                chunk->file = NULL;
                chunk->line = 0;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
                chunk->requested_size = 0; /* mark it free */
                chunk->prenum = 0;
#endif
                if (a_fidx < ALLOCSET_NUM_FREELISTS) {
                    chunk->aset = (void*)set->freelist[a_fidx];
                    set->freelist[a_fidx] = chunk;
                } else {
                    chunk->aset = (void*)set->extern_freelist[a_fidx - ALLOCSET_NUM_FREELISTS];
                    set->extern_freelist[a_fidx - ALLOCSET_NUM_FREELISTS] = chunk;
                }
            }

            /* Mark that we need to create a new block */
            block = NULL;
        }
    }

    /*
     * Time to create a new regular (multi-chunk) block?
     */
    if (block == NULL) {
        /*
         * The first such block has size initBlockSize, and we double the
         * space in each succeeding block, but not more than maxBlockSize.
         */
        blksize = set->nextBlockSize;
        set->nextBlockSize <<= 1;
        if (set->nextBlockSize > set->maxBlockSize)
            set->nextBlockSize = set->maxBlockSize;

        block = (AllocBlock)RackMallocConverter(blksize);
        if (block == NULL) {
            if (is_shared)
                MemoryContextUnlock(context);
            return NULL;
        }

        block->aset = set;
        block->freeptr = ((char*)block) + ALLOC_BLOCKHDRSZ;
        block->endptr = ((char*)block) + blksize;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = BlkMagicNum;
#endif

        set->totalSpace += blksize;
        set->freeSpace += blksize - ALLOC_BLOCKHDRSZ;

        /* update the memory tracking information when allocating memory */
        if (is_tracked)
            MemoryTrackingAllocInfo(context, blksize);
        /*
         * If this is the first block of the set, make it the "keeper" block.
         * Formerly, a keeper block could only be created during context
         * creation, but allowing it to happen here lets us have fast reset
         * cycling even for contexts created with minContextSize = 0; that way
         * we don't have to force space to be allocated in contexts that might
         * never need any space.  Don't mark an oversize block as a keeper,
         * however.
         */
        if (set->keeper == NULL && blksize == set->initBlockSize)
            set->keeper = block;

        block->prev = NULL;
        block->next = set->blocks;
        if (block->next)
            block->next->prev = block;
        set->blocks = block;
    }

    /*
     * OK, do the allocation
     */
    chunk = (AllocChunk)(block->freeptr);

    block->freeptr += (chunk_size + ALLOC_CHUNKHDRSZ);

    set->freeSpace -= (chunk_size + ALLOC_CHUNKHDRSZ);

    Assert(block->freeptr <= block->endptr);

    chunk->aset = (void*)set;
    chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_TRACK
    chunk->file = file;
    chunk->line = line;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    chunk->prenum = PremagicNum;
    /* set mark to catch clobber of "unused" space */
    if (size < chunk->size)
        set_sentinel(AllocChunkGetPointer(chunk), size - ALLOC_MAGICHDRSZ);
    AllocMagicData* magic =
        (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
    magic->aset = set;
    magic->size = chunk->size;
    magic->posnum = PosmagicNum;

    /* track the detail allocation information */
    MemoryTrackingDetailInfo(context, size, chunk->size, file, line);
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
    /* fill the allocated space with junk */
    randomize_mem((char*)AllocChunkGetPointer(chunk), size);
#endif

    AllocAllocInfo(set, chunk);

    if (is_shared)
        MemoryContextUnlock(context);

    return AllocChunkGetPointer(chunk);
#endif
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void RackMemoryAllocator::AllocSetFree(MemoryContext context, void* pointer)
{
    RackAllocSet set = (RackAllocSet)context;
    AllocChunk chunk = AllocPointerGetChunk(pointer);
    Size tempSize = 0;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AllocSetIsValid(set));

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        CHECK_CONTEXT_OWNER(context);
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    AllocFreeInfo(set, chunk);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Test for someone scribbling on unused space in chunk */
    if (chunk->requested_size != (Size)MAXALIGN(chunk->requested_size) && chunk->requested_size < chunk->size)
        if (!sentinel_ok(pointer, chunk->requested_size - ALLOC_MAGICHDRSZ)) {
            if (is_shared) {
                MemoryContextUnlock(context);
            }
            ereport(PANIC, (errmsg("detected write past chunk end in %s", set->header.name)));
        }
    AllocMagicData* magic =
        (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
    Assert(magic->aset == set && magic->size == chunk->size && magic->posnum == PosmagicNum);
#endif

#ifndef ENABLE_MEMORY_CHECK
    if (chunk->size > set->allocChunkLimit) {
#endif
        /*
         * Big chunks are certain to have been allocated as single-chunk
         * blocks.	Find the containing block and return it to malloc().
         */
        AllocBlock block = (AllocBlock)(((char*)chunk) - ALLOC_BLOCKHDRSZ);

        check_pointer_valid(block, is_shared, context, chunk);

        /* OK, remove block from aset's list and free it */
        if (block->prev)
            block->prev->next = block->next;
        else
            set->blocks = block->next;

        if (block->next)
            block->next->prev = block->prev;

        tempSize = block->allocSize;

        set->totalSpace -= block->allocSize;

        /* clean the structure of block */
        block->aset = NULL;
        block->prev = NULL;
        block->next = NULL;
        block->freeptr = NULL;
        block->endptr = NULL;
        block->allocSize = 0;

        if (is_tracked)
            MemoryTrackingFreeInfo(context, tempSize);

        RackFreeConverter(block);
#ifndef ENABLE_MEMORY_CHECK
    } else {
        /* Normal case, put the chunk into appropriate freelist */
        int fidx = AllocSetFreeIndex(chunk->size);
        if (fidx < ALLOCSET_NUM_FREELISTS) {
            chunk->aset = (void*)set->freelist[fidx];
        } else {
            chunk->aset = (void*)set->extern_freelist[fidx - ALLOCSET_NUM_FREELISTS];
        }
        set->freeSpace += chunk->size + ALLOC_CHUNKHDRSZ;

#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = NULL;
        chunk->line = 0;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
        /* Reset requested_size to 0 in chunks that are on freelist */
        chunk->requested_size = 0;
        chunk->prenum = 0;
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
        magic->aset = NULL;
        magic->size = 0;
        magic->posnum = 0;
#endif
        if (fidx < ALLOCSET_NUM_FREELISTS) {
            set->freelist[fidx] = chunk;
        } else {
            set->extern_freelist[fidx - ALLOCSET_NUM_FREELISTS] = chunk;
        }
        Assert(chunk->aset != set);
    }
#endif
    if (is_shared)
        MemoryContextUnlock(context);
}

/*
 * AllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void* RackMemoryAllocator::AllocSetRealloc(
    MemoryContext context, void* pointer, Size align, Size size, const char* file, int line)
{
    RackAllocSet set = (RackAllocSet)context;
    AllocChunk chunk = AllocPointerGetChunk(pointer);
    Size oldsize = chunk->size;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AllocSetIsValid(set));
    AssertArg(align == 0);

#ifdef MEMORY_CONTEXT_CHECKING
    /* memory enjection */
    if (gs_memory_enjection() && 0 != strcmp(context->name, "ErrorContext"))
        return NULL;
#endif

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        CHECK_CONTEXT_OWNER(context);
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* Test for someone scribbling on unused space in chunk */
    if (chunk->requested_size != (Size)MAXALIGN(chunk->requested_size) && chunk->requested_size < oldsize)
        if (!sentinel_ok(pointer, chunk->requested_size - ALLOC_MAGICHDRSZ)) {
            if (is_shared) {
                MemoryContextUnlock(context);
            }
            ereport(PANIC, (errmsg("detected write past chunk end in %s", set->header.name)));
        }
    AllocMagicData* magic =
        (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
    Assert(magic->aset == set && magic->size == chunk->size && magic->posnum == PosmagicNum);
#endif

    /*
     * Chunk sizes are aligned to power of 2 in AllocSetAlloc(). Maybe the
     * allocated area already is >= the new size.  (In particular, we always
     * fall out here if the requested size is a decrease.)
     */
    if (oldsize >= (size + ALLOC_MAGICHDRSZ)) {
        size += ALLOC_MAGICHDRSZ;
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
#ifdef RANDOMIZE_ALLOCATED_MEMORY
        /* We can only fill the extra space if we know the prior request */
        if (size > chunk->requested_size)
            randomize_mem((char*)AllocChunkGetPointer(chunk) + chunk->requested_size, size - chunk->requested_size);
#endif
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < oldsize)
            set_sentinel(pointer, size - ALLOC_MAGICHDRSZ);
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
        magic->aset = set;
        magic->size = chunk->size;
        magic->posnum = PosmagicNum;

        /* track the detail allocation information */
        MemoryTrackingDetailInfo(context, size, chunk->size, file, line);
#endif
        if (is_shared)
            MemoryContextUnlock(context);
        return pointer;
    }

#ifndef ENABLE_MEMORY_CHECK
    if (oldsize > set->allocChunkLimit)
#endif
    {
        /*
         * The chunk must have been allocated as a single-chunk block.	Find
         * the containing block and use realloc() to make it bigger with
         * minimum space wastage.
         */
        AllocBlock block = (AllocBlock)(((char*)chunk) - ALLOC_BLOCKHDRSZ);
        AllocBlock oldBlock = NULL;
        Size chksize;
        Size blksize;

        size += ALLOC_MAGICHDRSZ;

        /*
         * Try to verify that we have a sane block pointer: it should
         * reference the correct aset, and freeptr and endptr should point
         * just past the chunk.
         */
        check_pointer_valid(block, is_shared, context, chunk);

        /* Do the realloc */
        chksize = MAXALIGN(size);
        blksize = chksize + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
        oldBlock = block;
        oldsize = block->allocSize;

        block = (AllocBlock)RackReallocConverter(oldBlock, oldsize, blksize);
        if (block == NULL) {
            if (is_shared)
                MemoryContextUnlock(context);
            return NULL;
        }
        block->freeptr = block->endptr = ((char*)block) + blksize;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = BlkMagicNum;
#endif

        set->totalSpace += blksize - oldsize;

        /* update the memory tracking information when allocating memory */
        if (is_tracked)
            MemoryTrackingAllocInfo(context, blksize - oldsize);

        /* Update pointers since block has likely been moved */
        chunk = (AllocChunk)(((char*)block) + ALLOC_BLOCKHDRSZ);
        if (block->prev)
            block->prev->next = block;
        else
            set->blocks = block;
        if (block->next)
            block->next->prev = block;
        chunk->size = chksize;
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
#endif
#ifdef MEMORY_CONTEXT_CHECKING
#ifdef RANDOMIZE_ALLOCATED_MEMORY
        /* We can only fill the extra space if we know the prior request */
        randomize_mem((char*)AllocChunkGetPointer(chunk) + chunk->requested_size, size - chunk->requested_size);
#endif
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < chunk->size)
            set_sentinel(AllocChunkGetPointer(chunk), size - ALLOC_MAGICHDRSZ);
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);
        magic->aset = set;
        magic->size = chunk->size;
        magic->posnum = PosmagicNum;

        /* track the detail allocation information */
        MemoryTrackingDetailInfo(context, size, chunk->size, file, line);
#endif

        if (is_shared)
            MemoryContextUnlock(context);
        return AllocChunkGetPointer(chunk);
    }
#ifndef ENABLE_MEMORY_CHECK
    else {
        /*
         * Small-chunk case.  We just do this by brute force, ie, allocate a
         * new chunk and copy the data.  Since we know the existing data isn't
         * huge, this won't involve any great memcpy expense, so it's not
         * worth being smarter.  (At one time we tried to avoid memcpy when it
         * was possible to enlarge the chunk in-place, but that turns out to
         * misbehave unpleasantly for repeated cycles of
         * palloc/repalloc/pfree: the eventually freed chunks go into the
         * wrong freelist for the next initial palloc request, and so we leak
         * memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
         */
        AllocPointer newPointer;
        errno_t ret = 0;

        if (is_shared)
            MemoryContextUnlock(context);

        /* allocate new chunk */
        newPointer =
            AllocSetAlloc<enable_memoryprotect, is_shared, is_tracked>((MemoryContext)set, align, size, file, line);

        /* leave immediately if request was not completed */
        if (newPointer == NULL)
            return NULL;

            /* transfer existing data (certain to fit) */
#ifdef MEMORY_CONTEXT_CHECKING
        Size memlen = MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ;
        if (0 != memlen)
            ret = memcpy_s(newPointer, memlen, pointer, memlen);
#else
        ret = memcpy_s(newPointer, oldsize, pointer, oldsize);
#endif
        if (ret != EOK) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Error on %s Memory Context happened when executing memcpy_s:%d", context->name, (int)ret),
                    errdetail("Maybe the parameter is error"),
                    errhint("Please contact engineer to support.")));
        }

        /* free old chunk */
        AllocSetFree<enable_memoryprotect, is_shared, is_tracked>((MemoryContext)set, pointer);

        return newPointer;
    }
#endif
}

/*
 * AllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */
void RackMemoryAllocator::AllocSetStats(MemoryContext context, int level)
{
    RackAllocSet set = (RackAllocSet)context;
    long nblocks = 0;
    long nchunks = 0;
    long totalspace = 0;
    long freespace = 0;
    AllocBlock block;
    AllocChunk chunk;
    int fidx;
    int i;

    for (block = set->blocks; block != NULL; block = block->next) {
        nblocks++;
        totalspace += block->endptr - ((char*)block);
        freespace += block->endptr - block->freeptr;
    }
    for (fidx = 0; fidx < ALLOCSET_NUM_FREELISTS; fidx++) {
        for (chunk = set->freelist[fidx]; chunk != NULL; chunk = (AllocChunk)chunk->aset) {
            nchunks++;
            freespace += chunk->size + ALLOC_CHUNKHDRSZ;
        }
    }
    for (fidx = 0; fidx < RACKALLOCSET_NUM_FREELISTS - ALLOCSET_NUM_FREELISTS; fidx++) {
        for (chunk = set->extern_freelist[fidx]; chunk != NULL; chunk = (AllocChunk)chunk->aset) {
            nchunks++;
            freespace += chunk->size + ALLOC_CHUNKHDRSZ;
        }
    }

    for (i = 0; i < level; i++)
        fprintf(stderr, "  ");

    fprintf(stderr,
        "%s: %ld total in %ld blocks; %ld free (%ld chunks); %ld used\n",
        set->header.name,
        totalspace,
        nblocks,
        freespace,
        nchunks,
        totalspace - freespace);
}

#ifdef MEMORY_CONTEXT_CHECKING

#endif /* MEMORY_CONTEXT_CHECKING */