/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * asetstk.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/asetstk.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/mman.h>

#include "postgres.h"
#include "knl/knl_variable.h"

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

#define ALLOC_CHUNK_LIMIT 8192  // 8K

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
#define STACK_BLOCKHDRSZ MAXALIGN(sizeof(StackBlockData))

/*
 * AllocPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void* AllocPointer;

typedef StackSetContext* StackSet;

extern void MemoryContextControlSet(AllocSet context, const char* name);

#ifdef MEMORY_CONTEXT_CHECKING
const uint32 StkBlkMagicNum = 0xDADADADA;
#endif

typedef struct StackBlockData {
    StackSet aset;   /* aset that owns this block */
    StackBlock next; /* next block in aset's blocks list */
    Size allocSize;  /* allocated size */
    uint32 offset;   /* the offset ready to allocate */
#ifdef MEMORY_CONTEXT_CHECKING
    uint32 magicNum; /* DADA */
#endif
} StackBlockData;

/*
 * AllocPointerIsValid
 *		True if pointer is valid allocation pointer.
 */
#define AllocSetIsValid(set) PointerIsValid(set)

/*
 * AllocSetMethodDefinition
 *      Define the method functions based on the templated value
 */
template <bool is_tracked>
void StackMemoryAllocator::AllocSetMethodDefinition(MemoryContextMethods* method)
{
    method->alloc = &StackMemoryAllocator::AllocSetAlloc<is_tracked>;
    method->free_p = &StackMemoryAllocator::AllocSetFree;
    method->realloc = &StackMemoryAllocator::AllocSetRealloc;
    method->init = &StackMemoryAllocator::AllocSetInit;
    method->reset = &StackMemoryAllocator::AllocSetReset<is_tracked>;
    method->delete_context = &StackMemoryAllocator::AllocSetDelete<is_tracked>;
    method->get_chunk_space = &StackMemoryAllocator::AllocSetGetChunkSpace;
    method->is_empty = &StackMemoryAllocator::AllocSetIsEmpty;
    method->stats = &StackMemoryAllocator::AllocSetStats;
#ifdef MEMORY_CONTEXT_CHECKING
    method->check = &StackMemoryAllocator::AllocSetCheck;
#endif
}

/*
 * AllocSetContextSetMethods
 *		set the method functions
 */
void StackMemoryAllocator::AllocSetContextSetMethods(unsigned long value, MemoryContextMethods* method)
{
    bool isTracked = (value & IS_TRACKED) ? true : false;
    if (isTracked)
        AllocSetMethodDefinition<true>(method);
    else
        AllocSetMethodDefinition<false>(method);
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
MemoryContext StackMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    StackSet context = NULL;
    bool isTracked = false;
    unsigned long value = 0;
    MemoryProtectFuncDef* func = NULL;

    if (!isSession && (parent == NULL || parent->session_id == 0))
        func = &GenericFunctions;
    else
        func = &SessionFunctions;

    /* only track the memory context after t_thrd.mem_cxt.mem_track_mem_cxt is created */
    if (func == &GenericFunctions && parent && u_sess->attr.attr_memory.memory_tracking_mode > MEMORY_TRACKING_PEAKMEMORY &&
        t_thrd.mem_cxt.mem_track_mem_cxt &&
        (t_thrd.utils_cxt.ExecutorMemoryTrack == NULL || ((AllocSet)parent)->track)) {
        isTracked = true;
        value |= IS_TRACKED;
    }

    // Do the type-independent part of context creation
    //
    context = (StackSet)MemoryContextCreate(
        T_StackAllocSetContext, sizeof(StackSetContext), parent, name, __FILE__, __LINE__);

    if (isShared && maxSize == DEFAULT_MEMORY_CONTEXT_MAX_SIZE) {
        // default maxSize of shared memory context.
        context->maxSpaceSize = SHARED_MEMORY_CONTEXT_MAX_SIZE;
    } else {
        // set by user (shared or generic),default generic max size(eg. 10M),
        // these three types run following scope:
        context->maxSpaceSize = maxSize + SELF_GENRIC_MEMCTX_LIMITATION;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextControlSet((AllocSet)context, name);
#endif

    /* assign the method function with specified templated to the context */
    AllocSetContextSetMethods(value, ((MemoryContext)context)->methods);

    // Make sure alloc parameters are reasonable, and save them.
    // We somewhat arbitrarily enforce a minimum 1K block size.
    //
    initBlockSize = MAXALIGN(initBlockSize);
    if (initBlockSize < 1024) {
        initBlockSize = 1024;
    }
    maxBlockSize = MAXALIGN(maxBlockSize);
    if (maxBlockSize < initBlockSize) {
        maxBlockSize = initBlockSize;
    }

    context->initBlockSize = initBlockSize;
    context->maxBlockSize = maxBlockSize;
    context->nextBlockSize = initBlockSize;
    context->allocChunkLimit = ALLOC_CHUNK_LIMIT;

    // create the memory tracking structure
    if (isTracked)
        MemoryTrackingCreate((MemoryContext)context, parent);

    // Init the first Block
    if (minContextSize > STACK_BLOCKHDRSZ) {
        Size blksize = MAXALIGN(minContextSize);
        StackBlock block;

        if (GS_MP_INITED)
            block = (StackBlock)(*func->malloc)(blksize, true);
        else
            gs_malloc(blksize, block, StackBlock);

        if (block == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                    errmsg("memory is temporarily unavailable"),
                    errdetail("Failed while creating memory context \"%s\".", name)));
        }
        block->aset = context;
        block->offset = STACK_BLOCKHDRSZ;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = StkBlkMagicNum;
#endif

        context->totalSpace += blksize;
        context->freeSpace += blksize - STACK_BLOCKHDRSZ;

        /* update the memory tracking information when allocating memory */
        if (isTracked)
            MemoryTrackingAllocInfo((MemoryContext)context, blksize);

        block->next = context->blocks;
        context->blocks = block;
        /* Mark block as not to be released at reset time */
        context->keeper = block;
    }

    return (MemoryContext)context;
}

/*
 * AllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
template <bool is_tracked>
void* StackMemoryAllocator::AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
{
    StackBlock block;
    StackSet set = (StackSet)context;
    Size blksize;
    void* addr = NULL;
    MemoryProtectFuncDef* func = NULL;

    if (context->session_id > 0)
        func = &SessionFunctions;
    else
        func = &GenericFunctions;

    AssertArg(align == 0);

#ifdef MEMORY_CONTEXT_CHECKING
    /* memory enjection */
    if (gs_memory_enjection())
        return NULL;
#endif

    /*
     * If requested size exceeds maximum for chunks, allocate an entire block
     * for this request.
     */
    if (size > set->allocChunkLimit) {
        blksize = STACK_BLOCKHDRSZ + MAXALIGN(size);

        if (GS_MP_INITED)
            block = (StackBlock)(*func->malloc)(blksize, true);
        else
            gs_malloc(blksize, block, StackBlock);

        if (block == NULL)
            return NULL;
        block->aset = set;
        block->offset = blksize;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = StkBlkMagicNum;
#endif

        /* enlarge total space only. */
        set->totalSpace += blksize;

        /* update the memory tracking information when allocating memory */
        if (is_tracked)
            MemoryTrackingAllocInfo((MemoryContext)context, blksize);

#ifdef MEMORY_CONTEXT_CHECKING
        /* track the detail allocation information */
        MemoryTrackingDetailInfo(context, size, blksize, file, line);
#endif

        /*
         * Stick the new block underneath the active allocation block, so that
         * we don't lose the use of the space remaining therein.
         */
        if (set->blocks != NULL) {
            block->next = set->blocks->next;
            set->blocks->next = block;
        } else {
            block->next = NULL;
            set->blocks = block;
        }

        return (void*)(((char*)block) + STACK_BLOCKHDRSZ);
    }

    /* Allocate memory from available block */
    block = set->blocks;

    /* check if current block is available to allocate */
    if (block == NULL || (block->allocSize - (Size)block->offset) < (Size)MAXALIGN(size)) {
        // Allocate new block for allocate
        blksize = set->nextBlockSize;
        set->nextBlockSize <<= 1;
        if (set->nextBlockSize > set->maxBlockSize)
            set->nextBlockSize = set->maxBlockSize;

        /*
         * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
         * space... but try to keep it a power of 2.
         */
        Size required_size = STACK_BLOCKHDRSZ + MAXALIGN(size);
        while (blksize < required_size)
            blksize <<= 1;

        /* Try to allocate it */
        if (GS_MP_INITED)
            block = (StackBlock)(*func->malloc)(blksize, true);
        else
            gs_malloc(blksize, block, StackBlock);

        if (block == NULL)
            return NULL;
        block->aset = set;
        block->offset = STACK_BLOCKHDRSZ;
        block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magicNum = StkBlkMagicNum;
#endif

        set->totalSpace += blksize;
        set->freeSpace += blksize - STACK_BLOCKHDRSZ;

        /* update the memory tracking information when allocating memory */
        if (is_tracked)
            MemoryTrackingAllocInfo((MemoryContext)context, blksize);

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

        block->next = set->blocks;
        set->blocks = block;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* track the detail allocation information */
    MemoryTrackingDetailInfo(context, size, size, file, line);
#endif

    /*
     * OK, do the allocation
     */
    addr = (void*)((char*)block + block->offset);

    block->offset += MAXALIGN(size);

    set->freeSpace -= MAXALIGN(size);

    return addr;
}

void StackMemoryAllocator::AllocSetFree(MemoryContext context, void* pointer)
{
    ereport(
        ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unsupport to free memory under stack memory allocator")));
}

void* StackMemoryAllocator::AllocSetRealloc(
    MemoryContext context, void* pointer, Size align, Size size, const char* file, int line)
{
    AssertArg(align == 0);
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_OPERATION), errmsg("unsupport to reallocate memory under stack memory allocator")));
    return NULL;
}

void StackMemoryAllocator::AllocSetInit(MemoryContext context)
{
    //
    // we don't
    // have to do anything here: it's already OK.
    //
}

/*
 * AllocSetReset
 *		Frees all memory which is allocated in the given set.
 */
template <bool is_tracked>
void StackMemoryAllocator::AllocSetReset(MemoryContext context)
{
    StackSet set = (StackSet)context;
    StackBlock block;
    MemoryProtectFuncDef* func = NULL;

    if (context->session_id > 0)
        func = &SessionFunctions;
    else
        func = &GenericFunctions;

    AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    AllocSetCheck(context);
#endif

    block = set->blocks;

    /* New blocks list is either empty or just the keeper block */
    set->blocks = set->keeper;
    while (block != NULL) {
        StackBlock next = block->next;
        Size tempSize = block->allocSize;

        if (block == set->keeper) {
            block->offset = STACK_BLOCKHDRSZ;
            block->allocSize = tempSize;
            block->next = NULL;
        } else {
            if (is_tracked)
                MemoryTrackingFreeInfo(context, tempSize);

            if (GS_MP_INITED)
                (*func->free)(block, tempSize);
            else
                gs_free(block, tempSize);
        }
        block = next;
    }
    /* Reset block size allocation sequence, too */
    set->nextBlockSize = set->initBlockSize;

    if (set->blocks != NULL) {
        /* calculate memory statisic after reset. */
        block = set->blocks;

        set->freeSpace = block->allocSize - STACK_BLOCKHDRSZ;
        set->totalSpace = block->allocSize;
    } else {
        set->freeSpace = 0;
        set->totalSpace = 0;
    }
}

/*
 * AllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 */
template <bool is_tracked>
void StackMemoryAllocator::AllocSetDelete(MemoryContext context)
{
    StackSet set = (StackSet)context;
    StackBlock block = set->blocks;
    MemoryProtectFuncDef* func = NULL;

    if (context->session_id > 0)
        func = &SessionFunctions;
    else
        func = &GenericFunctions;

    AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    AllocSetCheck(context);
#endif

    /* Make it look empty, just in case... */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    set->blocks = NULL;
    set->keeper = NULL;

    while (block != NULL) {
        StackBlock next = block->next;
        Size tempSize = block->allocSize;

        if (is_tracked)
            MemoryTrackingFreeInfo(context, tempSize);

        if (GS_MP_INITED)
            (*func->free)(block, tempSize);
        else
            gs_free(block, tempSize);
        block = next;
    }

    /* reset to 0 after deletion. */
    set->totalSpace = 0;
    set->freeSpace = 0;
}

Size StackMemoryAllocator::AllocSetGetChunkSpace(MemoryContext context, void* pointer)
{
    ereport(ERROR,
        (errcode(ERRCODE_INVALID_OPERATION), errmsg("unsupport to get memory size under stack memory allocator")));
    return 0;
}

bool StackMemoryAllocator::AllocSetIsEmpty(MemoryContext context)
{
    //
    // For now, we say "empty" only if the context is new or just reset. We
    // could examine the freelists to determine if all space has been freed,
    // but it's not really worth the trouble for present uses of this
    // functionality.
    //
    if (context->isReset) {
        return true;
    }

    return false;
}

/*
 * AllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */
void StackMemoryAllocator::AllocSetStats(MemoryContext context, int level)
{
    StackSet set = (StackSet)context;
    long nblocks = 0;
    long totalspace = 0;
    long freespace = 0;
    StackBlock block;
    int i;

    for (block = set->blocks; block != NULL; block = block->next) {
        nblocks++;
        totalspace += block->allocSize;
        freespace += block->allocSize - block->offset;
    }

    for (i = 0; i < level; i++)
        fprintf(stderr, "  ");

    fprintf(stderr,
        "  %s: %ld total in %ld blocks; %ld free; %ld used\n",
        set->header.name,
        totalspace,
        nblocks,
        freespace,
        totalspace - freespace);
}

/*
 * AllocSetCheck
 *		Walk through chunks and check consistency of memory.
 */
#ifdef MEMORY_CONTEXT_CHECKING
void StackMemoryAllocator::AllocSetCheck(MemoryContext context)
{
    // do nothing actually now.
    return;
}
#endif
