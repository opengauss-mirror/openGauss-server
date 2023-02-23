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
 * asetalg.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/asetalg.cpp
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

extern void MemoryContextControlSet(AllocSet context, const char* name);

/*
 * AllocPointerIsValid
 *		True if pointer is valid allocation pointer.
 */
#define AllocSetIsValid(set) PointerIsValid(set)

/*
 * AllocSetMethodDefinition
 *      Define the method functions based on the templated value
 */
template <bool is_shared, bool is_tracked>
void AlignMemoryAllocator::AllocSetMethodDefinition(MemoryContextMethods* method)
{
    method->alloc = &AlignMemoryAllocator::AllocSetAlloc<is_shared, is_tracked>;
    method->free_p = &AlignMemoryAllocator::AllocSetFree<is_shared, is_tracked>;
    method->realloc = &AlignMemoryAllocator::AllocSetRealloc<is_shared, is_tracked>;
    method->init = &AlignMemoryAllocator::AllocSetInit;
    method->reset = &AlignMemoryAllocator::AllocSetReset<is_shared, is_tracked>;
    method->delete_context = &AlignMemoryAllocator::AllocSetDelete<is_shared, is_tracked>;
    method->get_chunk_space = &AlignMemoryAllocator::AllocSetGetChunkSpace;
    method->is_empty = &AlignMemoryAllocator::AllocSetIsEmpty;
    method->stats = &AlignMemoryAllocator::AllocSetStats;
#ifdef MEMORY_CONTEXT_CHECKING
    method->check = &AlignMemoryAllocator::AllocSetCheck;
#endif
}

/*
 * AllocSetContextSetMethods
 *		set the method functions
 *
 * Note: unsupprot to track the memory usage of shared context
 */
void AlignMemoryAllocator::AllocSetContextSetMethods(unsigned long value, MemoryContextMethods* method)
{
    bool isTracked = (value & IS_TRACKED) ? true : false;
    bool isShared = (value & IS_SHARED) ? true : false;

    if (isShared)
        AllocSetMethodDefinition<true, false>(method);
    else {
        if (isTracked)
            AllocSetMethodDefinition<false, true>(method);
        else
            AllocSetMethodDefinition<false, false>(method);
    }
}

// AllocSetContextCreate
//		Create a new AllocSet context.
//
MemoryContext AlignMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    AllocSet context;
    NodeTag type = isShared ? T_MemalignSharedAllocSetContext : T_MemalignAllocSetContext;
    bool isTracked = false;
    unsigned long value = isShared ? IS_SHARED : 0;
    MemoryProtectFuncDef* func = NULL;

    if (isShared)
        func = &SharedFunctions;
    else if (!isSession && (parent == NULL || parent->session_id == 0))
        func = &GenericFunctions;
    else
        func = &SessionFunctions;

    /* only track the unshared context after t_thrd.mem_cxt.mem_track_mem_cxt is created */
    if (func == &GenericFunctions && parent && (MEMORY_TRACKING_MODE > MEMORY_TRACKING_PEAKMEMORY) && t_thrd.mem_cxt.mem_track_mem_cxt &&
        (t_thrd.utils_cxt.ExecutorMemoryTrack == NULL || ((AllocSet)parent)->track)) {
        isTracked = true;
        value |= IS_TRACKED;
    }

    /* Do the type-independent part of context creation */
    context = (AllocSet)MemoryContextCreate(type, sizeof(AllocSetContext), parent, name, __FILE__, __LINE__);

    if (isShared && maxSize == DEFAULT_MEMORY_CONTEXT_MAX_SIZE) {
        // default maxSize of shared memory context.
        context->maxSpaceSize = SHARED_MEMORY_CONTEXT_MAX_SIZE;
    } else {
        // set by user (shared or generic),default generic max size(eg. 10M),
        // these three types run following scope:
        context->maxSpaceSize = maxSize + SELF_GENRIC_MEMCTX_LIMITATION;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextControlSet(context, name);
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

    context->maxBlockSize = maxBlockSize;
    context->initBlockSize = initBlockSize;
    context->nextBlockSize = initBlockSize;
    context->allocChunkLimit = ALLOC_CHUNK_LIMIT;

    /* create the memory tracking structure */
    if (isTracked)
        MemoryTrackingCreate((MemoryContext)context, parent);

    // Init the first Block
    Size blksize = MAXALIGN(initBlockSize);
    AllocBlock block;

    if (GS_MP_INITED)
        block = (AllocBlock)(*func->malloc)(blksize, true);
    else
        gs_malloc(blksize, block, AllocBlock);

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

    block->next = NULL;
    context->blocks = block;

    /* Mark block as not to be released at reset time */
    context->keeper = block;

    context->header.is_shared = isShared;
    if (isShared) {
        int ret = pthread_rwlock_init(&(context->header.lock), NULL);
        if (ret != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INITIALIZE_FAILED), errmsg("failed to initialize rwlock in AllocSetContextCreate.")));
        }
    }

    return (MemoryContext)context;
}

/*
 * AllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
template <bool is_shared, bool is_tracked>
void* AlignMemoryAllocator::AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
{
    AllocSet set = (AllocSet)context;
    AllocBlock block;
    Size blksize;
    MemoryProtectFuncDef* func = NULL;
    void* addr = NULL;
    int ret = 0;

    AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
    /* memory enjection */
    if (gs_memory_enjection())
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
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    /* allocate the align memory in one block */
    blksize = MAXALIGN(size) + ALLOC_BLOCKHDRSZ;

    if (GS_MP_INITED)
        ret = (*func->memalign)(&addr, align, blksize, true);
    else
        ret = posix_memalign(&addr, align, blksize);

    if (ret) {
        if (is_shared)
            MemoryContextUnlock(context);
        return NULL;
    }

    block = (AllocBlock)((char*)addr + MAXALIGN(size));

    block->aset = set;
    block->freeptr = block->endptr = ((char*)addr) + blksize;
    block->allocSize = blksize;
#ifdef MEMORY_CONTEXT_CHECKING
    block->magicNum = BlkMagicNum;
#endif

    /* enlarge total space only. */
    set->totalSpace += blksize;

    /* update the memory tracking information when allocating memory */
    if (is_tracked)
        MemoryTrackingAllocInfo(context, blksize);

    block->next = set->blocks->next;
    set->blocks->next = block;

    if (is_shared)
        MemoryContextUnlock(context);

    return addr;
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
template <bool is_shared, bool is_tracked>
void AlignMemoryAllocator::AllocSetFree(MemoryContext context, void* ptr)
{
    AllocSet set = (AllocSet)context;
    Size tempSize = 0;
    void* tmpptr = NULL;
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
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    /* look through all blocks and find the matched one */
    AllocBlock block = set->blocks;
    AllocBlock prevblock = NULL;

    while (block != NULL) {
        tmpptr = (void*)((char*)block - (block->allocSize - ALLOC_BLOCKHDRSZ));
        if (tmpptr == ptr)
            break;
        prevblock = block;
        block = block->next;
    }

    if (block == NULL) {
        if (is_shared)
            MemoryContextUnlock(context);
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("could not find block")));
    }

    /* OK, remove block from aset's list and free it */
    if (prevblock == NULL)
        set->blocks = block->next;
    else
        prevblock->next = block->next;

    tempSize = block->allocSize;

    set->totalSpace -= tempSize;

    if (is_tracked)
        MemoryTrackingFreeInfo(context, tempSize);

    if (GS_MP_INITED)
        (*func->free)(ptr, tempSize);
    else
        gs_free(ptr, tempSize);

    if (is_shared)
        MemoryContextUnlock(context);
}

/*
 * AllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 */
template <bool is_shared, bool is_tracked>
void* AlignMemoryAllocator::AllocSetRealloc(
    MemoryContext context, void* ptr, Size align, Size size, const char* file, int line)
{
    AllocSet set = (AllocSet)context;
    Size oldsize = 0;
    void* tmpptr = NULL;

    AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
    /* memory enjection */
    if (gs_memory_enjection())
        return NULL;
#endif

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (is_shared) {
        MemoryContextLock(context);
    }

    /* look through all blocks and find the matched one */
    AllocBlock block = set->blocks;

    while (block != NULL) {
        tmpptr = (void*)((char*)block - (block->allocSize - ALLOC_BLOCKHDRSZ));
        if (tmpptr == ptr)
            break;
        block = block->next;
    }

    if (block == NULL) {
        if (is_shared)
            MemoryContextUnlock(context);
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("could not find block")));
    }

    oldsize = block->allocSize;

    if (oldsize >= (size + ALLOC_BLOCKHDRSZ)) {
        size += ALLOC_BLOCKHDRSZ;
        block->allocSize = size;

        if (is_shared)
            MemoryContextUnlock(context);
    }

    /* reallocate the memory */
    AllocPointer newPointer;
    errno_t ret = 0;

    if (is_shared)
        MemoryContextUnlock(context);

    /* allocate new block */
    newPointer =
        AllocSetAlloc<is_shared, is_tracked>((MemoryContext)set, align, size, file, line);

    /* leave immediately if request was not completed */
    if (newPointer == NULL)
        return NULL;

    ret = memcpy_s(newPointer, oldsize, ptr, oldsize);
    if (ret != EOK) {
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("Error happen when execute memcpy_s:%d in reallocating aliged memory", (int)ret),
                errdetail("Maybe the parameter is error"),
                errhint("Please contact engineer to support.")));
    }

    /* free old block */
    AllocSetFree<is_shared, is_tracked>((MemoryContext)set, ptr);

    return newPointer;
}

void AlignMemoryAllocator::AllocSetInit(MemoryContext context)
{
    //
    // we don't
    // have to do anything here: it's already OK.
    //
}

/*
 * AllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 */
template <bool is_shared, bool is_tracked>
void AlignMemoryAllocator::AllocSetReset(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
    MemoryProtectFuncDef* func = NULL;
    AllocBlock block;

    AssertArg(AllocSetIsValid(set));

    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    AllocSetCheck(context);
#endif

    block = set->blocks;

    /* New blocks list is either empty or just the keeper block */
    set->blocks = set->keeper;
    while (block != NULL) {
        AllocBlock next = block->next;
        Size tempSize = block->allocSize;
        void* tmpptr = NULL;

        if (block == set->keeper) {
            /* Reset the block, but don't return it to malloc */
            char* datastart = ((char*)block) + ALLOC_BLOCKHDRSZ;

            block->freeptr = datastart;
            block->allocSize = tempSize;
            block->next = NULL;
        } else {
            tmpptr = (void*)((char*)block - (block->allocSize - ALLOC_BLOCKHDRSZ));

            if (is_tracked)
                MemoryTrackingFreeInfo(context, tempSize);

            /* Normal case, release the block */
            if (GS_MP_INITED)
                (*func->free)(tmpptr, tempSize);
            else
                gs_free(tmpptr, tempSize);
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
 */
template <bool is_shared, bool is_tracked>
void AlignMemoryAllocator::AllocSetDelete(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
    MemoryProtectFuncDef* func = NULL;
    AllocBlock block = NULL;

    AssertArg(AllocSetIsValid(set));

    if (set->blocks == NULL) {
        return;
    }

    block = set->blocks;

    if (is_shared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check for corruption and leaks before freeing */
    AllocSetCheck(context);
#endif

    /* Make it look empty, just in case... */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));

    while (block != NULL) {
        AllocBlock next = block->next;
        Size tempSize = block->allocSize;
        void* tmpptr = NULL;

        if (block == set->keeper)
            tmpptr = block;
        else
            tmpptr = (void*)((char*)block - (block->allocSize - ALLOC_BLOCKHDRSZ));

        if (is_tracked)
            MemoryTrackingFreeInfo(context, tempSize);

        if (GS_MP_INITED)
            (*func->free)(tmpptr, tempSize);
        else
            gs_free(tmpptr, tempSize);
        block = next;
    }

    set->blocks = NULL;
    set->keeper = NULL;

    /* reset to 0 after deletion. */
    set->freeSpace = 0;
    set->totalSpace = 0;

    if (is_shared) {
        MemoryContextUnlock(context);
    }
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
Size AlignMemoryAllocator::AllocSetGetChunkSpace(MemoryContext context, void* pointer)
{
    ereport(ERROR,
        (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
            errmsg("unsupport to get memory size under aligned memory allocator")));
    return 0;
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
bool AlignMemoryAllocator::AllocSetIsEmpty(MemoryContext context)
{
    bool ret = false;

    if (MemoryContextIsShared(context))
        MemoryContextLock(context);
    /*
     * For now, we say "empty" only if the context is new or just reset. We
     * could examine the freelists to determine if all space has been freed,
     * but it's not really worth the trouble for present uses of this
     * functionality.
     */
    if (context->isReset)
        ret = true;

    if (MemoryContextIsShared(context))
        MemoryContextUnlock(context);

    return ret;
}

/*
 * AllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */
void AlignMemoryAllocator::AllocSetStats(MemoryContext context, int level)
{
    AllocSet set = (AllocSet)context;
    AllocBlock block = set->blocks;
    long nblocks = 1;
    long totalspace = block->allocSize;
    long freespace = block->endptr - block->freeptr;
    int i;

    for (block = block->next; block != NULL; block = block->next) {
        nblocks++;
        totalspace += block->allocSize;
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

#ifdef MEMORY_CONTEXT_CHECKING
void AlignMemoryAllocator::AllocSetCheck(MemoryContext context)
{
    // do nothing actually now.
    return;
}
#endif
