/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Nippon Telegraph and Telephone Corporation
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
 * AsanMemoryAllocator.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/mmgr/AsanMemoryAllocator.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/mman.h>

#include "postgres.h"

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
#ifdef ENABLE_MEMORY_CHECK

#define ASAN_BLOCKRELSZ(sz) (MAXALIGN(sizeof(AsanBlockData)) + MAXALIGN(sz))

#define AllocPointerIsValid(pointer) PointerIsValid(pointer)

/*
 *  * AllocSetIsValid
 *   *      True iff set is valid allocation set.
 *    */
#define AsanSetIsValid(set) PointerIsValid(set)

#define AsanPointerGetBlock(ptr) ((AsanBlock)(((char*)(ptr)) - ASAN_BLOCKHDRSZ))
#define AsanBlockGetPointer(blk) ((AllocPointer)(((char*)((blk))) + ASAN_BLOCKHDRSZ))
/* ----------
 *  * Debug macros
 *   * ----------
 *    */
#ifdef HAVE_ALLOCINFO
#define AsanFreeInfo(_cxt, _asanblock) \
    fprintf(stderr, "AsanFree: %s: %p, %d\n", (_cxt)->header.name, (_asanblock), (_asanblock)->size)
#define AsanAllocInfo(_cxt, _asanblock) \
    fprintf(stderr, "AsanAlloc: %s: %p, %d\n", (_cxt)->header.name, (_asanblock), (_asanblock)->size)
#else
#define AsanFreeInfo(_cxt, _chunk)
#define AsanAllocInfo(_cxt, _chunk)
#endif

extern const char* built_in_white_list[];

/* Compare two strings similar to strcmp().
 * But our function can compare two strings by wildcards(*).
 * eg.
 * execute StrcmpByWildcards("abc*","abcdefg"),and return value is 0;
 * execute StrcmpByWildcards("abc","abcdefg") ,and return value is not 0.
 * NOTE:
 * There is only one '*'(wildcards character) in the string,and must be end of this string.
 */
static int StrcmpByWildcards(const char* str1, const char* str2)
{
    size_t len = strlen(str1);

    if (0 == strcmp(str1, str2)) {
        return 0;
    }

    if (('*' != str1[len - 1]) || (strlen(str2) <= len)) {
        return -1;
    }

    if (0 == strncmp(str1, str2, len - 1)) {
        const char* p = str2 + len - 1;
        while (*p && ((*p >= '0' && *p <= '9') || (*p == '_'))) {
            p++;
        }
        if (*p) {
            return -1;
        }
        return 0;
    }

    return -1;
}

/* set the white list value */
void AsanMemoryContextControlSet(AsanSet context, const char* name)
{
    bool isInWhiteList = false;

    if (u_sess != NULL) {
        for (memory_context_list* iter = u_sess->utils_cxt.memory_context_limited_white_list;
             iter && iter->value && u_sess->utils_cxt.enable_memory_context_control;
             iter = iter->next) {
            if (!StrcmpByWildcards(iter->value, name)) {
                context->maxSpaceSize = 0xffffffff;
                isInWhiteList = true;
                break;
            }
        }
    }

    if (!isInWhiteList) {
        for (const char** p = built_in_white_list; *p != NULL; p++) {
            if (!StrcmpByWildcards(*p, name)) {
                context->maxSpaceSize = 0xffffffff;
                break;
            }
        }
    }
    Assert(context->maxSpaceSize >= 0);
}

template <bool enableMemoryProtect, bool isShared, bool isTracked>
void AsanMemoryAllocator::AllocSetMethodDefinition(MemoryContextMethods* method)
{
    method->alloc = &AsanMemoryAllocator::AllocSetAlloc<enableMemoryProtect, isShared, isTracked>;
    method->free_p = &AsanMemoryAllocator::AllocSetFree<enableMemoryProtect, isShared, isTracked>;
    method->realloc = &AsanMemoryAllocator::AllocSetRealloc<enableMemoryProtect, isShared, isTracked>;
    method->init = &AsanMemoryAllocator::AllocSetInit;
    method->reset = &AsanMemoryAllocator::AllocSetReset<enableMemoryProtect, isShared, isTracked>;
    method->delete_context = &AsanMemoryAllocator::AllocSetDelete<enableMemoryProtect, isShared, isTracked>;
    method->get_chunk_space = &AsanMemoryAllocator::AllocSetGetChunkSpace;
    method->is_empty = &AsanMemoryAllocator::AllocSetIsEmpty;
    method->stats = &AsanMemoryAllocator::AllocSetStats;
#ifdef MEMORY_CONTEXT_CHECKING
    method->check = &AsanMemoryAllocator::AllocSetCheck;
#endif
}

void AsanMemoryAllocator::AllocSetContextSetMethods(unsigned long value, MemoryContextMethods* method)
{
    bool isProt = (value & IS_PROTECT) ? true : false;
    bool isShared = (value & IS_SHARED) ? true : false;
    bool isTracked = (value & IS_TRACKED) ? true : false;

    if (isProt) {
        if (isShared) {
            AllocSetMethodDefinition<true, true, false>(method);
        } else {
            if (isTracked) {
                AllocSetMethodDefinition<true, false, true>(method);
            } else {
                AllocSetMethodDefinition<true, false, false>(method);
            }
        }
    } else {
        if (isShared) {
            AllocSetMethodDefinition<false, true, false>(method);
        } else {
            if (isTracked) {
                AllocSetMethodDefinition<false, false, true>(method);
            } else {
                AllocSetMethodDefinition<false, false, false>(method);
            }
        }
    }
}

inline MemoryProtectFuncDef* setProtectFunc(bool isShared, bool isSession, MemoryContext parent)
{
    if (isShared) {
        return &SharedFunctions;
    } else if (!isSession && (parent == NULL || parent->session_id == 0)) {
        return &GenericFunctions;
    }

    return &SessionFunctions;
}

/*
 * Make sure alloc parameters are reasonable, and save them.
 * We somewhat arbitrarily enforce a minimum 1K block size.
 */
inline void CheckContextParameter(const char* name, bool isTracked, MemoryContext parent, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, AsanSet context, MemoryProtectFuncDef* func)
{
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

    /*initialize statistic*/
    context->totalSpace = 0;
    context->freeSpace = 0;

    /* create the memory track structure */
    if (isTracked) {
        MemoryTrackingCreate((MemoryContext)context, parent);
    }

    /*
     * Grab always-allocated space, if requested
     */
    if (minContextSize > ASAN_BLOCKHDRSZ) {
        Size blkSize = MAXALIGN(minContextSize);
        AsanBlock block;

        if (t_thrd.utils_cxt.gs_mp_inited) {
            block = (AsanBlock)(*func->malloc)(blkSize, true);
        } else {
            gs_malloc(blkSize, block, AsanBlock);
        }

        if (block == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                    errmsg("memory is temporarily unavailable"),
                    errdetail("Failed while creating memory context \"%s\".", name)));
        }
        block->aset = context;
        block->requestSize = minContextSize;
        block->file = __FILE__;
        block->line = __LINE__;

        context->totalSpace += blkSize;

        /* update the memory tracking information when allocating memory */
        if (isTracked)
            MemoryTrackingAllocInfo((MemoryContext)context, blkSize);

        block->prev = NULL;
        block->next = NULL;
        /* Remember block as part of block list */
        context->blocks = block;
        /* Mark block as not to be released at reset time */
        context->keeper = block;
    }
}

MemoryContext AsanMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    AsanSet context;
    NodeTag type = isShared ? T_SharedAllocSetContext : T_AsanSetContext;
    bool isTracked = false;
    unsigned long value = isShared ? IS_SHARED : 0;
    MemoryProtectFuncDef* func = setProtectFunc(isShared, isSession, parent);

    /* we want to be sure ErrorContext still has some memory
     * even if we've run out elsewhere!
     * Don't limit the memory allocation for ErrorContext.
     * And skip memory tracking memory allocation.
     */
    if ((strcmp(name, "ErrorContext") != 0) && (strcmp(name, "MemoryTrackMemoryContext") != 0)
        && (strcmp(name, "DolphinErrorData") != 0)) {
        value |= IS_PROTECT;
    }

    /* only track the unshared context after MemoryTrackMemoryContext is created */
    if (func == &GenericFunctions && parent && MEMORY_TRACKING_MODE && t_thrd.mem_cxt.mem_track_mem_cxt &&
        (t_thrd.utils_cxt.ExecutorMemoryTrack == NULL || ((AsanSet)parent)->track)) {
        isTracked = true;
        value |= IS_TRACKED;
    }

    /* Do the type-independent part of context creation */
    context = (AsanSet)MemoryContextCreate(type, sizeof(AsanSetContext), parent, name, __FILE__, __LINE__);

    if (isShared && maxSize == DEFAULT_MEMORY_CONTEXT_MAX_SIZE) {
        /* default maxSize of shared memory context. */
        context->maxSpaceSize = SHARED_MEMORY_CONTEXT_MAX_SIZE;
    } else {
        /* set by user (shared or generic),default generic max size(eg. 10M),
         * these three types run following scope:
         */
        context->maxSpaceSize = maxSize + SELF_GENRIC_MEMCTX_LIMITATION;
    }

    /*
     * If MemoryContext's name is in white list(GUC parameter,see @memory_context_limited_white_list),
     * then set maxSize as infinite,that is unlimited.
     */
    AsanMemoryContextControlSet(context, name);

    /* assign the method function with specified templated to the context */
    AllocSetContextSetMethods(value, ((MemoryContext)context)->methods);

    CheckContextParameter(name, isTracked, parent, minContextSize, initBlockSize, maxBlockSize, context, func);

    context->header.is_shared = isShared;
    if (isShared) {
        (void)pthread_rwlock_init(&(context->header.lock), NULL);
    }

    return (MemoryContext)context;
}

/*
 * AllocSetInit
 * Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AllocSetContextCreate can allocate memory when it gets control
 * back, however.)
 **/
void AsanMemoryAllocator::AllocSetInit(MemoryContext context)
{
    /*
     * Since MemoryContextCreate already zeroed the context node, we don't
     * have to do anything here: it's already OK.
     */
}

/*
 * AllocSetReset
 *      Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not necessarily
 * give back all the resources the set owns.  Our actual implementation is
 * that we hang onto any "keeper" block specified for the set.  In this way,
 * we don't thrash malloc() when a context is repeatedly reset after small
 * allocations, which is typical behavior for per-tuple contexts.
 */
template <bool enableMemoryProtect, bool isShared, bool isTracked>
void AsanMemoryAllocator::AllocSetReset(MemoryContext context)
{
    AsanSet set = (AsanSet)context;
    AsanBlock block;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AsanSetIsValid(set));

    if (isShared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }
    block = set->blocks;

    /* New blocks list is either empty or just the keeper block */
    set->blocks = set->keeper;
    while (block != NULL) {
        AsanBlock next = block->next;
        Size tempSize = ASAN_BLOCKRELSZ(block->requestSize);

        if (block == set->keeper) {
            /* Reset the block, but don't return it to malloc */
            block->next = NULL;
            block->prev = NULL;
        } else {
            if (isTracked) {
                MemoryTrackingFreeInfo(context, tempSize);
            }
            /* Normal case, release the block */
            if (GS_MP_INITED) {
                (*func->free)(block, tempSize);
            } else {
                gs_free(block, tempSize);
            }
        }
        block = next;
    }
    /* Reset block size allocation sequence, too */
    set->nextBlockSize = set->initBlockSize;

    if (set->blocks != NULL) {
        /*calculate memory statisic after reset.*/
        block = set->blocks;
        set->totalSpace = ASAN_BLOCKRELSZ(block->requestSize);
    } else {
        set->totalSpace = 0;
    }

    if (isShared) {
        MemoryContextUnlock(context);
    }
}

/*
 * AllocSetDelete
 *      Frees all memory which is allocated in the given set,
 *      in preparation for deletion of the set.
 *
 * Unlike AllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
template <bool enableMemoryProtect, bool isShared, bool isTracked>
void AsanMemoryAllocator::AllocSetDelete(MemoryContext context)
{
    AsanSet set = (AsanSet)context;
    AsanBlock block = set->blocks;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AsanSetIsValid(set));
    MemoryContextLock(context);

    if (set->blocks == NULL) {
        MemoryContextUnlock(context);
        return;
    }

    block = set->blocks;

    if (isShared) {
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    set->blocks = NULL;
    set->keeper = NULL;

    while (block != NULL) {
        AsanBlock next = block->next;
        Size tempSize = ASAN_BLOCKRELSZ(block->requestSize);
        if (isTracked) {
            MemoryTrackingFreeInfo(context, tempSize);
        }

        if (GS_MP_INITED) {
            (*func->free)(block, tempSize);
        } else {
            gs_free(block, tempSize);
        }
        block = next;
    }

    /*reset to 0 after deletion.*/
    set->totalSpace = 0;

    MemoryContextUnlock(context);
}

inline AsanBlock DoAllocMemoryInternal(MemoryContext context, Size size, const char* file, int line,
    MemoryProtectFuncDef* func, bool isTracked, bool isProtect)
{
    AsanSet set = (AsanSet)context;
    AsanBlock block;
    Size blkSize = ASAN_BLOCKRELSZ(size);

    AssertArg(AsanSetIsValid(set));

    if (GS_MP_INITED) {
        block = (AsanBlock)(*func->malloc)(blkSize, isProtect);
    } else {
        gs_malloc(blkSize, block, AsanBlock);
    }

    if (block == NULL) {
        return NULL;
    }

    block->aset = set;
    block->requestSize = size;

    /*enlarge total space only.*/
    set->totalSpace += blkSize;

    /* update the memory tracking information when allocating memory */
    if (isTracked) {
        MemoryTrackingAllocInfo(context, blkSize);
    }

    block->file = file;
    block->line = line;
    /* track the detail allocation information */
    MemoryTrackingDetailInfo(context, size, blkSize, file, line);

    /*
     * Stick the new block underneath the active allocation block, so that
     * we don't lose the use of the space remaining therein.
     */
    if (set->blocks != NULL) {
        block->prev = set->blocks;
        block->next = set->blocks->next;
        if (block->next) {
            block->next->prev = block;
        }
        set->blocks->next = block;
    } else {
        block->prev = NULL;
        block->next = NULL;
        set->blocks = block;
    }

    AsanAllocInfo(set, block);
    return block;
}

/*
 * AllocSetAlloc
 *      Returns pointer to allocated memory of given size; memory is added
 *      to the set.
 */
template <bool enableMemoryProtect, bool isShared, bool isTracked>
void* AsanMemoryAllocator::AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
{
    AsanBlock block;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(align == 0);

    /* memory enjection */
    if (gs_memory_enjection() && 0 != strcmp(context->name, "ErrorContext")) {
        return NULL;
    }

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (isShared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    /*
     * If requested size exceeds maximum for chunks, allocate an entire block
     * for this request.
     */
    block = DoAllocMemoryInternal(context, size, file, line, func, isTracked, enableMemoryProtect);

    if (isShared) {
        MemoryContextUnlock(context);
    }

    if (block == NULL) {
        return NULL;
    }

    return AsanBlockGetPointer(block);
}

/*
 * AllocSetFree
 *      Frees allocated memory; memory is removed from the set.
 */
template <bool enableMemoryProtect, bool isShared, bool isTracked>
void AsanMemoryAllocator::AllocSetFree(MemoryContext context, void* pointer)
{
    AsanSet set = (AsanSet)context;
    AsanBlock block = AsanPointerGetBlock(pointer);
    Size tempSize = 0;
    MemoryProtectFuncDef* func = NULL;

    AssertArg(AsanSetIsValid(set));

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (isShared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    AsanFreeInfo(set, block);

    /*
     * Big chunks are certain to have been allocated as single-chunk
     * blocks.  Find the containing block and return it to malloc().
     */
    if (block->aset != set) {
        if (isShared) {
            MemoryContextUnlock(context);
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("%s Memory Context could not find block containing block", context->name)));
        }
    }
    /* OK, remove block from aset's list and free it */
    if (block->prev) {
        block->prev->next = block->next;
    } else {
        set->blocks = block->next;
    }

    if (block->next) {
        block->next->prev = block->prev;
    }

    tempSize = ASAN_BLOCKRELSZ(block->requestSize);

    set->totalSpace -= tempSize;

    /* clean the structure of block */
    block->aset = NULL;
    block->prev = NULL;
    block->next = NULL;
    block->requestSize = 0;
    if (isTracked) {
        MemoryTrackingFreeInfo(context, tempSize);
    }

    if (GS_MP_INITED) {
        (*func->free)(block, tempSize);
    } else {
        gs_free(block, tempSize);
    }

    if (isShared) {
        MemoryContextUnlock(context);
    }
}

AsanBlock DoReallocMemoryInternal(MemoryContext context, AsanBlock oldBlock, Size oldSize, Size size, const char* file,
    int line, MemoryProtectFuncDef* func, bool isTracked, bool isProtect)
{
    AsanSet set = (AsanSet)context;
    AsanBlock newBlock;
    Size newSize = ASAN_BLOCKRELSZ(size);

    if (GS_MP_INITED) {
        newBlock = (AsanBlock)(*func->realloc)(oldBlock, oldSize, newSize, isProtect);
    } else {
        gs_realloc(oldBlock, oldSize, newBlock, newSize, AsanBlock);
    }

    if (newBlock == NULL) {
        return NULL;
    }
    newBlock->requestSize = size;
    set->totalSpace += newSize - oldSize;
    /* update the memory tracking information when allocating memory */
    if (isTracked) {
        MemoryTrackingAllocInfo(context, newSize - oldSize);
    }

    /* Update pointers since block has likely been moved */
    if (newBlock->prev) {
        newBlock->prev->next = newBlock;
    } else {
        set->blocks = newBlock;
    }
    if (newBlock->next) {
        newBlock->next->prev = newBlock;
    }
    newBlock->file = file;
    newBlock->line = line;
    MemoryTrackingDetailInfo(context, size, newSize, file, line);

    return newBlock;
}

/*
 * AllocSetRealloc
 *      Returns new pointer to allocated memory of given size; this memory
 *      is added to the set.  Memory associated with given pointer is copied
 *      into the new memory, and the old memory is freed.
 */
template <bool enableMemoryProtect, bool isShared, bool isTracked>
void* AsanMemoryAllocator::AllocSetRealloc(
    MemoryContext context, void* pointer, Size align, Size size, const char* file, int line)
{
    AsanSet set = (AsanSet)context;
    AsanBlock oldBlock = AsanPointerGetBlock(pointer);
    Size oldSize = ASAN_BLOCKRELSZ(oldBlock->requestSize);
    MemoryProtectFuncDef* func = NULL;
    AsanBlock newBlock;

    AssertArg(AsanSetIsValid(set));
    AssertArg(align == 0);

    /* memory enjection */
    if (gs_memory_enjection() && 0 != strcmp(context->name, "ErrorContext")) {
        return NULL;
    }

    /*
     * If this is a shared context, make it thread safe by acquiring
     * appropriate lock
     */
    if (isShared) {
        MemoryContextLock(context);
        func = &SharedFunctions;
    } else {
        if (context->session_id > 0)
            func = &SessionFunctions;
        else
            func = &GenericFunctions;
    }

    /*
     * Try to verify that we have a sane block pointer: it should
     * reference the correct aset, and freeptr and endptr should point
     * just past the chunk.
     */
    if (oldBlock->aset != set) {
        if (isShared) {
            MemoryContextUnlock(context);
        }
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("%s Memory Context could not find block", context->name)));
    }

    /* Do the realloc */
    newBlock =
        DoReallocMemoryInternal(context, oldBlock, oldSize, size, file, line, func, isTracked, enableMemoryProtect);

    if (isShared) {
        MemoryContextUnlock(context);
    }

    if (newBlock == NULL) {
        return NULL;
    }

    return AsanBlockGetPointer(newBlock);
}

Size AsanMemoryAllocator::AllocSetGetChunkSpace(MemoryContext context, void* pointer)
{
    AsanBlock block = AsanPointerGetBlock(pointer);
    return ASAN_BLOCKRELSZ(block->requestSize);
}

/*
 * AllocSetIsEmpty
 *      Is an AsanSet empty of any allocated space?
 */
bool AsanMemoryAllocator::AllocSetIsEmpty(MemoryContext context)
{
    bool ret = false;

    if (MemoryContextIsShared(context)) {
        MemoryContextLock(context);
    }
    /*
     * For now, we say "empty" only if the context is new or just reset. We
     * could examine the freelists to determine if all space has been freed,
     * but it's not really worth the trouble for present uses of this
     * functionality.
     */
    if (context->isReset) {
        ret = true;
    }

    if (MemoryContextIsShared(context)) {
        MemoryContextUnlock(context);
    }

    return ret;
}

/*
 * AllocSetStats
 *      Displays stats about memory consumption of an AsanSet.
 */
void AsanMemoryAllocator::AllocSetStats(MemoryContext context, int level)
{
    AsanSet set = (AsanSet)context;
    long nblocks = 0;
    long totalspace = 0;
    AsanBlock block;
    int i;

    for (block = set->blocks; block != NULL; block = block->next) {
        nblocks++;
        totalspace += ASAN_BLOCKRELSZ(block->requestSize);
    }

    for (i = 0; i < level; i++) {
        fprintf(stderr, "  ");
    }

    fprintf(stderr, "%s: %ld total in %ld blocks\n", set->header.name, totalspace, nblocks);
}

void AsanMemoryAllocator::AllocSetCheck(MemoryContext context)
{
    /*
     * We used asan to check context. We don't
     * have to do anything here: it's already OK.
     */
}

void dumpAsanBlock(AsanSet set, StringInfoData* memoryBuf)
{
    for (AsanBlock blk = set->blocks; blk != NULL; blk = blk->next) {
        uint32 requestSize = MAXALIGN(blk->requestSize);
        uint32 realSize = ASAN_BLOCKRELSZ(blk->requestSize);
        appendStringInfo(memoryBuf, "%s:%d, %u, %u\n", blk->file, blk->line, realSize, requestSize);
    }

    return;
}

void GetAsanBlockInfo(AsanSet set, StringInfoDataHuge* memoryBuf)
{
    for (AsanBlock blk = set->blocks; blk != NULL; blk = blk->next) {
        uint32 realSize = ASAN_BLOCKRELSZ(blk->requestSize);
        appendStringInfoHuge(memoryBuf, "%s:%d, %u\n", blk->file, blk->line, realSize);
    }

    return;
}

#endif
