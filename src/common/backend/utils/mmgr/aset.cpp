/* -------------------------------------------------------------------------
 *
 * aset.c
 *	  Allocation set definitions.
 *
 * AllocSet is our standard implementation of the abstract MemoryContext
 * type.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/aset.c
 *
 * NOTE:
 *	This is a new (Feb. 05, 1999) implementation of the allocation set
 *	routines. AllocSet...() does not use OrderedSet...() any more.
 *	Instead it manages allocations in a block pool by itself, combining
 *	many small allocations in a few bigger blocks. AllocSetFree() normally
 *	doesn't free() memory really. It just add's the free'd area to some
 *	list for later reuse by AllocSetAlloc(). All memory blocks are free()'d
 *	at once on AllocSetReset(), which happens when the memory context gets
 *	destroyed.
 *				Jan Wieck
 *
 *	Performance improvement from Tom Lane, 8/99: for extremely large request
 *	sizes, we do want to be able to give the memory back to free() as soon
 *	as it is pfree()'d.  Otherwise we risk tying up a lot of memory in
 *	freelist entries that might never be usable.  This is specially needed
 *	when the caller is repeatedly repalloc()'ing a block bigger and bigger;
 *	the previous instances of the block were guaranteed to be wasted until
 *	AllocSetReset() under the old way.
 *
 *	Further improvement 12/00: as the code stood, request sizes in the
 *	midrange between "small" and "large" were handled very inefficiently,
 *	because any sufficiently large free chunk would be used to satisfy a
 *	request, even if it was much larger than necessary.  This led to more
 *	and more wasted space in allocated chunks over time.  To fix, get rid
 *	of the midrange behavior: we now handle only "small" power-of-2-size
 *	chunks as chunks.  Anything "large" is passed off to malloc().	Change
 *	the number of freelists to change the small/large boundary.
 *
 *
 *	About CLOBBER_FREED_MEMORY:
 *
 *	If this symbol is defined, all freed memory is overwritten with 0x7F's.
 *	This is useful for catching places that reference already-freed memory.
 *
 *	About MEMORY_CONTEXT_CHECKING:
 *
 *	Since we usually round request sizes up to the next power of 2, there
 *	is often some unused space immediately after a requested data area.
 *	Thus, if someone makes the common error of writing past what they've
 *	requested, the problem is likely to go unnoticed ... until the day when
 *	there *isn't* any wasted space, perhaps because of different memory
 *	alignment on a new platform, or some other effect.	To catch this sort
 *	of problem, the MEMORY_CONTEXT_CHECKING option stores 0x7E just beyond
 *	the requested space whenever the request is less than the actual chunk
 *	size, and verifies that the byte is undamaged when the chunk is freed.
 *
 * -------------------------------------------------------------------------
 */

#include <sys/mman.h>

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
#define ALLOC_CHUNK_LIMIT (1 << (ALLOCSET_NUM_FREELISTS - 1 + ALLOC_MINBITS))
/* Size of largest chunk that we use a fixed size for */
#define ALLOC_CHUNK_FRACTION 4
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
 * AllocPointerIsValid
 *		True iff pointer is valid allocation pointer.
 */
#define AllocPointerIsValid(pointer) PointerIsValid(pointer)

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
		 * This is the same as
		 *		pg_leftmost_one_pos32((size - 1) >> ALLOC_MINBITS) + 1
		 * or equivalently
		 *		pg_leftmost_one_pos32(size - 1) - ALLOC_MINBITS + 1
		 *
		 * However, rather than just calling that function, we duplicate the
		 * logic here, allowing an additional optimization.  It's reasonable
		 * to assume that ALLOC_CHUNK_LIMIT fits in 16 bits, so we can unroll
		 * the byte-at-a-time loop in pg_leftmost_one_pos32 and just handle
		 * the last two bytes.
		 *
		 * Yes, this function is enough of a hot-spot to make it worth this
		 * much trouble.
		 *----------
		 */
#ifdef HAVE__BUILTIN_CLZ
		idx = 31 - __builtin_clz((uint32) size - 1) - ALLOC_MINBITS + 1;
#else
		uint32		t,
					tsize;

		/* Statically assert that we only have a 16-bit input value. */
		StaticAssertStmt(ALLOC_CHUNK_LIMIT < (1 << 16),
						 "ALLOC_CHUNK_LIMIT must be less than 64kB");

		tsize = size - 1;
		t = tsize >> 8;
		idx = t ? pg_leftmost_one_pos[t] + 8 : pg_leftmost_one_pos[tsize];
		idx -= ALLOC_MINBITS - 1;
#endif
        Assert(idx < ALLOCSET_NUM_FREELISTS);
    } else
        idx = 0;

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

#ifdef RANDOMIZE_ALLOCATED_MEMORY

/*
 * Fill a just-allocated piece of memory with "random" data.  It's not really
 * very random, just a repeating sequence with a length that's prime.  What
 * we mainly want out of it is to have a good probability that two palloc's
 * of the same number of bytes start out containing different data.
 */
static void randomize_mem(char* ptr, size_t size)
{
    static int save_ctr = 1;
    int ctr;

    ctr = save_ctr;
    while (size-- > 0) {
        *ptr++ = ctr;
        if (++ctr > 251)
            ctr = 1;
    }
    save_ctr = ctr;
}
#endif /* RANDOMIZE_ALLOCATED_MEMORY */

/* built-in white list of memory context. see more @ GenericMemoryAllocator::AllocSetContextCreate() */
const char* built_in_white_list[] = {"ThreadTopMemoryContext",
    "Postmaster",
    "CommunnicatorGlobalMemoryContext",
    "SELF MEMORY CONTEXT",
    "SRF multi-call context",
    "INSERT TEMP MEM CNXT",
    "CStore PARTITIONED TEMP INSERT",
    "CSTORE BULKLOAD_ROWS",
    "ADIO CU CACHE CNXT",
    "global_stats_context",  //~
    "CacheMemoryContext",
    "PortalHeapMemory",
    "Statistics snapshot",
    "PgStatCollectThdStatus",
    "gs_signal",
    NULL};
#define COUNT_ARRAY_SIZE(array) (sizeof((array)) / sizeof(*(array)))

/* Compare two strings similar to strcmp().
 * But our function can compare two strings by wildcards(*).
 * eg.
 * execute strcmp_by_wildcards("abc*","abcdefg"),and return value is 0;
 * execute strcmp_by_wildcards("abc","abcdefg") ,and return value is not 0.
 * NOTE:
 * There is only one '*'(wildcards character) in the string,and must be end of this string.
 */
static int strcmp_by_wildcards(const char* str1, const char* str2)
{
    size_t len = strlen(str1);

    if (0 == strcmp(str1, str2))
        return 0;

    if (('*' != str1[len - 1]) || (strlen(str2) <= len))
        return -1;

    if (0 == strncmp(str1, str2, len - 1)) {
        const char* p = str2 + len - 1;
        while (*p && ((*p >= '0' && *p <= '9') || (*p == '_')))
            p++;

        if (*p) {
            return -1;
        }

        return 0;
    }

    return -1;
}

/* set the white list value */
void MemoryContextControlSet(AllocSet context, const char* name)
{
    bool isInWhiteList = false;

    if (u_sess == NULL)
        return;

    for (memory_context_list* iter = u_sess->utils_cxt.memory_context_limited_white_list;
        iter && iter->value && ENABLE_MEMORY_CONTEXT_CONTROL;
        iter = iter->next) {
        if (!strcmp_by_wildcards(iter->value, name)) {
            context->maxSpaceSize = 0xffffffff;
            isInWhiteList = true;
            break;
        }
    }

    if (!isInWhiteList) {
        for (const char** p = built_in_white_list; *p != NULL; p++) {
            if (!strcmp_by_wildcards(*p, name)) {
                context->maxSpaceSize = 0xffffffff;
                break;
            }
        }
    }

    Assert(context->maxSpaceSize >= 0);
}

/*
 * Public routines
 */
//
// AllocSetContextCreate
//		Create a new AllocSet context.
//      parameter:
//		@maxSize: Determine if memory allocation(eg. palloc function) is out of threshold.
//				  This parameter is the threshold.
//
MemoryContext AllocSetContextCreate(_in_ MemoryContext parent, _in_ const char* name, _in_ Size minContextSize,
    _in_ Size initBlockSize, _in_ Size maxBlockSize, _in_ MemoryContextType contextType, _in_ Size maxSize,
    _in_ bool isSession)
{
    switch (contextType) {
#ifndef ENABLE_MEMORY_CHECK
        case STANDARD_CONTEXT: {
            if (g_instance.attr.attr_memory.disable_memory_stats) {
                return opt_AllocSetContextCreate(parent, name, minContextSize, initBlockSize, maxBlockSize);
            } else {
                return GenericMemoryAllocator::AllocSetContextCreate(
                    parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, false, isSession);
            }
        }
        case SHARED_CONTEXT:
            /* The following situation is forbidden, parent context is not shared, while current context is shared. */
            if (parent != NULL && !parent->is_shared && contextType == SHARED_CONTEXT) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Failed while creating shared memory context \"%s\" from standard context\"%s\".",
                            name, parent->name)));
            }

            return GenericMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, true, false);
#else
        case STANDARD_CONTEXT:
            return AsanMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, false, isSession);
        case SHARED_CONTEXT:
            /* The following situation is forbidden, parent context is not shared, while current context is shared. */
            if (parent != NULL && !parent->is_shared && contextType == SHARED_CONTEXT) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_FAILED),
                        errmsg("Failed while creating shared memory context \"%s\" from standard context\"%s\".",
                            name, parent->name)));
            }

            return AsanMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, true, false);
#endif
        case STACK_CONTEXT:
            return StackMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, false, isSession);

        case MEMALIGN_CONTEXT:
            return AlignMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, false, isSession);
        case MEMALIGN_SHRCTX:
            return AlignMemoryAllocator::AllocSetContextCreate(
                parent, name, minContextSize, initBlockSize, maxBlockSize, maxSize, true, false);
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized context type")));
            break;
    }

    return NULL;
}

/*
 * AllocSetMethodDefinition
 *      Define the method functions based on the templated value
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void GenericMemoryAllocator::AllocSetMethodDefinition(MemoryContextMethods* method)
{
    method->alloc = &GenericMemoryAllocator::AllocSetAlloc<enable_memoryprotect, is_shared, is_tracked>;
    method->free_p = &GenericMemoryAllocator::AllocSetFree<enable_memoryprotect, is_shared, is_tracked>;
    method->realloc = &GenericMemoryAllocator::AllocSetRealloc<enable_memoryprotect, is_shared, is_tracked>;
    method->init = &GenericMemoryAllocator::AllocSetInit;
    method->reset = &GenericMemoryAllocator::AllocSetReset<enable_memoryprotect, is_shared, is_tracked>;
    method->delete_context = &GenericMemoryAllocator::AllocSetDelete<enable_memoryprotect, is_shared, is_tracked>;
    method->get_chunk_space = &GenericMemoryAllocator::AllocSetGetChunkSpace;
    method->is_empty = &GenericMemoryAllocator::AllocSetIsEmpty;
    method->stats = &GenericMemoryAllocator::AllocSetStats;
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
void GenericMemoryAllocator::AllocSetContextSetMethods(unsigned long value, MemoryContextMethods* method)
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
MemoryContext GenericMemoryAllocator::AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize, Size maxSize, bool isShared, bool isSession)
{
    AllocSet context;
    NodeTag type = isShared ? T_SharedAllocSetContext : T_AllocSetContext;
    bool isTracked = false;
    unsigned long value = isShared ? IS_SHARED : 0;
    MemoryProtectFuncDef* func = NULL;

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
    /*
     * If MemoryContext's name is in white list(GUC parameter,see @memory_context_limited_white_list),
     * then set maxSize as infinite,that is unlimited.
     */
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextControlSet(context, name);
#endif

    /* assign the method function with specified templated to the context */
    AllocSetContextSetMethods(value, ((MemoryContext)context)->methods);

    /*
     * Make sure alloc parameters are reasonable, and save them.
     *
     * We somewhat arbitrarily enforce a minimum 1K block size.
     */
    initBlockSize = MAXALIGN(initBlockSize);
    if (initBlockSize < 1024)
        initBlockSize = 1024;
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
    context->allocChunkLimit = ALLOC_CHUNK_LIMIT;
    while ((Size)(context->allocChunkLimit + ALLOC_CHUNKHDRSZ) >
           (Size)((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION))
        context->allocChunkLimit >>= 1;

    /*
     * Grab always-allocated space, if requested
     */
    if (minContextSize > ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ) {
        Size blksize = MAXALIGN(minContextSize);
        AllocBlock block;

        if (GS_MP_INITED)
            block = (AllocBlock)(*func->malloc)(blksize, (value & IS_PROTECT) == 1 ? true : false);
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

    return (MemoryContext)context;
}

/*
 * AllocSetInit
 *		Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AllocSetContextCreate can allocate memory when it gets control
 * back, however.)
 */
void GenericMemoryAllocator::AllocSetInit(MemoryContext context)
{
    /*
     * Since MemoryContextCreate already zeroed the context node, we don't
     * have to do anything here: it's already OK.
     */
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
void GenericMemoryAllocator::AllocSetReset(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
    AllocBlock block;
    MemoryProtectFuncDef* func = NULL;

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
    AllocSetCheck(context);
#endif

    /* Clear chunk freelists */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));

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

            /* Normal case, release the block */
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
void GenericMemoryAllocator::AllocSetDelete(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
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
    AllocSetCheck(context);
#endif

    /* Make it look empty, just in case... */
    MemSetAligned(set->freelist, 0, sizeof(set->freelist));
    set->blocks = NULL;
    set->keeper = NULL;

    while (block != NULL) {
        AllocBlock next = block->next;
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
void* GenericMemoryAllocator::AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
{
    Assert(file != NULL);
    Assert(line != 0);
    AllocSet set = (AllocSet)context;
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

        if (GS_MP_INITED) {
            block = (AllocBlock)(*func->malloc)(blksize, enable_memoryprotect);
        } else {
            gs_malloc(blksize, block, AllocBlock);
        }
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
    chunk = set->freelist[fidx];
    if (chunk != NULL) {
        Assert(chunk->size >= size);
        Assert(chunk->aset != set);
        Assert((int)fidx == AllocSetFreeIndex(chunk->size));

        set->freelist[fidx] = (AllocChunk)chunk->aset;

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
                chunk->aset = (void*)set->freelist[a_fidx];
                set->freelist[a_fidx] = chunk;
            }

            /* Mark that we need to create a new block */
            block = NULL;
        }
    }

    /*
     * Time to create a new regular (multi-chunk) block?
     */
    if (block == NULL) {
        Size required_size;

        /*
         * The first such block has size initBlockSize, and we double the
         * space in each succeeding block, but not more than maxBlockSize.
         */
        blksize = set->nextBlockSize;
        set->nextBlockSize <<= 1;
        if (set->nextBlockSize > set->maxBlockSize)
            set->nextBlockSize = set->maxBlockSize;

        /*
         * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
         * space... but try to keep it a power of 2.
         */
        required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
        while (blksize < required_size)
            blksize <<= 1;

        /* Try to allocate it */
        if (GS_MP_INITED)
            block = (AllocBlock)(*func->malloc)(blksize, enable_memoryprotect);
        else
            gs_malloc(blksize, block, AllocBlock);
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

static void check_pointer_valid(AllocBlock block, bool is_shared,
    MemoryContext context, AllocChunk chunk)
{
    AllocSet set = (AllocSet)context;

    if (block->aset != set) {
        if (is_shared)
            MemoryContextUnlock(context);
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
            errmsg("The block was freed before this time.")));
    }
    
    if (block->freeptr != block->endptr ||
        block->freeptr != ((char*)block) + (chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ)) {
        if (is_shared)
            MemoryContextUnlock(context);
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
            errmsg("The memory use was overflow.")));
    }
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
template <bool enable_memoryprotect, bool is_shared, bool is_tracked>
void GenericMemoryAllocator::AllocSetFree(MemoryContext context, void* pointer)
{
    AllocSet set = (AllocSet)context;
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

        if (GS_MP_INITED)
            (*func->free)(block, tempSize);
        else
            gs_free(block, tempSize);
#ifndef ENABLE_MEMORY_CHECK
    } else {
        /* Normal case, put the chunk into appropriate freelist */
        int fidx = AllocSetFreeIndex(chunk->size);

        chunk->aset = (void*)set->freelist[fidx];
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
        set->freelist[fidx] = chunk;
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
void* GenericMemoryAllocator::AllocSetRealloc(
    MemoryContext context, void* pointer, Size align, Size size, const char* file, int line)
{
    AllocSet set = (AllocSet)context;
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

        if (GS_MP_INITED)
            block = (AllocBlock)(*func->realloc)(oldBlock, oldBlock->allocSize, blksize, enable_memoryprotect);
        else
            gs_realloc(oldBlock, oldBlock->allocSize, block, blksize, AllocBlock);

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
 * AllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
Size GenericMemoryAllocator::AllocSetGetChunkSpace(MemoryContext context, void* pointer)
{
    AllocChunk chunk = AllocPointerGetChunk(pointer);

    return chunk->size + ALLOC_CHUNKHDRSZ;
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
bool GenericMemoryAllocator::AllocSetIsEmpty(MemoryContext context)
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
void GenericMemoryAllocator::AllocSetStats(MemoryContext context, int level)
{
    AllocSet set = (AllocSet)context;
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

void AllocSetCheckPointer(void* pointer)
{
    AllocChunkData* chunk = (AllocChunkData*)(((char*)(pointer)) - ALLOC_CHUNKHDRSZ);

    /* For opt memory context, we use sentinel instead of magic number to protect memory overflow. */
    if (!IsOptAllocSetContext(chunk->aset)) {
        AllocMagicData* magic =
            (AllocMagicData*)(((char*)chunk) + ALLOC_CHUNKHDRSZ + MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ);

        Assert(magic->aset == chunk->aset && magic->size == chunk->size && magic->posnum == PosmagicNum);
    }
}

/*
 * AllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
void GenericMemoryAllocator::AllocSetCheck(MemoryContext context)
{
#ifndef ENABLE_MEMORY_CHECK
    AllocSet set = (AllocSet)context;
    char* name = set->header.name;
    AllocBlock prevblock;
    AllocBlock block;

    for (prevblock = NULL, block = set->blocks; block != NULL; prevblock = block, block = block->next) {
        char* bpoz = ((char*)block) + ALLOC_BLOCKHDRSZ;
        long blk_used = block->freeptr - bpoz;
        long blk_data = 0;
        long nchunks = 0;

        /*
         * Empty block - empty can be keeper-block only
         */
        if (!blk_used) {
            if (set->keeper != block) {
                Assert(0);
                ereport(WARNING, (errmsg("problem in alloc set %s: empty block", name)));
            }
        }

        /*
         * Check block header fields
         */
        if (block->aset != set || block->prev != prevblock || block->freeptr < bpoz || block->freeptr > block->endptr) {
            ereport(WARNING, (errmsg("problem in alloc set %s: corrupt header in block", name)));
        }

        /*
         * Chunk walker
         */
        while (bpoz < block->freeptr) {
            AllocChunk chunk = (AllocChunk)bpoz;
            Size chsize, dsize;

            chsize = chunk->size;          /* aligned chunk size */
            dsize = chunk->requested_size; /* real data */

            /*
             * Check chunk size
             */
            if (dsize > chsize) {
                Assert(0);
                ereport(WARNING,
                    (errmsg("problem in alloc set %s: req size > alloc size",
                        name)));
            }
            if (chsize < (1 << ALLOC_MINBITS)) {
                Assert(0);
                ereport(WARNING,
                    (errmsg("problem in alloc set %s: bad size %lu",
                        name,
                        (unsigned long)chsize)));
            }

            /* single-chunk block? */
            if (chsize > set->allocChunkLimit && (long)(chsize + ALLOC_CHUNKHDRSZ) != blk_used) {
                Assert(0);
                ereport(
                    WARNING, (errmsg("problem in alloc set %s: bad single-chunk in block", name)));
            }

            /*
             * If chunk is allocated, check for correct aset pointer. (If it's
             * free, the aset is the freelist pointer, which we can't check as
             * easily...)
             */
            if (dsize > 0 && chunk->aset != (void*)set) {
                Assert(0);
                ereport(WARNING,
                    (errmsg("problem in alloc set %s: bogus aset link", name)));
            }

            /*
             * Check for overwrite of "unallocated" space in chunk
             */
            if (dsize > 0 && dsize < chsize && dsize != (Size)MAXALIGN(dsize) &&
                !sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dsize - ALLOC_MAGICHDRSZ)) {
                Assert(0);
                ereport(WARNING,
                    (errmsg("problem in alloc set %s: detected write past chunk end", name)));
            }

            blk_data += chsize;
            nchunks++;

            bpoz += ALLOC_CHUNKHDRSZ + chsize;
        }

        if ((blk_data + (nchunks * ALLOC_CHUNKHDRSZ)) != (unsigned long)blk_used) {
            Assert(0);
            ereport(WARNING, (errmsg("problem in alloc set %s: found inconsistent memory block", name)));
        }
    }
#endif
}

/*
 * chunk walker
 */
static void dumpAllocChunk(AllocBlock blk, StringInfoData* memoryBuf)
{
    char* name = blk->aset->header.name;
    char* bpoz = ((char*)blk) + ALLOC_BLOCKHDRSZ;
    while (bpoz < blk->freeptr) {
        AllocChunk chunk = (AllocChunk)bpoz;
        Size chsize = chunk->size;
        Size dsize = chunk->requested_size;

        // the chunk is free, so skip it
        if (0 == chunk->requested_size) {
            bpoz += ALLOC_CHUNKHDRSZ + chsize;
            continue;
        }

        // check chunk size
        if (dsize > chsize) {
            ereport(LOG,
                (errmsg("dump_memory: ERROR in chunk: req size > alloc size for "
                        "chunk in block of context %s",
                    name)));
            ereport(LOG, (errmsg("dump_memory: don't dump all chunks after invalid chunk in this block!")));
            return;
        }

        appendStringInfo(memoryBuf,
            "%s:%d, %lu, %lu\n",
            chunk->file,
            chunk->line,
            (unsigned long)chunk->size,
            (unsigned long)chunk->requested_size);

        bpoz += ALLOC_CHUNKHDRSZ + chsize;
    }
}

void dumpAllocBlock(AllocSet set, StringInfoData* memoryBuf)
{
    for (AllocBlock blk = set->blocks; blk != NULL; blk = blk->next) {
        char* bpoz = ((char*)blk) + ALLOC_BLOCKHDRSZ;
        long blk_used = blk->freeptr - bpoz;

        // empty block - empty can be keeper-block only (from AllocSetCheck())
        if (!blk_used)
            continue;

        // there are chunks in block
        dumpAllocChunk(blk, memoryBuf);
    }

    return;
}

#endif /* MEMORY_CONTEXT_CHECKING */

#ifdef MEMORY_CONTEXT_TRACK
/*
 * chunk walker
 */
static void GetAllocChunkInfo(AllocSet set, AllocBlock blk, StringInfoDataHuge* memoryBuf)
{
    char* bpoz = ((char*)blk) + ALLOC_BLOCKHDRSZ;
    while (bpoz < blk->freeptr) {
        AllocChunk chunk = (AllocChunk)bpoz;
        Size chsize = chunk->size;

        /* the chunk is free, so skip it */
        if (chunk->aset != set) {
            bpoz += ALLOC_CHUNKHDRSZ + chsize;
            continue;
        }

        if (memoryBuf != NULL) {
            appendStringInfoHuge(memoryBuf, "%s:%d, %lu\n", chunk->file, chunk->line, chunk->size);
        }

        bpoz += ALLOC_CHUNKHDRSZ + chsize;
    }
}

void GetAllocBlockInfo(AllocSet set, StringInfoDataHuge* memoryBuf)
{
    for (AllocBlock blk = set->blocks; blk != NULL; blk = blk->next) {
        char* bpoz = ((char*)blk) + ALLOC_BLOCKHDRSZ;
        long blk_used = blk->freeptr - bpoz;

        /* empty block - empty can be keeper-block only (from AllocSetCheck()) */
        if (!blk_used)
            continue;

        /* there are chunks in block */
        GetAllocChunkInfo(set, blk, memoryBuf);
    }

    return;
}

#endif
/*
 * alloc_trunk_size
 *	Given a width, calculate how many bytes are actually allocated
 *
 * Parameters:
 *	@in width: input width
 *
 * Returns: actually allocated mem bytes
 */
int alloc_trunk_size(int width)
{
    return Max((int)sizeof(Datum), (1 << (unsigned int)my_log2((width) + ALLOC_MAGICHDRSZ))) + ALLOC_CHUNKHDRSZ;
}
