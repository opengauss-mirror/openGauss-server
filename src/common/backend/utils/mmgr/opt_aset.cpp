/* -------------------------------------------------------------------------
 *
 * std_aset.cpp
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

/*
 * Table for opt_AllocSetFreeIndex
 */
#define LT16(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n

static const unsigned char LogTable256[256] = {0,
    1,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    LT16(5),
    LT16(6),
    LT16(6),
    LT16(7),
    LT16(7),
    LT16(7),
    LT16(7),
    LT16(8),
    LT16(8),
    LT16(8),
    LT16(8),
    LT16(8),
    LT16(8),
    LT16(8),
    LT16(8)};

#define MAX_FREE_CONTEXTS 100	/* arbitrary limit on freelist length */

typedef struct AllocSetFreeList
{
    int			num_free;		/* current list length */
    AllocSetContext *first_free;	/* list header */
} AllocSetFreeList;

/* context_freelists[0] is for default params, [1] for small params */
THR_LOCAL AllocSetFreeList context_freelists[2] =
{
    {
        0, NULL
    },
    {
        0, NULL
    }
};

/* ----------
 * opt_AllocSetFreeIndex -
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= ALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int opt_AllocSetFreeIndex(Size size)
{
    int idx;

    if (size > (1 << ALLOC_MINBITS)) {
        idx = 31 - __builtin_clz((uint32) size - 1) - ALLOC_MINBITS + 1;
        Assert(idx < ALLOCSET_NUM_FREELISTS);
    } else
        idx = 0;

    return idx;
}

static void* opt_AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line);
static void opt_AllocSetFree(MemoryContext context, void* pointer);
static void* opt_AllocSetRealloc(MemoryContext context, void* pointer, Size align, Size size, const char* file, int line);
static void opt_AllocSetInit(MemoryContext context);
static void opt_AllocSetReset(MemoryContext context);
static void opt_AllocSetDelete(MemoryContext context);
static Size opt_AllocSetGetChunkSpace(MemoryContext context, void* pointer);
static bool opt_AllocSetIsEmpty(MemoryContext context);
static void opt_AllocSetStats(MemoryContext context, int level);
#ifdef MEMORY_CONTEXT_CHECKING
static void opt_AllocSetCheck(MemoryContext context);
#endif

/*
 * Public routines
 */
static MemoryContextMethods AllocSetMethods = {
    opt_AllocSetAlloc,
    opt_AllocSetFree,
    opt_AllocSetRealloc,
    opt_AllocSetInit,
    opt_AllocSetReset,
    opt_AllocSetDelete,
    opt_AllocSetGetChunkSpace,
    opt_AllocSetIsEmpty,
    opt_AllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
    ,opt_AllocSetCheck
#endif
};

#ifdef MEMORY_CONTEXT_CHECKING
static inline void
set_sentinel(void *base, Size offset)
{
    char	   *ptr = (char *) base + offset;

    *ptr = 0x7E;
}

static inline bool
sentinel_ok(const void *base, Size offset)
{
    const char *ptr = (const char *) base + offset;
    bool		ret;

    ret = *ptr == 0x7E;

    return ret;
}
#endif

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
MemoryContext opt_AllocSetContextCreate(MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize)
{
    AllocSet context;
    AllocBlock	block;
    int freeListIndex;
    Size		firstBlockSize;

    if (minContextSize == ALLOCSET_DEFAULT_MINSIZE &&
        initBlockSize == ALLOCSET_DEFAULT_INITSIZE)
        freeListIndex = 0;
    else if (minContextSize == ALLOCSET_SMALL_MINSIZE &&
             initBlockSize == ALLOCSET_SMALL_INITSIZE)
        freeListIndex = 1;
    else
        freeListIndex = -1;

    if (freeListIndex >= 0)
    {
        AllocSetFreeList *freelist = &context_freelists[freeListIndex];

        if (freelist->first_free != NULL)
        {
            /* Remove entry from freelist */
            context = freelist->first_free;
            freelist->first_free = (AllocSet) context->header.nextchild;
            freelist->num_free--;

            /* Update its maxBlockSize; everything else should be OK */
            context->maxBlockSize = maxBlockSize;

            /* Reinitialize its header, installing correct name and parent */
            opt_MemoryContextCreate((MemoryContext) context,
                                T_OptAllocSetContext,
                                &AllocSetMethods,
                                parent,
                                name);

            context->maxSpaceSize = SELF_GENRIC_MEMCTX_LIMITATION;
            return (MemoryContext) context;
        }
    }

    /* Determine size of initial block */
    firstBlockSize = MAXALIGN(sizeof(AllocSetContext)) +
        ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
    if (minContextSize != 0)
        firstBlockSize = Max(firstBlockSize, minContextSize);
    else
        firstBlockSize = Max(firstBlockSize, initBlockSize);

    context = (AllocSet) malloc(firstBlockSize);
    if (context == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed while creating memory context \"%s\".",
                           name)));

    block = (AllocBlock) (((char *) context) + MAXALIGN(sizeof(AllocSetContext)));
    block->aset = context;
    block->freeptr = ((char*)block) + ALLOC_BLOCKHDRSZ;
    block->endptr = ((char*)context) + firstBlockSize;
    block->allocSize = firstBlockSize;
    
    context->totalSpace = firstBlockSize;
    context->freeSpace = block->endptr - block->freeptr;
    
    block->prev = NULL;
    block->next = NULL;

    /* Remember block as part of block list */
    context->blocks = block;
    /* Mark block as not to be released at reset time */
    context->keeper = block;

    MemSetAligned(context->freelist, 0, sizeof(context->freelist));

    context->initBlockSize = initBlockSize;
    context->maxBlockSize = maxBlockSize;
    context->nextBlockSize = initBlockSize;
    context->freeListIndex = freeListIndex;

    context->allocChunkLimit = ALLOC_CHUNK_LIMIT;
    while ((Size)(context->allocChunkLimit + ALLOC_CHUNKHDRSZ) >
           (Size)((maxBlockSize - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION)) {
        context->allocChunkLimit >>= 1;
    }

    opt_MemoryContextCreate((MemoryContext) context,
                        T_OptAllocSetContext,
                        &AllocSetMethods,
                        parent,
                        name);

    context->maxSpaceSize = SELF_GENRIC_MEMCTX_LIMITATION;

    return (MemoryContext) context;
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
static void opt_AllocSetInit(MemoryContext context)
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
static void opt_AllocSetReset(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
    AllocBlock block;

    AssertArg(AllocSetIsValid(set));

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
            free(block);
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
}

/*
 * AllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike AllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void opt_AllocSetDelete(MemoryContext context)
{
    AllocSet set = (AllocSet)context;
    AssertArg(AllocSetIsValid(set));

    AllocBlock block = set->blocks;

    if (set->freeListIndex >= 0)
    {
        AllocSetFreeList *freelist = &context_freelists[set->freeListIndex];

        if (!context->isReset) {
            context->methods->reset(context);
            context->isReset = true;
        }

        if (freelist->num_free >= MAX_FREE_CONTEXTS)
        {
            while (freelist->first_free != NULL)
            {
                AllocSetContext *oldset = freelist->first_free;

                freelist->first_free = (AllocSetContext *) oldset->header.nextchild;
                freelist->num_free--;

                /* All that remains is to free the header/initial block */
                free(oldset);
            }
            Assert(freelist->num_free == 0);
        }

        /* Now add the just-deleted context to the freelist. */
        set->header.nextchild = (MemoryContext) freelist->first_free;
        freelist->first_free = set;
        freelist->num_free++;

        return;
    }

    while (block != NULL) {
        AllocBlock next = block->next;

        if (block != set->keeper)
            free(block);

        block = next;
    }

    free(set);
}

/*
 * AllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
static void* opt_AllocSetAlloc(MemoryContext context, Size align, Size size, const char* file, int line)
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

    AssertArg(AllocSetIsValid(set));
    AssertArg(align == 0);
    AssertArg(MemoryContextIsValid(context));

    /*
     * If requested size exceeds maximum for chunks, allocate an entire block
     * for this request.
     */
#ifndef ENABLE_MEMORY_CHECK
    if (size > set->allocChunkLimit) {
#endif
        chunk_size = MAXALIGN(size);
        blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

        block = (AllocBlock) malloc(blksize);
        if (block == NULL) {
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

        chunk = (AllocChunk)(((char*)block) + ALLOC_BLOCKHDRSZ);
        chunk->aset = set;
        chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < chunk_size)
            set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
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

        return AllocChunkGetPointer(chunk);
#ifndef ENABLE_MEMORY_CHECK
    }

    /*
     * Request is small enough to be treated as a chunk.  Look in the
     * corresponding free list to see if there is a free chunk we could reuse.
     * If one is found, remove it from the free list, make it again a member
     * of the alloc set and return its data address.
     */
    fidx = opt_AllocSetFreeIndex(size);
    chunk = set->freelist[fidx];
    if (chunk != NULL) {
        Assert(chunk->size >= size);
        Assert(chunk->aset != set);
        Assert((int)fidx == opt_AllocSetFreeIndex(chunk->size));

        set->freelist[fidx] = (AllocChunk)chunk->aset;

        chunk->aset = (void*)set;
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        chunk->prenum = PremagicNum;
        /* set mark to catch clobber of "unused" space */
        if (size < chunk->size)
            set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef MEMORY_CONTEXT_TRACK
        chunk->file = file;
        chunk->line = line;
#endif

        set->freeSpace -= (chunk->size + ALLOC_CHUNKHDRSZ);

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
                int a_fidx = opt_AllocSetFreeIndex(availchunk);

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
#ifdef MEMORY_CONTEXT_CHECKING
                chunk->requested_size = 0;
                chunk->prenum = 0;
#endif
#ifdef MEMORY_CONTEXT_TRACK
                chunk->file = file;
                chunk->line = line;
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
        block = (AllocBlock) malloc(blksize);
        if (block == NULL) {
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
#ifdef MEMORY_CONTEXT_CHECKING
    chunk->requested_size = size;
    chunk->prenum = PremagicNum;
    /* set mark to catch clobber of "unused" space */
    if (size < chunk_size)
        set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
#ifdef MEMORY_CONTEXT_TRACK
    chunk->file = file;
    chunk->line = line;
#endif

    return AllocChunkGetPointer(chunk);
#endif
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void opt_AllocSetFree(MemoryContext context, void* pointer)
{
    AllocSet set = (AllocSet)context;
    AllocChunk chunk = AllocPointerGetChunk(pointer);
    Size tempSize = 0;

    AssertArg(AllocSetIsValid(set));

#ifndef ENABLE_MEMORY_CHECK
    if (chunk->size > set->allocChunkLimit) {
#endif
        /*
         * Big chunks are certain to have been allocated as single-chunk
         * blocks.	Find the containing block and return it to malloc().
         */
        AllocBlock block = (AllocBlock)(((char*)chunk) - ALLOC_BLOCKHDRSZ);


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

        free(block);
#ifndef ENABLE_MEMORY_CHECK
    } else {
        /* Normal case, put the chunk into appropriate freelist */
        int fidx = opt_AllocSetFreeIndex(chunk->size);

        chunk->aset = (void*)set->freelist[fidx];
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = 0;
#endif
        set->freeSpace += chunk->size + ALLOC_CHUNKHDRSZ;
        set->freelist[fidx] = chunk;
        Assert(chunk->aset != set);
    }
#endif
}

/*
 * AllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 */
static void* opt_AllocSetRealloc(MemoryContext context, void* pointer, Size align, Size size, const char* file, int line)
{
    AllocSet set = (AllocSet)context;
    AllocChunk chunk = AllocPointerGetChunk(pointer);
    Size oldsize = chunk->size;

    AssertArg(AllocSetIsValid(set));
    AssertArg(align == 0);

    /*
     * Chunk sizes are aligned to power of 2 in AllocSetAlloc(). Maybe the
     * allocated area already is >= the new size.  (In particular, we always
     * fall out here if the requested size is a decrease.)
     */
    if (oldsize >= size) {
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        if (size < oldsize)
            set_sentinel(pointer, size);
#endif
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

        /*
         * Try to verify that we have a sane block pointer: it should
         * reference the correct aset, and freeptr and endptr should point
         * just past the chunk.
         */

        /* Do the realloc */
        chksize = MAXALIGN(size);
        blksize = chksize + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
        oldBlock = block;
        oldsize = block->allocSize;

        block = (AllocBlock) realloc(block, blksize);
        if (block == NULL) {
            return NULL;
        }
        block->freeptr = block->endptr = ((char*)block) + blksize;
        block->allocSize = blksize;

        set->totalSpace += blksize - oldsize;

        /* Update pointers since block has likely been moved */
        chunk = (AllocChunk)(((char*)block) + ALLOC_BLOCKHDRSZ);
        if (block->prev)
            block->prev->next = block;
        else
            set->blocks = block;
        if (block->next)
            block->next->prev = block;
        chunk->size = chksize;
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = size;
        if (size < chunk->size)
            set_sentinel(AllocChunkGetPointer(chunk), size);
#endif
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

        /* allocate new chunk */
        newPointer =
            opt_AllocSetAlloc((MemoryContext)set, align, size, __FILE__, __LINE__);

        /* leave immediately if request was not completed */
        if (newPointer == NULL)
            return NULL;

        /* transfer existing data (certain to fit) */
        memcpy(newPointer, pointer, oldsize);

        /* free old chunk */
        opt_AllocSetFree((MemoryContext)set, pointer);

        return newPointer;
    }
#endif
}

/*
 * AllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size opt_AllocSetGetChunkSpace(MemoryContext context, void* pointer)
{
    AllocChunk chunk = AllocPointerGetChunk(pointer);

    return chunk->size + ALLOC_CHUNKHDRSZ;
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
static bool opt_AllocSetIsEmpty(MemoryContext context)
{
    /*
     * For now, we say "empty" only if the context is new or just reset. We
     * could examine the freelists to determine if all space has been freed,
     * but it's not really worth the trouble for present uses of this
     * functionality.
     */
    if (context->isReset)
        return true;

    return false;
}

/*
 * AllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */
static void opt_AllocSetStats(MemoryContext context, int level)
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
/*
 * opt_AllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
opt_AllocSetCheck(MemoryContext context)
{
    AllocSet	set = (AllocSet) context;
    const char *name = set->header.name;
    AllocBlock	prevblock;
    AllocBlock	block;
    Size		total_allocated = 0;

    for (prevblock = NULL, block = set->blocks;
         block != NULL;
         prevblock = block, block = block->next)
    {
        char	   *bpoz = ((char *) block) + ALLOC_BLOCKHDRSZ;
        long		blk_used = block->freeptr - bpoz;
        long		blk_data = 0;
        long		nchunks = 0;

        if (set->keeper == block)
            total_allocated += block->endptr - ((char *) set);
        else
            total_allocated += block->endptr - ((char *) block);

        /*
         * Empty block - empty can be keeper-block only
         */
        if (!blk_used)
        {
            if (set->keeper != block)
                elog(WARNING, "problem in alloc set %s: empty block %p",
                     name, block);
        }

        /*
         * Check block header fields
         */
        if (block->aset != set ||
            block->prev != prevblock ||
            block->freeptr < bpoz ||
            block->freeptr > block->endptr)
            elog(WARNING, "problem in alloc set %s: corrupt header in block %p",
                 name, block);

        /*
         * Chunk walker
         */
        while (bpoz < block->freeptr)
        {
            AllocChunk	chunk = (AllocChunk) bpoz;
            Size		chsize,
                        dsize;

            chsize = chunk->size;	/* aligned chunk size */
            dsize = chunk->requested_size;	/* real data */

            /*
             * Check chunk size
             */
            if (dsize > chsize)
                elog(WARNING, "problem in alloc set %s: req size > alloc size for chunk %p in block %p",
                     name, chunk, block);
            if (chsize < (1 << ALLOC_MINBITS))
                elog(WARNING, "problem in alloc set %s: bad size %zu for chunk %p in block %p",
                     name, chsize, chunk, block);

            /* single-chunk block? */
            if (chsize > set->allocChunkLimit &&
                chsize + ALLOC_CHUNKHDRSZ != (Size)blk_used)
                elog(WARNING, "problem in alloc set %s: bad single-chunk %p in block %p",
                     name, chunk, block);

            /*
             * If chunk is allocated, check for correct aset pointer. (If it's
             * free, the aset is the freelist pointer, which we can't check as
             * easily...)  Note this is an incomplete test, since palloc(0)
             * produces an allocated chunk with requested_size == 0.
             */
            if (dsize > 0 && chunk->aset != (void *) set)
                elog(WARNING, "problem in alloc set %s: bogus aset link in block %p, chunk %p",
                     name, block, chunk);

            /*
             * Check for overwrite of padding space in an allocated chunk.
             */
            if (chunk->aset == (void *) set && dsize < chsize &&
                !sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dsize))
                elog(WARNING, "problem in alloc set %s: detected write past chunk end in block %p, chunk %p",
                     name, block, chunk);

            blk_data += chsize;
            nchunks++;

            bpoz += ALLOC_CHUNKHDRSZ + chsize;
        }

        if ((blk_data + (nchunks * (long)ALLOC_CHUNKHDRSZ)) != blk_used)
            elog(WARNING, "problem in alloc set %s: found inconsistent memory block %p",
                 name, block);
    }
}

#endif							/* MEMORY_CONTEXT_CHECKING */
