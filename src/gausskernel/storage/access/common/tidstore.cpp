/*-------------------------------------------------------------------------
 *
 * tidstore.c
 *        TID (bktId + ItemPointerData) storage implementation.
 *
 * TidStore is a in-memory data structure to store TIDs (bktId + ItemPointerData).
 * Internally it uses a radix tree as the storage for TIDs. The key is the
 * (bktId, BlockNumber) and the value is a bitmap of offsets, BlocktableEntry.
 *
 * TidStore is not thread-safe for now.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      src/backend/access/common/tidstore.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tidstore.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/lock/lwlock.h"


#define WORDNUM(x)    ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)    ((x) % BITS_PER_BITMAPWORD)

/* number of active words for a page: */
#define WORDS_PER_PAGE(n) ((n) / BITS_PER_BITMAPWORD + 1)

/* number of offsets we can store in the header of a BlocktableEntry */
#define NUM_FULL_OFFSETS (int) ((sizeof(uintptr_t) - sizeof(uint8) - sizeof(int8)) / sizeof(OffsetNumber))

/*
 * Encoding of radix tree's key
 * |-- 16 bits --|-- 16 bits --|-- 32 bits --|
 * |- reserved --|- bucket id -|- block id --|
 */
#define BKTID_MASK 0xFFFFUL
#define BLKNO_MASK 0xFFFFFFFFUL
#define BLKNO_SHIFT (sizeof(BlockNumber) * 8)
#define COMBINE_KEY(bktId, blkno) (uint64)( ( ( (uint64)((bktId)) & BKTID_MASK ) << BLKNO_SHIFT ) | ( (uint64)((blkno)) & BLKNO_MASK ) )
#define EXTRACT_BKTID_FROM_KEY(key) (int2)( ( (uint64)((key)) >> BLKNO_SHIFT ) & BKTID_MASK )
#define EXTRACT_BLKNO_FROM_KEY(key) (BlockNumber)( (uint64)((key)) & BLKNO_MASK )
/*
 * This is named similarly to PagetableEntry in tidbitmap.c
 * because the two have a similar function.
 */
typedef struct BlocktableEntry
{
    struct
    {
#ifndef WORDS_BIGENDIAN
        /*
         * We need to position this member to reserve space for the backing
         * radix tree to tag the lowest bit when struct 'header' is stored
         * inside a pointer or DSA pointer.
         */
        uint8        flags;

        int8        nwords;
#endif

        /*
         * We can store a small number of offsets here to avoid wasting space
         * with a sparse bitmap.
         */
        OffsetNumber full_offsets[NUM_FULL_OFFSETS];

#ifdef WORDS_BIGENDIAN
        int8        nwords;
        uint8        flags;
#endif
    }            header;

    /*
     * We don't expect any padding space here, but to be cautious, code
     * creating new entries should zero out space up to 'words'.
     */

    bitmapword    words[FLEXIBLE_ARRAY_MEMBER];
} BlocktableEntry;

/*
 * The type of 'nwords' limits the max number of words in the 'words' array.
 * This computes the max offset we can actually store in the bitmap. In
 * practice, it's almost always the same as MaxOffsetNumber.
 */
#define MAX_OFFSET_IN_BITMAP Min(BITS_PER_BITMAPWORD * PG_INT8_MAX - 1, MaxOffsetNumber)

#define MaxBlocktableEntrySize \
    offsetof(BlocktableEntry, words) + \
        (sizeof(bitmapword) * WORDS_PER_PAGE(MAX_OFFSET_IN_BITMAP))

#define RT_PREFIX local_ts
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE BlocktableEntry
#define RT_VARLEN_VALUE_SIZE(page) \
    (offsetof(BlocktableEntry, words) + \
    sizeof(bitmapword) * (page)->header.nwords)
#define RT_RUNTIME_EMBEDDABLE_VALUE
#include "lib/radixtree.h"

/* Per-backend state for a TidStore */
struct TidStore
{
    /* MemoryContext where the TidStore is allocated */
    MemoryContext context;

    /* MemoryContext that the radix tree uses */
    MemoryContext rt_context;

    /* Storage for TIDs. */
    local_ts_radix_tree *tree;

};

/* Iterator for TidStore */
struct TidStoreIter
{
    TidStore   *ts;

    /* iterator of radix tree. */
    local_ts_iter *tree_iter;

    /* output for the caller */
    TidStoreIterResult output;
};

static void tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key,
                                       BlocktableEntry *page);

/*
 * Create a TidStore. The TidStore will live in the memory context that is
 * CurrentMemoryContext at the time of this call. The TID storage, backed
 * by a radix tree, will live in its child memory context, rt_context.
 *
 * "max_bytes" is not an internally-enforced limit; it is used only as a
 * hint to cap the memory block size of the memory context for TID storage.
 * This reduces space wastage due to over-allocation. If the caller wants to
 * monitor memory usage, it must compare its limit with the value reported
 * by TidStoreMemoryUsage().
 *
 * "insert_only" is true means StackSetContext will be used for memory allocation
 * for leaf node radix tree to reduce memory bloat problem; Otherwise, AllocSetContext
 * will be used.
 */
TidStore *
TidStoreCreateLocal(size_t max_bytes, bool insert_only)
{
    TidStore   *ts;
    size_t        initBlockSize = ALLOCSET_DEFAULT_INITSIZE;
    size_t        minContextSize = ALLOCSET_DEFAULT_MINSIZE;
    size_t        maxBlockSize = ALLOCSET_DEFAULT_MAXSIZE;

    ts = (TidStore *) palloc0(sizeof(TidStore));
    ts->context = CurrentMemoryContext;

    /* choose the maxBlockSize to be no larger than 1/16 of max_bytes */
    while (16 * maxBlockSize > max_bytes)
        maxBlockSize >>= 1;

    if (maxBlockSize < ALLOCSET_DEFAULT_INITSIZE)
        maxBlockSize = ALLOCSET_DEFAULT_INITSIZE;

    /* Create a memory context for the TID storage
     *
     * STACK_CONTEXT can reduce memory bloat problem for
     * such a multi small memory allocation scene.
     */
    MemoryContextType type = insert_only ? STACK_CONTEXT : STANDARD_CONTEXT;
    ts->rt_context = AllocSetContextCreate(CurrentMemoryContext,
                                           "TID storage",
                                           minContextSize,
                                           initBlockSize,
                                           maxBlockSize,
                                           type);

    ts->tree = local_ts_create(ts->rt_context);

    return ts;
}

/*
 * Destroy a TidStore, returning all memory.
 *
 * Note that the caller must be certain that no other backend will attempt to
 * access the TidStore before calling this function.
 */
void
TidStoreDestroy(TidStore *ts)
{
    local_ts_free(ts->tree);

    MemoryContextDelete(ts->rt_context);

    pfree(ts);
}

/*
 * Create or replace an entry for the given (bktId, block) and array of offsets.
 *
 * NB: This function is designed and optimized for vacuum's heap scanning
 * phase, so has some limitations:
 *
 * - The offset numbers "offsets" must be sorted in ascending order.
 * - If the (bktId, block) number already exists, the entry will be replaced --
 *     there is no way to add or remove offsets from an entry.
 */
void
TidStoreSetBlockOffsets(TidStore *ts, int2 bktId, BlockNumber blkno,
                        OffsetNumber *offsets, int num_offsets)
{
    union
    {
        char        data[MaxBlocktableEntrySize];
        BlocktableEntry force_align_entry;
    }            data;
    BlocktableEntry *page = (BlocktableEntry *) data.data;
    bitmapword    word;
    int            wordnum;
    int            next_word_threshold;
    int            idx = 0;

    Assert(num_offsets > 0);

    /* Check if the given offset numbers are ordered */
    for (int i = 1; i < num_offsets; i++)
        Assert(offsets[i] > offsets[i - 1]);

    memset(page, 0, offsetof(BlocktableEntry, words));

    if (num_offsets <= NUM_FULL_OFFSETS)
    {
        for (int i = 0; i < num_offsets; i++)
        {
            OffsetNumber off = offsets[i];

            /* safety check to ensure we don't overrun bit array bounds */
            if (off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP)
                elog(ERROR, "tuple offset out of range: %u", off);

            page->header.full_offsets[i] = off;
        }

        page->header.nwords = 0;
    }
    else
    {
        for (wordnum = 0, next_word_threshold = BITS_PER_BITMAPWORD;
             wordnum <= WORDNUM(offsets[num_offsets - 1]);
             wordnum++, next_word_threshold += BITS_PER_BITMAPWORD)
        {
            word = 0;

            while (idx < num_offsets)
            {
                OffsetNumber off = offsets[idx];

                /* safety check to ensure we don't overrun bit array bounds */
                if (off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP)
                    elog(ERROR, "tuple offset out of range: %u", off);

                if (off >= next_word_threshold)
                    break;

                word |= ((bitmapword) 1 << BITNUM(off));
                idx++;
            }

            /* write out offset bitmap for this wordnum */
            page->words[wordnum] = word;
        }

        page->header.nwords = wordnum;
        Assert(page->header.nwords == WORDS_PER_PAGE(offsets[num_offsets - 1]));
    }

    uint64 key = COMBINE_KEY(bktId, blkno);
    local_ts_set(ts->tree, key, page);
}

/* Return true if the given (bktId, TID) is present in the TidStore */
bool
TidStoreIsMember(TidStore *ts, int2 bktId, ItemPointer tid)
{
    int            wordnum;
    int            bitnum;
    BlocktableEntry *page;
    BlockNumber blk = ItemPointerGetBlockNumber(tid);
    OffsetNumber off = ItemPointerGetOffsetNumber(tid);
    uint64 key = COMBINE_KEY(bktId, blk);

    page = local_ts_find(ts->tree, key);

    /* no entry for the blk */
    if (page == NULL)
        return false;

    if (page->header.nwords == 0)
    {
        /* we have offsets in the header */
        for (int i = 0; i < NUM_FULL_OFFSETS; i++)
        {
            if (page->header.full_offsets[i] == off)
                return true;
        }
        return false;
    }
    else
    {
        wordnum = WORDNUM(off);
        bitnum = BITNUM(off);

        /* no bitmap for the off */
        if (wordnum >= page->header.nwords)
            return false;

        return (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;
    }
}

/*
 * Prepare to iterate through a TidStore.
 *
 * The TidStoreIter struct is created in the caller's memory context, and it
 * will be freed in TidStoreEndIterate.
 *
 * The caller is responsible for locking TidStore until the iteration is
 * finished.
 */
TidStoreIter *
TidStoreBeginIterate(TidStore *ts)
{
    TidStoreIter *iter;

    iter = (TidStoreIter *) palloc0(sizeof(TidStoreIter));
    iter->ts = ts;

    /*
     * We start with an array large enough to contain at least the offsets
     * from one completely full bitmap element.
     */
    iter->output.max_offset = 2 * BITS_PER_BITMAPWORD;
    iter->output.offsets = (OffsetNumber *) palloc(sizeof(OffsetNumber) * iter->output.max_offset);

    iter->tree_iter = local_ts_begin_iterate(ts->tree);

    return iter;
}


/*
 * Scan the TidStore and return the TIDs of the next (bktId, block). The offsets in
 * each iteration result are ordered, as are the (bktId, block) numbers over all
 * iterations.
 */
TidStoreIterResult *
TidStoreIterateNext(TidStoreIter *iter)
{
    uint64        key;
    BlocktableEntry *page;

    page = local_ts_iterate_next(iter->tree_iter, &key);

    if (page == NULL)
        return NULL;

    /* Collect TIDs from the key-value pair */
    tidstore_iter_extract_tids(iter, key, page);

    return &(iter->output);
}

/*
 * Finish the iteration on TidStore.
 *
 * The caller is responsible for releasing any locks.
 */
void
TidStoreEndIterate(TidStoreIter *iter)
{
    local_ts_end_iterate(iter->tree_iter);

    pfree(iter->output.offsets);
    pfree(iter);
}

/*
 * Return the memory usage of TidStore.
 */
size_t
TidStoreMemoryUsage(TidStore *ts)
{
    return local_ts_memory_usage(ts->tree);
}

/* Extract TIDs from the given key-value pair */
static void
tidstore_iter_extract_tids(TidStoreIter *iter, uint64 key, BlocktableEntry *page)
{
    TidStoreIterResult *result = (&iter->output);
    int            wordnum;

    result->bktId = EXTRACT_BKTID_FROM_KEY(key);
    result->blkno = EXTRACT_BLKNO_FROM_KEY(key);
    result->num_offsets = 0;

    if (page->header.nwords == 0)
    {
        /* we have offsets in the header */
        for (int i = 0; i < NUM_FULL_OFFSETS; i++)
        {
            if (page->header.full_offsets[i] != InvalidOffsetNumber)
                result->offsets[result->num_offsets++] = page->header.full_offsets[i];
        }
    }
    else
    {
        for (wordnum = 0; wordnum < page->header.nwords; wordnum++)
        {
            bitmapword    w = page->words[wordnum];
            int            off = wordnum * BITS_PER_BITMAPWORD;

            /* Make sure there is enough space to add offsets */
            if ((result->num_offsets + BITS_PER_BITMAPWORD) > result->max_offset)
            {
                result->max_offset *= 2;
                result->offsets = (OffsetNumber *) repalloc(result->offsets,
                                           sizeof(OffsetNumber) * result->max_offset);
            }

            while (w != 0)
            {
                if (w & 1)
                    result->offsets[result->num_offsets++] = (OffsetNumber) off;
                off++;
                w >>= 1;
            }
        }
    }
}
