/* -------------------------------------------------------------------------
 *
 * tidbitmap.cpp
 *	  openGauss tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 * The "lossy" storage uses one bit per disk page, so at the standard 8K
 * BLCKSZ, we can represent all pages in 64Gb of disk space in about 1Mb
 * of memory.  People pushing around tables of that size should have a
 * couple of Mb to spare, so we don't worry about providing a second level
 * of lossiness.  In theory we could fall back to page ranges at some
 * point, but for now that seems useless complexity.
 *
 * We also support the notion of candidate matches, or rechecking.	This
 * means we know that a search need visit only some tuples on a page,
 * but we are not certain that all of those tuples are real matches.
 * So the eventual heap scan must recheck the quals for these tuples only,
 * rather than rechecking the quals for all tuples on the page as in the
 * lossy-bitmap case.  Rechecking can be specified when TIDs are inserted
 * into a bitmap, and it can also happen internally when we AND a lossy
 * and a non-lossy page.
 *
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/tidbitmap.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "access/htup.h"
#include "nodes/bitmapset.h"
#include "nodes/tidbitmap.h"
#include "utils/hsearch.h"
#include "access/ustore/knl_upage.h"

/*
 * The maximum number of tuples per page is not large (typically 256 with
 * 8K pages, or 1024 with 32K pages).  So there's not much point in making
 * the per-page bitmaps variable size.	We just legislate that the size
 * is this:
 */
#define MAX_TUPLES_PER_HEAP_PAGE MaxHeapTuplesPerPage
#define MAX_TUPLES_PER_UHEAP_PAGE MaxPossibleUHeapTuplesPerPage

/*
 * When we have to switch over to lossy storage, we use a data structure
 * with one bit per page, where all pages having the same number DIV
 * PAGES_PER_CHUNK are aggregated into one chunk.  When a chunk is present
 * and has the bit set for a given page, there must not be a per-page entry
 * for that page in the page table.
 *
 * We actually store both exact pages and lossy chunks in the same hash
 * table, using identical data structures.	(This is because dynahash.c's
 * memory management doesn't allow space to be transferred easily from one
 * hashtable to another.)  Therefore it's best if PAGES_PER_CHUNK is the
 * same as MAX_TUPLES_PER_PAGE, or at least not too different.	But we
 * also want PAGES_PER_CHUNK to be a power of 2 to avoid expensive integer
 * remainder operations.  So, define it like this:
 */
#define PAGES_PER_HEAP_CHUNK (BLCKSZ / 32)
#define PAGES_PER_UHEAP_CHUNK (BLCKSZ / 16)


/* We use BITS_PER_BITMAPWORD and typedef bitmapword from nodes/bitmapset.h */

#define WORDNUM(x) ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x) ((x) % BITS_PER_BITMAPWORD)

/* number of active words for an exact page: */
#define WORDS_PER_HEAP_PAGE ((MAX_TUPLES_PER_HEAP_PAGE - 1) / BITS_PER_BITMAPWORD + 1)
#define WORDS_PER_UHEAP_PAGE ((MAX_TUPLES_PER_UHEAP_PAGE - 1) / BITS_PER_BITMAPWORD + 1)


/* number of active words for a lossy chunk: */
#define WORDS_PER_HEAP_CHUNK ((PAGES_PER_HEAP_CHUNK - 1) / BITS_PER_BITMAPWORD + 1)
#define WORDS_PER_UHEAP_CHUNK ((PAGES_PER_UHEAP_CHUNK - 1) / BITS_PER_BITMAPWORD + 1)
#define WORDS_PER_CHUNK ((PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1)
/* compare two entry node. For regular table, partitionOid is set to Invalid */
#define IS_ENTRY_NODE_MATCH(tarNode, matchNode)                                                         \
    ((tarNode).blockNo == (matchNode).blockNo && (tarNode).partitionOid == (matchNode).partitionOid &&  \
        (tarNode).bucketid == (matchNode).bucketid)

#define IS_CHUNK_BEFORE_PAGE(chunkNode, pageNode)                   \
    ((chunkNode).partitionOid < (pageNode).partitionOid             \
        ? true                                                      \
        : ((chunkNode).partitionOid > (pageNode).partitionOid       \
            ? false                                                 \
            : ((chunkNode).bucketid < (pageNode).bucketid           \
                ? true                                              \
                : ((chunkNode).bucketid > (pageNode).bucketid       \
                    ? false                                         \
                    : ((chunkNode).blockNo < (pageNode).blockNo ? true : false)))))
/*
 * Used as key of hash table for PagetableEntry.
 */
typedef struct PagetableEntryNode_s {
    BlockNumber blockNo;    /* page number (hashtable key) */
    Oid partitionOid;       /* used for GLOBAL partition index to indicate partition table */
    int2 bucketid;          /* used for cross-bucket index on hashbucket table */
    int2 padding;           /* padding to align with four bytes */
} PagetableEntryNode;
/*
 * The hashtable entries are represented by this data structure.  For
 * an exact page, blockno is the page number and bit k of the bitmap
 * represents tuple offset k+1.  For a lossy chunk, blockno is the first
 * page in the chunk (this must be a multiple of PAGES_PER_CHUNK) and
 * bit k represents page blockno+k.  Note that it is not possible to
 * have exact storage for the first page of a chunk if we are using
 * lossy storage for any page in the chunk's range, since the same
 * hashtable entry has to serve both purposes.
 *
 * recheck is used only on exact pages --- it indicates that although
 * only the stated tuples need be checked, the full index qual condition
 * must be checked for each (ie, these are candidate matches).
 */
typedef struct PagetableEntry {
    PagetableEntryNode entryNode;
    bool ischunk;        /* T = lossy storage, F = exact */
    bool recheck; /* should the tuples be rechecked? */
    bitmapword
        words[Max(Max(WORDS_PER_HEAP_PAGE, WORDS_PER_HEAP_CHUNK), Max(WORDS_PER_UHEAP_PAGE, WORDS_PER_UHEAP_CHUNK))];
} PagetableEntry;
/*
 * dynahash.c is optimized for relatively large, long-lived hash tables.
 * This is not ideal for TIDBitMap, particularly when we are using a bitmap
 * scan on the inside of a nestloop join: a bitmap may well live only long
 * enough to accumulate one entry in such cases.  We therefore avoid creating
 * an actual hashtable until we need two pagetable entries.  When just one
 * pagetable entry is needed, we store it in a fixed field of TIDBitMap.
 * (NOTE: we don't get rid of the hashtable if the bitmap later shrinks down
 * to zero or one page again.  So, status can be TBM_HASH even when nentries
 * is zero or one.)
 */
typedef enum {
    TBM_EMPTY,    /* no hashtable, nentries == 0 */
    TBM_ONE_PAGE, /* entry1 contains the single entry */
    TBM_HASH      /* pagetable is valid, entry1 is not */
} TBMStatus;

/*
 * Here is the representation for a whole TIDBitMap:
 */
struct TIDBitmap {
    NodeTag type;          /* to make it a valid Node */
    MemoryContext mcxt;    /* memory context containing me */
    TBMStatus status;      /* see codes above */
    HTAB* pagetable;       /* hash table of PagetableEntry's */
    int nentries;          /* number of entries in pagetable */
    int maxentries;        /* limit on same to meet maxbytes */
    int npages;            /* number of exact entries in pagetable */
    int nchunks;           /* number of lossy entries in pagetable */
    bool iterating;        /* tbm_begin_iterate called? */
    bool isGlobalPart;     /* represent global partition index tbm */
    bool crossbucket;      /* represent crossbucket index tbm */
    PagetableEntry entry1; /* used when status == TBM_ONE_PAGE */
    /* these are valid when iterating is true: */
    PagetableEntry** spages;  /* sorted exact-page list, or NULL */
    PagetableEntry** schunks; /* sorted lossy-chunk list, or NULL */
    bool is_ustore;
    int max_tuples_page;
    int pages_per_chunk;
    int words_per_page;
};

/*
 * When iterating over a bitmap in sorted order, a TBMIterator is used to
 * track our progress.	There can be several iterators scanning the same
 * bitmap concurrently.  Note that the bitmap becomes read-only as soon as
 * any iterator is created.
 */
struct TBMIterator {
    TIDBitmap* tbm;          /* TIDBitmap we're iterating over */
    int spageptr;            /* next spages index */
    int schunkptr;           /* next schunks index */
    int schunkbit;           /* next bit to check in current schunk */
    TBMIterateResult output; /* MUST BE LAST (because variable-size) */
};

/* Local function prototypes */
static void tbm_union_page(TIDBitmap* a, const PagetableEntry* bpage);
static bool tbm_intersect_page(TIDBitmap* a, PagetableEntry* apage, const TIDBitmap* b);
static const PagetableEntry* tbm_find_pageentry(const TIDBitmap* tbm, PagetableEntryNode pageNode);
static PagetableEntry* tbm_get_pageentry(TIDBitmap* tbm, PagetableEntryNode pageNode);
static bool tbm_page_is_lossy(const TIDBitmap* tbm, PagetableEntryNode pageNode);
static void tbm_mark_page_lossy(TIDBitmap* tbm, PagetableEntryNode pageNode);
static void tbm_lossify(TIDBitmap* tbm);
static int tbm_comparator(const void* left, const void* right);

/*
 * TbmCreate - create an initially-empty bitmap
 *
 * The bitmap will live in the memory context that is CurrentMemoryContext
 * at the time of this call.  It will be limited to (approximately) maxbytes
 * total memory consumption.
 */
TIDBitmap* TbmCreate(long maxbytes, bool is_ustore)
{
    TIDBitmap* tbm = NULL;
    long nbuckets;

    /* Create the TIDBitmap struct and zero all its fields */
    tbm = makeNode(TIDBitmap);

    tbm->mcxt = CurrentMemoryContext;
    tbm->status = TBM_EMPTY;
    tbm->isGlobalPart = false;
    /*
     * Estimate number of hashtable entries we can have within maxbytes. This
     * estimates the hash overhead at MAXALIGN(sizeof(HASHELEMENT)) plus a
     * pointer per hash entry, which is crude but good enough for our purpose.
     * Also count an extra Pointer per entry for the arrays created during
     * iteration readout.
     */
    nbuckets = maxbytes /
               (MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(sizeof(PagetableEntry)) + sizeof(Pointer) + sizeof(Pointer));
    nbuckets = Min(nbuckets, INT_MAX - 1); /* safety limit */
    nbuckets = Max(nbuckets, 16);          /* sanity limit */
    tbm->maxentries = (int)nbuckets;
    tbm->is_ustore = is_ustore;

    if (is_ustore) {
        tbm->max_tuples_page = MAX_TUPLES_PER_UHEAP_PAGE;
        tbm->pages_per_chunk = PAGES_PER_UHEAP_CHUNK;
        tbm->words_per_page = WORDS_PER_UHEAP_PAGE;
    } else {
        tbm->max_tuples_page = MAX_TUPLES_PER_HEAP_PAGE;
        tbm->pages_per_chunk = PAGES_PER_HEAP_CHUNK;
        tbm->words_per_page = WORDS_PER_HEAP_PAGE;
    }

    return tbm;
}

/*
 * Actually create the hashtable.  Since this is a moderately expensive
 * proposition, we don't do it until we have to.
 */
static void tbm_create_pagetable(TIDBitmap* tbm)
{
    HASHCTL hash_ctl;
    int rc = 0;

    Assert(tbm->status != TBM_HASH);
    Assert(tbm->pagetable == NULL);

    /* Create the hashtable proper */
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");
    hash_ctl.keysize = sizeof(PagetableEntryNode);
    hash_ctl.entrysize = sizeof(PagetableEntry);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = tbm->mcxt;
    tbm->pagetable = hash_create("TIDBitmap",
        128, /* start small and extend */
        &hash_ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* If entry1 is valid, push it into the hashtable */
    if (tbm->status == TBM_ONE_PAGE) {
        PagetableEntry* page = NULL;
        bool found = false;

        page = (PagetableEntry*)hash_search(tbm->pagetable, (void*)&tbm->entry1.entryNode, HASH_ENTER, &found);
        Assert(!found);
        errno_t rc = memcpy_s(page, sizeof(PagetableEntry), &tbm->entry1, sizeof(PagetableEntry));
        securec_check(rc, "\0", "\0");
    }

    tbm->status = TBM_HASH;
}

/*
 * tbm_free - free a TIDBitmap
 */
void tbm_free(TIDBitmap* tbm)
{
    if (tbm->pagetable != NULL) {
        hash_destroy(tbm->pagetable);
    }
    if (tbm->spages != NULL) {
        pfree_ext(tbm->spages);
    }
    if (tbm->schunks != NULL) {
        pfree_ext(tbm->schunks);
    }
    pfree_ext(tbm);
}

/*
 * tbm_add_tuples - add some tuple IDs to a TIDBitmap
 *
 * If recheck is true, then the recheck flag will be set in the
 * TBMIterateResult when any of these tuples are reported out.
 */
void tbm_add_tuples(TIDBitmap* tbm, const ItemPointer tids, int ntids, bool recheck, Oid partitionOid, int2 bucketid)
{
    int i;

    Assert(!tbm->iterating);
    for (i = 0; i < ntids; i++) {
        BlockNumber blk = ItemPointerGetBlockNumber(tids + i);
        OffsetNumber off = ItemPointerGetOffsetNumber(tids + i);
        PagetableEntry* page = NULL;
        PagetableEntryNode pageNode = {blk, partitionOid, bucketid};
        int wordnum, bitnum;

        /* safety check to ensure we don't overrun bit array bounds */
        if (off < 1 || off > tbm->max_tuples_page) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmodule(MOD_EXECUTOR),
                    errmsg("tuple offset out of range: %u", off)));
        }

        if (tbm_page_is_lossy(tbm, pageNode)) {
            continue; /* whole page is already marked */
        }

        page = tbm_get_pageentry(tbm, pageNode);

        if (page->ischunk) {
            /* The page is a lossy chunk header, set bit for itself */
            wordnum = bitnum = 0;
        } else {
            /* Page is exact, so set bit for individual tuple */
            wordnum = WORDNUM(off - 1);
            bitnum = BITNUM(off - 1);
        }
        page->words[wordnum] |= ((bitmapword)1 << (unsigned int)bitnum);
        page->recheck |= recheck;

        if (tbm->nentries > tbm->maxentries) {
            tbm_lossify(tbm);
        }
    }
}

/*
 * tbm_add_page - add a whole page to a TIDBitmap
 *
 * This causes the whole page to be reported (with the recheck flag)
 * when the TIDBitmap is scanned.
 */
void tbm_add_page(TIDBitmap* tbm, BlockNumber pageno, Oid partitionOid, int2 bucketid)
{
    PagetableEntryNode pnode = {pageno, partitionOid, bucketid};
    /* Enter the page in the bitmap, or mark it lossy if already present */
    tbm_mark_page_lossy(tbm, pnode);
    /* If we went over the memory limit, lossify some more pages */
    if (tbm->nentries > tbm->maxentries) {
        tbm_lossify(tbm);
    }
}

/*
 * tbm_union - set union
 *
 * a is modified in-place, b is not changed
 */
void tbm_union(TIDBitmap* a, const TIDBitmap* b)
{
    Assert(!a->iterating);
    /* Nothing to do if b is empty */
    if (b->nentries == 0) {
        return;
    }
    /* Scan through chunks and pages in b, merge into a */
    if (b->status == TBM_ONE_PAGE) {
        tbm_union_page(a, &b->entry1);
    } else {
        HASH_SEQ_STATUS status;
        PagetableEntry* bpage = NULL;

        Assert(b->status == TBM_HASH);
        hash_seq_init(&status, b->pagetable);
        while ((bpage = (PagetableEntry*)hash_seq_search(&status)) != NULL) {
            tbm_union_page(a, bpage);
        }
    }
}

/* Process one page of b during a union op */
static void tbm_union_page(TIDBitmap* a, const PagetableEntry* bpage)
{
    PagetableEntry* apage = NULL;
    int wordnum;

    if (bpage->ischunk) {
        /* Scan b's chunk, mark each indicated page lossy in a */
        for (wordnum = 0; wordnum < a->words_per_page; wordnum++) {
            bitmapword w = bpage->words[wordnum];

            if (w != 0) {
                BlockNumber pg;

                pg = bpage->entryNode.blockNo + (wordnum * BITS_PER_BITMAPWORD);
                while (w != 0) {
                    if (w & 1) {
                        PagetableEntryNode unionNode = {pg, bpage->entryNode.partitionOid, bpage->entryNode.bucketid};
                        tbm_mark_page_lossy(a, unionNode);
                    }
                    pg++;
                    w >>= 1;
                }
            }
        }
    } else if (tbm_page_is_lossy(a, bpage->entryNode)) {
        /* page is already lossy in a, nothing to do */
        return;
    } else {
        apage = tbm_get_pageentry(a, bpage->entryNode);
        if (apage->ischunk) {
            /* The page is a lossy chunk header, set bit for itself */
            apage->words[0] |= ((bitmapword)1 << 0);
        } else {
            /* Both pages are exact, merge at the bit level */
            for (wordnum = 0; wordnum < a->words_per_page; wordnum++) {
                apage->words[wordnum] |= bpage->words[wordnum];
            }
            apage->recheck = apage->recheck || bpage->recheck;
        }
    }

    if (a->nentries > a->maxentries) {
        tbm_lossify(a);
    }
}

/*
 * tbm_intersect - set intersection
 *
 * a is modified in-place, b is not changed
 */
void tbm_intersect(TIDBitmap* a, const TIDBitmap* b)
{
    Assert(!a->iterating);
    /* Nothing to do if a is empty */
    if (a->nentries == 0) {
        return;
    }
    /* Scan through chunks and pages in a, try to match to b */
    if (a->status == TBM_ONE_PAGE) {
        if (tbm_intersect_page(a, &a->entry1, b)) {
            /* Page is now empty, remove it from a */
            Assert(!a->entry1.ischunk);
            a->npages--;
            a->nentries--;
            Assert(a->nentries == 0);
            a->status = TBM_EMPTY;
        }
    } else {
        HASH_SEQ_STATUS status;
        PagetableEntry* apage = NULL;

        Assert(a->status == TBM_HASH);
        hash_seq_init(&status, a->pagetable);
        while ((apage = (PagetableEntry*)hash_seq_search(&status)) != NULL) {
            if (tbm_intersect_page(a, apage, b)) {
                /* Page or chunk is now empty, remove it from a */
                if (apage->ischunk) {
                    a->nchunks--;
                } else {
                    a->npages--;
                }
                a->nentries--;
                if (hash_search(a->pagetable, (void*)&apage->entryNode, HASH_REMOVE, NULL) == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED), errmodule(MOD_EXECUTOR), errmsg("hash table corrupted")));
               }
            }
        }
    }
}

/*
 * Process one page of a during an intersection op
 *
 * Returns TRUE if apage is now empty and should be deleted from a
 */
static bool tbm_intersect_page(TIDBitmap* a, PagetableEntry* apage, const TIDBitmap* b)
{
    const PagetableEntry* bpage = NULL;
    int wordnum;

    Assert(a->words_per_page == b->words_per_page);

    if (apage->ischunk) {
        /* Scan each bit in chunk, try to clear */
        bool candelete = true;

        for (wordnum = 0; wordnum < a->words_per_page; wordnum++) {
            bitmapword w = apage->words[wordnum];

            if (w != 0) {
                bitmapword neww = w;
                BlockNumber pg;
                int bitnum;

                pg = apage->entryNode.blockNo + (wordnum * BITS_PER_BITMAPWORD);
                bitnum = 0;
                while (w != 0) {
                    if (w & 1) {
                        PagetableEntryNode pNode = {pg, apage->entryNode.partitionOid, apage->entryNode.bucketid};
                        if (!tbm_page_is_lossy(b, pNode) && tbm_find_pageentry(b, pNode) == NULL) {
                            /* Page is not in b at all, lose lossy bit */
                            neww &= ~((bitmapword)1 << (unsigned int)bitnum);
                        }
                    }
                    pg++;
                    bitnum++;
                    w >>= 1;
                }
                apage->words[wordnum] = neww;
                if (neww != 0) {
                    candelete = false;
                }
            }
        }
        return candelete;
    } else if (tbm_page_is_lossy(b, apage->entryNode)) {
        /*
         * Some of the tuples in 'a' might not satisfy the quals for 'b', but
         * because the page 'b' is lossy, we don't know which ones. Therefore
         * we mark 'a' as requiring rechecks, to indicate that at most those
         * tuples set in 'a' are matches.
         */
        apage->recheck = true;
        return false;
    } else {
        bool candelete = true;

        bpage = tbm_find_pageentry(b, apage->entryNode);
        if (bpage != NULL) {
            /* Both pages are exact, merge at the bit level */
            Assert(!bpage->ischunk);
            for (wordnum = 0; wordnum < a->words_per_page; wordnum++) {
                apage->words[wordnum] &= bpage->words[wordnum];
                if (apage->words[wordnum] != 0) {
                    candelete = false;
                }
            }
            apage->recheck = apage->recheck || bpage->recheck;
        }
        /* If there is no matching b page, we can just delete the a page */
        return candelete;
    }
}

/*
 * tbm_is_empty - is a TIDBitmap completely empty?
 */
bool tbm_is_empty(const TIDBitmap* tbm)
{
    return (tbm->nentries == 0);
}

/*
 * tbm_begin_iterate - prepare to iterate through a TIDBitmap
 *
 * The TBMIterator struct is created in the caller's memory context.
 * For a clean shutdown of the iteration, call tbm_end_iterate; but it's
 * okay to just allow the memory context to be released, too.  It is caller's
 * responsibility not to touch the TBMIterator anymore once the TIDBitmap
 * is freed.
 *
 * NB: after this is called, it is no longer allowed to modify the contents
 * of the bitmap.  However, you can call this multiple times to scan the
 * contents repeatedly, including parallel scans.
 */
TBMIterator* tbm_begin_iterate(TIDBitmap* tbm)
{
    TBMIterator* iterator = NULL;

    /*
     * Create the TBMIterator struct, with enough trailing space to serve the
     * needs of the TBMIterateResult sub-struct.
     */
    iterator = (TBMIterator*)palloc(sizeof(TBMIterator) + tbm->max_tuples_page * sizeof(OffsetNumber));
    iterator->tbm = tbm;

    /*
     * Initialize iteration pointers.
     */
    iterator->spageptr = 0;
    iterator->schunkptr = 0;
    iterator->schunkbit = 0;

    /*
     * If we have a hashtable, create and fill the sorted page lists, unless
     * we already did that for a previous iterator.  Note that the lists are
     * attached to the bitmap not the iterator, so they can be used by more
     * than one iterator.
     */
    if (tbm->status == TBM_HASH && !tbm->iterating) {
        HASH_SEQ_STATUS status;
        PagetableEntry* page = NULL;
        int npages;
        int nchunks;

        if (tbm->spages == NULL && tbm->npages > 0) {
            tbm->spages = (PagetableEntry**)MemoryContextAlloc(tbm->mcxt, tbm->npages * sizeof(PagetableEntry*));
        }
        if ((tbm->schunks == NULL) && tbm->nchunks > 0) {
            tbm->schunks = (PagetableEntry**)MemoryContextAlloc(tbm->mcxt, tbm->nchunks * sizeof(PagetableEntry*));
        }
        
        hash_seq_init(&status, tbm->pagetable);
        npages = nchunks = 0;
        while ((page = (PagetableEntry*)hash_seq_search(&status)) != NULL) {
            if (page->ischunk) {
                tbm->schunks[nchunks++] = page;
            } else {
                tbm->spages[npages++] = page;
            }
        }
        Assert(npages == tbm->npages);
        Assert(nchunks == tbm->nchunks);
        if (npages > 1) {
            qsort(tbm->spages, npages, sizeof(PagetableEntry*), tbm_comparator);
        }
        if (nchunks > 1) {
            qsort(tbm->schunks, nchunks, sizeof(PagetableEntry*), tbm_comparator);
        }
    }

    tbm->iterating = true;

    return iterator;
}

/*
 * tbm_iterate - scan through next page of a TIDBitmap
 *
 * Returns a TBMIterateResult representing one page, or NULL if there are
 * no more pages to scan.  Pages are guaranteed to be delivered in numerical
 * order.  If result->ntuples < 0, then the bitmap is "lossy" and failed to
 * remember the exact tuples to look at on this page --- the caller must
 * examine all tuples on the page and check if they meet the intended
 * condition.  If result->recheck is true, only the indicated tuples need
 * be examined, but the condition must be rechecked anyway.  (For ease of
 * testing, recheck is always set true when ntuples < 0.)
 */
TBMIterateResult* tbm_iterate(TBMIterator* iterator)
{
    TIDBitmap* tbm = iterator->tbm;
    TBMIterateResult* output = &(iterator->output);

    Assert(tbm->iterating);

    /*
     * If lossy chunk pages remain, make sure we've advanced schunkptr/
     * schunkbit to the next set bit.
     */
    while (iterator->schunkptr < tbm->nchunks) {
        PagetableEntry* chunk = tbm->schunks[iterator->schunkptr];
        int schunkbit = iterator->schunkbit;

        while (schunkbit < tbm->pages_per_chunk) {
            int wordnum = WORDNUM(schunkbit);
            int bitnum = BITNUM(schunkbit);

            if ((chunk->words[wordnum] & ((bitmapword)1 << (unsigned int)bitnum)) != 0) {
                break;
            }
            schunkbit++;
        }
        if (schunkbit < tbm->pages_per_chunk) {
            iterator->schunkbit = schunkbit;
            break;
        }
        /* advance to next chunk */
        iterator->schunkptr++;
        iterator->schunkbit = 0;
    }

    /*
     * If both chunk and per-page data remain, must output the numerically
     * earlier page.
     */
    if (iterator->schunkptr < tbm->nchunks) {
        PagetableEntry* chunk = tbm->schunks[iterator->schunkptr];
        PagetableEntryNode pnode;
        pnode.blockNo = chunk->entryNode.blockNo + iterator->schunkbit;
        pnode.partitionOid = chunk->entryNode.partitionOid;
        pnode.bucketid = chunk->entryNode.bucketid;
        pnode.padding = chunk->entryNode.padding;
        if (iterator->spageptr >= tbm->npages ||
            IS_CHUNK_BEFORE_PAGE(pnode, tbm->spages[iterator->spageptr]->entryNode)) {
            /* Return a lossy page indicator from the chunk */
            output->blockno = pnode.blockNo;
            output->partitionOid = pnode.partitionOid;
            output->bucketid = pnode.bucketid;
            output->ntuples = -1;
            output->recheck = true;
            iterator->schunkbit++;
            return output;
        }
    }

    if (iterator->spageptr < tbm->npages) {
        PagetableEntry* page = NULL;
        int ntuples;
        int wordnum;

        /* In ONE_PAGE state, we don't allocate an spages[] array */
        if (tbm->status == TBM_ONE_PAGE) {
            page = &tbm->entry1;
        } else {
            page = tbm->spages[iterator->spageptr];
        }

        /* scan bitmap to extract individual offset numbers */
        ntuples = 0;
        for (wordnum = 0; wordnum < tbm->words_per_page; wordnum++) {
            bitmapword w = page->words[wordnum];

            if (w != 0) {
                int off = wordnum * BITS_PER_BITMAPWORD + 1;

                while (w != 0) {
                    if (w & 1) {
                        output->offsets[ntuples++] = (OffsetNumber)off;
                    }
                    off++;
                    w >>= 1;
                }
            }
        }
        output->blockno = page->entryNode.blockNo;
        output->partitionOid = page->entryNode.partitionOid;
        output->bucketid = page->entryNode.bucketid;
        output->ntuples = ntuples;
        output->recheck = page->recheck;
        iterator->spageptr++;
        return output;
    }

    /* Nothing more in the bitmap */
    return NULL;
}

/*
 * tbm_end_iterate - finish an iteration over a TIDBitmap
 *
 * Currently this is just a pfree, but it might do more someday.  (For
 * instance, it could be useful to count open iterators and allow the
 * bitmap to return to read/write status when there are no more iterators.)
 */
void tbm_end_iterate(TBMIterator* iterator)
{
    pfree_ext(iterator);
}

/*
 * tbm_find_pageentry - find a PagetableEntry for the pageno
 *
 * Returns NULL if there is no non-lossy entry for the pageno.
 */
static const PagetableEntry* tbm_find_pageentry(const TIDBitmap* tbm, PagetableEntryNode pageNode)
{
    const PagetableEntry* page = NULL;

    if (tbm->nentries == 0) { /* in case pagetable doesn't exist */
        return NULL;
    }
    
    if (tbm->status == TBM_ONE_PAGE) {
        page = &tbm->entry1;
        if (!IS_ENTRY_NODE_MATCH(page->entryNode, pageNode)) {
            return NULL;
        }
        Assert(!page->ischunk);
        return page;
    }

    page = (PagetableEntry*)hash_search(tbm->pagetable, (void*)&pageNode, HASH_FIND, NULL);
    if (page == NULL) {
        return NULL;
    }
    if (page->ischunk) {
        return NULL; /* don't want a lossy chunk header */
    }
    return page;
}

/*
 * tbm_get_pageentry - find or create a PagetableEntry for the pageno
 *
 * If new, the entry is marked as an exact (non-chunk) entry.
 *
 * This may cause the table to exceed the desired memory size.	It is
 * up to the caller to call tbm_lossify() at the next safe point if so.
 */
static PagetableEntry* tbm_get_pageentry(TIDBitmap* tbm, PagetableEntryNode pageNode)
{
    PagetableEntry* page = NULL;
    bool found = false;
    int rc  = 0;

    if (tbm->status == TBM_EMPTY) {
        /* Use the fixed slot */
        page = &tbm->entry1;
        found = false;
        tbm->status = TBM_ONE_PAGE;
    } else {
        if (tbm->status == TBM_ONE_PAGE) {
            page = &tbm->entry1;
            if (IS_ENTRY_NODE_MATCH(page->entryNode, pageNode)) {
                return page;
            }
            /* Time to switch from one page to a hashtable */
            tbm_create_pagetable(tbm);
        }

        /* Look up or create an entry */
        page = (PagetableEntry*)hash_search(tbm->pagetable, (void*)&pageNode, HASH_ENTER, &found);
    }

    /* Initialize it if not present before */
    if (!found) {
        rc = memset_s(page, sizeof(PagetableEntry), 0, sizeof(PagetableEntry));
        securec_check(rc, "", "");
        page->entryNode.blockNo = pageNode.blockNo;
        page->entryNode.partitionOid = pageNode.partitionOid;
        page->entryNode.bucketid = pageNode.bucketid;
        /* must count it too */
        tbm->nentries++;
        tbm->npages++;
    }

    return page;
}

/*
 * tbm_page_is_lossy - is the page marked as lossily stored?
 */
static bool tbm_page_is_lossy(const TIDBitmap* tbm, PagetableEntryNode pageNode)
{
    PagetableEntry* page = NULL;
    BlockNumber chunkPageNo;
    int bitno;

    /* we can skip the lookup if there are no lossy chunks */
    if (tbm->nchunks == 0) {
        return false;
    }
    Assert(tbm->status == TBM_HASH);

    bitno = pageNode.blockNo % tbm->pages_per_chunk;
    chunkPageNo = pageNode.blockNo - bitno;
    PagetableEntryNode chunkNode = {chunkPageNo, pageNode.partitionOid, pageNode.bucketid};
    page = (PagetableEntry*)hash_search(tbm->pagetable, (void*)&chunkNode, HASH_FIND, NULL);
    if (page != NULL && page->ischunk) {
        int wordnum = WORDNUM(bitno);
        int bitnum = BITNUM(bitno);

        if ((page->words[wordnum] & ((bitmapword)1 << (unsigned int)bitnum)) != 0) {
            return true;
        }
    }
    return false;
}

/*
 * tbm_mark_page_lossy - mark the page number as lossily stored
 *
 * This may cause the table to exceed the desired memory size.	It is
 * up to the caller to call tbm_lossify() at the next safe point if so.
 */
static void tbm_mark_page_lossy(TIDBitmap* tbm, PagetableEntryNode pageNode)
{
    PagetableEntry* page = NULL;
    bool found = false;
    BlockNumber chunkPageNo;
    int bitno;
    int wordnum;
    int bitnum;
    int rc  = 0;

    /* We force the bitmap into hashtable mode whenever it's lossy */
    if (tbm->status != TBM_HASH) {
        tbm_create_pagetable(tbm);
    }

    bitno = pageNode.blockNo % tbm->pages_per_chunk;
    chunkPageNo = pageNode.blockNo - bitno;
    PagetableEntryNode chunkNode = {chunkPageNo, pageNode.partitionOid, pageNode.bucketid};
    /*
     * Remove any extant non-lossy entry for the page.	If the page is its own
     * chunk header, however, we skip this and handle the case below.
     */
    if (bitno != 0) {
        if (hash_search(tbm->pagetable, (void*)&pageNode, HASH_REMOVE, NULL) != NULL) {
            /* It was present, so adjust counts */
            tbm->nentries--;
            tbm->npages--; /* assume it must have been non-lossy */
        }
    }

    /* Look up or create entry for chunk-header page */
    page = (PagetableEntry*)hash_search(tbm->pagetable, (void*)&chunkNode, HASH_ENTER, &found);

    /* Initialize it if not present before */
    if (!found) {
        rc = memset_s(page, sizeof(PagetableEntry), 0, sizeof(PagetableEntry));
        securec_check(rc, "", "");
        page->entryNode = chunkNode;
        page->ischunk = true;
        /* must count it too */
        tbm->nentries++;
        tbm->nchunks++;
    } else if (!page->ischunk) {
        /* chunk header page was formerly non-lossy, make it lossy */
        rc = memset_s(page, sizeof(PagetableEntry), 0, sizeof(PagetableEntry));
        securec_check(rc, "", "");
        page->entryNode = chunkNode;
        page->ischunk = true;
        /* we assume it had some tuple bit(s) set, so mark it lossy */
        page->words[0] = ((bitmapword)1 << 0);
        /* adjust counts */
        tbm->nchunks++;
        tbm->npages--;
    }

    /* Now set the original target page's bit */
    wordnum = WORDNUM(bitno);
    bitnum = BITNUM(bitno);
    page->words[wordnum] |= ((bitmapword)1 << bitnum);
}

/*
 * tbm_lossify - lose some information to get back under the memory limit
 */
static void tbm_lossify(TIDBitmap* tbm)
{
    HASH_SEQ_STATUS status;
    PagetableEntry* page = NULL;

    /*
     * XXX Really stupid implementation: this just lossifies pages in
     * essentially random order.  We should be paying some attention to the
     * number of bits set in each page, instead.
     *
     * Since we are called as soon as nentries exceeds maxentries, we should
     * push nentries down to significantly less than maxentries, or else we'll
     * just end up doing this again very soon.	We shoot for maxentries/2.
     */
    Assert(!tbm->iterating);
    Assert(tbm->status == TBM_HASH);

    hash_seq_init(&status, tbm->pagetable);
    while ((page = (PagetableEntry*)hash_seq_search(&status)) != NULL) {
        if (page->ischunk) {
            continue; /* already a chunk header */
        }
        /*
         * If the page would become a chunk header, we won't save anything by
         * converting it to lossy, so skip it.
         */
        if ((page->entryNode.blockNo % tbm->pages_per_chunk) == 0) {
            continue;
        }
        
        /* This does the dirty work ... */
        tbm_mark_page_lossy(tbm, page->entryNode);

        if (tbm->nentries <= tbm->maxentries / 2) {
            /* we have done enough */
            hash_seq_term(&status);
            break;
        }

        /*
         * Note: tbm_mark_page_lossy may have inserted a lossy chunk into the
         * hashtable.  We can continue the same seq_search scan since we do
         * not care whether we visit lossy chunks or not.
         */
    }

    /*
     * With a big bitmap and small work_mem, it's possible that we cannot get
     * under maxentries.  Again, if that happens, we'd end up uselessly
     * calling tbm_lossify over and over.  To prevent this from becoming a
     * performance sink, force maxentries up to at least double the current
     * number of entries.  (In essence, we're admitting inability to fit
     * within work_mem when we do this.)  Note that this test will not fire if
     * we broke out of the loop early; and if we didn't, the current number of
     * entries is simply not reducible any further.
     */
    if (tbm->nentries > tbm->maxentries / 2) {
        tbm->maxentries = Min(tbm->nentries, (INT_MAX - 1) / 2) * 2;
    }
}

/*
 * qsort comparator to handle PagetableEntry pointers.
 */
static int tbm_comparator(const void* left, const void* right)
{
    PagetableEntryNode l = (*((PagetableEntry* const*)left))->entryNode;
    PagetableEntryNode r = (*((PagetableEntry* const*)right))->entryNode;

    if (l.partitionOid < r.partitionOid) {
        return -1;
    } else if (l.partitionOid > r.partitionOid) {
        return 1;
    } else if (l.bucketid < r.bucketid) {
        return -1;
    } else if (l.bucketid >  r.bucketid) {
        return 1;
    } else if (l.blockNo < r.blockNo) {
        return -1;
    } else if (l.blockNo > r.blockNo) {
        return 1;
    }
    return 0;
}

bool tbm_is_global(const TIDBitmap* tbm)
{
    return tbm->isGlobalPart;
}

void tbm_set_global(TIDBitmap* tbm, bool isGlobal)
{
    tbm->isGlobalPart = isGlobal;
}

bool tbm_is_crossbucket(const TIDBitmap* tbm)
{
    return tbm->crossbucket;
}

void tbm_set_crossbucket(TIDBitmap* tbm, bool crossbucket)
{
    tbm->crossbucket = crossbucket;
}
