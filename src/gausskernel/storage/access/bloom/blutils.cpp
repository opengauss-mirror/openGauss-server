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
 * blutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/bloom/blutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/amapi.h"
#include "access/multi_redo_api.h"
#include "catalog/index.h"
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"

#include "access/bloom.h"

/* Signature dealing macros */
#define BITSIGNTYPE (BITS_PER_BYTE * sizeof(BloomSignatureWord))
#define GETWORD(x, i) (*((BloomSignatureWord*)(x) + (int)((i) / BITSIGNTYPE)))
#define CLRBIT(x, i) GETWORD(x, i) &= ~(0x01 << ((i) % BITSIGNTYPE))
#define SETBIT(x, i) GETWORD(x, i) |= (0x01 << ((i) % BITSIGNTYPE))
#define GETBIT(x, i) ((GETWORD(x, i) >> ((i) % BITSIGNTYPE)) & 0x01)

/* Random number seed for Bloom index */
#define BLOOM_RANDOM_NUM1       127773
#define BLOOM_RANDOM_NUM2       16807
#define BLOOM_RANDOM_NUM3       2836

/* Range of random number values */
#define BLOOM_RANDOM_RANGE1     0x7fffffff
#define BLOOM_RANDOM_RANGE2     0x7ffffffe

/* Kind of relation options for bloom index */
static THR_LOCAL relopt_kind bl_relopt_kind;

/* parse table for fillRelOptions */
static THR_LOCAL relopt_parse_elt bl_relopt_tab[INDEX_MAX_KEYS + 1];

static int32 myRand();
static void mySrand(uint32 seed);
static void InitBloomOptions(void);
static BloomOptions *makeDefaultBloomOptions(void);

/*
 * Module initialize function: initialize info about Bloom relation options.
 *
 * Note: keep this in sync with makeDefaultBloomOptions().
 */
static void InitBloomOptions(void)
{
    int         i;
    char        buf[16];
    errno_t     rc;
    bl_relopt_kind = RELOPT_KIND_BLOOM;

    /* Option for length of signature */
    add_int_reloption(bl_relopt_kind, "length",
                      "Length of signature in bits",
                      DEFAULT_BLOOM_LENGTH, 1, MAX_BLOOM_LENGTH);
    bl_relopt_tab[0].optname = "length";
    bl_relopt_tab[0].opttype = RELOPT_TYPE_INT;
    bl_relopt_tab[0].offset = offsetof(BloomOptions, bloomLength);

    /* Number of bits for each possible index column: col1, col2, ... */
    for (i = 0; i < INDEX_MAX_KEYS; i++) {
        rc = snprintf_s(buf, sizeof(buf), sizeof(buf), "col%d", i + 1);
        securec_check_ss(rc, "", "");
        add_int_reloption(bl_relopt_kind, buf,
                          "Number of bits generated for each index column",
                          DEFAULT_BLOOM_BITS, 1, MAX_BLOOM_BITS);
        bl_relopt_tab[i + 1].optname = MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), buf);
        bl_relopt_tab[i + 1].opttype = RELOPT_TYPE_INT;
        bl_relopt_tab[i + 1].offset = offsetof(BloomOptions, bitSize[0]) + sizeof(int) * i;
    }
}

/*
 * Construct a default set of Bloom options.
 */
static BloomOptions *makeDefaultBloomOptions(void)
{
    BloomOptions    *opts;
    int             i;

    opts = (BloomOptions *)palloc0(sizeof(BloomOptions));
    /* Convert DEFAULT_BLOOM_LENGTH from # of bits to # of words */
    opts->bloomLength = (DEFAULT_BLOOM_LENGTH + SIGNWORDBITS - 1) / SIGNWORDBITS;
    for (i = 0; i < INDEX_MAX_KEYS; i++)
        opts->bitSize[i] = DEFAULT_BLOOM_BITS;
    SET_VARSIZE(opts, sizeof(BloomOptions));
    return opts;
}

/*
 * Fill BloomState structure for particular index.
 */
void initBloomState(BloomState *state, Relation index)
{
    int i;
    errno_t rc;
    BloomOptions *options;

    state->nColumns = index->rd_att->natts;

    /* Initialize hash function for each attribute */
    for (i = 0; i < index->rd_att->natts; i++) {
        fmgr_info_copy(&(state->hashFn[i]),
                       index_getprocinfo(index, i + 1, BLOOM_HASH_PROC),
                       CurrentMemoryContext);
        state->collations[i] = index->rd_indcollation[i];
    }

    /* Initialize amcache if needed with options from metapage */
    if (!index->rd_amcache) {
        Buffer              buffer;
        Page                page;
        BloomMetaPageData   *meta;
        BloomOptions        *opts;

        opts = (BloomOptions *)MemoryContextAlloc(index->rd_indexcxt, sizeof(BloomOptions));
        buffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        if (!BloomPageIsMeta(page)) {
            UnlockReleaseBuffer(buffer);
            elog(ERROR, "Relation is not a bloom index");
        }

        meta = BloomPageGetMeta(BufferGetPage(buffer));
        if (meta->magickNumber != BLOOM_MAGICK_NUMBER) {
            UnlockReleaseBuffer(buffer);
            elog(ERROR, "Relation is not a bloom index");
        }

        *opts = meta->opts;
        UnlockReleaseBuffer(buffer);
        index->rd_amcache = (void *)opts;
    }

    options = (BloomOptions*)(index->rd_options ? index->rd_options: index->rd_amcache);
    rc = memcpy_s(&state->opts, sizeof(state->opts), options, sizeof(state->opts));
    securec_check(rc, "\0", "\0");

    state->sizeOfBloomTuple = BLOOMTUPLEHDRSZ + sizeof(BloomSignatureWord) * state->opts.bloomLength;
}

/*
 * Random generator copied from FreeBSD.  Using own random generator here for
 * two reasons:
 *
 * 1) In this case random numbers are used for on-disk storage.  Usage of
 *      PostgreSQL number generator would obstruct it from all possible changes.
 * 2) Changing seed of PostgreSQL random generator would be undesirable side
 *      effect.
 */
static THR_LOCAL int32 next;

static int32 myRand()
{
    /*
     * Compute x = (7^5 * x) mod (2^31 - 1)
     * without overflowing 31 bits:
     *      (2^31 - 1) = 127773 * (7^5) + 2836
     * From "Random number generators: good ones are hard to find",
     * Park and Miller, Communications of the ACM, vol. 31, no. 10,
     * October 1988, p. 1195.
     */
    int32 hi;
    int32 lo;
    int32 x;

    /* Must be in [1, 0x7ffffffe] range at this point. */
    hi = next / BLOOM_RANDOM_NUM1;
    lo = next % BLOOM_RANDOM_NUM1;
    x = BLOOM_RANDOM_NUM2 * lo - BLOOM_RANDOM_NUM3 * hi;
    if (x < 0)
        x += BLOOM_RANDOM_RANGE1;
    next = x;
    /* Transform to [0, 0x7ffffffd] range. */
    return (x - 1);
}

void mySrand(uint32 seed)
{
    next = seed;
    /* Transform to [1, 0x7ffffffe] range. */
    next = (next % BLOOM_RANDOM_RANGE2) + 1;
}

/*
 * Add bits of given value to the signature.
 */
void signValue(BloomState *state, BloomSignatureWord *sign, Datum value, int attno)
{
    uint32        hashVal;
    int           nBit;
    int           j;

    /*
     * init generator with "column's" number to get "hashed" seed for new
     * value. We don't want to map the same numbers from different columns
     * into the same bits!
     */
    mySrand(attno);

    /*
     * Init hash sequence to map our value into bits. the same values in
     * different columns will be mapped into different bits because of step
     * above
     */
    hashVal = DatumGetInt32(FunctionCall1Coll(&state->hashFn[attno], state->collations[attno], value));
    mySrand(hashVal ^ myRand());

    for (j = 0; j < state->opts.bitSize[attno]; j++) {
        /* prevent mutiple evaluation */
        nBit = myRand() % (state->opts.bloomLength * BITSIGNTYPE);
        SETBIT(sign, nBit);
    }
}

/*
 * Make bloom tuple from values.
 */
BloomTuple *BloomFormTuple(BloomState *state, ItemPointer iptr, Datum *values, const bool *isnull)
{
    int i;
    BloomTuple *res = (BloomTuple *)palloc0(state->sizeOfBloomTuple);

    res->heapPtr = *iptr;

    /* Blooming each column */
    for (i = 0; i < state->nColumns; i++) {
        /* skip nulls */
        if (isnull[i])
            continue;

        signValue(state, res->sign, values[i], i);
    }

    return res;
}

/*
 * Add new bloom tuple to the page.  Returns true if new tuple was successfully
 * added to the page.  Returns false if it doesn't git the page.
 */
bool BloomPageAddItem(BloomState *state, Page page, BloomTuple *tuple)
{
    BloomTuple *itup;
    BloomPageOpaque opaque;
    Pointer ptr;
    errno_t rc;

    /* We shouldn't be pointed to an invalid page */
    Assert(!PageIsNew(page) && !BloomPageIsDeleted(page));

    /* Does new tuple fit on the page? */
    if (BloomPageGetFreeSpace(state, page) < state->sizeOfBloomTuple) {
        return false;
    }
    /* Copy new tuple to the end of page */
    opaque = BloomPageGetOpaque(page);
    itup = BloomPageGetTuple(state, page, opaque->maxoff + 1);
    rc = memcpy_s((Pointer)itup, state->sizeOfBloomTuple, (Pointer)tuple, state->sizeOfBloomTuple);
    securec_check(rc, "\0", "\0");

    /* Adjust maxoff and pd_lower */
    opaque->maxoff++;
    ptr = (Pointer)BloomPageGetTuple(state, page, opaque->maxoff + 1);
    ((PageHeader) page)->pd_lower = ptr - page;

    /* Assert we didn't overrun available space */
    Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);

    return true;
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling BloomInitBuffer
 */
Buffer BloomNewBuffer(Relation index)
{
    Buffer      buffer;
    bool        needLock;

    /* First, try to get a page from FSM */
    for (;;) {
        BlockNumber blkno = GetFreeIndexPage(index);
        if (blkno == InvalidBlockNumber) {
            break;
        }

        buffer = ReadBuffer(index, blkno);
        /*
         * We have to guard against the possibility that someone else already
         * recycled this page; the buffer may be locked if so.
         */
        if (ConditionalLockBuffer(buffer)) {
            Page    page = BufferGetPage(buffer);
            if (PageIsNew(page)) {
                return buffer;    /* OK to use, if never initialized */
            }

            if (BloomPageIsDeleted(page)) {
                return buffer;    /* OK to use */
            }

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        }

        /* Can't use it, so release buffer and try again */
        ReleaseBuffer(buffer);
    }

    /* Must extend the file */
    needLock = !RELATION_IS_LOCAL(index);
    if (needLock)
        LockRelationForExtension(index, ExclusiveLock);

    buffer = ReadBuffer(index, P_NEW);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    if (needLock)
        UnlockRelationForExtension(index, ExclusiveLock);

    return buffer;
}

/*
 * Initialize bloom page.
 */
void BloomInitPage(Page page, uint16 flags)
{
    BloomPageOpaque opaque;

    PageInit(page, BLCKSZ, sizeof(BloomPageOpaqueData));

    opaque = BloomPageGetOpaque(page);
    errno_t rc = memset_s(opaque, sizeof(BloomPageOpaqueData), 0, sizeof(BloomPageOpaqueData));
    securec_check(rc, "\0", "\0");
    opaque->flags = flags;
    opaque->bloom_page_id = BLOOM_PAGE_ID;
}

/*
 * Fill in metapage for bloom index.
 */
void BloomFillMetapage(Relation index, Page metaPage)
{
    BloomOptions        *opts;
    BloomMetaPageData   *metadata;
    errno_t rc;

    /*
     * Choose the index's options.  If reloptions have been assigned, use
     * those, otherwise create default options.
     */
    opts = (BloomOptions *)index->rd_options;
    if (!opts)
        opts = makeDefaultBloomOptions();

    /*
     * Initialize contents of meta page, including a copy of the options,
     * which are now frozen for the life of the index.
     */
    BloomInitPage(metaPage, BLOOM_META);
    metadata = BloomPageGetMeta(metaPage);
    rc = memset_s(metadata, sizeof(BloomMetaPageData), 0, sizeof(BloomMetaPageData));
    securec_check(rc, "\0", "\0");
    metadata->magickNumber = BLOOM_MAGICK_NUMBER;
    metadata->opts = *opts;
    ((PageHeader) metaPage)->pd_lower += sizeof(BloomMetaPageData);

    /* If this fails, probably FreeBlockNumberArray size calc is wrong: */
    Assert(((PageHeader) metaPage)->pd_lower <= ((PageHeader) metaPage)->pd_upper);
}

/*
 * Initialize metapage for bloom index.
 */
void BloomInitMetapage(Relation index, ForkNumber forknum)
{
    Page        metaPage;
    Buffer      metaBuffer;
    GenericXLogState *state;
    errno_t rc;
    /*
     * Make a new page; since it is first page it should be associated with
     * block number 0 (BLOOM_METAPAGE_BLKNO).  No need to hold the extension
     * lock because there cannot be concurrent inserters yet.
     */
    metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
    Assert(BufferGetBlockNumber(metaBuffer) == BLOOM_METAPAGE_BLKNO);

    /* Initialize contents of meta page */
    state = GenericXLogStart(index);
    metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
                                         GENERIC_XLOG_FULL_IMAGE);
    BloomFillMetapage(index, metaPage);
    GenericXLogFinish(state);

    UnlockReleaseBuffer(metaBuffer);
}

/*
 * Initialize options for bloom index.
 */
bytea *bloptions_internal(Datum reloptions, bool validate)
{
    relopt_value    *options;
    int             numoptions;
    BloomOptions    *rdopts;
    int             i;
    char            buf[16];
    errno_t         rc;

    /* Option for length of signature */
    if (bl_relopt_kind != RELOPT_KIND_BLOOM) {
        InitBloomOptions();
    }

    options = parseRelOptions(reloptions, validate, bl_relopt_kind, &numoptions);
    rdopts = (BloomOptions*)allocateReloptStruct(sizeof(BloomOptions), options, numoptions);
    fillRelOptions((void *) rdopts, sizeof(BloomOptions), options, numoptions,
                   validate, bl_relopt_tab, INDEX_MAX_KEYS + 1);

    /* Convert signature length from # of bits to # to words, rounding up */
    if (rdopts)
        rdopts->bloomLength = (rdopts->bloomLength + SIGNWORDBITS - 1) / SIGNWORDBITS;

    return (bytea *) rdopts;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blbuild);
Datum blbuild(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bloom index do not support extreme rto.");
    }
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = blbuild_internal(heap, index, indexInfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blbuildempty);
Datum blbuildempty(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bloom index do not support extreme rto.");
    }
    Relation index = (Relation)PG_GETARG_POINTER(0);
    blbuildempty_internal(index);
    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blinsert);
Datum blinsert(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bloom index do not support extreme rto.");
    }
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = (bool *)(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heapRel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkUnique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = blinsert_internal(rel, values, isnull, ht_ctid, heapRel, checkUnique);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blbulkdelete);
Datum blbulkdelete(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bloom index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void *callbackState = static_cast<void *>(PG_GETARG_POINTER(3));
    stats = blbulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blvacuumcleanup);
Datum blvacuumcleanup(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "bloom index do not support extreme rto.");
    }
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    stats = blvacuumcleanup_internal(info, stats);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blcostestimate);
Datum blcostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loopCount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost* indexStartupCost = (Cost*)PG_GETARG_POINTER(3);
    Cost* indexTotalCost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* indexSelectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* indexCorrelation = (double *)(PG_GETARG_POINTER(6));
    blcostestimate_internal(root, path, loopCount, indexStartupCost,
                            indexTotalCost, indexSelectivity, indexCorrelation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(bloptions);
Datum bloptions(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);
    bytea *result = bloptions_internal(reloptions, validate);
    if (result != NULL) {
        PG_RETURN_BYTEA_P(result);
    }

    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blbeginscan);
Datum blbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = blbeginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blrescan);
Datum blrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    blrescan_internal(scan, scankey);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blendscan);
Datum blendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    blendscan_internal(scan);
    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(blgetbitmap);
Datum blgetbitmap(PG_FUNCTION_ARGS)
{
    int64 ntids = 0;
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    TIDBitmap *tbm = (TIDBitmap *)PG_GETARG_POINTER(1);
    ntids = blgetbitmap_internal(scan, tbm);
    PG_RETURN_INT64(ntids);
}
