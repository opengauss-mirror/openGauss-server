/* -------------------------------------------------------------------------
 *
 * gistutil.cpp
 *	  utilities routines for the openGauss GiST index access method.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/gist/gistutil.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include <limits>

#include "access/gist_private.h"
#include "access/reloptions.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"

/*
 * Write itup vector to page, has no control of free space.
 */
void gistfillbuffer(Page page, IndexTuple *itup, int len, OffsetNumber off)
{
    OffsetNumber l = InvalidOffsetNumber;
    int i;

    if (off == InvalidOffsetNumber)
        off = (PageIsEmpty(page)) ? FirstOffsetNumber : OffsetNumberNext(PageGetMaxOffsetNumber(page));

    for (i = 0; i < len; i++) {
        Size sz = IndexTupleSize(itup[i]);

        l = PageAddItem(page, (Item)itup[i], sz, off, false, false);
        if (l == InvalidOffsetNumber)
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("failed to add item to GiST index page, item %d out of %d, size %d bytes", i, len,
                                   (int)sz)));
        off++;
    }
}

/*
 * Check space for itup vector on page
 */
bool gistnospace(Page page, IndexTuple *itvec, int len, OffsetNumber todelete, Size freespace)
{
    unsigned int size = freespace;
    unsigned int deleted = 0;
    int i;

    for (i = 0; i < len; i++)
        size += IndexTupleSize(itvec[i]) + sizeof(ItemIdData);

    if (todelete != InvalidOffsetNumber) {
        IndexTuple itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, todelete));

        deleted = IndexTupleSize(itup) + sizeof(ItemIdData);
    }

    return (PageGetFreeSpace(page) + deleted < size);
}

bool gistfitpage(IndexTuple *itvec, int len)
{
    int i;
    Size size = 0;

    for (i = 0; i < len; i++)
        size += IndexTupleSize(itvec[i]) + sizeof(ItemIdData);

    return (size <= GiSTPageSize);
}

/*
 * Read buffer into itup vector
 */
IndexTuple *gistextractpage(Page page, int *len /* out */)
{
    OffsetNumber i, maxoff;
    IndexTuple *itvec = NULL;

    maxoff = PageGetMaxOffsetNumber(page);
    *len = maxoff;
    itvec = (IndexTupleData **)palloc(sizeof(IndexTuple) * maxoff);
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
        itvec[i - FirstOffsetNumber] = (IndexTuple)PageGetItem(page, PageGetItemId(page, i));

    return itvec;
}

/*
 * join two vectors into one
 */
IndexTuple *gistjoinvector(IndexTuple *itvec, int *len, IndexTuple *additvec, int addlen)
{
    errno_t ret = EOK;

    itvec = (IndexTuple *)repalloc((void *)itvec, sizeof(IndexTuple) * ((*len) + addlen));
    ret = memmove_s(&itvec[*len], sizeof(IndexTuple) * addlen, additvec, sizeof(IndexTuple) * addlen);
    securec_check(ret, "", "");
    *len += addlen;
    return itvec;
}

/*
 * make plain IndexTupleVector
 */
IndexTupleData *gistfillitupvec(IndexTuple *vec, int veclen, int *memlen)
{
    char *ptr = NULL;
    char *ret = NULL;
    int i;
    errno_t rc = EOK;

    *memlen = 0;

    for (i = 0; i < veclen; i++)
        *memlen += IndexTupleSize(vec[i]);

    ptr = ret = (char *)palloc(*memlen);

    for (i = 0; i < veclen; i++) {
        rc = memcpy_s(ptr, *memlen, vec[i], IndexTupleSize(vec[i]));
        securec_check(rc, "", "");
        ptr += IndexTupleSize(vec[i]);
    }

    return (IndexTupleData *)ret;
}

/*
 * Make unions of keys in IndexTuple vector (one union datum per index column).
 * Union Datums are returned into the attr/isnull arrays.
 * Resulting Datums aren't compressed.
 */
void gistMakeUnionItVec(GISTSTATE *giststate, IndexTuple *itvec, int len, Datum *attr, bool *isnull)
{
    int i;
    GistEntryVector *evec = NULL;
    int attrsize;
    evec = (GistEntryVector *)palloc((len + 2) * sizeof(GISTENTRY) + GEVHDRSZ);

    for (i = 0; i < giststate->tupdesc->natts; i++) {
        int j;

        /* Collect non-null datums for this column */
        evec->n = 0;
        for (j = 0; j < len; j++) {
            Datum datum;
            bool IsNull = false;

            datum = index_getattr(itvec[j], i + 1, giststate->tupdesc, &IsNull);
            if (IsNull)
                continue;

            gistdentryinit(giststate, i, evec->vector + evec->n, datum, NULL, NULL, (OffsetNumber)0, FALSE, IsNull);
            evec->n++;
        }

        /* If this column was all NULLs, the union is NULL */
        if (evec->n == 0) {
            attr[i] = (Datum)0;
            isnull[i] = TRUE;
        } else {
            if (evec->n == 1) {
                /* unionFn may expect at least two inputs */
                evec->n = 2;
                evec->vector[1] = evec->vector[0];
            }

            /* Make union and store in attr array */
            attr[i] = FunctionCall2Coll(&giststate->unionFn[i], giststate->supportCollation[i], PointerGetDatum(evec),
                                        PointerGetDatum(&attrsize));

            isnull[i] = FALSE;
        }
    }
}

/*
 * Return an IndexTuple containing the result of applying the "union"
 * method to the specified IndexTuple vector.
 */
IndexTuple gistunion(Relation r, IndexTuple *itvec, int len, GISTSTATE *giststate)
{
    Datum attr[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    gistMakeUnionItVec(giststate, itvec, len, attr, isnull);

    return gistFormTuple(giststate, r, attr, isnull, false);
}

/*
 * makes union of two key
 */
#pragma GCC diagnostic push
void gistMakeUnionKey(GISTSTATE *giststate, int attno, GISTENTRY *entry1, bool isnull1, GISTENTRY *entry2, bool isnull2,
                      Datum *dst, bool *dstisnull)
{
    /* we need a GistEntryVector with room for exactly 2 elements */
    union {
        GistEntryVector gev;
        char padding[2 * sizeof(GISTENTRY) + GEVHDRSZ];
    } storage;
    GistEntryVector *evec = &storage.gev;
    int dstsize;

    evec->n = 2;

    if (isnull1 && isnull2) {
        *dstisnull = TRUE;
        *dst = (Datum)0;
    } else {
        if (isnull1 == FALSE && isnull2 == FALSE) {
            evec->vector[0] = *entry1;
            evec->vector[1] = *entry2;
        } else if (isnull1 == FALSE) {
            evec->vector[0] = *entry1;
            evec->vector[1] = *entry1;
        } else {
            evec->vector[0] = *entry2;
            evec->vector[1] = *entry2;
        }

        *dstisnull = FALSE;
        *dst = FunctionCall2Coll(&giststate->unionFn[attno], giststate->supportCollation[attno], PointerGetDatum(evec),
                                 PointerGetDatum(&dstsize));
    }
}
#pragma GCC diagnostic pop
bool gistKeyIsEQ(GISTSTATE *giststate, int attno, Datum a, Datum b)
{
    bool result = false;

    FunctionCall3Coll(&giststate->equalFn[attno], giststate->supportCollation[attno], a, b, PointerGetDatum(&result));
    return result;
}

/*
 * Decompress all keys in tuple
 */
void gistDeCompressAtt(GISTSTATE *giststate, Relation r, IndexTuple tuple, Page p, OffsetNumber o, GISTENTRY *attdata,
                       bool *isnull)
{
    int i;

    if (r->rd_att->natts >= INDEX_MAX_KEYS)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to decompress keys in tuple")));

    for (i = 0; i < r->rd_att->natts; i++) {
        Datum datum = index_getattr(tuple, i + 1, giststate->tupdesc, &isnull[i]);

        gistdentryinit(giststate, i, &attdata[i], datum, r, p, o, FALSE, isnull[i]);
    }
}

/*
 * Forms union of oldtup and addtup, if union == oldtup then return NULL
 */
IndexTuple gistgetadjusted(Relation r, IndexTuple oldtup, IndexTuple addtup, GISTSTATE *giststate)
{
    bool neednew = FALSE;
    GISTENTRY oldentries[INDEX_MAX_KEYS], addentries[INDEX_MAX_KEYS];
    bool oldisnull[INDEX_MAX_KEYS], addisnull[INDEX_MAX_KEYS];
    Datum attr[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    IndexTuple newtup = NULL;
    int i;

    gistDeCompressAtt(giststate, r, oldtup, NULL, (OffsetNumber)0, oldentries, oldisnull);

    gistDeCompressAtt(giststate, r, addtup, NULL, (OffsetNumber)0, addentries, addisnull);

    for (i = 0; i < r->rd_att->natts; i++) {
        gistMakeUnionKey(giststate, i, oldentries + i, oldisnull[i], addentries + i, addisnull[i], attr + i,
                         isnull + i);

        if (neednew)
            /* we already need new key, so we can skip check */
            continue;

        if (isnull[i])
            /* union of key may be NULL if and only if both keys are NULL */
            continue;

        if (!addisnull[i]) {
            if (oldisnull[i] || !gistKeyIsEQ(giststate, i, oldentries[i].key, attr[i]))
                neednew = true;
        }
    }

    if (neednew) {
        /* need to update key */
        newtup = gistFormTuple(giststate, r, attr, isnull, false);
        newtup->t_tid = oldtup->t_tid;
    }

    return newtup;
}

/*
 * Search an upper index page for the entry with lowest penalty for insertion
 * of the new index key contained in "it".
 *
 * Returns the index of the page entry to insert into.
 */
OffsetNumber gistchoose(Relation r, Page p, IndexTuple it, /* it has compressed entry */
                        GISTSTATE *giststate)
{
    OffsetNumber result;
    OffsetNumber maxoff;
    OffsetNumber i;
    float best_penalty[INDEX_MAX_KEYS];
    GISTENTRY entry, identry[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    Assert(!GistPageIsLeaf(p));

    gistDeCompressAtt(giststate, r, it, NULL, (OffsetNumber)0, identry, isnull);

    /* we'll return FirstOffsetNumber if page is empty (shouldn't happen) */
    result = FirstOffsetNumber;

    /*
     * The index may have multiple columns, and there's a penalty value for
     * each column.  The penalty associated with a column that appears earlier
     * in the index definition is strictly more important than the penalty of
     * a column that appears later in the index definition.
     *
     * best_penalty[j] is the best penalty we have seen so far for column j,
     * or -1 when we haven't yet examined column j.  Array entries to the
     * right of the first -1 are undefined.
     */
    best_penalty[0] = -1;

    /*
     * Loop over tuples on page.
     */
    maxoff = PageGetMaxOffsetNumber(p);
    Assert(maxoff >= FirstOffsetNumber);

    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        IndexTuple itup = (IndexTuple)PageGetItem(p, PageGetItemId(p, i));
        bool zero_penalty = false;
        int j;

        zero_penalty = true;

        /* Loop over index attributes. */
        for (j = 0; j < r->rd_att->natts; j++) {
            Datum datum;
            float usize;
            bool IsNull = false;

            /* Compute penalty for this column. */
            datum = index_getattr(itup, j + 1, giststate->tupdesc, &IsNull);
            gistdentryinit(giststate, j, &entry, datum, r, p, i, FALSE, IsNull);
            usize = gistpenalty(giststate, j, &entry, IsNull, &identry[j], isnull[j]);
            if (usize > 0)
                zero_penalty = false;

            if (best_penalty[j] < 0 || usize < best_penalty[j]) {
                /*
                 * New best penalty for column.  Tentatively select this tuple
                 * as the target, and record the best penalty.	Then reset the
                 * next column's penalty to "unknown" (and indirectly, the
                 * same for all the ones to its right).  This will force us to
                 * adopt this tuple's penalty values as the best for all the
                 * remaining columns during subsequent loop iterations.
                 */
                result = i;
                best_penalty[j] = usize;

                if (j < r->rd_att->natts - 1)
                    best_penalty[j + 1] = -1;
            } else if (best_penalty[j] == usize) {
                /*
                 * The current tuple is exactly as good for this column as the
                 * best tuple seen so far.	The next iteration of this loop
                 * will compare the next column.
                 */
            } else {
                /*
                 * The current tuple is worse for this column than the best
                 * tuple seen so far.  Skip the remaining columns and move on
                 * to the next tuple, if any.
                 */
                zero_penalty = false; /* so outer loop won't exit */
                break;
            }
        }

        /*
         * If we find a tuple with zero penalty for all columns, there's no
         * need to examine remaining tuples; just break out of the loop and
         * return it.
         */
        if (zero_penalty)
            break;
    }

    return result;
}

/*
 * initialize a GiST entry with a decompressed version of key
 */
void gistdentryinit(GISTSTATE *giststate, int nkey, GISTENTRY *e, Datum k, Relation r, Page pg, OffsetNumber o, bool l,
                    bool isNull)
{
    if (!isNull) {
        GISTENTRY *dep = NULL;

        gistentryinit(*e, k, r, pg, o, l);
        dep = (GISTENTRY *)DatumGetPointer(
            FunctionCall1Coll(&giststate->decompressFn[nkey], giststate->supportCollation[nkey], PointerGetDatum(e)));
        /* decompressFn may just return the given pointer */
        if (dep != e) {
            gistentryinit(*e, dep->key, dep->rel, dep->page, dep->offset, dep->leafkey);
        }
    } else {
        gistentryinit(*e, (Datum)0, r, pg, o, l);
    }
}

/*
 * initialize a GiST entry with a compressed version of key
 */
void gistcentryinit(GISTSTATE *giststate, int nkey, GISTENTRY *e, Datum k, Relation r, Page pg, OffsetNumber o, bool l,
                    bool isNull)
{
    if (!isNull) {
        GISTENTRY *cep = NULL;

        gistentryinit(*e, k, r, pg, o, l);
        cep = (GISTENTRY *)DatumGetPointer(
            FunctionCall1Coll(&giststate->compressFn[nkey], giststate->supportCollation[nkey], PointerGetDatum(e)));
        /* compressFn may just return the given pointer */
        if (cep != e) {
            gistentryinit(*e, cep->key, cep->rel, cep->page, cep->offset, cep->leafkey);
        }
    } else {
        gistentryinit(*e, (Datum)0, r, pg, o, l);
    }
}

IndexTuple gistFormTuple(GISTSTATE *giststate, Relation r, Datum attdata[], const bool isnull[], bool newValues)
{
    GISTENTRY centry[INDEX_MAX_KEYS];
    Datum compatt[INDEX_MAX_KEYS];
    int i;
    IndexTuple res;

    if (r->rd_att->natts >= INDEX_MAX_KEYS)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to form tuple from gist")));

    for (i = 0; i < r->rd_att->natts; i++) {
        if (isnull[i])
            compatt[i] = (Datum)0;
        else {
            gistcentryinit(giststate, i, &centry[i], attdata[i], r, NULL, (OffsetNumber)0, newValues, FALSE);
            compatt[i] = centry[i].key;
        }
    }

    res = index_form_tuple(giststate->tupdesc, compatt, isnull);

    /*
     * The offset number on tuples on internal pages is unused. For historical
     * reasons, it is set 0xffff.
     */
    ItemPointerSetOffsetNumber(&(res->t_tid), 0xffff);
    return res;
}

float gistpenalty(GISTSTATE *giststate, int attno, GISTENTRY *orig, bool isNullOrig, GISTENTRY *add, bool isNullAdd)
{
    float penalty = 0.0;

    if (giststate->penaltyFn[attno].fn_strict == FALSE || (isNullOrig == FALSE && isNullAdd == FALSE)) {
        FunctionCall3Coll(&giststate->penaltyFn[attno], giststate->supportCollation[attno], PointerGetDatum(orig),
                          PointerGetDatum(add), PointerGetDatum(&penalty));
        /* disallow negative or NaN penalty */
        if (isnan(penalty) || penalty < 0.0) {
            penalty = 0.0;
        }
    } else if (isNullOrig && isNullAdd) {
        penalty = 0.0;
    } else {
        /* try to prevent mixing null and non-null values */
        penalty = std::numeric_limits<float>::infinity();
    }

    return penalty;
}

void GISTInitPage(Page page, uint32 f, Size pageSize)
{
    GISTPageOpaque opaque;
    PageInit(page, pageSize, sizeof(GISTPageOpaqueData));
    opaque = GistPageGetOpaque(page);
    /* page was already zeroed by PageInit, so this is not needed: */
    opaque->rightlink = InvalidBlockNumber;
    opaque->flags = f;
    opaque->gist_page_id = GIST_PAGE_ID;
}

/*
 * Initialize a new index page
 */
void GISTInitBuffer(Buffer b, uint32 f)
{
    Page page;
    Size pageSize;

    pageSize = BufferGetPageSize(b);
    page = BufferGetPage(b);
    GISTInitPage(page, f, pageSize);
}

/*
 * Verify that a freshly-read page looks sane.
 */
void gistcheckpage(Relation rel, Buffer buf)
{
    Page page = BufferGetPage(buf);
    /*
     * ReadBuffer verifies that every newly-read page passes
     * PageHeaderIsValid, which means it either contains a reasonably sane
     * page header or is all-zero.	We have to defend against the all-zero
     * case, however.
     */
    if (PageIsNew(page))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("index \"%s\" contains unexpected zero page at block %u", RelationGetRelationName(rel),
                               BufferGetBlockNumber(buf)),
                        errhint("Please REINDEX it.")));

    /*
     * Additionally check that the special area looks sane.
     */
    if (PageGetSpecialSize(page) != MAXALIGN(sizeof(GISTPageOpaqueData)))
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("index \"%s\" contains corrupted page at block %u", RelationGetRelationName(rel),
                               BufferGetBlockNumber(buf)),
                        errhint("Please REINDEX it.")));
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 *
 * The returned buffer is already pinned and exclusive-locked
 *
 * Caller is responsible for initializing the page by calling GISTInitBuffer
 */
Buffer gistNewBuffer(Relation r)
{
    Buffer buffer;
    bool needLock = false;

    /* First, try to get a page from FSM */
    for (;;) {
        BlockNumber blkno = GetFreeIndexPage(r);
        if (blkno == InvalidBlockNumber)
            break; /* nothing left in FSM */

        buffer = ReadBuffer(r, blkno);
        /*
         * We have to guard against the possibility that someone else already
         * recycled this page; the buffer may be locked if so.
         */
        if (ConditionalLockBuffer(buffer)) {
            Page page = BufferGetPage(buffer);
            if (PageIsNew(page))
                return buffer; /* OK to use, if never initialized */

            gistcheckpage(r, buffer);

            if (GistPageIsDeleted(page))
                return buffer; /* OK to use */

            LockBuffer(buffer, GIST_UNLOCK);
        }

        /* Can't use it, so release buffer and try again */
        ReleaseBuffer(buffer);
    }

    /* Must extend the file */
    needLock = !RELATION_IS_LOCAL(r);
    if (needLock)
        LockRelationForExtension(r, ExclusiveLock);

    buffer = ReadBuffer(r, P_NEW);
    LockBuffer(buffer, GIST_EXCLUSIVE);

    if (needLock)
        UnlockRelationForExtension(r, ExclusiveLock);

    return buffer;
}

Datum gistoptions(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);
    relopt_value *options = NULL;
    GiSTOptions *rdopts = NULL;
    int numoptions;
    static const relopt_parse_elt tab[] = {
        { "fillfactor", RELOPT_TYPE_INT, offsetof(GiSTOptions, fillfactor) },
        { "buffering", RELOPT_TYPE_STRING, offsetof(GiSTOptions, bufferingModeOffset) }
    };

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_GIST, &numoptions);

    /* if none set, we're done */
    if (numoptions == 0)
        PG_RETURN_NULL();

    rdopts = (GiSTOptions *)allocateReloptStruct(sizeof(GiSTOptions), options, numoptions);

    fillRelOptions((void *)rdopts, sizeof(GiSTOptions), options, numoptions, validate, tab, lengthof(tab));

    pfree(options);

    PG_RETURN_BYTEA_P(rdopts);
}

/*
 * Temporary GiST indexes are not WAL-logged, but we need LSNs to detect
 * concurrent page splits anyway. GetXLogRecPtrForTemp() provides a fake
 * sequence of LSNs for that purpose. Each call generates an LSN that is
 * greater than any previous value returned by this function in the same
 * session.
 */
XLogRecPtr GetXLogRecPtrForTemp(void)
{
    u_sess->index_cxt.counter++;
    return u_sess->index_cxt.counter;
}
