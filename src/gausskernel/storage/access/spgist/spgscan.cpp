/* -------------------------------------------------------------------------
 *
 * spgscan.cpp
 *	  routines for scanning SP-GiST indexes
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/gausskernel/storage/access/spgist/spgscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/spgist_private.h"
#include "utils/rel_gs.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"

typedef void (*storeRes_func)(SpGistScanOpaque so, ItemPointer heapPtr, Datum leafValue, bool isnull, bool recheck);

typedef struct ScanStackEntry {
    Datum reconstructedValue; /* value reconstructed from parent */
    int level;                /* level of items on this page */
    ItemPointerData ptr;      /* block and offset to scan from */
} ScanStackEntry;

/* Free a ScanStackEntry */
static void freeScanStackEntry(SpGistScanOpaque so, ScanStackEntry *stackEntry)
{
    if (!so->state.attType.attbyval && DatumGetPointer(stackEntry->reconstructedValue) != NULL) {
        pfree(DatumGetPointer(stackEntry->reconstructedValue));
    }
    pfree(stackEntry);
    stackEntry = NULL;
}

/* Free the entire stack */
static void freeScanStack(SpGistScanOpaque so)
{
    ListCell *lc = NULL;

    foreach (lc, so->scanStack) {
        freeScanStackEntry(so, (ScanStackEntry *)lfirst(lc));
    }
    list_free(so->scanStack);
    so->scanStack = NIL;
}

/*
 * Initialize scanStack to search the root page, resetting
 * any previously active scan
 */
static void resetSpGistScanOpaque(SpGistScanOpaque so)
{
    ScanStackEntry *startEntry = NULL;

    freeScanStack(so);

    if (so->searchNulls) {
        /* Stack a work item to scan the null index entries */
        startEntry = (ScanStackEntry *)palloc0(sizeof(ScanStackEntry));
        ItemPointerSet(&startEntry->ptr, (uint32)SPGIST_NULL_BLKNO, FirstOffsetNumber);
        so->scanStack = lappend(so->scanStack, startEntry);
    }

    if (so->searchNonNulls) {
        /* Stack a work item to scan the non-null index entries */
        startEntry = (ScanStackEntry *)palloc0(sizeof(ScanStackEntry));
        ItemPointerSet(&startEntry->ptr, (uint32)SPGIST_ROOT_BLKNO, FirstOffsetNumber);
        so->scanStack = lappend(so->scanStack, startEntry);
    }

    if (so->want_itup) {
        /* Must pfree IndexTuples to avoid memory leak */
        for (int i = 0; i < so->nPtrs; i++) {
            pfree(so->indexTups[i]);
            so->indexTups[i] = NULL;
        }
    }
    so->iPtr = so->nPtrs = 0;
}

/*
 * Prepare scan keys in SpGistScanOpaque from caller-given scan keys
 *
 * Sets searchNulls, searchNonNulls, numberOfKeys, keyData fields of *so.
 *
 * The point here is to eliminate null-related considerations from what the
 * opclass consistent functions need to deal with.	We assume all SPGiST-
 * indexable operators are strict, so any null RHS value makes the scan
 * condition unsatisfiable.  We also pull out any IS NULL/IS NOT NULL
 * conditions; their effect is reflected into searchNulls/searchNonNulls.
 */
static void spgPrepareScanKeys(IndexScanDesc scan)
{
    SpGistScanOpaque so = (SpGistScanOpaque)scan->opaque;
    bool qual_ok = true;
    bool haveIsNull = false;
    bool haveNotNull = false;
    int nkeys;
    int i;

    if (scan->numberOfKeys <= 0) {
        /* If no quals, whole-index scan is required */
        so->searchNulls = true;
        so->searchNonNulls = true;
        so->numberOfKeys = 0;
        return;
    }

    /* Examine the given quals */
    qual_ok = true;
    haveIsNull = haveNotNull = false;
    nkeys = 0;
    for (i = 0; i < scan->numberOfKeys; i++) {
        ScanKey skey = &scan->keyData[i];

        if (skey->sk_flags & SK_SEARCHNULL) {
            haveIsNull = true;
        } else if (skey->sk_flags & SK_SEARCHNOTNULL) {
            haveNotNull = true;
        } else if (skey->sk_flags & SK_ISNULL) {
            /* ordinary qual with null argument - unsatisfiable */
            qual_ok = false;
            break;
        } else {
            /* ordinary qual, propagate into so->keyData */
            so->keyData[nkeys++] = *skey;
            /* this effectively creates a not-null requirement */
            haveNotNull = true;
        }
    }

    /* IS NULL in combination with something else is unsatisfiable */
    if (haveIsNull && haveNotNull) {
        qual_ok = false;
    }

    /* Emit results */
    if (qual_ok) {
        so->searchNulls = haveIsNull;
        so->searchNonNulls = haveNotNull;
        so->numberOfKeys = nkeys;
    } else {
        so->searchNulls = false;
        so->searchNonNulls = false;
        so->numberOfKeys = 0;
    }
}

Datum spgbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int keysz = PG_GETARG_INT32(1);

    IndexScanDesc scan;
    SpGistScanOpaque so;

    scan = RelationGetIndexScan(rel, keysz, 0);

    so = (SpGistScanOpaque)palloc0(sizeof(SpGistScanOpaqueData));
    if (keysz > 0) {
        so->keyData = (ScanKey)palloc(sizeof(ScanKeyData) * keysz);
    }
    initSpGistState(&so->state, scan->indexRelation);
    so->tempCxt = AllocSetContextCreate(CurrentMemoryContext, "SP-GiST search temporary context",
                                        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /* Set up indexTupDesc and xs_itupdesc in case it's an index-only scan */
    so->indexTupDesc = scan->xs_itupdesc = RelationGetDescr(rel);

    scan->opaque = so;

    PG_RETURN_POINTER(scan);
}

Datum spgrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    SpGistScanOpaque so = (SpGistScanOpaque)scan->opaque;
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    /* copy scankeys into local storage */
    if (scankey && scan->numberOfKeys > 0) {
        errno_t rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData), scankey,
                               scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    /* preprocess scankeys, set up the representation in *so */
    spgPrepareScanKeys(scan);

    /* set up starting stack entries */
    resetSpGistScanOpaque(so);

    PG_RETURN_VOID();
}

Datum spgendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    SpGistScanOpaque so = (SpGistScanOpaque)scan->opaque;

    MemoryContextDelete(so->tempCxt);

    PG_RETURN_VOID();
}

Datum spgmarkpos(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPGiST does not support mark/restore")));
    PG_RETURN_VOID();
}

Datum spgrestrpos(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SPGiST does not support mark/restore")));
    PG_RETURN_VOID();
}

/*
 * Test whether a leaf tuple satisfies all the scan keys
 *
 * *leafValue is set to the reconstructed datum, if provided
 * *recheck is set true if any of the operators are lossy
 */
static bool spgLeafTest(Relation index, SpGistScanOpaque so, SpGistLeafTuple leafTuple, bool isnull, int level,
                        Datum reconstructedValue, Datum *leafValue, bool *recheck)
{
    bool result = false;
    Datum leafDatum;
    spgLeafConsistentIn in;
    spgLeafConsistentOut out;
    FmgrInfo *procinfo = NULL;
    MemoryContext oldCtx;

    if (isnull) {
        /* Should not have arrived on a nulls page unless nulls are wanted */
        Assert(so->searchNulls);
        *leafValue = (Datum)0;
        *recheck = false;
        return true;
    }

    leafDatum = SGLTDATUM(leafTuple, &so->state);

    /* use temp context for calling leaf_consistent */
    oldCtx = MemoryContextSwitchTo(so->tempCxt);

    in.scankeys = so->keyData;
    in.nkeys = so->numberOfKeys;
    in.reconstructedValue = reconstructedValue;
    in.level = level;
    in.returnData = so->want_itup;
    in.leafDatum = leafDatum;

    out.leafValue = (Datum)0;
    out.recheck = false;

    procinfo = index_getprocinfo(index, 1, SPGIST_LEAF_CONSISTENT_PROC);
    result = DatumGetBool(
        FunctionCall2Coll(procinfo, index->rd_indcollation[0], PointerGetDatum(&in), PointerGetDatum(&out)));

    *leafValue = out.leafValue;
    *recheck = out.recheck;

    (void)MemoryContextSwitchTo(oldCtx);

    return result;
}

/*
 * Walk the tree and report all tuples passing the scan quals to the storeRes
 * subroutine.
 *
 * If scanWholeIndex is true, we'll do just that.  If not, we'll stop at the
 * next page boundary once we have reported at least one tuple.
 */
static void spgWalk(Relation index, SpGistScanOpaque so, bool scanWholeIndex, storeRes_func storeRes)
{
    Buffer buffer = InvalidBuffer;
    bool reportedSome = false;

    while (scanWholeIndex || !reportedSome) {
        ScanStackEntry *stackEntry = NULL;
        BlockNumber blkno;
        OffsetNumber offset;
        Page page;
        bool isnull = false;

        /* Pull next to-do item from the list */
        if (so->scanStack == NIL) {
            break; /* there are no more pages to scan */
        }

        stackEntry = (ScanStackEntry *)linitial(so->scanStack);
        so->scanStack = list_delete_first(so->scanStack);

    redirect:
        /* Check for interrupts, just in case of infinite loop */
        CHECK_FOR_INTERRUPTS();

        blkno = ItemPointerGetBlockNumber(&stackEntry->ptr);
        offset = ItemPointerGetOffsetNumber(&stackEntry->ptr);

        if (buffer == InvalidBuffer) {
            buffer = ReadBuffer(index, blkno);
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
        } else if (blkno != BufferGetBlockNumber(buffer)) {
            UnlockReleaseBuffer(buffer);
            buffer = ReadBuffer(index, blkno);
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
        }
        /* else new pointer points to the same page, no work needed */
        page = BufferGetPage(buffer);

        isnull = SpGistPageStoresNulls(page) ? true : false;

        if (SpGistPageIsLeaf(page)) {
            SpGistLeafTuple leafTuple;
            OffsetNumber max = PageGetMaxOffsetNumber(page);
            Datum leafValue = (Datum)0;
            bool recheck = false;

            if (SpGistBlockIsRoot(blkno)) {
                /* When root is a leaf, examine all its tuples */
                for (offset = FirstOffsetNumber; offset <= max; offset++) {
                    leafTuple = (SpGistLeafTuple)PageGetItem(page, PageGetItemId(page, offset));
                    if (leafTuple->tupstate != SPGIST_LIVE) {
                        /* all tuples on root should be live */
                        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                        errmsg("unexpected SPGiST tuple state: %d", leafTuple->tupstate)));
                    }

                    Assert(ItemPointerIsValid(&leafTuple->heapPtr));
                    if (spgLeafTest(index, so, leafTuple, isnull, stackEntry->level, stackEntry->reconstructedValue,
                                    &leafValue, &recheck)) {
                        storeRes(so, &leafTuple->heapPtr, leafValue, isnull, recheck);
                        reportedSome = true;
                    }
                }
            } else {
                /* Normal case: just examine the chain we arrived at */
                while (offset != InvalidOffsetNumber) {
                    Assert(offset >= FirstOffsetNumber && offset <= max);
                    leafTuple = (SpGistLeafTuple)PageGetItem(page, PageGetItemId(page, offset));
                    if (leafTuple->tupstate != SPGIST_LIVE) {
                        if (leafTuple->tupstate == SPGIST_REDIRECT) {
                            /* redirection tuple should be first in chain */
                            Assert(offset == ItemPointerGetOffsetNumber(&stackEntry->ptr));
                            /* transfer attention to redirect point */
                            stackEntry->ptr = ((SpGistDeadTuple)leafTuple)->pointer;
                            Assert(ItemPointerGetBlockNumber(&stackEntry->ptr) != SPGIST_METAPAGE_BLKNO);
                            goto redirect;
                        }
                        if (leafTuple->tupstate == SPGIST_DEAD) {
                            /* dead tuple should be first in chain */
                            Assert(offset == ItemPointerGetOffsetNumber(&stackEntry->ptr));
                            /* No live entries on this page */
                            Assert(leafTuple->nextOffset == InvalidOffsetNumber);
                            break;
                        }
                        /* We should not arrive at a placeholder */
                        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                        errmsg("unexpected SPGiST tuple state: %d", leafTuple->tupstate)));
                    }

                    Assert(ItemPointerIsValid(&leafTuple->heapPtr));
                    if (spgLeafTest(index, so, leafTuple, isnull, stackEntry->level, stackEntry->reconstructedValue,
                                    &leafValue, &recheck)) {
                        storeRes(so, &leafTuple->heapPtr, leafValue, isnull, recheck);
                        reportedSome = true;
                    }

                    offset = leafTuple->nextOffset;
                }
            }
        } else { /* page is inner */
            SpGistInnerTuple innerTuple;
            spgInnerConsistentIn in;
            spgInnerConsistentOut out;
            FmgrInfo *procinfo = NULL;
            SpGistNodeTuple *nodes = NULL;
            SpGistNodeTuple node;
            int i = 0;
            MemoryContext oldCtx;

            innerTuple = (SpGistInnerTuple)PageGetItem(page, PageGetItemId(page, offset));
            if (innerTuple->tupstate != SPGIST_LIVE) {
                if (innerTuple->tupstate == SPGIST_REDIRECT) {
                    /* transfer attention to redirect point */
                    stackEntry->ptr = ((SpGistDeadTuple)innerTuple)->pointer;
                    Assert(ItemPointerGetBlockNumber(&stackEntry->ptr) != SPGIST_METAPAGE_BLKNO);
                    goto redirect;
                }
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("unexpected SPGiST tuple state: %d", innerTuple->tupstate)));
            }

            /* use temp context for calling inner_consistent */
            oldCtx = MemoryContextSwitchTo(so->tempCxt);

            in.scankeys = so->keyData;
            in.nkeys = so->numberOfKeys;
            in.reconstructedValue = stackEntry->reconstructedValue;
            in.level = stackEntry->level;
            in.returnData = so->want_itup;
            in.allTheSame = innerTuple->allTheSame;
            in.hasPrefix = (innerTuple->prefixSize > 0);
            in.prefixDatum = SGITDATUM(innerTuple, &so->state);
            in.nNodes = innerTuple->nNodes;
            in.nodeLabels = spgExtractNodeLabels(&so->state, innerTuple);

            /* collect node pointers */
            nodes = (SpGistNodeTuple *)palloc(sizeof(SpGistNodeTuple) * in.nNodes);
            SGITITERATE(innerTuple, i, node)
            {
                nodes[i] = node;
            }

            errno_t rc = memset_s(&out, sizeof(out), 0, sizeof(out));
            securec_check(rc, "\0", "\0");

            if (!isnull) {
                /* use user-defined inner consistent method */
                procinfo = index_getprocinfo(index, 1, SPGIST_INNER_CONSISTENT_PROC);
                FunctionCall2Coll(procinfo, index->rd_indcollation[0], PointerGetDatum(&in), PointerGetDatum(&out));
            } else {
                /* force all children to be visited */
                out.nNodes = in.nNodes;
                out.nodeNumbers = (int *)palloc(sizeof(int) * in.nNodes);
                for (i = 0; i < in.nNodes; i++)
                    out.nodeNumbers[i] = i;
            }

            (void)MemoryContextSwitchTo(oldCtx);

            /* If allTheSame, they should all or none of 'em match */
            if (innerTuple->allTheSame && out.nNodes != 0 && out.nNodes != in.nNodes) {
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                                errmsg("inconsistent inner_consistent results for allTheSame inner tuple")));
            }

            for (i = 0; i < out.nNodes; i++) {
                int nodeN = out.nodeNumbers[i];

                Assert(nodeN >= 0 && nodeN < in.nNodes);
                if (ItemPointerIsValid(&nodes[nodeN]->t_tid)) {
                    ScanStackEntry *newEntry = NULL;

                    /* Create new work item for this node */
                    newEntry = (ScanStackEntry *)palloc(sizeof(ScanStackEntry));
                    newEntry->ptr = nodes[nodeN]->t_tid;
                    newEntry->level = stackEntry->level;
                    if (out.levelAdds != NULL) {
                        newEntry->level += out.levelAdds[i];
                    }

                    /* Must copy value out of temp context */
                    if (out.reconstructedValues) {
                        newEntry->reconstructedValue = datumCopy(out.reconstructedValues[i], so->state.attType.attbyval,
                                                                 so->state.attType.attlen);
                    } else {
                        newEntry->reconstructedValue = (Datum)0;
                    }

                    so->scanStack = lcons(newEntry, so->scanStack);
                }
            }
        }

        /* done with this scan stack entry */
        freeScanStackEntry(so, stackEntry);
        /* clear temp context before proceeding to the next one */
        MemoryContextReset(so->tempCxt);
    }

    if (buffer != InvalidBuffer) {
        UnlockReleaseBuffer(buffer);
    }
}

/* storeRes subroutine for getbitmap case */
static void storeBitmap(SpGistScanOpaque so, ItemPointer heapPtr, Datum leafValue, bool isnull, bool recheck)
{
    tbm_add_tuples(so->tbm, heapPtr, 1, recheck, so->partHeapOid);
    so->ntids++;
}

Datum spggetbitmap(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    TIDBitmap *tbm = (TIDBitmap *)PG_GETARG_POINTER(1);
    SpGistScanOpaque so = (SpGistScanOpaque)scan->opaque;

    /* Copy want_itup to *so so we don't need to pass it around separately */
    so->want_itup = false;
    so->tbm = tbm;
    so->ntids = 0;
    so->partHeapOid = IndexScanGetPartHeapOid(scan);

    spgWalk(scan->indexRelation, so, true, storeBitmap);

    PG_RETURN_INT64(so->ntids);
}

/* storeRes subroutine for gettuple case */
static void storeGettuple(SpGistScanOpaque so, ItemPointer heapPtr, Datum leafValue, bool isnull, bool recheck)
{
    Assert(so->nPtrs < MaxIndexTuplesPerPage);
    so->heapPtrs[so->nPtrs] = *heapPtr;
    so->recheck[so->nPtrs] = recheck;
    if (so->want_itup) {
        /*
         * Reconstruct desired IndexTuple.	We have to copy the datum out of
         * the temp context anyway, so we may as well create the tuple here.
         */
        so->indexTups[so->nPtrs] = index_form_tuple(so->indexTupDesc, &leafValue, &isnull);
    }
    so->nPtrs++;
}

Datum spggettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection dir = (ScanDirection)PG_GETARG_INT32(1);
    SpGistScanOpaque so = (SpGistScanOpaque)scan->opaque;

    if (dir != ForwardScanDirection) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SP-GiST only supports forward scan direction")));
    }

    /* Copy want_itup to *so so we don't need to pass it around separately */
    so->want_itup = scan->xs_want_itup;

    for (;;) {
        if (so->iPtr < so->nPtrs) {
            /* continuing to return tuples from a leaf page */
            scan->xs_ctup.t_self = so->heapPtrs[so->iPtr];
            scan->xs_recheck = so->recheck[so->iPtr];
            scan->xs_itup = so->indexTups[so->iPtr];
            so->iPtr++;
            PG_RETURN_BOOL(true);
        }

        if (so->want_itup) {
            /* Must pfree IndexTuples to avoid memory leak */
            for (int i = 0; i < so->nPtrs; i++) {
                pfree(so->indexTups[i]);
                so->indexTups[i] = NULL;
            }
        }
        so->iPtr = so->nPtrs = 0;

        spgWalk(scan->indexRelation, so, false, storeGettuple);

        if (so->nPtrs == 0) {
            break; /* must have completed scan */
        }
    }

    PG_RETURN_BOOL(false);
}

Datum spgcanreturn(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    SpGistCache *cache = NULL;

    /* We can do it if the opclass config function says so */
    cache = spgGetCache(index);

    PG_RETURN_BOOL(cache->config.canReturnData);
}
