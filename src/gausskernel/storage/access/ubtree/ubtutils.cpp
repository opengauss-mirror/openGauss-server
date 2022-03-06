/* -------------------------------------------------------------------------
 *
 * ubtutils.cpp
 *	  Utility code for openGauss btree implementation.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/ubtree/ubtutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <time.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

static bool UBTreeVisibilityCheckXid(TransactionId xmin, TransactionId xmax, bool xminCommitted, bool xmaxCommitted,
    Snapshot snapshot, bool isUpsert = false);
static bool UBTreeXidSatisfiesMVCC(TransactionId xid, bool committed, Snapshot snapshot);
static int UBTreeKeepNatts(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itupKey);
static bool UBTreeVisibilityCheckCid(IndexScanDesc scan, IndexTuple itup, bool *needRecheck);
static bool UBTreeItupEquals(IndexTuple itup1, IndexTuple itup2);

#define MAX(A, B) (((B) > (A)) ? (B) : (A))
#define MIN(A, B) (((B) < (A)) ? (B) : (A))

/*
 * _bt_mkscankey
 *      Build an insertion scan key that contains comparison data from itup
 *      as well as comparator routines appropriate to the key datatypes.
 *
 *      When itup is a non-pivot tuple, the returned insertion scan key is
 *      suitable for finding a place for it to go on the leaf level.  Pivot
 *      tuples can be used to re-find leaf page with matching high key, but
 *      then caller needs to set scan key's pivotsearch field to true.  This
 *      allows caller to search for a leaf page with a matching high key,
 *      which is usually to the left of the first leaf page a non-pivot match
 *      might appear on.
 *
 *      The result is intended for use with _bt_compare() and _bt_truncate().
 *      Callers that don't need to fill out the insertion scankey arguments
 *      (e.g. they use an ad-hoc comparison routine, or only need a scankey
 *      for _bt_truncate()) can pass a NULL index tuple.  The scankey will
 *      be initialized as if an "all truncated" pivot tuple was passed
 *      instead.
 *
 *      Note that we may occasionally have to share lock the metapage to
 *      determine whether or not the keys in the index are expected to be
 *      unique (i.e. if this is a "heapkeyspace" index).  We assume a
 *      heapkeyspace index when caller passes a NULL tuple, allowing index
 *      build callers to avoid accessing the non-existent metapage.
 */
BTScanInsert UBTreeMakeScanKey(Relation rel, IndexTuple itup)
{
    BTScanInsert key;
    ScanKey skey;
    TupleDesc itupdesc;
    int indnkeyatts;
    int16* indoption = NULL;
    int	tupnatts;
    int i;

    itupdesc = RelationGetDescr(rel);
    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    indoption = rel->rd_indoption;
    tupnatts = itup ? UBTreeTupleGetNAtts(itup, rel) : 0;

    Assert(indnkeyatts != 0);
    Assert(tupnatts <= IndexRelationGetNumberOfAttributes(rel));
    Assert(indnkeyatts <= IndexRelationGetNumberOfAttributes(rel));

    /*
     * We'll execute search using scan key constructed on key columns.
     * Truncated attributes and non-key attributes are omitted from the final
     * scan key.
     */
    key = (BTScanInsert) palloc(offsetof(BTScanInsertData, scankeys) +
                                sizeof(ScanKeyData) * indnkeyatts);

    /* ubtree always take TID as tie-breaker */
    key->heapkeyspace = true;
    key->anynullkeys = false; /* initial assumption */
    key->nextkey = false;
    key->pivotsearch = false;
    key->keysz = Min(indnkeyatts, tupnatts);
    key->scantid = (key->heapkeyspace && itup) ?
                   UBTreeTupleGetHeapTID(itup) : NULL;
    skey = key->scankeys;
    for (i = 0; i < indnkeyatts; i++) {
        FmgrInfo* procinfo = NULL;
        Datum arg;
        bool null = false;
        uint32 flags;

        /*
         * We can use the cached (default) support procs since no cross-type
         * comparison can be needed.
         */
        procinfo = index_getprocinfo(rel, i + 1, (uint16)BTORDER_PROC);

        /*
         * Key arguments built from truncated attributes (or when caller
         * provides no tuple) are defensively represented as NULL values. They
         * should never be used.
         */
        if (i < tupnatts) {
            arg = index_getattr(itup, i + 1, itupdesc, &null);
        } else {
            arg = (Datum)0;
            null = true;
        }
        flags = (null ? SK_ISNULL : 0) | (((uint16)indoption[i]) << SK_BT_INDOPTION_SHIFT);
        ScanKeyEntryInitializeWithInfo(&skey[i], flags, (AttrNumber)(i + 1), InvalidStrategy, InvalidOid,
                                       rel->rd_indcollation[i], procinfo, arg);
        /* Record if any key attribute is NULL (or truncated) */
        if (null)
            key->anynullkeys = true;
    }

    return key;
}

/*
 * Test whether an indextuple satisfies all the scankey conditions.
 *
 * If so, return the address of the index tuple on the index page.
 * If not, return NULL.
 *
 * If the tuple fails to pass the qual, we also determine whether there's
 * any need to continue the scan beyond this tuple, and set *continuescan
 * accordingly.  See comments for _bt_preprocess_keys(), above, about how
 * this is done.
 *
 * scan: index scan descriptor (containing a search-type scankey)
 * page: buffer page containing index tuple
 * offnum: offset number of index tuple (must be a valid item!)
 * dir: direction we are scanning in
 * continuescan: output parameter (will be set correctly in all cases)
 *
 * Caller must hold pin and lock on the index page.
 */
IndexTuple UBTreeCheckKeys(IndexScanDesc scan, Page page, OffsetNumber offnum, ScanDirection dir,
    bool *continuescan, bool *needRecheck)
{
    ItemId iid = PageGetItemId(page, offnum);
    bool tupleAlive = false;
    bool tupleVisible = true;
    IndexTuple tuple;
    TupleDesc tupdesc;
    BTScanOpaque so;
    int keysz;
    int ikey;
    ScanKey key;

    *continuescan = true; /* default assumption */

    /*
     * If the scan specifies not to return killed tuples, then we treat a
     * killed tuple as not passing the qual.  Most of the time, it's a win to
     * not bother examining the tuple's index keys, but just return
     * immediately with continuescan = true to proceed to the next tuple.
     * However, if this is the last tuple on the page, we should check the
     * index keys to prevent uselessly advancing to the next page.
     */
    if (scan->ignore_killed_tuples && ItemIdIsDead(iid)) {
        /* return immediately if there are more tuples on the page */
        if (ScanDirectionIsForward(dir)) {
            if (offnum < PageGetMaxOffsetNumber(page)) {
                return NULL;
            }
        } else {
            UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            if (offnum > P_FIRSTDATAKEY(opaque)) {
                return NULL;
            }
        }

        /*
         * OK, we want to check the keys so we can set continuescan correctly,
         * but we'll return NULL even if the tuple passes the key tests.
         */
        tupleAlive = false;
    } else {
        tupleAlive = true;
    }

    tuple = (IndexTuple)PageGetItem(page, iid);
    tupdesc = RelationGetDescr(scan->indexRelation);
    so = (BTScanOpaque)scan->opaque;
    keysz = so->numberOfKeys;

    for (key = so->keyData, ikey = 0; ikey < keysz; key++, ikey++) {
        Datum datum;
        bool isNull = false;
        Datum test;

        /* row-comparison keys need special processing */
        if (key->sk_flags & SK_ROW_HEADER) {
            if (_bt_check_rowcompare(key, tuple, tupdesc, dir, continuescan)) {
                continue;
            }
            return NULL;
        }

        datum = index_getattr(tuple, key->sk_attno, tupdesc, &isNull);

        if (key->sk_flags & SK_ISNULL) {
            /* Handle IS NULL/NOT NULL tests */
            if (key->sk_flags & SK_SEARCHNULL) {
                if (isNull) {
                    continue; /* tuple satisfies this qual */
                }
            } else {
                Assert(key->sk_flags & SK_SEARCHNOTNULL);
                if (!isNull) {
                    continue; /* tuple satisfies this qual */
                }
            }

            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             */
            if ((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                *continuescan = false;
            } else if ((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                *continuescan = false;
            }

            /*
             * In any case, this indextuple doesn't match the qual.
             */
            return NULL;
        }

        if (isNull) {
            if (key->sk_flags & SK_BT_NULLS_FIRST) {
                /*
                 * Since NULLs are sorted before non-NULLs, we know we have
                 * reached the lower limit of the range of values for this
                 * index attr.	On a backward scan, we can stop if this qual
                 * is one of the "must match" subset.  We can stop regardless
                 * of whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a forward scan, however, we must keep going, because we may
                 * have initially positioned to the start of the index.
                 */
                if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsBackward(dir)) {
                    *continuescan = false;
                }
            } else {
                /*
                 * Since NULLs are sorted after non-NULLs, we know we have
                 * reached the upper limit of the range of values for this
                 * index attr.	On a forward scan, we can stop if this qual is
                 * one of the "must match" subset.	We can stop regardless of
                 * whether the qual is > or <, so long as it's required,
                 * because it's not possible for any future tuples to pass. On
                 * a backward scan, however, we must keep going, because we
                 * may have initially positioned to the end of the index.
                 */
                if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) && ScanDirectionIsForward(dir)) {
                    *continuescan = false;
                }
            }

            /*
             * In any case, this indextuple doesn't match the qual.
             */
            return NULL;
        }

        test = FunctionCall2Coll(&key->sk_func, key->sk_collation, datum, key->sk_argument);
        if (!DatumGetBool(test)) {
            /*
             * Tuple fails this qual.  If it's a required qual for the current
             * scan direction, then we can conclude no further tuples will
             * pass, either.
             *
             * Note: because we stop the scan as soon as any required equality
             * qual fails, it is critical that equality quals be used for the
             * initial positioning in _bt_first() when they are available. See
             * comments in _bt_first().
             */
            if ((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) {
                *continuescan = false;
            } else if ((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)) {
                *continuescan = false;
            }

            /*
             * In any case, this indextuple doesn't match the qual.
             */
            return NULL;
        }
    }

    bool needVisibilityCheck = scan->xs_snapshot->satisfies != SNAPSHOT_ANY &&
                          scan->xs_snapshot->satisfies != SNAPSHOT_TOAST;
    TransactionId xmin, xmax;
    bool isDead = false;
    bool xminCommitted = false;
    bool xmaxCommitted = false;
    isDead = UBTreeItupGetXminXmax(page, offnum, InvalidTransactionId, &xmin, &xmax, &xminCommitted, &xmaxCommitted);
    tupleVisible = !isDead; /* without visibility check, return non-dead tuple */
    if (needVisibilityCheck) {
        /*
         * If this IndexTuple is not visible to the current Snapshot, try to get the next one.
         * We're not going to tell heap to skip visibility check, because it doesn't cost a lot and we need heap
         * to check the visibility with CID when snapshot's xid equals to xmin or xmax.
         */
        if (scan->xs_snapshot->satisfies == SNAPSHOT_MVCC &&
            (TransactionIdIsCurrentTransactionId(xmin) || TransactionIdIsCurrentTransactionId(xmax))) {
            tupleVisible = UBTreeVisibilityCheckCid(scan, tuple, needRecheck); /* need check cid */
        } else {
            tupleVisible = (!isDead) &&
                UBTreeVisibilityCheckXid(xmin, xmax, xminCommitted, xmaxCommitted, scan->xs_snapshot, scan->isUpsert);
        }
    }

    /* Check for failure due to it being a killed tuple. */
    if (!tupleAlive || !tupleVisible) {
        return NULL;
    }

    /* If we get here, the tuple passes all index quals. */
    return tuple;
}

/*
 *  BtCheckXid() -- return the status of the given xid
 *
 *      XID_COMMITTED / XID_ABORTED / XID_INPROGRESS
 */
TransactionIdStatus UBTreeCheckXid(TransactionId xid)
{
    TransactionIdStatus ts = TransactionIdGetStatus(xid);
    /* Please refer to HeapTupleSatisfiesVaccum */
    if (ts == XID_INPROGRESS) {
        if (TransactionIdIsInProgress(xid)) {
            /* Inprogress */
        } else if (TransactionIdDidCommit(xid)) {
            ts = XID_COMMITTED;
        } else {
            ts = XID_ABORTED;
        }
    }
    return ts;
}

/*
 *  _bt_itup_get_xmin_xmax() -- Get xmin/xmax from a IndexTuple, aborted xid won't be returned.
 *
 * We promise if the returned xids are not Invalid, they must not be aborted at this moment.
 *
 * return (isDead)
 */
bool UBTreeItupGetXminXmax(Page page, OffsetNumber offnum, TransactionId oldest_xmin, TransactionId *xmin,
    TransactionId *xmax, bool *xminCommitted, bool *xmaxCommitted)
{
    ItemId iid = PageGetItemId(page, offnum);
    IndexTuple itup = (IndexTuple)PageGetItem(page, iid);
    UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
    bool isDead = false;
    bool needCheckXmin = true;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    *xminCommitted = *xmaxCommitted = false;

    if (ItemIdIsDead(iid)) {
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        return true;
    }

    *xmin = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmin);
    *xmax = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmax);

    /* examine xmax */
    if (TransactionIdIsValid(*xmax)) {
        TransactionIdStatus ts = UBTreeCheckXid(*xmax);
        switch (ts) {
            case XID_INPROGRESS:
                if (TransactionIdEquals(*xmin, *xmax)) {
                    needCheckXmin = false;
                }
                break;
            case XID_COMMITTED:
                *xminCommitted = *xmaxCommitted = true;
                needCheckXmin = false;
                break;
            case XID_ABORTED:
                uxid->xmax = InvalidTransactionId;
                *xmax = InvalidTransactionId;
                if (TransactionIdEquals(*xmin, *xmax)) {
                    /* xmin xmax aborted */
                    uxid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    needCheckXmin = false;
                }
                break;
        }
    }

    /* examine xmin */
    if (needCheckXmin) {
        if (IndexItemIdIsFrozen(iid)) {
            *xminCommitted = true;
        } else if (TransactionIdIsValid(*xmin)) {
            TransactionIdStatus ts = UBTreeCheckXid(*xmin);
            switch (ts) {
                case XID_INPROGRESS:
                    break;
                case XID_COMMITTED:
                    *xminCommitted = true;
                    break;
                case XID_ABORTED:
                    uxid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    break;
            }
        }
    }

    /* if there is no passed oldest_xmin, we will ues the current oldest_xmin */
    if (!TransactionIdIsValid(oldest_xmin)) {
        GetOldestXminForUndo(&oldest_xmin);
    }

    if (!TransactionIdIsValid(*xmin)) {
        isDead = true;
    }
    /* before we mark the tuple as DEAD because of xmax, must comfirm that xmax has committed */
    if (*xmaxCommitted && TransactionIdPrecedes(*xmax, oldest_xmin)) {
        isDead = true;
    }

    /* before we mark the tuple as FROZEN, must comfirm that xmin has committed */
    if (IndexItemIdIsFrozen(iid)) {
        *xmin = FrozenTransactionId;
    } else if (*xminCommitted && TransactionIdPrecedes(*xmin, oldest_xmin)) {
        IndexItemIdSetFrozen(iid);
        *xmin = FrozenTransactionId;
    }

    if (isDead) {
        ItemIdMarkDead(iid);
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        *xminCommitted = *xmaxCommitted = false;
    }

    return isDead;
}

/*
 *  _bt_truncate() -- create tuple without unneeded suffix attributes.
 *
 * Returns truncated pivot index tuple allocated in caller's memory context,
 * with key attributes copied from caller's firstright argument.  If rel is
 * an INCLUDE index, non-key attributes will definitely be truncated away,
 * since they're not part of the key space.  More aggressive suffix
 * truncation can take place when it's clear that the returned tuple does not
 * need one or more suffix key attributes.  We only need to keep firstright
 * attributes up to and including the first non-lastleft-equal attribute.
 * Caller's insertion scankey is used to compare the tuples; the scankey's
 * argument values are not considered here.
 *
 * Sometimes this routine will return a new pivot tuple that takes up more
 * space than firstright, because a new heap TID attribute had to be added to
 * distinguish lastleft from firstright.  This should only happen when the
 * caller is in the process of splitting a leaf page that has many logical
 * duplicates, where it's unavoidable.
 *
 * Note that returned tuple's t_tid offset will hold the number of attributes
 * present, so the original item pointer offset is not represented.  Caller
 * should only change truncated tuple's downlink.  Note also that truncated
 * key attributes are treated as containing "minus infinity" values by
 * _bt_compare().
 *
 * In the worst case (when a heap TID is appended) the size of the returned
 * tuple is the size of the first right tuple plus an additional MAXALIGN()'d
 * item pointer.  This guarantee is important, since callers need to stay
 * under the 1/3 of a page restriction on tuple size.  If this routine is ever
 * taught to truncate within an attribute/datum, it will need to avoid
 * returning an enlarged tuple to caller when truncation + TOAST compression
 * ends up enlarging the final datum.
 */
IndexTuple UBTreeTruncate(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itup_key,
    bool itup_extended)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    int16 nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    int keepnatts;
    IndexTuple pivot;
    IndexTuple tidpivot;
    ItemPointer pivotheaptid;
    Size newsize;

    /*
     * We should only ever truncate non-pivot tuples from leaf pages.  It's
     * never okay to truncate when splitting an internal page.
     */
    Assert(!UBTreeTupleIsPivot(lastleft) && !UBTreeTupleIsPivot(firstright));
    Assert(itup_key != NULL);

    /* Determine how many attributes must be kept in truncated tuple */
    keepnatts = UBTreeKeepNatts(rel, lastleft, firstright, itup_key);

#ifdef DEBUG_NO_TRUNCATE
    /* Force truncation to be ineffective for testing purposes */
    keepnatts = nkeyatts + 1;
#endif

    pivot = UBTreeIndexTruncateTuple(itupdesc, firstright, Min(keepnatts, nkeyatts), itup_extended);

    /*
     * If there is a distinguishing key attribute within pivot tuple, we're
     * done
     */
    if (keepnatts <= nkeyatts) {
        UBTreeTupleSetNAtts(pivot, keepnatts, false);
        return pivot;
    }

    /*
     * We have to store a heap TID in the new pivot tuple, since no non-TID
     * key attribute value in firstright distinguishes the right side of the
     * split from the left side.  nbtree conceptualizes this case as an
     * inability to truncate away any key attributes, since heap TID is
     * treated as just another key attribute (despite lacking a pg_attribute
     * entry).
     *
     * Use enlarged space that holds a copy of pivot.  We need the extra space
     * to store a heap TID at the end (using the special pivot tuple
     * representation).
     */
    newsize = MAXALIGN(IndexTupleSize(pivot)) + MAXALIGN(sizeof(ItemPointerData));
    tidpivot = (IndexTuple) palloc0(newsize);
    errno_t rc = memcpy_s(tidpivot, newsize, pivot, MAXALIGN(IndexTupleSize(pivot)));
    securec_check(rc, "\0", "\0");
    /* Cannot leak memory here */
    pfree(pivot);

    /*
     * Store all of firstright's key attribute values plus a tiebreaker heap
     * TID value in enlarged pivot tuple
     */
    tidpivot->t_info &= ~INDEX_SIZE_MASK;
    tidpivot->t_info |= newsize;
    UBTreeTupleSetNAtts(tidpivot, nkeyatts, true);
    pivotheaptid = UBTreeTupleGetHeapTID(tidpivot);

    /*
     * Lehman & Yao use lastleft as the leaf high key in all cases, but don't
     * consider suffix truncation.  It seems like a good idea to follow that
     * example in cases where no truncation takes place -- use lastleft's heap
     * TID.  (This is also the closest value to negative infinity that's
     * legally usable.)
     */
    ItemPointerCopy(UBTreeTupleGetMaxHeapTID(lastleft), pivotheaptid);

    /*
     * We're done.  Assert() that heap TID invariants hold before returning.
     *
     * Lehman and Yao require that the downlink to the right page, which is to
     * be inserted into the parent page in the second phase of a page split be
     * a strict lower bound on items on the right page, and a non-strict upper
     * bound for items on the left page.  Assert that heap TIDs follow these
     * invariants, since a heap TID value is apparently needed as a
     * tiebreaker.
     *
     * With multi-version index, several index tuples may indicate to the same
     * heap tuple. In this case, even TID can be equal.
     */
#ifndef DEBUG_NO_TRUNCATE
    Assert(ItemPointerCompare(UBTreeTupleGetMaxHeapTID(lastleft), UBTreeTupleGetHeapTID(firstright)) <= 0);
    Assert(ItemPointerCompare(pivotheaptid, UBTreeTupleGetHeapTID(lastleft)) >= 0);
    Assert(ItemPointerCompare(pivotheaptid, UBTreeTupleGetHeapTID(firstright)) <= 0);
#else
    /*
     * Those invariants aren't guaranteed to hold for lastleft + firstright
     * heap TID attribute values when they're considered here only because
     * DEBUG_NO_TRUNCATE is defined (a heap TID is probably not actually
     * needed as a tiebreaker).  DEBUG_NO_TRUNCATE must therefore use a heap
     * TID value that always works as a strict lower bound for items to the
     * right.  In particular, it must avoid using firstright's leading key
     * attribute values along with lastleft's heap TID value when lastleft's
     * TID happens to be greater than firstright's TID.
     */
    ItemPointerCopy(UBTreeTupleGetHeapTID(firstright), pivotheaptid);

    /*
     * Pivot heap TID should never be fully equal to firstright.  Note that
     * the pivot heap TID will still end up equal to lastleft's heap TID when
     * that's the only usable value.
     */
    ItemPointerSetOffsetNumber(pivotheaptid, OffsetNumberPrev(ItemPointerGetOffsetNumber(pivotheaptid)));
    Assert(ItemPointerCompare(pivotheaptid, UBTreeTupleGetHeapTID(firstright)) < 0);
#endif

    return tidpivot;
}

static bool UBTreeItupEquals(IndexTuple itup1, IndexTuple itup2)
{
    if (itup1 == NULL || itup2 == NULL) {
        return false;
    }
    if (IndexTupleSize(itup1) == 0 || IndexTupleSize(itup2) == 0) {
        return false;
    }
    /*
     * compare the binary directly. If these index tuples are formed from the
     * same uheap tuple, they should be exactly the same.
     */
    return memcmp(itup1, itup2, IndexTupleSize(itup1)) == 0;
}

static bool UBTreeVisibilityCheckCid(IndexScanDesc scan, IndexTuple itup, bool *needRecheck)
{
    BTScanOpaque so = (BTScanOpaque)scan->opaque;

    if (UBTreeItupEquals((IndexTuple)so->lastSelfModifiedItup, itup)) {
        *needRecheck = false;
        return false; /* tuples with same key and TID will only returned once */
    }

    /* save this index tuple as lastSelfModifiedItup */
    /* Step1: Check that the buffer space is large enough. */
    uint newSize = 0;
    if (so->lastSelfModifiedItup == NULL) {
        newSize = IndexTupleSize(itup);
    } else if (so->lastSelfModifiedItupBufferSize < IndexTupleSize(itup)) {
        newSize = MAX(so->lastSelfModifiedItupBufferSize * 2, IndexTupleSize(itup));
        newSize = MIN(newSize, UBTDefaultMaxItemSize);
        pfree(so->lastSelfModifiedItup);
    }
    /* Step2: Extend when necessary. */
    if (newSize != 0) {
        so->lastSelfModifiedItup = (char*)palloc(newSize);
        so->lastSelfModifiedItupBufferSize = newSize;
    }
    /* Step3: Save the current IndexTuple. */
    errno_t rc = memcpy_s(so->lastSelfModifiedItup, UBTDefaultMaxItemSize, itup, IndexTupleSize(itup));
    securec_check(rc, "", "");

    *needRecheck = true;
    return true; /* treat as visible, but need recheck */
}

/*
 *  BtVisibilityCheck() -- check visibility
 *
 *      xmin/xmax come from _bt_itup_get_xmin_xmax(), in-progress or committed, won't be aborted at that moment.
 *
 *      xminCommitted && xmaxCommitted are just hint: true means committed, but false may also be committed.
 */
static bool UBTreeVisibilityCheckXid(TransactionId xmin, TransactionId xmax, bool xminCommitted, bool xmaxCommitted,
    Snapshot snapshot, bool isUpsert)
{
    if (snapshot->satisfies == SNAPSHOT_DIRTY && isUpsert) {
        bool xmaxVisible = xmaxCommitted || TransactionIdIsCurrentTransactionId(xmax);
        if (xmaxVisible) {
            return false;
        }
        return true;
    }

    /* only support MVCC and NOW, ereport used to locate bug */
    if (snapshot->satisfies != SNAPSHOT_VERSION_MVCC && 
        snapshot->satisfies != SNAPSHOT_MVCC && snapshot->satisfies != SNAPSHOT_NOW) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unsupported snapshot type %u for ustore's multi-version index.", snapshot->satisfies)));
    }

    /* handle snapshot MVCC */
    if (snapshot->satisfies == SNAPSHOT_VERSION_MVCC || snapshot->satisfies == SNAPSHOT_MVCC) {
        if (UBTreeXidSatisfiesMVCC(xmax, xmaxCommitted, snapshot)) {
            return false; /* already deleted */
        }
        if (!UBTreeXidSatisfiesMVCC(xmin, xminCommitted, snapshot)) {
            return false; /* have not inserted yet */
        }
    }

    /* handle snapshot NOW */
    if (snapshot->satisfies == SNAPSHOT_NOW) {
        return xminCommitted && !xmaxCommitted;
    }

    return true;
}

/*
 * BtXidSatisfiesMvcc() -- Check whether the xid is visible for the given MVCC snapshot.
 */
static bool UBTreeXidSatisfiesMVCC(TransactionId xid, bool committed, Snapshot snapshot)
{
    TransactionIdStatus ignore;

    if (!TransactionIdIsValid(xid))
        return false; /* invisible */
    if (xid == FrozenTransactionId)
        return true; /* frozen */

    /*
     * We can use snapshot's xmin/xmax as fast bypass after they become valid again.
     * Currently, snapshot's csn and xmin/xmax may be inconsistent. The reason is
     * that there is a problem with the cooperation of committing and subtransaction.
     */

    /* we can't tell visibility by snapshot's xmin/xmax alone, check snapshot */
    return XidVisibleInSnapshot(xid, snapshot, &ignore, InvalidBuffer, NULL);
}

/*
 * BtKeepNatts - how many key attributes to keep when truncating.
 *
 * Caller provides two tuples that enclose a split point.  Caller's insertion
 * scankey is used to compare the tuples; the scankey's argument values are
 * not considered here.
 *
 * This can return a number of attributes that is one greater than the
 * number of key attributes for the index relation.  This indicates that the
 * caller must use a heap TID as a unique-ifier in new pivot tuple.
 */
static int UBTreeKeepNatts(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itupKey)
{
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    TupleDesc itupdesc = RelationGetDescr(rel);
    int	keepnatts;
    ScanKey	scankey;

    /*
     * _bt_compare() treats truncated key attributes as having the value minus
     * infinity, which would break searches within !heapkeyspace indexes.  We
     * must still truncate away non-key attribute values, though.
     */
    if (!itupKey->heapkeyspace)
        return nkeyatts;

    scankey = itupKey->scankeys;
    keepnatts = 1;
    for (int attnum = 1; attnum <= nkeyatts; attnum++, scankey++) {
        Datum datum1, datum2;
        bool isNull1 = false;
        bool isNull2 = false;

        datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
        datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);

        if (isNull1 != isNull2) {
            break;
        }

        if (!isNull1 && DatumGetInt32(FunctionCall2Coll(&scankey->sk_func, scankey->sk_collation, datum1, datum2)) != 0)
            break;

        keepnatts++;
    }

    return keepnatts;
}

/*
 * BtKeepNattsFast - fast bitwise variant of BtKeepNatts.
 *
 * This is exported so that a candidate split point can have its effect on
 * suffix truncation inexpensively evaluated ahead of time when finding a
 * split location.  A naive bitwise approach to datum comparisons is used to
 * save cycles.
 *
 * The approach taken here usually provides the same answer as BtKeepNatts
 * will (for the same pair of tuples from a heapkeyspace index), since the
 * majority of btree opclasses can never indicate that two datums are equal
 * unless they're bitwise equal after detoasting.
 *
 * These issues must be acceptable to callers, typically because they're only
 * concerned about making suffix truncation as effective as possible without
 * leaving excessive amounts of free space on either side of page split.
 * Callers can rely on the fact that attributes considered equal here are
 * definitely also equal according to BtKeepNatts.
 */
int UBTreeKeepNattsFast(Relation rel, IndexTuple lastleft, IndexTuple firstright)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    int keysz = (rel->rd_rel->relnatts);
    int keepnatts;

    keepnatts = 1;
    for (int attnum = 1; attnum <= keysz; attnum++) {
        Datum datum1, datum2;
        bool isNull1 = false;
        bool isNull2 = false;
        Form_pg_attribute att;

        datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
        datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);
        att = TupleDescAttr(itupdesc, attnum - 1);

        if (isNull1 != isNull2) {
            break;
        }

        if (!isNull1 && !DatumImageEq(datum1, datum2, att->attbyval, att->attlen))
            break;

        keepnatts++;
    }

    return keepnatts;
}

/*
 * Check if index tuple have appropriate number of attributes.
 */
bool UBTreeCheckNatts(const Relation index, bool heapkeyspace, Page page, OffsetNumber offnum)
{
    int16 natts = IndexRelationGetNumberOfAttributes(index);
    int16 nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    IndexTuple itup;
    int tupnatts;

    /*
     * We cannot reliably test a deleted or half-dead page, since they have
     * dummy high keys
     */
    if (P_IGNORE(opaque)) {
        return true;
    }

    Assert(offnum >= FirstOffsetNumber && offnum <= PageGetMaxOffsetNumber(page));

    itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

    if (P_ISLEAF(opaque) && offnum >= P_FIRSTDATAKEY(opaque)) {
        /*
         * Regular leaf tuples have as every index attributes
         */
        return (UBTreeTupleGetNAtts(itup, index) == natts);
    } else if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque)) {
        /*
         * Leftmost tuples on non-leaf pages have no attributes, or haven't
         * INDEX_ALT_TID_MASK set in pg_upgraded indexes.
         */
        return (UBTreeTupleGetNAtts(itup, index) == 0 || ((itup->t_info & INDEX_ALT_TID_MASK) == 0));
    } else {
        /*
         * Pivot tuples stored in non-leaf pages and hikeys of leaf pages
         * contain only key attributes
         */
        if (!heapkeyspace) {
            return (UBTreeTupleGetNAtts(itup, index) <= nkeyatts);
        }
    }

    /* Handle heapkeyspace pivot tuples (excluding minus infinity items) */
    Assert(heapkeyspace);

    /*
     * Explicit representation of the number of attributes is mandatory with
     * heapkeyspace index pivot tuples, regardless of whether or not there are
     * non-key attributes.
     */
    if (!UBTreeTupleIsPivot(itup))
        return false;

    tupnatts = UBTreeTupleGetNAtts(itup, index);
    /*
     * Heap TID is a tiebreaker key attribute, so it cannot be untruncated
     * when any other key attribute is truncated
     */
    if (UBTreeTupleGetHeapTID(itup) != NULL && tupnatts != nkeyatts) {
        return false;
    }

    /*
     * Pivot tuple must have at least one untruncated key attribute (minus
     * infinity pivot tuples are the only exception).  Pivot tuples can never
     * represent that there is a value present for a key attribute that
     * exceeds pg_index.indnkeyatts for the index.
     */
    return tupnatts > 0 && tupnatts <= nkeyatts;
}

/*
 *  BtCheckThirdPage() -- check whether tuple fits on a btree page at all.
 *
 * We actually need to be able to fit three items on every page, so restrict
 * any one item to 1/3 the per-page available space.  Note that itemsz should
 * not include the ItemId overhead.
 *
 * It might be useful to apply TOAST methods rather than throw an error here.
 * Using out of line storage would break assumptions made by suffix truncation
 * and by contrib/amcheck, though.
 */
void UBTreeCheckThirdPage(Relation rel, Relation heap, bool needheaptidspace, Page page, IndexTuple newtup)
{
    Size itemsz;
    UBTPageOpaqueInternal opaque;

    itemsz = MAXALIGN(IndexTupleSize(newtup));
    /* Double check item size against limit */
    if (itemsz <= UBTMaxItemSize(page)) {
        return;
    }

    /*
     * Internal page insertions cannot fail here, because that would mean that
     * an earlier leaf level insertion that should have failed didn't
     */
    opaque = (UBTPageOpaqueInternal) PageGetSpecialPointer(page);
    if (!P_ISLEAF(opaque))
        elog(ERROR, "cannot insert oversized tuple of size %zu on internal page of index \"%s\"",
             itemsz, RelationGetRelationName(rel));

    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("index row size %lu exceeds maximum %lu for index \"%s\"", (unsigned long)itemsz,
                   (unsigned long)(UBTMaxItemSize(page)),
                   RelationGetRelationName(rel)),
            errdetail("Index row references tuple (%u,%u) in relation \"%s\".",
                      ItemPointerGetBlockNumber(&newtup->t_tid),
                      ItemPointerGetOffsetNumber(&newtup->t_tid),
                      heap ? RelationGetRelationName(heap) : "unknown"),
            errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
                    "Consider a function index of an MD5 hash of the value, "
                    "or use full text indexing.")));
}
