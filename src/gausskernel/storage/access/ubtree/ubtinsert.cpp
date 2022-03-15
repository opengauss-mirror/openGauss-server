/* -------------------------------------------------------------------------
 *
 * ubtinsert.cpp
 *    Item insertion in Lehman and Yao btrees for Postgres.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/ubtree/ubtinsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/smgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"
#include "pgstat.h"

static TransactionId UBTreeCheckUnique(Relation rel, IndexTuple itup, Relation heapRel, Buffer buf,
    OffsetNumber offset, BTScanInsert itup_key, IndexUniqueCheck checkUnique, bool *is_unique, GPIScanDesc gpiScan);
static OffsetNumber UBTreeFindInsertLoc(Relation rel, Buffer *bufptr, OffsetNumber firstlegaloff, BTScanInsert itup_key,
    IndexTuple newtup, bool checkingunique, BTStack stack, Relation heapRel);
static OffsetNumber UBTreeFindDeleteLoc(Relation rel, Buffer* bufP, OffsetNumber offset,
    BTScanInsert itup_key, IndexTuple itup);
static void UBTreeInsertOnPage(Relation rel, BTScanInsert itup_key, Buffer buf, Buffer cbuf, BTStack stack,
    IndexTuple itup, OffsetNumber newitemoff, bool split_only_page);
static Buffer UBTreeSplit(Relation rel, Buffer buf, Buffer cbuf, OffsetNumber firstright, OffsetNumber newitemoff,
    Size newitemsz, IndexTuple newitem, bool newitemonleft, BTScanInsert itup_key);
static bool UBTreePageAddTuple(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off, bool isnew);
static bool UBTreeIsEqual(Relation idxrel, Page page, OffsetNumber offnum, int keysz, ScanKey scankey);
static void UBTreeDeleteOnPage(Relation rel, Buffer buf, OffsetNumber offset);
static Buffer UBTreeNewRoot(Relation rel, Buffer lbuf, Buffer rbuf);

/*
 *  UBTreePagePruneOpt() -- Optional prune a index page
 *
 * This function trying to prune the page if possible, and always do BtPageRepairFragmentation()
 * if we deleted any dead tuple.
 *
 * The passed tryDelete indicate whether we just try to delete the whole page. When this value is
 * true, we will do nothing if there is an alive tuple in the page, and delete the whole page if all
 * tuples are dead.
 *
 * IMPORTANT: Make sure we've got the write lock on the buf before call this function.
 *
 *      Note: The page must be a leaf page.
 */
bool UBTreePagePruneOpt(Relation rel, Buffer buf, bool tryDelete)
{
    Page page = BufferGetPage(buf);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    TransactionId oldestXmin = InvalidTransactionId;

    WHITEBOX_TEST_STUB("UBTreePagePruneOpt", WhiteboxDefaultErrorEmit);

    /*
     * We can't write WAL in recovery mode, so there's no point trying to
     * clean the page. The master will likely issue a cleaning WAL record soon
     * anyway, so this is no particular loss.
     */
    if (RecoveryInProgress()) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    if (!P_ISLEAF(opaque)) {
        /* we can't prune internal pages */
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    if (tryDelete && P_RIGHTMOST(opaque)) {
        /* can't delete the right most page */
        _bt_relbuf(rel, buf);
        return false;
    }

    GetOldestXminForUndo(&oldestXmin);

    Assert(TransactionIdIsValid(oldestXmin));

    /*
     * Should not prune page when use local snapshot, InitPostgres e.g.
     * If use local snapshot RecentXmin might not consider xid cn send later,
     * wrong xid status might be judged in TransactionIdIsInProgress,
     * and wrong tuple infomask might be set in HeapTupleSatisfiesVacuum.
     * So if useLocalSnapshot is ture in postmaster env, we don't prune page.
     * Keep prune page can be done in single mode (standlone --single), so just in PostmasterEnvironment.
     */
    if ((t_thrd.xact_cxt.useLocalSnapshot && IsPostmasterEnvironment) ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster || u_sess->attr.attr_common.upgrade_mode == 1) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    /*
     * Let's see if we really need pruning.
     *
     * Forget it if page is not hinted to contain something prunable that's
     * older than oldestXmin.
     */
    if (TransactionIdIsValid(PageGetPruneXid(page)) && !IndexPageIsPrunable(page, oldestXmin)) {
        if (tryDelete)
            _bt_relbuf(rel, buf);
        return false;
    }

    /* Ok to prune */

    if (tryDelete) {
        /* if we are trying to delete a page, prune it first (we need WAL), then check empty and delete */
        UBTreePagePrune(rel, buf, oldestXmin);

        Assert(!P_RIGHTMOST(opaque));
        if (PageGetMaxOffsetNumber(page) == 1) {
            /* already empty (only HIKEY left), ok to delete */
            return UBTreePageDel(rel, buf) > 0;
        } else {
            _bt_relbuf(rel, buf);
            return false;
        }
    }

    return UBTreePagePrune(rel, buf, oldestXmin);
}

/*
 *  UBTreePagePrune() -- Apply page pruning to the page.
 *
 * If passed tryDelete parameter is false, delete all dead tuples in the page, then repair the fragmentation.
 * If passed tryDelete is true, only delete the whole page when all tuples are dead.
 *
 *      Note: we won't delete the high key (if any).
 */
bool UBTreePagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts)
{
    int npreviousDead = 0;
    int16 activeTupleCount = 0;
    bool has_pruned = false;
    Page page = BufferGetPage(buf);
    UBTPageOpaqueInternal opaque;
    OffsetNumber offnum, maxoff;
    IndexPruneState prstate;

    WHITEBOX_TEST_STUB("UBTreePagePruneOpt", WhiteboxDefaultErrorEmit);

#ifdef XLOG_BTREE_PRUNE_DEBUG
    errno_t rc = memcpy_s(xlrec.pagebak, sizeof(xl_ubtree_prune_page), page, BLCKSZ);
    securec_check(rc, "\0", "\0");
#endif

    prstate.new_prune_xid = InvalidTransactionId;
    prstate.latestRemovedXid = InvalidTransactionId;
    prstate.ndead = 0;
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* Scan the page */
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;

        itemid = PageGetItemId(page, offnum);
        /* Nothing to do if slot is already dead, just count npreviousDead */
        if (ItemIdIsDead(itemid)) {
            prstate.previousdead[npreviousDead] = offnum;
            npreviousDead++;
            continue;
        }

        if (RelationIsGlobalIndex(rel) && invisibleParts != NULL) {
            AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
            TupleDesc tupdesc = RelationGetDescr(rel);

            IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);

            bool isnull = false;
            Oid partOid = DatumGetUInt32(index_getattr(itup, partitionOidAttr, tupdesc, &isnull));
            Assert(!isnull);

            if (OidRBTreeMemberOid(invisibleParts, partOid)) {
                /* record dead */
                prstate.nowdead[prstate.ndead] = offnum;
                prstate.ndead++;
                continue;
            }
        }

        /* check visibility and record dead tuples */
        if (UBTreePruneItem(page, offnum, oldestXmin, &prstate)) {
            activeTupleCount++;
        }
    }


    /* Any error while applying the changes is critical */
    START_CRIT_SECTION();

    /* Have we found any prunable items? */
    if (npreviousDead > 0 || prstate.ndead > 0) {
        /* Set up flags and try to repair page  fragmentation */
        WaitState oldStatus = pgstat_report_waitstatus(STATE_PRUNE_INDEX);
        UBTreePagePruneExecute(page, prstate.previousdead, npreviousDead, &prstate);
        UBTreePagePruneExecute(page, prstate.nowdead, prstate.ndead, &prstate);

        UBTreePageRepairFragmentation(page);

        has_pruned = true;

        /*
         * Update the page's pd_prune_xid field to either zero, or the lowest
         * XID of any soon-prunable tuple.
         */
        PageSetPruneXid(page, prstate.new_prune_xid);
        opaque->activeTupleCount = activeTupleCount;

        MarkBufferDirty(buf);

        /* xlog stuff */
        if (RelationNeedsWAL(rel)) {
            XLogRecPtr recptr;
            xl_ubtree_prune_page xlrec;
            xlrec.count = npreviousDead + prstate.ndead;
            xlrec.new_prune_xid = prstate.new_prune_xid;
            xlrec.latestRemovedXid = prstate.latestRemovedXid;

            XLogBeginInsert();
            XLogRegisterData((char*)&xlrec, SizeOfUBTreePrunePage);
            XLogRegisterData((char*)&prstate.previousdead, npreviousDead * sizeof(OffsetNumber));
            XLogRegisterData((char*)&prstate.nowdead, prstate.ndead * sizeof(OffsetNumber));

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

            recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_PRUNE_PAGE);

            PageSetLSN(page, recptr);
        }
        pgstat_report_waitstatus(oldStatus);
    } else {
        /*
         * If we didn't prune anything, but have found a new value for the
         * pd_prune_xid field, update it and mark the buffer dirty. This is
         * treated as a non-WAL-logged hint.
         *
         * Also clear the "page is full" flag if it is set, since there's no
         * point in repeating the prune/defrag process until something else
         * happens to the page.
         */
        if (PageGetPruneXid(page) != prstate.new_prune_xid || opaque->activeTupleCount != activeTupleCount) {
            PageSetPruneXid(page, prstate.new_prune_xid);
            opaque->activeTupleCount = activeTupleCount;
            MarkBufferDirtyHint(buf, true);
        }
    }

    END_CRIT_SECTION();

    return has_pruned;
}

/*
 *  UBTreePruneItem() -- Check a item's status, and update prune state if found any dead tuple or new_prune_xid.
 *
 * The return value indicates whether the item is active.
 */
bool UBTreePruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate)
{
    TransactionId xmin, xmax;
    bool isDead = false;
    bool xminCommitted = false;
    bool xmaxCommitted = false;

    isDead = UBTreeItupGetXminXmax(page, offnum, oldestXmin, &xmin, &xmax,
        &xminCommitted, &xmaxCommitted);
    /*
    * INDEXTUPLE_DEAD: xmin invalid || xmax frozen
    * INDEXTUPLE_DELETED: xmax valid && xmax committed
    * INDEXTUPLE_ACTIVE: else
    */
    if (isDead) {
        /* record dead */
        prstate->nowdead[prstate->ndead] = offnum;
        prstate->ndead++;
        return false;
    }
    if (TransactionIdIsValid(xmax) && xmaxCommitted) {
        /* update xmax as new_prune_xid */
        if (!TransactionIdIsValid(prstate->new_prune_xid) || TransactionIdPrecedes(xmax, prstate->new_prune_xid))
            prstate->new_prune_xid = xmax;
    }

    /* when xmax is invalid, we treat the tuple as active */
    return !TransactionIdIsValid(xmax);
}

/*
 *  UBTreePagePruneExecute() -- Traverse the nowdead array, set the corresponding tuples as dead.
 */
void UBTreePagePruneExecute(Page page, OffsetNumber *nowdead, int ndead, IndexPruneState *prstate)
{
    OffsetNumber *offnum = NULL;
    int i;
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* Update all now-dead line pointers */
    offnum = nowdead;
    for (i = 0; i < ndead; i++) {
        OffsetNumber off = *offnum++;
        ItemId lp = PageGetItemId(page, off);
        /* NOTE: prstate is NULL iff in redo */
        if (prstate != NULL && ItemIdHasStorage(lp)) {
            IndexTuple itup = (IndexTuple)PageGetItem(page, lp);
            UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);

            TransactionId xid = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmax);
            if (TransactionIdIsNormal(xid)) {
                if (!TransactionIdIsValid(prstate->latestRemovedXid) ||
                    TransactionIdPrecedes(prstate->latestRemovedXid, xid)) {
                    /* update latestRemovedXid */
                    prstate->latestRemovedXid = xid;
                }
            }
        }

        ItemIdSetDead(lp);
    }
}

static int ItemOffCompare(const void* itemIdp1, const void* itemIdp2)
{
    /* Sort in decreasing itemoff order */
    return ((itemIdSort)itemIdp2)->itemoff - ((itemIdSort)itemIdp1)->itemoff;
}

/*
 *  UBTreePageRepairFragmentation() -- Repair the fragmentation in the page.
 *
 * On the basis of PageRepairFragmentation(Page page), we ignore redirected/unused, compact both
 * line pointers and index tuples.
 */
void UBTreePageRepairFragmentation(Page page)
{
    Offset pdLower = ((PageHeader)page)->pd_lower;
    Offset pdUpper = ((PageHeader)page)->pd_upper;
    Offset pdSpecial = ((PageHeader)page)->pd_special;

    WHITEBOX_TEST_STUB("UBTreePageRepairFragmentation", WhiteboxDefaultErrorEmit);

    if ((unsigned int)(pdLower) < SizeOfPageHeaderData || pdLower > pdUpper || pdUpper > pdSpecial ||
        pdSpecial > BLCKSZ || (unsigned int)(pdSpecial) != MAXALIGN(pdSpecial))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted page pointers: lower = %d, upper = %d, special = %d", pdLower, pdUpper, pdSpecial)));

    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    int nline = PageGetMaxOffsetNumber(page);
    int nstorage = 0;
    Size totallen = 0;

    itemIdSortData itemidbase[MaxIndexTuplesPerPage];
    itemIdSort itemidptr = itemidbase;

    for (int i = FirstOffsetNumber; i <= nline; i++) {
        ItemId lp = PageGetItemId(page, i);
        /* Include active and frozen tuples */
        if (ItemIdHasStorage(lp)) {
            nstorage++;
            itemidptr->offsetindex = nstorage;
            itemidptr->itemoff = ItemIdGetOffset(lp);
            itemidptr->olditemid = *lp;
            if (itemidptr->itemoff < (int)pdUpper || itemidptr->itemoff >= (int)pdSpecial)
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED), errmsg("corrupted item pointer: %d", itemidptr->itemoff)));
            itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
            totallen += itemidptr->alignedlen;
            itemidptr++;
        }
    }

    if (totallen > (Size)(pdSpecial - pdLower))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("corrupted item lengths: total %u, available space %d",
                       (unsigned int)totallen, pdSpecial - pdLower)));

    /* sort itemIdSortData array into decreasing itemoff order */
    qsort((char*)itemidbase, nstorage, sizeof(itemIdSortData), ItemOffCompare);

    /* compactify page and install new itemids */
    Offset upper = pdSpecial;

    itemidptr = itemidbase;
    for (int i = 0; i < nstorage; i++, itemidptr++) {
        ItemId lp = PageGetItemId(page, itemidptr->offsetindex);
        upper -= itemidptr->alignedlen;
        errno_t rc = memmove_s((char *)page + upper, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
                               itemidptr->alignedlen);
        securec_check(rc, "\0", "\0");
        *lp = itemidptr->olditemid;
        lp->lp_off = upper;
    }

    /* Mark the page as not containing any LP_DEAD items, which means vacuum is not needed */
    opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

    ((PageHeader)page)->pd_lower = GetPageHeaderSize(page) + nstorage * sizeof(ItemIdData);
    ((PageHeader)page)->pd_upper = upper;
}

/*
 *	UBTreeDoInsert() -- Handle insertion of a single index tuple in the tree.
 *
 *		This routine is called by the public interface routines, btbuild
 *		and btinsert.  By here, itup is filled in, including the TID.
 *
 *		If checkUnique is UNIQUE_CHECK_NO or UNIQUE_CHECK_PARTIAL, this
 *		will allow duplicates.	Otherwise (UNIQUE_CHECK_YES or
 *		UNIQUE_CHECK_EXISTING) it will throw error for a duplicate.
 *		For UNIQUE_CHECK_EXISTING we merely run the duplicate check, and
 *		don't actually insert.
 *
 *		The result value is only significant for UNIQUE_CHECK_PARTIAL
 *		it must be TRUE if the entry is known unique, else FALSE.
 *		(In the current implementation we'll also return TRUE after a
 *		successful UNIQUE_CHECK_YES or UNIQUE_CHECK_EXISTING call, but
 *		that's just a coding artifact.)
 */
bool UBTreeDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel)
{
    bool is_unique = false;
    BTScanInsert itupKey;
    BTStack stack = NULL;
    Buffer buf;
    OffsetNumber offset;
    Oid indexHeapRelOid = InvalidOid;
    Relation indexHeapRel = NULL;
    Partition part = NULL;
    Relation partRel = NULL;

    bool checkingunique = (checkUnique != UNIQUE_CHECK_NO);

    WHITEBOX_TEST_STUB("UBTreeDoInsert-begin", WhiteboxDefaultErrorEmit);
    if (rel == NULL || rel->rd_rel == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("relation or rd_rel is NULL")));
        return false;
    }
    /* we need an insertion scan key to do our search, so build one */
    itupKey = UBTreeMakeScanKey(rel, itup);
    GPIScanDesc gpiScan = NULL;

    if (RelationIsGlobalIndex(rel)) {
        Assert(RelationIsPartition(heapRel));
        GPIScanInit(&gpiScan);
        indexHeapRelOid = IndexGetRelation(rel->rd_id, false);
        Assert(OidIsValid(indexHeapRelOid));

        if (indexHeapRelOid == heapRel->grandparentId) {  // For subpartitiontable
            indexHeapRel = relation_open(indexHeapRelOid, AccessShareLock);
            Assert(RelationIsSubPartitioned(indexHeapRel));
            part = partitionOpen(indexHeapRel, heapRel->parentId, AccessShareLock);
            partRel = partitionGetRelation(indexHeapRel, part);
            gpiScan->parentRelation = partRel;
            partitionClose(indexHeapRel, part, AccessShareLock);
        } else { /* gpi of patitioned table */
            gpiScan->parentRelation = relation_open(heapRel->parentId, AccessShareLock);
        }
    }

    if (checkingunique) {
        if (!itupKey->anynullkeys) {
            /* No (heapkeyspace) scantid until uniqueness established */
            itupKey->scantid = NULL;
        } else {
            /*
             * Scan key for new tuple contains NULL key values.  Bypass
             * checkingunique steps.  They are unnecessary because core code
             * considers NULL unequal to every value, including NULL.
             *
             * This optimization avoids O(N^2) behavior within the
             * _bt_findinsertloc() heapkeyspace path when a unique index has a
             * large number of "duplicates" with NULL key values.
             */
            checkingunique = false;
            /* Tuple is unique in the sense that core code cares about */
            Assert(checkUnique != UNIQUE_CHECK_EXISTING);
            is_unique = true;
        }
    }

    /*
     * It's very common to have an index on an auto-incremented or
     * monotonically increasing value. In such cases, every insertion happens
     * towards the end of the index. We try to optimise that case by caching
     * the right-most leaf of the index. If our cached block is still the
     * rightmost leaf, has enough free space to accommodate a new entry and
     * the insertion key is strictly greater than the first key in this page,
     * then we can safely conclude that the new key will be inserted in the
     * cached block. So we simply search within the cached block and insert the
     * key at the appropriate location. We call it a fastpath.
     *
     * Testing has revealed, though, that the fastpath can result in increased
     * contention on the exclusive-lock on the rightmost leaf page. So we
     * conditionally check if the lock is available. If it's not available then
     * we simply abandon the fastpath and take the regular path. This makes
     * sense because unavailability of the lock also signals that some other
     * backend might be concurrently inserting into the page, thus reducing our
     * chances to finding an insertion place in this page.
     */
top:
    bool fastpath = false;
    offset = InvalidOffsetNumber;

    if (RelationGetTargetBlock(rel) != InvalidBlockNumber) {
        /*
         * Conditionally acquire exclusive lock on the buffer before doing any
         * checks. If we don't get the lock, we simply follow slowpath. If we
         * do get the lock, this ensures that the index state cannot change, as
         * far as the rightmost part of the index is concerned.
         */
        buf = ReadBuffer(rel, RelationGetTargetBlock(rel));
        if (ConditionalLockBuffer(buf)) {
            _bt_checkpage(rel, buf);

            Page page = BufferGetPage(buf);
            UBTPageOpaqueInternal lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            Size itemsz = MAXALIGN(IndexTupleSize(itup));

            /*
             * Check if the page is still the rightmost leaf page, has enough
             * free space to accommodate the new tuple, no split is in progress
             * and the scankey is greater than or equal to the first key on the
             * page.
             */
            if (P_ISLEAF(lpageop) && P_RIGHTMOST(lpageop) && !P_INCOMPLETE_SPLIT(lpageop) && !P_IGNORE(lpageop) &&
                (PageGetFreeSpace(page) > itemsz) && PageGetMaxOffsetNumber(page) >= P_FIRSTDATAKEY(lpageop) &&
                UBTreeCompare(rel, itupKey, page, P_FIRSTDATAKEY(lpageop), InvalidBuffer) > 0) {
                fastpath = true;
            } else {
                _bt_relbuf(rel, buf);

                /*
                 * Something did not workout. Just forget about the cached
                 * block and follow the normal path. It might be set again if
                 * the conditions are favourble.
                 */
                RelationSetTargetBlock(rel, InvalidBlockNumber);
            }
        } else {
            ReleaseBuffer(buf);

            /*
             * If someone's holding a lock, it's likely to change anyway,
             * so don't try again until we get an updated rightmost leaf.
             */
            RelationSetTargetBlock(rel, InvalidBlockNumber);
        }
    }

    if (!fastpath) {
        /* find the first page containing this key, buffer returned by _bt_search() is locked in exclusive mode */
        stack = UBTreeSearch(rel, itupKey, &buf, BT_WRITE);
    }

    WHITEBOX_TEST_STUB("UBTreeDoInsert-found", WhiteboxDefaultErrorEmit);

    /*
     * If we're not allowing duplicates, make sure the key isn't already in
     * the index.
     *
     * NOTE: obviously, _bt_check_unique can only detect keys that are already
     * in the index; so it cannot defend against concurrent insertions of the
     * same key.  We protect against that by means of holding a write lock on
     * the first page the value could be on, regardless of the value of its
     * implicit heap TID tiebreaker attribute.  Any other would-be inserter of
     * the same key must acquire a write lock on the same page, so only one
     * would-be inserter can be making the check at one time.  Furthermore,
     * once we are past the check we hold write locks continuously until we
     * have performed our insertion, so no later inserter can fail to see our
     * insertion.  (This requires some care in _bt_findinsertloc.)
     *
     * If we must wait for another xact, we release the lock while waiting,
     * and then must start over completely.
     *
     * For a partial uniqueness check, we don't wait for the other xact. Just
     * let the tuple in and return false for possibly non-unique, or true for
     * definitely unique.
     */
    if (checkingunique) {
        TransactionId xwait;

        offset = UBTreeBinarySearch(rel, itupKey, buf, true);
        xwait = UBTreeCheckUnique(rel, itup, heapRel, buf, offset, itupKey, checkUnique, &is_unique, gpiScan);
        if (unlikely(TransactionIdIsValid(xwait))) {
            /* Have to wait for the other guy ... */
            _bt_relbuf(rel, buf);
            buf = InvalidBuffer;
            XactLockTableWait(xwait);
            /* start over... */
            if (stack) {
                _bt_freestack(stack);
            }
            goto top;
        }

        /* Uniqueness is established -- restore heap tid as scantid */
        if (itupKey->heapkeyspace) {
            itupKey->scantid = &itup->t_tid;
        }
    }

    /* skip insertion when we just want to check existing or find a conflict when executing upsert */
    if (checkUnique != UNIQUE_CHECK_EXISTING && !(checkUnique == UNIQUE_CHECK_UPSERT && !is_unique)) {
        /*
         * The only conflict predicate locking cares about for indexes is when
         * an index tuple insert conflicts with an existing lock.  We don't
         * know the actual page we're going to insert to yet because scantid
         * was not filled in initially, but it's okay to use the "first valid"
         * page instead.  This reasoning also applies to INCLUDE indexes,
         * whose extra attributes are not considered part of the key space.
         */
        CheckForSerializableConflictIn(rel, NULL, buf);
        /* do the insertion */
        offset = UBTreeFindInsertLoc(rel, &buf, offset, itupKey, itup, checkingunique, stack, heapRel);
        UBTreeInsertOnPage(rel, itupKey, buf, InvalidBuffer, stack, itup, offset, false);
    } else {
        /* just release the buffer */
        _bt_relbuf(rel, buf);
    }


    /* be tidy */
    if (stack) {
        _bt_freestack(stack);
    }
    pfree(itupKey);

    if (gpiScan != NULL) { // means rel switch happened
        if (indexHeapRelOid == heapRel->grandparentId) {  // For subpartitiontable
            releaseDummyRelation(&partRel);
            relation_close(indexHeapRel, AccessShareLock);
        } else {
            relation_close(gpiScan->parentRelation, AccessShareLock);
        }
        GPIScanEnd(gpiScan);
    }

    WHITEBOX_TEST_STUB("UBTreeDoInsert-end", WhiteboxDefaultErrorEmit);

    return is_unique;
}

/*
 *	UBTreeCheckUnique() -- Check for violation of unique index constraint
 *
 * offset points to the first possible item that could conflict. It can
 * also point to end-of-page, which means that the first tuple to check
 * is the first tuple on the next page.
 *
 * Returns InvalidTransactionId if there is no conflict, else an xact ID
 * we must wait for to see if it commits a conflicting tuple.	If an actual
 * conflict is detected, no return --- just ereport().
 *
 * However, if checkUnique == UNIQUE_CHECK_PARTIAL, we always return
 * InvalidTransactionId because we don't want to wait.  In this case we
 * set *is_unique to false if there is a potential conflict, and the
 * core code must redo the uniqueness check later.
 */
static TransactionId UBTreeCheckUnique(Relation rel, IndexTuple itup, Relation heapRel, Buffer buf, OffsetNumber offset,
    BTScanInsert itup_key, IndexUniqueCheck checkUnique, bool* is_unique, GPIScanDesc gpiScan)
{
    SnapshotData SnapshotDirty;
    OffsetNumber maxoff;
    Page page;
    UBTPageOpaqueInternal opaque;
    Buffer nbuf = InvalidBuffer;
    bool found = false;
    Relation tarRel = heapRel;

    WHITEBOX_TEST_STUB("UBTreeCheckUnique", WhiteboxDefaultErrorEmit);

    /* Assume unique until we find a duplicate */
    *is_unique = true;

    InitDirtySnapshot(SnapshotDirty);

    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    maxoff = PageGetMaxOffsetNumber(page);

    /*
     * Scan over all equal tuples, looking for live conflicts.
     */
    Assert(!itup_key->anynullkeys);
    Assert(itup_key->scantid == NULL);
    for (;;) {
        ItemId curitemid;
        IndexTuple curitup;
        BlockNumber nblkno;

        /*
         * make sure the offset points to an actual item before trying to
         * examine it...
         */
        if (offset <= maxoff) {
            curitemid = PageGetItemId(page, offset);
            /*
             * We can skip items that are marked killed.
             *
             * Formerly, we applied _bt_isequal() before checking the kill
             * flag, so as to fall out of the item loop as soon as possible.
             * However, in the presence of heavy update activity an index may
             * contain many killed items with the same key; running
             * _bt_isequal() on each killed item gets expensive. Furthermore
             * it is likely that the non-killed version of each key appears
             * first, so that we didn't actually get to exit any sooner
             * anyway. So now we just advance over killed items as quickly as
             * we can. We only apply _bt_isequal() when we get to a non-killed
             * item or the end of the page.
             */
            if (!ItemIdIsDead(curitemid)) {
                ItemPointerData htid;

                /*
                 * _bt_compare returns 0 for (1,NULL) and (1,NULL) - this's
                 * how we handling NULLs - and so we must not use _bt_compare
                 * in real comparison, but only for ordering/finding items on
                 * pages. - vadim 03/24/97
                 */
                if (!UBTreeIsEqual(rel, page, offset, itup_key->keysz, itup_key->scankeys))
                    break; /* we're past all the equal tuples */

                /* okay, we gotta fetch the heap tuple ... */
                curitup = (IndexTuple) PageGetItem(page, curitemid);
                htid = curitup->t_tid;
                Oid curPartOid = InvalidOid;
                Datum datum;
                bool isNull;
                if (RelationIsGlobalIndex(rel)) {
                    datum = index_getattr(curitup, IndexRelationGetNumberOfAttributes(rel),
                                          RelationGetDescr(rel), &isNull);
                    curPartOid = DatumGetUInt32(datum);
                    Assert(isNull == false);
                    if (curPartOid != gpiScan->currPartOid) {
                        GPISetCurrPartOid(gpiScan, curPartOid);
                        if (!GPIGetNextPartRelation(gpiScan, CurrentMemoryContext, AccessShareLock)) {
                            if (CheckPartitionIsInvisible(gpiScan)) {
                                ItemIdMarkDead(curitemid);
                            }
                            opaque->btpo_flags |= BTP_HAS_GARBAGE;
                            if (nbuf != InvalidBuffer) {
                                MarkBufferDirtyHint(nbuf, true);
                            } else {
                                MarkBufferDirtyHint(buf, true);
                            }
                            goto next;
                        } else {
                            tarRel = gpiScan->fakePartRelation;
                        }
                    }
                }

                /*
                 * If we are doing a recheck, we expect to find the tuple we
                 * are rechecking.	It's not a duplicate, but we have to keep
                 * scanning. For global partition index, part oid in index tuple
                 * is supposed to be same as heapRel oid, add check in case
                 * abnormal condition.
                 */
                if (checkUnique == UNIQUE_CHECK_EXISTING && ItemPointerCompare(&htid, &itup->t_tid) == 0) {
                    if (RelationIsGlobalIndex(rel) && curPartOid != heapRel->rd_id) {
                        ereport(ERROR,
                                (errcode(ERRCODE_INDEX_CORRUPTED),
                                        errmsg("failed to re-find tuple within GPI \"%s\"",
                                               RelationGetRelationName(rel))));
                    }
                    found = true;
                    /*
                     * We check the whole HOT-chain to see if there is any tuple
                     * that satisfies SnapshotDirty.  This is necessary because we
                     * have just a single index entry for the entire chain.
                     */
                } else {
                    TransactionId xmin, xmax;
                    bool isdead = false;
                    bool xminCommitted = false;
                    bool xmaxCommitted = false;
                    bool xminVisible = false;
                    bool xmaxVisible = false;

                    isdead = UBTreeItupGetXminXmax(page, offset, InvalidTransactionId,
                                                   &xmin, &xmax, &xminCommitted, &xmaxCommitted);

                    /* here we guarantee the same semantics as UNIQUE_CHECK_PARTIAL */
                    if (IndexUniqueCheckNoError(checkUnique)) {
                        if (TransactionIdIsValid(xmin) && !TransactionIdIsValid(xmax)) {
                            if (nbuf != InvalidBuffer)
                                _bt_relbuf(rel, nbuf);
                            *is_unique = false;
                            return InvalidTransactionId;
                        }
                    }

                    xminVisible = xminCommitted || TransactionIdIsCurrentTransactionId(xmin);
                    xmaxVisible = xmaxCommitted || TransactionIdIsCurrentTransactionId(xmax);

                    /*
                     * By here, there are 3 cases:
                     *      1. conflict, when xmin is valid *and* xmax is not valid.
                     *      2. not conflict, when xmin is not valid *or* xmax is valid.
                     *      3. unknown, when xmin or xmax is in-progress.
                     *
                     * In case 3, we have to return the xid in order to tell the caller to wait for the xid.
                     */

                    if (TransactionIdIsValid(xmin) && xminVisible && !TransactionIdIsValid(xmax)) {
                        Datum values[INDEX_MAX_KEYS];
                        bool isnull[INDEX_MAX_KEYS];
                        char *key_desc = NULL;

                        index_deform_tuple(itup, RelationGetDescr(rel), values, isnull);

                        key_desc = BuildIndexValueDescription(rel, values, isnull);

                        /* save the primary scene once index relation of CU Descriptor
                         * violates unique constraint. this means wrong max cu id happens.
                         */
                        Assert(memcmp(NameStr(rel->rd_rel->relname), "pg_cudesc_", strlen("pg_cudesc_")) != 0);

                        ereport(ERROR,
                                (errcode(ERRCODE_UNIQUE_VIOLATION),
                                        errmsg("duplicate key value violates unique constraint \"%s\"",
                                               RelationGetRelationName(rel)),
                                        key_desc ? errdetail("Key %s already exists.", key_desc) : 0));
                    }

                    if (!TransactionIdIsValid(xmin) || (TransactionIdIsValid(xmax) && xmaxVisible))
                        goto next;

                    if (nbuf != InvalidBuffer)
                        _bt_relbuf(rel, nbuf);
                    return TransactionIdIsValid(xmax) ? xmax : xmin;
                }
            }
        }

        next:
        /*
         * Advance to next tuple to continue checking.
         */
        if (offset < maxoff)
            offset = OffsetNumberNext(offset);
        else {
            /* If scankey == hikey we gotta check the next page too */
            if (P_RIGHTMOST(opaque))
                break;
            int highkeycmp = UBTreeCompare(rel, itup_key, page, P_HIKEY, InvalidBuffer);
            Assert(highkeycmp <= 0);
            if (highkeycmp != 0) {
                break;
            }
            /* Advance to next non-dead page --- there must be one */
            for (;;) {
                nblkno = opaque->btpo_next;
                nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
                page = BufferGetPage(nbuf);
                opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
                if (!P_IGNORE(opaque))
                    break;
                if (P_RIGHTMOST(opaque))
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\"", RelationGetRelationName(rel))));
            }
            maxoff = PageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }

    /*
     * If we are doing a recheck then we should have found the tuple we are
     * checking.  Otherwise there's something very wrong --- probably, the
     * index is on a non-immutable expression.
     */
    if (checkUnique == UNIQUE_CHECK_EXISTING && !found)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to re-find tuple within index \"%s\"", RelationGetRelationName(rel)),
                errhint("This may be because of a non-immutable index expression.")));

    if (nbuf != InvalidBuffer)
        _bt_relbuf(rel, nbuf);

    return InvalidTransactionId;
}

/*
 *	UBTreeFindInsertLoc() -- Finds an insert location for a tuple
 *
 *	    On entry, *buf and *offsetptr point to the first legal position
 *		where the new tuple could be inserted.	The caller should hold an
 *		exclusive lock on *buf.  *offsetptr can also be set to
 *		InvalidOffsetNumber, in which case the function will search for the
 *		right location within the page if needed.
 *
 *	    If 'checkingunique' is true, the buffer on entry is the first page
 *		that contains duplicates of the new key.  If there are duplicates on
 *		multiple pages, the correct insertion position might be some page to
 *		the right, rather than the first page.  In that case, this function
 *		moves right to the correct target page.
 *
 *		If the new key is equal to one or more existing keys, we can
 *		legitimately place it anywhere in the series of equal keys --- in fact,
 *		if the new key is equal to the page's "high key" we can place it on
 *		the next page.	If it is equal to the high key, and there's not room
 *		to insert the new tuple on the current page without splitting, then
 *		we can move right hoping to find more free space and avoid a split.
 *		(We should not move right indefinitely, however, since that leads to
 *		O(N^2) insertion behavior in the presence of many equal keys.)
 *		Once we have chosen the page to put the key on, we'll insert it before
 *		any existing equal keys because of the way _bt_binsrch() works.
 *
 *		(In a !heapkeyspace index, there can be multiple pages with the same
 *		high key, where the new tuple could legitimately be placed on.  In
 *		that case, the caller passes the first page containing duplicates,
 *		just like when checkinunique=true.  If that page doesn't have enough
 *		room for the new tuple, this function moves right, trying to find a
 *		legal page that does.)
 *
 *		On exit, *buf and *offsetptr point to the chosen insert location. If
 *		_bt_findinsertloc decides to move right, the lock and pin on the
 *		original page will be released and the new page returned to the caller
 *		is exclusively locked instead.
 *
 *		If there is not enough room on the page for the new tuple, we try to
 *		make room by removing any LP_DEAD tuples. (Try pruning page in UStore's
 *		multi-version index)
 *
 *		newtup is the new tuple we're inserting, and scankey is an insertion
 *		type scan key for it.
 */
static OffsetNumber UBTreeFindInsertLoc(Relation rel, Buffer *bufptr, OffsetNumber firstlegaloff, BTScanInsert itup_key,
    IndexTuple newtup, bool checkingunique, BTStack stack, Relation heapRel)
{
    Buffer buf = *bufptr;
    Page page = BufferGetPage(buf);
    Size itemsz;
    UBTPageOpaqueInternal lpageop;
    bool movedright = false;
    bool pruned = false;

    lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    itemsz = IndexTupleDSize(*newtup);
    itemsz = MAXALIGN(itemsz); /* be safe, PageAddItem will do this but we
                                * need to be consistent */
    /* Check 1/3 of a page restriction */
    if (unlikely(itemsz > UBTMaxItemSize(page)))
        UBTreeCheckThirdPage(rel, heapRel, itup_key->heapkeyspace, page, newtup);

    Assert(P_ISLEAF(lpageop) && !P_INCOMPLETE_SPLIT(lpageop));
    Assert(!itup_key->heapkeyspace || itup_key->scantid != NULL);
    Assert(itup_key->heapkeyspace || itup_key->scantid == NULL);

    /* heapkeyspace always set in ubtree */
    Assert(itup_key->heapkeyspace);
    /*
     * If we're inserting into a unique index, we may have to walk right
     * through leaf pages to find the one leaf page that we must insert on
     * to.
     *
     * This is needed for checkingunique callers because a scantid was not
     * used when we called _bt_search().  scantid can only be set after
     * _bt_check_unique() has checked for duplicates.  The buffer
     * initially stored in insertstate->buf has the page where the first
     * duplicate key might be found, which isn't always the page that new
     * tuple belongs on.  The heap TID attribute for new tuple (scantid)
     * could force us to insert on a sibling page, though that should be
     * very rare in practice.
     */
    if (checkingunique) {
        while (!P_RIGHTMOST(lpageop) && UBTreeCompare(rel, itup_key, page, P_HIKEY, InvalidBuffer) > 0) {
            /* The new tuple belongs on the next page, try to step right */
            Buffer rbuf = InvalidBuffer;
            BlockNumber rblkno = lpageop->btpo_next;

            for (;;) {
                rbuf = _bt_relandgetbuf(rel, rbuf, rblkno, BT_WRITE);
                page = BufferGetPage(rbuf);
                lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
                /*
                 * If this page was incompletely split, finish the split now.
                 * We do this while holding a lock on the left sibling, which
                 * is not good because finishing the split could be a fairly
                 * lengthy operation.  But this should happen very seldom.
                 */
                if (P_INCOMPLETE_SPLIT(lpageop)) {
                    UBTreeFinishSplit(rel, rbuf, stack);
                    rbuf = InvalidBuffer;
                    continue;
                }
                if (!P_IGNORE(lpageop))
                    break;
                if (P_RIGHTMOST(lpageop))
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("fell off the end of index \"%s\"", RelationGetRelationName(rel))));

                rblkno = lpageop->btpo_next;
            }
            _bt_relbuf(rel, buf);
            buf = rbuf;
            movedright = true;
            pruned = false;
        }
    }

    /*
     * If the target page is full, see if we can obtain enough space by:
     *      (a) pruning this page if we have multi-version info.
     *      (b) erasing LP_DEAD items.
     */
    if (PageGetFreeSpace(page) < itemsz) {
        if (P_ISLEAF(lpageop)) {
            if (UBTreePagePruneOpt(rel, buf, false)) {
                /* just like vacuum, we mark pruned as true to find the newitemoff again */
                pruned = true;
            }
        }
    }

    /*
     * Now we are on the right page, so find the insert position. If we moved
     * right at all, we know we should insert at the start of the page. If we
     * didn't move right, we can use the firstlegaloff hint if the caller
     * supplied one, unless we pruned the page which might have moved tuples
     * around making the hint invalid.
     *
     * After we take TID as tie-breaker, tuples with the same key value need
     * to be sorted by TID. Since firstlegaloff does not consider TID, this
     * hint may not be accurate. We need to examine with _bt_compare() again,
     * because TID is already included in itup_key now (after checking unique).
     */

    *bufptr = buf;

    if (movedright) {
        /*
         * moving right means we may insert in the front of the page, but still
         * need to check carefully when itup_key->heapkeyspace.
         */
        firstlegaloff = P_FIRSTDATAKEY(lpageop);
    }

    /* shift xid-base if necessary */
    bool prunedByPrepareXid = IndexPagePrepareForXid(rel, page, GetCurrentTransactionId(), RelationNeedsWAL(rel), buf);
    pruned = pruned || prunedByPrepareXid;

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    if (firstlegaloff != InvalidOffsetNumber && !pruned &&
        (firstlegaloff > maxoff || UBTreeCompare(rel, itup_key, page, firstlegaloff, InvalidBuffer) <= 0)) {
        return firstlegaloff;
    }

    /* otherwise, we need to call _bt_binsrch() again with TID to find the insertion location */
    return UBTreeBinarySearch(rel, itup_key, buf, true);
}

/*
 * UBTreeInsertOnPage() -- Insert a tuple on a particular page in the index.
 *
 *      This recursive procedure does the following things
 *
 *      On entry, we must have the correct buffer in which to do the
 *      insertion, and the buffer must be pinned and write-locked.	On return,
 *      we will have dropped both the pin and the lock on the buffer.
 *
 *      When inserting to a non-leaf page, 'cbuf' is the left-sibling of the
 *      page we're inserting the downlink for.  This function will clear the
 *      INCOMPLETE_SPLIT flag on it, and release the buffer.
 *
 *      The locking interactions in this code are critical.  You should
 *      grok Lehman and Yao's paper before making any changes.  In addition,
 *      you need to understand how we disambiguate duplicate keys in this
 *      implementation, in order to be able to find our location using
 *      L&Y "move right" operations.  Since we may insert duplicate user
 *      keys, and since these dups may propagate up the tree, we use the
 *		'afteritem' parameter to position ourselves correctly for the
 *      insertion on internal pages.
 */
static void UBTreeInsertOnPage(Relation rel, BTScanInsert itup_key, Buffer buf, Buffer cbuf, BTStack stack,
    IndexTuple itup, OffsetNumber newitemoff, bool split_only_page)
{
    Page page;
    UBTPageOpaqueInternal lpageop;
    OffsetNumber firstright = InvalidOffsetNumber;
    Size itemsz;

    page = BufferGetPage(buf);
    lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* child buffer must be given iff inserting on an internal page */
    Assert(P_ISLEAF(lpageop) == !BufferIsValid(cbuf));
    /* tuple must have appropriate number of attributes */
    Assert(!P_ISLEAF(lpageop) ||
           UBTreeTupleGetNAtts(itup, rel) ==
           IndexRelationGetNumberOfAttributes(rel));
    Assert(P_ISLEAF(lpageop) ||
           UBTreeTupleGetNAtts(itup, rel) <=
           IndexRelationGetNumberOfKeyAttributes(rel));

    WHITEBOX_TEST_STUB("UBTreeInsertOnPage", WhiteboxDefaultErrorEmit);

    /*
     * Every internal page should have exactly one negative infinity item at
     * all times.  Only _bt_split() and _bt_newroot() should add items that
     * become negative infinity items through truncation, since they're the
     * only routines that allocate new internal pages.
     */
    Assert(P_ISLEAF(lpageop) || newitemoff > P_FIRSTDATAKEY(lpageop));

    /* The caller should've finished any incomplete splits already. */
    if (P_INCOMPLETE_SPLIT(lpageop)) {
        elog(ERROR, "cannot insert to incompletely split page %u", BufferGetBlockNumber(buf));
    }

    itemsz = IndexTupleDSize(*itup);
    itemsz = MAXALIGN(itemsz);
    /*
     * Do we need to split the page to fit the item on it?
     *
     * Note: PageGetFreeSpace() subtracts sizeof(ItemIdData) from its result,
     * so this comparison is correct even though we appear to be accounting
     * only for the item and not for its line pointer.
     */
    if (PageGetFreeSpace(page) < itemsz) {
        bool is_root = P_ISROOT(lpageop) > 0 ? true : false;
        bool is_only = P_LEFTMOST(lpageop) && P_RIGHTMOST(lpageop);
        bool newitemonleft = false;
        Buffer rbuf;
        /* Choose the split point */
        if (rel->rd_indexsplit == INDEXSPLIT_NO_INSERTPT) {
            /* Choose the split point */
            firstright = UBTreeFindsplitlocInsertpt(rel, page, newitemoff, itemsz, &newitemonleft, itup);
        } else {
            firstright = UBTreeFindsplitloc(rel, page, newitemoff, itemsz, &newitemonleft);
        }

        /* split the buffer into left and right halves */
        rbuf = UBTreeSplit(rel, buf, cbuf, firstright, newitemoff, itemsz, itup, newitemonleft, itup_key);
        PredicateLockPageSplit(rel, BufferGetBlockNumber(buf), BufferGetBlockNumber(rbuf));

        /* ----------
         * By here,
         *
         *		+  our target page has been split;
         *		+  the original tuple has been inserted;
         *		+  we have write locks on both the old (left half)
         *		   and new (right half) buffers, after the split; and
         *		+  we know the key we want to insert into the parent
         *		   (it's the "high key" on the left child page).
         *
         * We're ready to do the parent insertion.  We need to hold onto the
         * locks for the child pages until we locate the parent, but we can
         * release them before doing the actual insertion (see Lehman and Yao
         * for the reasoning).
         * ----------
         */
        UBTreeInsertParent(rel, buf, rbuf, stack, is_root, is_only);

        /* ubtree will try to recycle one page after allocate one page */
        UBTreeTryRecycleEmptyPage(rel);
    } else {
        Buffer metabuf = InvalidBuffer;
        Page metapg = NULL;
        BTMetaPageData *metad = NULL;
        OffsetNumber itup_off;
        BlockNumber itup_blkno;

        itup_off = newitemoff;
        itup_blkno = BufferGetBlockNumber(buf);

        /*
         * If we are doing this insert because we split a page that was the
         * only one on its tree level, but was not the root, it may have been
         * the "fast root".  We need to ensure that the fast root link points
         * at or above the current page.  We can safely acquire a lock on the
         * metapage here --- see comments for _bt_newroot().
         */
        if (split_only_page) {
            Assert(!P_ISLEAF(lpageop));

            metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
            metapg = BufferGetPage(metabuf);
            metad = BTPageGetMeta(metapg);
            if (metad->btm_fastlevel >= lpageop->btpo.level) {
                /* no update wanted */
                _bt_relbuf(rel, metabuf);
                metabuf = InvalidBuffer;
            }
        }

        /* Do the update.  No ereport(ERROR) until changes are logged */
        START_CRIT_SECTION();

        if (!UBTreePageAddTuple(page, itemsz, itup, newitemoff, true))
            ereport(PANIC,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add new item to block %u in index \"%s\"",
                                                              itup_blkno, RelationGetRelationName(rel))));

        /* update active hint */
        lpageop->activeTupleCount++;
        MarkBufferDirty(buf);

        if (BufferIsValid(metabuf)) {
            metad->btm_fastroot = itup_blkno;
            rel->rd_rootcache = InvalidBuffer;
            metad->btm_fastlevel = lpageop->btpo.level;
            MarkBufferDirty(metabuf);
        }

        /* clear INCOMPLETE_SPLIT flag on child if inserting a downlink */
        if (BufferIsValid(cbuf)) {
            Page cpage = BufferGetPage(cbuf);
            UBTPageOpaqueInternal cpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(cpage);
            Assert(P_INCOMPLETE_SPLIT(cpageop));
            cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
            MarkBufferDirty(cbuf);
        }

        /* XLOG stuff */
        if (RelationNeedsWAL(rel)) {
            xl_btree_insert xlrec;
            xl_btree_metadata xlmeta;
            uint8 xlinfo;
            XLogRecPtr recptr;
            IndexTupleData trunctuple;

            xlrec.offnum = itup_off;

            XLogBeginInsert();
            XLogRegisterData((char *)&xlrec, SizeOfBtreeInsert);

            if (P_ISLEAF(lpageop)) {
                xlinfo = XLOG_UBTREE_INSERT_LEAF;

                /*
                 * Cache the block information if we just inserted into the
                 * rightmost leaf page of the index.
                */
                if (P_RIGHTMOST(lpageop)) {
                    RelationSetTargetBlock(rel, BufferGetBlockNumber(buf));
                }
            } else {
                /*
                 * Register the left child whose INCOMPLETE_SPLIT flag was
                 * cleared.
                 */
                XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);
                xlinfo = XLOG_UBTREE_INSERT_UPPER;
            }

            if (BufferIsValid(metabuf)) {
                xlmeta.root = metad->btm_root;
                xlmeta.level = metad->btm_level;
                xlmeta.fastroot = metad->btm_fastroot;
                xlmeta.fastlevel = metad->btm_fastlevel;

                XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
                XLogRegisterBufData(2, (char *)&xlmeta, sizeof(xl_btree_metadata));
                xlinfo = XLOG_UBTREE_INSERT_META;
            }

            /* Read comments in _bt_pgaddtup */
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            if (!P_ISLEAF(lpageop) && newitemoff == P_FIRSTDATAKEY(lpageop)) {
                trunctuple = *itup;
                trunctuple.t_info = sizeof(IndexTupleData);
                XLogRegisterBufData(0, (char*)&trunctuple, sizeof(IndexTupleData));
            } else {
                ItemId iid = PageGetItemId(page, itup_off);
                XLogRegisterBufData(0, (char*)PageGetItem(page, iid), itemsz);
            }

            recptr = XLogInsert(RM_UBTREE_ID, xlinfo);

            if (BufferIsValid(metabuf)) {
                PageSetLSN(metapg, recptr);
            }
            if (BufferIsValid(cbuf)) {
                PageSetLSN(BufferGetPage(cbuf), recptr);
            }
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        /* release buffers */
        if (BufferIsValid(metabuf)) {
            _bt_relbuf(rel, metabuf);
        }
        if (BufferIsValid(cbuf)) {
            _bt_relbuf(rel, cbuf);
        }
        _bt_relbuf(rel, buf);
    }
}

/*
 *	UBTreeSplit() -- split a page in the btree.
 *
 *		On entry, buf is the page to split, and is pinned and write-locked.
 *		firstright is the item index of the first item to be moved to the
 *		new right page.  newitemoff etc. tell us about the new item that
 *		must be inserted along with the data from the old page.
 *
 *		itup_key is used for suffix truncation on leaf pages (internal
 *		page callers pass NULL).  When splitting a non-leaf page, 'cbuf'
 *		is the left-sibling of the page we're inserting the downlink for.
 *		This function will clear the INCOMPLETE_SPLIT flag on it, and
 *		release the buffer.
 *
 *		Returns the new right sibling of buf, pinned and write-locked.
 *		The pin and lock on buf are maintained.
 */
static Buffer UBTreeSplit(Relation rel, Buffer buf, Buffer cbuf, OffsetNumber firstrightoff, OffsetNumber newitemoff,
    Size newitemsz, IndexTuple newitem, bool newitemonleft, BTScanInsert itup_key)
{
    Buffer rbuf;
    Page origpage;
    Page leftpage, rightpage;
    BlockNumber origpagenumber, rightpagenumber;
    UBTPageOpaqueInternal ropaque, lopaque, oopaque;
    Buffer sbuf = InvalidBuffer;
    Page spage = NULL;
    UBTPageOpaqueInternal sopaque = NULL;
    Size itemsz;
    ItemId itemid;
    IndexTuple item;
    OffsetNumber leftoff, rightoff, newitemleftoff = InvalidOffsetNumber;
    OffsetNumber maxoff;
    OffsetNumber i;
    bool isroot = false;
    bool isleaf = false;
    errno_t rc;
    IndexTuple firstright, lefthighkey;

    WHITEBOX_TEST_STUB("UBTreeSplit", WhiteboxDefaultErrorEmit);

    /*
     * Acquire a new page to split into.
     *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
     *            before we release the Exclusive lock.
     */
    UBTRecycleQueueAddress addr;
    rbuf = UBTreeGetNewPage(rel, &addr);

    /*
     * origpage is the original page to be split.  leftpage is a temporary
     * buffer that receives the left-sibling data, which will be copied back
     * into origpage on success.  rightpage is the new page that receives the
     * right-sibling data.	If we fail before reaching the critical section,
     * origpage hasn't been modified and leftpage is only workspace. In
     * principle we shouldn't need to worry about rightpage either, because it
     * hasn't been linked into the btree page structure; but to avoid leaving
     * possibly-confusing junk behind, we are careful to rewrite rightpage as
     * zeroes before throwing any error.
     */
    origpage = BufferGetPage(buf);
    leftpage = PageGetTempPage(origpage);
    rightpage = BufferGetPage(rbuf);

    origpagenumber = BufferGetBlockNumber(buf);
    rightpagenumber = BufferGetBlockNumber(rbuf);

    UBTreePageInit(leftpage, BufferGetPageSize(buf));
    /* rightpage was already initialized by _bt_getbuf
     * Copy the original page's LSN into leftpage, which will become the
     * updated version of the page.  We need this because XLogInsert will
     * examine the LSN and possibly dump it in a page image.
     */
    PageSetLSN(leftpage, PageGetLSN(origpage));

    /* init btree private data */
    oopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(origpage);
    lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(leftpage);
    ropaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rightpage);

    isroot = P_ISROOT(oopaque) > 0 ? true : false;
    isleaf = P_ISLEAF(oopaque) > 0 ? true : false;

    /* if we're splitting this page, it won't be the root when we're done */
    /* also, clear the SPLIT_END and HAS_GARBAGE flags in both pages */
    lopaque->btpo_flags = oopaque->btpo_flags;
    lopaque->btpo_flags &= ~(BTP_ROOT | BTP_SPLIT_END | BTP_HAS_GARBAGE);
    ropaque->btpo_flags = lopaque->btpo_flags;
    /* set flag in left page indicating that the right page has no downlink */
    lopaque->btpo_flags |= BTP_INCOMPLETE_SPLIT;
    lopaque->btpo_prev = oopaque->btpo_prev;
    lopaque->btpo_next = rightpagenumber;
    ropaque->btpo_prev = origpagenumber;
    ropaque->btpo_next = oopaque->btpo_next;
    lopaque->btpo.level = ropaque->btpo.level = oopaque->btpo.level;
    /* Since we already have write-lock on both pages, ok to read cycleid */
    lopaque->btpo_cycleid = _bt_vacuum_cycleid(rel);
    ropaque->btpo_cycleid = lopaque->btpo_cycleid;
    /* copy other fields */
    lopaque->last_delete_xid = oopaque->last_delete_xid;
    ropaque->last_delete_xid = oopaque->last_delete_xid;
    lopaque->xid_base = oopaque->xid_base;
    ropaque->xid_base = oopaque->xid_base;
    /* reset the active hint, update later */
    lopaque->activeTupleCount = 0;
    ropaque->activeTupleCount = 0;

    /*
     * If the page we're splitting is not the rightmost page at its level in
     * the tree, then the first entry on the page is the high key for the
     * page.  We need to copy that to the right half.  Otherwise (meaning the
     * rightmost page case), all the items on the right half will be user
     * data.
     */
    rightoff = P_HIKEY;

    if (!P_RIGHTMOST(oopaque)) {
        itemid = PageGetItemId(origpage, P_HIKEY);
        itemsz = ItemIdGetLength(itemid);
        item = (IndexTuple)PageGetItem(origpage, itemid);
        Assert(UBTreeTupleGetNAtts(item, rel) > 0);
        Assert(UBTreeTupleGetNAtts(item, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
        if (PageAddItem(rightpage, (Item)item, itemsz, rightoff, false, false) == InvalidOffsetNumber) {
            rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
            securec_check(rc, "", "");
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to add hikey to the right sibling while splitting block %u of index \"%s\"",
                           origpagenumber, RelationGetRelationName(rel))));
        }
        rightoff = OffsetNumberNext(rightoff);
    }

    /*
     * The "high key" for the new left page will be the first key that's going
     * to go into the new right page, or possibly a truncated version if this
     * is a leaf page split.  This might be either the existing data item at
     * position firstright, or the incoming tuple.
     *
     * The high key for the left page is formed using the first item on the
     * right page, which may seem to be contrary to Lehman & Yao's approach of
     * using the left page's last item as its new high key when splitting on
     * the leaf level.  It isn't, though: suffix truncation will leave the
     * left page's high key fully equal to the last item on the left page when
     * two tuples with equal key values (excluding heap TID) enclose the split
     * point.  It isn't actually necessary for a new leaf high key to be equal
     * to the last item on the left for the L&Y "subtree" invariant to hold.
     * It's sufficient to make sure that the new leaf high key is strictly
     * less than the first item on the right leaf page, and greater than or
     * equal to (not necessarily equal to) the last item on the left leaf
     * page.
     *
     * In other words, when suffix truncation isn't possible, L&Y's exact
     * approach to leaf splits is taken.  (Actually, even that is slightly
     * inaccurate.  A tuple with all the keys from firstright but the heap TID
     * from lastleft will be used as the new high key, since the last left
     * tuple could be physically larger despite being opclass-equal in respect
     * of all attributes prior to the heap TID attribute.)
     */
    bool itup_extended = false;
    if (!newitemonleft && newitemoff == firstrightoff) {
        /* incoming tuple will become first on right page */
        itemsz = newitemsz;
        firstright = newitem;
        /*
         * we have expanded the itup size by 8B in multi-version index to represent the
         * space it actually occupies. When the itup is written to the page, the 8B is
         * restored.
         *
         * if we choose newitem as firstright here, it may be copied as HIKEY. So we need
         * to remind the 8B for this case.
         */
        itup_extended = isleaf; /* expanded only in multi-version index */
    } else {
        /* existing item at firstright will become first on right page */
        itemid = PageGetItemId(origpage, firstrightoff);
        firstright = (IndexTuple)PageGetItem(origpage, itemid);
        itemsz = IndexTupleSize(firstright);
    }

    /*
     * Truncate unneeded key and non-key attributes of the high key item
     * before inserting it on the left page.  This can only happen at the leaf
     * level, since in general all pivot tuple values originate from leaf
     * level high keys.  A pivot tuple in a grandparent page must guide a
     * search not only to the correct parent page, but also to the correct
     * leaf page.
     */
    if (isleaf) {
        IndexTuple lastleft;

        /*
         * Determine which tuple will become the last on the left page.  This
         * is needed to decide how many attributes from the first item on the
         * right page must remain in new high key for left page.
         */
        if (newitemonleft && newitemoff == firstrightoff) {
            /* incoming tuple becomes lastleft */
            lastleft = newitem;
        } else {
            OffsetNumber lastleftoff;

            /* existing item before firstright becomes lastleft */
            lastleftoff = OffsetNumberPrev(firstrightoff);
            Assert(lastleftoff >= P_FIRSTDATAKEY(oopaque));
            itemid = PageGetItemId(origpage, lastleftoff);
            lastleft = (IndexTuple) PageGetItem(origpage, itemid);
        }

        /* itup_key is NULL only when inserting parent */
        if (itup_key == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("itup_key for truncate is NULL")));
            return InvalidBuffer;
        }
        lefthighkey = UBTreeTruncate(rel, lastleft, firstright, itup_key, itup_extended);
        itemsz = IndexTupleSize(lefthighkey);
        itemsz = MAXALIGN(itemsz);
    } else {
        /*
         * Don't perform suffix truncation on a copy of firstright to make
         * left page high key for internal page splits.  Must use firstright
         * as new high key directly.
         *
         * Each distinct separator key value originates as a leaf level high
         * key; all other separator keys/pivot tuples are copied from one
         * level down.  A separator key in a grandparent page must be
         * identical to high key in rightmost parent page of the subtree to
         * its left, which must itself be identical to high key in rightmost
         * child page of that same subtree (this even applies to separator
         * from grandparent's high key).  There must always be an unbroken
         * "seam" of identical separator keys that guide index scans at every
         * level, starting from the grandparent.  That's why suffix truncation
         * is unsafe here.
         *
         * Internal page splits will truncate firstright into a "negative
         * infinity" data item when it gets inserted on the new right page
         * below, though.  This happens during the call to _bt_pgaddtup() for
         * the new first data item for right page.  Do not confuse this
         * mechanism with suffix truncation.  It is just a convenient way of
         * implementing page splits that split the internal page "inside"
         * firstright.  The lefthighkey separator key cannot appear a second
         * time in the right page (only firstright's downlink goes in right
         * page).
         */
        lefthighkey = firstright;
    }

    /*
     * Add new high key to leftpage
     */
    leftoff = P_HIKEY;

    Assert(UBTreeTupleGetNAtts(lefthighkey, rel) > 0);
    Assert(UBTreeTupleGetNAtts(lefthighkey, rel) <= IndexRelationGetNumberOfKeyAttributes(rel));
    Assert(itemsz == MAXALIGN(IndexTupleSize(lefthighkey)));


    if (PageAddItem(leftpage, (Item)lefthighkey, itemsz, leftoff, false, false) == InvalidOffsetNumber) {
        rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
        securec_check(rc, "", "");
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add hikey to the left sibling while splitting block %u of index \"%s\"",
                               origpagenumber,
                               RelationGetRelationName(rel))));
    }
    leftoff = OffsetNumberNext(leftoff);

    /* be tidy */
    if (lefthighkey != firstright) {
        /* this means the lefthighkey is copied from first right and be truncated */
        pfree(lefthighkey);
    }

    /*
     * Now transfer all the data items to the appropriate page.
     *
     * Note: we *must* insert at least the right page's items in item-number
     * order, for the benefit of _bt_restore_page().
     */
    maxoff = PageGetMaxOffsetNumber(origpage);

    for (i = P_FIRSTDATAKEY(oopaque); i <= maxoff; i = OffsetNumberNext(i)) {
        itemid = PageGetItemId(origpage, i);
        itemsz = ItemIdGetLength(itemid);
        item = (IndexTuple)PageGetItem(origpage, itemid);

        /* does new item belong before this one? */
        if (i == newitemoff) {
            if (newitemonleft) {
                newitemleftoff = leftoff;
                if (!UBTreePageAddTuple(leftpage, newitemsz, newitem, leftoff, true)) {
                    rc = memset_s(leftpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                    securec_check(rc, "", "");
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add new item to the left sibling while splitting block %u of index \"%s\"",
                            origpagenumber, RelationGetRelationName(rel))));
                }
                leftoff = OffsetNumberNext(leftoff);
                /* update active hint */
                lopaque->activeTupleCount++;
            } else {
                if (!UBTreePageAddTuple(rightpage, newitemsz, newitem, rightoff, true)) {
                    rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                    securec_check(rc, "", "");
                    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("failed to add new item to the right sibling while splitting block %u of index \"%s\"",
                            origpagenumber, RelationGetRelationName(rel))));
                }
                rightoff = OffsetNumberNext(rightoff);
                /* update active hint */
                ropaque->activeTupleCount++;
            }
        }

        /* decide which page to put it on */
        if (i < firstrightoff) {
            if (!UBTreePageAddTuple(leftpage, itemsz, item, leftoff, false)) {
                rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                securec_check(rc, "", "");
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to add old item to the left sibling while splitting block %u of index \"%s\"",
                        origpagenumber, RelationGetRelationName(rel))));
            }
            leftoff = OffsetNumberNext(leftoff);
            if (isleaf && !TransactionIdIsValid(((UstoreIndexXid)UstoreIndexTupleGetXid(item))->xmax))
                lopaque->activeTupleCount++; /* not deleted, count as active */
        } else {
            if (!UBTreePageAddTuple(rightpage, itemsz, item, rightoff, false)) {
                rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
                securec_check(rc, "", "");
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to add old item to the right sibling while splitting block %u of index \"%s\"",
                        origpagenumber, RelationGetRelationName(rel))));
            }
            rightoff = OffsetNumberNext(rightoff);
            if (isleaf && !TransactionIdIsValid(((UstoreIndexXid)UstoreIndexTupleGetXid(item))->xmax))
                ropaque->activeTupleCount++; /* not deleted, count as active */
        }
    }


    /* cope with possibility that newitem goes at the end */
    if (i <= newitemoff) {
        /*
         * Can't have newitemonleft here; that would imply we were told to put
         * *everything* on the left page, which cannot fit (if it could, we'd
         * not be splitting the page).
         */
        Assert(!newitemonleft);
        if (!UBTreePageAddTuple(rightpage, newitemsz, newitem, rightoff, true)) {
            rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
            securec_check(rc, "", "");
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add new item to the right sibling while splitting block %u of index \"%s\"",
                    origpagenumber, RelationGetRelationName(rel))));
        }
        rightoff = OffsetNumberNext(rightoff);
        /* update active hint */
        ropaque->activeTupleCount++;
    }

    /*
     * We have to grab the right sibling (if any) and fix the prev pointer
     * there. We are guaranteed that this is deadlock-free since no other
     * writer will be holding a lock on that page and trying to move left, and
     * all readers release locks on a page before trying to fetch its
     * neighbors.
     */
    if (!P_RIGHTMOST(oopaque)) {
        sbuf = _bt_getbuf(rel, oopaque->btpo_next, BT_WRITE);
        spage = BufferGetPage(sbuf);
        sopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(spage);
        if (sopaque->btpo_prev != origpagenumber) {
            rc = memset_s(rightpage, BLCKSZ, 0, BufferGetPageSize(rbuf));
            securec_check(rc, "", "");

            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("right sibling's left-link doesn't match: block %u links to %u instead of expected %u in "
                    "index \"%s\"",
                    oopaque->btpo_next, sopaque->btpo_prev, origpagenumber, RelationGetRelationName(rel))));
        }

        /*
         * Check to see if we can set the SPLIT_END flag in the right-hand
         * split page; this can save some I/O for vacuum since it need not
         * proceed to the right sibling.  We can set the flag if the right
         * sibling has a different cycleid: that means it could not be part of
         * a group of pages that were all split off from the same ancestor
         * page.  If you're confused, imagine that page A splits to A B and
         * then again, yielding A C B, while vacuum is in progress.  Tuples
         * originally in A could now be in either B or C, hence vacuum must
         * examine both pages.	But if D, our right sibling, has a different
         * cycleid then it could not contain any tuples that were in A when
         * the vacuum started.
         */
        if (sopaque->btpo_cycleid != ropaque->btpo_cycleid)
            ropaque->btpo_flags |= BTP_SPLIT_END;
    }

    /*
     * Right sibling is locked, new siblings are prepared, but original page
     * is not updated yet.
     *
     * NO EREPORT(ERROR) till right sibling is updated.  We can get away with
     * not starting the critical section till here because we haven't been
     * scribbling on the original page yet; see comments above.
     */
    START_CRIT_SECTION();

    /*
     * By here, the original data page has been split into two new halves, and
     * these are correct.  The algorithm requires that the left page never
     * move during a split, so we copy the new left page back on top of the
     * original.  Note that this is not a waste of time, since we also require
     * (in the page management code) that the center of a page always be
     * clean, and the most efficient way to guarantee this is just to compact
     * the data by reinserting it into a new left page.  (XXX the latter
     * comment is probably obsolete; but in any case it's good to not scribble
     * on the original page until we enter the critical section.)
     *
     * We need to do this before writing the WAL record, so that XLogInsert
     * can WAL log an image of the page if necessary.
     */
    PageRestoreTempPage(leftpage, origpage);
    /* leftpage, lopaque must not be used below here */
    MarkBufferDirty(buf);
    MarkBufferDirty(rbuf);

    if (!P_RIGHTMOST(ropaque)) {
        sopaque->btpo_prev = rightpagenumber;
        MarkBufferDirty(sbuf);
    }

    /*
     * Clear INCOMPLETE_SPLIT flag on child if inserting the new item finishes
     * a split.
     */
    if (!isleaf) {
        Page cpage = BufferGetPage(cbuf);
        UBTPageOpaqueInternal cpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(cpage);

        cpageop->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
        MarkBufferDirty(cbuf);
    }


    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_ubtree_split xlrec;
        uint8 xlinfo;
        XLogRecPtr recptr;

        xlrec.level = ropaque->btpo.level;
        xlrec.firstright = firstrightoff;
        xlrec.newitemoff = newitemoff;
        xlrec.opaqueVersion = UBTREE_OPAQUE_VERSION_RCR;
        xlrec.lopaque = *(UBTPageOpaqueInternal)PageGetSpecialPointer(origpage);
        xlrec.ropaque = *ropaque;

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfUBtreeSplit);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
        XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
        /* Log the right sibling, because we've changed its' prev-pointer. */
        if (!P_RIGHTMOST(ropaque)) {
            XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
        }

        if (BufferIsValid(cbuf)) {
            XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);
        }

        /*
         * Log the new item, if it was inserted on the left page. (If it was
         * put on the right page, we don't need to explicitly WAL log it
         * because it's included with all the other items on the right page.)
         * Show the new item as belonging to the left page buffer, so that it
         * is not stored if XLogInsert decides it needs a full-page image of
         * the left page.  We store the offset anyway, though, to support
         * archive compression of these records.
         */
        if (newitemonleft) {
            Assert(newitemleftoff != InvalidOffsetNumber);
            ItemId iid = PageGetItemId(origpage, newitemleftoff);
            IndexTuple itup = (IndexTuple)PageGetItem(origpage, iid);
            XLogRegisterBufData(0, (char*)itup, ItemIdGetLength(iid));
        }

        /* Log the left page's new high key */
        itemid = PageGetItemId(origpage, P_HIKEY);
        item = (IndexTuple)PageGetItem(origpage, itemid);
        XLogRegisterBufData(0, (char*)item, MAXALIGN(IndexTupleSize(item)));

        /*
         * Log the contents of the right page in the format understood by
         * _bt_restore_page(). We set lastrdata->buffer to InvalidBuffer,
         * because we're going to recreate the whole page anyway, so it should
         * never be stored by XLogInsert.
         *
         * Direct access to page is not good but faster - we should implement
         * some new func in page API.  Note we only store the tuples
         * themselves, knowing that they were inserted in item-number order
         * and so the item pointers can be reconstructed.
         * See comments for _bt_restore_page().
         */
        XLogRegisterBufData(1, (char *)rightpage + ((PageHeader)rightpage)->pd_upper,
                            ((PageHeader)rightpage)->pd_special - ((PageHeader)rightpage)->pd_upper);

        if (isroot) {
            xlinfo = newitemonleft ? XLOG_UBTREE_SPLIT_L_ROOT : XLOG_UBTREE_SPLIT_R_ROOT;
        } else {
            xlinfo = newitemonleft ? XLOG_UBTREE_SPLIT_L : XLOG_UBTREE_SPLIT_R;
        }
        recptr = XLogInsert(RM_UBTREE_ID, xlinfo | BTREE_SPLIT_OPAQUE_FLAG);

        PageSetLSN(origpage, recptr);
        PageSetLSN(rightpage, recptr);
        if (!P_RIGHTMOST(ropaque)) {
            PageSetLSN(spage, recptr);
        }
        if (!isleaf) {
            PageSetLSN(BufferGetPage(cbuf), recptr);
        }
    }

    END_CRIT_SECTION();

    /* discard this page from the Recycle Queue */
    UBTreeRecordUsedPage(rel, addr);

    /* release the old right sibling */
    if (!P_RIGHTMOST(ropaque)) {
        _bt_relbuf(rel, sbuf);
    }
    /* release the child */
    if (!isleaf) {
        _bt_relbuf(rel, cbuf);
    }

    /* split's done */
    return rbuf;
}

/*
 * UBTreeDoDelete() -- Handle deletion of a single index tuple in the tree.
 */
bool UBTreeDoDelete(Relation rel, IndexTuple itup)
{
    bool found = true;
    bool retried = false;
    BTScanInsert itupKey;
    Buffer buf;
    OffsetNumber offset;

    /* we need an insertion scan key to do our search, so build one */
    itupKey = UBTreeMakeScanKey(rel, itup);

    /* find the first page containing this key */
    (void)UBTreeSearch(rel, itupKey, &buf, BT_WRITE, false);

retry:
    /* looking for the first item >= scankey */
    offset = UBTreeBinarySearch(rel, itupKey, buf, true);

    WHITEBOX_TEST_STUB("UBTreeDoDelete-found", WhiteboxDefaultErrorEmit);

    /* looking for itup with the same ctid */
    offset = UBTreeFindDeleteLoc(rel, &buf, offset, itupKey, itup);
    if (offset == InvalidOffsetNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to delete index tuple in \"%s\"", RelationGetRelationName(rel))));
    }

    /* shift xid-base if necessary */
    Page page = BufferGetPage(buf);
    bool pruned = IndexPagePrepareForXid(rel, page, GetCurrentTransactionId(), RelationNeedsWAL(rel), buf);
    if (pruned) {
        if (retried) {
            ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("failed to delete index tuple in \"%s\"", RelationGetRelationName(rel))));
        }
        retried = true;
        goto retry;
    }

    UBTreeDeleteOnPage(rel, buf, offset);
    /* be tidy */
    pfree(itupKey);

    return found;
}

/*
 *	UBTreeFindDeleteLoc() -- Finds the location of the given tuple
 *
 *      We consider the location is right when we found a tuple whose t_tid equals
 *      the target, and the xmax is invalid (or aborted).
 *
 *      Currently, we find the first tuple with the same key by _bt_binsrch(),
 *      but we need to go through all the equal tuples to find the tuple with
 *      same t_tid. This can be optimized by sort by key and t_tid when inserting
 *      and comparing.
 *
 *      If we found a tuple whose xmin is aborted, we will mark it as dead.
 */
static OffsetNumber UBTreeFindDeleteLoc(Relation rel, Buffer* bufP, OffsetNumber offset,
    BTScanInsert itup_key, IndexTuple itup)
{
    OffsetNumber maxoff;
    Page page;
    UBTPageOpaqueInternal opaque;
    TransactionId xmin, xmax;

    WHITEBOX_TEST_STUB("UBTreeFindDeleteLoc", WhiteboxDefaultErrorEmit);

    page = BufferGetPage(*bufP);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    maxoff = PageGetMaxOffsetNumber(page);

    AttrNumber partitionOidAttr = IndexRelationGetNumberOfAttributes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);
    Oid partOid = InvalidOid;
    bool isnull = false;

    if (RelationIsGlobalIndex(rel)) {
        partOid = DatumGetUInt32(index_getattr(itup, partitionOidAttr, tupdesc, &isnull));
        Assert(!isnull);
    }

    /*
     * Scan over all equal tuples, looking for itup.
     */
    for (;;) {
        ItemId curitemid;
        IndexTuple curitup;
        BlockNumber nblkno;

        if (offset <= maxoff) {
            curitemid = PageGetItemId(page, offset);
            if (!ItemIdIsDead(curitemid)) {
                /*
                 * _bt_compare returns 0 for (1,NULL) and (1,NULL) - this's
                 * how we handling NULLs - and so we must not use _bt_compare
                 * in real comparison, but only for ordering/finding items on
                 * pages. - vadim 03/24/97
                 */
                if (!UBTreeIsEqual(rel, page, offset, itup_key->keysz, itup_key->scankeys))
                    /* we've traversed all the equal tuples, but we haven't found itup */
                    goto ret;

                curitup = (IndexTuple)PageGetItem(page, curitemid);

                bool isDead = false;
                bool xminCommitted = false;
                bool xmaxCommitted = false;
                isDead = UBTreeItupGetXminXmax(page, offset, InvalidTransactionId,
                                               &xmin, &xmax, &xminCommitted, &xmaxCommitted);

                if (!isDead && ItemPointerEquals(&itup->t_tid, &curitup->t_tid) && !TransactionIdIsValid(xmax)) {
                    /* check partOid match for global index */
                    if (RelationIsGlobalIndex(rel)) {
                        Oid curPartOid = DatumGetUInt32(index_getattr(curitup, partitionOidAttr, tupdesc, &isnull));
                        Assert(!isnull);

                        if (partOid == curPartOid)
                            return offset;
                    } else {
                        /* t_tid is correct, and there is no xmax on it, we found it */
                        return offset;
                    }
                }
            }
        }

        /*
         * Advance to next tuple to continue checking.
         */
        if (offset < maxoff)
            offset = OffsetNumberNext(offset);
        else {
            int highkeycmp;

            /* If scankey == hikey we gotta check the next page too */
            if (P_RIGHTMOST(opaque))
                break;

            highkeycmp = UBTreeCompare(rel, itup_key, page, P_HIKEY, InvalidBuffer);
            Assert(highkeycmp <= 0);
            if (highkeycmp != 0) {
                break;
            }
            /* Advance to next non-dead page --- there must be one */
            for (;;) {
                nblkno = opaque->btpo_next;
                *bufP = _bt_relandgetbuf(rel, *bufP, nblkno, BT_WRITE);
                page = BufferGetPage(*bufP);
                opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
                if (!P_IGNORE(opaque))
                    break;
                if (P_RIGHTMOST(opaque))
                    ereport(ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED),
                                    errmsg("fell off the end of index \"%s\"", RelationGetRelationName(rel))));
            }
            maxoff = PageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }
    /* No corresponding itup was found, release buffer and return InvalidOffsetNumber */
    ret:
    if (*bufP != InvalidBuffer)
        _bt_relbuf(rel, *bufP);
    return InvalidOffsetNumber;
}

/*
 *	UBTreePageAddTuple() -- add a tuple to a particular page in the index.
 *
 *		This routine adds the tuple to the page as requested.  It does
 *		not affect pin/lock status, but you'd better have a write lock
 *		and pin on the target buffer!  Don't forget to write and release
 *		the buffer afterwards, either.
 *
 *		The main difference between this routine and a bare PageAddItem call
 *		is that this code knows that the leftmost index tuple on a non-leaf
 *		btree page doesn't need to have a key.  Therefore, it strips such
 *		tuples down to just the tuple header.  CAUTION: this works ONLY if
 *		we insert the tuples in order, so that the given itup_off does
 *		represent the final position of the tuple!
 */
static bool UBTreePageAddTuple(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off, bool isnew)
{
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTDATAKEY(opaque)) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        UBTreeTupleSetNAtts(&trunctuple, 0, false);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (!(isnew && P_ISLEAF(opaque))) {
        /* if isnew and !P_HAS_XMIN_XMAX, it must inserting downlink to parent and itup is normal, no special handle
         * needed */
        if (PageAddItem(page, (Item)itup, itemsize, itup_off, false, false) == InvalidOffsetNumber)
            return false;
    } else {
        Size newsize = itemsize - sizeof(ShortTransactionId) * 2;
        IndexTupleSetSize(itup, newsize);
        /*
         * New inserted tuple need mark xmin, except HIKEY.
         * We assume itemsize included the reserved space for xmin/xmax.
         */
        bool needXid = (itup_off >= P_FIRSTDATAKEY(opaque));

        ((PageHeader)page)->pd_upper -= sizeof(ShortTransactionId) * 2;
        UstoreIndexXid uxid = (UstoreIndexXid)(((char*)page) + ((PageHeader)page)->pd_upper);

        TransactionId xmin = needXid ? GetCurrentTransactionId() : FrozenTransactionId;

        uxid->xmin = NormalTransactionIdToShort(opaque->xid_base, xmin);
        uxid->xmax = InvalidTransactionId;

        if (PageAddItem(page, (Item)itup, newsize, itup_off, false, false) == InvalidOffsetNumber) {
            /* restore page->upper before we return false */
            ((PageHeader)page)->pd_upper += sizeof(ShortTransactionId) * 2;
            return false;
        }

        ItemId iid = PageGetItemId(page, itup_off);
        ItemIdSetNormal(iid, ((PageHeader)page)->pd_upper, itemsize);
    }

    return true;
}

/*
 * UBTreeIsEqual() -- used in _bt_doinsert in check for duplicates.
 *
 * This is very similar to _bt_compare, except for NULL and negative infinity
 * handling. Rule is simple: NOT_NULL not equal NULL, NULL equal NULL.
 */
static bool UBTreeIsEqual(Relation idxrel, Page page, OffsetNumber offnum, int keysz, ScanKey scankey)
{
    TupleDesc itupdesc = RelationGetDescr(idxrel);
    IndexTuple itup;
    int i;

    /* Better be comparing to a leaf item */
    Assert(P_ISLEAF((UBTPageOpaqueInternal)PageGetSpecialPointer(page)));
    Assert(offnum >= P_FIRSTDATAKEY((UBTPageOpaqueInternal) PageGetSpecialPointer(page)));

    itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
    /*
     * Index tuple shouldn't be truncated.	Despite we technically could
     * compare truncated tuple as well, this function should be only called
     * for regular non-truncated leaf tuples and P_HIKEY tuple on
     * rightmost leaf page.
     */
    for (i = 1; i <= keysz; i++, scankey++) {
        AttrNumber attno = scankey->sk_attno;
        Assert(attno == i);
        bool datumIsNull = false;
        bool skeyIsNull = ((scankey->sk_flags & SK_ISNULL) ? true : false);
        Datum datum = index_getattr(itup, attno, itupdesc, &datumIsNull);

        if (datumIsNull && skeyIsNull)
            continue; /* NULL equal NULL */
        if (datumIsNull != skeyIsNull)
            return false; /* NOT_NULL not equal NULL */

        if (DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
                                            scankey->sk_collation, datum, scankey->sk_argument)) != 0) {
            return false;
        }
    }

    /* if we get here, the keys are equal */
    return true;
}

/*
 *  UBTreeDeleteOnPage() -- Delete a tuple on a particular page in the index.
 *
 *      If the deletion deleted the last tuple in the page, the page may
 *      become empty after the last transaction committed. So we record it as
 *      empty page for further page recycle logic.
 */
static void UBTreeDeleteOnPage(Relation rel, Buffer buf, OffsetNumber offset)
{
    Page page;
    UBTPageOpaqueInternal opaque;
    IndexTuple itup;
    UstoreIndexXid uxid;
    TransactionId xid = GetCurrentTransactionId();

    WHITEBOX_TEST_STUB("UBTreeDeleteOnPage", WhiteboxDefaultErrorEmit);

    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offset));
    uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    uxid->xmax = NormalTransactionIdToShort(opaque->xid_base, xid);

    /* update active hint */
    opaque->activeTupleCount--;
    if (TransactionIdPrecedes(opaque->last_delete_xid, xid))
        opaque->last_delete_xid = xid;

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_ubtree_mark_delete xlrec;
        XLogRecPtr recptr;

        xlrec.xid = xid;
        xlrec.offset = offset;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, SizeOfUBTreeMarkDelete);
        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

        recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_MARK_DELETE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    bool needRecordEmpty = (opaque->activeTupleCount == 0);
    if (needRecordEmpty) {
        /*
        * This delete operation deleted the last tuple in the page, This page is likely
        * to be empty later, so record this empty hint into a queue.
        */
        UBTreeRecordEmptyPage(rel, BufferGetBlockNumber(buf), opaque->last_delete_xid);
        /* release buffer */
        _bt_relbuf(rel, buf);
        /* at the same time, try to recycle an empty page already exists */
        UBTreeTryRecycleEmptyPage(rel);
    } else {
        /* just release buffer */
        _bt_relbuf(rel, buf);
    }
}

/*
 * UBTreeInsertParent() -- Insert downlink into parent after a page split.
 *
 * On entry, buf and rbuf are the left and right split pages, which we
 * still hold write locks on per the L&Y algorithm.  We release the
 * write locks once we have write lock on the parent page. (Any sooner,
 * and it'd be possible for some other process to try to split or delete
 * one of these pages, and get confused because it cannot find the downlink.)
 *
 * stack - stack showing how we got here.  May be NULL in cases that don't
 *         have to be efficient (concurrent ROOT split, WAL recovery)
 * is_root - we split the true root
 * is_only - we split a page alone on its level (might have been fast root)
 */
void UBTreeInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only)
{
    /*
     * Here we have to do something Lehman and Yao don't talk about: deal with
     * a root split and construction of a new root.  If our stack is empty
     * then we have just split a node on what had been the root level when we
     * descended the tree. If it was still the root then we perform a
     * new-root construction.  If it *wasn't* the root anymore, search to find
     * the next higher level that someone constructed meanwhile, and find the
     * right place to insert as for the normal case.
     *
     * If we have to search for the parent level, we do so by re-descending
     * from the root.  This is not super-efficient, but it's rare enough not
     * to matter.  (This path is also taken when called from WAL recovery ---
     * we have no stack in that case.)
     */
    if (is_root) {
        Buffer rootbuf;

        Assert(stack == NULL);
        Assert(is_only);
        /* create a new root node and update the metapage */
        rootbuf = UBTreeNewRoot(rel, buf, rbuf);
        /* release the split buffers */
        _bt_relbuf(rel, rootbuf);
        _bt_relbuf(rel, rbuf);
        _bt_relbuf(rel, buf);
    } else {
        BlockNumber bknum = BufferGetBlockNumber(buf);
        BlockNumber rbknum = BufferGetBlockNumber(rbuf);
        Page page = BufferGetPage(buf);
        IndexTuple new_item;
        BTStackData fakestack;
        IndexTuple ritem;
        Buffer pbuf;

        if (stack == NULL) {
            UBTPageOpaqueInternal lpageop;

            if (!t_thrd.xlog_cxt.InRecovery) {
                ereport(DEBUG2, (errmsg("concurrent ROOT page split")));
            }
            lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
            /* Find the leftmost page at the next level up */
            pbuf = UBTreeGetEndPoint(rel, lpageop->btpo.level + 1, false);
            /* Set up a phony stack entry pointing there */
            stack = &fakestack;
            stack->bts_blkno = BufferGetBlockNumber(pbuf);
            stack->bts_offset = InvalidOffsetNumber;
            stack->bts_btentry = InvalidBlockNumber;
            stack->bts_parent = NULL;
            _bt_relbuf(rel, pbuf);
        }

        /* get high key from left, a strict lower bound for new right page */
        ritem = (IndexTuple)PageGetItem(page, PageGetItemId(page, P_HIKEY));

        /* form an index tuple that points at the new right page
         * assure that memory is properly allocated, prevent from missing log of insert parent */
        START_CRIT_SECTION();
        new_item = CopyIndexTuple(ritem);
        UBTreeTupleSetDownLink(new_item, rbknum);
        END_CRIT_SECTION();

        /*
         * Find the parent buffer and get the parent page.
         *
         * Oops - if we were moved right then we need to change stack item! We
         * want to find parent pointing to where we are, right ? - vadim
         * 05/27/97
         */
        stack->bts_btentry = bknum;
        pbuf = UBTreeGetStackBuf(rel, stack);

        /* Now we can unlock the right child. The left child will be unlocked
         * by _bt_insertonpg()
         */
        _bt_relbuf(rel, rbuf);

        /* Check for error only after writing children */
        if (pbuf == InvalidBuffer) {
            XLogWaitFlush(t_thrd.xlog_cxt.LogwrtResult->Write);
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("failed to re-find parent key in index \"%s\" for split pages %u/%u",
                           RelationGetRelationName(rel), bknum, rbknum)));
        }
        /* Recursively update the parent */
        UBTreeInsertOnPage(rel, NULL, pbuf, buf, stack->bts_parent, new_item, stack->bts_offset + 1, is_only);

        /* be tidy */
        pfree(new_item);
    }
}

/*
 *	UBTreeNewRoot() -- Create a new root page for the index.
 *
 *		We've just split the old root page and need to create a new one.
 *		In order to do this, we add a new root page to the file, then lock
 *		the metadata page and update it.  This is guaranteed to be deadlock-
 *		free, because all readers release their locks on the metadata page
 *		before trying to lock the root, and all writers lock the root before
 *		trying to lock the metadata page.  We have a write lock on the old
 *		root page, so we have not introduced any cycles into the waits-for
 *		graph.
 *
 *		On entry, lbuf (the old root) and rbuf (its new peer) are write-
 *		locked. On exit, a new root page exists with entries for the
 *		two new children, metapage is updated and unlocked/unpinned.
 *		The new root buffer is returned to caller which has to unlock/unpin
 *		lbuf, rbuf & rootbuf.
 */
static Buffer UBTreeNewRoot(Relation rel, Buffer lbuf, Buffer rbuf)
{
    Buffer rootbuf;
    Page lpage, rootpage;
    BlockNumber lbkno, rbkno;
    BlockNumber rootblknum;
    UBTPageOpaqueInternal rootopaque;
    UBTPageOpaqueInternal lopaque;
    ItemId itemid;
    IndexTuple item;
    IndexTuple left_item;
    Size left_item_sz;
    IndexTuple right_item;
    Size right_item_sz;
    Buffer metabuf;
    Page metapg;
    BTMetaPageData *metad = NULL;

    lbkno = BufferGetBlockNumber(lbuf);
    rbkno = BufferGetBlockNumber(rbuf);
    lpage = BufferGetPage(lbuf);
    lopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);

    /*
     *	We have split the old root page and need to create a new one.
     *	Here we can not have any ERROR before the new root is created,
     *	and the metapage is update.
     *
     *	If we meet the ERROR here, the old root has been split into two
     *	pages, and the new root is failed to create. In this situation, the B+
     *	tree is without the root page, and the metadata is wrong.
     *
     *	1. root 1 splits to 1 and 2, block 1's root flag has changed.
     *	2. when we get in this function and haven't updated metapage's
     *	 rootpointer. ERROR here and we release our LWlock on 1 and 2.
     *	3. we continue insert into 1 and block 1 splits again. we use outdated
     *	 metapage to find 'trueroot' . deaklock.
     *
     *	Don't worry about incomplete btree pages as a result of PANIC here,
     *	because we can complete this incomplete action in btree_xlog_cleanup
     *	after redo our xlog.
     */
    START_CRIT_SECTION();

    /*
     * get a new root page
     *      NOTE: after the page is absolutely used, call UBTreeRecordUsedPage()
     *            before we release the Exclusive lock.
     */
    UBTRecycleQueueAddress addr;
    rootbuf = UBTreeGetNewPage(rel, &addr);
    rootpage = BufferGetPage(rootbuf);
    rootblknum = BufferGetBlockNumber(rootbuf);

    /* acquire lock on the metapage */
    metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
    metapg = BufferGetPage(metabuf);
    metad = BTPageGetMeta(metapg);

    /*
     * Create downlink item for left page (old root).  Since this will be the
     * first item in a non-leaf page, it implicitly has minus-infinity key
     * value, so we need not store any actual key in it.
     */
    left_item_sz = sizeof(IndexTupleData);
    left_item = (IndexTuple)palloc(left_item_sz);
    left_item->t_info = (unsigned short)left_item_sz;

    UBTreeTupleSetDownLink(left_item, lbkno);
    UBTreeTupleSetNAtts(left_item, 0, false);

    /*
     * Create downlink item for right page.  The key for it is obtained from
     * the "high key" position in the left page.
     */
    itemid = PageGetItemId(lpage, P_HIKEY);
    item = (IndexTuple)PageGetItem(lpage, itemid);
    right_item = CopyIndexTuple(item);

    UBTreeTupleSetDownLink(right_item, rbkno);
    right_item_sz = IndexTupleSize(right_item);

    /* set btree special data */
    rootopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(rootpage);
    rootopaque->btpo_prev = rootopaque->btpo_next = P_NONE;
    rootopaque->btpo_flags = BTP_ROOT;
    rootopaque->btpo.level = ((UBTPageOpaqueInternal)PageGetSpecialPointer(lpage))->btpo.level + 1;
    rootopaque->btpo_cycleid = 0;

    /* update metapage data */
    metad->btm_root = rootblknum;
    metad->btm_level = rootopaque->btpo.level;
    metad->btm_fastroot = rootblknum;
    metad->btm_fastlevel = rootopaque->btpo.level;
    rel->rd_rootcache = InvalidBuffer;

    /*
     * Insert the left page pointer into the new root page.  The root page is
     * the rightmost page on its level so there is no "high key" in it; the
     * two items will go into positions P_HIKEY and P_FIRSTKEY.
     *
     * Note: we *must* insert the two items in item-number order, for the
     * benefit of _bt_restore_page().
     */
    if (PageAddItem(rootpage, (Item)left_item, left_item_sz, P_HIKEY, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add leftkey to new root page while splitting block %u of index \"%s\"",
                       BufferGetBlockNumber(lbuf), RelationGetRelationName(rel))));

    /*
     * insert the right page pointer into the new root page.
     */
    if (PageAddItem(rootpage, (Item)right_item, right_item_sz, P_FIRSTKEY, false, false) == InvalidOffsetNumber)
        ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("failed to add rightkey to new root page while splitting block %u of index \"%s\"",
                       BufferGetBlockNumber(lbuf), RelationGetRelationName(rel))));

    /* Clear the incomplete-split flag in the left child */
    Assert(P_INCOMPLETE_SPLIT(lopaque));
    lopaque->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
    MarkBufferDirty(lbuf);

    MarkBufferDirty(rootbuf);
    MarkBufferDirty(metabuf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_btree_newroot xlrec;
        XLogRecPtr recptr;
        xl_btree_metadata md;

        xlrec.rootblk = rootblknum;
        xlrec.level = metad->btm_level;

        Assert(xlrec.level > 0);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfBtreeNewroot);

        XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
        XLogRegisterBuffer(2, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

        md.root = rootblknum;
        md.level = metad->btm_level;
        md.fastroot = rootblknum;
        md.fastlevel = metad->btm_level;

        XLogRegisterBufData(2, (char *)&md, sizeof(xl_btree_metadata));

        /*
         * Direct access to page is not good but faster - we should implement
         * some new func in page API.
         */
        XLogRegisterBufData(0, (char *)rootpage + ((PageHeader)rootpage)->pd_upper,
                            ((PageHeader)rootpage)->pd_special - ((PageHeader)rootpage)->pd_upper);

        recptr = XLogInsert(RM_UBTREE_ID, XLOG_UBTREE_NEWROOT);

        PageSetLSN(lpage, recptr);
        PageSetLSN(rootpage, recptr);
        PageSetLSN(metapg, recptr);
    }

    END_CRIT_SECTION();

    /* done with metapage */
    _bt_relbuf(rel, metabuf);

    /* discard this page from the Recycle Queue */
    UBTreeRecordUsedPage(rel, addr);

    pfree(left_item);
    pfree(right_item);

    return rootbuf;
}

/*
 * UBTreeFinishSplit() -- Finish an incomplete split
 *
 * A crash or other failure can leave a split incomplete.  The insertion
 * routines won't allow to insert on a page that is incompletely split.
 * Before inserting on such a page, call _bt_finish_split().
 *
 * On entry, 'lbuf' must be locked in write-mode. On exit, it is unlocked
 * and unpinned.
 */
void UBTreeFinishSplit(Relation rel, Buffer lbuf, BTStack stack)
{
    Page lpage = BufferGetPage(lbuf);
    UBTPageOpaqueInternal lpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(lpage);
    Buffer rbuf;
    Page rpage;
    UBTPageOpaqueInternal rpageop;
    bool wasRoot = false;

    Assert(P_INCOMPLETE_SPLIT(lpageop));

    /* Lock right sibling, the one missing the downlink */
    rbuf = _bt_getbuf(rel, lpageop->btpo_next, BT_WRITE);
    rpage = BufferGetPage(rbuf);
    rpageop = (UBTPageOpaqueInternal)PageGetSpecialPointer(rpage);

    /* Could this be a root split? */
    if (!stack) {
        Buffer metabuf;
        Page metapg;
        BTMetaPageData *metad;

        /* acquire lock on the metapage */
        metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
        metapg = BufferGetPage(metabuf);
        metad = BTPageGetMeta(metapg);

        wasRoot = (metad->btm_root == BufferGetBlockNumber(lbuf));

        _bt_relbuf(rel, metabuf);
    } else {
        wasRoot = false;
    }

    /* Was this the only page on the level before split? */
    bool wasOnly = (P_LEFTMOST(lpageop) && P_RIGHTMOST(rpageop));

    elog(DEBUG1, "finishing incomplete split of %u/%u", BufferGetBlockNumber(lbuf), BufferGetBlockNumber(rbuf));

    UBTreeInsertParent(rel, lbuf, rbuf, stack, wasRoot, wasOnly);
}

/*
 *	UBTreeGetStackBuf() -- Walk back up the tree one step, and find the item
 *						 we last looked at in the parent.
 *
 *		This is possible because we save the downlink from the parent item,
 *		which is enough to uniquely identify it.  Insertions into the parent
 *		level could cause the item to move right; deletions could cause it
 *		to move left, but not left of the page we previously found it in.
 *
 *		Adjusts bts_blkno & bts_offset if changed.
 *
 *		Returns InvalidBuffer if item not found (should not happen).
 */
Buffer UBTreeGetStackBuf(Relation rel, BTStack stack)
{
    BlockNumber blkno;
    OffsetNumber start;

    blkno = stack->bts_blkno;
    start = stack->bts_offset;
    t_thrd.storage_cxt.is_btree_split = true;
    for (;;) {
        Buffer buf;
        Page page;
        UBTPageOpaqueInternal opaque;

        buf = _bt_getbuf(rel, blkno, BT_WRITE);
        page = BufferGetPage(buf);
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
        if (P_INCOMPLETE_SPLIT(opaque)) {
            UBTreeFinishSplit(rel, buf, stack->bts_parent);
            continue;
        }

        if (!P_IGNORE(opaque)) {
            OffsetNumber offnum, minoff, maxoff;
            ItemId itemid;
            IndexTuple item;

            minoff = P_FIRSTDATAKEY(opaque);
            maxoff = PageGetMaxOffsetNumber(page);

            /*
             * start = InvalidOffsetNumber means "search the whole page". We
             * need this test anyway due to possibility that page has a high
             * key now when it didn't before.
             */
            if (start < minoff)
                start = minoff;

            /*
             * Need this check too, to guard against possibility that page
             * split since we visited it originally.
             */
            if (start > maxoff)
                start = OffsetNumberNext(maxoff);

            /*
             * These loops will check every item on the page --- but in an
             * order that's attuned to the probability of where it actually
             * is.	Scan to the right first, then to the left.
             */
            for (offnum = start; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
                itemid = PageGetItemId(page, offnum);
                item = (IndexTuple)PageGetItem(page, itemid);
                bool getRightItem = false;
                getRightItem = BTreeInnerTupleGetDownLink(item) == stack->bts_btentry;
                if (getRightItem) {
                    /* Return accurate pointer to where link is now */
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    t_thrd.storage_cxt.is_btree_split = false;
                    return buf;
                }
            }

            for (offnum = OffsetNumberPrev(start); offnum >= minoff; offnum = OffsetNumberPrev(offnum)) {
                itemid = PageGetItemId(page, offnum);
                item = (IndexTuple)PageGetItem(page, itemid);
                bool getRightItem = false;
                getRightItem = BTreeInnerTupleGetDownLink(item) == stack->bts_btentry;
                if (getRightItem) {
                    /* Return accurate pointer to where link is now */
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    t_thrd.storage_cxt.is_btree_split = false;
                    return buf;
                }
            }
        }

        /*
         * The item we're looking for moved right at least one page.
         */
        if (P_RIGHTMOST(opaque)) {
            _bt_relbuf(rel, buf);
            t_thrd.storage_cxt.is_btree_split = false;
            return InvalidBuffer;
        }
        blkno = opaque->btpo_next;
        start = InvalidOffsetNumber;
        _bt_relbuf(rel, buf);
    }
}
