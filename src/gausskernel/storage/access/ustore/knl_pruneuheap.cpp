
/* -------------------------------------------------------------------------
 *
 * knl_pruneuheap.cpp
 * uheap page pruning
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_pruneuheap.cpp
 * --------------------------------------------------------------------------------
 * --------------------------------------------------------------------------------
 * In uheap, we can reclaim space on following operations
 * a. non-inplace updates, when committed or rolled back.
 * b. inplace updates that reduces the tuple length, when committed.
 * c. deletes, when committed.
 * d. inserts, when rolled back.
 *
 * Since we only store xid which changed the page in prune_xid, to prune
 * the page, we can check if prune_xid is in progress.  This can sometimes
 * lead to unwanted page pruning calls as a side effect, example in case of
 * rolled back deletes.  If there is nothing to prune, then the call to prune
 * is cheap, so we don't want to optimize it at this stage.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gtm.h"
#include "access/csnlog.h"
#include "access/transam.h"
#include "utils/snapmgr.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/procarray.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_utils.h"
#include "access/multi_redo_api.h"
#include "access/ustore/knl_whitebox_test.h"

static int UHeapPruneItem(const RelationBuffer *relbuf, OffsetNumber rootoffnum, TransactionId oldestXmin,
    UPruneState *prstate, Size *spaceFreed);
static void UHeapPruneRecordPrunable(UPruneState *prstate, TransactionId xid);
static void UHeapPruneRecordDead(UPruneState *prstate, OffsetNumber offnum, Relation relation);
static void UHeapPruneRecordDeleted(UPruneState *prstate, OffsetNumber offnum, Relation relation);

static int Itemoffcompare(const void *itemidp1, const void *itemidp2)
{
    /* Sort in decreasing itemoff order */
    return ((itemIdSort)itemidp2)->itemoff - ((itemIdSort)itemidp1)->itemoff;
}

bool UHeapPagePruneOptPage(Relation relation, Buffer buffer, TransactionId xid, bool acquireContionalLock)
{
    Page page;
    TransactionId oldestXmin = InvalidTransactionId;
    TransactionId ignore = InvalidTransactionId;
    Size minfree;
    bool pruned = false;

    page = BufferGetPage(buffer);

    /*
     * We can't write WAL in recovery mode, so there's no point trying to
     * clean the page. The master will likely issue a cleaning WAL record soon
     * anyway, so this is no particular loss.
     */
    if (RecoveryInProgress()) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_IN_RECOVERY);
#endif
        return false;
    }

    GetOldestXminForUndo(&oldestXmin);
    Assert(TransactionIdIsValid(oldestXmin));

    if ((t_thrd.xact_cxt.useLocalSnapshot && IsPostmasterEnvironment) ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster || u_sess->attr.attr_common.upgrade_mode == 1) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_INVALID);
#endif
        return false;
    }

    /*
     * Let's see if we really need pruning.
     *
     * Forget it if page is not hinted to contain something prunable that's committed
     * and we don't want to forcefully prune the page. Furthermore, even if the page
     * contains prunable items, we do not want to do pruning every so often and hamper
     * server performance. The current heuristic to delay pruning is the difference
     * between pd_prune_xid and oldestxmin must be atleast 3 times or more the difference
     * of xid and oldestxmin.
     */
    if (!UPageIsPrunableWithXminHorizon(page, oldestXmin) || (TransactionIdIsValid(xid) &&
        (oldestXmin - ((UHeapPageHeaderData *)(page))->pd_prune_xid) < (3 * (xid - oldestXmin)))) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_NO_SPACE);
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_XID_FILTER);
#endif
        return false;
    }

    minfree = RelationGetTargetPageFreeSpacePrune(relation, HEAP_DEFAULT_FILLFACTOR);
    minfree = Max(minfree, BLCKSZ / 10);
    if (PageIsFull(page) || PageGetExactUHeapFreeSpace(page) < minfree) {
        RelationBuffer relbuf = {relation, buffer};
        if (!acquireContionalLock) {
            /* Exclusive lock is acquired, OK to prune */
            (void)UHeapPagePrune(&relbuf, oldestXmin, true, &ignore, &pruned);
        } else {
            if (!ConditionalLockUHeapBufferForCleanup(buffer)) {
                return false;
            }

            if (PageIsFull(page) || PageGetExactUHeapFreeSpace(page) < minfree) {
                (void)UHeapPagePrune(&relbuf, oldestXmin, true, &ignore, &pruned);
            }

            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        }
    } else {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_FILLFACTOR);
#endif
    }

    if (pruned) {
        return true;
    }

    return false;
}

int UHeapPagePrune(const RelationBuffer *relbuf, TransactionId oldestXmin, bool reportStats,
    TransactionId *latestRemovedXid, bool *pruned)
{
    int ndeleted = 0;
    Size spaceFreed = 0;
    Page page = BufferGetPage(relbuf->buffer);
    OffsetNumber offnum;
    OffsetNumber maxoff;
    UPruneState prstate;
    bool executePruning = false;
    errno_t rc;
    bool hasPruned = false;

    if (pruned) {
        *pruned = false;
    }

    /*
     * Our strategy is to scan the page and make lists of items to change,
     * then apply the changes within a critical section.  This keeps as much
     * logic as possible out of the critical section, and also ensures that
     * WAL replay will work the same as the normal case.
     *
     * First, initialize the new pd_prune_xid value to zero (indicating no
     * prunable tuples).  If we find any tuples which may soon become
     * prunable, we will save the lowest relevant XID in new_prune_xid. Also
     * initialize the rest of our working state.
     */
    prstate.new_prune_xid = InvalidTransactionId;
    prstate.latestRemovedXid = *latestRemovedXid;
    prstate.ndeleted = prstate.ndead = prstate.nunused = 0;
    rc = memset_s(prstate.marked, sizeof(prstate.marked), 0, sizeof(prstate.marked));
    securec_check(rc, "\0", "\0");

    /*
     * If caller has asked to rearrange the page and page is not marked for
     * pruning, then skip scanning the page.
     *
     * XXX We might want to remove this check once we have some optimal
     * strategy to rearrange the page where we anyway need to traverse all
     * rows.
     */
    if (!UPageIsPrunableWithXminHorizon(page, oldestXmin)) {
        ; /* no need to scan */
    } else {
        /* Scan the page */
        maxoff = UHeapPageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            RowPtr *itemid = NULL;

            /* Ignore items already processed as part of an earlier chain */
            if (prstate.marked[offnum]) {
                continue;
            }

            /*
             * Nothing to do if slot is empty, already dead or marked as
             * deleted.
             */
            itemid = UPageGetRowPtr(page, offnum);
            if (!RowPtrIsUsed(itemid) || RowPtrIsDead(itemid) || RowPtrIsDeleted(itemid)) {
                continue;
            }

            /* Process this item */
            ndeleted += UHeapPruneItem(relbuf, offnum, oldestXmin, &prstate, &spaceFreed);
        }
    }

    /* Do we want to prune? */
    if (prstate.ndeleted > 0 || prstate.ndead > 0 || prstate.nunused > 0) {
        executePruning = true;
    }

    /* Any error while applying the changes is critical */
    START_CRIT_SECTION();

    if (executePruning) {
        /*
         * Apply the planned item changes, then repair page fragmentation, and
         * update the page's hint bit about whether it has free line pointers.
         * first print relation oid
         */

        WaitState oldStatus = pgstat_report_waitstatus(STATE_PRUNE_TABLE);
        UHeapPagePruneExecute(relbuf->buffer, InvalidOffsetNumber, &prstate);

        /* prune the dead line pointers */
        if (prstate.ndead > 0) {
            int i;
            OffsetNumber *offptr = prstate.nowdead;
            // Apparently, we do not set unused line pointers until we reach here.
            // We should ensure it is not already set so we do not overwrite what
            // is already there.
            Assert(prstate.nunused == 0);
            for (i = 0; i < prstate.ndead; i++) {
                OffsetNumber off = *offptr++;
                RowPtr *lp;

                lp = UPageGetRowPtr(page, off);

                RowPtrSetUnused(lp);
                prstate.nowunused[prstate.nunused] = off;
                prstate.nunused++;
            }
        }

        /*
         * Finally, repair any fragmentation, and update the page's hint bit
         * whether it has free pointers.
         */
        UPageRepairFragmentation(relbuf->buffer, InvalidOffsetNumber, 0, &hasPruned, false);

#ifdef DEBUG_UHEAP
        if (hasPruned) {
            UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_SUCCESS);
        } else {
            UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_UPDATE_IN_PROGRESS);
        }
#endif

        /*
         * Update the page's pd_prune_xid field to either zero, or the lowest
         * XID of any soon-prunable tuple.
         */
        ((UHeapPageHeaderData *)page)->pd_prune_xid = prstate.new_prune_xid;

        /*
         * Also clear the "page is full" flag, since there's no point in
         * repeating the prune/defrag process until something else happens to
         * the page.
         */
        UPageClearFull(page);

        MarkBufferDirty(relbuf->buffer);

        /*
         * Emit a WAL INPLACEHEAP_CLEAN record showing what we did
         */
        if (RelationNeedsWAL(relbuf->relation)) {
            XLogRecPtr recptr;

            recptr = LogUHeapClean(relbuf->relation, relbuf->buffer, InvalidOffsetNumber, 0, prstate.nowdeleted,
                prstate.ndeleted, prstate.nowdead, 0, prstate.nowunused, prstate.nunused, prstate.latestRemovedXid,
                hasPruned);

            PageSetLSN(BufferGetPage(relbuf->buffer), recptr);
        }

        if (pruned) {
            *pruned = hasPruned;
        }
        pgstat_report_waitstatus(oldStatus);
    } else {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_NO_SPACE);
#endif

        /*
         * If we didn't prune anything, but have found a new value for the
         * pd_prune_xid field, update it and mark the buffer dirty. This is
         * treated as a non-WAL-logged hint.
         *
         * Also clear the "page is full" flag if it is set, since there's no
         * point in repeating the prune/defrag process until something else
         * happens to the page.
         */
        if (((UHeapPageHeaderData *)page)->pd_prune_xid != prstate.new_prune_xid || UPageIsFull(page)) {
            ((UHeapPageHeaderData *)page)->pd_prune_xid = prstate.new_prune_xid;
            UPageClearFull(page);
            MarkBufferDirtyHint(relbuf->buffer, true);
        }
    }

    END_CRIT_SECTION();

    /*
     * Report the number of tuples reclaimed to pgstats. This is ndeleted
     * minus ndead, because we don't want to count a now-DEAD item or a
     * now-DELETED item as a deletion for this purpose.
     * -- no autovacuum so need to report how many we cleaned up.
     */
    if (reportStats && hasPruned && ndeleted > 0) {
        pgstat_update_heap_dead_tuples(relbuf->relation, ndeleted);
    }

    *latestRemovedXid = prstate.latestRemovedXid;

    /*
     * XXX Should we update FSM information for this?  Not doing so will
     * increase the chances of in-place updates.  See heap_page_prune for a
     * detailed reason.
     */

    return ndeleted;
}

/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * Caller must have exclusive lock on the page.
 *
 * oldestXmin is the cutoff XID used to distinguish whether tuples are DEAD
 * or RECENTLY_DEAD (see InplaceHeapTupleSatisfiesOldestXmin).
 *
 * This is an opportunistic function.  It will perform housekeeping only if
 * the page has effect of transaction that has modified data which can be
 * pruned.
 *
 * Note: This is called only when we need some space in page to perform the
 * action which otherwise would need a different page.  It is called when an
 * update statement has to update the existing tuple such that new tuple is
 * bigger than old tuple and the same can't fit on page.
 *
 * Returns true, if we are able to free up the space such that the new tuple
 * can fit into same page, otherwise, false.
 */
bool UHeapPagePruneOpt(Relation relation, Buffer buffer, OffsetNumber offnum, Size spaceRequired)
{
    Page page;
    TransactionId oldestXmin;
    TransactionId ignore = InvalidTransactionId;
    Size pagefree;
    bool forcePrune = false;
    bool pruned = false;
    RelationBuffer relbuf = {relation, buffer};

    page = BufferGetPage(buffer);

    /*
     * We can't write WAL in recovery mode, so there's no point trying to
     * clean the page. The master will likely issue a cleaning WAL record soon
     * anyway, so this is no particular loss.
     */
    if (RecoveryInProgress())
        return false;

    GetOldestXminForUndo(&oldestXmin);
    Assert(TransactionIdIsValid(oldestXmin));

    if (OffsetNumberIsValid(offnum)) {
        pagefree = PageGetExactUHeapFreeSpace(page);
        /*
         * We want to forcefully prune the page if we are sure that the
         * required space is available.  This will help in rearranging the
         * page such that we will be able to make space adjacent to required
         * offset number.
         */
        if (spaceRequired < pagefree)
            forcePrune = true;
    }

    /*
     * Let's see if we really need pruning.
     *
     * Forget it if page is not hinted to contain something prunable that's
     * committed and we don't want to forcefully prune the page.
     */
    if (!UPageIsPrunable(page) && !forcePrune) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_NO_SPACE);
#endif
        return false;
    }

    UHeapPagePruneGuts(&relbuf, oldestXmin, offnum, spaceRequired, true, forcePrune, &ignore, &pruned);
    if (pruned) {
        return true;
    }

    return false;
}

/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.
 *
 * oldestXmin is the cutoff XID used to distinguish whether tuples are DEAD
 * or RECENTLY_DEAD (see InplaceHeapTupleSatisfiesOldestXmin).
 *
 * To perform pruning, we make the copy of the page.  We don't scribble on
 * that copy, rather it is only used during repair fragmentation to copy
 * the tuples.  So, we need to ensure that after making the copy, we operate
 * on tuples, otherwise, the temporary copy will become useless.  It is okay
 * scribble on itemid's or special space of page.
 *
 * If reportStats is true then we send the number of reclaimed tuples to
 * pgstats.  (This must be false during vacuum, since vacuum will send its own
 * own new total to pgstats, and we don't want this delta applied on top of
 * that.)
 *
 * Returns the number of tuples deleted from the page and sets
 * latestRemovedXid.  It returns 0, when removed the dead tuples can't free up
 * the space required.
 */
int UHeapPagePruneGuts(const RelationBuffer *relbuf, TransactionId oldestXmin, OffsetNumber targetOffnum,
    Size spaceRequired, bool reportStats, bool forcePrune, TransactionId *latestRemovedXid, bool *pruned)
{
    int ndeleted = 0;
    Size spaceFreed = 0;
    Page page = BufferGetPage(relbuf->buffer);
    OffsetNumber offnum;
    OffsetNumber maxoff;
    UPruneState prstate;
    bool executePruning = false;
    bool hasPruned = false;

    errno_t rc;

    if (pruned) {
        *pruned = false;
    }

    /* initialize the space_free with already existing free space in page */
    spaceFreed = PageGetExactUHeapFreeSpace(page);

    /*
     * Our strategy is to scan the page and make lists of items to change,
     * then apply the changes within a critical section.  This keeps as much
     * logic as possible out of the critical section, and also ensures that
     * WAL replay will work the same as the normal case.
     *
     * First, initialize the new pd_prune_xid value to zero (indicating no
     * prunable tuples).  If we find any tuples which may soon become
     * prunable, we will save the lowest relevant XID in new_prune_xid. Also
     * initialize the rest of our working state.
     */
    prstate.new_prune_xid = InvalidTransactionId;
    prstate.latestRemovedXid = *latestRemovedXid;
    prstate.ndeleted = prstate.ndead = prstate.nunused = 0;
    rc = memset_s(prstate.marked, sizeof(prstate.marked), 0, sizeof(prstate.marked));
    securec_check(rc, "\0", "\0");

    /*
     * If caller has asked to rearrange the page and page is not marked for
     * pruning, then skip scanning the page.
     *
     * XXX We might want to remove this check once we have some optimal
     * strategy to rearrange the page where we anyway need to traverse all
     * rows.
     */
    if (forcePrune && !UPageIsPrunable(page)) {
        ; /* no need to scan */
    } else {
        /* Scan the page */
        maxoff = UHeapPageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            RowPtr *itemid = NULL;

            /* Ignore items already processed as part of an earlier chain */
            if (prstate.marked[offnum]) {
                continue;
            }

            /*
             * Nothing to do if slot is empty, already dead or marked as
             * deleted.
             */
            itemid = UPageGetRowPtr(page, offnum);
            if (!RowPtrIsUsed(itemid) || RowPtrIsDead(itemid) || RowPtrIsDeleted(itemid)) {
                continue;
            }

            /* Process this item */
            ndeleted += UHeapPruneItem(relbuf, offnum, oldestXmin, &prstate, &spaceFreed);
        }
    }

    /*
     * There is not much advantage in continuing, if we can't free the space
     * required by the caller or we are not asked to forcefully prune the
     * page.
     *
     * XXX - In theory, we can still continue and perform pruning in the hope
     * that some future update in this page will be able to use that space.
     * However, it will lead to additional writes without any guaranteed
     * benefit, so we skip the pruning for now.
     */
    if (spaceFreed < spaceRequired) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_NO_SPACE);
#endif

        return 0;
    }

    /* Do we want to prune? */
    if (prstate.ndeleted > 0 || prstate.ndead > 0 || prstate.nunused > 0 || forcePrune) {
        executePruning = true;
    }

    /* Any error while applying the changes is critical */
    START_CRIT_SECTION();

    if (executePruning) {
        /*
         * Apply the planned item changes, then repair page fragmentation, and
         * update the page's hint bit about whether it has free line pointers.
         */
        WaitState oldStatus = pgstat_report_waitstatus(STATE_PRUNE_TABLE);
        UHeapPagePruneExecute(relbuf->buffer, targetOffnum, &prstate);

        /* prune the dead line pointers */
        if (prstate.ndead > 0) {
            int i;
            OffsetNumber *offptr = prstate.nowdead;
            // Apparently, we do not set unused line pointers until we reach here.
            // We should ensure it is not already set so we do not overwrite what
            // is already there.
            Assert(prstate.nunused == 0);
            for (i = 0; i < prstate.ndead; i++) {
                OffsetNumber off = *offptr++;
                RowPtr *lp;

                lp = UPageGetRowPtr(page, off);

                RowPtrSetUnused(lp);
                prstate.nowunused[prstate.nunused] = off;
                prstate.nunused++;
            }
        }

        /*
         * Finally, repair any fragmentation, and update the page's hint bit
         * whether it has free pointers.
         */
        UPageRepairFragmentation(relbuf->buffer, targetOffnum, spaceRequired, &hasPruned, false);

#ifdef DEBUG_UHEAP
        if (hasPruned) {
            UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_SUCCESS);
        } else {
            UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_UPDATE_IN_PROGRESS);
        }
#endif

        /*
         * Update the page's pd_prune_xid field to either zero, or the lowest
         * XID of any soon-prunable tuple.
         */
        ((UHeapPageHeaderData *)page)->pd_prune_xid = prstate.new_prune_xid;

        /*
         * Also clear the "page is full" flag, since there's no point in
         * repeating the prune/defrag process until something else happens to
         * the page.
         */
        UPageClearFull(page);

        MarkBufferDirty(relbuf->buffer);

        /*
         * Emit a WAL INPLACEHEAP_CLEAN record showing what we did
         */
        if (RelationNeedsWAL(relbuf->relation)) {
            XLogRecPtr recptr;

            recptr = LogUHeapClean(relbuf->relation, relbuf->buffer, targetOffnum, spaceRequired, prstate.nowdeleted,
                prstate.ndeleted, prstate.nowdead, prstate.ndead, prstate.nowunused, prstate.nunused,
                prstate.latestRemovedXid, hasPruned);

            PageSetLSN(BufferGetPage(relbuf->buffer), recptr);
        }

        if (pruned) {
            *pruned = hasPruned;
        }
        pgstat_report_waitstatus(oldStatus);
    } else {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_PRUNEPAGE(PRUNE_PAGE_NO_SPACE);
#endif

        /*
         * If we didn't prune anything, but have found a new value for the
         * pd_prune_xid field, update it and mark the buffer dirty. This is
         * treated as a non-WAL-logged hint.
         *
         * Also clear the "page is full" flag if it is set, since there's no
         * point in repeating the prune/defrag process until something else
         * happens to the page.
         */
        if (((UHeapPageHeaderData *)page)->pd_prune_xid != prstate.new_prune_xid || UPageIsFull(page)) {
            ((UHeapPageHeaderData *)page)->pd_prune_xid = prstate.new_prune_xid;
            UPageClearFull(page);
            MarkBufferDirtyHint(relbuf->buffer, true);
        }
    }

    END_CRIT_SECTION();

    /*
     * Report the number of tuples reclaimed to pgstats. This is ndeleted
     * minus ndead, because we don't want to count a now-DEAD item or a
     * now-DELETED item as a deletion for this purpose.
     * -- no autovacuum so need to report how many we cleaned up
     */
    if (reportStats && hasPruned && ndeleted > 0) {
        pgstat_update_heap_dead_tuples(relbuf->relation, ndeleted);
    }

    *latestRemovedXid = prstate.latestRemovedXid;

    /*
     * XXX Should we update FSM information for this?  Not doing so will
     * increase the chances of in-place updates.  See heap_page_prune for a
     * detailed reason.
     */

    return ndeleted;
}

/*
 * Perform the actual page changes needed by uheap_page_prune_guts.
 * It is expected that the caller has suitable pin and lock on the
 * buffer, and is inside a critical section.
 */
void UHeapPagePruneExecute(Buffer buffer, OffsetNumber targetOffnum, const UPruneState *prstate)
{
    Page page = (Page)BufferGetPage(buffer);
    const OffsetNumber *offnum;
    int i;

    WHITEBOX_TEST_STUB(UHEAP_PAGE_PRUNE_FAILED, WhiteboxDefaultErrorEmit);

    /* Update all deleted line pointers */
    /* print ndeleted and detailed BufferGetBlockNumber(buffer) and off */
    offnum = prstate->nowdeleted;
    for (i = 0; i < prstate->ndeleted; i++) {
        int tdSlot;
        unsigned int utdSlot;
        uint8 visInfo = 0;
        OffsetNumber off = *offnum++;
        RowPtr *lp = NULL;

        /* The target offset must not be deleted. */
        Assert(targetOffnum != off);

        lp = UPageGetRowPtr(page, off);

        UHeapDiskTuple tup = (UHeapDiskTuple)UPageGetRowData(page, lp);
        tdSlot = UHeapTupleHeaderGetTDSlot(tup);

        /*
         * The frozen slot indicates tuple is dead, so we must not see them in
         * the array of tuples to be marked as deleted.
         */
        Assert(tdSlot != UHEAPTUP_SLOT_FROZEN);

        if (UHeapDiskTupleDeleted(tup))
            visInfo = ROWPTR_DELETED;
        if (UHeapTupleHasInvalidXact(tup->flag))
            visInfo |= ROWPTR_XACT_INVALID;

        /*
         * Mark the Item as deleted and copy the visibility info and
         * transaction slot information from tuple to RowPtr.
         */
        Assert(tdSlot >= 0);
        utdSlot = (unsigned int)tdSlot;
        RowPtrSetDeleted(lp, utdSlot, visInfo);
    }

    /* Update all now-dead line pointers */
    // same log message as above
    offnum = prstate->nowdead;
    for (i = 0; i < prstate->ndead; i++) {
        OffsetNumber off = *offnum++;
        RowPtr *lp = NULL;

        /* The target offset must not be dead. */
        Assert(targetOffnum != off);

        lp = UPageGetRowPtr(page, off);

        RowPtrSetDead(lp);
    }

    /* Update all now-unused line pointers */
    offnum = prstate->nowunused;
    for (i = 0; i < prstate->nunused; i++) {
        OffsetNumber off = *offnum++;
        RowPtr *lp = NULL;

        /* The target offset must not be unused. */
        Assert(targetOffnum != off);

        lp = UPageGetRowPtr(page, off);

        RowPtrSetUnused(lp);
    }
}

/*
 * Prune specified item pointer.
 *
 * oldestXmin is the cutoff XID used to identify dead tuples.
 *
 * We don't actually change the page here.  We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be set to RP_DEAD state are
 * added to nowdead[]; items to be set to RP_DELETED are added to nowdeleted[];
 * and items to be set to RP_UNUSED state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int UHeapPruneItem(const RelationBuffer *relbuf, OffsetNumber offnum, TransactionId oldestXmin,
    UPruneState *prstate, Size *spaceFreed)
{
    UHeapTupleData tup;
    RowPtr *lp;
    Page dp = (Page)BufferGetPage(relbuf->buffer);
    int ndeleted = 0;
    TransactionId xid = InvalidTransactionId;
    bool tupdead = false;
    bool recentDead = false;

    lp = UPageGetRowPtr(dp, offnum);

    Assert(RowPtrIsNormal(lp));

    tup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(dp, lp);
    tup.disk_tuple_size = RowPtrGetLen(lp);
    ItemPointerSet(&(tup.ctid), BufferGetBlockNumber(relbuf->buffer), offnum);
    tup.table_oid = RelationGetRelid(relbuf->relation);

    /*
     * Check tuple's visibility status.
     */
    tupdead = recentDead = false;

    switch (UHeapTupleSatisfiesOldestXmin(&tup, oldestXmin, relbuf->buffer, false, NULL, &xid, NULL)) {
        case UHEAPTUPLE_DEAD:
            tupdead = true;
            break;

        case UHEAPTUPLE_RECENTLY_DEAD:
            recentDead = true;
            break;

        case UHEAPTUPLE_DELETE_IN_PROGRESS:
            /*
             * This tuple may soon become DEAD.  Update the hint field so that
             * the page is reconsidered for pruning in future.
             */
            UHeapPruneRecordPrunable(prstate, xid);
            break;

        case UHEAPTUPLE_LIVE:
        case UHEAPTUPLE_INSERT_IN_PROGRESS:
            /*
             * If we wanted to optimize for aborts, we might consider marking
             * the page prunable when we see INSERT_IN_PROGRESS. But we don't.
             * See related decisions about when to mark the page prunable in
             * heapam.c.
             */
            break;

        case UHEAPTUPLE_ABORT_IN_PROGRESS:
            /*
             * We can simply skip the tuple if it has inserted/operated by
             * some aborted transaction and its rollback is still pending.
             * It'll be taken care of by future prune calls.
             */
            break;
        default:
            elog(ERROR, "unexpected InplaceHeapTupleSatisfiesOldestXmin result");
            break;
    }

    if (tupdead) {
        UHeapTupleHeaderAdvanceLatestRemovedXid(tup.disk_tuple, xid, &prstate->latestRemovedXid);
    }

    if (tupdead || recentDead) {
        /*
         * Count dead or recently dead tuple in result and update the space
         * that can be freed.
         */
        ndeleted++;
        if (TransactionIdIsValid(xid) && TransactionIdIsInProgress(xid)) {
            ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Tuple will be pruned but xid is inprogress, xid=%lu, oldestxmin=%lu, oldestXidInUndo=%lu.",
                xid, oldestXmin, g_instance.undo_cxt.oldestXidInUndo)));
        }
        /* short aligned */
        *spaceFreed += SHORTALIGN(tup.disk_tuple_size);
    }

    /* Record dead item */
    if (tupdead) {
        UHeapPruneRecordDead(prstate, offnum, relbuf->relation);
    }

    /* Record deleted item */
    if (recentDead) {
        UHeapPruneRecordDeleted(prstate, offnum, relbuf->relation);
    }

    return ndeleted;
}

/* Record lowest soon-prunable XID */
static void UHeapPruneRecordPrunable(UPruneState *prstate, TransactionId xid)
{
    /*
     * This should exactly match the PageSetPrunable macro.  We can't store
     * directly into the page header yet, so we update working state.
     */
    Assert(TransactionIdIsNormal(xid));
    if (!TransactionIdIsValid(prstate->new_prune_xid) || TransactionIdPrecedes(xid, prstate->new_prune_xid))
        prstate->new_prune_xid = xid;
}

/* Record item pointer to be marked dead */
static void UHeapPruneRecordDead(UPruneState *prstate, OffsetNumber offnum, Relation relation)
{
    Assert(prstate->ndead < MaxUHeapTuplesPerPage(relation));
    prstate->nowdead[prstate->ndead] = offnum;
    prstate->ndead++;
    Assert(offnum < MaxUHeapTuplesPerPage(relation) + 1);
    Assert(!prstate->marked[offnum]);
    prstate->marked[offnum] = true;
}

/* Record item pointer to be deleted */
static void UHeapPruneRecordDeleted(UPruneState *prstate, OffsetNumber offnum, Relation relation)
{
    Assert(prstate->ndead < MaxUHeapTuplesPerPage(relation));
    prstate->nowdeleted[prstate->ndeleted] = offnum;
    prstate->ndeleted++;
    Assert(offnum < MaxUHeapTuplesPerPage(relation) + 1);
    Assert(!prstate->marked[offnum]);
    prstate->marked[offnum] = true;
}


/*
 * After removing or marking some line pointers unused, move the tuples to
 * remove the gaps caused by the removed items.  Here, we are rearranging
 * the page such that tuples will be placed in itemid order.  It will help
 * in the speedup of future sequential scans.
 *
 * If targetOffnum is a valid offset, then it means the caller is
 * extending the size of that tuple. Because of that, we cant extend the tuple in place
 * because the extension could overwrite the tuple next to it.
 * A solution to this is compactify all the tuples except the one at targetOffnum.
 * The tuple at targetOffnum is copied last instead.
 */
static void CompactifyTuples(itemIdSort itemidbase, int nitems, Page page, OffsetNumber targetOffnum)
{
    UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;

    /* We copy over the targetOffnum last */
    union {
        UHeapDiskTupleData hdr;
        char data[MaxPossibleUHeapTupleSize];
    } tbuf;
    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");
    UHeapTupleData tuple;
    tuple.disk_tuple = &(tbuf.hdr);
    tuple.disk_tuple_size = 0;
    uint16 newTupleLen = 0;

    if (targetOffnum != InvalidOffsetNumber) {
        RowPtr *rp = UPageGetRowPtr(page, targetOffnum);
        Assert(RowPtrIsNormal(rp));
        Assert(sizeof(tbuf) >= rp->len);

        tuple.disk_tuple_size = rp->len;
        UHeapDiskTuple item = (UHeapDiskTuple)UPageGetRowData(page, rp);
        errno_t rc = memcpy_s((char *)tuple.disk_tuple, tuple.disk_tuple_size, (char *)item, tuple.disk_tuple_size);
        securec_check(rc, "", "");
    }

    /* sort itemIdSortData array into decreasing itemoff order */
    qsort((char *)itemidbase, nitems, sizeof(itemIdSortData), Itemoffcompare);

    Offset curr = phdr->pd_special;
    for (int i = 0; i < nitems; i++) {
        itemIdSort itemidptr = &itemidbase[i];

        if (targetOffnum == itemidptr->offsetindex + 1) {
            newTupleLen = itemidptr->alignedlen;
            continue;
        }

        RowPtr *lp = UPageGetRowPtr(page, itemidptr->offsetindex + 1);
        curr -= itemidptr->alignedlen;

        errno_t rc = memmove_s((char *)page + curr, itemidptr->alignedlen, (char *)page + itemidptr->itemoff,
            itemidptr->alignedlen);
        securec_check(rc, "\0", "\0");

        lp->offset = curr;
    }

    if (targetOffnum != InvalidOffsetNumber) {
        curr -= newTupleLen;
        errno_t rc = memcpy_s((char *)page + curr, newTupleLen, (char *)tuple.disk_tuple, tuple.disk_tuple_size);
        securec_check(rc, "", "");

        RowPtr *rp = UPageGetRowPtr(page, targetOffnum);
        rp->offset = curr;
    }

    phdr->pd_upper = curr;
}

/*
 * UPageRepairFragmentation
 *
 * Frees fragmented space on a page.
 *
 * The basic idea is same as PageRepairFragmentation, but here we additionally
 * deal with unused items that can't be immediately reclaimed.  We don't allow
 * page to be pruned, if there is an inplace update from an open transaction.
 * The reason is that we don't know the size of previous row in undo which
 * could be bigger in which case we might not be able to perform rollback once
 * the page is repaired.  Now, we can always traverse the undo chain to find
 * the size of largest tuple in the chain, but we don't do that for now as it
 * can take time especially if there are many such tuples on the page.
 *
 * The unusedSet boolean argument is used to prevent re-evaluation of
 * itemId when it is already set with transaction slot information in the
 * caller function.
 */
void UPageRepairFragmentation(Buffer buffer, OffsetNumber targetOffnum, Size spaceRequired, bool *pruned,
    bool unusedSet)
{
    Page page = BufferGetPage(buffer);
    uint16 pdLower = ((UHeapPageHeaderData *)page)->pd_lower;
    uint16 pdUpper = ((UHeapPageHeaderData *)page)->pd_upper;
    uint16 pdSpecial = ((UHeapPageHeaderData *)page)->pd_special;
    itemIdSortData itemidbase[MaxPossibleUHeapTuplesPerPage];
    itemIdSort itemidptr;
    RowPtr *lp = NULL;
    int nline;
    int nstorage;
    int nunused;
    int i;
    Size totallen;

    /*
     * It's worth the trouble to be more paranoid here than in most places,
     * because we are about to reshuffle data in (what is usually) a shared
     * disk buffer.  If we aren't careful then corrupted pointers, lengths,
     * etc. could cause us to clobber adjacent disk buffers, spreading the
     * data loss further.  So, check everything.
     */
    if (pdLower < (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData((UHeapPageHeaderData *)page)) || pdLower > pdUpper ||
        pdUpper > pdSpecial || pdSpecial > BLCKSZ || pdSpecial != MAXALIGN(pdSpecial)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", pdLower, pdUpper, pdSpecial)));
    }

    nline = UHeapPageGetMaxOffsetNumber(page);

    WHITEBOX_TEST_STUB(UHEAP_REPAIR_FRAGMENTATION_FAILED, WhiteboxDefaultErrorEmit);

    /*
     * If there are any tuples which are inplace updated by any open
     * transactions we shall not compactify the page contents, otherwise,
     * rollback of those transactions will not be possible.  There could be a
     * case, where within a transaction tuple is first inplace updated and
     * then, either updated or deleted. So for now avoid compaction if there
     * are any tuples which are marked inplace updated, updated or deleted by
     * an open transaction.
     */
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = UPageGetRowPtr(page, i);
        if (RowPtrIsUsed(lp) && RowPtrHasStorage(lp)) {
            UHeapDiskTuple tup = (UHeapDiskTuple)UPageGetRowData(page, lp);
            if (!(tup->flag & (UHEAP_INPLACE_UPDATED | UHEAP_UPDATED | UHEAP_DELETED))) {
                continue;
            }
            if (!UHeapTupleHasInvalidXact(tup->flag)) {
                UHeapTupleTransInfo txactinfo;
                txactinfo.td_slot = UHeapTupleHeaderGetTDSlot(tup);
                if (txactinfo.td_slot == UHEAPTUP_SLOT_FROZEN) {
                    continue;
                }

                /*
                 * XXX There is possibility that the updater's slot got reused
                 * by a locker in such a case the INVALID_XACT will be moved
                 * to lockers undo.  Now, we will find that the tuple has
                 * in-place update flag but it doesn't have INVALID_XACT flag
                 * and the slot transaction is also running, in such case we
                 * will not prune this page.  Ideally if the multi-locker is
                 * set we can get the actual transaction and check the status
                 * of the transaction.
                 */
                GetTDSlotInfo(buffer, txactinfo.td_slot, &txactinfo);

                /*
                 * It is quite possible that the item is showing some valid
                 * transaction slot, but actual slot has been frozen. This can
                 * happen when the tuple is in active state but the entry's xid
                 * is older than oldestXidInUndo 
                 */
                if (txactinfo.td_slot == UHEAPTUP_SLOT_FROZEN) {
                    continue;
                }
                /* For parallel replay, clog of txactinfo.xid may not finish replay */
                if (t_thrd.xlog_cxt.InRecovery && get_real_recovery_parallelism() > 1)
                    ;
                else if (!UHeapTransactionIdDidCommit(txactinfo.xid)) {
                    return;
                }
            }
        }
    }

    /*
     * Run through the line pointer array and collect data about live items.
     */
    itemidptr = itemidbase;
    nunused = totallen = 0;
    for (i = FirstOffsetNumber; i <= nline; i++) {
        lp = UPageGetRowPtr(page, i);
        if (RowPtrIsUsed(lp)) {
            if (RowPtrHasStorage(lp)) {
                itemidptr->offsetindex = i - 1;
                itemidptr->itemoff = RowPtrGetOffset(lp);
                if (unlikely(itemidptr->itemoff < (int)pdUpper || itemidptr->itemoff >= (int)pdSpecial)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED), errmsg("corrupted item pointer: %u", itemidptr->itemoff)));
                }
                /*
                 * We need to save additional space for the target offset, so
                 * that we can save the space for new tuple.
                 */
                if (i == targetOffnum) {
                    itemidptr->alignedlen = SHORTALIGN(RowPtrGetLen(lp) + spaceRequired);
                } else {
                    itemidptr->alignedlen = SHORTALIGN(RowPtrGetLen(lp));
                }
                totallen += itemidptr->alignedlen;
                itemidptr++;
            }
        } else {
            nunused++;
            /*
             * We allow Unused entries to be reused only if there is no
             * transaction information for the entry or the transaction is
             * committed.
             */
            if (RowPtrHasPendingXact(lp)) {
                UHeapTupleTransInfo txactinfo;

                /*
                 * If unusedSet is true, it means that itemIds are already
                 * set unused with transaction slot information by the caller
                 * and we should not clear it.
                 */
                if (unusedSet) {
                    continue;
                }
                txactinfo.td_slot = RowPtrGetTDSlot(lp);

                /*
                 * Here, we are relying on the transaction information in slot
                 * as if the corresponding slot has been reused, then
                 * transaction information from the entry would have been
                 * cleared.  See PageFreezeTransSlots.
                 */
                if (txactinfo.td_slot != UHEAPTUP_SLOT_FROZEN) {
                    GetTDSlotInfo(buffer, txactinfo.td_slot, &txactinfo);

                    /*
                     * It is quite possible that the item is showing some
                     * valid transaction slot, but actual slot has been
                     * frozen. This can happen when the tuple is in active state 
                     * but the entry's xid is older than oldestXidInUndo 
                     */
                    if (txactinfo.td_slot != UHEAPTUP_SLOT_FROZEN && !UHeapTransactionIdDidCommit(txactinfo.xid)) {
                        continue;
                    }
                }
            }

            /* Unused entries should have lp_len = 0, but make sure */
            RowPtrSetUnused(lp);
        }
    }

    nstorage = itemidptr - itemidbase;
    if (nstorage == 0) {
        /* Page is completely empty, so just reset it quickly */
        ((UHeapPageHeaderData *)page)->pd_upper = pdSpecial;
    } else {
        /* Need to compact the page the hard way */
        if (totallen > (Size)(pdSpecial - pdLower)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(
                "corrupted item lengths: total %u, available space %u", (unsigned int)totallen, pdSpecial - pdLower)));
        }

        CompactifyTuples(itemidbase, nstorage, page, targetOffnum);
    }

    /* Set hint bit for PageAddItem */
    if (nunused > 0) {
        UPageSetHasFreeLinePointers(page);
    } else {
        UPageClearHasFreeLinePointers(page);
    }

    /* indicate that the page has been pruned */
    if (pruned) {
        *pruned = true;
    }
}
