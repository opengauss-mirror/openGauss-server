/* -------------------------------------------------------------------------
 *
 * pruneheap.cpp
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/heap/pruneheap.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "commands/vacuum.h"
#include "catalog/catalog.h"
#include "utils/snapmgr.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "storage/procarray.h"

/* Working data for heap_page_prune and subroutines */
typedef struct {
    TransactionId new_prune_xid;    /* new prune hint value for page */
    TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
    int nredirected;                /* numbers of entries in arrays below */
    int ndead;
    int nunused;
    /* arrays that accumulate indexes of items to be changed */
    OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
    OffsetNumber nowdead[MaxHeapTuplesPerPage];
    OffsetNumber nowunused[MaxHeapTuplesPerPage];
    /* marked[i] is TRUE if item i is entered in one of the above arrays */
    bool marked[MaxHeapTuplesPerPage + 1];
} PruneState;

/* Local functions */
static int heap_prune_chain(Relation relation, Buffer buffer, OffsetNumber rootoffnum, TransactionId oldest_xmin,
                            PruneState *prstate);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate, OffsetNumber offnum, OffsetNumber rdoffnum);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);

/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 *
 * OldestXmin is the cutoff XID used to distinguish whether tuples are DEAD
 * or RECENTLY_DEAD (see HeapTupleSatisfiesVacuum).
 */
void heap_page_prune_opt(Relation relation, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    Size minfree;
    TransactionId oldest_xmin;
    TransactionId CatalogXmin;
    /*
     * We can't write WAL in recovery mode, so there's no point trying to
     * clean the page. The master will likely issue a cleaning WAL record soon
     * anyway, so this is no particular loss.
     */
    if (RecoveryInProgress() || (ENABLE_DMS && SSIsServerModeReadOnly()))
        return;

    oldest_xmin = u_sess->utils_cxt.RecentGlobalXmin;
    if (IsCatalogRelation(relation) ||
        RelationIsAccessibleInLogicalDecoding(relation)) {
        CatalogXmin = GetReplicationSlotCatalogXmin();
        if (CatalogXmin != 0 && CatalogXmin < oldest_xmin) {
            oldest_xmin = CatalogXmin;
        }
    }


    Assert(TransactionIdIsValid(oldest_xmin));

    /*
     * Should not prune page when use local snapshot, InitPostgres e.g.
     * If use local snapshot RecentXmin might not consider xid cn send later,
     * wrong xid status might be judged in TransactionIdIsInProgress,
     * and wrong tuple infomask might be set in HeapTupleSatisfiesVacuum.
     * So if useLocalSnapshot is ture in postmaster env, we don't prune page.
     * Keep prune page can be done in single mode (standlone --single), so just in PostmasterEnvironment.
     */
    if ((t_thrd.xact_cxt.useLocalSnapshot && IsPostmasterEnvironment) ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster ||
        u_sess->attr.attr_common.upgrade_mode == 1 ||
        g_instance.streaming_dr_cxt.isInSwitchover)
        return;

    /*
     * Let's see if we really need pruning.
     *
     * Forget it if page is not hinted to contain something prunable that's
     * older than OldestXmin.
     */
    if (!PageIsPrunable(page, oldest_xmin))
        return;

    /*
     * We prune when a previous UPDATE failed to find enough space on the page
     * for a new tuple version, or when free space falls below the relation's
     * fill-factor target (but not less than 10%).
     *
     * Checking free space here is questionable since we aren't holding any
     * lock on the buffer; in the worst case we could get a bogus answer. It's
     * unlikely to be *seriously* wrong, though, since reading either pd_lower
     * or pd_upper is probably atomic.	Avoiding taking a lock seems more
     * important than sometimes getting a wrong answer in what is after all
     * just a heuristic estimate.
     */
    minfree = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);
    minfree = Max(minfree, BLCKSZ / 10);
    if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree) {
        /* OK, try to get exclusive buffer lock */
        if (!ConditionalLockBufferForCleanup(buffer))
            return;

        /*
         * Now that we have buffer lock, get accurate information about the
         * page's free space, and recheck the heuristic about whether to
         * prune. (We needn't recheck PageIsPrunable, since no one else could
         * have pruned while we hold pin.)
         */
        if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree) {
            TransactionId ignore = InvalidTransactionId; /* return value not needed */

            /* OK to prune */
            (void)heap_page_prune(relation, buffer, oldest_xmin, true, &ignore, true);
        }

        /* And release buffer lock */
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }
}

/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.
 *
 * OldestXmin is the cutoff XID used to distinguish whether tuples are DEAD
 * or RECENTLY_DEAD (see HeapTupleSatisfiesVacuum).
 *
 * If report_stats is true then we send the number of reclaimed heap-only
 * tuples to pgstats. (This must be FALSE during vacuum, since vacuum will
 * send its own new total to pgstats, and we don't want this delta applied
 * on top of that.)
 *
 * Returns the number of tuples deleted from the page and sets latestRemovedXid.
 */
int heap_page_prune(Relation relation, Buffer buffer, TransactionId oldest_xmin, bool report_stats,
                    TransactionId *latest_removed_xid, bool repair_fragmentation)
{
    int ndeleted = 0;
    Page page = BufferGetPage(buffer);
    OffsetNumber offnum, maxoff;
    PruneState prstate;
    errno_t rc;

    gstrace_entry(GS_TRC_ID_heap_page_prune);
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
    prstate.latestRemovedXid = *latest_removed_xid;
    prstate.nredirected = prstate.ndead = prstate.nunused = 0;
    rc = memset_s(prstate.marked, sizeof(prstate.marked), 0, sizeof(prstate.marked));
    securec_check(rc, "", "");
    /* Scan the page */
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;

        /* Ignore items already processed as part of an earlier chain */
        if (prstate.marked[offnum])
            continue;

        /* Nothing to do if slot is empty or already dead */
        itemid = HeapPageGetItemId(page, offnum);
        if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
            continue;

        /* Process this item or chain of items */
        ndeleted += heap_prune_chain(relation, buffer, offnum, oldest_xmin, &prstate);
    }

    /* Any error while applying the changes is critical */
    START_CRIT_SECTION();

    /* Have we found any prunable items? */
    if (prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0) {
        /*
         * Apply the planned item changes, then repair page fragmentation, and
         * update the page's hint bit about whether it has free line pointers.
         */
        heap_page_prune_execute(page, prstate.redirected, prstate.nredirected, prstate.nowdead, prstate.ndead,
                                prstate.nowunused, prstate.nunused, repair_fragmentation);

        /*
         * Update the page's pd_prune_xid field to either zero, or the lowest
         * XID of any soon-prunable tuple.
         */
        HeapPageSetPruneXid(page, prstate.new_prune_xid);

        /*
         * Also clear the "page is full" flag, since there's no point in
         * repeating the prune/defrag process until something else happens to
         * the page.
         */
        PageClearFull(page);

        MarkBufferDirty(buffer);

        /*
         * Emit a WAL HEAP_CLEAN record showing what we did
         */
        if (RelationNeedsWAL(relation)) {
            XLogRecPtr recptr;

            recptr = log_heap_clean(relation, buffer, prstate.redirected, prstate.nredirected, prstate.nowdead,
                                    prstate.ndead, prstate.nowunused, prstate.nunused, prstate.latestRemovedXid,
                                    repair_fragmentation);

            PageSetLSN(BufferGetPage(buffer), recptr);
        }
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
        if (HeapPageGetPruneXid(page) != prstate.new_prune_xid || PageIsFull(page)) {
            HeapPageSetPruneXid(page, prstate.new_prune_xid);
            PageClearFull(page);
            MarkBufferDirtyHint(buffer, true);
        }
    }

    END_CRIT_SECTION();

    /*
     * If requested, report the number of tuples reclaimed to pgstats. This is
     * ndeleted minus ndead, because we don't want to count a now-DEAD root
     * item as a deletion for this purpose.
     */
    if (report_stats && ndeleted > prstate.ndead)
        pgstat_update_heap_dead_tuples(relation, ndeleted - prstate.ndead);

    *latest_removed_xid = prstate.latestRemovedXid;

    /*
     * XXX Should we update the FSM information of this page ?
     *
     * There are two schools of thought here. We may not want to update FSM
     * information so that the page is not used for unrelated UPDATEs/INSERTs
     * and any free space in this page will remain available for further
     * UPDATEs in *this* page, thus improving chances for doing HOT updates.
     *
     * But for a large table and where a page does not receive further UPDATEs
     * for a long time, we might waste this space by not updating the FSM
     * information. The relation may get extended and fragmented further.
     *
     * One possibility is to leave "fillfactor" worth of space in this page
     * and update FSM with the remaining space.
     *
     * In any case, the current FSM implementation doesn't accept
     * one-page-at-a-time updates, so this is all academic for now.
     */
    gstrace_exit(GS_TRC_ID_heap_page_prune);
    return ndeleted;
}

/*
 * Prune specified item pointer or a HOT chain originating at that item.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain. We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, the OldestXmin test is just too coarse to detect it.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * OldestXmin is the cutoff XID used to identify dead tuples.
 *
 * We don't actually change the page here, except perhaps for hint-bit updates
 * caused by HeapTupleSatisfiesVacuum. We just add entries to the arrays in
 * prstate showing the changes to be made. Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int heap_prune_chain(Relation relation, Buffer buffer, OffsetNumber rootoffnum, TransactionId oldest_xmin,
                            PruneState *prstate)
{
    int ndeleted = 0;
    Page dp = (Page)BufferGetPage(buffer);
    TransactionId prior_xmax = InvalidTransactionId;
    ItemId rootlp;
    HeapTupleHeader htup;
    OffsetNumber latestdead = InvalidOffsetNumber;
    OffsetNumber maxoff = PageGetMaxOffsetNumber(dp);
    OffsetNumber offnum = InvalidOffsetNumber;
    OffsetNumber chainitems[MaxHeapTuplesPerPage];
    int nchain = 0;
    int i = 0;
    HeapTupleData tup;
    tup.t_tableOid = RelationGetRelid(relation);
    tup.t_bucketId = RelationGetBktid(relation);
    bool keepInvisible = false;

    gstrace_entry(GS_TRC_ID_heap_prune_chain);

    HeapTupleCopyBaseFromPage(&tup, dp);

    rootlp = PageGetItemId(dp, rootoffnum);

    /*
     * If it's a heap-only tuple, then it is not the start of a HOT chain.
     */
    if (ItemIdIsNormal(rootlp)) {
        htup = (HeapTupleHeader)PageGetItem(dp, rootlp);

        tup.t_data = htup;
        tup.t_len = ItemIdGetLength(rootlp);
        tup.t_tableOid = RelationGetRelid(relation);
        tup.t_bucketId = RelationGetBktid(relation);
        ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), rootoffnum);

        if (HeapTupleHeaderIsHeapOnly(htup)) {
            /*
             * If the tuple is DEAD and doesn't chain to anything else, mark
             * it unused immediately.  (If it does chain, we can only remove
             * it as part of pruning its chain.)
             *
             * We need this primarily to handle aborted HOT updates, that is,
             * XMIN_INVALID heap-only tuples.  Those might not be linked to by
             * any chain, since the parent tuple might be re-updated before
             * any pruning occurs.	So we have to be able to reap them
             * separately from chain-pruning.  (Note that
             * HeapTupleHeaderIsHotUpdated will never return true for an
             * XMIN_INVALID tuple, so this code will work even when there were
             * sequential updates within the aborted transaction.)
             *
             * Note that we might first arrive at a dead heap-only tuple
             * either here or while following a chain below.  Whichever path
             * gets there first will mark the tuple unused.
             */
            if (HeapTupleSatisfiesVacuum(&tup, oldest_xmin, buffer) == HEAPTUPLE_DEAD &&
                !HeapTupleHeaderIsHotUpdated(htup)) {

                if (HeapKeepInvisibleTuple(&tup, RelationGetDescr(relation))) {
                    return ndeleted;
                }

                heap_prune_record_unused(prstate, rootoffnum);
                HeapTupleHeaderAdvanceLatestRemovedXid(&tup, &prstate->latestRemovedXid);
                ndeleted++;
            }
            gstrace_exit(GS_TRC_ID_heap_prune_chain);
            /* Nothing more to do */
            return ndeleted;
        }
    }

    /* Start from the root tuple */
    offnum = rootoffnum;

    /* while not end of the chain */
    for (;;) {
        ItemId lp;
        bool tupdead = false;
        bool recent_dead = false;

        /* Some sanity checks */
        if (offnum < FirstOffsetNumber || offnum > maxoff) {
            break;
        }

        /* If item is already processed, stop --- it must not be same chain */
        if (prstate->marked[offnum]) {
            break;
        }

        lp = PageGetItemId(dp, offnum);
        /* Unused item obviously isn't part of the chain */
        if (!ItemIdIsUsed(lp)) {
            break;
        }

        /*
         * If we are looking at the redirected root line pointer, jump to the
         * first normal tuple in the chain.  If we find a redirect somewhere
         * else, stop --- it must not be same chain.
         */
        if (ItemIdIsRedirected(lp)) {
            if (nchain > 0) {
                break; /* not at start of chain */
            }
            chainitems[nchain++] = offnum;
            offnum = ItemIdGetRedirect(rootlp);
            continue;
        }

        /*
         * Likewise, a dead item pointer can't be part of the chain. (We
         * already eliminated the case of dead root tuple outside this
         * function.)
         */
        if (ItemIdIsDead(lp)) {
            break;
        }

        Assert(ItemIdIsNormal(lp));
        htup = (HeapTupleHeader)PageGetItem(dp, lp);
        tup.t_data = htup;
        tup.t_len = ItemIdGetLength(lp);
        ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), offnum);

        HeapTupleCopyBaseFromPage(&tup, dp);
        /*
         * Check the tuple XMIN against prior XMAX, if any
         */
        if (TransactionIdIsValid(prior_xmax) && !TransactionIdEquals(HeapTupleGetRawXmin(&tup), prior_xmax)) {
            break;
        }

        /*
         * OK, this tuple is indeed a member of the chain.
         */
        chainitems[nchain++] = offnum;

        /*
         * Check tuple's visibility status.
         */
        tupdead = recent_dead = false;
        switch (HeapTupleSatisfiesVacuum(&tup, oldest_xmin, buffer)) {
            case HEAPTUPLE_DEAD:
                keepInvisible = HeapKeepInvisibleTuple(&tup, RelationGetDescr(relation));
                tupdead = true;
                break;

            case HEAPTUPLE_RECENTLY_DEAD:
                recent_dead = true;

                /*
                 * This tuple may soon become DEAD.  Update the hint field so
                 * that the page is reconsidered for pruning in future.
                 */
                heap_prune_record_prunable(prstate, HeapTupleGetUpdateXid(&tup));
                break;

            case HEAPTUPLE_DELETE_IN_PROGRESS:

                /*
                 * This tuple may soon become DEAD.  Update the hint field so
                 * that the page is reconsidered for pruning in future.
                 */
                heap_prune_record_prunable(prstate, HeapTupleGetUpdateXid(&tup));
                break;

            case HEAPTUPLE_LIVE:
            case HEAPTUPLE_INSERT_IN_PROGRESS:

                /*
                 * If we wanted to optimize for aborts, we might consider
                 * marking the page prunable when we see INSERT_IN_PROGRESS.
                 * But we don't.  See related decisions about when to mark the
                 * page prunable in heapam.c.
                 */
                break;

            default:
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                break;
        }
        /*
         * Remember the last DEAD tuple seen.  We will advance past
         * RECENTLY_DEAD tuples just in case there's a DEAD one after them;
         * but we can't advance past anything else.  (XXX is it really worth
         * continuing to scan beyond RECENTLY_DEAD?  The case where we will
         * find another DEAD tuple is a fairly unusual corner case.)
         */
        if (tupdead) {
            latestdead = offnum;
            HeapTupleHeaderAdvanceLatestRemovedXid(&tup, &prstate->latestRemovedXid);
        } else if (!recent_dead) {
            break;
        }

        /*
         * If the tuple is not HOT-updated, then we are at the end of this
         * HOT-update chain.
         */
        if (!HeapTupleHeaderIsHotUpdated(htup)) {
            break;
        }

        /*
         * Advance to next chain member.
         */
        Assert(ItemPointerGetBlockNumber(&htup->t_ctid) == BufferGetBlockNumber(buffer));
        offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
        prior_xmax = HeapTupleGetUpdateXid(&tup);
    }

    /* There is only one dead tuple that needs to be retained and no processing is performed */
    if (keepInvisible && (nchain == 1)) {
        latestdead = InvalidOffsetNumber;
    }

    /*
     * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
     * the DEAD tuples at the start of the chain are removed and the root line
     * pointer is appropriately redirected.
     */
    if (OffsetNumberIsValid(latestdead)) {
        /*
         * Mark as unused each intermediate item that we are able to remove
         * from the chain.
         *
         * When the previous item is the last dead tuple seen, we are at the
         * right candidate for redirection.
         */
        for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++) {
            // The entire chain is dead, but need to keep invisble tuple
            if (keepInvisible && (i == nchain - 1)) {
                break;
            }
            heap_prune_record_unused(prstate, chainitems[i]);
            ndeleted++;
        }

        /*
         * If the root entry had been a normal tuple, we are deleting it, so
         * count it in the result.	But changing a redirect (even to DEAD
         * state) doesn't count.
         */
        if (ItemIdIsNormal(rootlp)) {
            ndeleted++;
        }

        /*
         * If the DEAD tuple is at the end of the chain, the entire chain is
         * dead and the root line pointer can be marked dead.  Otherwise just
         * redirect the root to the correct chain member.
         */
        if (i >= nchain) {
            heap_prune_record_dead(prstate, rootoffnum);
        } else {
            heap_prune_record_redirect(prstate, rootoffnum, chainitems[i]);
        }
    } else if (nchain < 2 && ItemIdIsRedirected(rootlp)) {
        /*
         * We found a redirect item that doesn't point to a valid follow-on
         * item.  This can happen if the loop in heap_page_prune caused us to
         * visit the dead successor of a redirect item before visiting the
         * redirect item.  We can clean up by setting the redirect item to
         * DEAD state.
         */
        heap_prune_record_dead(prstate, rootoffnum);
    }
    gstrace_exit(GS_TRC_ID_heap_prune_chain);
    return ndeleted;
}

/* Record lowest soon-prunable XID */
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
    /*
     * This should exactly match the PageSetPrunable macro.  We can't store
     * directly into the page header yet, so we update working state.
     */
    Assert(TransactionIdIsNormal(xid));
    if (!TransactionIdIsValid(prstate->new_prune_xid) || TransactionIdPrecedes(xid, prstate->new_prune_xid)) {
        prstate->new_prune_xid = xid;
    }
}

/* Record item pointer to be redirected */
static void heap_prune_record_redirect(PruneState *prstate, OffsetNumber offnum, OffsetNumber rdoffnum)
{
    Assert(prstate->nredirected < MaxHeapTuplesPerPage);
    prstate->redirected[prstate->nredirected * 2] = offnum;
    prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
    prstate->nredirected++;
    Assert(!prstate->marked[offnum]);
    prstate->marked[offnum] = true;
    Assert(!prstate->marked[rdoffnum]);
    prstate->marked[rdoffnum] = true;
}

/* Record item pointer to be marked dead */
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum)
{
    Assert(prstate->ndead < MaxHeapTuplesPerPage);
    prstate->nowdead[prstate->ndead] = offnum;
    prstate->ndead++;
    Assert(!prstate->marked[offnum]);
    prstate->marked[offnum] = true;
}

/* Record item pointer to be marked unused */
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
    Assert(prstate->nunused < MaxHeapTuplesPerPage);
    prstate->nowunused[prstate->nunused] = offnum;
    prstate->nunused++;
    Assert(!prstate->marked[offnum]);
    prstate->marked[offnum] = true;
}

/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j,
 * then we set root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * We zero out all unused entries.
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
    OffsetNumber offnum, maxoff;

    errno_t ret = memset_s(root_offsets, MaxHeapTuplesPerPage * sizeof(OffsetNumber), 0,
                           MaxHeapTuplesPerPage * sizeof(OffsetNumber));
    securec_check(ret, "", "");

    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId lp = PageGetItemId(page, offnum);
        HeapTupleHeader htup;
        OffsetNumber nextoffnum;
        TransactionId prior_xmax;
        HeapTupleData tup;

        /* skip unused and dead items */
        if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
            continue;

        if (ItemIdIsNormal(lp)) {
            htup = (HeapTupleHeader)PageGetItem(page, lp);
            tup.t_data = htup;
            HeapTupleCopyBaseFromPage(&tup, page);

            /*
             * Check if this tuple is part of a HOT-chain rooted at some other
             * tuple. If so, skip it for now; we'll process it when we find
             * its root.
             */
            if (HeapTupleHeaderIsHeapOnly(htup))
                continue;

            /*
             * This is either a plain tuple or the root of a HOT-chain.
             * Remember it in the mapping.
             */
            root_offsets[offnum - 1] = offnum;

            /* If it's not the start of a HOT-chain, we're done with it */
            if (!HeapTupleHeaderIsHotUpdated(htup))
                continue;

            /* Set up to scan the HOT-chain */
            nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
            prior_xmax = HeapTupleGetUpdateXid(&tup);
        } else {
            /* Must be a redirect item. We do not set its root_offsets entry */
            Assert(ItemIdIsRedirected(lp));
            /* Set up to scan the HOT-chain */
            nextoffnum = ItemIdGetRedirect(lp);
            prior_xmax = InvalidTransactionId;
        }

        /*
         * Now follow the HOT-chain and collect other tuples in the chain.
         *
         * Note: Even though this is a nested loop, the complexity of the
         * function is O(N) because a tuple in the page should be visited not
         * more than twice, once in the outer loop and once in HOT-chain
         * chases.
         */
        for (;;) {
            lp = PageGetItemId(page, nextoffnum);
            /* Check for broken chains */
            if (!ItemIdIsNormal(lp))
                break;
            htup = (HeapTupleHeader)PageGetItem(page, lp);
            tup.t_data = htup;
            HeapTupleCopyBaseFromPage(&tup, page);

            if (TransactionIdIsValid(prior_xmax) && !TransactionIdEquals(prior_xmax, HeapTupleGetRawXmin(&tup)))
                break;

            /* Remember the root line pointer for this item */
            root_offsets[nextoffnum - 1] = offnum;

            /* Advance to next chain member, if any */
            if (!HeapTupleHeaderIsHotUpdated(htup))
                break;

            nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
            prior_xmax = HeapTupleGetUpdateXid(&tup);
        }
    }
}
