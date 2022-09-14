/* -------------------------------------------------------------------------
 *
 * knl_uvacuumlazy.cpp
 * 	  Concurrent ("lazy") vacuuming.
 *
 *
 * The lazy vacuum in uheap is similar to heap such that it uses three-passes to
 * clean up the dead tuples in data page and index. First pass is to mark dead item
 * pointers as DEAD, not UNUSED because these item pointers could still have index entries
 * pointing to them. The second pass is to vacuum the index. Third pass is to set DEAD
 * item pointers as UNUSED so that other backends can start using them.
 *
 * The other important aspect that is ensured in this system is that we don't
 * item ids that are marked as unused to be reused till the transaction that
 * has marked them unused is committed.
 *
 * The dead tuple tracking works in the same way as in heap.  See lazyvacuum.c.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uvacuumlazy.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_utuple.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/gtm.h"
#include "access/csnlog.h"
#include "catalog/storage.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "storage/buf/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"

/* A few variables that don't seem worth passing around as parameters */
static THR_LOCAL int elevel = -1;

static THR_LOCAL BufferAccessStrategy vac_strategy;

/*
 * Guesstimate the number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES MaxPossibleUHeapTuplesPerPage

/* non-export function prototypes */
static int LazyVacuumUPage(Relation onerel, BlockNumber blkno, Buffer buffer, int tupindex, LVRelStats *vacrelstats,
    TransactionId oldestXmin);
static void LazySpaceUalloc(LVRelStats *vacrelstats, BlockNumber relblocks, Relation onerel);
static IndexBulkDeleteResult **LazyScanUHeap(Relation onerel, LVRelStats *vacrelstats, VacuumStmt *vacstmt,
    Relation *Irel, int nindexes, TransactionId oldestXmin);
static void LazyVacuumUHeap(Relation onerel, LVRelStats *vacrelstats, TransactionId oldestXmin);

/*
 * 	LazyVacuumUPage() -- free dead tuples on a page
 * 					 and repair its fragmentation.
 *
 * Caller must hold pin and buffer exclusive lock on the buffer.
 *
 * tupindex is the index in vacrelstats->dead_tuples of the first dead
 * tuple for this page.  We assume the rest follow sequentially.
 * The return value is the first tupindex after the tuples of this page.
 */
static int LazyVacuumUPage(Relation onerel, BlockNumber blkno, Buffer buffer, int tupindex, LVRelStats *vacrelstats,
    TransactionId oldestXmin)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber unused[MaxOffsetNumber];
    int uncnt = 0;
    bool pruned = false;

    WHITEBOX_TEST_STUB(UHEAP_LAZY_VACUUM_FAILED, WhiteboxDefaultErrorEmit);

    START_CRIT_SECTION();

    for (; tupindex < vacrelstats->num_dead_tuples; tupindex++) {
        BlockNumber tblk = ItemPointerGetBlockNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupindex]));
        if (tblk != blkno) {
            break; /* past end of tuples for this block */
        }
        OffsetNumber toff = ItemPointerGetOffsetNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupindex]));
        RowPtr *itemid = UPageGetRowPtr(page, toff);
        RowPtrSetUnused(itemid);
        unused[uncnt++] = toff;
    }

    UPageRepairFragmentation(onerel, buffer, InvalidOffsetNumber, 0, &pruned, false);

    /*
     * Mark buffer dirty before we write WAL.
     */
    MarkBufferDirty(buffer);

    if (RelationNeedsWAL(onerel)) {
        XLogRecPtr recptr = LogUHeapClean(onerel, buffer, InvalidOffsetNumber, 0, NULL, 0, NULL, 0, unused,
            uncnt, NULL, NULL, 0, vacrelstats->latestRemovedXid, pruned);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    return tupindex;
}

/*
 * LazyVacuumUHeap()
 *
 * This routine marks dead tuples as unused and compacts out free
 * space on their pages.  Pages not having dead tuples recorded from
 * lazy_scan_uheap are not visited at all.
 *
 * Note: the reason for doing this as a second pass is we cannot remove
 * the tuples until we've removed their index entries, and we want to
 * process index entry removal in batches as large as possible.
 * This is the same as lazy_vacuum_heap but on u tuples.
 */
static void LazyVacuumUHeap(Relation onerel, LVRelStats *vacrelstats, TransactionId oldestXmin)
{
    PGRUsage ru0;
    pg_rusage_init(&ru0);

    int npages = 0;
    int tupIndex = 0;

    while (tupIndex < vacrelstats->num_dead_tuples) {
        vacuum_delay_point();

        BlockNumber tblk = ItemPointerGetBlockNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupIndex]));
        Buffer buf = ReadBufferExtended(onerel, MAIN_FORKNUM, tblk, RBM_NORMAL, vac_strategy);
        if (!ConditionalLockBuffer(buf)) {
            ReleaseBuffer(buf);
            ++tupIndex;
            continue;
        }
        tupIndex = LazyVacuumUPage(onerel, tblk, buf, tupIndex, vacrelstats, oldestXmin);

        /* Now that we've compacted the page, record its available space */
        Page page = BufferGetPage(buf);
        Size freespace = PageGetUHeapFreeSpace(page);

        UnlockReleaseBuffer(buf);
        RecordPageWithFreeSpace(onerel, tblk, freespace);
        npages++;
    }

    ereport(elevel,
        (errmsg("\"%s\": removed %d row versions in %d pages", RelationGetRelationName(onerel), tupIndex, npages),
        errdetail("%s.", pg_rusage_show(&ru0))));
}


/*
 * 	LazyScanUHeap() -- scan an open u relation
 *
 * 		This routine prunes each page in the table, which will among other
 * 		things truncate dead tuples to dead line pointers, truncate recently
 * 		dead tuples to deleted line pointers and defragment the page
 * 		(see UHeapPagePrune).  It also builds lists of dead tuples and pages
 * 		with free space, calculates statistics on the number of live tuples in
 * 		the relation. It then reclaims all dead line pointers and write undo for
 * 		each of them, so that it can rollback the operation if there is any error.
 * 		Invoke vacuuming of indexes when done, or when we run low on space for
 * 		dead-tuple TIDs.
 *
 * 		We also need to ensure that the heap-TIDs won't get reused till the
 * 		transaction that has performed this vacuum is committed.  To achieve
 * 		that, we need to store transaction slot information in the line
 * 		pointers that are marked unused in the first-pass of heap.
 *
 * 		If there are no indexes then we can reclaim line pointers without
 * 		writing any undo;
 */
static IndexBulkDeleteResult **LazyScanUHeap(Relation onerel, LVRelStats *vacrelstats, VacuumStmt *vacstmt,
    Relation *Irel, int nindexes, TransactionId oldestXmin)
{
    UHeapTupleData tuple;
    BlockNumber emptyPages;
    BlockNumber vacuumedPages;
    double numTuples;
    double tupsVacuumed;
    double nkeep;
    double nunused;
    IndexBulkDeleteResult **indstats;
    StringInfoData infobuf;
    int i;
    int tupindex = 0;
    PGRUsage ru0;
    BlockNumber nblocks = RelationGetNumberOfBlocks(onerel);
    BlockNumber blkno = InvalidBlockNumber;
    BlockNumber scanStartingBlock = 0;
    RelationBuffer relbuf = {onerel, InvalidBuffer};
    bool gpiVacuumed = false;

    pg_rusage_init(&ru0);

    char *relname = RelationGetRelationName(onerel);
    ereport(elevel, (errmsg("vacuuming uheap \"%s.%s\" oldestXmin:%ld",
        get_namespace_name(RelationGetNamespace(onerel)), relname, oldestXmin)));

    emptyPages = vacuumedPages = 0;
    numTuples = tupsVacuumed = nkeep = nunused = 0;

    indstats = (IndexBulkDeleteResult **)palloc0(nindexes * sizeof(IndexBulkDeleteResult *));

    vacrelstats->rel_pages = nblocks;
    vacrelstats->new_visible_pages = 0;
    vacrelstats->scanned_pages = 0;
    vacrelstats->tupcount_pages = 0;
    vacrelstats->nonempty_pages = 0;
    vacrelstats->latestRemovedXid = InvalidTransactionId;

    LazySpaceUalloc(vacrelstats, nblocks, onerel);

    WHITEBOX_TEST_STUB(UHEAP_LAZY_SCAN_FAILED, WhiteboxDefaultErrorEmit);

    for (blkno = scanStartingBlock; blkno < nblocks; blkno++) {
        Buffer buf;
        Page page;
        TransactionId xid = InvalidTransactionId;
        OffsetNumber offnum;
        OffsetNumber maxoff;
        Size freespace;
        bool tupgone = false;
        bool hastup = false;
        bool has_dead_tuples;

        /* IO collector and IO scheduler for vacuum */
#ifdef ENABLE_MULTIPLE_NODES
        if (ENABLE_WORKLOAD_CONTROL) {
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
        }
#endif
        vacuum_delay_point();

        /*
         * If we are close to overrunning the available space for dead-tuple
         * TIDs, pause and do a cycle of vacuuming before we tackle this page.
         */
        if ((vacrelstats->max_dead_tuples - vacrelstats->num_dead_tuples) < MaxUHeapTuplesPerPage(onerel) &&
            vacrelstats->num_dead_tuples > 0) {
            /* Log cleanup info before we touch indexes */
            vacuum_log_cleanup_info(onerel, vacrelstats);

            /* Remove index entries. */
            for (i = 0; i < nindexes; i++) {
                if (!RelationIsGlobalIndex(Irel[i])) {
                    lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
                } else if (!vacstmt->gpi_vacuumed) {
                    lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
                    gpiVacuumed = true;
                }
            }

            /* Remove tuples from heap */
            LazyVacuumUHeap(onerel, vacrelstats, oldestXmin);

            /*
             * Forget the now-vacuumed tuples, and press on, but be careful
             * not to reset latestRemovedXid since we want that value to be
             * valid.
             */
            tupindex = 0;
            vacrelstats->num_dead_tuples = 0;
            vacrelstats->num_index_scans++;
        }

        buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno, RBM_NORMAL, vac_strategy);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        vacrelstats->scanned_pages++;
        vacrelstats->tupcount_pages++;

        page = BufferGetPage(buf);
        if (PageIsNew(page)) {
            /*
             * An all-zeros page could be left over if a backend extends the
             * relation but crashes before initializing the page, or when
             * bulk-extending the relation (which creates a number of empty
             * pages at the tail end of the relation, but enters them into the
             * FSM)Reclaim such pages for use.  See the similar code in
             * lazy_scan_heap to know why we have used relation extension
             * lock.
             */
            Size freespace = 0;

            emptyPages++;

            /*
             * Perform checking of FSM after releasing lock, the fsm is
             * approximate, after all.
             */
            UnlockReleaseBuffer(buf);

            if (GetRecordedFreeSpace(onerel, blkno) == 0) {
                freespace = BufferGetPageSize(buf) - SizeOfUHeapPageHeaderData;
            }
            if (freespace > 0) {
                RecordPageWithFreeSpace(onerel, blkno, freespace);
                elog(DEBUG1, "relation \"%s\" page %u is uninitialized and not in fsm, fixing", relname, blkno);
            }
            continue;
        }

        if (UPageIsEmpty((UHeapPageHeaderData *)page)) {
            emptyPages++;
            freespace = PageGetUHeapFreeSpace(page);
            UnlockReleaseBuffer(buf);
            RecordPageWithFreeSpace(onerel, blkno, freespace);
            continue;
        }

        /*
         * We count tuples removed by the pruning step as removed by VACUUM.
         */
        relbuf.buffer = buf;
        tupsVacuumed += UHeapPagePruneGuts(onerel, &relbuf, oldestXmin, InvalidOffsetNumber, 0, false, false,
            &vacrelstats->latestRemovedXid, NULL);

        /* Now scan the page to collect vacuumable items. */
        hastup = false;
        freespace = 0;
        maxoff = UHeapPageGetMaxOffsetNumber(page);
        has_dead_tuples = false;

        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            RowPtr *itemid = UPageGetRowPtr(page, offnum);

            /* Unused items require no processing, but we count 'em */
            if (!RowPtrIsUsed(itemid)) {
                nunused += 1;
                continue;
            }

            /* Deleted items mustn't be touched */
            if (RowPtrIsDeleted(itemid)) {
                hastup = true; /* this page cannot be truncated */
                continue;
            }

            ItemPointerSet(&(tuple.ctid), blkno, offnum);

            /*
             * DEAD item pointers are to be vacuumed normally; but we don't
             * count them in tupsVacuumed, else we'd be double-counting (at
             * least in the common case where uheap_page_prune_guts() just
             * freed up a tuple).
             */
            if (RowPtrIsDead(itemid)) {
                lazy_record_dead_tuple(vacrelstats, &(tuple.ctid));
                continue;
            }

            Assert(RowPtrIsNormal(itemid));

            tuple.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, itemid);
            tuple.disk_tuple_size = RowPtrGetLen(itemid);
            tuple.table_oid = RelationGetRelid(onerel);
            tuple.xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
            tupgone = false;

            switch (UHeapTupleSatisfiesOldestXmin(&tuple, oldestXmin, buf, false, NULL, &xid,
                NULL, onerel)) { // UHeapPruneItem
                case UHEAPTUPLE_DEAD:
                    /*
                     * Ordinarily, DEAD tuples would have been removed by
                     * uheap_page_prune_guts(), but it's possible that the
                     * tuple state changed since heap_page_prune() looked. In
                     * particular an INSERT_IN_PROGRESS tuple could have
                     * changed to DEAD if the inserter aborted.  So this
                     * cannot be considered an error condition.
                     */
                    tupgone = true; /* we can delete the tuple */
                    break;
                case UHEAPTUPLE_LIVE:
                    break;
                case UHEAPTUPLE_RECENTLY_DEAD:
                    /*
                     * If tuple is recently deleted then we must not remove it
                     * from relation.
                     */
                    nkeep += 1;
                    break;
                case UHEAPTUPLE_INSERT_IN_PROGRESS:
                case UHEAPTUPLE_DELETE_IN_PROGRESS:
                    break;
                case UHEAPTUPLE_ABORT_IN_PROGRESS:
                    /*
                     * We can simply skip the tuple if it has
                     * inserted/operated by some aborted transaction and its
                     * rollback is still pending. It'll be taken care of by
                     * future vacuum calls.
                     */
                    break;
                default:
                    elog(ERROR, "unexpected UHeapTupleSatisfiesOldestXmin result");
                    break;
            }

            if (tupgone) {
                lazy_record_dead_tuple(vacrelstats, &(tuple.ctid));
                UHeapTupleHeaderAdvanceLatestRemovedXid(tuple.disk_tuple, xid, &vacrelstats->latestRemovedXid);
                tupsVacuumed += 1;
                has_dead_tuples = true;
            } else {
                numTuples += 1;
                hastup = true;
            }
        }

        /*
         * If there are no indexes then we can vacuum the page right now
         * instead of doing a second scan.
         */
        if ((vacrelstats->num_dead_tuples > 0) && (!vacrelstats->hasindex)) {
            /* Remove tuples from uheap */
            tupindex = LazyVacuumUPage(onerel, blkno, buf, tupindex, vacrelstats,
                oldestXmin); // Calls UPageRepairFragmentation
            has_dead_tuples = false;

            /*
             * Forget the now-vacuumed tuples, and press on, but be
             * careful not to reset latestRemovedXid since we want that
             * value to be valid.
             */
            vacrelstats->num_dead_tuples = 0;
            tupindex = 0;
            vacuumedPages++;
        }

        /* Now that we are done with the page, get its available space */
        freespace = PageGetUHeapFreeSpace(page);

        UnlockReleaseBuffer(buf);

        /* Remember the location of the last page with non-removable tuples */
        if (hastup) {
            vacrelstats->nonempty_pages = blkno + 1;
        }
        /* We're done with this page, so remember its free space as-is. */
        if (freespace) {
            RecordPageWithFreeSpace(onerel, blkno, freespace);
        }
    }

    /* save stats for use later */
    vacrelstats->tuples_deleted = tupsVacuumed;
    vacrelstats->new_dead_tuples = nkeep;

    /*
     * Now we can compute the new value for pg_class.reltuples.  To compensate
     * for metapage pass one less than the actual nblocks.
     */
    vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, nblocks, vacrelstats->tupcount_pages, numTuples);

    if (vacrelstats->num_dead_tuples > 0) {
        /* Log cleanup info before we touch indexes */
        vacuum_log_cleanup_info(onerel, vacrelstats);

        /* Remove index entries. */
        for (i = 0; i < nindexes; i++) {
            if (!RelationIsGlobalIndex(Irel[i])) {
                lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
            } else if (!vacstmt->gpi_vacuumed) {
                lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
                gpiVacuumed = true;
            }
        }
        /* Remove tuples from heap */
        LazyVacuumUHeap(onerel, vacrelstats, oldestXmin);
        vacrelstats->num_index_scans++;
    }

    if (gpiVacuumed) {
        vacstmt->gpi_vacuumed = true;
    }

    /* Do post-vacuum cleanup and statistics update for each index */
    for (i = 0; i < nindexes; i++) {
        if (RelationIsGlobalIndex(Irel[i]) && !gpiVacuumed) {
            continue;
        }
        /* IO collector and IO scheduler for vacuum */
#ifdef ENABLE_MULTIPLE_NODES
        if (ENABLE_WORKLOAD_CONTROL) {
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);
        }
#endif
        indstats[i] = lazy_cleanup_index(Irel[i], indstats[i], vacrelstats, vac_strategy);
    }

    pfree_ext(vacrelstats->dead_tuples);
    /*
     * This is pretty messy, but we split it up so that we can skip emitting
     * individual parts of the message when not applicable.
     */
    initStringInfo(&infobuf);
    appendStringInfo(&infobuf, _("%.0f dead row versions cannot be removed yet, oldest xmin: %lu\n"), nkeep,
        oldestXmin);
    appendStringInfo(&infobuf, _("There were %.0f unused item pointers.\n"), nunused);
    appendStringInfo(&infobuf, ngettext("%u page is entirely empty.\n", "%u pages are entirely empty.\n", emptyPages),
        emptyPages);
    appendStringInfo(&infobuf, _("%s."), pg_rusage_show(&ru0));

    ereport(elevel, (errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
        RelationGetRelationName(onerel), tupsVacuumed, numTuples, vacrelstats->scanned_pages, nblocks),
        errdetail_internal("%s", infobuf.data)));
    pfree(infobuf.data);
    return indstats;
}

static void LazyScanURel(Relation onerel, LVRelStats *vacrelstats, VacuumStmt *vacstmt, Relation *irel,
    int nindexes, TransactionId oldestXmin)
{
    IndexBulkDeleteResult **indstats = NULL;
    int idx;

    if (nindexes > 0) {
        vacrelstats->new_idx_pages = (BlockNumber*)palloc0(nindexes * sizeof(BlockNumber));
        vacrelstats->new_idx_tuples = (double*)palloc0(nindexes * sizeof(double));
        vacrelstats->idx_estimated = (bool*)palloc0(nindexes * sizeof(bool));
    }

    indstats = LazyScanUHeap(onerel, vacrelstats, vacstmt, irel, nindexes, oldestXmin);

    /* Vacuum the Free Space Map */
    FreeSpaceMapVacuum(onerel);

    for (idx = 0; idx < nindexes; idx++) {
        /* summarize the index status information */
        if (indstats[idx] != NULL) {
            vacrelstats->new_idx_pages[idx] = indstats[idx]->num_pages;
            vacrelstats->new_idx_tuples[idx] = indstats[idx]->num_index_tuples;
            vacrelstats->idx_estimated[idx] = indstats[idx]->estimated_count;
            pfree_ext(indstats[idx]);
        }
    }

    pfree_ext(indstats);
}


/*
 * ForceVacuumUHeapRelBypass() -- deal with force AutoVacuum for UStore
 *
 *  AutoVacuum will only apply to UStore by force vacuum for recycle clog
 *
 *  we just need to get a FrozenXid for this relation that all clog of xid less than
 *  that FrozenXid can be safely recycled.
 *
 *  Note: UStore with undo module can use oldestXidInUndo to ensure that every xid we
 *        found in heap page less than that value is already committed, otherwise we
 *        won't find it (rollback is already performed).
 */
static void ForceVacuumUHeapRelBypass(Relation onerel, VacuumStmt *vacstmt, BufferAccessStrategy bstrategy)
{
    Assert(IsAutoVacuumWorkerProcess());
    TransactionId newFrozenXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);

    /* now we only need to vacuum all indexes to make sure we won't have old xid remain there */
    /* STEP 1: construct fake vacrelstats context for index vacuum */
    LVRelStats* vacrelstats = (LVRelStats *)palloc0(sizeof(LVRelStats));

    vacrelstats->scanned_pages = 0;
    /* we won't vacuum heap, just set new pages and tuples to old values */
    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);
        vacrelstats->rel_pages = vacstmt->onepart->pd_part->relpages;
        vacrelstats->new_rel_tuples = vacstmt->onepart->pd_part->reltuples;
    } else {
        vacrelstats->rel_pages = onerel->rd_rel->relpages;
        vacrelstats->new_rel_tuples = onerel->rd_rel->reltuples;
    }

    /* STEP 2: open all indexes of the relation */
    int nindexes = 0;
    int nindexesGlobal = 0;
    Relation *irel = NULL;
    Relation *indexrel = NULL;
    Partition *indexpart = NULL;
    if (RelationIsPartition(onerel)) {
        vac_open_part_indexes(vacstmt, RowExclusiveLock, &nindexes, &nindexesGlobal, &irel, &indexrel, &indexpart);
    } else {
        vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &irel);
    }

    /* STEP 3: vacuum indexes */
    /* Do post-vacuum cleanup and statistics update for each index */
    bool gpiVacuumed = false;
    IndexBulkDeleteResult** indstats = (IndexBulkDeleteResult **)palloc0(nindexes * sizeof(IndexBulkDeleteResult *));
    for (int i = 0; i < nindexes; i++) {
        /* IO collector and IO scheduler for vacuum */
#ifdef ENABLE_MULTIPLE_NODES
        if (ENABLE_WORKLOAD_CONTROL) {
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);
        }
#endif
        if (!RelationIsGlobalIndex(irel[i])) {
            indstats[i] = lazy_cleanup_index(irel[i], indstats[i], vacrelstats, vac_strategy);
        } else if (!vacstmt->gpi_vacuumed) {
            indstats[i] = lazy_cleanup_index(irel[i], indstats[i], vacrelstats, vac_strategy);
            gpiVacuumed = true;
        }
    }
    if (gpiVacuumed) {
        vacstmt->gpi_vacuumed = true;
    }

    /* STEP 4: update FrozenXid for this relation */
    BlockNumber newRelAllvisible = 0; /* UStore don't have VisibilityMap */
    if (RelationIsPartition(onerel)) {
        vac_update_partstats(vacstmt->onepart, vacrelstats->rel_pages, vacrelstats->new_rel_tuples,
                             newRelAllvisible, newFrozenXid, InvalidMultiXactId);
        if (!vacstmt->issubpartition) {
            vac_update_pgclass_partitioned_table(vacstmt->onepartrel, vacstmt->onepartrel->rd_rel->relhasindex,
                                                 newFrozenXid, InvalidMultiXactId);
        } else {
            vac_update_pgclass_partitioned_table(vacstmt->parentpartrel, vacstmt->parentpartrel->rd_rel->relhasindex,
                                                 newFrozenXid, InvalidMultiXactId);
        }
    } else {
        Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
        vac_update_relstats(onerel, classRel, vacrelstats->rel_pages, vacrelstats->new_rel_tuples,
                            newRelAllvisible, nindexes > 0, newFrozenXid, InvalidMultiXactId);
        heap_close(classRel, RowExclusiveLock);
    }

    /* STEP 5: done with indexes */
    if (RelationIsPartition(onerel)) {
        vac_close_part_indexes(nindexes, nindexesGlobal, irel, indexrel, indexpart, NoLock);
    } else {
        vac_close_indexes(nindexes, irel, NoLock);
    }

    /* SETP 6: cleanup */
    for (int i = 0; i < nindexes; i++) {
        /* summarize the index status information */
        if (indstats[i] != NULL) {
            pfree_ext(indstats[i]);
        }
    }
    pfree_ext(indstats);
    pfree_ext(vacrelstats);
}

/*
 * 	lazy_vacuum_uheap_rel() -- perform LAZY VACUUM for one uheap relation
 */
void LazyVacuumUHeapRel(Relation onerel, VacuumStmt *vacstmt, BufferAccessStrategy bstrategy)
{
    int nindexesGlobal;
    LVRelStats *vacrelstats;
    Relation *irel = NULL;
    int nindexes;
    PGRUsage ru0;
    TimestampTz starttime = 0;
    long secs;
    int usecs;
    double readRate, writeRate;
    BlockNumber newRelPages;
    BlockNumber newRelAllvisible = 0;
    double newRelTuples;
    double newLiveTuples;
    Relation *indexrel = NULL;
    Partition *indexpart = NULL;

    /* the statFlag is used in PgStat_StatTabEntry, seen in pgstat_report_vacuum and pgstat_recv_vacuum */
    uint32 statFlag = InvalidOid;
    if (RelationIsSubPartitionOfSubPartitionTable(onerel)) {
        statFlag = onerel->grandparentId;
    } else if (RelationIsPartition(onerel)) {
        statFlag = onerel->parentId;
    }

    WHITEBOX_TEST_STUB(UHEAP_LAZY_VACUUM_REL_FAILED, WhiteboxDefaultErrorEmit);

    /* measure elapsed time iff autovacuum logging requires it */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        pg_rusage_init(&ru0);
        starttime = GetCurrentTimestamp();
    }

    if (vacstmt->options & VACOPT_VERBOSE) {
        elevel = INFO;
    } else {
        elevel = GetVacuumLogLevel();
    }

    vac_strategy = bstrategy;

    /*
     * We can't ignore processes running lazy vacuum on uheap relations
     * because like other backends operating on uheap, lazy vacuum also
     * reserves a transaction slot in the page for pruning purpose.
     */
    TransactionId oldestXmin = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    if (!TransactionIdIsNormal(oldestXmin)) {
        ereport(WARNING,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmodule(MOD_VACUUM),
                errmsg("globalRecycleXid(%lu) is abnormal.", oldestXmin),
                errdetail("N/A"),
                errcause("There is a high probability that the DML operation "
                "has not been performed on any ustore table. "),
                erraction("Check the value of globalRecycleXid in undo_cxt.")));
        return;
    }

    if (IsAutoVacuumWorkerProcess()) {
        /* must be force recycle, use another bypass */
        ForceVacuumUHeapRelBypass(onerel, vacstmt, bstrategy);
        return;
    }

    vacrelstats = (LVRelStats *)palloc0(sizeof(LVRelStats));

    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);
        vacrelstats->old_rel_pages = vacstmt->onepart->pd_part->relpages;
        vacrelstats->old_live_tuples = vacstmt->onepart->pd_part->reltuples;
    } else {
        vacrelstats->old_rel_pages = onerel->rd_rel->relpages;
        vacrelstats->old_live_tuples = onerel->rd_rel->reltuples;
    }
    vacrelstats->curr_heap_start = 0;
    vacrelstats->num_index_scans = 0;
    vacrelstats->pages_removed = 0;
    vacrelstats->lock_waiter_detected = false;

    /* Open all indexes of the relation */
    if (RelationIsPartition(onerel)) {
        vac_open_part_indexes(vacstmt, RowExclusiveLock, &nindexes, &nindexesGlobal, &irel, &indexrel, &indexpart);
    } else {
        vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &irel);
    }

    /* Always enable index vacuuming when the relation has an index. */
    vacrelstats->hasindex = (nindexes > 0);

    LazyScanURel(onerel, vacrelstats, vacstmt, irel, nindexes, oldestXmin);

    /*
     * Update statistics in pg_class.
     *
     * A corner case here is that if we scanned no pages at all because every
     * page is all-visible, we should not update relpages/reltuples, because
     * we have no new information to contribute.  In particular this keeps us
     * from replacing relpages=reltuples=0 (which means "unknown tuple
     * density") with nonzero relpages and reltuples=0 (which means "zero
     * tuple density") unless there's some actual evidence for the latter.
     *
     * We can use either tupcount_pages or scanned_pages for the check
     * described above as both the values should be same.  However, we use
     * earlier so as to be consistent with heap.
     *
     * Fixme: We do need to update relallvisible as in heap once we start
     * using visibilitymap or something equivalent to it.
     *
     * relfrozenxid/relminmxid are invalid as we don't perform freeze
     * operation in uheap.
     */
    newRelPages = vacrelstats->rel_pages;
    newRelTuples = vacrelstats->new_rel_tuples;
    if (vacrelstats->tupcount_pages == 0 && newRelPages > 0) {
        newRelPages = vacrelstats->old_rel_pages;
        newRelTuples = vacrelstats->old_rel_tuples;
    }
    newRelAllvisible = visibilitymap_count(onerel, NULL);
    if (newRelAllvisible > newRelPages) {
        newRelAllvisible = newRelPages;
    }

    TransactionId newFrozenXid = oldestXmin;

    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);

        vac_update_partstats(vacstmt->onepart, newRelPages, newRelTuples, newRelAllvisible, newFrozenXid,
            InvalidMultiXactId);
        /*
         * when vacuum partition, do not change the relhasindex field in pg_class
         * for partitioned table, as some partition may be altered as "all local
         * indexes unuable", in which case, setting the partitioned table as "no index"
         * will lead to misbehave when update other index usable partitions ---the horrible
         * misdguge as hot update even if update indexes columns.
         */
        if (!vacstmt->issubpartition) {
            vac_update_pgclass_partitioned_table(vacstmt->onepartrel, vacstmt->onepartrel->rd_rel->relhasindex,
                newFrozenXid, InvalidMultiXactId);
        } else {
            vac_update_pgclass_partitioned_table(vacstmt->parentpartrel, vacstmt->parentpartrel->rd_rel->relhasindex,
                newFrozenXid, InvalidMultiXactId);
        }

        /* update stats of local partition indexes */
        for (int idx = 0; idx < nindexes - nindexesGlobal; idx++) {
            if (vacrelstats->idx_estimated[idx]) {
                continue;
            }

            vac_update_partstats(indexpart[idx],
                vacrelstats->new_idx_pages[idx],
                vacrelstats->new_idx_tuples[idx],
                0,
                InvalidTransactionId,
                InvalidMultiXactId);

            vac_update_pgclass_partitioned_table(indexrel[idx], false, InvalidTransactionId, InvalidMultiXactId);
        }

        /* update stats of global partition indexes */
        Assert((nindexes - nindexesGlobal) >= 0);
        Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
        for (int idx = nindexes - nindexesGlobal; idx < nindexes; idx++) {
            if (vacrelstats->idx_estimated[idx]) {
                continue;
            }

            vac_update_relstats(irel[idx],
                classRel,
                vacrelstats->new_idx_pages[idx],
                vacrelstats->new_idx_tuples[idx],
                0,
                false,
                InvalidTransactionId,
                InvalidMultiXactId);
        }
        heap_close(classRel, RowExclusiveLock);
    } else {
        Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
        vac_update_relstats(onerel, classRel, newRelPages, newRelTuples, newRelAllvisible, nindexes > 0, newFrozenXid,
            InvalidMultiXactId);

        for (int idx = 0; idx < nindexes; idx++) {
            /* update index status */
            if (vacrelstats->idx_estimated[idx]) {
                continue;
            }

            vac_update_relstats(irel[idx],
                classRel,
                vacrelstats->new_idx_pages[idx],
                vacrelstats->new_idx_tuples[idx],
                0,
                false,
                InvalidTransactionId,
                InvalidMultiXactId);
        }
        heap_close(classRel, RowExclusiveLock);
    }

    /* Done with indexes */
    if (RelationIsPartition(onerel)) {
        vac_close_part_indexes(nindexes, nindexesGlobal, irel, indexrel, indexpart, NoLock);
    } else {
        vac_close_indexes(nindexes, irel, NoLock);
    }

    /* report results to the stats collector, too */
    newLiveTuples = newRelTuples - vacrelstats->new_dead_tuples;
    if (newLiveTuples < 0) {
        newLiveTuples = 0; /* just in case */
    }

    if (nindexes > 0) {
        pfree_ext(vacrelstats->new_idx_pages);
        pfree_ext(vacrelstats->new_idx_tuples);
        pfree_ext(vacrelstats->idx_estimated);
    }

    pgstat_report_vacuum(RelationGetRelid(onerel), statFlag, onerel->rd_rel->relisshared,
        vacrelstats->tuples_deleted);

    /* and log the action if appropriate */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        TimestampTz endtime = GetCurrentTimestamp();
        if (u_sess->attr.attr_storage.Log_autovacuum_min_duration == 0 ||
            TimestampDifferenceExceeds(starttime, endtime, u_sess->attr.attr_storage.Log_autovacuum_min_duration)) {
            StringInfoData buf;
            char *msgfmt = NULL;

            TimestampDifference(starttime, endtime, &secs, &usecs);

            readRate = 0;
            writeRate = 0;
            const int pageSize = (1024 * 1024);
            if ((secs > 0) || (usecs > 0)) {
                readRate =
                    (double)BLCKSZ * t_thrd.vacuum_cxt.VacuumPageMiss / pageSize / (secs + usecs / 1000000.0);
                writeRate =
                    (double)BLCKSZ * t_thrd.vacuum_cxt.VacuumPageDirty / pageSize / (secs + usecs / 1000000.0);
            }

            /*
             * This is pretty messy, but we split it up so that we can skip
             * emitting individual parts of the message when not applicable.
             */
            initStringInfo(&buf);
            msgfmt = _("automatic vacuum of table \"%s.%s.%s\": index scans: %d\n");
            appendStringInfo(&buf, msgfmt, get_database_name(u_sess->proc_cxt.MyDatabaseId),
                get_namespace_name(RelationGetNamespace(onerel)), RelationGetRelationName(onerel),
                vacrelstats->num_index_scans);
            /*
             * If enable_vm is set to off, then we lose track on all-visible pages info
             */
            appendStringInfo(&buf,
                _("pages: %u removed, %u remaining, %u scanned, %u new all-visible, %u total all-visible\n"),
                vacrelstats->pages_removed, vacrelstats->rel_pages, vacrelstats->scanned_pages,
                vacrelstats->new_visible_pages, newRelAllvisible);
            appendStringInfo(&buf,
                _("tuples: %.0f removed, %.0f remain, %.0f are dead but not yet removable, oldest xmin: %lu\n"),
                vacrelstats->tuples_deleted, vacrelstats->new_rel_tuples, vacrelstats->new_dead_tuples, oldestXmin);
            appendStringInfo(&buf, _("buffer usage: %d hits, %d misses, %d dirtied\n"), t_thrd.vacuum_cxt.VacuumPageHit,
                t_thrd.vacuum_cxt.VacuumPageMiss, t_thrd.vacuum_cxt.VacuumPageDirty);
            appendStringInfo(&buf, _("avg read rate: %.3f MB/s, avg write rate: %.3f MB/s\n"), readRate, writeRate);
            appendStringInfo(&buf, _("system usage: %s"), pg_rusage_show(&ru0));

            ereport(elevel, (errmsg_internal("%s", buf.data)));
            pfree(buf.data);
        }
    }

    pfree_ext(vacrelstats);
}

/*
 * lazy_space_ualloc - space allocation decisions for lazy vacuum
 *
 * See the comments at the head of this file for rationale.
 */
static void LazySpaceUalloc(LVRelStats *vacrelstats, BlockNumber relblocks, Relation onerel)
{
    unsigned long maxtuples;

    if (vacrelstats->hasindex) {
        maxtuples = (u_sess->attr.attr_memory.maintenance_work_mem * 1024L) / sizeof(VacItemPointerData);
        maxtuples = Min(maxtuples, INT_MAX);
        maxtuples = Min(maxtuples, MaxAllocSize / sizeof(VacItemPointerData));
        /* curious coding here to ensure the multiplication can't overflow */
        if ((BlockNumber)(maxtuples / LAZY_ALLOC_TUPLES) > relblocks)
            maxtuples = relblocks * LAZY_ALLOC_TUPLES;

        /* stay sane if small maintenance_work_mem */
        maxtuples = Max(maxtuples, (unsigned long)MaxUHeapTuplesPerPage(onerel));
    } else {
        maxtuples = MaxUHeapTuplesPerPage(onerel);
    }

    vacrelstats->num_dead_tuples = 0;
    vacrelstats->max_dead_tuples = (int)maxtuples;
    vacrelstats->dead_tuples = (VacItemPointer)palloc(maxtuples * sizeof(VacItemPointerData));
}
