/* -------------------------------------------------------------------------
 *
 * vacuumlazy.cpp
 *	  Concurrent ("lazy") vacuuming.
 *
 *
 * The major space usage for LAZY VACUUM is storage for the array of dead
 * tuple TIDs, with the next biggest need being storage for per-disk-page
 * free space info.  We want to ensure we can vacuum even the very largest
 * relations with finite memory space usage.  To do that, we set upper bounds
 * on the number of tuples and pages we will keep track of at once.
 *
 * We are willing to use at most maintenance_work_mem memory space to keep
 * track of dead tuples.  We initially allocate an array of TIDs of that size,
 * with an upper limit that depends on table size (this limit ensures we don't
 * allocate a huge area uselessly for vacuuming small tables).	If the array
 * threatens to overflow, we suspend the heap scan phase and perform a pass of
 * index cleanup and page compaction, then resume the heap scan with an empty
 * TID array.
 *
 * If we're processing a table with no indexes, we can just vacuum each page
 * as we go; there's no need to save up multiple tuples to minimize the number
 * of index scans performed.  So we don't use maintenance_work_mem memory for
 * the TID array, just enough to hold as many heap tuples as fit on one page.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/vacuumlazy.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "access/cstore_am.h"
#include "access/cstore_insert.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/multixact.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "catalog/pg_hashbucket_fn.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "portability/instr_time.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "storage/buf/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"
#include "utils/partcache.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/commands_gstrace.h"
#include "access/ustore/knl_upage.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

/*
 * Guesstimation of number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES MaxHeapTuplesPerPage

/*
  * Space/time tradeoff parameters: do these need to be user-tunable?
  *
  * To consider truncating the relation, we want there to be at least
  * REL_TRUNCATE_MINIMUM or (relsize / REL_TRUNCATE_FRACTION) (whichever
  * is less) potentially-freeable pages.
  */
const int REL_TRUNCATE_MINIMUM = 1000;
const int REL_TRUNCATE_FRACTION = 16;

 /*
  * Timing parameters for truncate locking heuristics.
  *
  * These were not exposed as user tunable GUC values because it didn't seem
  * that the potential for improvement was great enough to merit the cost of
  * supporting them.
  */
const int AUTOVACUUM_TRUNCATE_LOCK_CHECK_INTERVAL = 20; /* ms */
const int AUTOVACUUM_TRUNCATE_LOCK_WAIT_INTERVAL = 50;              /* ms */
const int AUTOVACUUM_TRUNCATE_LOCK_TIMEOUT = 5000;                  /* ms */

/*
 * Before we consider skipping a page that's marked as clean in
 * visibility map, we must've seen at least this many clean pages.
 */
#define SKIP_PAGES_THRESHOLD ((BlockNumber)32)

#define CHANGE_XID_BASE (MaxShortTransactionId * 0.1)

typedef struct ValPrefetchList {
    uint32 block_guard; /* record last block id need to prefetch */
    uint32 count;       /* prefetch count */
    uint32 quantity;    /* prefetch quantity, max */
} ValPrefetchList;

typedef struct ValPrefetch {
    ValPrefetchList* fetchlist; /* point current used prefetch list */
    ValPrefetchList fetchlist1; /* fetchlist1 and fetchlist2 is ping pang list to keep prefetch blocks */
    ValPrefetchList fetchlist2;
    bool init; /* whether the prefetch list inited done or not */
} ValPrefetch;

/* A few variables that don't seem worth passing around as parameters */
static THR_LOCAL int elevel = -1;

static THR_LOCAL BufferAccessStrategy vac_strategy;

/* non-export function prototypes */
static IndexBulkDeleteResult** lazy_scan_heap(
    Relation onerel, LVRelStats* vacrelstats, Relation* Irel, int nindexes,
    bool scan_all, double* ptrDeleteTupleNum, VacRelPrintStats* printStats);
static void lazy_scan_bucket(Relation onerel, LVRelStats* vacrelstats, VacuumStmt* vacstmt, Relation* Irel,
    int nindexes, bool scan_all, double* deleteTupleNum);
static void lazy_scan_rel(Relation onerel, LVRelStats* vacrelstats, VacuumStmt* vacstmt, Relation* Irel, int nindexes,
    bool scan_all, double* deleteTupleNum);
static void lazy_vacuum_all_heap(Relation onerel, LVRelStats* vacrelstats);
static int lazy_vacuum_heap(Relation onerel, LVRelStats* vacrelstats, int tupindex = 0);
static bool lazy_check_needs_freeze(Buffer buf);
extern void lazy_vacuum_index(Relation indrel, IndexBulkDeleteResult** stats, LVRelStats* vacrelstats);
static int lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer, int tupindex, LVRelStats* vacrelstats);
static void lazy_truncate_heap(Relation onerel, VacuumStmt *vacstmt, LVRelStats *vacrelstats);
static BlockNumber count_nondeletable_pages(Relation onerel,
    LVRelStats *vacrelstats);
static void lazy_space_alloc(LVRelStats* vacrelstats, BlockNumber relblocks);
extern void lazy_record_dead_tuple(LVRelStats* vacrelstats, ItemPointer itemptr);
static bool cbi_lazy_tid_reaped(ItemPointer itemptr, void* state, Oid partOid = InvalidOid, int2 bktId = InvalidBktId);
static bool lazy_tid_reaped(ItemPointer itemptr, void* state, Oid partOid = InvalidOid, int2 bktId = InvalidBktId);
static int cbi_vac_cmp_itemptr(const void* left, const void* right);
static int vac_cmp_itemptr(const void* left, const void* right);
extern void vacuum_log_cleanup_info(Relation rel, LVRelStats* vacrelstats);
static bool HeapPageCheckForUsedLinePointer(Page page);
static bool UHeapPageCheckForUsedLinePointer(Page page, Relation relation);

/*
 *	lazy_vacuum_rel() -- perform LAZY VACUUM for one heap relation
 *
 *		This routine vacuums a single heap, cleans out its indexes, and
 *		updates its relpages and reltuples statistics.
 *
 *		At entry, we have already established a transaction and opened
 *		and locked the relation.
 */
void lazy_vacuum_rel(Relation onerel, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy)
{
    LVRelStats* vacrelstats = NULL;
    Relation* Irel = NULL;
    int nindexes;
    int nindexesGlobal;
    BlockNumber possibly_freeable;
    PGRUsage ru0;
    TimestampTz starttime = 0;
    long secs;
    int usecs;
    double read_rate, write_rate;
    bool scan_all = false;     /* actually scanned all such pages? */
    TransactionId freezeTableLimit = 0;
    BlockNumber new_rel_pages;
    double new_rel_tuples;
    BlockNumber new_rel_allvisible;
    TransactionId new_frozen_xid;
    MultiXactId	new_min_multi;
    Relation* indexrel = NULL;
    Partition* indexpart = NULL;
    uint32 statFlag = onerel->parentId;
    double deleteTupleNum = 0;

    gstrace_entry(GS_TRC_ID_lazy_vacuum_rel);
    if (RelationIsColStore(onerel)) {
        vacuum_set_xid_limits(onerel,
            vacstmt->freeze_min_age,
            vacstmt->freeze_table_age,
            &u_sess->cmd_cxt.OldestXmin,
            &u_sess->cmd_cxt.FreezeLimit,
            &freezeTableLimit,
            &u_sess->cmd_cxt.MultiXactFrzLimit);

        new_frozen_xid = u_sess->cmd_cxt.FreezeLimit;

        if (RelationIsPartition(onerel)) {
            Assert(vacstmt->onepart != NULL);

            /* update the frozen xid of the partition */
            CStoreVacUpdatePartitionStats(PartitionGetPartid(vacstmt->onepart), new_frozen_xid);

            /* update the frozen xid of the partition relation */
            CStoreVacUpdatePartitionRelStats(vacstmt->onepartrel, new_frozen_xid);
        } else {
            /* update the frozen xid of normal relation */
            Relation pgclassRel = heap_open(RelationRelationId, RowExclusiveLock);
            CStoreVacUpdateNormalRelStats(RelationGetRelid(onerel), new_frozen_xid, pgclassRel);
            heap_close(pgclassRel, RowExclusiveLock);
        }

        /* start to move rows from delta to main table using delete */
        Relation deltaRel = heap_open(onerel->rd_rel->reldeltarelid, RowExclusiveLock);

        if (RelationIsCUFormat(onerel)) {
            /* initialize the delta insert */
            TableScanDesc deltaScanDesc = tableam_scan_begin(deltaRel, GetActiveSnapshot(), 0, NULL);

            InsertArg args;
            HeapTuple deltaTup = NULL;
            ResultRelInfo *resultRelInfo = NULL;
            if (onerel->rd_rel->relhasindex) {
                resultRelInfo = makeNode(ResultRelInfo);
                if (vacstmt->issubpartition) {
                    InitResultRelInfo(resultRelInfo, vacstmt->parentpartrel, 1, 0);
                } else if (vacstmt->onepartrel != NULL) {
                    InitResultRelInfo(resultRelInfo, vacstmt->onepartrel, 1, 0);
                } else {
                    InitResultRelInfo(resultRelInfo, onerel, 1, 0);
                }

                ExecOpenIndices(resultRelInfo, false);
            }
            CStoreInsert::InitInsertArg(onerel, resultRelInfo, true, args);
            CStoreInsert cstoreInsert(onerel, args, false, NULL, NULL);
            TupleDesc tupDesc = onerel->rd_att;
            Datum* val = (Datum*)palloc(sizeof(Datum) * tupDesc->natts);
            bool* null = (bool*)palloc(sizeof(bool) * tupDesc->natts);
            bulkload_rows batchRow(tupDesc, RelationGetMaxBatchRows(onerel), true);

            while ((deltaTup = (HeapTuple) tableam_scan_getnexttuple(deltaScanDesc, ForwardScanDirection)) != NULL) {
                tableam_tops_deform_tuple(deltaTup, tupDesc, val, null);

                /* ignore returned value because only one tuple is appended into */
                (void)batchRow.append_one_tuple(val, null, tupDesc);

                /* delete the current tuple from delta table */
                simple_heap_delete(deltaRel, &deltaTup->t_self);

                if (batchRow.full_rownum()) {
                    /*  insert into main table */
                    cstoreInsert.BatchInsert(&batchRow, 0);
                    batchRow.reset(true);
                }
            }
            cstoreInsert.SetEndFlag();
            cstoreInsert.BatchInsert(&batchRow, 0);
            tableam_scan_end(deltaScanDesc);

            /* clean cstore insert */
            pfree(val);
            pfree(null);
            CStoreInsert::DeInitInsertArg(args);
            batchRow.Destroy();
            cstoreInsert.Destroy();
            if (resultRelInfo != NULL) {
                ExecCloseIndices(resultRelInfo);
                pfree(resultRelInfo);
            }
        }

        /* clean part info before vacuum delta and desc table */
        vacstmt->onepartrel = NULL;
        vacstmt->onepart = NULL;
        vacstmt->parentpartrel = NULL;
        vacstmt->parentpart = NULL;
        vacstmt->issubpartition = false;
        vacstmt->flags = VACFLG_SIMPLE_HEAP;

        Oid toast_relid = InvalidOid;
        Relation toastRel = NULL;

        /* vacuum delta table */
        lazy_vacuum_rel(deltaRel, vacstmt, bstrategy);
        toast_relid = deltaRel->rd_rel->reltoastrelid;
        if (OidIsValid(toast_relid)) {
            toastRel = heap_open(toast_relid, RowExclusiveLock);
            lazy_vacuum_rel(toastRel, vacstmt, bstrategy);
            heap_close(toastRel, RowExclusiveLock);
        }
        heap_close(deltaRel, RowExclusiveLock);

        /* vacuum desc table */
        Relation descRel = heap_open(onerel->rd_rel->relcudescrelid, RowExclusiveLock);
        lazy_vacuum_rel(descRel, vacstmt, bstrategy);
        toast_relid = descRel->rd_rel->reltoastrelid;
        if (OidIsValid(toast_relid)) {
            toastRel = heap_open(toast_relid, RowExclusiveLock);
            lazy_vacuum_rel(toastRel, vacstmt, bstrategy);
            heap_close(toastRel, RowExclusiveLock);
        }
        heap_close(descRel, RowExclusiveLock);

        pgstat_report_vacuum(RelationGetRelid(onerel), onerel->parentId, onerel->rd_rel->relisshared, 0);
        gstrace_exit(GS_TRC_ID_lazy_vacuum_rel);
        return;
    }

    /* measure elapsed time iff autovacuum logging requires it */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        pg_rusage_init(&ru0);
        starttime = GetCurrentTimestamp();
    }

    if (vacstmt->options & VACOPT_VERBOSE)
        elevel = VERBOSEMESSAGE;
    else
        elevel = DEBUG2;

    vac_strategy = bstrategy;

    vacuum_set_xid_limits(onerel,
        vacstmt->freeze_min_age,
        vacstmt->freeze_table_age,
        &u_sess->cmd_cxt.OldestXmin,
        &u_sess->cmd_cxt.FreezeLimit,
        &freezeTableLimit,
        &u_sess->cmd_cxt.MultiXactFrzLimit);

    bool isNull = false;
    TransactionId relfrozenxid;
    HeapTuple tup;
    Datum xid64datum;
    Relation rel;
    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);

        rel = heap_open(PartitionRelationId, AccessShareLock);
        tup = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(vacstmt->onepart->pd_id));
        if (!HeapTupleIsValid(tup)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for relation %u", RelationGetRelid(onerel))));
        }

        xid64datum = tableam_tops_tuple_getattr(tup, Anum_pg_partition_relfrozenxid64, RelationGetDescr(rel), &isNull);
        heap_close(rel, AccessShareLock);

        if (isNull) {
            relfrozenxid = vacstmt->onepart->pd_part->relfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                !TransactionIdIsNormal(relfrozenxid))
                relfrozenxid = FirstNormalTransactionId;
        } else {
            relfrozenxid = DatumGetTransactionId(xid64datum);
        }

        scan_all = TransactionIdPrecedesOrEquals(relfrozenxid, freezeTableLimit);
    } else {
        rel = heap_open(RelationRelationId, AccessShareLock);
        tup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(onerel->rd_id));
        if (!HeapTupleIsValid(tup)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for relation %u", RelationGetRelid(onerel))));
        }
        xid64datum = tableam_tops_tuple_getattr(tup, Anum_pg_class_relfrozenxid64, RelationGetDescr(rel), &isNull);
        heap_close(rel, AccessShareLock);

        if (isNull) {
            relfrozenxid = onerel->rd_rel->relfrozenxid;

            if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid) ||
                !TransactionIdIsNormal(relfrozenxid))
                relfrozenxid = FirstNormalTransactionId;
        } else {
            relfrozenxid = DatumGetTransactionId(xid64datum);
        }

        scan_all = TransactionIdPrecedesOrEquals(relfrozenxid, freezeTableLimit);
    }
    heap_freetuple(tup);
    vacrelstats = (LVRelStats*)palloc0(sizeof(LVRelStats));

    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);
        vacrelstats->old_rel_pages = vacstmt->onepart->pd_part->relpages;
        vacrelstats->old_rel_tuples = vacstmt->onepart->pd_part->reltuples;
        vacrelstats->currVacuumPartOid = RelationGetRelid(onerel);
    } else {
        vacrelstats->old_rel_pages = onerel->rd_rel->relpages;
        vacrelstats->old_rel_tuples = onerel->rd_rel->reltuples;
        vacrelstats->currVacuumPartOid = InvalidOid;
    }
    vacrelstats->curr_heap_start = 0;
    vacrelstats->currVacuumBktId = InvalidBktId;
    vacrelstats->bucketlist = NULL;
    vacrelstats->num_index_scans = 0;
    vacrelstats->pages_removed = 0;
    vacrelstats->lock_waiter_detected = false;
    vacrelstats->hasKeepInvisbleTuples = false;

    ereport(LOG, (errmodule(MOD_VACUUM),
        errmsg("%s vacuum rel \"%s.%s\" freeze %ld OldestXmin %lu, FreezeLimit %lu, freezeTableLimit %lu",
            scan_all ? "aggressively" : "normally",
            get_namespace_name(RelationGetNamespace(onerel)),
            onerel->rd_rel->relname.data,
            vacstmt->freeze_min_age,
            u_sess->cmd_cxt.OldestXmin,
            u_sess->cmd_cxt.FreezeLimit,
            freezeTableLimit)));

    /* Open all indexes of the relation */
    if (RelationIsPartition(onerel)) {
        vac_open_part_indexes(vacstmt, RowExclusiveLock, &nindexes, &nindexesGlobal, &Irel, &indexrel, &indexpart);
    } else {
        vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &Irel);
    }

    vacrelstats->hasindex = (nindexes > 0);

    if (RELATION_CREATE_BUCKET(onerel)) {
        lazy_scan_bucket(onerel, vacrelstats, vacstmt, Irel, nindexes, scan_all, &deleteTupleNum);
    } else {
        lazy_scan_rel(onerel, vacrelstats, vacstmt, Irel, nindexes, scan_all, &deleteTupleNum);
    }

    if (!RelationIsPartition(onerel)) {
        for (int idx = 0; idx < nindexes; idx++) {
            if (RelationIsCrossBucketIndex(Irel[idx]) && IndexEnableWaitCleanCbi(Irel[idx])) {
                cbi_set_enable_clean(Irel[idx]);
            }
        }
    }
    if (vacrelstats->scanned_pages < vacrelstats->rel_pages || vacrelstats->hasKeepInvisbleTuples) {
        scan_all = false;
    } else {
        scan_all = true;
    }

    /*
     * Try to truncate tail blocks if they are all empty.
     * If data replication is enable, we cannot truncate free pages directly.
     * because in slave node replication data may be deleted while TRUNCATE
     * will happen in xlog redo.
     */
    if (!enable_heap_bcm_data_replication()) {
        /*
         * Optionally truncate the relation.
         *
         * Don't even think about it unless we have a shot at releasing a goodly
         * number of pages.  Otherwise, the time taken isn't worth it.
         */
        possibly_freeable = vacrelstats->rel_pages - vacrelstats->nonempty_pages;
        if (possibly_freeable > 0 && (possibly_freeable >= REL_TRUNCATE_MINIMUM ||
            possibly_freeable >= vacrelstats->rel_pages / REL_TRUNCATE_FRACTION))
            lazy_truncate_heap(onerel, vacstmt, vacrelstats);
    }

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
     * We do update relallvisible even in the corner case, since if the table
     * is all-visible we'd definitely like to know that.  But clamp the value
     * to be not more than what we're setting relpages to.
     *
     * Also, don't change relfrozenxid if we skipped any pages, since then we
     * don't know for certain that all tuples have a newer xmin.
     */
    new_rel_pages = vacrelstats->rel_pages;
    new_rel_tuples = vacrelstats->new_rel_tuples;
    if (vacrelstats->scanned_pages == 0 && new_rel_pages > 0) {
        new_rel_pages = vacrelstats->old_rel_pages;
        new_rel_tuples = vacrelstats->old_rel_tuples;
    }
    if (vacstmt->issubpartition) {
        Assert(vacstmt->parentpartrel != NULL);
        Assert(vacstmt->parentpart != NULL);
        new_rel_allvisible = visibilitymap_count(vacstmt->parentpartrel, vacstmt->parentpart);
    } else if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepartrel != NULL);
        Assert(vacstmt->onepart != NULL);
        new_rel_allvisible = visibilitymap_count(vacstmt->onepartrel, vacstmt->onepart);
    } else {
        new_rel_allvisible = visibilitymap_count(onerel, vacstmt->onepart);
    }
    if (new_rel_allvisible > new_rel_pages)
        new_rel_allvisible = new_rel_pages;

    new_frozen_xid = scan_all ? u_sess->cmd_cxt.FreezeLimit : InvalidTransactionId;
    new_min_multi = scan_all ? u_sess->cmd_cxt.MultiXactFrzLimit : InvalidMultiXactId;

    if (RelationIsPartition(onerel)) {
        Assert(vacstmt->onepart != NULL);

        vac_update_partstats(vacstmt->onepart, new_rel_pages, new_rel_tuples, new_rel_allvisible, new_frozen_xid,
                             new_min_multi);
        /*
         * when vacuum partition, do not change the relhasindex field in pg_class
         * for partitioned table, as some partition may be altered as "all local
         * indexes unuable", in which case, setting the partitioned table as "no index"
         * will lead to misbehave when update other index usable partitions ---the horrible
         * misdguge as hot update even if update indexes columns.
         */
        if (vacstmt->issubpartition) {
            vac_update_pgclass_partitioned_table(
                vacstmt->parentpartrel, vacstmt->parentpartrel->rd_rel->relhasindex, new_frozen_xid, new_min_multi);
        } else {
            vac_update_pgclass_partitioned_table(
                vacstmt->onepartrel, vacstmt->onepartrel->rd_rel->relhasindex, new_frozen_xid, new_min_multi);
        }

        // update stats of local partition indexes
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

        // update stats of global partition indexes
        Assert((nindexes - nindexesGlobal) >= 0);
        Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
        for (int idx = nindexes - nindexesGlobal; idx < nindexes; idx++) {
            if (vacrelstats->idx_estimated[idx]) {
                continue;
            }

            vac_update_relstats(Irel[idx],
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
        vac_update_relstats(
            onerel, classRel, new_rel_pages, new_rel_tuples, new_rel_allvisible,
            vacrelstats->hasindex, new_frozen_xid, new_min_multi);

        for (int idx = 0; idx < nindexes; idx++) {
            /* update index status */
            if (vacrelstats->idx_estimated[idx]) {
                continue;
            }

            vac_update_relstats(Irel[idx],
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
        vac_close_part_indexes(nindexes, nindexesGlobal, Irel, indexrel, indexpart, NoLock);
    } else {
        vac_close_indexes(nindexes, Irel, NoLock);
    }

    if (nindexes > 0) {
        pfree_ext(vacrelstats->new_idx_pages);
        pfree_ext(vacrelstats->new_idx_tuples);
        pfree_ext(vacrelstats->idx_estimated);
    }

    /*
     * we use oldestxmin to delete dead tuples, some dead tuples may recently dead
     * and are not deleted immediately. Here we send deleteTupleNum to PgStatCollector.
     */
    pgstat_report_vacuum(RelationGetRelid(onerel), statFlag, onerel->rd_rel->relisshared, deleteTupleNum);

    /* and log the action if appropriate */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        TimestampTz endtime = GetCurrentTimestamp();
        if (u_sess->attr.attr_storage.Log_autovacuum_min_duration == 0 ||
            TimestampDifferenceExceeds(starttime, endtime, u_sess->attr.attr_storage.Log_autovacuum_min_duration)) {
            TimestampDifference(starttime, endtime, &secs, &usecs);

            read_rate = 0;
            write_rate = 0;
            const int PAGE_SIZE = (1024 * 1024);
            if ((secs > 0) || (usecs > 0)) {
                read_rate =
                    (double)BLCKSZ * t_thrd.vacuum_cxt.VacuumPageMiss / (PAGE_SIZE) / (secs + usecs / 1000000.0);
                write_rate =
                    (double)BLCKSZ * t_thrd.vacuum_cxt.VacuumPageDirty / (PAGE_SIZE) / (secs + usecs / 1000000.0);
            }
            ereport(LOG,
                (errmsg("automatic vacuum of table \"%s.%s.%s\": index scans: %d\n"
                        "pages: %u removed, %u remain\n"
                        "tuples: %.0f removed, %.0f remain\n"
                        "buffer usage: %d hits, %d misses, %d dirtied\n"
                        "avg read rate: %.3f MiB/s, avg write rate: %.3f MiB/s\n"
                        "system usage: %s",
                    get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId),
                    get_namespace_name(RelationGetNamespace(onerel)),
                    RelationGetRelationName(onerel),
                    vacrelstats->num_index_scans,
                    vacrelstats->pages_removed,
                    vacrelstats->rel_pages,
                    vacrelstats->tuples_deleted,
                    vacrelstats->new_rel_tuples,
                    t_thrd.vacuum_cxt.VacuumPageHit,
                    t_thrd.vacuum_cxt.VacuumPageMiss,
                    t_thrd.vacuum_cxt.VacuumPageDirty,
                    read_rate,
                    write_rate,
                    pg_rusage_show(&ru0))));
        }
    }
    pfree_ext(vacrelstats);
    gstrace_exit(GS_TRC_ID_lazy_vacuum_rel);
}

/*
 * For Hot Standby we need to know the highest transaction id that will
 * be removed by any change. VACUUM proceeds in a number of passes so
 * we need to consider how each pass operates. The first phase runs
 * heap_page_prune(), which can issue XLOG_HEAP2_CLEAN records as it
 * progresses - these will have a latestRemovedXid on each record.
 * In some cases this removes all of the tuples to be removed, though
 * often we have dead tuples with index pointers so we must remember them
 * for removal in phase 3. Index records for those rows are removed
 * in phase 2 and index blocks do not have MVCC information attached.
 * So before we can allow removal of any index tuples we need to issue
 * a WAL record containing the latestRemovedXid of rows that will be
 * removed in phase three. This allows recovery queries to block at the
 * correct place, i.e. before phase two, rather than during phase three
 * which would be after the rows have become inaccessible.
 */
extern void vacuum_log_cleanup_info(Relation rel, LVRelStats* vacrelstats)
{
    /*
     * Skip this for relations for which no WAL is to be written, or if we're
     * not trying to support archive recovery.
     */
    if (!RelationNeedsWAL(rel) || !XLogIsNeeded())
        return;

    /*
     * No need to write the record at all unless it contains a valid PAGE_SIZE
     */
    if (TransactionIdIsValid(vacrelstats->latestRemovedXid))
        (void)log_heap_cleanup_info(&(rel->rd_node), vacrelstats->latestRemovedXid);
}

/*
 * @Description: vacuum full prefetch block use ping pang
 * @Param[IN] nblocks: prefetch count
 * @Param[IN] onerel: relation
 * @Param[IN] start: block id start
 */
void lazy_vacuum_prefetch(Relation onerel, BlockNumber start, BlockNumber nblocks, ValPrefetch* valprefetch)
{
    if (!valprefetch->init) {
        /* preftch first list */
        valprefetch->fetchlist1.count = ((start + valprefetch->fetchlist1.quantity) < nblocks)
                                            ? valprefetch->fetchlist1.quantity
                                            : (nblocks - start);
        if (valprefetch->fetchlist1.count != 0) {
            PageRangePrefetch(onerel, MAIN_FORKNUM, start, valprefetch->fetchlist1.count, 0, 0);
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("vacuum prefetch for %s,  start(%u), count(%u)",
                        RelationGetRelationName(onerel),
                        start,
                        valprefetch->fetchlist1.count)));
        }
        valprefetch->fetchlist1.block_guard = start + valprefetch->fetchlist1.count - 1;

        start += valprefetch->fetchlist1.count;

        /* preftch second list */
        valprefetch->fetchlist2.count = ((start + valprefetch->fetchlist2.quantity) < nblocks)
                                            ? valprefetch->fetchlist2.quantity
                                            : (nblocks - start);
        if (valprefetch->fetchlist2.count != 0) {
            PageRangePrefetch(onerel, MAIN_FORKNUM, start, valprefetch->fetchlist2.count, 0, 0);
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("vacuum prefetch for %s,  start(%u), count(%u)",
                        RelationGetRelationName(onerel),
                        start,
                        valprefetch->fetchlist2.count)));
        }
        valprefetch->fetchlist2.block_guard = start + valprefetch->fetchlist2.count - 1;
        valprefetch->fetchlist = &valprefetch->fetchlist1;
        valprefetch->init = true;
    }

    if (valprefetch->fetchlist->count == 0) {
        return;
    }

    /* exchange list and trigger prefetch */
    if (start == valprefetch->fetchlist->block_guard) {
        ValPrefetchList* list = valprefetch->fetchlist;
        valprefetch->fetchlist =
            (valprefetch->fetchlist == &valprefetch->fetchlist1) ? &valprefetch->fetchlist2 : &valprefetch->fetchlist1;
        start = valprefetch->fetchlist->block_guard + 1;
        list->count = ((start + list->quantity) < nblocks) ? list->quantity : (nblocks - start);
        if (list->count != 0) {
            PageRangePrefetch(onerel, MAIN_FORKNUM, start, list->count, 0, 0);
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("vacuum prefetch for %s,  start(%u), count(%u)",
                        RelationGetRelationName(onerel),
                        start,
                        list->count)));
        }
        list->block_guard = start + list->count - 1;
    }

    return;
}

static void InitVacPrintStat(VacRelPrintStats *printStats)
{
    printStats->tupsVacuumed = 0;
    printStats->numTuples = 0;
    printStats->scannedPages = 0;
    printStats->nblocks = 0;
    printStats->nkeep = 0;
    printStats->nunused = 0;
    printStats->emptyPages = 0;
    pg_rusage_init(&printStats->ruVac);
}

static void PrintVacStatsInfo(const VacRelPrintStats *printStats, Relation onerel)
{
    ereport(LOG, (errmodule(MOD_VACUUM),
        errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
            RelationGetRelationName(onerel),
            printStats->tupsVacuumed,
            printStats->numTuples,
            printStats->scannedPages,
            printStats->nblocks),
            errdetail("%.0f dead row versions cannot be removed yet. "
                      "There were %.0f unused item pointers. "
                      "%u pages are entirely empty. "
                      "%s.",
            printStats->nkeep,
            printStats->nunused,
            printStats->emptyPages,
            pg_rusage_show(&printStats->ruVac))));
}

static void lazy_scan_rel(Relation onerel, LVRelStats* vacrelstats, VacuumStmt* vacstmt, Relation* Irel, int nindexes,
    bool scan_all, double* deleteTupleNum)
{
    IndexBulkDeleteResult** indstats = NULL;
    VacRelPrintStats printStats;
    int idx;

    if (nindexes > 0) {
        vacrelstats->new_idx_pages = (BlockNumber*)palloc0(nindexes * sizeof(BlockNumber));
        vacrelstats->new_idx_tuples = (double*)palloc0(nindexes * sizeof(double));
        vacrelstats->idx_estimated = (bool*)palloc0(nindexes * sizeof(bool));
    }
    int nblocks = RelationGetNumberOfBlocks(onerel);
    lazy_space_alloc(vacrelstats, nblocks);
    InitVacPrintStat(&printStats);
    indstats = lazy_scan_heap(onerel, vacrelstats, Irel, nindexes, scan_all, deleteTupleNum, &printStats);
    PrintVacStatsInfo(&printStats, onerel);
    if (vacrelstats->num_dead_tuples > 0) {
        lazy_vacuum_heap(onerel, vacrelstats);
    }

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
    pfree_ext(vacrelstats->dead_tuples);
    pfree_ext(indstats);
}

static void lazy_scan_bucket(Relation onerel, LVRelStats* vacrelstats, VacuumStmt* vacstmt, Relation* Irel,
    int nindexes, bool scan_all, double* deleteTupleNum)
{
    Relation buckRel = NULL;
    Relation* ibuckRel = NULL;
    LVRelStats* vacbucketstats = NULL;
    VacRelPrintStats printStats;
    double deleteTupletemp = 0;
    IndexBulkDeleteResult** indstats = NULL;
    int idx;

    vacbucketstats = (LVRelStats*)palloc0(sizeof(LVRelStats));

    if (nindexes > 0) {
        Assert(vacrelstats->hasindex);
        vacrelstats->new_idx_pages = (BlockNumber*)palloc0(nindexes * sizeof(BlockNumber));
        vacrelstats->new_idx_tuples = (double*)palloc0(nindexes * sizeof(double));
        vacrelstats->idx_estimated = (bool*)palloc0(nindexes * sizeof(bool));
        ibuckRel = (Relation*)palloc0(nindexes * sizeof(Relation));
        vacbucketstats->hasindex = true;
    }
    int nblocks = RelationGetNumberOfBlocks(onerel);
    lazy_space_alloc(vacbucketstats, nblocks);
    vacbucketstats->num_dead_tuples = 0;
    vacbucketstats->curr_heap_start = 0;
    vacbucketstats->currVacuumBktId = InvalidBktId;
    vacbucketstats->currVacuumPartOid = vacrelstats->currVacuumPartOid;

    oidvector* bucketlist = searchHashBucketByOid(onerel->rd_bucketoid);
    vacbucketstats->bucketlist = bucketlist;
    InitVacPrintStat(&printStats);
    for (int i = 0; i < bucketlist->dim1; i++) {
        buckRel = bucketGetRelation(onerel, NULL, bucketlist->values[i]);
        vacbucketstats->currVacuumBktId = bucketlist->values[i];
        vacbucketstats->curr_heap_start = vacbucketstats->num_dead_tuples;

        for (idx = 0; idx < nindexes; idx++) {
            if (RelationIsCrossBucketIndex(Irel[idx])) {
                ibuckRel[idx] = Irel[idx];
            } else {
                ibuckRel[idx] = bucketGetRelation(Irel[idx], NULL, bucketlist->values[i]);
            }
        }

        /* do vacuuming on bucket heap */
        indstats = lazy_scan_heap(buckRel, vacbucketstats, ibuckRel,
            nindexes, scan_all, &deleteTupletemp, &printStats);

        for (idx = 0; idx < nindexes; idx++) {
            /* close index bucket relation */
            if (!RelationIsCrossBucketIndex(Irel[idx])) {
                bucketCloseRelation(ibuckRel[idx]);
            }

            /* summarize the index status information */
            if (indstats[idx] != NULL) {
                vacrelstats->new_idx_pages[idx] += indstats[idx]->num_pages;
                vacrelstats->new_idx_tuples[idx] += indstats[idx]->num_index_tuples;
                if (indstats[idx]->estimated_count == true) {
                    vacrelstats->idx_estimated[idx] = indstats[idx]->estimated_count;
                }
                pfree_ext(indstats[idx]);
            }
        }
        pfree_ext(indstats);
        bucketCloseRelation(buckRel);

        /* summarize the vacuum status information */
        vacrelstats->num_index_scans += vacbucketstats->num_index_scans;
        vacrelstats->pages_removed += vacbucketstats->pages_removed;
        vacrelstats->tuples_deleted += vacbucketstats->tuples_deleted;
        vacrelstats->scanned_pages += vacbucketstats->scanned_pages;
        vacrelstats->scanned_tuples += vacbucketstats->scanned_tuples;
        vacrelstats->rel_pages += vacbucketstats->rel_pages;
        if (vacbucketstats->lock_waiter_detected == true) {
            vacrelstats->lock_waiter_detected = true;
        }
        *deleteTupleNum += deleteTupletemp;
    }

    PrintVacStatsInfo(&printStats, onerel);

    for (idx = 0; idx < nindexes; idx++) {
        if (!RelationIsCrossBucketIndex(Irel[idx])) {
            continue;
        }
        IndexBulkDeleteResult *indstat = NULL;
        lazy_vacuum_index(Irel[idx], &indstat, vacbucketstats, vac_strategy);
        indstat = lazy_cleanup_index(Irel[idx], indstat, vacrelstats, vac_strategy);
        vacrelstats->new_idx_pages[idx] = indstat->num_pages;
        vacrelstats->new_idx_tuples[idx] = indstat->num_index_tuples;
        if (indstat->estimated_count == true) {
            vacrelstats->idx_estimated[idx] = indstat->estimated_count;
        }
        pfree_ext(indstat);
    }
    if (vacbucketstats->num_dead_tuples > 0) {
        lazy_vacuum_all_heap(onerel, vacbucketstats);
    }
    /* Vacuum the Free Space Map */
    for (int i = 0; i < bucketlist->dim1; i++) {
        buckRel = bucketGetRelation(onerel, NULL, bucketlist->values[i]);
        FreeSpaceMapVacuum(buckRel);
        bucketCloseRelation(buckRel);
    }
    /* estimate new rel tuples at last after we finished scanning all buckets */
    ereport(LOG, (errmsg("Vacuum bucket table %s and set reltuples, old pages: %lf, old tuples: %lf, total pages: %u, scanned pages: %u, scanned tuples: %lf",
        RelationGetRelationName(onerel), onerel->rd_rel->relpages, onerel->rd_rel->reltuples, vacrelstats->rel_pages,
        vacrelstats->scanned_pages, vacrelstats->scanned_tuples)));
    vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, vacrelstats->rel_pages, vacrelstats->scanned_pages,
        vacrelstats->scanned_tuples);

    pfree_ext(ibuckRel);
    pfree_ext(vacbucketstats->dead_tuples);
    pfree_ext(vacbucketstats);
}

/*
 *	lazy_scan_heap() -- scan an open heap relation
 *
 *		This routine prunes each page in the heap, which will among other
 *		things truncate dead tuples to dead line pointers, defragment the
 *		page, and set commit status bits (see heap_page_prune).  It also builds
 *		lists of dead tuples and pages with free space, calculates statistics
 *		on the number of live tuples in the heap, and marks pages as
 *		all-visible if appropriate.  When done, or when we run low on space for
 *		dead-tuple TIDs, invoke vacuuming of indexes and call lazy_vacuum_heap
 *		to reclaim dead line pointers.
 *
 *		If there are no indexes then we can reclaim line pointers on the fly;
 *		dead line pointers need only be retained until all index pointers that
 *		reference them have been killed.
 */
// add paras IPartRel, IPart
static IndexBulkDeleteResult** lazy_scan_heap(
    Relation onerel, LVRelStats* vacrelstats, Relation* Irel, int nindexes,
    bool scan_all, double* ptrDeleteTupleNum, VacRelPrintStats* printStats)
{
    BlockNumber nblocks, blkno;
    HeapTupleData tuple;
    char* relname = NULL;
    BlockNumber empty_pages, vacuumed_pages;
    double num_tuples, tups_vacuumed, nkeep, nunused;
    IndexBulkDeleteResult** indstats;
    int i;
    PGRUsage ru0;
    Buffer vmbuffer = InvalidBuffer;
    BlockNumber next_not_all_visible_block;
    bool skipping_all_visible_blocks = false;
    ValPrefetch valprefetch;
    TdeInfo tde_info = {0};

    gstrace_entry(GS_TRC_ID_lazy_scan_heap);

    pg_rusage_init(&ru0);

    relname = RelationGetRelationName(onerel);
    ereport(elevel, (errmsg("vacuuming \"%s.%s\"", get_namespace_name(RelationGetNamespace(onerel)), relname)));

    empty_pages = vacuumed_pages = 0;
    num_tuples = tups_vacuumed = nkeep = nunused = 0;

    indstats = (IndexBulkDeleteResult**)palloc0(nindexes * sizeof(IndexBulkDeleteResult*));

    nblocks = RelationGetNumberOfBlocks(onerel);
    vacrelstats->rel_pages = nblocks;
    vacrelstats->scanned_pages = 0;
    vacrelstats->nonempty_pages = 0;
    vacrelstats->latestRemovedXid = InvalidTransactionId;

    /*
     * We want to skip pages that don't require vacuuming according to the
     * visibility map, but only when we can skip at least SKIP_PAGES_THRESHOLD
     * consecutive pages.  Since we're reading sequentially, the OS should be
     * doing readahead for us, so there's no gain in skipping a page now and
     * then; that's likely to disable readahead and so be counterproductive.
     * Also, skipping even a single page means that we can't update
     * relfrozenxid, so we only want to do it if we can skip a goodly number
     * of pages.
     *
     * Before entering the main loop, establish the invariant that
     * next_not_all_visible_block is the next block number >= blkno that's not
     * all-visible according to the visibility map, or nblocks if there's no
     * such block.	Also, we set up the skipping_all_visible_blocks flag,
     * which is needed because we need hysteresis in the decision: once we've
     * started skipping blocks, we may as well skip everything up to the next
     * not-all-visible block.
     *
     * Note: if scan_all is true, we won't actually skip any pages; but we
     * maintain next_not_all_visible_block anyway, so as to set up the
     * all_visible_according_to_vm flag correctly for each page.
     *
     * Note: The value returned by visibilitymap_test could be slightly
     * out-of-date, since we make this test before reading the corresponding
     * heap page or locking the buffer.  This is OK.  If we mistakenly think
     * that the page is all-visible when in fact the flag's just been cleared,
     * we might fail to vacuum the page.  But it's OK to skip pages when
     * scan_all is not set, so no great harm done; the next vacuum will find
     * them.  If we make the reverse mistake and vacuum a page unnecessarily,
     * it'll just be a no-op.
     */
    for (next_not_all_visible_block = 0; next_not_all_visible_block < nblocks; next_not_all_visible_block++) {
        if (!visibilitymap_test(onerel, next_not_all_visible_block, &vmbuffer))
            break;
        vacuum_delay_point();
    }
    if (next_not_all_visible_block >= SKIP_PAGES_THRESHOLD)
        skipping_all_visible_blocks = true;
    else
        skipping_all_visible_blocks = false;

    ADIO_RUN()
    {
        uint32 quantity = (uint32)u_sess->attr.attr_storage.prefetch_quantity;
        valprefetch.fetchlist1.quantity = (uint32)((quantity > (nblocks / 2 + 1)) ? (nblocks / 2 + 1) : quantity);
        valprefetch.fetchlist1.block_guard = 0;
        valprefetch.fetchlist1.count = 0;

        valprefetch.fetchlist2.quantity = valprefetch.fetchlist1.quantity;
        valprefetch.fetchlist2.block_guard = 0;
        valprefetch.fetchlist2.count = 0;
        valprefetch.init = false;
        ereport(DEBUG1,
            (errmodule(MOD_ADIO),
                errmsg("vacuum prefetch for %s,  prefetch quantity(%u)",
                    RelationGetRelationName(onerel),
                    valprefetch.fetchlist1.quantity)));
    }
    ADIO_END();

    for (blkno = 0; blkno < nblocks; blkno++) {
        Buffer buf;
        Page page;
        OffsetNumber offnum, maxoff;
        bool tupgone = false;
        bool hastup = false;
        bool keepThisInvisibleTuple = false;
        int prev_dead_count;
        OffsetNumber invalid[MaxOffsetNumber];
        OffsetNumber frozen[MaxOffsetNumber];
        int ninvalid = 0;
        int nfrozen;
        Size freespace;
        bool all_visible_according_to_vm = false;
        bool all_visible = false;
        bool has_dead_tuples = false;
        TransactionId visibility_cutoff_xid = InvalidTransactionId;
        bool changedMultiXid;

        /* IO collector and IO scheduler for vacuum */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);

        if (blkno == next_not_all_visible_block) {
            /* Time to advance next_not_all_visible_block */
            for (next_not_all_visible_block++; next_not_all_visible_block < nblocks; next_not_all_visible_block++) {
                if (!visibilitymap_test(onerel, next_not_all_visible_block, &vmbuffer))
                    break;
                vacuum_delay_point();
            }

            /*
             * We know we can't skip the current block.  But set up
             * skipping_all_visible_blocks to do the right thing at the
             * following blocks.
             */
            if (next_not_all_visible_block - blkno > SKIP_PAGES_THRESHOLD)
                skipping_all_visible_blocks = true;
            else
                skipping_all_visible_blocks = false;
            all_visible_according_to_vm = false;
        } else {
            /* Current block is all-visible */
            if (skipping_all_visible_blocks && !scan_all)
                continue;
            all_visible_according_to_vm = true;
        }

        ADIO_RUN()
        {
            lazy_vacuum_prefetch(onerel, blkno, nblocks, &valprefetch);
        }
        ADIO_END();

        vacuum_delay_point();

        /*
         * If we are close to overrunning the available space for dead-tuple
         * TIDs, pause and do a cycle of vacuuming before we tackle this page.
         */
        if ((vacrelstats->max_dead_tuples - vacrelstats->num_dead_tuples) < MaxHeapTuplesPerPage &&
            vacrelstats->num_dead_tuples > 0) {
            /*
             * Before beginning index vacuuming, we release any pin we may
             * hold on the visibility map page.  This isn't necessary for
             * correctness, but we do it anyway to avoid holding the pin
             * across a lengthy, unrelated operation.
             */
            if (BufferIsValid(vmbuffer)) {
                ReleaseBuffer(vmbuffer);
                vmbuffer = InvalidBuffer;
            }

            /* Log cleanup info before we touch indexes */
            vacuum_log_cleanup_info(onerel, vacrelstats);

            /* Remove index entries */
            for (i = 0; i < nindexes; i++)
                lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
            /* Remove tuples from heap */
            lazy_vacuum_all_heap(onerel, vacrelstats);

            /*
             * Forget the now-vacuumed tuples, and press on, but be careful
             * not to reset latestRemovedXid since we want that value to be
             * valid.
             */
            vacrelstats->num_dead_tuples = 0;
            vacrelstats->curr_heap_start = 0;
            vacrelstats->num_index_scans++;
        }

        /*
         * Pin the visibility map page in case we need to mark the page
         * all-visible.  In most cases this will be very cheap, because we'll
         * already have the correct page pinned anyway.  However, it's
         * possible that (a) next_not_all_visible_block is covered by a
         * different VM page than the current block or (b) we released our pin
         * and did a cycle of index vacuuming.
         */
        visibilitymap_pin(onerel, blkno, &vmbuffer);

        buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno, RBM_NORMAL, vac_strategy);
        /* We need buffer cleanup lock so that we can prune HOT chains. */
        if (!ConditionalLockBufferForCleanup(buf)) {
            /*
             * If we're not scanning the whole relation to guard against XID
             * wraparound, it's OK to skip vacuuming a page.  The next vacuum
             * will clean it up.
             */
            if (!scan_all) {
                ReleaseBuffer(buf);
                continue;
            }

            /*
             * If this is a freeze checking vacuum, then we read the page
             * with share lock to see if any xids need to be frozen. If the
             * page doesn't need attention we just skip and continue. If it
             * does, we wait for cleanup lock.
             *
             * We could defer the lock request further by remembering the page
             * and coming back to it later, or we could even register
             * ourselves for multiple buffers and then service whichever one
             * is received first.  For now, this seems good enough.
             */
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            if (!lazy_check_needs_freeze(buf)) {
                UnlockReleaseBuffer(buf);
                vacrelstats->scanned_pages++;
                continue;
            }
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockBufferForCleanup(buf);
            /* drop through to normal processing */
        }
        vacrelstats->scanned_pages++;
        page = BufferGetPage(buf);
        if (PageIsNew(page)) {
            /*
             * An all-zeroes page could be left over if a backend extends the
             * relation but crashes before initializing the page. Reclaim such
             * pages for use.
             *
             * We have to be careful here because we could be looking at a
             * page that someone has just added to the relation and not yet
             * been able to initialize (see RelationGetBufferForTuple). To
             * protect against that, release the buffer lock, grab the
             * relation extension lock momentarily, and re-lock the buffer. If
             * the page is still uninitialized by then, it must be left over
             * from a crashed backend, and we can initialize it.
             *
             * We don't really need the relation lock when this is a new or
             * temp relation, but it's probably not worth the code space to
             * check that, since this surely isn't a critical path.
             *
             * Note: the comparable code in vacuum.c need not worry because
             * it's got exclusive lock on the whole relation.
             */
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            LockRelationForExtension(onerel, ExclusiveLock);
            UnlockRelationForExtension(onerel, ExclusiveLock);
            LockBufferForCleanup(buf);
            if (PageIsNew(page)) {
                ereport(WARNING, (errmsg("relation \"%s\" page %u is uninitialized --- fixing", relname, blkno)));
                HeapPageHeader phdr = (HeapPageHeader)page;
                PageInit(page, BufferGetPageSize(buf), 0, true);
                phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
                phdr->pd_multi_base = 0;
                const char* algo = RelationGetAlgo(onerel);
                if (RelationisEncryptEnable(onerel) || (algo && *algo != '\0')) {
                    /* 
                     * For the reason of saving TdeInfo,
                     * we need to move the pointer(pd_special) forward by the length of TdeInfo.
                     */
                    phdr->pd_upper -= sizeof(TdePageInfo);
                    phdr->pd_special -= sizeof(TdePageInfo);
                    PageSetTDE(page);
                }
                empty_pages++;
            }
            freespace = PageGetHeapFreeSpace(page);
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);

            RecordPageWithFreeSpace(onerel, blkno, freespace);
            continue;
        }

        if (PageIsEmpty(page)) {
            empty_pages++;
            freespace = PageGetHeapFreeSpace(page);

            /* empty pages are always all-visible */
            if (!PageIsAllVisible(page)) {
                START_CRIT_SECTION();

                /* mark buffer dirty before writing a WAL record */
                MarkBufferDirty(buf);

                /*
                 * It's possible that another backend has extended the heap,
                 * initialized the page, and then failed to WAL-log the page
                 * due to an ERROR.  Since heap extension is not WAL-logged,
                 * recovery might try to replay our record setting the
                 * page all-visible and find that the page isn't initialized,
                 * which will cause a PANIC.  To prevent that, check whether
                 * the page has been previously WAL-logged, and if not, do that
                 * now.
                 *
                 * XXX: It would be nice to use a logging method supporting
                 * standard buffers here since log_newpage_buffer() will write
                 * the full block instead of omitting the hole.
                 */
                if (RelationNeedsWAL(onerel) && XLByteEQ(PageGetLSN(page), InvalidXLogRecPtr)) {
                    if (RelationisEncryptEnable(onerel)) {
                        GetTdeInfoFromRel(onerel, &tde_info);
                    }
                    log_newpage_buffer(buf, true, &tde_info);
                }

                bool free_dict = false;
                PageSetAllVisible(page);
                if (PageIsCompressed(page)) {
                    /* free dict and update freespace */
                    free_dict = PageFreeDict(page);
                    Assert(free_dict == true);
                    freespace = PageGetHeapFreeSpace(page);
                }
                visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr, vmbuffer, InvalidTransactionId, free_dict);
                END_CRIT_SECTION();
            }

            UnlockReleaseBuffer(buf);
            RecordPageWithFreeSpace(onerel, blkno, freespace);
            continue;
        }

        /*
         * Prune all HOT-update chains in this page.
         *
         * We count tuples removed by the pruning step as removed by VACUUM.
         */
        tups_vacuumed +=
            heap_page_prune(onerel, buf, u_sess->cmd_cxt.OldestXmin, false, &vacrelstats->latestRemovedXid, true);
        /*
         * Now scan the page to collect vacuumable items and check for tuples
         * requiring freezing.
         */
        all_visible = true;
        has_dead_tuples = false;
        nfrozen = 0;
        changedMultiXid = false;
        hastup = false;
        prev_dead_count = vacrelstats->num_dead_tuples;
        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            ItemId itemid;
            itemid = PageGetItemId(page, offnum);
            /* Unused items require no processing, but we count 'em */
            if (!ItemIdIsUsed(itemid)) {
                nunused += 1;
                continue;
            }
            /* Redirect items mustn't be touched */
            if (ItemIdIsRedirected(itemid)) {
                hastup = true; /* this page won't be truncatable */
                continue;
            }

            ItemPointerSet(&(tuple.t_self), blkno, offnum);

            /*
             * DEAD item pointers are to be vacuumed normally; but we don't
             * count them in tups_vacuumed, else we'd be double-counting (at
             * least in the common case where heap_page_prune() just freed up
             * a non-HOT tuple).
             */
            if (ItemIdIsDead(itemid)) {
                lazy_record_dead_tuple(vacrelstats, &(tuple.t_self));
                all_visible = false;
                continue;
            }

            Assert(ItemIdIsNormal(itemid));

            tuple.t_data = (HeapTupleHeader)PageGetItem(page, itemid);
            tuple.t_len = ItemIdGetLength(itemid);
            tuple.t_tableOid = RelationGetRelid(onerel);
            tuple.t_bucketId = RelationGetBktid(onerel);
            HeapTupleCopyBaseFromPage(&tuple, page);
            tupgone = false;
            keepThisInvisibleTuple = false;

            if (u_sess->attr.attr_storage.enable_debug_vacuum)
                t_thrd.utils_cxt.pRelatedRel = onerel;

            switch (HeapTupleSatisfiesVacuum(&tuple, u_sess->cmd_cxt.OldestXmin, buf)) {
                case HEAPTUPLE_DEAD:

                    /*
                     * Ordinarily, DEAD tuples would have been removed by
                     * heap_page_prune(), but it's possible that the tuple
                     * state changed since heap_page_prune() looked.  In
                     * particular an INSERT_IN_PROGRESS tuple could have
                     * changed to DEAD if the inserter aborted.  So this
                     * cannot be considered an error condition.
                     *
                     * If the tuple is HOT-updated then it must only be
                     * removed by a prune operation; so we keep it just as if
                     * it were RECENTLY_DEAD.  Also, if it's a heap-only
                     * tuple, we choose to keep it, because it'll be a lot
                     * cheaper to get rid of it in the next pruning pass than
                     * to treat it like an indexed tuple.
                     */
                    keepThisInvisibleTuple = HeapKeepInvisibleTuple(&tuple, RelationGetDescr(onerel));
                    if (HeapTupleIsHotUpdated(&tuple) || HeapTupleIsHeapOnly(&tuple) || keepThisInvisibleTuple) {
                        nkeep += 1;
                    } else {
                        tupgone = true; /* we can delete the tuple */
                    }
                    all_visible = false;
                    break;
                case HEAPTUPLE_LIVE:
                    /* Tuple is good --- but let's do some validity checks */
                    if (onerel->rd_rel->relhasoids && !OidIsValid(HeapTupleGetOid(&tuple))) {
                        ereport(WARNING,
                            (errmsg("relation \"%s\" TID %u/%hu: OID is invalid", relname, blkno, offnum)));
                    }

                    /*
                     * Is the tuple definitely visible to all transactions?
                     *
                     * NB: Like with per-tuple hint bits, we can't set the
                     * PD_ALL_VISIBLE flag if the inserter committed
                     * asynchronously. See SetHintBits for more info. Check
                     * that the HEAP_XMIN_COMMITTED hint bit is set because of
                     * that.
                     */
                    if (all_visible) {
                        TransactionId xmin = 0;

                        if (!HeapTupleHeaderXminCommitted(tuple.t_data)) {
                            all_visible = false;
                            break;
                        }

                        /*
                         * The inserter definitely committed. But is it old
                         * enough that everyone sees it as committed?
                         */
                        xmin = HeapTupleHeaderGetXmin(page, tuple.t_data);
                        if (!TransactionIdPrecedes(xmin, u_sess->cmd_cxt.OldestXmin)) {
                            all_visible = false;
                            break;
                        }

                        /* Track newest xmin on page. */
                        if (TransactionIdFollows(xmin, visibility_cutoff_xid))
                            visibility_cutoff_xid = xmin;
                    }
                    break;
                case HEAPTUPLE_RECENTLY_DEAD:

                    /*
                     * If tuple is recently deleted then we must not remove it
                     * from relation.
                     */
                    nkeep += 1;
                    all_visible = false;
                    break;
                case HEAPTUPLE_INSERT_IN_PROGRESS:
                    /* This is an expected case during concurrent vacuum */
                    all_visible = false;
                    break;
                case HEAPTUPLE_DELETE_IN_PROGRESS:
                    /* This is an expected case during concurrent vacuum */
                    all_visible = false;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_COLLATION_MISMATCH), errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                    break;
            }

            if (tupgone) {
                lazy_record_dead_tuple(vacrelstats, &(tuple.t_self));
                HeapTupleHeaderAdvanceLatestRemovedXid(&tuple, &vacrelstats->latestRemovedXid);

                if (u_sess->attr.attr_storage.enable_debug_vacuum)
                    elogVacuumInfo(onerel, &tuple, "lazy_scan_heap", u_sess->cmd_cxt.OldestXmin);

                tups_vacuumed += 1;
                has_dead_tuples = true;
            } else if (keepThisInvisibleTuple) {
                if (t_thrd.proc->workingVersionNum >= INVALID_INVISIBLE_TUPLE_VERSION
                    && !HeapTupleIsHotUpdated(&tuple)) {
                    heap_invalid_invisible_tuple(&tuple);
                    Assert(tuple.t_tableOid == PartitionRelationId);
                    invalid[ninvalid++] = offnum;
                } else {
                    vacrelstats->hasKeepInvisbleTuples = true;
                }
            } else {
                num_tuples += 1;
                hastup = true;

                /*
                 * Each non-removable tuple must be checked to see if it needs
                 * freezing.  Note we already have exclusive buffer lock.
                 */
                if (heap_freeze_tuple(&tuple, u_sess->cmd_cxt.FreezeLimit, u_sess->cmd_cxt.MultiXactFrzLimit,
                                      &changedMultiXid))
                    frozen[nfrozen++] = offnum;
            }

            t_thrd.utils_cxt.pRelatedRel = NULL;
        } /* scan along page */

        /*
         * If we froze any tuples, mark the buffer dirty, and write a WAL
         * record recording the changes.  We must log the changes to be
         * crash-safe against future truncation of CLOG.
         */
        if (nfrozen > 0) {
            START_CRIT_SECTION();
            MarkBufferDirty(buf);
            if (RelationNeedsWAL(onerel)) {
                XLogRecPtr recptr;

                recptr = log_heap_freeze(onerel, buf, u_sess->cmd_cxt.FreezeLimit,
                                         changedMultiXid ?  u_sess->cmd_cxt.MultiXactFrzLimit : InvalidMultiXactId,
                                         frozen, nfrozen);
                PageSetLSN(page, recptr);
            }
            END_CRIT_SECTION();
            if (TransactionIdPrecedes(((HeapPageHeader)page)->pd_xid_base, u_sess->utils_cxt.RecentXmin)) {
                if (u_sess->utils_cxt.RecentXmin - ((HeapPageHeader)page)->pd_xid_base > CHANGE_XID_BASE)
                    (void)heap_change_xidbase_after_freeze(onerel, buf);
            }
        }

        if (ninvalid > 0) {
            START_CRIT_SECTION();
            MarkBufferDirty(buf);
            if (RelationNeedsWAL(onerel)) {
                XLogRecPtr recptr;

                recptr = log_heap_invalid(onerel, buf, u_sess->cmd_cxt.FreezeLimit,
                                          invalid, ninvalid);
                PageSetLSN(page, recptr);
            }
            END_CRIT_SECTION();
            if (TransactionIdPrecedes(((HeapPageHeader)page)->pd_xid_base, u_sess->utils_cxt.RecentXmin)) {
                if (u_sess->utils_cxt.RecentXmin - ((HeapPageHeader)page)->pd_xid_base > CHANGE_XID_BASE)
                    (void)heap_change_xidbase_after_freeze(onerel, buf);
            }
        }

        /*
         * If there are no indexes then we can vacuum the page right now
         * instead of doing a second scan.
         */
        if (nindexes == 0 && vacrelstats->num_dead_tuples > vacrelstats->curr_heap_start) {
            /* Remove tuples from heap */
            lazy_vacuum_page(onerel, blkno, buf, 0, vacrelstats);
            has_dead_tuples = false;

            /*
             * Forget the now-vacuumed tuples, and press on, but be careful
             * not to reset latestRemovedXid since we want that value to be
             * valid.
             */
            vacrelstats->num_dead_tuples = 0;
            vacuumed_pages++;
        }

        freespace = PageGetHeapFreeSpace(page);

        /* mark page all-visible, if appropriate */
        if (all_visible) {
            if (!PageIsAllVisible(page)) {
                bool free_dict = false;
                PageSetAllVisible(page);
                if (PageIsCompressed(page)) {
                    free_dict = PageFreeDict(page);
                    freespace = PageGetHeapFreeSpace(page);
                }
                MarkBufferDirty(buf);
                visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr, vmbuffer, visibility_cutoff_xid, free_dict);
            } else if (!all_visible_according_to_vm) {
                /*
                 * It should never be the case that the visibility map page is
                 * set while the page-level bit is clear, but the reverse is
                 * allowed.  Set the visibility map bit as well so that we get
                 * back in sync.
                 */
                visibilitymap_set(onerel, blkno, buf, InvalidXLogRecPtr, vmbuffer, visibility_cutoff_xid, false);
            }
        } else if (all_visible_according_to_vm && !PageIsAllVisible(page) &&
                 visibilitymap_test(onerel, blkno, &vmbuffer)) {
            /*
             * As of PostgreSQL 9.2, the visibility map bit should never be set if
             * the page-level bit is clear.  However, it's possible that the bit
             * got cleared after we checked it and before we took the buffer
             * content lock, so we must recheck before jumping to the conclusion
             * that something bad has happened.
             */
            ereport(WARNING,
                (errmsg("page is not marked all-visible but visibility map bit is set in relation \"%s\" page %u",
                    relname,
                    blkno)));
            visibilitymap_clear(onerel, blkno, vmbuffer);
        } else if (PageIsAllVisible(page) && has_dead_tuples) {
            /*
             * It's possible for the value returned by GetOldestXmin() to move
             * backwards, so it's not wrong for us to see tuples that appear to
             * not be visible to everyone yet, while PD_ALL_VISIBLE is already
             * set. The real safe xmin value never moves backwards, but
             * GetOldestXmin() is conservative and sometimes returns a value
             * that's unnecessarily small, so if we see that contradiction it just
             * means that the tuples that we think are not visible to everyone yet
             * actually are, and the PD_ALL_VISIBLE flag is correct.
             *
             * There should never be dead tuples on a page with PD_ALL_VISIBLE
             * set, however.
             */
            ereport(WARNING,
                (errmsg("page containing dead tuples is marked as all-visible in relation \"%s\" page %u",
                    relname,
                    blkno)));
            PageClearAllVisible(page);
            MarkBufferDirty(buf);
            visibilitymap_clear(onerel, blkno, vmbuffer);
        }

        UnlockReleaseBuffer(buf);

        /* Remember the location of the last page with nonremovable tuples */
        if (hastup)
            vacrelstats->nonempty_pages = blkno + 1;

        /*
         * If we remembered any tuples for deletion, then the page will be
         * visited again by lazy_vacuum_heap, which will compute and record
         * its post-compaction free space.	If not, then we're done with this
         * page, so remember its free space as-is.	(This path will always be
         * taken if there are no indexes.)
         */
        if (vacrelstats->num_dead_tuples == prev_dead_count)
            RecordPageWithFreeSpace(onerel, blkno, freespace);
    }

    /* save stats for use later */
    vacrelstats->scanned_tuples = num_tuples;
    vacrelstats->tuples_deleted = tups_vacuumed;

    /* now we can compute the new value for pg_class.reltuples */
    vacrelstats->new_rel_tuples = vac_estimate_reltuples(onerel, nblocks, vacrelstats->scanned_pages, num_tuples);

    /*
     * Release any remaining pin on visibility map page.
     */
    if (BufferIsValid(vmbuffer)) {
        ReleaseBuffer(vmbuffer);
        vmbuffer = InvalidBuffer;
    }

    /* If any tuples need to be deleted, perform final vacuum cycle */
    /* XXX put a threshold on min number of tuples here? */
    if (vacrelstats->num_dead_tuples > vacrelstats->curr_heap_start) {
        /* Log cleanup info before we touch indexes */
        vacuum_log_cleanup_info(onerel, vacrelstats);

        /* Remove index entries */
        for (i = 0; i < nindexes; i++) {
            if (!RelationIsCrossBucketIndex(Irel[i])) {
                lazy_vacuum_index(Irel[i], &indstats[i], vacrelstats, vac_strategy);
            }
        }
        vacrelstats->num_index_scans++;
    }

    /* Do post-vacuum cleanup and statistics update for each index */
    for (i = 0; i < nindexes; i++) {
        if (RelationIsCrossBucketIndex(Irel[i])) {
            continue;
        }
        /* IO collector and IO scheduler for vacuum */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);

        indstats[i] = lazy_cleanup_index(Irel[i], indstats[i], vacrelstats, vac_strategy);
    }

    /* record vacuumed tuple for reporting to PgStatCollector */
    *ptrDeleteTupleNum = tups_vacuumed;

    /* If no indexes, make log report that lazy_vacuum_heap would've made */
    if (vacuumed_pages)
        ereport(elevel,
            (errmsg("\"%s\": removed %.0f row versions in %u pages",
                RelationGetRelationName(onerel),
                tups_vacuumed,
                vacuumed_pages)));
    /* If use vacuum verbose, send messages to client, otherwise log detail result */
    ereport(elevel, (errmodule(MOD_VACUUM),
        errmsg("\"%s\": found %.0f removable, %.0f nonremovable row versions in %u out of %u pages",
            RelationGetRelationName(onerel),
            tups_vacuumed,
            num_tuples,
            vacrelstats->scanned_pages,
            nblocks),
            errdetail("%.0f dead row versions cannot be removed yet. "
                      "There were %.0f unused item pointers. "
                      "%u pages are entirely empty. "
                      "%s.",
                nkeep,
                nunused,
                empty_pages,
                pg_rusage_show(&ru0))));
    /* Add info to print stat */
    printStats->tupsVacuumed += tups_vacuumed;
    printStats->numTuples += num_tuples;
    printStats->scannedPages += vacrelstats->scanned_pages;
    printStats->nblocks += nblocks;
    printStats->nkeep += nkeep;
    printStats->nunused += nunused;
    printStats->emptyPages += empty_pages;

    gstrace_exit(GS_TRC_ID_lazy_scan_heap);
    return indstats;
}

static void lazy_vacuum_all_heap(Relation onerel, LVRelStats* vacrelstats)
{
    int tupindex = 0;
    while (tupindex < vacrelstats->num_dead_tuples) {
        Relation targetRel = NULL;
        int2 bktId = vacrelstats->dead_tuples[tupindex].bktId;
        if (bktId == InvalidBktId || bktId == RelationGetBktid(onerel)) {
            targetRel = onerel;
        } else {
            Relation parentRel = RelationIsBucket(onerel) ? onerel->parent : onerel;
            targetRel = bucketGetRelation(parentRel, NULL, bktId);
        }
        tupindex = lazy_vacuum_heap(targetRel, vacrelstats, tupindex);
        if (targetRel != onerel) {
            bucketCloseRelation(targetRel);
        }
    }
}

/*
 *	lazy_vacuum_heap() -- second pass over the heap
 *
 *		This routine marks dead tuples as unused and compacts out free
 *		space on their pages.  Pages not having dead tuples recorded from
 *		lazy_scan_heap are not visited at all.
 *
 * Note: the reason for doing this as a second pass is we cannot remove
 * the tuples until we've removed their index entries, and we want to
 * process index entry removal in batches as large as possible.
 */

static int lazy_vacuum_heap(Relation onerel, LVRelStats* vacrelstats, int tupindex)
{
    int npages;
    PGRUsage ru0;

    gstrace_entry(GS_TRC_ID_lazy_vacuum_heap);

    pg_rusage_init(&ru0);
    npages = 0;

    while (tupindex < vacrelstats->num_dead_tuples) {
        int2 bktId;
        BlockNumber tblk;
        Buffer buf;
        Page page;
        Size freespace;

        vacuum_delay_point();
        bktId = vacrelstats->dead_tuples[tupindex].bktId;
        if (bktId != InvalidBktId && bktId != RelationGetBktid(onerel)) {
            break;
        }
        tblk = ItemPointerGetBlockNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupindex]));
        buf = ReadBufferExtended(onerel, MAIN_FORKNUM, tblk, RBM_NORMAL, vac_strategy);
        if (!ConditionalLockBufferForCleanup(buf)) {
            ReleaseBuffer(buf);
            ++tupindex;
            continue;
        }
        tupindex = lazy_vacuum_page(onerel, tblk, buf, tupindex, vacrelstats);

        /* Now that we've compacted the page, record its available space */
        page = BufferGetPage(buf);
        freespace = PageGetHeapFreeSpace(page);

        UnlockReleaseBuffer(buf);
        RecordPageWithFreeSpace(onerel, tblk, freespace);
        npages++;
    }

    ereport(elevel,
        (errmsg("vacuum %u/%u/%u, \"%s\": removed %d row versions in %d pages",
            onerel->rd_node.spcNode, onerel->rd_node.dbNode, onerel->rd_node.relNode,
            RelationGetRelationName(onerel), tupindex, npages),
            errdetail("%s.", pg_rusage_show(&ru0))));
    gstrace_exit(GS_TRC_ID_lazy_vacuum_heap);

    return tupindex;
}

/*
 *	lazy_vacuum_page() -- free dead tuples on a page
 *					 and repair its fragmentation.
 *
 * Caller must hold pin and buffer cleanup lock on the buffer.
 *
 * tupindex is the index in vacrelstats->dead_tuples of the first dead
 * tuple for this page.  We assume the rest follow sequentially.
 * The return value is the first tupindex after the tuples of this page.
 */
static int lazy_vacuum_page(Relation onerel, BlockNumber blkno, Buffer buffer, int tupindex, LVRelStats* vacrelstats)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber unused[MaxOffsetNumber];
    int uncnt = 0;

    START_CRIT_SECTION();

    for (; tupindex < vacrelstats->num_dead_tuples; tupindex++) {
        BlockNumber tblk;
        OffsetNumber toff;
        ItemId itemid;

        int2 bktId = vacrelstats->dead_tuples[tupindex].bktId;
        if (bktId != InvalidBktId && bktId != RelationGetBktid(onerel)) 
            break;
        tblk = ItemPointerGetBlockNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupindex]));
        if (tblk != blkno)
            break; /* past end of tuples for this block */
        toff = ItemPointerGetOffsetNumber(VacItemPtrDataGetItemPtr(vacrelstats->dead_tuples[tupindex]));
        itemid = PageGetItemId(page, toff);
        ItemIdSetUnused(itemid);
        unused[uncnt++] = toff;
    }

    PageRepairFragmentation(page);

    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(onerel)) {
        XLogRecPtr recptr;

        recptr = log_heap_clean(onerel, buffer, NULL, 0, NULL, 0, unused, uncnt, vacrelstats->latestRemovedXid, true);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    return tupindex;
}

/*
 *	lazy_check_needs_freeze() -- scan page to see if any tuples
 *					 need to be cleaned or freezen
 *
 * Returns true if the page needs to be vacuumed using cleanup lock.
 */
static bool lazy_check_needs_freeze(Buffer buf)
{
    Page page;
    OffsetNumber offnum, maxoff;
    HeapTupleData tuple;
    page = BufferGetPage(buf);
    if (PageIsNew(page) || PageIsEmpty(page)) {
        /* PageIsNew probably shouldn't happen... */
        return false;
    }
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        itemid = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(itemid))
            continue;

        tuple.t_data = (HeapTupleHeader)PageGetItem(page, itemid);
        tuple.t_len = ItemIdGetLength(itemid);
        HeapTupleCopyBaseFromPage(&tuple, page);
        ItemPointerSet(&(tuple.t_self), BufferGetBlockNumber(buf), offnum);

        if (heap_tuple_needs_freeze(&tuple, u_sess->cmd_cxt.FreezeLimit, u_sess->cmd_cxt.MultiXactFrzLimit, buf))
            return true;
    } /* scan along page */

    return false;
}

/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		vacrelstats->dead_tuples, and update running statistics.
 */
void lazy_vacuum_index(Relation indrel, IndexBulkDeleteResult **stats, const LVRelStats *vacrelstats,
    BufferAccessStrategy vacStrategy)
{
    IndexVacuumInfo ivinfo;
    PGRUsage ru0;

    gstrace_entry(GS_TRC_ID_lazy_vacuum_index);
    pg_rusage_init(&ru0);

    ivinfo.index = indrel;
    ivinfo.analyze_only = false;
    ivinfo.estimated_count = true;
    ivinfo.message_level = elevel;
    ivinfo.num_heap_tuples = vacrelstats->old_rel_tuples;
    ivinfo.strategy = vacStrategy;
    ivinfo.invisibleParts = NULL;

    /* Do bulk deletion */
    if (RelationIsCrossBucketIndex(indrel)) {
        *stats = index_bulk_delete(&ivinfo, *stats, cbi_lazy_tid_reaped, (void*)vacrelstats);
    } else {
        *stats = index_bulk_delete(&ivinfo, *stats, lazy_tid_reaped, (void*)vacrelstats);
    }

    ereport(elevel,
        (errmsg("scanned index \"%s\" to remove %d row versions",
            RelationGetRelationName(indrel),
            vacrelstats->num_dead_tuples),
            errdetail("%s.", pg_rusage_show(&ru0))));
    gstrace_exit(GS_TRC_ID_lazy_vacuum_index);
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
/* ADD PARA: indPartRel, indPart */
extern IndexBulkDeleteResult *lazy_cleanup_index(Relation indrel, IndexBulkDeleteResult *stats, LVRelStats *vacrelstats,
    BufferAccessStrategy vac_strategy)
{
    IndexVacuumInfo ivinfo;
    PGRUsage ru0;

    gstrace_entry(GS_TRC_ID_lazy_cleanup_index);
    pg_rusage_init(&ru0);

    ivinfo.index = indrel;
    ivinfo.analyze_only = false;
    ivinfo.estimated_count = (vacrelstats->scanned_pages < vacrelstats->rel_pages);
    ivinfo.message_level = elevel;
    ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
    ivinfo.strategy = vac_strategy;
    ivinfo.invisibleParts = NULL;
    stats = index_vacuum_cleanup(&ivinfo, stats);
    if (stats != NULL) {
        ereport(elevel,
            (errmsg("index \"%s\" now contains %.0f row versions in %u pages",
                RelationGetRelationName(indrel),
                stats->num_index_tuples,
                stats->num_pages),
                errdetail("%.0f index row versions were removed.\n"
                          "%u index pages have been deleted, %u are currently reusable.\n"
                          "%s.",
                    stats->tuples_removed,
                    stats->pages_deleted,
                    stats->pages_free,
                    pg_rusage_show(&ru0))));
    }
    gstrace_exit(GS_TRC_ID_lazy_cleanup_index);
    return stats;
}

#define UNLOCK_REL_FOR_TRUNCATE(onerel, prel, relid)                        \
    do {                                                                    \
        if (RelationIsPartition(onerel)) {                                  \
            UnLockPartitionVacuum((prel), (relid), AccessExclusiveLock);    \
        } else {                                                            \
            UnlockRelation((onerel), AccessExclusiveLock);                  \
        }                                                                   \
    } while (0)

/*
 * lazy_truncate_heap - try to truncate off any empty pages at the end
 */
static void
lazy_truncate_heap(Relation onerel, VacuumStmt *vacstmt, LVRelStats *vacrelstats)
{
    /* It's meaningless to truncate segment-page tables, because they do not truncate physical files actually. */
    if (RelationIsSegmentTable(onerel)) {
        return;
    }

    BlockNumber old_rel_pages = vacrelstats->rel_pages;
    BlockNumber new_rel_pages;
    PGRUsage    ru0;
    Relation    prel = vacstmt->onepartrel;
    Oid         relid = onerel->rd_id;
    int         lock_retry;
    int         messageLevel = -1;

    gstrace_entry(GS_TRC_ID_lazy_truncate_heap);
    pg_rusage_init(&ru0);
    Assert(OidIsValid(relid));

    /*
     * Loop until no more truncating can be done.
     */
    do {
        /*
         * We need full exclusive lock on the relation in order to do
         * truncation. If we can't get it, give up rather than waiting --- we
         * don't want to block other backends, and we don't want to deadlock
         * (which is quite possible considering we already hold a lower-grade
         * lock).
         */
        vacrelstats->lock_waiter_detected = false;
        lock_retry = 0;
        while (true) {
            if (RelationIsPartition(onerel)) {
                if (ConditionalLockPartitionVacuum(prel, relid, AccessExclusiveLock))
                    break;
            } else {
                if (ConditionalLockRelation(onerel, AccessExclusiveLock))
                    break;
            }

            /*
             * Check for interrupts while trying to (re-)acquire the exclusive
             * lock.
             */
            CHECK_FOR_INTERRUPTS();

            if (++lock_retry > (AUTOVACUUM_TRUNCATE_LOCK_TIMEOUT / AUTOVACUUM_TRUNCATE_LOCK_WAIT_INTERVAL)) {
                /*
                 * We failed to establish the lock in the specified number of
                 * retries. This means we give up truncating. We used to
                 * skip following statistics report to pgstat, as well as ANALYZE
                 * option, hoping that autovaccum will be triggerd again to vacuum
                 * this rel and try truncation. However, the tail blocks may no longer
                 * be empty by that time and query performance may deteriorate without
                 * ANALYZE. Therefore, at present, we do not skip these steps any more.
                 */
                vacrelstats->lock_waiter_detected = true;
                ereport(LOG,
                        (errmsg("automatic vacuum of table \"%s.%s.%s\": could not (re)acquire exclusive "
                        "lock for truncate scan", get_database_name(u_sess->proc_cxt.MyDatabaseId),
                        get_namespace_name(RelationGetNamespace(onerel)), RelationGetRelationName(onerel))));
                gstrace_exit(GS_TRC_ID_lazy_truncate_heap);
                return;
            }

            pg_usleep(AUTOVACUUM_TRUNCATE_LOCK_WAIT_INTERVAL);
        }

        /*
         * Now that we have exclusive lock, look to see if the rel has grown
         * whilst we were vacuuming with non-exclusive lock.  If so, give up;
         * the newly added pages presumably contain non-deletable tuples.
         */
        new_rel_pages = RelationGetNumberOfBlocks(onerel);
        if (new_rel_pages != old_rel_pages) {
            /*
             * Note: we intentionally don't update vacrelstats->rel_pages with
             * the new rel size here.  If we did, it would amount to assuming
             * that the new pages are empty, which is unlikely. Leaving the
             * numbers alone amounts to assuming that the new pages have the
             * same tuple density as existing ones, which is less unlikely.
             */
            UNLOCK_REL_FOR_TRUNCATE(onerel, prel, relid);
            gstrace_exit(GS_TRC_ID_lazy_truncate_heap);
            return;
        }

        /*
         * Scan backwards from the end to verify that the end pages actually
         * contain no tuples.  This is *necessary*, not optional, because
         * other backends could have added tuples to these pages whilst we
         * were vacuuming.
         */
        new_rel_pages = count_nondeletable_pages(onerel, vacrelstats);

        if (new_rel_pages >= old_rel_pages) {
            UNLOCK_REL_FOR_TRUNCATE(onerel, prel, relid);
            gstrace_exit(GS_TRC_ID_lazy_truncate_heap);
            return;
        }

        /*
         * Okay to truncate.
         */
        if (RelationIsPartition(onerel)) {
            Assert(vacstmt->onepart && vacstmt->onepartrel);
            PartitionTruncate(vacstmt->onepartrel, vacstmt->onepart, new_rel_pages);
        } else {
            RelationTruncate(onerel, new_rel_pages);
        }

        /*
         * We can release the exclusive lock as soon as we have truncated.
         * Other backends can't safely access the relation until they have
         * processed the smgr invalidation that smgrtruncate sent out ... but
         * that should happen as part of standard invalidation processing once
         * they acquire lock on the relation.
         */
        UNLOCK_REL_FOR_TRUNCATE(onerel, prel, relid);

        /*
         * Update statistics.  Here, it *is* correct to adjust rel_pages
         * without also touching reltuples, since the tuple count wasn't
         * changed by the truncation.
         */
        vacrelstats->pages_removed += old_rel_pages - new_rel_pages;
        vacrelstats->rel_pages = new_rel_pages;

        if ((uint32)vacstmt->options & VACOPT_VERBOSE) {
            messageLevel = VERBOSEMESSAGE;
        } else {
            messageLevel = elevel;
        }

        ereport(messageLevel,
                (errmsg("\"%s\": truncated %u to %u pages",
                    RelationGetRelationName(onerel), old_rel_pages, new_rel_pages),
                 errdetail("%s.", pg_rusage_show(&ru0))));
        old_rel_pages = new_rel_pages;
    } while (new_rel_pages > vacrelstats->nonempty_pages && vacrelstats->lock_waiter_detected);
    gstrace_exit(GS_TRC_ID_lazy_truncate_heap);
}

/*
 * Rescan end pages to verify that they are (still) empty of tuples.
 *
 * Returns number of nondeletable pages (last nonempty page + 1).
 */
static BlockNumber
count_nondeletable_pages(Relation onerel, LVRelStats *vacrelstats)
{
    BlockNumber blkno;
    instr_time	starttime;
    instr_time	currenttime;
    instr_time	elapsed;

    /* Initialize the starttime if we check for conflicting lock requests */
    INSTR_TIME_SET_CURRENT(starttime);

    /* Strange coding of loop control is needed because blkno is unsigned */
    blkno = vacrelstats->rel_pages;
    while (blkno > vacrelstats->nonempty_pages) {
        Buffer          buf;
        Page            page;
        bool            hastup = NULL;

        /*
         * Check if another process requests a lock on our relation. We are
         * holding an AccessExclusiveLock here, so they will be waiting. We
         * only do this in autovacuum_truncate_lock_check millisecond
         * intervals, and we only check if that interval has elapsed once
         * every 32 blocks to keep the number of system calls and actual
         * shared lock table lookups to a minimum.
         */
        if ((blkno % 32) == 0) {
            INSTR_TIME_SET_CURRENT(currenttime);
            elapsed = currenttime;
            INSTR_TIME_SUBTRACT(elapsed, starttime);
            if ((INSTR_TIME_GET_MICROSEC(elapsed) / 1000) >= AUTOVACUUM_TRUNCATE_LOCK_CHECK_INTERVAL) {
                if (RelationIsPartition(onerel)) {
                    if (LockHasWaitersPartition(onerel, AccessExclusiveLock)) {
                        ereport(elevel,
                                (errmsg("\"%s\": suspending truncate due to conflicting lock request",
                                RelationGetRelationName(onerel))));

                        vacrelstats->lock_waiter_detected = true;
                        return blkno;
                    }
                } else {
                    if (LockHasWaitersRelation(onerel, AccessExclusiveLock)) {
                        ereport(elevel,
                                (errmsg("\"%s\": suspending truncate due to conflicting lock request",
                                RelationGetRelationName(onerel))));

                        vacrelstats->lock_waiter_detected = true;
                        return blkno;
                    }
                }
                starttime = currenttime;
            }
        }

        /*
         * We don't insert a vacuum delay point here, because we have an
         * exclusive lock on the table which we want to hold for as short a
         * time as possible.  We still need to check for interrupts however.
         */
        CHECK_FOR_INTERRUPTS();

        blkno--;

        buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno, RBM_NORMAL, vac_strategy);

        /* In this phase we only need shared access to the buffer */
        LockBuffer(buf, BUFFER_LOCK_SHARE);

        page = BufferGetPage(buf);

        hastup = RelationIsUstoreFormat(onerel) ? UHeapPageCheckForUsedLinePointer(page, onerel) :
                                                          HeapPageCheckForUsedLinePointer(page);

        UnlockReleaseBuffer(buf);

        /* Done scanning if we found a tuple here */
        if (hastup)
            return blkno + 1;
    }

    /*
     * If we fall out of the loop, all the previously-thought-to-be-empty
     * pages still are; we need not bother to look at the last known-nonempty
     * page.
     */
    return vacrelstats->nonempty_pages;
}

/*
 * lazy_space_alloc - space allocation decisions for lazy vacuum
 *
 * See the comments at the head of this file for rationale.
 */
static void lazy_space_alloc(LVRelStats* vacrelstats, BlockNumber relblocks)
{
    unsigned long maxtuples;

    if (vacrelstats->hasindex) {
        maxtuples = (u_sess->attr.attr_memory.maintenance_work_mem * 1024L) / sizeof(VacItemPointerData);
        maxtuples = Min(maxtuples, INT_MAX);
        maxtuples = Min(maxtuples, (int)(MaxAllocSize / sizeof(VacItemPointerData)));
        /* curious coding here to ensure the multiplication can't overflow */
        if ((BlockNumber)(maxtuples / LAZY_ALLOC_TUPLES) > relblocks)
            maxtuples = relblocks * LAZY_ALLOC_TUPLES;
        /* stay sane if small maintenance_work_mem */
        maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
    } else {
        maxtuples = MaxHeapTuplesPerPage;
    }

    vacrelstats->num_dead_tuples = 0;
    vacrelstats->max_dead_tuples = (int)maxtuples;
    vacrelstats->dead_tuples = (VacItemPointer)palloc(maxtuples * sizeof(VacItemPointerData));
}

/*
 * lazy_record_dead_tuple - remember one deletable tuple
 */
extern void lazy_record_dead_tuple(LVRelStats* vacrelstats, ItemPointer itemptr)
{
    /*
     * The array shouldn't overflow under normal behavior, but perhaps it
     * could if we are given a really small maintenance_work_mem. In that
     * case, just forget the last few tuples (we'll get 'em next time).
     */
    if (vacrelstats->num_dead_tuples < vacrelstats->max_dead_tuples) {
        vacrelstats->dead_tuples[vacrelstats->num_dead_tuples].itemPointerData = *itemptr;
        vacrelstats->dead_tuples[vacrelstats->num_dead_tuples].bktId = vacrelstats->currVacuumBktId;
        vacrelstats->num_dead_tuples++;
    }
}

/*
 * lazy_tid_reaped() -- is a particular tid deletable?
 *      This has the right signature to be an IndexBulkDeleteCallback.
 *      Assumes dead_tuples array is in sorted order.
 *      inputparam partOid is valid only when index is global partition index
 *      inputparam bktId is valid only when index is crossbucket index
 */


static bool cbi_lazy_tid_reaped(ItemPointer itemptr, void* state, Oid partOid, int2 bktId)
{
    LVRelStats* vacrelstats = (LVRelStats*)state;
    VacItemPointer res;

    VacItemPointerData vacItemPointerData;
    vacItemPointerData.itemPointerData = *itemptr;
    vacItemPointerData.bktId = bktId;

    /* global partition index tuple need to check the tuple's partOid is same to current partition */
    if (lookupHBucketid(vacrelstats->bucketlist, 0, bktId) == -1) {
        return true;
    }
    if (partOid != InvalidOid && vacrelstats->currVacuumPartOid != partOid) {
        return false;
    }

    res = (VacItemPointer)bsearch((void*)&vacItemPointerData,
        (void*)vacrelstats->dead_tuples,
        vacrelstats->num_dead_tuples,
        sizeof(VacItemPointerData),
        cbi_vac_cmp_itemptr);

    return (res != NULL);
}

static bool lazy_tid_reaped(ItemPointer itemptr, void* state, Oid partOid, int2 bktId)
{
    LVRelStats* vacrelstats = (LVRelStats*)state;
    VacItemPointer res;

    VacItemPointerData vacItemPointerData;
    vacItemPointerData.itemPointerData = *itemptr;
    vacItemPointerData.bktId = bktId;

    /* global partition index tuple need to check the tuple's partOid is same to current partition */
    if (partOid != InvalidOid && vacrelstats->currVacuumPartOid != partOid) {
        return false;
    }

    res = (VacItemPointer)bsearch((void*)&vacItemPointerData,
        (void*)&(vacrelstats->dead_tuples[vacrelstats->curr_heap_start]),
        vacrelstats->num_dead_tuples - vacrelstats->curr_heap_start,
        sizeof(VacItemPointerData),
        vac_cmp_itemptr);

    return (res != NULL);
}

/*
 * Comparator routines for use with qsort() and bsearch().
 */

static int cbi_vac_cmp_itemptr(const void* left, const void* right)
{
    int2 lbktid, rbktid;
    BlockNumber lblk, rblk;
    OffsetNumber loff, roff;
    lbktid = ((VacItemPointer)left)->bktId;
    rbktid = ((VacItemPointer)right)->bktId;
    if (lbktid < rbktid)
        return -1;
    if (lbktid > rbktid)
        return 1;
    lblk = ItemPointerGetBlockNumber(VacItemPtrGetItemPtr((VacItemPointer)left));
    rblk = ItemPointerGetBlockNumber(VacItemPtrGetItemPtr((VacItemPointer)right));
    if (lblk < rblk)
        return -1;
    if (lblk > rblk)
        return 1;
    loff = ItemPointerGetOffsetNumber(VacItemPtrGetItemPtr((VacItemPointer)left));
    roff = ItemPointerGetOffsetNumber(VacItemPtrGetItemPtr((VacItemPointer)right));
    if (loff < roff)
        return -1;
    if (loff > roff)
        return 1;
    return 0;
}
static int vac_cmp_itemptr(const void* left, const void* right)
{
    BlockNumber lblk, rblk;
    OffsetNumber loff, roff;
    lblk = ItemPointerGetBlockNumber(VacItemPtrGetItemPtr((VacItemPointer)left));
    rblk = ItemPointerGetBlockNumber(VacItemPtrGetItemPtr((VacItemPointer)right));
    if (lblk < rblk)
        return -1;
    if (lblk > rblk)
        return 1;
    loff = ItemPointerGetOffsetNumber(VacItemPtrGetItemPtr((VacItemPointer)left));
    roff = ItemPointerGetOffsetNumber(VacItemPtrGetItemPtr((VacItemPointer)right));
    if (loff < roff)
        return -1;
    if (loff > roff)
        return 1;
    return 0;
}

void elogVacuumInfo(Relation rel, HeapTuple tuple, char* funcName, TransactionId oldestxmin)
{
    bool ignore = false;
    HeapTupleHeader htup = tuple->t_data;
    /* Just log info of pg_statistic and related toast table */
    if ((RelationGetRelid(rel) == StatisticRelationId) ||
        (!RelationIsPartition(rel) && (IsToastRelationbyOid(RelationGetRelid(rel))) &&
            (pg_toast_get_baseid(RelationGetRelid(rel), &ignore) == StatisticRelationId)) ||
        (RelationGetRelid(rel) == AttributeRelationId) || (RelationGetRelid(rel) == TypeRelationId) ||
        (RelationGetRelid(rel) == RelationRelationId) || (RelationGetRelid(rel) == PartitionRelationId)) {
        if (HeapTupleHeaderHasOid(htup))
            ereport(LOG,
                (errmsg("In %s: xid:" XID_FMT ", pid: %lu; tuple: oid:%u, xmin:" XID_FMT ", xmax:" XID_FMT
                        ", ctid:%u/%hu,"
                        "infomask:%hu, infomask2:%hu; RecentXmin:" XID_FMT ", OldestXmin:" XID_FMT
                        ", useLocalSnapshot:%d, relationid: %u.",
                    funcName,
                    t_thrd.pgxact->xid,
                    t_thrd.proc->pid,
                    HeapTupleHeaderGetOid(htup),
                    HeapTupleGetRawXmin(tuple),
                    HeapTupleGetRawXmax(tuple),
                    ItemPointerGetBlockNumber(&htup->t_ctid),
                    ItemPointerGetOffsetNumber(&htup->t_ctid),
                    htup->t_infomask,
                    htup->t_infomask2,
                    u_sess->utils_cxt.RecentXmin,
                    oldestxmin,
                    t_thrd.xact_cxt.useLocalSnapshot,
                    RelationGetRelid(rel))));
        else
            ereport(LOG,
                (errmsg("In %s: xid:" XID_FMT ", pid: %lu; tuple: xmin:" XID_FMT ", xmax:" XID_FMT ", ctid:%u/%hu,"
                        "infomask:%hu, infomask2:%hu; RecentXmin:" XID_FMT ", OldestXmin:" XID_FMT
                        ", useLocalSnapshot:%d, relationid: %u.",
                    funcName,
                    t_thrd.pgxact->xid,
                    t_thrd.proc->pid,
                    HeapTupleGetRawXmin(tuple),
                    HeapTupleGetRawXmax(tuple),
                    ItemPointerGetBlockNumber(&htup->t_ctid),
                    ItemPointerGetOffsetNumber(&htup->t_ctid),
                    htup->t_infomask,
                    htup->t_infomask2,
                    u_sess->utils_cxt.RecentXmin,
                    oldestxmin,
                    t_thrd.xact_cxt.useLocalSnapshot,
                    RelationGetRelid(rel))));
    }
}

/*
 * Return true if there is any used line pointer on the page.
 * Assume the caller has atleast a shared lock
 */
static bool UHeapPageCheckForUsedLinePointer(Page page, Relation relation)
{
    if (PageIsNew(page) || UPageIsEmpty((UHeapPageHeaderData *)page)) {
        return false;
    }

    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber((char *)page);
    for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        RowPtr *rp = UPageGetRowPtr(page, offnum);
        if (RowPtrIsUsed(rp)) {
            return true;
        }
    }

    return false;
}

/*
 * Same as UHeapPageCheckForUsedLinePointer but for Heap.
 */
static bool HeapPageCheckForUsedLinePointer(Page page)
{
    if (PageIsNew(page) || PageIsEmpty(page)) {
        return false;
    }

    OffsetNumber maxoff = PageGetMaxOffsetNumber((char *)page);
    for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (ItemIdIsUsed(itemid)) {
            return true;
        }
    }

    return false;
}
