/* -------------------------------------------------------------------------
 *
 * indexam.cpp
 *	  general index access method routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/index/indexam.cpp
 *
 * INTERFACE ROUTINES
 *		index_open		- open an index relation by relation OID
 *		index_close		- close an index relation
 *		index_beginscan - start a scan of an index with amgettuple
 *		index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *		index_rescan	- restart a scan of an index
 *		index_endscan	- end a scan
 *		index_insert	- insert an index tuple into a relation
 *		index_markpos	- mark a scan position
 *		index_restrpos	- restore a scan position
 *		index_getnext_tid	- get the next TID from a scan
 *		IndexFetchTuple		- get the scan's next tuple
 *		index_getnext	- get the next tuple from a scan
 *		index_getbitmap - get all tuples from a scan
 *		index_column_getbitmap
 *		index_bulk_delete	- bulk deletion of index tuples
 *		index_vacuum_cleanup	- post-deletion cleanup of an index
 *		index_can_return	- does index support index-only scans?
 *		index_getprocid - get a support procedure OID
 *		index_getprocinfo - get a support procedure's lookup info
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *
 * old comments
 *		Scans are implemented as follows:
 *
 *		`0' represents an invalid item pointer.
 *		`-' represents an unknown item pointer.
 *		`X' represents a known item pointers.
 *		`+' represents known or invalid item pointers.
 *		`*' represents any item pointers.
 *
 *		State is represented by a triple of these symbols in the order of
 *		previous, current, next.  Note that the case of reverse scans works
 *		identically.
 *
 *				State	Result
 *		(1)		+ + -	+ 0 0			(if the next item pointer is invalid)
 *		(2)				+ X -			(otherwise)
 *		(3)		* 0 0	* 0 0			(no change)
 *		(4)		+ X 0	X 0 0			(shift)
 *		(5)		* + X	+ X -			(shift, add unknown)
 *
 *		All other states cannot occur.
 *
 *		Note: It would be possible to cache the status of the previous and
 *			  next item pointer using the flags.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "catalog/catalog.h"
#include "pgstat.h"
#include "replication/bcm.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/knl_uscan.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "vecexecutor/vecnodes.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"

#ifdef PGXC
#include "utils/lsyscache.h"
#include "pgxc/pgxc.h"
#endif

/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.	This helps to catch
 * things that don't work when reindexing system catalogs.  The assertion
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS do { \
    AssertMacro(RelationIsValid(index_relation));                             \
    AssertMacro(PointerIsValid(index_relation->rd_am));                       \
    AssertMacro(!ReindexIsProcessingIndex(RelationGetRelid(index_relation))); \
} while (0)

#define SCAN_CHECKS do { \
    AssertMacro(IndexScanIsValid(scan));                     \
    AssertMacro(RelationIsValid(scan->indexRelation));       \
    AssertMacro(PointerIsValid(scan->indexRelation->rd_am)); \
} while (0)

#define GET_REL_PROCEDURE(pname) do { \
    procedure = &index_relation->rd_aminfo->pname;                              \
    if (!OidIsValid(procedure->fn_oid)) {                                       \
        RegProcedure procOid = index_relation->rd_am->pname;                    \
        if (!RegProcedureIsValid(procOid))                                      \
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),       \
                            errmsg("invalid %s regproc", CppAsString(pname)))); \
        fmgr_info_cxt(procOid, procedure, index_relation->rd_indexcxt);         \
    }                                                                           \
} while (0)

#define GET_SCAN_PROCEDURE(pname) do { \
    procedure = &scan->indexRelation->rd_aminfo->pname;                         \
    if (!OidIsValid(procedure->fn_oid)) {                                       \
        RegProcedure procOid = scan->indexRelation->rd_am->pname;               \
        if (!RegProcedureIsValid(procOid))                                      \
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),       \
                            errmsg("invalid %s regproc", CppAsString(pname)))); \
        fmgr_info_cxt(procOid, procedure, scan->indexRelation->rd_indexcxt);    \
    }                                                                           \
} while (0)

static IndexScanDesc index_beginscan_internal(Relation index_relation, int nkeys, int norderbys, Snapshot snapshot);

/* ----------------
 *		index_open - open an index relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the index.	(Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		index already.)
 *
 *		An error is raised if the index does not exist.
 *
 *		This is a convenience routine adapted for indexscan use.
 *		Some callers may prefer to use relation_open directly.
 * ----------------
 */
Relation index_open(Oid relation_id, LOCKMODE lockmode, int2 bucket_id)
{
    Relation r;

    r = relation_open(relation_id, lockmode, bucket_id);
    if (!RelationIsIndex(r)) {
        ereport(
            ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not an index", RelationGetRelationName(r))));
    }
    return r;
}

/* ----------------
 *		index_close - close an index relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond index_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void index_close(Relation relation, LOCKMODE lockmode)
{
    Relation rel = relation;
    LockRelId relid;

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    if (RelationIsBucket(relation)) {
        rel = relation->parent;
        Assert(RELATION_OWN_BUCKET(rel));
        bucketCloseRelation(relation);
    }

    relid = rel->rd_lockInfo.lockRelId;

    /* The relcache does the real work... */
    RelationClose(rel);

    if (lockmode != NoLock)
        UnlockRelationId(&relid, lockmode);
}

bool UBTreeDelete(Relation indexRelation, Datum* values, const bool* isnull, ItemPointer heapTCtid,
    bool isRollbackIndex);

void index_delete(Relation index_relation, Datum* values, const bool* isnull, ItemPointer heap_t_ctid,
    bool isRollbackIndex)
{
    /* Assert(Ustore) Assert(B tree) */
    UBTreeDelete(index_relation, values, isnull, heap_t_ctid, isRollbackIndex);
}


/* ----------------
 *		index_insert - insert an index tuple into a relation
 * ----------------
 */
bool index_insert(Relation index_relation, Datum *values, const bool *isnull, ItemPointer heap_t_ctid,
                  Relation heap_relation, IndexUniqueCheck check_unique)
{
    FmgrInfo *procedure = NULL;

    RELATION_CHECKS;
    GET_REL_PROCEDURE(aminsert);

    if (!(index_relation->rd_am->ampredlocks))
        CheckForSerializableConflictIn(index_relation, (HeapTuple)NULL, InvalidBuffer);

    /*
     * have the am's insert proc do all the work.
     */
    if (u_sess->attr.attr_common.enable_indexscan_optimization && index_relation->rd_rel->relam == BTREE_AM_OID) {
        return index_relation->rd_amroutine->aminsert(index_relation, values, isnull, heap_t_ctid, heap_relation, check_unique);
    } else {
        return DatumGetBool(FunctionCall6(procedure, PointerGetDatum(index_relation), PointerGetDatum(values),
                                          PointerGetDatum(isnull), PointerGetDatum(heap_t_ctid),
                                          PointerGetDatum(heap_relation), Int32GetDatum((int32)check_unique)));
    }
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc index_beginscan(
    Relation heap_relation, Relation index_relation, Snapshot snapshot, int nkeys, int norderbys, ScanState* scan_state)
{
    IndexScanDesc scan;

    scan = index_beginscan_internal(index_relation, nkeys, norderbys, snapshot);

    /*
     * Save additional parameters into the scandesc.  Everything else was set
     * up by RelationGetIndexScan.
     */
    scan->heapRelation = heap_relation;
    scan->xs_snapshot = snapshot;

    if (scan->xs_want_ext_oid) {
        scan->xs_gpi_scan->parentRelation = heap_relation;
    }
	
	/* prepare to fetch index matches from table */
    scan->xs_heapfetch = tableam_scan_index_fetch_begin(heap_relation);

    return scan;
}

/*
 * index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *
 * As above, caller had better be holding some lock on the parent heap
 * relation, even though it's not explicitly mentioned here.
 */
IndexScanDesc index_beginscan_bitmap(Relation index_relation, Snapshot snapshot, int nkeys, ScanState* scan_state)
{
    IndexScanDesc scan;

    scan = index_beginscan_internal(index_relation, nkeys, 0, snapshot);

    /*
     * Save additional parameters into the scandesc.  Everything else was set
     * up by RelationGetIndexScan.
     */
    scan->xs_snapshot = snapshot;
#ifdef USE_SPQ
    scan->spq_scan = NULL;
#endif
    return scan;
}

/*
 * index_beginscan_internal --- common code for index_beginscan variants
 */
static IndexScanDesc index_beginscan_internal(Relation index_relation, int nkeys, int norderbys, Snapshot snapshot)
{
    IndexScanDesc scan;
    FmgrInfo *procedure = NULL;

    RELATION_CHECKS;
    GET_REL_PROCEDURE(ambeginscan);

    if (!(index_relation->rd_am->ampredlocks))
        PredicateLockRelation(index_relation, snapshot);

    /*
     * We hold a reference count to the relcache entry throughout the scan.
     */
    RelationIncrementReferenceCount(index_relation);

    /*
     * Tell the AM to open a scan.
     */
    if (u_sess->attr.attr_common.enable_indexscan_optimization && index_relation->rd_rel->relam == BTREE_AM_OID) {
        scan = index_relation->rd_amroutine->ambeginscan(index_relation, nkeys, norderbys);
    } else {
        scan = (IndexScanDesc)DatumGetPointer(
            FunctionCall3(procedure, PointerGetDatum(index_relation), Int32GetDatum(nkeys), Int32GetDatum(norderbys)));
    }
#ifdef USE_SPQ
    scan->spq_scan = NULL;
#endif

    return scan;
}

/* ----------------
 *		index_rescan  - (re)start a scan of an index
 *
 * During a restart, the caller may specify a new set of scankeys and/or
 * orderbykeys; but the number of keys cannot differ from what index_beginscan
 * was told.  (Later we might relax that to "must not exceed", but currently
 * the index AMs tend to assume that scan->numberOfKeys is what to believe.)
 * To restart the scan without changing keys, pass NULL for the key arrays.
 * (Of course, keys *must* be passed on the first call, unless
 * scan->numberOfKeys is zero.)
 * ----------------
 */
void index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    FmgrInfo *procedure = NULL;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amrescan);

    Assert(nkeys == scan->numberOfKeys);
    Assert(norderbys == scan->numberOfOrderBys);

    /* Release resources (like buffer pins) from table accesses */
    if (scan->xs_heapfetch)
        tableam_scan_index_fetch_reset(scan->xs_heapfetch);

    /* Release any held pin on a heap page */
    if (BufferIsValid(scan->xs_cbuf)) {
        ReleaseBuffer(scan->xs_cbuf);
        scan->xs_cbuf = InvalidBuffer;
    }

    scan->xs_continue_hot = false;

    scan->kill_prior_tuple = false; /* for safety */

    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        scan->indexRelation->rd_amroutine->amrescan(scan, keys);
    } else {
        (void)FunctionCall5(procedure, PointerGetDatum(scan), PointerGetDatum(keys), Int32GetDatum(nkeys),
                            PointerGetDatum(orderbys), Int32GetDatum(norderbys));
    }
}

/* ----------------
 *		index_endscan - end a scan
 * ----------------
 */
void index_endscan(IndexScanDesc scan)
{
    FmgrInfo *procedure = NULL;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amendscan);

    /* Release resources (like buffer pins) from table accesses */
    if (scan->xs_heapfetch)
    {
        tableam_scan_index_fetch_end(scan->xs_heapfetch);
        scan->xs_heapfetch = NULL;
    }

    /* Release any held pin on a heap page */
    if (BufferIsValid(scan->xs_cbuf)) {
        ReleaseBuffer(scan->xs_cbuf);
        scan->xs_cbuf = InvalidBuffer;
    }

    /* End the AM's scan */
    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        scan->indexRelation->rd_amroutine->amendscan(scan);
    } else {
        (void)FunctionCall1(procedure, PointerGetDatum(scan));
    }

    /* Release index refcount acquired by index_beginscan */
    RelationDecrementReferenceCount(scan->indexRelation);

    if (scan->xs_gpi_scan != NULL) {
        GPIScanEnd(scan->xs_gpi_scan);
    }

    if (scan->xs_cbi_scan != NULL) {
        cbi_scan_end(scan->xs_cbi_scan);
    }

    /* Release the scan data structure itself */
    IndexScanEnd(scan);
}

/* ----------------
 *		index_markpos  - mark a scan position
 * ----------------
 */
void index_markpos(IndexScanDesc scan)
{
    FmgrInfo *procedure = NULL;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(ammarkpos);

    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        scan->indexRelation->rd_amroutine->ammarkpos(scan);
    } else {
        (void)FunctionCall1(procedure, PointerGetDatum(scan));
    }
}

/* ----------------
 *		index_restrpos	- restore a scan position
 *
 * NOTE: this only restores the internal scan state of the index AM.
 * The current result tuple (scan->xs_ctup) doesn't change.  See comments
 * for ExecRestrPos().
 *
 * NOTE: in the presence of HOT chains, mark/restore only works correctly
 * if the scan's snapshot is MVCC-safe; that ensures that there's at most one
 * returnable tuple in each HOT chain, and so restoring the prior state at the
 * granularity of the index AM is sufficient.  Since the only current user
 * of mark/restore functionality is nodeMergejoin.c, this effectively means
 * that merge-join plans only work for MVCC snapshots.	This could be fixed
 * if necessary, but for now it seems unimportant.
 * ----------------
 */
void index_restrpos(IndexScanDesc scan)
{
    FmgrInfo *procedure = NULL;

    Assert(IsMVCCSnapshot(scan->xs_snapshot));

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amrestrpos);

    /* Release resources (like buffer pins) from table accesses */
    if (scan->xs_heapfetch)
        tableam_scan_index_fetch_reset(scan->xs_heapfetch);

    scan->xs_continue_hot = false;

    scan->kill_prior_tuple = false; /* for safety */

    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        scan->indexRelation->rd_amroutine->amrestrpos(scan);
    } else {
        (void)FunctionCall1(procedure, PointerGetDatum(scan));
    }

}

/* ----------------
 * index_getnext_tid - get the next TID from a scan
 *
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * ----------------
 */
ItemPointer index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
    FmgrInfo *procedure = NULL;
    bool found = false;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amgettuple);

    Assert(TransactionIdIsValid(u_sess->utils_cxt.RecentGlobalXmin));
#ifdef USE_SPQ
rescan:
#endif
    /*
     * The AM's amgettuple proc finds the next index entry matching the scan
     * keys, and puts the TID into scan->xs_ctup.t_self.  It should also set
     * scan->xs_recheck and possibly scan->xs_itup, though we pay no attention
     * to those fields here.
     */
    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        found = scan->indexRelation->rd_amroutine->amgettuple(scan, direction);
    } else {
        found = DatumGetBool(FunctionCall2(procedure, PointerGetDatum(scan), Int32GetDatum(direction)));
    }

    /* Reset kill flag immediately for safety */
    scan->kill_prior_tuple = false;

    /* If we're out of index entries, we're done */
    if (!found) {
        /* Release resources (like buffer pins) from table accesses */
        if (scan->xs_heapfetch)
            tableam_scan_index_fetch_reset(scan->xs_heapfetch);
        /* ... but first, release any held pin on a heap page */
        if (BufferIsValid(scan->xs_cbuf)) {
            ReleaseBuffer(scan->xs_cbuf);
            scan->xs_cbuf = InvalidBuffer;
        }
        return NULL;
    }
#ifdef USE_SPQ
    if (IS_SPQ_EXECUTOR && scan->spq_scan != NULL) {
        BlockNumber unitno = SPQSCAN_BlockNum2UnitNum(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self));
        if ((unitno % scan->spq_scan->slice_num) != scan->spq_scan->instance_id)
            goto rescan;
    }
#endif
    pgstat_count_index_tuples(scan->indexRelation, 1);

    /* Return the TID of the tuple we found. */
    return &scan->xs_ctup.t_self;
}

bool 
IndexFetchSlot(IndexScanDesc scan, TupleTableSlot *slot, bool isUHeap, bool* has_cur_xact_write)
{
    if (isUHeap) {
        return IndexFetchUHeap(scan, slot);
    } else {
        HeapTuple tuple = (HeapTuple)IndexFetchTuple(scan, has_cur_xact_write);
        return tuple != NULL;
    }
}

/* ----------------
 *		index_fetch_heap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, IndexFetchTuple or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
Tuple IndexFetchTuple(IndexScanDesc scan, bool* has_cur_xact_write)
{
    bool all_dead = false;
    Tuple fetchedTuple = NULL;


    fetchedTuple = tableam_scan_index_fetch_tuple(scan, &all_dead, has_cur_xact_write);

    if (fetchedTuple) {
        pgstat_count_heap_fetch(scan->indexRelation);
        return fetchedTuple;
    }
    /*
     * If we scanned a whole HOT chain and found only dead tuples, tell index
     * AM to kill its entry for that TID (this will take effect in the next
     * amgettuple call, in index_getnext_tid).	We do not do this when in
     * recovery because it may violate MVCC to do so.
     * See comments in RelationGetIndexScan().
     */
    if (!scan->xactStartedInRecovery)
        scan->kill_prior_tuple = all_dead;

    return NULL;
}

UHeapTuple UHeapamIndexFetchTuple(IndexScanDesc scan, bool *all_dead, bool* has_cur_xact_write)
{
    Relation rel = scan->heapRelation;
    Buffer xsCbuf = scan->xs_cbuf;
    ItemPointer tid = &scan->xs_ctup.t_self;

    /* Switch to correct buffer if we don't have it already */
    scan->xs_cbuf = ReleaseAndReadBuffer(xsCbuf, rel, ItemPointerGetBlockNumber(tid));

    /*
     * In single mode and hot standby, we may get a null buffer if index
     * replayed before the tid replayed. This is acceptable, so we return
     * null without reporting error.
     */
    if (RecoveryInProgress() && !BufferIsValid(scan->xs_cbuf)) {
        return NULL;
    }

    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);

    UHeapTuple uheapTuple = UHeapSearchBuffer(tid, rel, scan->xs_cbuf, scan->xs_snapshot, all_dead);

    /* Save the XID of the last modified tuple, which is used to determine whether the tuple of the USTORE table 
     * was modified by another transaction. For performance reasons, we reuse t_xid_base or t_multi_base 
     * to record the last modified XID of a tuple.
     */
    if (scan->isUpsert && uheapTuple) {
        if (UHeapTupleHasMultiLockers((uheapTuple)->disk_tuple->flag)) {
            uheapTuple->t_xid_base = UHeapTupleGetTransXid(uheapTuple, scan->xs_cbuf, false, has_cur_xact_write);
        } else {
            uheapTuple->t_multi_base = UHeapTupleGetTransXid(uheapTuple, scan->xs_cbuf, false, has_cur_xact_write);
        }
    }

    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);


    return uheapTuple;
}

bool UHeapamIndexFetchTupleInSlot(IndexScanDesc scan, ItemPointer tid, Snapshot snapshot,
TupleTableSlot *slot, bool *callAgain, bool *allDead, bool* has_cur_xact_write)
{
    Relation rel = scan->heapRelation;
    Buffer xsCbuf = scan->xs_cbuf;

    /* No HOT chains in UStore. */
    Assert(!*callAgain);

    /*
     * IndexScanDesc contains a memory that can hold up to MaxHeapTupleSize,
     * see RelationGetIndexScan() where the memory for IndexScanDesc is allocated.
     * That memory is used to uncompress Heap tuples but currently is not used in Ustore.
     * So in Ustore, we will start using that memory to hold the datapage tuple instead. 
     * This allows us to reduce memory allocation in UHeapSearchBuffer() -> UHeapGetTuple().
     */
    Size usableSize PG_USED_FOR_ASSERTS_ONLY = (Size) (SizeofHeapTupleHeader + MaxHeapTupleSize);
    UHeapTuple dataPageTuple = (UHeapTuple)&scan->xs_ctbuf_hdr;
    dataPageTuple->disk_tuple = (UHeapDiskTuple) ((char *) dataPageTuple + UHeapTupleDataSize);

    /* Switch to correct buffer if we don't have it already */
    scan->xs_cbuf = ReleaseAndReadBuffer(xsCbuf, rel, ItemPointerGetBlockNumber(tid));

    /*
     * In single mode and hot standby, we may get a invalid buffer if index
     * replayed before the tid replayed. This is acceptable, so we return
     * null without reporting error.
     */
    if (RecoveryInProgress() && !BufferIsValid(scan->xs_cbuf)) {
        return false;
    }

    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
    UHeapTuple visibleTuple = UHeapSearchBuffer(tid, rel, scan->xs_cbuf, snapshot, allDead, dataPageTuple,
                                                has_cur_xact_write);

    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

    if (visibleTuple) {
        Assert(visibleTuple->disk_tuple_size <= usableSize);
        ExecStoreTuple(visibleTuple, slot, InvalidBuffer, (visibleTuple != dataPageTuple));
    }

    return visibleTuple != NULL;
}


/* ----------------
 * IndexFetchUHeap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, IndexFetchTuple or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool IndexFetchUHeap(IndexScanDesc scan, TupleTableSlot *slot, bool* has_cur_xact_write)
{
    ItemPointer tid = &scan->xs_ctup.t_self;
    bool allDead = false;
    bool found = UHeapamIndexFetchTupleInSlot(scan, tid, scan->xs_snapshot, 
                                              slot, &scan->xs_continue_hot, &allDead, has_cur_xact_write);

    if (found) {
        pgstat_count_heap_fetch(scan->indexRelation);
        return true;
    }

    /*
     * If we scanned a whole HOT chain and found only dead tuples, tell index
     * AM to kill its entry for that TID (this will take effect in the next
     * amgettuple call, in index_getnext_tid).      We do not do this when in
     * recovery because it may violate MVCC to do so.  See comments in
     * RelationGetIndexScan().
     */
    if (!scan->xactStartedInRecovery)
        scan->kill_prior_tuple = allDead;

    return false;
}


/* ----------------
 *		index_getnext - get the next tuple from a scan
 *
 * The result is the next tuple satisfying the scan keys and the
 * snapshot, or NULL if no more matching tuples exist.
 *
 * On success, the buffer containing the tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, IndexFetchTuple or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 */
Tuple index_getnext(IndexScanDesc scan, ScanDirection direction, bool* has_cur_xact_write)
{
    Tuple tuple;
    ItemPointer tid;

    for (;;) {
        /* IO collector and IO scheduler */
#ifdef ENABLE_MULTIPLE_NODES
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
#endif
        if (likely(!scan->xs_continue_hot)) {
            /* Time to fetch the next TID from the index */
            tid = index_getnext_tid(scan, direction);
            /* If we're out of index entries, we're done */
            if (tid == NULL) {
                break;
            }
            if (IndexScanNeedSwitchPartRel(scan)) {
                /*
                 * Change the heapRelation in indexScanDesc to Partition Relation of current index
                 */
                if (!GPIGetNextPartRelation(scan->xs_gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                    continue;
                } 
                scan->heapRelation = scan->xs_gpi_scan->fakePartRelation;
            }
        } else {
            /*
             * We are resuming scan of a HOT chain after having returned an
             * earlier member.	Must still hold pin on current heap page.
             */
            Assert(BufferIsValid(scan->xs_cbuf));
            Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
        }

        /*
         * Fetch the next (or only) visible heap tuple for this index entry.
         * If we don't find anything, loop around and grab the next TID from
         * the index.
         */
        tuple = IndexFetchTuple(scan, has_cur_xact_write);
        if (tuple != NULL) {
            return tuple;
        }
    }
    return NULL; /* failure exit */
}

bool UHeapSysIndexGetnextSlot(SysScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
    Assert(scan->iscan);
    return IndexGetnextSlot(scan->iscan, direction, slot);
}

/* ---------------
 *              IndexGetnextSlot - get the next heap tuple from a scan
 *
 * The result is true if a tuple satisfying the scan keys and the snapshot was
 * found, false otherwise.  The tuple is stored in the specified slot.
 *
 * On success, resources (like buffer pins) are likely to be held, and will be
 * dropped by a future index_getnext_tid, IndexFetchTuple or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.

 * ----------------
 */
bool IndexGetnextSlot(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot, bool* has_cur_xact_write)
{
    ItemPointer tid;
    TupleTableSlot* tmpslot = NULL;
    tmpslot = MakeSingleTupleTableSlot(RelationGetDescr(scan->heapRelation),
        false, scan->heapRelation->rd_tam_ops);
    for (;;) {
        /* IO collector and IO scheduler */
#ifdef ENABLE_MULTIPLE_NODES
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
#endif

        if (likely(!scan->xs_continue_hot)) {
            /* Time to fetch the next TID from the index */
            tid = index_getnext_tid(scan, direction);

            /* If we're out of index entries, we're done */
            if (tid == NULL)
                break;

            if (IndexScanNeedSwitchPartRel(scan)) {
                /*
                 * Change the heapRelation in indexScanDesc to Partition Relation of current index
                 */
                if (!GPIGetNextPartRelation(scan->xs_gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                    continue;
                }
                scan->heapRelation = scan->xs_gpi_scan->fakePartRelation;
            }
        } else {
            /*
             * We are resuming scan of a HOT chain after having returned an
             * earlier member.      Must still hold pin on current heap page.
             */
            Assert(BufferIsValid(scan->xs_cbuf));
            Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
        }

        /*
         * Fetch the next (or only) visible heap tuple for this index entry.
         * If we don't find anything, loop around and grab the next TID from
         * the index.
         */

        if (IndexFetchUHeap(scan, slot, has_cur_xact_write)) {
            /* recheck IndexTuple when necessary */
            if (scan->xs_recheck_itup) {
                if (!IndexFetchUHeap(scan, tmpslot, has_cur_xact_write))
                    ereport(PANIC, (errmsg("Failed to refetch UHeapTuple. This shouldn't happen.")));
                if (!RecheckIndexTuple(scan, tmpslot))
                    continue;
            }
            ExecDropSingleTupleTableSlot(tmpslot);
            return true;
        }
    }
    ExecDropSingleTupleTableSlot(tmpslot);
    return false; /* failure exit */
}

/* ----------------
 * index_getbitmap - get all tuples at once from an index scan
 *
 * Adds the TIDs of all heap tuples satisfying the scan keys to a bitmap.
 * Since there's no interlock between the index scan and the eventual heap
 * access, this is only safe to use with MVCC-based snapshots: the heap
 * item slot could have been replaced by a newer tuple by the time we get
 * to it.
 *
 * Returns the number of matching tuples found.  (Note: this might be only
 * approximate, so it should only be used for statistical purposes.)
 * ----------------
 */
int64 index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap)
{
    FmgrInfo *procedure = NULL;
    int64 ntids;
    Datum d;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amgetbitmap);

    /* just make sure this is false... */
    scan->kill_prior_tuple = false;

    /*
     * have the am's getbitmap proc do all the work.
     */
    if (u_sess->attr.attr_common.enable_indexscan_optimization && scan->indexRelation->rd_rel->relam == BTREE_AM_OID) {
        d = scan->indexRelation->rd_amroutine->amgetbitmap(scan, bitmap);
    } else {
        d = FunctionCall2(procedure, PointerGetDatum(scan), PointerGetDatum(bitmap));
    }

    ntids = DatumGetInt64(d);

    /* If int8 is pass-by-ref, must free the result to avoid memory leak */
#ifndef USE_FLOAT8_BYVAL
    pfree(DatumGetPointer(d));
#endif

    pgstat_count_index_tuples(scan->indexRelation, ntids);

    return ntids;
}

int64 index_column_getbitmap(IndexScanDesc scan, const void *sort, VectorBatch *tids)
{
    FmgrInfo *procedure = NULL;
    int64 ntids;
    Datum d;

    SCAN_CHECKS;
    GET_SCAN_PROCEDURE(amgetbitmap);

    /* just make sure this is false... */
    scan->kill_prior_tuple = false;

    /*
     * have the am's getbitmap proc do all the work.
     */
    d = FunctionCall3(procedure, PointerGetDatum(scan), PointerGetDatum(sort), PointerGetDatum(tids));

    ntids = DatumGetInt64(d);

    /* If int8 is pass-by-ref, must free the result to avoid memory leak */
#ifndef USE_FLOAT8_BYVAL
    pfree(DatumGetPointer(d));
#endif

    pgstat_count_index_tuples(scan->indexRelation, ntids);

    return ntids;
}

/* ----------------
 *		index_bulk_delete - do mass deletion of index entries
 *
 *		callback routine tells whether a given main-heap tuple is
 *		to be deleted
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *index_bulk_delete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                         IndexBulkDeleteCallback callback, const void *callback_state)
{
    Relation index_relation = info->index;
    FmgrInfo *procedure = NULL;
    IndexBulkDeleteResult *result = NULL;

    RELATION_CHECKS;
    GET_REL_PROCEDURE(ambulkdelete);

    if (u_sess->attr.attr_common.enable_indexscan_optimization && index_relation->rd_rel->relam == BTREE_AM_OID) {
        result = index_relation->rd_amroutine->ambulkdelete(info, stats, callback, callback_state);
    } else {
        result = (IndexBulkDeleteResult *)DatumGetPointer(
            FunctionCall4(procedure, PointerGetDatum(info), PointerGetDatum(stats), PointerGetDatum((Pointer)callback),
                          PointerGetDatum(callback_state)));
    }

    return result;
}

/* ----------------
 *		index_vacuum_cleanup - do post-deletion cleanup of an index
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *index_vacuum_cleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    Relation index_relation = info->index;
    FmgrInfo *procedure = NULL;
    IndexBulkDeleteResult *result = NULL;

    RELATION_CHECKS;
    GET_REL_PROCEDURE(amvacuumcleanup);

    if (u_sess->attr.attr_common.enable_indexscan_optimization && index_relation->rd_rel->relam == BTREE_AM_OID) {
        result = index_relation->rd_amroutine->amvacuumcleanup(info, stats);
    } else {
        result = (IndexBulkDeleteResult *)DatumGetPointer(
            FunctionCall2(procedure, PointerGetDatum(info), PointerGetDatum(stats)));
    }

    return result;
}

/* ----------------
 *		index_can_return - does index support index-only scans?
 * ----------------
 */
bool index_can_return(Relation index_relation)
{
    FmgrInfo *procedure = NULL;
    bool result;

    RELATION_CHECKS;

    /* amcanreturn is optional; assume FALSE if not provided by AM */
    if (!RegProcedureIsValid(index_relation->rd_am->amcanreturn))
        return false;

    GET_REL_PROCEDURE(amcanreturn);

    if (u_sess->attr.attr_common.enable_indexscan_optimization && index_relation->rd_rel->relam == BTREE_AM_OID) {
        result = index_relation->rd_amroutine->amcanreturn();
    } else {
        result = DatumGetBool(FunctionCall1(procedure, PointerGetDatum(index_relation)));
    }    

    return result;
}

/* ----------------
 *		index_getprocid
 *
 *		Index access methods typically require support routines that are
 *		not directly the implementation of any WHERE-clause query operator
 *		and so cannot be kept in pg_amop.  Instead, such routines are kept
 *		in pg_amproc.  These registered procedure OIDs are assigned numbers
 *		according to a convention established by the access method.
 *		The general index code doesn't know anything about the routines
 *		involved; it just builds an ordered list of them for
 *		each attribute on which an index is defined.
 *
 *		As of Postgres 8.3, support routines within an operator family
 *		are further subdivided by the "left type" and "right type" of the
 *		query operator(s) that they support.  The "default" functions for a
 *		particular indexed attribute are those with both types equal to
 *		the index opclass' opcintype (note that this is subtly different
 *		from the indexed attribute's own type: it may be a binary-compatible
 *		type instead).	Only the default functions are stored in relcache
 *		entries --- access methods can use the syscache to look up non-default
 *		functions.
 *
 *		This routine returns the requested default procedure OID for a
 *		particular indexed attribute.
 * ----------------
 */
RegProcedure index_getprocid(Relation irel, AttrNumber attnum, uint16 procnum)
{
    RegProcedure *loc = NULL;
    int nproc;
    int procindex;

    nproc = irel->rd_am->amsupport;

    Assert(procnum > 0 && procnum <= (uint16)nproc);

    procindex = (nproc * (attnum - 1)) + (procnum - 1);

    loc = irel->rd_support;

    Assert(loc != NULL);

    return loc[procindex];
}

/* ----------------
 *		index_getprocinfo
 *
 *		This routine allows index AMs to keep fmgr lookup info for
 *		support procs in the relcache.	As above, only the "default"
 *		functions for any particular indexed attribute are cached.
 *
 * Note: the return value points into cached data that will be lost during
 * any relcache rebuild!  Therefore, either use the callinfo right away,
 * or save it only after having acquired some type of lock on the index rel.
 * ----------------
 */
FmgrInfo *index_getprocinfo(Relation irel, AttrNumber attnum, uint16 procnum)
{
    FmgrInfo *locinfo = NULL;
    int nproc;
    int procindex;

    nproc = irel->rd_am->amsupport;

    Assert(procnum > 0 && procnum <= (uint16)nproc);

    procindex = (nproc * (attnum - 1)) + (procnum - 1);

    locinfo = irel->rd_supportinfo;

    Assert(locinfo != NULL);

    locinfo += procindex;

    /* Initialize the lookup info if first time through */
    if (locinfo->fn_oid == InvalidOid) {
        RegProcedure *loc = irel->rd_support;
        RegProcedure procId;

        Assert(loc != NULL);

        procId = loc[procindex];

        /*
         * Complain if function was not found during index_support_initialize.
         * This should not happen unless the system tables contain bogus
         * entries for the index opclass.  (If an AM wants to allow a support
         * function to be optional, it can use index_getprocid.)
         */
        if (!RegProcedureIsValid(procId))
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
                            errmsg("missing support function %d for attribute %d of index \"%s\"", procnum, attnum,
                                   RelationGetRelationName(irel))));

        fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);
    }

    return locinfo;
}
