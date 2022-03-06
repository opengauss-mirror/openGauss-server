/* -------------------------------------------------------------------------
 *
 * snapbuild.cpp
 *
 *	Infrastructure for building historic catalog snapshots based on contents
 *	of the WAL, for the purpose of decoding heapam.c style values in the
 *	WAL.
 *
 * NOTES:
 *
 * We build snapshots which can *only* be used to read catalog contents and we
 * do so by reading and interpreting the WAL stream. The aim is to build a
 * snapshot that behaves the same as a freshly taken MVCC snapshot would have
 * at the time the XLogRecord was generated.
 *
 * To build the snapshots we reuse the infrastructure built for Hot
 * Standby. The in-memory snapshots we build look different than HS' because
 * we have different needs. To successfully decode data from the WAL we only
 * need to access catalog tables and (sys|rel|cat)cache, not the actual user
 * tables since the data we decode is wholly contained in the WAL
 * records. Also, our snapshots need to be different in comparison to normal
 * MVCC ones because in contrast to those we cannot fully rely on the clog and
 * pg_subtrans for information about committed transactions because they might
 * commit in the future from the POV of the WAL entry we're currently
 * decoding. This definition has the advantage that we only need to prevent
 * removal of catalog rows, while normal table's rows can still be
 * removed. This is achieved by using the replication slot mechanism.
 *
 * As the percentage of transactions modifying the catalog normally is fairly
 * small in comparisons to ones only manipulating user data, we keep track of
 * the committed catalog modifying ones inside (xmin, xmax) instead of keeping
 * track of all running transactions like its done in a normal snapshot. Note
 * that we're generally only looking at transactions that have acquired an
 * xid. That is we keep a list of transactions between snapshot->(xmin, xmax)
 * that we consider committed, everything else is considered aborted/in
 * progress. That also allows us not to care about subtransactions before they
 * have committed which means this modules, in contrast to HS, doesn't have to
 * care about suboverflowed subtransactions and similar.
 *
 * One complexity of doing this is that to e.g. handle mixed DDL/DML
 * transactions we need Snapshots that see intermediate versions of the
 * catalog in a transaction. During normal operation this is achieved by using
 * CommandIds/cmin/cmax. The problem with that however is that for space
 * efficiency reasons only one value of that is stored
 * (c.f. combocid.c). Since ComboCids are only available in memory we log
 * additional information which allows us to get the original (cmin, cmax)
 * pair during visibility checks. Check the reorderbuffer.c's comment above
 * ResolveCminCmaxDuringDecoding() for details.
 *
 * To facilitate all this we need our own visibility routine, as the normal
 * ones are optimized for different usecases.
 *
 * To replace the normal catalog snapshots with decoding ones use the
 * SetupHistoricSnapshot() and TeardownHistoricSnapshot() functions.
 *
 *
 *
 * The snapbuild machinery is starting up in in several stages, as illustrated
 * by the following graph describing the SnapBuild->state transitions:
 *
 *		   +-------------------------+
 *	  +----|		 START			 |-------------+
 *	  |	+-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |		   running_xacts #1					   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |	+-------------------------+			   v
 *	  |	|   BUILDING_SNAPSHOT	 |------------>|
 *	  |	+-------------------------+			   |
 *	  |					|						   |
 *	  |					|						   |
 *	  | running_xacts #2, xacts from #1 finished   |
 *	  |					|						   |
 *	  |					|						   |
 *	  |					v						   |
 *	  |	+-------------------------+			   v
 *	  |	|	   FULL_SNAPSHOT	 |------------>|
 *	  |	+-------------------------+			   |
 *	  |					|						   |
 * running_xacts		|					   saved snapshot
 * with zero xacts		|				  at running_xacts's lsn
 *	  |					|						   |
 *	  | running_xacts with xacts from #2 finished  |
 *	  |					|						   |
 *	  |					v						   |
 *	  |	+-------------------------+			   |
 *	  +--->|SNAPBUILD_CONSISTENT	 |<------------+
 *		   +-------------------------+
 *
 * Initially the machinery is in the START stage. When a xl_running_xacts
 * record is read that is sufficiently new (above the safe xmin horizon),
 * there's a state transation. If there were no running xacts when the
 * runnign_xacts record was generated, we'll directly go into CONSISTENT
 * state, otherwise we'll switch to the BUILDING_SNAPSHOT state. Having a full
 * snapshot means that all transactions that start henceforth can be decoded
 * in their entirety, but transactions that started previously can't. In
 * FULL_SNAPSHOT we'll switch into CONSISTENT once all those previously
 * running transactions have committed or aborted.
 *
 * Only transactions that commit after CONSISTENT state has been reached will
 * be replayed, even though they might have started while still in
 * FULL_SNAPSHOT. That ensures that we'll reach a point where no previous
 * changes has been exported, but all the following ones will be. That point
 * is a convenient point to initialize replication from, which is why we
 * export a snapshot at that point, which *can* be used to read normal data.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/snapbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "miscadmin.h"

#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"

#include "storage/buf/block.h" /* debugging output */
#include "storage/smgr/fd.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/standby.h"
#include "storage/copydir.h"

/*
 * This struct contains the current state of the snapshot building
 * machinery. Besides a forward declaration in the header, it is not exposed
 * to the public, so we can easily change it's contents.
 */
struct SnapBuild {
    /* how far are we along building our first full snapshot */
    SnapBuildState state;

    /* private memory context used to allocate memory for this module. */
    MemoryContext context;

    /* all transactions < than this have committed/aborted */
    TransactionId xmin;

    /* all transactions >= than this are uncommitted */
    TransactionId xmax;

    /*
     * Don't replay commits from an LSN < this LSN. This can be set
     * externally but it will also be advanced (never retreat) from within
     * snapbuild.c.
     */
    XLogRecPtr start_decoding_at;

    /*
     * Don't start decoding WAL until the "xl_running_xacts" information
     * indicates there are no running xids with a xid smaller than this.
     */
    TransactionId initial_xmin_horizon;

    /* Indicates if we are building full snapshot or just catalog one. */
    bool building_full_snapshot;

    /*
     * Snapshot that's valid to see the catalog state seen at this moment.
     */
    Snapshot snapshot;

    /*
     * LSN of the last location we are sure a snapshot has been serialized to.
     */
    XLogRecPtr last_serialized_snapshot;

    /*
     * The reorderbuffer we need to update with usable snapshots et al.
     */
    ReorderBuffer *reorder;

    /*
     * Outdated: This struct isn't used for its original purpose anymore, but
     * can't be removed / changed in a minor version, because it's stored
     * on-disk.
     */
    struct {
        /*
         * NB: This field is misused, until a major version can break on-disk
         * compatibility.
         * See the functions SnapBuildNextPhaseAt() / SnapBuildStartNextPhaseAt().
         */
        TransactionId was_xmax;

        size_t was_xcnt;        /* number of used xip entries */
        size_t was_xcnt_space;  /* allocated size of xip */
        TransactionId *was_xip; /* running xacts array, xidComparator-sorted */
    } was_running;

    /*
     * Array of transactions which could have catalog changes that committed
     * between xmin and xmax.
     */
    struct {
        /* number of committed transactions */
        size_t xcnt;

        /* available space for committed transactions */
        size_t xcnt_space;

        /*
         * Until we reach a CONSISTENT state, we record commits of all
         * transactions, not just the catalog changing ones. Record when that
         * changes so we know we cannot export a snapshot safely anymore.
         */
        bool includes_all_transactions;

        /*
         * Array of committed transactions that have modified the catalog.
         *
         * As this array is frequently modified we do *not* keep it in
         * xidComparator order. Instead we sort the array when building &
         * distributing a snapshot.
         *
         * description: It's unclear whether that reasoning has much merit. Every
         * time we add something here after becoming consistent will also
         * require distributing a snapshot. Storing them sorted would
         * potentially also make it easier to purge (but more complicated wrt
         * wraparound?). Should be improved if sorting while building the
         * snapshot shows up in profiles.
         */
        TransactionId *xip;
    } committed;
};

#ifndef MAX_SIZE_T_NUM
#define MAX_SIZE_T_NUM 4294967295
#endif

/* ->committed manipulation */
static void SnapBuildPurgeCommittedTxn(SnapBuild *builder);

/* snapshot building/manipulation/distribution functions */
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder, TransactionId xid);

static void SnapBuildFreeSnapshot(Snapshot snap);

static void SnapBuildSnapIncRefcount(Snapshot snap);

static void SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn);

/* xlog reading helper functions for SnapBuildProcessRecord */
static bool SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running);
static void SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff);

/* serialization functions */
static void SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn);
static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn);

/*
 * Return TransactionId after which the next phase of initial snapshot
 * building will happen.
 */
static inline TransactionId SnapBuildNextPhaseAt(SnapBuild *builder)
{
    /*
     * For backward compatibility reasons this has to be stored in the wrongly
     * named field.  Will be fixed in next major version.
     */
    return builder->was_running.was_xmax;
}

/*
 * Set TransactionId after which the next phase of initial snapshot building
 * will happen.
 */
static inline void SnapBuildStartNextPhaseAt(SnapBuild *builder, TransactionId at)
{
    /*
     * For backward compatibility reasons this has to be stored in the wrongly
     * named field.  Will be fixed in next major version.
     */
    builder->was_running.was_xmax = at;
}

/*
 * Allocate a new snapshot builder.
 *
 * xmin_horizon is the xid >=which we can be sure no catalog rows have been
 * removed, start_lsn is the LSN >= we want to replay commits.
 */
SnapBuild *AllocateSnapshotBuilder(ReorderBuffer *reorder, TransactionId xmin_horizon, XLogRecPtr start_lsn,
                                   bool need_full_snapshot)
{
    MemoryContext context;
    MemoryContext oldcontext;
    SnapBuild *builder = NULL;

    /* allocate memory in own context, to have better accountability */
    context = AllocSetContextCreate(CurrentMemoryContext, "snapshot builder context", ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldcontext = MemoryContextSwitchTo(context);

    builder = (SnapBuild *)palloc0(sizeof(SnapBuild));

    builder->state = SNAPBUILD_START;
    builder->context = context;
    builder->reorder = reorder;
    /* Other struct members initialized by zeroing via palloc0 above */
    builder->committed.xcnt = 0;
    builder->committed.xcnt_space = 128; /* arbitrary number */
    builder->committed.xip = (TransactionId *)palloc0(builder->committed.xcnt_space * sizeof(TransactionId));
    builder->committed.includes_all_transactions = true;
    builder->initial_xmin_horizon = xmin_horizon;
    builder->start_decoding_at = start_lsn;
    builder->building_full_snapshot = need_full_snapshot;

    (void)MemoryContextSwitchTo(oldcontext);

    return builder;
}

void FreeSnapshotBuilder(SnapBuild *builder)
{
    MemoryContext context = builder->context;

    /* free snapshot explicitly, that contains some error checking */
    if (builder->snapshot != NULL) {
        SnapBuildSnapDecRefcount(builder->snapshot);
        builder->snapshot = NULL;
    }

    /* other resources are deallocated via memory context reset */
    MemoryContextDelete(context);
}

/*
 * Free an unreferenced snapshot that has previously been built by us.
 */
static void SnapBuildFreeSnapshot(Snapshot snap)
{
    /* make sure we don't get passed an external snapshot */
    Assert(snap->satisfies == SNAPSHOT_HISTORIC_MVCC);

    /* make sure nobody modified our snapshot */
    Assert(snap->curcid == FirstCommandId);
    Assert(!snap->suboverflowed);
    Assert(!snap->takenDuringRecovery);
    Assert(snap->regd_count == 0);

    /* slightly more likely, so it's checked even without c-asserts */
    if (snap->copied)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot free a copied snapshot")));

    if (snap->active_count)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot free an active snapshot")));

    if (GTM_LITE_MODE && (snap->prepared_array != NULL))
        pfree_ext(snap->prepared_array);

    pfree(snap);
    snap = NULL;
}

/*
 * In which state of snapshot building are we?
 */
SnapBuildState SnapBuildCurrentState(SnapBuild *builder)
{
    return builder->state;
}

/*
 * Should the contents of transaction ending at 'ptr' be decoded?
 */
bool SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr)
{
    return XLByteLT(ptr, builder->start_decoding_at);
}

/*
 * Increase refcount of a snapshot.
 *
 * This is used when handing out a snapshot to some external resource or when
 * adding a Snapshot as builder->snapshot.
 */
static void SnapBuildSnapIncRefcount(Snapshot snap)
{
    snap->active_count++;
}

/*
 * Decrease refcount of a snapshot and free if the refcount reaches zero.
 *
 * Externally visible, so that external resources that have been handed an
 * IncRef'ed Snapshot can adjust its refcount easily.
 */
void SnapBuildSnapDecRefcount(Snapshot snap)
{
    /* make sure we don't get passed an external snapshot */
	Assert(snap->satisfies == SNAPSHOT_HISTORIC_MVCC);

    /* make sure nobody modified our snapshot */
    Assert(snap->curcid == FirstCommandId);
    Assert(!snap->suboverflowed);
    Assert(!snap->takenDuringRecovery);

    Assert(snap->regd_count == 0);

    Assert(snap->active_count > 0);

    /* slightly more likely, so its checked even without casserts */
    if (snap->copied)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot free a copied snapshot")));

    snap->active_count--;
    if (snap->active_count == 0)
        SnapBuildFreeSnapshot(snap);
}

/*
 * Build a new snapshot, based on currently committed catalog-modifying
 * transactions.
 *
 * In-progress transactions with catalog access are *not* allowed to modify
 * these snapshots; they have to copy them and fill in appropriate ->curcid
 * and ->subxip/subxcnt values.
 */
static Snapshot SnapBuildBuildSnapshot(SnapBuild *builder, TransactionId xid)
{
    Snapshot snapshot;
    Size ssize;
    int rc = 0;
    Assert(builder->state >= SNAPBUILD_FULL_SNAPSHOT);

    ssize = sizeof(SnapshotData) + sizeof(TransactionId) * builder->committed.xcnt +
            sizeof(TransactionId) * 1 /* toplevel xid */;

    snapshot = (Snapshot)MemoryContextAllocZero(builder->context, ssize);

    snapshot->satisfies = SNAPSHOT_HISTORIC_MVCC;

    /*
     * We misuse the original meaning of SnapshotData's xip and subxip fields
     * to make the more fitting for our needs.
     *
     * In the 'xip' array we store transactions that have to be treated as
     * committed. Since we will only ever look at tuples from transactions
     * that have modified the catalog its more efficient to store those few
     * that exist between xmin and xmax (frequently there are none).
     *
     * Snapshots that are used in transactions that have modified the catalog
     * also use the 'subxip' array to store their toplevel xid and all the
     * subtransaction xids so we can recognize when we need to treat rows as
     * visible that are not in xip but still need to be visible. Subxip only
     * gets filled when the transaction is copied into the context of a
     * catalog modifying transaction since we otherwise share a snapshot
     * between transactions. As long as a txn hasn't modified the catalog it
     * doesn't need to treat any uncommitted rows as visible, so there is no
     * need for those xids.
     *
     * Both arrays are qsort'ed so that we can use bsearch() on them.
     */
    Assert(TransactionIdIsNormal(builder->xmin));
    Assert(TransactionIdIsNormal(builder->xmax));

    snapshot->xmin = builder->xmin;
    snapshot->xmax = builder->xmax;

    /* store all transactions to be treated as committed by this snapshot */
    snapshot->xip = (TransactionId *)((char *)snapshot + sizeof(SnapshotData));
    snapshot->xcnt = (uint32)(builder->committed.xcnt);
    if (builder->committed.xcnt != 0) {
        rc = memcpy_s(snapshot->xip, builder->committed.xcnt * sizeof(TransactionId), builder->committed.xip,
                      builder->committed.xcnt * sizeof(TransactionId));
        securec_check(rc, "", "");
    }
    /* sort so we can bsearch() */
    qsort(snapshot->xip, snapshot->xcnt, sizeof(TransactionId), xidComparator);

    /*
     * Initially, subxip is empty, i.e. it's a snapshot to be used by
     * transactions that don't modify the catalog. Will be filled by
     * ReorderBufferCopySnap() if necessary.
     */
    snapshot->subxcnt = 0;
    snapshot->subxip = NULL;

    snapshot->suboverflowed = false;
    snapshot->takenDuringRecovery = false;
    snapshot->copied = false;
    snapshot->curcid = FirstCommandId;
    snapshot->active_count = 0;
    snapshot->regd_count = 0; /* mark as registered so nobody frees it */

    return snapshot;
}

/*
 * Export a snapshot so it can be set in another session with SET TRANSACTION
 * SNAPSHOT.
 *
 * For that we need to start a transaction in the current backend as the
 * importing side checks whether the source transaction is still open to make
 * sure the xmin horizon hasn't advanced since then.
 *
 * After that we convert a locally built snapshot into the normal variant
 * understood by HeapTupleSatisfiesMVCC et al.
 */
const char *SnapBuildExportSnapshot(SnapBuild *builder)
{
    Snapshot snap = NULL;
    char *snapname = NULL;
    TransactionId *newxip = NULL;
    const int newxcnt = 0;
    int maxcnt = GetMaxSnapshotXidCount();

    if (builder->state != SNAPBUILD_CONSISTENT)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot export a snapshot before reaching a consistent state")));

    if (!builder->committed.includes_all_transactions)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot export a snapshot, not all transactions are monitored anymore")));

    /* so we don't overwrite the existing value */
    if (TransactionIdIsValid(t_thrd.pgxact->xmin))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot export a snapshot when MyPgXact->xmin already is valid")));
    if (IsTransactionOrTransactionBlock())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot export a snapshot from within a transaction")));

    if (t_thrd.logical_cxt.SavedResourceOwnerDuringExport)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can only export one snapshot at a time")));

    t_thrd.logical_cxt.SavedResourceOwnerDuringExport = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.logical_cxt.ExportInProgress = true;

    StartTransactionCommand();

    Assert(!u_sess->utils_cxt.FirstSnapshotSet);

    /* There doesn't seem to a nice API to set these */
    u_sess->utils_cxt.XactIsoLevel = XACT_REPEATABLE_READ;
    u_sess->attr.attr_common.XactReadOnly = true;

    snap = SnapBuildBuildSnapshot(builder, GetTopTransactionId());

    /*
     * We know that snap->xmin is alive, enforced by the logical xmin
     * mechanism. Due to that we can do this without locks, we're only
     * changing our own value.
     */
    t_thrd.pgxact->xmin = snap->xmin;

    if (snap->xcnt > (uint32)maxcnt)
        maxcnt = snap->xcnt;

    /* allocate in transaction context */
    newxip = (TransactionId *)palloc(sizeof(TransactionId) * maxcnt);

    /*
     * snapbuild.c builds transactions in an "inverted" manner, which means it
     * stores committed transactions in ->xip, not ones in progress. Build a
     * classical snapshot by marking all non-committed transactions as
     * in-progress. This can be expensive.
     */
    snap->xcnt = newxcnt;
    snap->xip = newxip;

    /*
     * now that we've built a plain snapshot, use the normal mechanisms for
     * exporting it
     */
    snapname = ExportSnapshot(snap, NULL);
    if (!RecoveryInProgress())
        ereport(LOG, (errmsg("exported logical decoding snapshot: \"%s\" with %u xids", snapname, snap->xcnt)));
    return snapname;
}

/*
 * Reset a previously SnapBuildExportSnapshot()'ed snapshot if there is
 * any. Aborts the previously started transaction and resets the resource
 * owner back to it's original value.
 */
void SnapBuildClearExportedSnapshot()
{
    /* nothing exported, thats the usual case */
    if (!t_thrd.logical_cxt.ExportInProgress)
        return;

    if (!IsTransactionState())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("clearing exported snapshot in wrong transaction state")));

    /* make sure nothing  could have ever happened */
    AbortCurrentTransaction();

    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.logical_cxt.SavedResourceOwnerDuringExport;
    t_thrd.logical_cxt.SavedResourceOwnerDuringExport = NULL;
    t_thrd.logical_cxt.ExportInProgress = false;
}

/*
 * Handle the effects of a single heap change, appropriate to the current state
 * of the snapshot builder and returns whether changes made at (xid, lsn) can
 * be decoded.
 */
bool SnapBuildProcessChange(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn)
{
    /*
     * We can't handle data in transactions if we haven't built a snapshot
     * yet, so don't store them.
     */
    if (builder->state < SNAPBUILD_FULL_SNAPSHOT)
        return false;

    /*
     * No point in keeping track of changes in transactions that we don't have
     * enough information about to decode. This means that they started before
     * we got into the SNAPBUILD_FULL_SNAPSHOT state.
     */
    if (builder->state < SNAPBUILD_CONSISTENT && TransactionIdPrecedes(xid, SnapBuildNextPhaseAt(builder)))
        return false;

    /*
     * If the reorderbuffer doesn't yet have a snapshot, add one now, it will
     * be needed to decode the change we're currently processing.
     */
    if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid)) {
        /* only build a new snapshot if we don't have a prebuilt one */
        if (builder->snapshot == NULL) {
            builder->snapshot = SnapBuildBuildSnapshot(builder, xid);
            /* inrease refcount for the snapshot builder */
            SnapBuildSnapIncRefcount(builder->snapshot);
        }

        /*
         * Increase refcount for the transaction we're handing the snapshot
         * out to.
         */
        SnapBuildSnapIncRefcount(builder->snapshot);
        ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn, builder->snapshot);
    }

    return true;
}

/*
 * Do CommandId/ComboCid handling after reading a xl_heap_new_cid record. This
 * implies that a transaction has done some form of write to system catalogs.
 */
void SnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid, XLogRecPtr lsn, xl_heap_new_cid *xlrec,
                            int bucket_id)
{
    CommandId cid;

    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, xlrec->target_node, bucket_id);

    /*
     * we only log new_cid's if a catalog tuple was modified, so mark
     * the transaction as containing catalog modifications
     */
    ReorderBufferXidSetCatalogChanges(builder->reorder, xid, lsn);

    ReorderBufferAddNewTupleCids(builder->reorder, xlrec->top_xid, lsn, tmp_node, xlrec->target_tid, xlrec->cmin,
                                 xlrec->cmax, xlrec->combocid);

    /* figure out new command id */
    if (xlrec->cmin != InvalidCommandId && xlrec->cmax != InvalidCommandId)
        cid = Max(xlrec->cmin, xlrec->cmax);
    else if (xlrec->cmax != InvalidCommandId)
        cid = xlrec->cmax;
    else if (xlrec->cmin != InvalidCommandId)
        cid = xlrec->cmin;
    else {
        cid = InvalidCommandId; /* silence compiler */
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("xl_heap_new_cid record without a valid CommandId")));
    }

    ReorderBufferAddNewCommandId(builder->reorder, xid, lsn, cid + 1);
}

/*
 * Add a new Snapshot to all transactions we're decoding that currently are
 * in-progress so they can see new catalog contents made by the transaction
 * that just committed. This is necessary because those in-progress
 * transactions will use the new catalog's contents from here on (at the very
 * least everything they do needs to be compatible with newer catalog
 * contents).
 */
static void SnapBuildDistributeNewCatalogSnapshot(SnapBuild *builder, XLogRecPtr lsn)
{
    dlist_iter txn_i;
    ReorderBufferTXN *txn = NULL;

    /*
     * Iterate through all toplevel transactions. This can include
     * subtransactions which we just don't yet know to be that, but that's
     * fine, they will just get an unneccesary snapshot queued.
     */
    dlist_foreach(txn_i, &builder->reorder->toplevel_by_lsn)
    {
        txn = dlist_container(ReorderBufferTXN, node, txn_i.cur);

        Assert(TransactionIdIsValid(txn->xid));

        /*
         * If we don't have a base snapshot yet, there are no changes in this
         * transaction which in turn implies we don't yet need a snapshot at
         * all. We'll add add a snapshot when the first change gets queued.
         *
         * NB: This works correctly even for subtransactions because
         * ReorderBufferAssignChild() takes care to transfer the base snapshot
         * to the top-level transaction, and while iterating the changequeue
         * we'll get the change from the subtxn.
         */
        if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, txn->xid))
            continue;

        if (!RecoveryInProgress())
            ereport(DEBUG2,
                    (errmsg("adding a new snapshot to %lu at %X/%X", txn->xid, (uint32)(lsn >> 32), (uint32)lsn)));

        /*
         * increase the snapshot's refcount for the transaction we are handing
         * it out to
         */
        SnapBuildSnapIncRefcount(builder->snapshot);
        ReorderBufferAddSnapshot(builder->reorder, txn->xid, lsn, builder->snapshot);
    }
}

/*
 * Keep track of a new catalog changing transaction that has committed.
 */
static void SnapBuildAddCommittedTxn(SnapBuild *builder, TransactionId xid)
{
    Assert(TransactionIdIsValid(xid));

    if (builder->committed.xcnt == builder->committed.xcnt_space) {
        builder->committed.xcnt_space = builder->committed.xcnt_space * 2 + 1;

        if (!RecoveryInProgress())
            ereport(DEBUG1, (errmsg("increasing space for committed transactions to %u",
                                    (uint32)builder->committed.xcnt_space)));

        builder->committed.xip = (TransactionId *)repalloc(builder->committed.xip,
                                                           builder->committed.xcnt_space * sizeof(TransactionId));
    }

    /*
     * description: It might make sense to keep the array sorted here instead of
     * doing it every time we build a new snapshot. On the other hand this
     * gets called repeatedly when a transaction with subtransactions commits.
     */
    builder->committed.xip[builder->committed.xcnt++] = xid;
}

/*
 * Remove knowledge about transactions we treat as committed that are smaller
 * than ->xmin. Those won't ever get checked via the ->commited array but via
 * the clog machinery, so we don't need to waste memory on them.
 */
static void SnapBuildPurgeCommittedTxn(SnapBuild *builder)
{
    size_t off = 0;
    TransactionId *workspace = NULL;
    uint32 surviving_xids = 0;
    int rc = 0;
    /* not ready yet */
    if (!TransactionIdIsNormal(builder->xmin))
        return;

    /* description: Neater algorithm than just copying and iterating? */
    workspace = (TransactionId *)MemoryContextAlloc(builder->context, builder->committed.xcnt * sizeof(TransactionId));

    /* copy xids that still are interesting to workspace */
    for (off = 0; off < builder->committed.xcnt; off++) {
        if (NormalTransactionIdPrecedes(builder->committed.xip[off], builder->xmin))
            ; /* remove */
        else
            workspace[surviving_xids++] = builder->committed.xip[off];
    }
    if (surviving_xids) {
        /* copy workspace back to persistent state */
        rc = memcpy_s(builder->committed.xip, surviving_xids * sizeof(TransactionId), workspace,
                      surviving_xids * sizeof(TransactionId));
        securec_check(rc, "", "");
    }
    if (!RecoveryInProgress())
        ereport(DEBUG3, (errmsg("purged committed transactions from %lu to %u, xmin: %lu, xmax: %lu",
                                builder->committed.xcnt, surviving_xids, builder->xmin, builder->xmax)));
    builder->committed.xcnt = surviving_xids;

    pfree(workspace);
    workspace = NULL;
}

/*
 * SnapBuild handle commtted transaction.
 */
static void SnapBuildChange(bool forced_timetravel, bool *top_needs_timetravel, bool sub_needs_timetravel,
    SnapBuild *builder, TransactionId xid)
{
    if (forced_timetravel) {
        if (!RecoveryInProgress()) {
            ereport(DEBUG1, (errmsg("forced transaction %lu to do timetravel.", xid)));
        }
        SnapBuildAddCommittedTxn(builder, xid);
    } else if (ReorderBufferXidHasCatalogChanges(builder->reorder, xid)) {
        /* add toplevel transaction to base snapshot */
        if (!RecoveryInProgress()) {
            ereport(DEBUG2, (errmsg("found top level transaction %lu, with catalog changes!", xid)));
        }
        *top_needs_timetravel = true;
        SnapBuildAddCommittedTxn(builder, xid);
    } else if (sub_needs_timetravel) {
        /* mark toplevel txn as timetravel as well */
        SnapBuildAddCommittedTxn(builder, xid);
    }
}

/*
 * Handle everything that needs to be done when a transaction commits
 */
void SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn, TransactionId xid, int nsubxacts, TransactionId *subxacts)
{
    int nxact;

    bool forced_timetravel = false;
    bool sub_needs_timetravel = false;
    bool top_needs_timetravel = false;

    TransactionId xmax = xid;

    /*
     * If we couldn't observe every change of a transaction because it was
     * already running at the point we started to observe we have to assume it
     * made catalog changes.
     *
     * This has the positive benefit that we afterwards have enough
     * information to build an exportable snapshot that's usable by pg_dump et
     * al.
     */
    if (builder->state < SNAPBUILD_CONSISTENT) {
        /* ensure that only commits after this are getting replayed */
        if (XLByteLE(builder->start_decoding_at, lsn)) {
            XLByteAdvance(lsn, 1);
            builder->start_decoding_at = lsn;
        }

        /*
         * We could avoid treating !SnapBuildTxnIsRunning transactions as
         * timetravel ones, but we want to be able to export a snapshot when
         * we reached consistency.
         */
        forced_timetravel = true;
        if (!RecoveryInProgress())
            ereport(DEBUG1,
                    (errmsg("forced to assume catalog changes for xid %lu because it was running to early", xid)));
    }

    for (nxact = 0; nxact < nsubxacts; nxact++) {
        TransactionId subxid = subxacts[nxact];

        // If we're forcing time-travel we also need visibility information
        // about sub-transaction, so keep track of sub-transaction's state.
        if (forced_timetravel) {
            SnapBuildAddCommittedTxn(builder, subxid);
            if (NormalTransactionIdFollows(subxid, xmax)) {
                xmax = subxid;
            }
        } else if (ReorderBufferXidHasCatalogChanges(builder->reorder, subxid)) {
            // Add sub-transaction to base snapshot if it DDL, we don't distinguish
            // to top-level transactions there.
            sub_needs_timetravel = true;
            if (!RecoveryInProgress()) {
                ereport(DEBUG2, (errmsg("found subtransaction %lu:%lu with catalog changes.", xid, subxid)));
            }
            SnapBuildAddCommittedTxn(builder, subxid);
            if (NormalTransactionIdFollows(subxid, xmax)) {
                xmax = subxid;
            }
        }
    }

    SnapBuildChange(forced_timetravel, &top_needs_timetravel, sub_needs_timetravel, builder, xid);

    /* if there's any reason to build a historic snapshot, to so now */
    if (forced_timetravel || top_needs_timetravel || sub_needs_timetravel) {
        /*
         * Adjust xmax of the snapshot builder, we only do that for committed,
         * catalog modifying, transactions, everything else isn't interesting
         * for us since we'll never look at the respective rows.
         */
        if (!TransactionIdIsValid(builder->xmax) || TransactionIdFollowsOrEquals(xmax, builder->xmax)) {
            builder->xmax = xmax;
            TransactionIdAdvance(builder->xmax);
        }

        /*
         * If we haven't built a complete snapshot yet there's no need to hand
         * it out, it wouldn't (and couldn't) be used anyway.
         */
        if (builder->state < SNAPBUILD_FULL_SNAPSHOT) {
            return;
        }

        /*
         * Decrease the snapshot builder's refcount of the old snapshot, note
         * that it still will be used if it has been handed out to the
         * reorderbuffer earlier.
         */
        if (builder->snapshot != NULL) {
            SnapBuildSnapDecRefcount(builder->snapshot);
        }
        builder->snapshot = SnapBuildBuildSnapshot(builder, xid);

        /* we might need to execute invalidations, add snapshot */
        if (!ReorderBufferXidHasBaseSnapshot(builder->reorder, xid)) {
            SnapBuildSnapIncRefcount(builder->snapshot);
            ReorderBufferSetBaseSnapshot(builder->reorder, xid, lsn, builder->snapshot);
        }

        /* refcount of the snapshot builder for the new snapshot */
        SnapBuildSnapIncRefcount(builder->snapshot);

        /* add a new SnapshotNow to all currently running transactions */
        SnapBuildDistributeNewCatalogSnapshot(builder, lsn);
    } else {
        /* record that we cannot export a general snapshot anymore */
        builder->committed.includes_all_transactions = false;
    }
}

/* -----------------------------------
 * Snapshot building functions dealing with xlog records
 * -----------------------------------
 *
 * Process a running xacts record, and use it's information to first build a
 * historic snapshot and later to release resources that aren't needed
 * anymore.
 */
void SnapBuildProcessRunningXacts(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
    ReorderBufferTXN *txn = NULL;
    TransactionId xmin;

    /*
     * If we're not consistent yet, inspect the record to see whether it
     * allows to get closer to being consistent. If we are consistent, dump
     * our snapshot so others or we, after a restart, can use it.
     */
    if (builder->state < SNAPBUILD_CONSISTENT) {
        /* returns false if there's no point in performing cleanup just yet */
        if (!SnapBuildFindSnapshot(builder, lsn, running)) {
            return;
        }
    } else {
        SnapBuildSerialize(builder, lsn);
    }

    /*
     * Update range of interesting xids base don the running xacts
     * information. We don't increase ->xmax using it, because once we are in
     * a consistent state we can do that ourselves and much more efficiently
     * so, because we only need to do it for catalog transactions since we
     * only ever look at those.
     *
     * NB: Because of that xmax can be lower than xmin, because we only
     * increase xmax when a catalog modifying transaction commits. While odd
     * looking, its correct and actually more efficient this way since we hit
     * fast paths in heapam_visibility.c.
     */
    builder->xmin = running->oldestRunningXid;

    /* Remove transactions we don't need to keep track off anymore */
    SnapBuildPurgeCommittedTxn(builder);

    /*
     * Advance the xmin limit for the current replication slot, to allow
     * vacuum to clean up the tuples this slot has been protecting.
     *
     * The reorderbuffer might have an xmin among the currently running
     * snapshots; use it if so.  If not, we need only consider the snapshots
     * we'll produce later, which can't be less than the oldest running xid in
     * the record we're reading now.
     */
    xmin = ReorderBufferGetOldestXmin(builder->reorder);
    if (xmin == InvalidTransactionId) {
        xmin = running->oldestRunningXid;
    }
    ereport(DEBUG3, (errmsg("xmin: %lu, xmax: %lu, oldest running: %lu, oldest xmin: %lu", builder->xmin, builder->xmax,
                            running->oldestRunningXid, xmin)));
    LogicalIncreaseXminForSlot(lsn, xmin);

    /*
     * Also tell the slot where we can restart decoding from. We don't want to
     * do that after every commit because changing that implies an fsync of
     * the logical slot's state file, so we only do it every time we see a
     * running xacts record.
     *
     * Do so by looking for the oldest in progress transaction (determined by
     * the first LSN of any of its relevant records). Every transaction
     * remembers the last location we stored the snapshot to disk before its
     * beginning. That point is where we can restart from.
     *
     *
     * Can't know about a serialized snapshot's location if we're not
     * consistent.
     */
    if (builder->state < SNAPBUILD_CONSISTENT) {
        return;
    }

    txn = ReorderBufferGetOldestTXN(builder->reorder);
    /*
     * oldest ongoing txn might have started when we didn't yet serialize
     * anything because we hadn't reached a consistent state yet.
     */
    if (txn != NULL && !XLByteEQ(txn->restart_decoding_lsn, InvalidXLogRecPtr)) {
        LogicalIncreaseRestartDecodingForSlot(lsn, txn->restart_decoding_lsn);
    } else if (txn == NULL && !XLByteEQ(builder->reorder->current_restart_decoding_lsn, InvalidXLogRecPtr) &&
               !XLByteEQ(builder->last_serialized_snapshot, InvalidXLogRecPtr)) {
        // No in-progress transaction, can reuse the last serialized snapshot if we have one.
        LogicalIncreaseRestartDecodingForSlot(lsn, builder->last_serialized_snapshot);
    }
}

/*
 * Build the start of a snapshot that's capable of decoding the catalog.
 *
 * Helper function for SnapBuildProcessRunningXacts() while we're not yet
 * consistent.
 *
 * Returns true if there is a point in performing internal maintenance/cleanup
 * using the xl_running_xacts record.
 */
static bool SnapBuildFindSnapshot(SnapBuild *builder, XLogRecPtr lsn, xl_running_xacts *running)
{
    /* ---
     * Build catalog decoding snapshot incrementally using information about
     * the currently running transactions. There are several ways to do that:
     *
     * a) There were no running transactions when the xl_running_xacts record
     *	  was inserted, jump to CONSISTENT immediately. We might find such a
     *	  state while waiting on c)'s sub-states.
     *
     * b) This (in a previous run) or another decoding slot serialized a
     *	  snapshot to disk that we can use.  Can't use this method for the
     *	  initial snapshot when slot is being created and needs full snapshot
     *	  for export or direct use, as that snapshot will only contain catalog
     *	  modifying transactions.
     *
     * c) First incrementally build a snapshot for catalog tuples
     *	  (BUILDING_SNAPSHOT), that requires all, already in-progress,
     *	  transactions to finish.  Every transaction starting after that
     *	  (FULL_SNAPSHOT state), has enough information to be decoded.  But
     *	  for older running transactions no viable snapshot exists yet, so
     *	  CONSISTENT will only be reached once all of those have finished.
     * ---
     *
     *
     * xl_running_xact record is older than what we can use, we might not have
     * all necessary catalog rows anymore.
     */
    if (TransactionIdIsNormal(builder->initial_xmin_horizon) &&
        NormalTransactionIdPrecedes(running->oldestRunningXid, builder->initial_xmin_horizon)) {
        if (!RecoveryInProgress()) {
            ereport(DEBUG1,
                    (errmsg("skipping snapshot at %X/%X while building logical decoding snapshot, xmin horizon too low",
                            (uint32)(lsn >> 32), (uint32)lsn),
                     errdetail("initial xmin horizon of %lu vs the snapshot's %lu", builder->initial_xmin_horizon,
                               running->oldestRunningXid)));
        }
        SnapBuildWaitSnapshot(running, builder->initial_xmin_horizon);
        return true;
    }

    /*
     * a) No transaction were running or persistency is RS_PERSISTENT,
     * This is not affected by races around xl_running_xacts, because we can
     * miss transaction commits, but currently not transactions starting.
     *
     * NB: We might have already started to incrementally assemble a snapshot,
     * so we need to be careful to deal with that.
     * Persistency is RS_PERSISTENT means that we create logical slot success.
     * So wen can set snapbuild state to SNAPBUILD_CONSISTENT.
     */
    if (running->oldestRunningXid == running->nextXid ||
        GET_SLOT_PERSISTENCY(t_thrd.slot_cxt.MyReplicationSlot->data) == RS_PERSISTENT) {
        if (XLByteEQ(builder->start_decoding_at, InvalidXLogRecPtr) || XLByteLE(builder->start_decoding_at, lsn)) {
            /* can decode everything after this */
            XLByteAdvance(lsn, 1);
            builder->start_decoding_at = lsn;
        }

        /* As no transactions were running xmin/xmax can be trivially set. */
        builder->xmin = running->nextXid; /* < are finished */
        builder->xmax = running->nextXid; /* >= are running */

        /* so we can safely use the faster comparisons */
        Assert(TransactionIdIsNormal(builder->xmin));
        Assert(TransactionIdIsNormal(builder->xmax));

        builder->state = SNAPBUILD_CONSISTENT;
        SnapBuildStartNextPhaseAt(builder, InvalidTransactionId);
        if (!RecoveryInProgress()) {
            ereport(LOG, (errmsg("logical decoding found consistent point at %X/%X", (uint32)(lsn >> 32), (uint32)lsn),
                          errdetail("running xacts with xcnt == 0")));
        }

        return false;
    } else if (!builder->building_full_snapshot && SnapBuildRestore(builder, lsn)) {
        /* b) valid on disk state and not building full snapshot
         *
         * there won't be any state to cleanup
         */
        return false;
    } else if (builder->state == SNAPBUILD_START) {
        /*
         * c) transition from START to BUILDING_SNAPSHOT.
         *
         * In START state, and a xl_running_xacts record with running xacts is
         * encountered.  In that case, switch to BUILDING_SNAPSHOT state, and
         * record xl_running_xacts->nextXid.  Once all running xacts have finished
         * (i.e. they're all >= nextXid), we have a complete catalog snapshot.  It
         * might look that we could use xl_running_xact's ->xids information to
         * get there quicker, but that is problematic because transactions marked
         * as running, might already have inserted their commit record - it's
         * infeasible to change that with locking.
         */
        builder->state = SNAPBUILD_BUILDING_SNAPSHOT;
        SnapBuildStartNextPhaseAt(builder, running->nextXid);

        /*
         * Start with an xmin/xmax that's correct for future, when all the
         * currently running transactions have finished. We'll update both
         * while waiting for the pending transactions to finish.
         */
        builder->xmin = running->nextXid; /* < are finished */
        builder->xmax = running->nextXid; /* >= are running */

        /* so we can safely use the faster comparisons */
        Assert(TransactionIdIsNormal(builder->xmin));
        Assert(TransactionIdIsNormal(builder->xmax));

        if (!RecoveryInProgress()) {
            ereport(LOG,
                    (errmsg("logical decoding found initial starting point at %X/%X", (uint32)(lsn >> 32), (uint32)lsn),
                     errdetail("Waiting for transactions (approximately %d) older than %lu to end.", running->xcnt,
                               running->nextXid)));
        }
        SnapBuildWaitSnapshot(running, running->nextXid);
    } else if (builder->state == SNAPBUILD_BUILDING_SNAPSHOT &&
               TransactionIdPrecedesOrEquals(SnapBuildNextPhaseAt(builder), running->oldestRunningXid)) {
        /*
         * c) transition from BUILDING_SNAPSHOT to FULL_SNAPSHOT.
         *
         * In BUILDING_SNAPSHOT state, and this xl_running_xacts' oldestRunningXid
         * is >= than nextXid from when we switched to BUILDING_SNAPSHOT.  This
         * means all transactions starting afterwards have enough information to
         * be decoded.  Switch to FULL_SNAPSHOT.
         */
        builder->state = SNAPBUILD_FULL_SNAPSHOT;
        SnapBuildStartNextPhaseAt(builder, running->nextXid);
        ereport(LOG,
                (errmsg("logical decoding found initial consistent point at %X/%X", (uint32)(lsn >> 32), (uint32)lsn),
                 errdetail("Waiting for transactions (approximately %d) older than %lu to end.", running->xcnt,
                           running->nextXid)));

        SnapBuildWaitSnapshot(running, running->nextXid);
    } else if (builder->state == SNAPBUILD_FULL_SNAPSHOT &&
               TransactionIdPrecedesOrEquals(SnapBuildNextPhaseAt(builder), running->oldestRunningXid)) {
        /*
         * c) transition from FULL_SNAPSHOT to CONSISTENT.
         *
         * In FULL_SNAPSHOT state (see d) ), and this xl_running_xacts'
         * oldestRunningXid is >= than nextXid from when we switched to
         * FULL_SNAPSHOT.  This means all transactions that are currently in
         * progress have a catalog snapshot, and all their changes have been
         * collected.  Switch to CONSISTENT.
         */
        builder->state = SNAPBUILD_CONSISTENT;
        SnapBuildStartNextPhaseAt(builder, InvalidTransactionId);

        ereport(LOG, (errmsg("logical decoding found consistent point at %X/%X", (uint32)(lsn >> 32), (uint32)lsn),
                      errdetail("There are no old transactions anymore.")));
    }

    /*
     * We already started to track running xacts and need to wait for all
     * in-progress ones to finish. We fall through to the normal processing of
     * records so incremental cleanup can be performed.
     */
    return true;
}

/* ---
 * Iterate through xids in record, wait for all older than the cutoff to
 * finish.  Then, if possible, log a new xl_running_xacts record.
 *
 * This isn't required for the correctness of decoding, but to:
 * a) allow isolationtester to notice that we're currently waiting for
 *	something.
 * b) log a new xl_running_xacts record where it'd be helpful, without having
 *	to write for bgwriter or checkpointer.
 * ---
 */
static void SnapBuildWaitSnapshot(xl_running_xacts *running, TransactionId cutoff)
{
    for (int off = 0; off < running->xcnt; off++) {
        TransactionId xid = running->xids[off];

        /*
         * Upper layers should prevent that we ever need to wait on
         * ourselves. Check anyway, since failing to do so would either
         * result in an endless wait or an Assert() failure.
         */
        if (TransactionIdIsCurrentTransactionId(xid)) {
            ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("waiting for ourselves")));
        }

        if (TransactionIdFollows(xid, cutoff)) {
            continue;
        }

        XactLockTableWait(xid);
    }

    /*
     * All transactions we needed to finish finished - try to ensure there is
     * another xl_running_xacts record in a timely manner, without having to
     * write for bgwriter or checkpointer to log one.  During recovery we
     * can't enforce that, so we'll have to wait.
     */
    if (!RecoveryInProgress()) {
        (void)LogStandbySnapshot();
    }
}

/* -----------------------------------
 * Snapshot serialization support
 * -----------------------------------
 *
 *
 * We store current state of struct SnapBuild on disk in the following manner:
 *
 * struct SnapBuildOnDisk;
 * TransactionId * running.xcnt_space;
 * TransactionId * committed.xcnt; (*not xcnt_space*)
 *
 */
typedef struct SnapBuildOnDisk {
    /* first part of this struct needs to be version independent
     * data not covered by checksum
     */
    uint32 magic;
    pg_crc32 checksum;

    /* data covered by checksum
     * version, in case we want to support pg_upgrade
     */
    uint32 version;
    /* how large is the on disk data, excluding the constant sized part */
    uint32 length;

    /* version dependent part */
    SnapBuild builder;

    /* variable amount of TransactionIds follows */
} SnapBuildOnDisk;

#define SnapBuildOnDiskConstantSize offsetof(SnapBuildOnDisk, builder)
#define SnapBuildOnDiskNotChecksummedSize offsetof(SnapBuildOnDisk, version)

#define SNAPBUILD_MAGIC 0x51A1E001
#define SNAPBUILD_VERSION 1

/*
 * Store/Load a snapshot from disk, depending on the snapshot builder's state.
 *
 * Supposed to be used by external (i.e. not snapbuild.c) code that just reada
 * record that's a potential location for a serialized snapshot.
 */
void SnapBuildSerializationPoint(SnapBuild *builder, XLogRecPtr lsn)
{
    if (builder->state < SNAPBUILD_CONSISTENT) {
        (void)SnapBuildRestore(builder, lsn);
    } else {
        (void)SnapBuildSerialize(builder, lsn);
    }
}

/*
 * Serialize the snapshot 'builder' at the location 'lsn' if it hasn't already
 * been done by another decoding process.
 */
static void SnapBuildSerialize(SnapBuild *builder, XLogRecPtr lsn)
{
    Size needed_length = 0;
    SnapBuildOnDisk *ondisk = NULL;
    char *ondisk_c = NULL;
    int rc = 0;
    int fd = 0;
    char tmppath[MAXPGPATH];
    char path[MAXPGPATH];
    int ret = 0;
    struct stat stat_buf;
    uint32 sz = 0;
    int nRet = 0;
    Assert(!XLByteEQ(lsn, InvalidXLogRecPtr));
    Assert(XLByteEQ(builder->last_serialized_snapshot, InvalidXLogRecPtr) ||
           XLByteLE(builder->last_serialized_snapshot, lsn));

    /*
     * no point in serializing if we cannot continue to work immediately after
     * restoring the snapshot
     */
    if (builder->state < SNAPBUILD_CONSISTENT) {
        return;
    }

    /*
     * We identify snapshots by the LSN they are valid for. We don't need to
     * include timelines in the name as each LSN maps to exactly one timeline
     * unless the user used pg_resetxlog or similar. If a user did so, there's
     * no hope continuing to decode anyway.
     */
    nRet = sprintf_s(path, MAXPGPATH, "pg_llog/snapshots/%X-%X.snap", (uint32)(lsn >> 32), (uint32)lsn);
    securec_check_ss(nRet, "", "");
    /*
     * first check whether some other backend already has written the snapshot
     * for this LSN. It's perfectly fine if there's none, so we accept ENOENT
     * as a valid state. Everything else is an unexpected error.
     */
    ret = stat(path, &stat_buf);
    if (ret != 0 && errno != ENOENT) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("could not stat file \"%s\": %m", path)));
    } else if (ret == 0) {
        /*
         * somebody else has already serialized to this point, don't overwrite
         * but remember location, so we don't need to read old data again.
         *
         * To be sure it has been synced to disk after the rename() from the
         * tempfile filename to the real filename, we just repeat the
         * fsync. That ought to be cheap because in most scenarios it should
         * already be safely on disk.
         */
        fsync_fname(path, false);
        fsync_fname("pg_llog/snapshots", true);

        builder->last_serialized_snapshot = lsn;
        goto out;
    }

    /*
     * there is an obvious race condition here between the time we stat(2) the
     * file and us writing the file. But we rename the file into place
     * atomically and all files created need to contain the same data anyway,
     * so this is perfectly fine, although a bit of a resource waste. Locking
     * seems like pointless complication.
     */
    if (!RecoveryInProgress()) {
        ereport(DEBUG1, (errmsg("serializing snapshot to %s", path)));
    }

    /* to make sure only we will write to this tempfile, include pid */
    rc = sprintf_s(tmppath, sizeof(tmppath), "pg_llog/snapshots/%X-%X.snap.%u.tmp", (uint32)(lsn >> 32), (uint32)lsn,
                   (uint32)t_thrd.proc_cxt.MyProcPid);
    securec_check_ss(rc, "", "");

    /*
     * Unlink temporary file if it already exists, needs to have been before a
     * crash/error since we won't enter this function twice from within a
     * single decoding slot/backend and the temporary file contains the pid of
     * the current process.
     */
    if (unlink(tmppath) != 0 && errno != ENOENT) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));
    }

    needed_length = sizeof(SnapBuildOnDisk) + sizeof(TransactionId) * builder->committed.xcnt;

    ondisk_c = (char *)MemoryContextAllocZero(builder->context, needed_length);
    ondisk = (SnapBuildOnDisk *)ondisk_c;
    ondisk->magic = SNAPBUILD_MAGIC;
    ondisk->version = SNAPBUILD_VERSION;
    ondisk->length = needed_length;
    INIT_CRC32(ondisk->checksum);
    COMP_CRC32(ondisk->checksum, ((char *)ondisk) + SnapBuildOnDiskNotChecksummedSize,
               SnapBuildOnDiskConstantSize - SnapBuildOnDiskNotChecksummedSize);
    ondisk_c += sizeof(SnapBuildOnDisk);

    nRet = memcpy_s(&ondisk->builder, sizeof(SnapBuild), builder, sizeof(SnapBuild));
    securec_check(nRet, "", "");
    /* NULL-ify memory-only data */
    ondisk->builder.context = NULL;
    ondisk->builder.snapshot = NULL;
    ondisk->builder.reorder = NULL;
    ondisk->builder.committed.xip = NULL;

    COMP_CRC32(ondisk->checksum, &ondisk->builder, sizeof(SnapBuild));

    /* there shouldn't be any running xacts */
    Assert(builder->was_running.was_xcnt == 0);

    /* copy committed xacts */
    sz = sizeof(TransactionId) * builder->committed.xcnt;
    if (builder->committed.xcnt) {
        nRet = memcpy_s(ondisk_c, sz, builder->committed.xip, sz);
        securec_check(nRet, "", "");
    }
    COMP_CRC32(ondisk->checksum, ondisk_c, sz);
    ondisk_c += sz;

    /* we have valid data now, open tempfile and write it there */
    fd = OpenTransientFile(tmppath, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmsg("could not open file \"%s\": %m", path)));
    }

    if (Size(write(fd, ondisk, needed_length)) != needed_length) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", tmppath)));
    }

    /*
     * fsync the file before renaming so that even if we crash after this we
     * have either a fully valid file or nothing.
     *
     * It's safe to just ERROR on fsync() here because we'll retry the whole
     * operation including the writes.
     *
     * description: Do the fsync() via checkpoints/restartpoints, doing it here has
     * some noticeable overhead since it's performed synchronously during
     * decoding?
     */
    if (pg_fsync(fd) != 0) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", tmppath)));
    }
    (void)CloseTransientFile(fd);

    fsync_fname("pg_llog/snapshots", true);

    /*
     * We may overwrite the work from some other backend, but that's ok, our
     * snapshot is valid as well, we'll just have done some superflous work.
     */
    if (rename(tmppath, path) != 0) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m", tmppath, path)));
    }

    /* make sure we persist */
    fsync_fname(path, false);
    fsync_fname("pg_llog/snapshots", true);

    /*
     * Now there's no way we can loose the dumped state anymore, remember
     * this as a serialization point.
     */
    builder->last_serialized_snapshot = lsn;

out:
    ReorderBufferSetRestartPoint(builder->reorder, builder->last_serialized_snapshot);
}

static void CheckDiskFile(SnapBuildOnDisk* ondisk, char* path, size_t pathLen)
{
    if (ondisk->magic != SNAPBUILD_MAGIC) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("snapbuild state file \"%s\" has wrong magic %u instead of %d", path, ondisk->magic,
                               SNAPBUILD_MAGIC)));
    }

    if (ondisk->version != SNAPBUILD_VERSION) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                        errmsg("snapbuild state file \"%s\" has unsupported version %u instead of %d", path,
                               ondisk->version, SNAPBUILD_VERSION)));
    }
}
/*
 * Restore a snapshot into 'builder' if previously one has been stored at the
 * location indicated by 'lsn'. Returns true if successful, false otherwise.
 */
static bool SnapBuildRestore(SnapBuild *builder, XLogRecPtr lsn)
{
    SnapBuildOnDisk ondisk;
    int fd;
    char path[MAXPGPATH];
    Size sz;
    Size readBytes;
    pg_crc32 checksum;
    int nRet = 0;
    /* no point in loading a snapshot if we're already there */
    if (builder->state == SNAPBUILD_CONSISTENT)
        return false;

    nRet = sprintf_s(path, sizeof(path), "pg_llog/snapshots/%X-%X.snap", (uint32)(lsn >> 32), (uint32)lsn);
    securec_check_ss(nRet, "", "");

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0 && errno == ENOENT) {
        return false;
    } else if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
    }

    /* ----
     * Make sure the snapshot had been stored safely to disk, that's normally
     * cheap.
     * Note that we do not need PANIC here, nobody will be able to use the
     * slot without fsyncing, and saving it won't suceed without an fsync()
     * either...
     * ----
     */
    fsync_fname(path, false);
    fsync_fname("pg_llog/snapshots", true);

    /* read statically sized portion of snapshot */
    readBytes = (Size)read(fd, &ondisk, SnapBuildOnDiskConstantSize);
    if (readBytes != SnapBuildOnDiskConstantSize) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\", read %lu of %lu: %m", path,
                                                          readBytes, SnapBuildOnDiskConstantSize)));
    }

    CheckDiskFile(&ondisk, path, strlen(path));

    INIT_CRC32(checksum);
    COMP_CRC32(checksum, ((char *)&ondisk) + SnapBuildOnDiskNotChecksummedSize,
               SnapBuildOnDiskConstantSize - SnapBuildOnDiskNotChecksummedSize);

    /* read SnapBuild */
    readBytes = (Size)read(fd, &ondisk.builder, sizeof(SnapBuild));
    if (readBytes != sizeof(SnapBuild)) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("could not read file \"%s\", read %lu of %lu: %m", path, readBytes, sizeof(SnapBuild))));
    }
    COMP_CRC32(checksum, &ondisk.builder, sizeof(SnapBuild));

    /* restore running xacts (dead, but kept for backward compat) */
    if (MAX_SIZE_T_NUM / sizeof(TransactionId) < ondisk.builder.was_running.was_xcnt_space) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\", size overflow", path)));
    }
    /* restore running xacts (dead, but kept for backward compat) */
    sz = sizeof(TransactionId) * ondisk.builder.was_running.was_xcnt_space;
    ondisk.builder.was_running.was_xip = (TransactionId *)MemoryContextAllocZero(builder->context, sz);
    readBytes = read(fd, ondisk.builder.was_running.was_xip, sz);
    if (readBytes != sz) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\", read %lu of %lu: %m", path,
                                                          readBytes, (long unsigned int)sz)));
    }
    COMP_CRC32(checksum, ondisk.builder.was_running.was_xip, sz);

    /* restore committed xacts information */
    if (MAX_SIZE_T_NUM / sizeof(TransactionId) < ondisk.builder.committed.xcnt) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\", size overflow", path)));
    }
    sz = sizeof(TransactionId) * ondisk.builder.committed.xcnt;
    if (sz > 0) {
        ondisk.builder.committed.xip = (TransactionId *)MemoryContextAllocZero(builder->context, sz);
        readBytes = read(fd, ondisk.builder.committed.xip, sz);
        if (readBytes != sz) {
            (void)CloseTransientFile(fd);
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not read file \"%s\", read %lu of %lu: %m", path, readBytes, sz)));
        }
        COMP_CRC32(checksum, ondisk.builder.committed.xip, sz);
    }
    (void)CloseTransientFile(fd);

    /* verify checksum of what we've read */
    if (!EQ_CRC32(checksum, ondisk.checksum)) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("snapbuild state file %s: checksum mismatch, is %u, should be %u",
                                                   path, checksum, ondisk.checksum)));
    }

    /*
     * ok, we now have a sensible snapshot here, figure out if it has more
     * information than we have.
     *
     *
     * We are only interested in consistent snapshots for now, comparing
     * whether one imcomplete snapshot is more "advanced" seems to be
     * unnecessarily complex.
     */
    if (ondisk.builder.state < SNAPBUILD_CONSISTENT) {
        goto snapshot_not_interesting;
    }

    /*
     * Don't use a snapshot that requires an xmin that we cannot guarantee to
     * be available.
     */
    if (TransactionIdPrecedes(ondisk.builder.xmin, builder->initial_xmin_horizon)) {
        goto snapshot_not_interesting;
    }

    /* ok, we think the snapshot is sensible, copy over everything important */
    builder->xmin = ondisk.builder.xmin;
    builder->xmax = ondisk.builder.xmax;
    builder->state = ondisk.builder.state;

    builder->committed.xcnt = ondisk.builder.committed.xcnt;
    /*
     * We only allocated/stored xcnt, not xcnt_space xids !
     * don't overwrite preallocated xip, if we don't have anything here
     */
    if (builder->committed.xcnt > 0) {
        pfree(builder->committed.xip);
        builder->committed.xip = NULL;
        builder->committed.xcnt_space = ondisk.builder.committed.xcnt;
        builder->committed.xip = ondisk.builder.committed.xip;
    }
    ondisk.builder.committed.xip = NULL;

    /* our snapshot is not interesting anymore, build a new one */
    if (builder->snapshot != NULL) {
        SnapBuildSnapDecRefcount(builder->snapshot);
    }
    builder->snapshot = SnapBuildBuildSnapshot(builder, InvalidTransactionId);
    SnapBuildSnapIncRefcount(builder->snapshot);

    ReorderBufferSetRestartPoint(builder->reorder, lsn);

    Assert(builder->state == SNAPBUILD_CONSISTENT);

    if (!RecoveryInProgress()) {
        ereport(LOG, (errmsg("logical decoding found consistent point at %X/%X", (uint32)(lsn >> 32), (uint32)lsn),
                      errdetail("found initial snapshot in snapbuild file")));
    }
    return true;

snapshot_not_interesting:
    pfree_ext(ondisk.builder.committed.xip);
    return false;
}

static DIR *CheckSnapDir(const char *basedirpath, const char *snsdirpath)
{
    DIR *snap_dir = AllocateDir(snsdirpath);
    if (!(snap_dir == NULL && errno == ENOENT)) {
        return snap_dir;
    }
    
    /* create dir if not exist */
    if (mkdir(snsdirpath, S_IRWXU) < 0) {
        if (errno == ENOENT) {
            /* create parent dir first if not exist */
            if (mkdir(basedirpath, S_IRWXU) < 0) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", basedirpath)));
            }
            /* and then create the sub dir */
            if (mkdir(snsdirpath, S_IRWXU) < 0) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", snsdirpath)));
            }
        } else {
            /* Failure other than not exists */
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", snsdirpath)));
        }
    }
    snap_dir = AllocateDir(snsdirpath);
    return snap_dir;
}
/*
 * Remove all serialized snapshots that are not required anymore because no
 * slot can need them. This doesn't actually have to run during a checkpoint,
 * but it's a convenient point to schedule this.
 *
 * NB: We run this during checkpoints even if logical decoding is disabled so
 * we cleanup old slots at some point after it got disabled.
 */
void CheckPointSnapBuild(void)
{
    XLogRecPtr cutoff = InvalidXLogRecPtr;
    XLogRecPtr redo = InvalidXLogRecPtr;
    DIR *snap_dir = NULL;
    struct dirent *snap_de = NULL;
    char path[MAXPGPATH];
    const char *basedirpath = "pg_llog";
    const char *snsdirpath = "pg_llog/snapshots";
    int nRet = 0;
    /*
     * We start of with a minimum of the last redo pointer. No new replication
     * slot will start before that, so that's a safe upper bound for removal.
     */
    redo = GetRedoRecPtr();

    /* now check for the restart ptrs from existing slots */
    cutoff = ReplicationSlotsComputeLogicalRestartLSN();
    /* don't start earlier than the restart lsn */
    if (XLByteLT(redo, cutoff)) {
        cutoff = redo;
    }

    snap_dir = CheckSnapDir(basedirpath, snsdirpath);
    while ((snap_de = ReadDir(snap_dir, snsdirpath)) != NULL) {
        uint32 hi;
        uint32 lo;
        XLogRecPtr lsn;
        struct stat statbuf;

        if (strcmp(snap_de->d_name, ".") == 0 || strcmp(snap_de->d_name, "..") == 0) {
            continue;
        }

        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", snsdirpath, snap_de->d_name);
        securec_check_ss_c(nRet, "", "");

        if (lstat(path, &statbuf) == 0 && !S_ISREG(statbuf.st_mode)) {
            if (!RecoveryInProgress()) {
                ereport(DEBUG1, (errmsg("only regular files expected: %s", path)));
            }
            continue;
        }

        /*
         * temporary filenames from SnapBuildSerialize() include the LSN and
         * everything but are postfixed by .$pid.tmp. We can just remove them
         * the same as other files because there can be none that are currently
         * being written that are older than cutoff.
         *
         * We just log a message if a file doesn't fit the pattern, it's
         * probably some editors lock/state file or similar...
         */
        if (sscanf_s(snap_de->d_name, "%X-%X.snap", &hi, &lo) != 2) {
            if (!RecoveryInProgress()) {
                ereport(LOG, (errmsg("could not parse filename \"%s\"", path)));
            }
            continue;
        }

        lsn = ((uint64(hi)) << 32) + lo;
        /* check whether we still need it */
        if (XLByteLT(lsn, cutoff) || XLByteEQ(cutoff, InvalidXLogRecPtr)) {
            if (!RecoveryInProgress()) {
                ereport(DEBUG1, (errmsg("removing snapbuild snapshot %s", path)));
            }

            /*
             * It's not particularly harmful, though strange, if we can't
             * remove the file here. Don't prevent the checkpoint from
             * completing, that'd be cure worse than the disease.
             */
            if (unlink(path) < 0) {
                if (!RecoveryInProgress()) {
                    ereport(LOG, (errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));
                }
                continue;
            }
        }
    }
    (void)FreeDir(snap_dir);
}
