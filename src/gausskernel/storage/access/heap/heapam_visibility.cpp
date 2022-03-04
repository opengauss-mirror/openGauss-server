/* -------------------------------------------------------------------------
 *
 * heapam_visibility.c
 *	  Tuple visibility rules for tuples stored in heap.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: must check TransactionIdIsInProgress (which looks in PGXACT array)
 * before TransactionIdDidCommit/TransactionIdDidAbort (which look in
 * pg_clog).  Otherwise we have a race condition: we might decide that a
 * just-committed transaction crashed, because none of the tests succeed.
 * xact.c is careful to record commit/abort in pg_clog before it unsets
 * MyPgXact->xid in PGXACT array.  That fixes that problem, but it also
 * means there is a window where TransactionIdIsInProgress and
 * TransactionIdDidCommit will both return true.  If we check only
 * TransactionIdDidCommit, we could consider a tuple committed when a
 * later GetSnapshotData call will still think the originating transaction
 * is in progress, which leads to application-level inconsistency.	The
 * upshot is that we gotta check TransactionIdIsInProgress first in all
 * code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any
 * race condition.
 *
 * Summary of visibility functions:
 *
 *	 HeapTupleSatisfiesMVCC()
 *		  visible to supplied snapshot, excludes current command
 *	 HeapTupleSatisfiesNow()
 *		  visible to instant snapshot, excludes current command
 *	 HeapTupleSatisfiesUpdate()
 *		  like HeapTupleSatisfiesNow(), but with user-supplied command
 *		  counter and more complex result
 *	 HeapTupleSatisfiesSelf()
 *		  visible to instant snapshot and current command
 *	 HeapTupleSatisfiesDirty()
 *		  like HeapTupleSatisfiesSelf(), but includes open transactions
 *	 HeapTupleSatisfiesVacuum()
 *		  visible to any running transaction, used by VACUUM
 *	 HeapTupleSatisfiesToast()
 *		  visible unless part of interrupted vacuum, used for TOAST
 *	 HeapTupleSatisfiesAny()
 *		  all tuples are visible
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/heap/heapam_visibility.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/clog.h"
#include "access/csnlog.h"
#include "access/htup.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pgxc_group.h"
#include "storage/buf/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/snapmgr.h"
#include "commands/vacuum.h"

/* Log SetHintBits() */
static inline void LogSetHintBit(HeapTupleHeader tuple, Buffer buffer, uint16 infomask)
{
    switch (infomask) {
        case HEAP_XMIN_COMMITTED:
            ereport(LOG,
                (errmsg("Set Hint Bits: set oid %u infomask %s. Tuple Xmin : " XID_FMT ", Xmax : " XID_FMT ".",
                    HeapTupleHeaderGetOid(tuple),
                    "HEAP_XMIN_COMMITTED",
                    HeapTupleHeaderGetXmin(BufferGetPage(buffer), tuple),
                    HeapTupleHeaderGetXmax(BufferGetPage(buffer), tuple))));
            break;
        case HEAP_XMIN_INVALID:
            ereport(LOG,
                (errmsg("Set Hint Bits: set oid %u infomask %s. Tuple Xmin : " XID_FMT ", Xmax : " XID_FMT ".",
                    HeapTupleHeaderGetOid(tuple),
                    "HEAP_XMIN_INVALID",
                    HeapTupleHeaderGetXmin(BufferGetPage(buffer), tuple),
                    HeapTupleHeaderGetXmax(BufferGetPage(buffer), tuple))));
            break;
        case HEAP_XMAX_COMMITTED:
            ereport(LOG,
                (errmsg("Set Hint Bits: set oid %u infomask %s. Tuple Xmin : " XID_FMT ", Xmax : " XID_FMT ".",
                    HeapTupleHeaderGetOid(tuple),
                    "HEAP_XMAX_COMMITTED",
                    HeapTupleHeaderGetXmin(BufferGetPage(buffer), tuple),
                    HeapTupleHeaderGetXmax(BufferGetPage(buffer), tuple))));
            break;
        case HEAP_XMAX_INVALID:
            ereport(LOG,
                (errmsg("Set Hint Bits: set oid %u infomask %s. Tuple Xmin : " XID_FMT ", Xmax : " XID_FMT ".",
                    HeapTupleHeaderGetOid(tuple),
                    "HEAP_XMAX_INVALID",
                    HeapTupleHeaderGetXmin(BufferGetPage(buffer), tuple),
                    HeapTupleHeaderGetXmax(BufferGetPage(buffer), tuple))));
            break;
        default:
            break;
    }
}

/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record has been flushed to disk, or if the table is
 * temporary or unlogged and will be obliterated by a crash anyway.  We
 * cannot change the LSN of the page here because we may hold only a share
 * lock on the buffer, so we can't use the LSN to interlock this; we have to
 * just refrain from setting the hint bit until some future re-examination
 * of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.	(Some
 * code in heapam.c relies on that!)
 *
 * Also, if we are cleaning up HEAP_MOVED_IN or HEAP_MOVED_OFF entries, then
 * we can always set the hint bits, since pre-9.0 VACUUM FULL always used
 * synchronous commits and didn't move tuples that weren't previously
 * hinted.	(This is not known by this subroutine, but is applied by its
 * callers.)  Note: old-style VACUUM FULL is gone, but we have to keep this
 * module's support for MOVED_OFF/MOVED_IN flag bits for as long as we
 * support in-place update from pre-9.0 databases.
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */
static inline void SetHintBits(HeapTupleHeader tuple, Buffer buffer, uint16 infomask, TransactionId xid)
{
#ifdef PGXC
    // The following scenario may use local snapshot, so do not set hint bits.
    // Notice: we don't support two or more bits within infomask.
    //
    Assert(infomask > 0);
    Assert(0 == (infomask & (infomask - 1)));
    if ((t_thrd.xact_cxt.useLocalSnapshot && !(infomask & HEAP_XMIN_COMMITTED)) || RecoveryInProgress() ||
        g_instance.attr.attr_storage.IsRoachStandbyCluster) {
        ereport(DEBUG2, (errmsg("ignore setting tuple hint bits when local snapshot is used.")));
        return;
    }

    if (XACT_READ_UNCOMMITTED == u_sess->utils_cxt.XactIsoLevel && !(infomask & HEAP_XMIN_COMMITTED)) {
        ereport(DEBUG2, (errmsg("ignore setting tuple hint bits when XACT_READ_UNCOMMITTED is used.")));
        return;
    }
    /* The redistribution thread don't set. */
    if (u_sess->attr.attr_sql.enable_cluster_resize) {
        return;
    }
#endif

    /* The infomask has been set. */
    if (tuple->t_infomask & infomask) {
        return;
    }

    if (TransactionIdIsValid(xid)) {
        /* NB: xid must be known committed here! */
        XLogRecPtr commitLSN = TransactionIdGetCommitLSN(xid);

        if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) && XLByteLT(BufferGetLSNAtomic(buffer), commitLSN)) {
            /* not flushed and no LSN interlock, so don't set hint */
            return;
        }
    }

    tuple->t_infomask |= infomask;
    MarkBufferDirtyHint(buffer, true);

#ifdef USE_ASSERT_CHECKING
    if (u_sess->attr.attr_storage.enable_debug_vacuum && HeapTupleHeaderHasOid(tuple))
        LogSetHintBit(tuple, buffer, infomask);
#endif
}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer, uint16 infomask, TransactionId xid)
{
    SetHintBits(tuple, buffer, infomask, xid);
}

static bool DealCurrentTansactionNotCommited(HeapTupleHeader tuple, Page page, Buffer buffer)
{
    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
        return true;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) /* not deleter */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));
        /* updating subtransaction must have aborted */
        if (!TransactionIdIsCurrentTransactionId(xmax)) {
            return true;
        } else {
            return false;
        }
    }

    if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
        /* deleting subtransaction must have aborted */
        Assert(!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple)));
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    return false;
}

/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
bool HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    /* do not need sync, because snapshot is not used */
    Page page = BufferGetPage(buffer);

    ereport(DEBUG1,
        (errmsg("HeapTupleSatisfiesSelf self(%d,%d) ctid(%d,%d) cur_xid %ld xmin %ld"
                " xmax %ld csn %lu",
            ItemPointerGetBlockNumber(&htup->t_self),
            ItemPointerGetOffsetNumber(&htup->t_self),
            ItemPointerGetBlockNumber(&tuple->t_ctid),
            ItemPointerGetOffsetNumber(&tuple->t_ctid),
            GetCurrentTransactionIdIfAny(),
            HeapTupleHeaderGetXmin(page, tuple),
            HeapTupleHeaderGetXmax(page, tuple),
            snapshot->snapshotcsn)));

    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {
            return DealCurrentTansactionNotCommited(tuple, page, buffer);
        } else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(page, tuple)))
            return false;
        else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(page, tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));
        else {
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    NULL,
                    "HeapTupleSatisfiesSelf set HEAP_XMIN_INVALID xid don't abort");
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        return false; /* updated by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;

        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));
        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;
        if (TransactionIdIsInProgress(xmax)) {
            return true;
        }
        if (TransactionIdDidCommit(xmax))
            return false;
        /* it must have aborted or crashed */
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        return false;
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(page, tuple)))
        return true;

    if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple))) {
        if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
            LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                NULL,
                "HeapTupleSatisfiesSelf set HEAP_XMAX_INVALID xid don't abort");
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));
    return false;
}

/* Only sync pg_class. */
static inline bool NeedSyncXact(uint32 syncFlag, Oid tableOid, Snapshot snapshot)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* Skip sync when seqscan on pg_class */
    if (snapshot == SnapshotNowNoSync) {
        return false;
    }
    return ((syncFlag & SNAPSHOT_NOW_NEED_SYNC) && IsNormalProcessingMode()) &&
        (tableOid == RelationRelationId) && !u_sess->storage_cxt.twoPhaseCommitInProgress &&
        !IsAnyAutoVacuumProcess() && !IS_SINGLE_NODE &&
        !u_sess->attr.attr_common.xc_maintenance_mode && !t_thrd.xact_cxt.bInAbortTransaction;
#else
    return false;
#endif
}
/*
 * HeapTupleSatisfiesNow
 *		True iff heap tuple is valid "now".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *
 * Note we do _not_ include changes made by the current command.  This
 * solves the "Halloween problem" wherein an UPDATE might try to re-update
 * its own output tuples, http://en.wikipedia.org/wiki/Halloween_Problem.
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "now" requires the following:
 *
 * ((Xmin == my-transaction &&				inserted by the current transaction
 *	 Cmin < my-command &&					before this command, and
 *	 (Xmax is null ||						the row has not been deleted, or
 *	  (Xmax == my-transaction &&			it was deleted by the current transaction
 *	   Cmax >= my-command)))				but not before this command,
 * ||										or
 *	(Xmin is committed &&					the row was inserted by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *		 (Xmax == my-transaction &&			the row is being deleted by this transaction
 *		  Cmax >= my-command) ||			but it's not deleted "yet", or
 *		 (Xmax != my-transaction &&			the row was deleted by another transaction
 *		  Xmax is not committed))))			that has not been committed
 *
 */
bool HeapTupleSatisfiesNow(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    uint32 needSync = 0;
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    /* do not need sync, because snapshot is not used */
    Page page = BufferGetPage(buffer);

    ereport(DEBUG1,
        (errmsg("HeapTupleSatisfiesNow self(%d,%d) ctid (%d,%d) cur_xid %lu xmin %ld xmax %ld",
            ItemPointerGetBlockNumber(&htup->t_self),
            ItemPointerGetOffsetNumber(&htup->t_self),
            ItemPointerGetBlockNumber(&tuple->t_ctid),
            ItemPointerGetOffsetNumber(&tuple->t_ctid),
            GetCurrentTransactionIdIfAny(),
            HeapTupleHeaderGetXmin(page, tuple),
            HeapTupleHeaderGetXmax(page, tuple))));

restart:
    if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED)) {
        if (tuple->t_infomask & HEAP_XMIN_INVALID)
            return false;

        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {
            if (HeapTupleHeaderGetCmin(tuple, page) >= GetCurrentCommandId(false))
                return false; /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
                if (!TransactionIdIsValid(xmax)) {
                    return true;
                }

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax)) {
                    return true;
                } else {
                    return false;
                }
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
                /* deleting subtransaction must have aborted */
                Assert(!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple)));
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }

            if (HeapTupleHeaderGetCmax(tuple, page) >= GetCurrentCommandId(false))
                return true; /* deleted after scan started */
            else
                return false; /* deleted before scan started */
        } else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(page, tuple),
            &needSync, false, false, false, false)) {
            if (NeedSyncXact(needSync, htup->t_tableOid, snapshot)) {
                needSync = 0;
                SyncWaitXidEnd(HeapTupleHeaderGetXmin(page, tuple), buffer);
                goto restart;
            }
            return false;
        } else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(page, tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));
        else {
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    NULL,
                    "HeapTupleSatisfiesNow set HEAP_XMIN_INVALID xid don't abort");

            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        return false;
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;

        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        if (!TransactionIdIsValid(xmax)) {
            return true;
        }
        if (TransactionIdIsCurrentTransactionId(xmax)) {
            if (HeapTupleHeaderGetCmax(tuple, page) >= GetCurrentCommandId(false)) {
                return true; /* deleted after scan started */
            } else {
                return false; /* deleted before scan started */
            }
        }
        if (TransactionIdIsInProgress(xmax)) {
            return true;
        }
        if (TransactionIdDidCommit(xmax)) {
            return false;
        }
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        if (HeapTupleHeaderGetCmax(tuple, page) >= GetCurrentCommandId(false))
            return true; /* deleted after scan started */
        else
            return false; /* deleted before scan started */
    }

    needSync = 0;
    if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(page, tuple), &needSync, false, false, false, false)) {
        if (NeedSyncXact(needSync, htup->t_tableOid, snapshot)) {
            needSync = 0;
            SyncWaitXidEnd(HeapTupleHeaderGetXmax(page, tuple), buffer);
            goto restart;
        }
        return true;
    }

    if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple))) {
        if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
            LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                NULL,
                "HeapTupleSatisfiesNow set HEAP_XMAX_INVALID xid don't abort");

        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));
    return false;
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
static bool HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    return true;
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.	However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
bool HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    /* do not need sync, because snapshot is not used */

    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /*
         * An invalid Xmin can be left behind by a speculative insertion that
         * is cancelled by super-deleting the tuple.  We shouldn't see any of
         * those in TOAST tables, but better safe than sorry.
         */
        if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(BufferGetPage(buffer), tuple)))
             return false;
    }

    /* otherwise assume the tuple is valid for TOAST. */
    return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	Same logic as HeapTupleSatisfiesNow, but returns a more detailed result
 *	code, since UPDATE needs to know more than "is it visible?".  Also,
 *	tuples of my own xact are tested against the passed CommandId not
 *	CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	HeapTupleInvisible: the tuple didn't exist at all when the scan started,
 *	e.g. it was created by a later CommandId.
 *
 *	HeapTupleMayBeUpdated: The tuple is valid and visible, so it may be
 *	updated.
 *
 *	HeapTupleSelfUpdated: The tuple was updated by the current transaction,
 *	after the current scan started.
 *
 *	HeapTupleUpdated: The tuple was updated by a committed transaction.
 *
 *	HeapTupleBeingUpdated: The tuple is being updated by an in-progress
 *	transaction other than the current transaction.  (Note: this includes
 *	the case where the tuple is share-locked by a MultiXact, even if the
 *	MultiXact includes the current transaction.  Callers that want to
 *	distinguish that case must test for it themselves.)
 */
TM_Result HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid, Buffer buffer, bool self_visible)
{
    uint32 needSync = 0;
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    Page page = BufferGetPage(buffer);

    /* do not need sync, because snapshot is not used */
    ereport(DEBUG1,
        (errmsg("HeapTupleSatisfiesUpdate self(%u,%u) ctid(%u,%u) cur_xid " XID_FMT " xmin"
                XID_FMT " xmax " XID_FMT " infomask %u",
            ItemPointerGetBlockNumber(&htup->t_self),
            ItemPointerGetOffsetNumber(&htup->t_self),
            ItemPointerGetBlockNumber(&tuple->t_ctid),
            ItemPointerGetOffsetNumber(&tuple->t_ctid),
            GetCurrentTransactionIdIfAny(),
            HeapTupleHeaderGetXmin(page, tuple),
            HeapTupleHeaderGetXmax(page, tuple),
            tuple->t_infomask)));

restart:
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return TM_Invisible;

        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {
            if (HeapTupleHeaderGetCmin(tuple, page) > curcid)
                return TM_Invisible; /* inserted after scan started */
            else if (HeapTupleHeaderGetCmin(tuple, page) == curcid && !self_visible)
                return TM_SelfCreated; /* inserted during the scan */

            if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
                return TM_Ok;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
                TransactionId xmax = HeapTupleHeaderGetRawXmax(page, tuple);

                /*
                 * Careful here: even though this tuple was created by our own
                 * transaction, it might be locked by other transactions, if
                 * the original version was key-share locked when we updated
                 * it.
                 */
                if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                    if (MultiXactIdIsRunning(xmax))
                        return TM_BeingModified;
                    else
                        return TM_Ok;
                }

                /*
                 * If the locker is gone, then there is nothing of interest
                 * left in this Xmax; otherwise, report the tuple as
                 * locked/updated.
                 */
                if (!TransactionIdIsInProgress(xmax))
                    return TM_Ok;

                return TM_BeingModified;
            }

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                TransactionId xmax= HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);

                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* deleting subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax)) {
                    if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(page, tuple)))
                        return TM_BeingModified;
                    return TM_Ok;
                } else {
                    if (HeapTupleHeaderGetCmax(tuple, page) >= curcid)
                        return TM_SelfModified; /* updated after scan started */
                    else
                        return TM_Invisible; /* updated before scan started */
                }
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
                /* deleting subtransaction must have aborted */
                Assert(!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple)));
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return TM_Ok;
            }

            if (HeapTupleHeaderGetCmax(tuple, page) >= curcid)
                return TM_SelfModified; /* updated after scan started */
            else
                return TM_Invisible; /* updated before scan started */
        } else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(page, tuple),
            &needSync, false, false, false, false)) {
            if (needSync & SNAPSHOT_UPDATE_NEED_SYNC) {
                needSync = 0;
                SyncWaitXidEnd(HeapTupleHeaderGetXmin(page, tuple), buffer);
                goto restart;
            }
            return TM_Invisible;
        } else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(page, tuple))) {
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));
        } else {
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    NULL,
                    "HeapTupleSatisfiesUpdate set HEAP_XMIN_INVALID xid don't abort");
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return TM_Invisible;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */ {
        return TM_Ok;
    }
    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
            return TM_Ok;
        }
        if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid)) {
            return TM_Updated; /* updated by other */
        } else {
            return TM_Deleted; /* deleted by other */
        }
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
            /*
             * If it's only locked but HEAP_LOCK_MASK is not set,
             * it cannot possibly be running.  Otherwise need to check.
             */
            if ((tuple->t_infomask & HEAP_LOCK_MASK) &&
                MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(page, tuple)))
                return TM_BeingModified;

            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        }

        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsCurrentTransactionId(xmax)) {
            if (HeapTupleHeaderGetCmax(tuple, page) >= curcid)
                return TM_SelfModified; /* updated after scan started */
            else
                return TM_Invisible; /* updated before scan started */
        }

        if (TransactionIdIsInProgress(xmax))
            return TM_BeingModified;

        if (TransactionIdDidCommit(xmax)) {
            if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid)) {
                return TM_Updated;
            } else {
                return TM_Deleted;
            }
        }

        /*
         * By here, the update in the Xmax is either aborted or crashed, but
         * what about the other members?
         */
        if (!MultiXactIdIsRunning(HeapTupleHeaderGetXmax(page, tuple))) {
            /*
             * There's no member, even just a locker, alive anymore, so we can
             * mark the Xmax as invalid.
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return TM_Ok;
        } else {
            /* There are lockers running */
            return TM_BeingModified;
        }
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
            return TM_BeingModified;
        }
        if (HeapTupleHeaderGetCmax(tuple, page) >= curcid) {
            return TM_SelfModified; /* updated after scan started */
        } else {
            return TM_Invisible; /* updated before scan started */
        }
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(page, tuple))) {
        return TM_BeingModified;
    }

    if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple))) {
        if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
            LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                NULL,
                "HeapTupleSatisfiesUpdate set HEAP_XMAX_INVALID xid don't abort");
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return TM_Ok;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));

    if (!ItemPointerEquals(&htup->t_self, &tuple->t_ctid)) {
        return TM_Updated; /* updated by other */
    } else {
        return TM_Deleted; /* deleted by other */
    }
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 *	Here, we consider the effects of:
 *		all committed and in-progress transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.  Similarly
 * for snapshot->xmax and the tuple's xmax.
 */
static bool HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    /* do not need sync, because snapshot is not used */
    Page page = BufferGetPage(buffer);

    ereport(DEBUG1,
        (errmsg("HeapTupleSatisfiesDirty self(%d,%d) ctid(%d,%d) cur_xid %lu xmin"
                " %ld xmax %ld infomask %u",
            ItemPointerGetBlockNumber(&htup->t_self),
            ItemPointerGetOffsetNumber(&htup->t_self),
            ItemPointerGetBlockNumber(&tuple->t_ctid),
            ItemPointerGetOffsetNumber(&tuple->t_ctid),
            GetCurrentTransactionIdIfAny(),
            HeapTupleHeaderGetXmin(page, tuple),
            HeapTupleHeaderGetXmax(page, tuple),
            tuple->t_infomask)));

    snapshot->xmin = snapshot->xmax = InvalidTransactionId;

    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {
            return DealCurrentTansactionNotCommited(tuple, page, buffer);
        } else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(page, tuple))) {
            snapshot->xmin = HeapTupleHeaderGetXmin(page, tuple);
            /* XXX shouldn't we fall through to look at xmax? */
            return true; /* in insertion by other */
        } else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(page, tuple)))
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));
        else {
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    NULL,
                    "HeapTupleSatisfiesDirty set HEAP_XMIN_INVALID xid don't abort");
            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return false;
        }
    }

    /* by here, the inserting transaction has committed */

    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
        return true;

    if (tuple->t_infomask & HEAP_XMAX_COMMITTED) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        return false; /* updated by other */
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
            return true;
        }
        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));
        if (TransactionIdIsCurrentTransactionId(xmax))
            return false;
        if (TransactionIdIsInProgress(xmax)) {
            snapshot->xmax = xmax;
            return true;
        }
        if (TransactionIdDidCommit(xmax)) {
            return false;
        }
        /* it must have aborted or crashed */
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
        if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
            return true;
        return false;
    }

    if (TransactionIdIsInProgress(HeapTupleHeaderGetXmax(page, tuple))) {
        snapshot->xmax = HeapTupleHeaderGetXmax(page, tuple);
        return true;
    }

    if (!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple))) {
        if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
            LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                NULL,
                "HeapTupleSatisfiesDirty set HEAP_XMAX_INVALID xid don't abort");
        /* it must have aborted or crashed */
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    /* xmax transaction committed */

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
        SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        return true;
    }

    SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));
    return false; /* updated by other */
}

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 *	Here, we consider the effects of:
 *		all transactions committed as of the time of the given snapshot
 *		previous commands of this transaction
 *
 *	Does _not_ include:
 *		transactions shown as in-progress by the snapshot
 *		transactions started after the snapshot was taken
 *		changes made by the current command
 *
 * This is the same as HeapTupleSatisfiesNow, except that transactions that
 * were in progress or as yet unstarted when the snapshot was taken will
 * be treated as uncommitted, even if they have committed by now.
 *
 * (Notice, however, that the tuple status hint bits will be updated on the
 * basis of the true state of the transaction, even if we then pretend we
 * can't see it.)
 */
static bool HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    bool visible = false;
    TransactionIdStatus hintstatus;
    Page page = BufferGetPage(buffer);

    ereport(DEBUG1,
        (errmsg("HeapTupleSatisfiesMVCC self(%d,%d) ctid(%d,%d) cur_xid %ld xmin %ld"
                " xmax %ld csn %lu",
            ItemPointerGetBlockNumber(&htup->t_self),
            ItemPointerGetOffsetNumber(&htup->t_self),
            ItemPointerGetBlockNumber(&tuple->t_ctid),
            ItemPointerGetOffsetNumber(&tuple->t_ctid),
            GetCurrentTransactionIdIfAny(),
            HeapTupleHeaderGetXmin(page, tuple),
            HeapTupleHeaderGetXmax(page, tuple),
            snapshot->snapshotcsn)));

    /*
     * Just valid for read-only transaction when u_sess->attr.attr_common.XactReadOnly is true.
     * Show any tuples including dirty ones when u_sess->attr.attr_storage.enable_show_any_tuples is true.
     * GUC param u_sess->attr.attr_storage.enable_show_any_tuples is just for analyse or maintenance
     */
    if (u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.enable_show_any_tuples)
        return true;

    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return false;

        /* IMPORTANT: Version snapshot is independent of the current transaction. */
        if (!IsVersionMVCCSnapshot(snapshot) &&
            TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {
            if ((tuple->t_infomask & HEAP_COMBOCID) && CheckStreamCombocid(tuple, snapshot->curcid, page))
                return true; /* delete after stream producer thread scan started */

            if (HeapTupleHeaderGetCmin(tuple, page) >= snapshot->curcid)
                return false; /* inserted after scan started */

            if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
                return true;

            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) /* not deleter */
                return true;

            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
                /* not LOCKED_ONLY, so it has to have an xmax */
                Assert(TransactionIdIsValid(xmax));

                /* updating subtransaction must have aborted */
                if (!TransactionIdIsCurrentTransactionId(xmax))
                    return true;
                else if (HeapTupleHeaderGetCmax(tuple, page) >= snapshot->curcid)
                    return true; /* updated after scan started */
                else
                    return false; /* updated before scan started */
            }

            if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
                /* deleting subtransaction must have aborted */
                Assert(!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple)));
                SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
                return true;
            }

            if (HeapTupleHeaderGetCmax(tuple, page) >= snapshot->curcid)
                return true; /* deleted after scan started */
            else
                return false; /* deleted before scan started */
        } else {
            visible = XidVisibleInSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, &hintstatus, buffer, NULL);
            if (hintstatus == XID_COMMITTED)
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));

            if (hintstatus == XID_ABORTED) {
                if (!LatestFetchCSNDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                    LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                        snapshot,
                        "HeapTupleSatisfiesMVCC set HEAP_XMIN_INVALID xid don't abort");

                SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            }

            if (!visible) {
                if (!GTM_LITE_MODE || u_sess->attr.attr_common.xc_maintenance_mode ||
                    snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                    !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, hintstatus,
                    HeapTupleHeaderGetXmin(page, tuple) == HeapTupleHeaderGetXmax(page, tuple), buffer, NULL)) {
                    return false;
                }
            }
        }
    } else {
        /* xmin is committed, but maybe not according to our snapshot */
        if (!HeapTupleHeaderXminFrozen(tuple) &&
            !CommittedXidVisibleInSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, buffer)) {
            /* tuple xmin has already committed, no need to use xc_maintenance_mod bypass */
            if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, XID_COMMITTED,
                HeapTupleHeaderGetXmin(page, tuple) == HeapTupleHeaderGetXmax(page, tuple), buffer, NULL)) {
                return false; /* treat as still in progress */
            }
        }
    }

recheck_xmax:
    if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid or aborted */
        return true;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
        return true;

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        TransactionId xmax = HeapTupleHeaderMultiXactGetUpdateXid(page, tuple);
        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));
        if (TransactionIdIsCurrentTransactionId(xmax)) {
            if (HeapTupleHeaderGetCmax(tuple, page) >= snapshot->curcid)
                return true; /* deleted after scan started */
            else
                return false; /* deleted before scan started */
        }
        if (TransactionIdIsInProgress(xmax))
            return true;
        if (TransactionIdDidCommit(xmax)) {
            /* updating transaction committed, but when? */
            if (!CommittedXidVisibleInSnapshot(xmax, snapshot, buffer))
                return true; /* treat as still in progress */
            return false;
        }
        /* it must have aborted or crashed */
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
        bool sync = false;
        TransactionId xmax = HeapTupleHeaderGetXmax(page, tuple);

        /* IMPORTANT: Version snapshot is independent of the current transaction. */
        if (!IsVersionMVCCSnapshot(snapshot) &&
            TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
            if (HeapTupleHeaderGetCmax(tuple, page) >= snapshot->curcid)
                return true; /* deleted after scan started */
            else
                return false; /* deleted before scan started */
        }

        visible = XidVisibleInSnapshot(HeapTupleHeaderGetXmax(page, tuple), snapshot, &hintstatus, buffer, &sync);
        /*
         * If sync wait, xmax may be modified by others. So we need to check xmax again after acquiring the page lock.
         */
        if (sync && (xmax != HeapTupleHeaderGetXmax(page, tuple))) {
            goto recheck_xmax;
        }
        if (hintstatus == XID_COMMITTED) {
            /* xmax transaction committed */
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));
        }
        if (hintstatus == XID_ABORTED) {
            if (!LatestFetchCSNDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot,
                    "HeapTupleSatisfiesMVCC set HEAP_XMAX_INVALID xid don't abort");

            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
        if (!visible) {
            if (!GTM_LITE_MODE || u_sess->attr.attr_common.xc_maintenance_mode ||
                snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot, hintstatus, false, buffer, &sync)) {
                if (sync && (xmax != HeapTupleHeaderGetXmax(page, tuple))) {
                    goto recheck_xmax;
                }    
                return true; /* treat as still in progress */
            }
        }
    } else {
        /* xmax is committed, but maybe not according to our snapshot */
        if (!CommittedXidVisibleInSnapshot(HeapTupleHeaderGetXmax(page, tuple), snapshot, buffer)) {
            if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot, XID_COMMITTED, false, buffer, NULL)) {
                return true; /* treat as still in progress */
            }
        }
    }
    return false;
}

static bool HeapTupleSatisfiesVersionMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    /*
     * IMPORTANT: We distinguish the SNAPSHOT_MVCC and SNAPSHOT_VERSION_MVCC 
     *     with IsVersionMVCCSnapshot MACRO in HeapTupleSatisfiesMVCC implement.
     *     Here we define HeapTupleSatisfiesVersionMVCC to point the differences.
     */
    return HeapTupleSatisfiesMVCC(htup, snapshot, buffer);
}

static bool HeapTupleSatisfiesLost(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    /*
     * Used for `TIMECAPSULE TABLE TO TIMESTAMP/CSN expr` stmt.
     *
     * Tuple is visiable if inserted before `snapshot` and deleted after `snapshot` until SnapshotNow.
     * SnapshotNow is safe to use as we already locked table in AccessExclusiveLock before scan.
     */

    return HeapTupleSatisfiesVersionMVCC(htup, snapshot, buffer) &&
        !HeapTupleSatisfiesNow(htup, SnapshotNow, buffer);
}

static bool HeapTupleSatisfiesDelta(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    /*
     * Used for `TIMECAPSULE TABLE TO TIMESTAMP/CSN expr` stmt.
     *
     * Tuple is visiable if inserted after `snapshot` until SnapshotNow.
     * SnapshotNow is safe to use as we already locked table in AccessExclusiveLock before scan.
     */

    return !HeapTupleSatisfiesVersionMVCC(htup, snapshot, buffer) &&
        HeapTupleSatisfiesNow(htup, SnapshotNow, buffer);
}

/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from GetOldestXmin()).	Tuples
 * deleted by XIDs >= OldestXmin are deemed "recently dead"; they might
 * still be visible to some open transaction, so we can't remove them,
 * even if we see that the deleting transaction has committed.
 */
HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin, Buffer buffer, bool isAnalyzing)
{
    HeapTupleHeader tuple = htup->t_data;
    TransactionIdStatus xidstatus;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    /* do not need sync, because snapshot is not used */
    Page page = BufferGetPage(buffer);
    HeapTupleCopyBaseFromPage(htup, page);

    if (SHOW_DEBUG_MESSAGE()) {
        ereport(DEBUG1,
            (errmsg("HeapTupleSatisfiesVacuum self(%d,%d) ctid(%d,%d) cur_xid %lu xmin %ld"
                    " xmax %ld OldestXmin %ld",
                ItemPointerGetBlockNumber(&htup->t_self),
                ItemPointerGetOffsetNumber(&htup->t_self),
                ItemPointerGetBlockNumber(&tuple->t_ctid),
                ItemPointerGetOffsetNumber(&tuple->t_ctid),
                GetCurrentTransactionIdIfAny(),
                HeapTupleHeaderGetXmin(page, tuple),
                HeapTupleHeaderGetXmax(page, tuple),
                OldestXmin)));
    }

    /*
     * Has inserting transaction committed?
     *
     * If the inserting transaction aborted, then the tuple was never visible
     * to any other transaction, so we can delete it immediately.
     */
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        if (HeapTupleHeaderXminInvalid(tuple))
            return HEAPTUPLE_DEAD;
        xidstatus = TransactionIdGetStatus(HeapTupleGetRawXmin(htup));
        if (TransactionIdIsCurrentTransactionId(HeapTupleGetRawXmin(htup))) {
            if (tuple->t_infomask & HEAP_XMAX_INVALID) /* xid invalid */
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2))
                return HEAPTUPLE_INSERT_IN_PROGRESS;
            /* inserted and then deleted by same xact */
            if (TransactionIdIsCurrentTransactionId(HeapTupleGetRawXmax(htup))) {
                return HEAPTUPLE_DELETE_IN_PROGRESS;
            }
            /* deleting subtransaction must have aborted */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        } else if (xidstatus == XID_INPROGRESS && TransactionIdIsInProgress(HeapTupleGetRawXmin(htup))) {
            /*
             * It'd be possible to discern between INSERT/DELETE in progress
             * here by looking at xmax - but that doesn't seem beneficial for
             * the majority of callers and even detrimental for some. We'd
             * rather have callers look at/wait for xmin than xmax. It's
             * always correct to return INSERT_IN_PROGRESS because that's
             * what's happening from the view of other backends.
             */
            return HEAPTUPLE_INSERT_IN_PROGRESS;
        } else if (xidstatus == XID_COMMITTED ||
            (xidstatus == XID_INPROGRESS && TransactionIdDidCommit(HeapTupleGetRawXmin(htup)))) {
            /* must recheck clog again, since csn could be commit in progress before check TransactionIdIsInProgress */
            if (!isAnalyzing) {
                SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleGetRawXmin(htup));
            }
        } else {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            if (u_sess->attr.attr_storage.enable_debug_vacuum && t_thrd.utils_cxt.pRelatedRel) {
                elogVacuumInfo(
                    t_thrd.utils_cxt.pRelatedRel, htup, "HeapTupleSatisfiedVacuum set HEAP_XMIN_INVALID", OldestXmin);
            }
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    NULL,
                    "HeapTupleSatisfiedVacuum set HEAP_XMIN_INVALID xid don't abort");
            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
            return ((!t_thrd.xact_cxt.useLocalSnapshot || IsInitdb) ? HEAPTUPLE_DEAD : HEAPTUPLE_LIVE);
        }

        /*
         * At this point the xmin is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Okay, the inserter committed, so it was good at some point.	Now what
     * about the deleting transaction?
     */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return HEAPTUPLE_LIVE;

    if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
        /*
         * "Deleting" xact really only locked it, so the tuple is live in any
         * case.  However, we should make sure that either XMAX_COMMITTED or
         * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
         * examining the tuple for future xacts.  Also, marking dead
         * MultiXacts as invalid.
         */
        if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
                if (MultiXactIdIsRunning(HeapTupleGetRawXmax(htup)))
                    return HEAPTUPLE_LIVE;
            } else {
                xidstatus = TransactionIdGetStatus(HeapTupleGetRawXmax(htup));
                if (xidstatus == XID_INPROGRESS && TransactionIdIsInProgress(HeapTupleGetRawXmax(htup))) {
                    return HEAPTUPLE_LIVE;
                }
            }

            /*
             * We don't really care whether xmax did commit, abort or crash.
             * We know that xmax did lock the tuple, but it did not and will
             * never actually update it.
             */
            if (u_sess->attr.attr_storage.enable_debug_vacuum && t_thrd.utils_cxt.pRelatedRel) {
                elogVacuumInfo(
                    t_thrd.utils_cxt.pRelatedRel, htup, "HeapTupleSatisfiedVacuum set HEAP_XMAX_INVALID ", OldestXmin);
            }
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
        return HEAPTUPLE_LIVE;
    }

    if (tuple->t_infomask & HEAP_XMAX_IS_MULTI) {
        TransactionId xmax = HeapTupleHeaderGetUpdateXid(page, tuple);

        /* already checked above */
        Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2));

        /* not LOCKED_ONLY, so it has to have an xmax */
        Assert(TransactionIdIsValid(xmax));

        if (TransactionIdIsInProgress(xmax)) {
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        } else if (TransactionIdDidCommit(xmax)) {
            /*
             * The multixact might still be running due to lockers.  If the
             * updater is below the xid horizon, we have to return DEAD
             * regardless -- otherwise we could end up with a tuple where the
             * updater has to be removed due to the horizon, but is not pruned
             * away.  It's not a problem to prune that tuple, because any
             * remaining lockers will also be present in newer tuple versions.
             */
            if (!TransactionIdPrecedes(xmax, OldestXmin))
                return HEAPTUPLE_RECENTLY_DEAD;
            return HEAPTUPLE_DEAD;
        } else if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(page, tuple))) {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed.
             * Mark the Xmax as invalid.
             */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
        /* it must have aborted or crashed */
        return HEAPTUPLE_LIVE;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
        xidstatus = TransactionIdGetStatus(HeapTupleGetRawXmax(htup));
        if (xidstatus == XID_INPROGRESS && TransactionIdIsInProgress(HeapTupleGetRawXmax(htup))) {
            return HEAPTUPLE_DELETE_IN_PROGRESS;
        } else if (xidstatus == XID_COMMITTED ||
            (xidstatus == XID_INPROGRESS && TransactionIdDidCommit(HeapTupleGetRawXmax(htup)))) {
            /* must recheck clog again, since csn could be commit in progress before check TransactionIdIsInProgress */
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleGetRawXmax(htup));
        } else {
            /*
             * Not in Progress, Not Committed, so either Aborted or crashed
             */
            if (!LatestFetchTransactionIdDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                    NULL,
                    "HeapTupleSatisfiedVacuum set HEAP_XMAX_INVALID xid don't abort");

            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            return HEAPTUPLE_LIVE;
        }

        /*
         * At this point the xmax is known committed, but we might not have
         * been able to set the hint bit yet; so we can no longer Assert that
         * it's set.
         */
    }

    /*
     * Deleter committed, but perhaps it was recent enough that some open
     * transactions could still see the tuple.
     */
    if (!TransactionIdPrecedes(HeapTupleGetRawXmax(htup), OldestXmin))
        return ((!t_thrd.xact_cxt.useLocalSnapshot || IsInitdb) ? HEAPTUPLE_RECENTLY_DEAD : HEAPTUPLE_LIVE);

    /* Otherwise, it's dead and removable */
    return ((!t_thrd.xact_cxt.useLocalSnapshot || IsInitdb) ? HEAPTUPLE_DEAD : HEAPTUPLE_LIVE);
}

/*
 * HeapTupleIsSurelyDead
 *
 *	Determine whether a tuple is surely dead.  We sometimes use this
 *	in lieu of HeapTupleSatisifesVacuum when the tuple has just been
 *	tested by HeapTupleSatisfiesMVCC and, therefore, any hint bits that
 *	can be set should already be set.  We assume that if no hint bits
 *	either for xmin or xmax, the transaction is still running.	This is
 *	therefore faster than HeapTupleSatisfiesVacuum, because we don't
 *	consult CLOG (and also because we don't need to give an exact answer,
 *	just whether or not the tuple is surely dead).
 */
bool HeapTupleIsSurelyDead(HeapTuple tuple, TransactionId OldestXmin)
{
    HeapTupleHeader tup = tuple->t_data;

    /*
     * If the inserting transaction is marked invalid, then it aborted, and
     * the tuple is definitely dead.  If it's marked neither committed nor
     * invalid, then we assume it's still alive (since the presumption is that
     * all relevant hint bits were just set moments ago).
     */
    if (!HeapTupleHeaderXminCommitted(tup))
        return HeapTupleHeaderXminInvalid(tup) ? true : false;

    /*
     * If the inserting transaction committed, but any deleting transaction
     * aborted, the tuple is still alive.
     */
    if (tup->t_infomask & HEAP_XMAX_INVALID)
        return false;
    
    /*
     * If the XMAX is just a lock, the tuple is still alive.
     */
    if (HEAP_XMAX_IS_LOCKED_ONLY(tup->t_infomask, tup->t_infomask2))
        return false;

    /*
     * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
     * know without checking pg_multixact.
     */
    if (tup->t_infomask & HEAP_XMAX_IS_MULTI)
        return false;

    /* If deleter isn't known to have committed, assume it's still running. */
    if (!(tup->t_infomask & HEAP_XMAX_COMMITTED))
        return false;

    /* Deleter committed, so tuple is dead if the XID is old enough. */
    return TransactionIdPrecedes(HeapTupleGetRawXmax(tuple), OldestXmin);
}

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same time qualification rules
 * laid out at the top of this file.
 */
bool HeapTupleIsOnlyLocked(HeapTuple tuple)
{
    TransactionId xmax;

    /* if there's no valid Xmax, then there's obviously no update either */
    if (tuple->t_data->t_infomask & HEAP_XMAX_INVALID) {
        return true;
    }

    if (tuple->t_data->t_infomask2 & HEAP_XMAX_LOCK_ONLY) {
        return true;
    }

    /* invalid xmax means no update */
    if (!TransactionIdIsValid(HeapTupleGetRawXmax(tuple))) {
        return true;
    }

    /*
     * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this
     * must necessarily have been updated
     */
    if (!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)) {
        return false;
    }

    /* ... but if it's a multi, then perhaps the updating Xid aborted. */
    xmax = HeapTupleMultiXactGetUpdateXid(tuple);
    if (!TransactionIdIsValid(xmax)) { /* shouldn't happen .. */
        return true;
    }

    if (TransactionIdIsCurrentTransactionId(xmax)) {
        return false;
    }
    if (TransactionIdIsInProgress(xmax)) {
        return false;
    }
    if (TransactionIdDidCommit(xmax)) {
        return false;
    }

    /*
     * not current, not in progress, not committed -- must have aborted or
     * crashed
     */
    return true;
}

/*
 * check whether the transaciont id 'xid' in in the pre-sorted array 'xip'.
 */
static bool TransactionIdInArray(TransactionId xid, TransactionId* xip, Size num)
{
    return bsearch(&xid, xip, num, sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't need to support HEAP_MOVED_(IN|OFF) for now because we only support
 * reading catalog pages which couldn't have been created in an older version.
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
static bool HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    HeapTupleHeader tuple = htup->t_data;
    TransactionId xmin = HeapTupleHeaderGetXmin(page, tuple);
    TransactionId xmax = HeapTupleHeaderGetRawXmax(page, tuple);

    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);

    /* inserting transaction aborted */
    if (HeapTupleHeaderXminInvalid(tuple)) {
        Assert(!TransactionIdDidCommit(xmin));
        return false;
    }
    /* check if its one of our txids, toplevel is also in there */
    else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt)) {
        bool resolved = false;
        CommandId cmin = HeapTupleHeaderGetRawCommandId(tuple);
        CommandId cmax = InvalidCommandId;

        /*
         * another transaction might have (tried to) delete this tuple or
         * cmin/cmax was stored in a combocid. S we need to to lookup the
         * actual values externally.
         */
        resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot, htup, buffer, &cmin, &cmax);

        if (!resolved)
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("could not resolve cmin/cmax of catalog tuple")));

        Assert(cmin != InvalidCommandId);

        if (cmin >= snapshot->curcid)
            return false; /* inserted after scan started */
                          /* fall through */
    }
    /* committed before our xmin horizon. Do a normal visibility check. */
    else if (TransactionIdPrecedes(xmin, snapshot->xmin)) {
        Assert(!(HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin)));

        /* check for hint bit first, consult clog afterwards */
        if (!HeapTupleHeaderXminCommitted(tuple) && !TransactionIdDidCommit(xmin))
            return false;
        /* fall through */
    }
    /* beyond our xmax horizon, i.e. invisible */
    else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax)) {
        return false;
    }
    /* check if it's a committed transaction in [xmin, xmax) */
    else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt)) {
        /* fall through */
    }
    /*
     * none of the above, i.e. between [xmin, xmax) but hasn't
     * committed. I.e. invisible.
     */
    else {
        return false;
    }

    /* at this point we know xmin is visible, go on to check xmax */

    /* xid invalid or aborted */
    if (tuple->t_infomask & HEAP_XMAX_INVALID)
        return true;

    /*
     * The content had been updated in patch
     * We keep the old style here.
     */
    Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

    /* check if its one of our txids, toplevel is also in there */
    if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt)) {
        bool resolved = false;
        CommandId cmin;
        CommandId cmax = HeapTupleHeaderGetRawCommandId(tuple);

        /* Lookup actual cmin/cmax values */
        resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot, htup, buffer, &cmin, &cmax);

        if (!resolved)
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("could not resolve combocid to cmax")));

        Assert(cmax != InvalidCommandId);

        if (cmax >= snapshot->curcid)
            return true; /* deleted after scan started */
        else
            return false; /* deleted before scan started */
    }
    /* below xmin horizon, normal transaction state is valid */
    else if (TransactionIdPrecedes(xmax, snapshot->xmin)) {
        Assert(!((tuple->t_infomask & HEAP_XMAX_COMMITTED) && (!TransactionIdDidCommit(xmax))));

        /* check hint bit first */
        if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
            return false;

        /* check clog */
        return !TransactionIdDidCommit(xmax);
    }
    /* above xmax horizon, we cannot possibly see the deleting transaction */
    else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
        return true;
    /* xmax is between [xmin, xmax), check known committed array */
    else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
        return false;
    /* xmax is between [xmin, xmax), but known not to have committed yet */
    else
        return true;
    return true;
}

/*
 * Decode MVCC check the visibility of uncommited xmin, return true if the visibility is determinate.
 */
static bool HeapTupleUncommitedXminCheckDecodeMVCC(HeapTupleHeader tuple, Snapshot snapshot, bool *visible,
    TransactionIdStatus *status, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    if (HeapTupleHeaderXminInvalid(tuple)) {
        *visible = false;
        return true;
    }

    /* IMPORTANT: Version snapshot is independent of the current transaction. */
    if (!IsVersionMVCCSnapshot(snapshot) &&
        TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(page, tuple))) {

        if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
            HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2)) {
            *visible = true;
            return true;
        }

        Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

        if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmax(page, tuple))) {
            /* deleting subtransaction must have aborted */
            Assert(!TransactionIdDidCommit(HeapTupleHeaderGetXmax(page, tuple)));
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
            *visible = true;
            return true;
        }
    } else {
        bool xidVisible = XidVisibleInDecodeSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, status, buffer);
        if (*status == XID_COMMITTED)
            SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetXmin(page, tuple));

        if (*status == XID_ABORTED) {
            if (!LatestFetchCSNDidAbort(HeapTupleHeaderGetXmin(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmin(page, tuple),
                    snapshot,
                    "HeapTupleSatisfiesMVCC set HEAP_XMIN_INVALID xid don't abort");

            SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
        }

        if (!xidVisible) {
            if (!GTM_LITE_MODE || u_sess->attr.attr_common.xc_maintenance_mode ||
                snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, *status,
                HeapTupleHeaderGetXmin(page, tuple) == HeapTupleHeaderGetXmax(page, tuple), buffer, NULL)) {
                *visible = false;
                return true;
            }
        }
    }
    return false;
}

/*
 * Decode MVCC check the visibility of commited xmin, return true if the visibility is determinate.
 */
static bool HeapTupleCommitedXminCheckDecodeMVCC(HeapTupleHeader tuple, Snapshot snapshot, bool *visible,
    TransactionIdStatus *status, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    /* xmin is committed, but maybe not according to our snapshot */
    if (!HeapTupleHeaderXminFrozen(tuple) &&
        !CommittedXidVisibleInDecodeSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, buffer)) {
        /* tuple xmin has already committed, no need to use xc_maintenance_mod bypass */
        if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
            !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmin(page, tuple), snapshot, XID_COMMITTED,
            HeapTupleHeaderGetXmin(page, tuple) == HeapTupleHeaderGetXmax(page, tuple), buffer, NULL)) {
            *visible = false;
            return true; /* treat as still in progress */
        }
    }
    return false;
}

/*
 * Decode MVCC check the visibility of xmin, return true if the visibility is determinate.
 */
static bool HeapTupleXmaxCheckDecodeMVCC(HeapTupleHeader tuple, Snapshot snapshot, bool *visible,
    TransactionIdStatus *status, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    if ((tuple->t_infomask & HEAP_XMAX_INVALID) || HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask, tuple->t_infomask2) ||
        (tuple->t_infomask & HEAP_XMAX_IS_MULTI)) {
        *visible = true;
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED)) {
        bool xidVisible = XidVisibleInDecodeSnapshot(HeapTupleHeaderGetXmax(page, tuple), snapshot, status, buffer);
        if (*status == XID_COMMITTED) {
            /* xmax transaction committed */
            SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetXmax(page, tuple));
        }
        if (*status == XID_ABORTED) {
            if (!LatestFetchCSNDidAbort(HeapTupleHeaderGetXmax(page, tuple)))
                LatestTransactionStatusError(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot,
                    "HeapTupleSatisfiesMVCC set HEAP_XMAX_INVALID xid don't abort");

            /* it must have aborted or crashed */
            SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
        if (!xidVisible) {
            if (!GTM_LITE_MODE || u_sess->attr.attr_common.xc_maintenance_mode ||
                snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot, *status, false, buffer, NULL)) {
                *visible = true;
                return true;
            }
        }

    } else {
        /* xmax is committed, but maybe not according to our snapshot */
        if (!CommittedXidVisibleInDecodeSnapshot(HeapTupleHeaderGetXmax(page, tuple), snapshot, buffer)) {
            if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type != GTM_SNAPSHOT_TYPE_LOCAL ||
                !IsXidVisibleInGtmLiteLocalSnapshot(HeapTupleHeaderGetXmax(page, tuple),
                    snapshot, XID_COMMITTED, false, buffer, NULL)) {
                *visible = true;
                return true;
            }
        }
    }
    return false;
}

/*
 * MVCC used in parallel decoding, which is mainly based on CSN.
 */
static bool HeapTupleSatisfiesDecodeMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    HeapTupleHeader tuple = htup->t_data;
    Assert(ItemPointerIsValid(&htup->t_self));
    Assert(htup->t_tableOid != InvalidOid);
    bool visible = false;
    TransactionIdStatus hintstatus;

    /*
     * Just valid for read-only transaction when u_sess->attr.attr_common.XactReadOnly is true.
     * Show any tuples including dirty ones when u_sess->attr.attr_storage.enable_show_any_tuples is true.
     * GUC param u_sess->attr.attr_storage.enable_show_any_tuples is just for analyse or maintenance
     */
    if (u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.enable_show_any_tuples)
        return true;

    bool getVisibility = false;
    if (!HeapTupleHeaderXminCommitted(tuple)) {
        getVisibility = HeapTupleUncommitedXminCheckDecodeMVCC(tuple, snapshot, &visible, &hintstatus, buffer);
        if (getVisibility) {
            return visible;
        }
    } else {
        getVisibility = HeapTupleCommitedXminCheckDecodeMVCC(tuple, snapshot, &visible, &hintstatus, buffer);
        if (getVisibility) {
            return visible;
        }
    }

    getVisibility = HeapTupleXmaxCheckDecodeMVCC(tuple, snapshot, &visible, &hintstatus, buffer);
    if (getVisibility) {
        return visible;
    }
    return false;
}

/*
 * HeapTupleSatisfiesVisibility
 *		True iff heap tuple satisfies a time qual.
 *
 * Notes:
 *	Assumes heap tuple is valid, and buffer at least share locked.
 *
 *	Hint bits in the HeapTuple's t_infomask may be updated as a side effect;
 *	if so, the indicated buffer is marked dirty.
 */
bool HeapTupleSatisfiesVisibility(HeapTuple tup, Snapshot snapshot, Buffer buffer)
{
    switch (snapshot->satisfies) {
        case SNAPSHOT_MVCC:
            return HeapTupleSatisfiesMVCC(tup, snapshot, buffer);
            break;
        case SNAPSHOT_VERSION_MVCC:
            return HeapTupleSatisfiesVersionMVCC(tup, snapshot, buffer);
            break;
        case SNAPSHOT_DELTA:
            return HeapTupleSatisfiesDelta(tup, snapshot, buffer);
            break;
        case SNAPSHOT_LOST:
            return HeapTupleSatisfiesLost(tup, snapshot, buffer);
            break;
        case SNAPSHOT_NOW:
            return HeapTupleSatisfiesNow(tup, snapshot, buffer);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case SNAPSHOT_NOW_NO_SYNC:
            return HeapTupleSatisfiesNow(tup, snapshot, buffer);
            break;
#endif
        case SNAPSHOT_SELF:
            return HeapTupleSatisfiesSelf(tup, snapshot, buffer);
            break;
        case SNAPSHOT_ANY:
            return HeapTupleSatisfiesAny(tup, snapshot, buffer);
            break;
        case SNAPSHOT_TOAST:
            return HeapTupleSatisfiesToast(tup, snapshot, buffer);
            break;
        case SNAPSHOT_DIRTY:
            return HeapTupleSatisfiesDirty(tup, snapshot, buffer);
            break;
        case SNAPSHOT_HISTORIC_MVCC:
            return HeapTupleSatisfiesHistoricMVCC(tup, snapshot, buffer);
            break;
        case SNAPSHOT_DECODE_MVCC:
            return HeapTupleSatisfiesDecodeMVCC(tup, snapshot, buffer);
            break;
    }

    return false; /* keep compiler quiet */
}

void HeapTupleCheckVisible(Snapshot snapshot, HeapTuple tuple, Buffer buffer)
{
    if (!IsolationUsesXactSnapshot())
        return;
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    if (!HeapTupleSatisfiesVisibility(tuple, snapshot, buffer)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("could not serialize access due to concurrent update")));
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

