/* -------------------------------------------------------------------------
 * snapmgr.c
 *		openGauss snapshot manager
 *
 * We keep track of snapshots in two ways: those "registered" by resowner.c,
 * and the "active snapshot" stack.  All snapshots in either of them live in
 * persistent memory.  When a snapshot is no longer in any of these lists
 * (tracked by separate refcounts on each snapshot), its memory can be freed.
 *
 * The same is true for historic snapshots used during logical decoding,
 * their lifetime is managed separately (as they life longer as one xact.c
 * transaction).
 *
 * The FirstXactSnapshot, if any, is treated a bit specially: we increment its
 * regd_count and count it in RegisteredSnapshots, but this reference is not
 * tracked by a resource owner. We used to use the TopTransactionResourceOwner
 * to track this snapshot reference, but that introduces logical circularity
 * and thus makes it impossible to clean up in a sane fashion.	It's better to
 * handle this reference as an internally-tracked registration, so that this
 * module is entirely lower-level than ResourceOwners.
 *
 * Likewise, any snapshots that have been exported by pg_export_snapshot
 * have regd_count = 1 and are counted in RegisteredSnapshots, but are not
 * tracked by any resource owner.
 *
 * These arrangements let us reset MyPgXact->xmin when there are no snapshots
 * referenced by this transaction.	(One possible improvement would be to be
 * able to advance Xmin when the snapshot with the earliest Xmin is no longer
 * referenced.	That's a bit harder though, it requires more locking, and
 * anyway it should be rather uncommon to keep temporary snapshots referenced
 * for too long.)
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/time/snapmgr.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>
#include "access/csnlog.h"
#include "access/multi_redo_api.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/multi_redo_api.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "ddes/dms/ss_common_attr.h"
#include "ddes/dms/ss_transaction.h"
#include "storage/file/fio_device.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "storage/sinvaladt.h"
SnapshotData CatalogSnapshotData = {SNAPSHOT_MVCC};

/*
 * Elements of the active snapshot stack.
 *
 * Each element here accounts for exactly one active_count on SnapshotData.
 *
 * NB: the code assumes that elements in this list are in non-increasing
 * order of as_level; also, the list must be NULL-terminated.
 */
typedef struct ActiveSnapshotElt {
    Snapshot as_snap;
    int as_level;
    struct ActiveSnapshotElt* as_next;
} ActiveSnapshotElt;

static THR_LOCAL bool RegisterStreamSnapshot = false;

/* Define pathname of exported-snapshot files */
#define SNAPSHOT_EXPORT_DIR (g_instance.datadir_cxt.snapshotsDir)

/* Structure holding info about exported snapshot. */
typedef struct ExportedSnapshot {
    char *snapfile;
    Snapshot snapshot;
} ExportedSnapshot;

#define XactExportFilePath(path, xid, num, suffix) \
    {                                              \
        int rc = snprintf_s(path,                  \
            sizeof(path),                          \
            sizeof(path) - 1,                      \
            "%s/%08X%08X-%d%s",                    \
            (g_instance.datadir_cxt.snapshotsDir), \
            (uint32)((xid) >> 32),                 \
            (uint32)(xid),                         \
            (num),                                 \
            (suffix));                             \
        securec_check_ss(rc, "", "");              \
    }
#define MAX_ULONG_LENGTH 22

/* Static variables representing various special snapshot semantics */
THR_LOCAL SnapshotData SnapshotNowData = {SNAPSHOT_NOW};
THR_LOCAL SnapshotData SnapshotSelfData = {SNAPSHOT_SELF};
THR_LOCAL SnapshotData SnapshotAnyData = {SNAPSHOT_ANY};
THR_LOCAL SnapshotData SnapshotToastData = {SNAPSHOT_TOAST};
#ifdef ENABLE_MULTIPLE_NODES
THR_LOCAL SnapshotData SnapshotNowNoSyncData = {SNAPSHOT_NOW_NO_SYNC};
#endif

/* local functions */
static Snapshot CopySnapshot(Snapshot snapshot);
static void FreeSnapshot(Snapshot snapshot);
static void SnapshotResetXmin(void);
static bool TransactionIdIsPreparedAtSnapshot(TransactionId xid, Snapshot snapshot);

/*
 * TransactionIdIsPreparedAtSnapshot
 *   At the time of snapshot, true iff transaction associated
 *   with the identifier is prepared for two-phase commit
 *
 * Note: only gxacts marked "valid" are considered; but notice we do not
 * check the locking status.
 *
 */
static bool TransactionIdIsPreparedAtSnapshot(TransactionId xid, Snapshot snapshot)
{
    for (int i = 0; i < snapshot->prepared_count; i++) {
        if (xid == snapshot->prepared_array[i]) {
            return true;
        }
    }
    return false;
}

bool IsXidVisibleInGtmLiteLocalSnapshot(TransactionId xid, Snapshot snapshot,
    TransactionIdStatus hint_status, bool xmin_equal_xmax, Buffer buffer, bool *sync)
{
    /*
     * calling XidVisibleInSnapshot(), it will determine that if it is visible correctly.
     * However, because we support mixed snapshots (global and local), we have to
     * examine if the xmin is in prepared list or not. Even if it is "invisible",
     * but if it is in the list, we will treat it as visible (if the transaction is NOT aborted at the end).
     * exceptional case for xmin == xmax when check xmin
     * xmin_qual_xmax always false when check xmax
     */
    if (hint_status == XID_ABORTED || xmin_equal_xmax) {
        return false;
    }

    /* if in abort transaction, no need to check prepared array */
    if (!t_thrd.xact_cxt.bInAbortTransaction && TransactionIdIsPreparedAtSnapshot(xid, snapshot)) {
        if (hint_status == XID_COMMITTED) {
            return true;
        }

        /* Don't need to sync wait at maintenance mode */
        if (u_sess->attr.attr_common.xc_maintenance_mode) {
            return false;
        }
        /* Wait for txn end and check again. */
        if (sync != NULL) {
            *sync = true;
        }
        SyncWaitXidEnd(xid, buffer);
        /* should we always use clog ? */
        if (TransactionIdDidCommit(xid) == true) {
            return true;
        }
    }
    return false;
}

void RecheckXidFinish(TransactionId xid, CommitSeqNo csn)
{
    if (TransactionIdIsInProgress(xid)) {
        ereport(defence_errlevel(), (errmsg("transaction id %lu is still running, "
            "cannot set csn %lu to abort.", xid, csn)));
    }
}

/*
 * XidVisibleInSnapshot
 *		Is the given XID visible according to the snapshot?
 *
 * On return, *hintstatus is set to indicate if the transaction had committed,
 * or aborted, whether or not it's not visible to us.
 */
bool XidVisibleInSnapshot(TransactionId xid, Snapshot snapshot, TransactionIdStatus* hintstatus, Buffer buffer, bool* sync)
{
    volatile CommitSeqNo csn;
    bool looped = false;
    TransactionId parentXid = InvalidTransactionId;

    *hintstatus = XID_INPROGRESS;

#ifdef XIDVIS_DEBUG
    ereport(DEBUG1,
        (errmsg("XidVisibleInSnapshot xid %ld cur_xid %ld snapshot csn %lu xmax %ld",
            xid,
            GetCurrentTransactionIdIfAny(),
            snapshot->snapshotcsn,
            snapshot->xmax)));
#endif

loop:
    if (ENABLE_DMS) {
        /* fetch TXN info locally if either reformer, original primary, or normal primary */
        if (SSCanFetchLocalSnapshotTxnRelatedInfo()) {
            csn = TransactionIdGetCommitSeqNo(xid, false, true, false, snapshot);
        } else {
            csn = SSTransactionIdGetCommitSeqNo(xid, false, true, false, snapshot, sync);
        }
    } else {
        csn = TransactionIdGetCommitSeqNo(xid, false, true, false, snapshot);
    }

#ifdef XIDVIS_DEBUG
    ereport(DEBUG1,
        (errmsg("XidVisibleInSnapshot xid %ld cur_xid %ld csn %ld snapshot"
                "csn %ld xmax %ld",
            xid,
            GetCurrentTransactionIdIfAny(),
            csn,
            snapshot->snapshotcsn,
            snapshot->xmax)));
#endif

    if (COMMITSEQNO_IS_COMMITTED(csn)) {
        *hintstatus = XID_COMMITTED;
        if (csn < snapshot->snapshotcsn)
            return true;
        else
            return false;
    } else if (COMMITSEQNO_IS_COMMITTING(csn)) {
        /* SS master node would've already sync-waited, so this should never happen */
        if (SS_STANDBY_MODE) {
            ereport(FATAL, (errmsg("SS xid %lu's csn %lu is still COMMITTING after Master txn waited.", xid, csn)));
        }
        if (looped) {
            /* don't change csn log in recovery */
            if (snapshot->takenDuringRecovery) {
                return false;
            }
            ereport(DEBUG1, (errmsg("transaction id %lu's csn %ld is changed to ABORT after lockwait.", xid, csn)));
            /* recheck if transaction id is finished */
            RecheckXidFinish(xid, csn);
            CSNLogSetCommitSeqNo(xid, 0, NULL, COMMITSEQNO_ABORTED);
            SetLatestFetchState(xid, COMMITSEQNO_ABORTED);
            *hintstatus = XID_ABORTED;
            return false;
        } else {
            if (!COMMITSEQNO_IS_SUBTRANS(csn)) {
                /* If snapshotcsn lower than csn stored in csn log, don't need to wait. */
                CommitSeqNo latestCSN = GET_COMMITSEQNO(csn);
                if (latestCSN >= snapshot->snapshotcsn) {
                    ereport(DEBUG1,
                        (errmsg(
                            "snapshotcsn %lu lower than csn %lu stored in csn log, don't need to sync wait, trx id %lu",
                            snapshot->snapshotcsn,
                            csn,
                            xid)));
                    return false;
                }
            } else {
                parentXid = (TransactionId)GET_PARENTXID(csn);
            }

            if (u_sess->attr.attr_common.xc_maintenance_mode || t_thrd.xact_cxt.bInAbortTransaction) {
                return false;
            }

            /* Wait for txn end and check again. */
            if (sync != NULL) {
                *sync = true;
            }
            if (TransactionIdIsValid(parentXid))
                SyncWaitXidEnd(parentXid, buffer, snapshot);
            else
                SyncWaitXidEnd(xid, buffer, snapshot);
            looped = true;
            parentXid = InvalidTransactionId;
            goto loop;
        }
    } else {
        if (csn == COMMITSEQNO_ABORTED)
            *hintstatus = XID_ABORTED;

        return false;
    }
}

bool UHeapXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot,
    TransactionIdStatus *hintstatus, Buffer buffer, bool *sync)
{
    if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type == GTM_SNAPSHOT_TYPE_LOCAL) {
        /*
         * Make a quick range check to eliminate most XIDs without looking at the
         * CSN log.
         */
        if (TransactionIdPrecedes(xid, snapshot->xmin)) {
            return true;
        }

        /*
         * Any xid >= xmax is in-progress (or aborted, but we don't distinguish
         * that here.
         */
        if (GTM_MODE && TransactionIdFollowsOrEquals(xid, snapshot->xmax)) {
            return false;
        }
    }

    return XidVisibleInSnapshot(xid, snapshot, hintstatus, buffer, sync);
}

bool XidVisibleInDecodeSnapshot(TransactionId xid, Snapshot snapshot, TransactionIdStatus* hintstatus, Buffer buffer)
{
    volatile CommitSeqNo csn;
    *hintstatus = XID_INPROGRESS;

    csn = TransactionIdGetCommitSeqNo(xid, false, true, false, snapshot);
    if (COMMITSEQNO_IS_COMMITTED(csn)) {
        *hintstatus = XID_COMMITTED;
        if (csn < snapshot->snapshotcsn) {
            return true;
        } else {
            return false;
        }
    } else {
        if (csn == COMMITSEQNO_ABORTED) {
            *hintstatus = XID_ABORTED;
        }
    }
    return false;
}

/*
 * CommittedXidVisibleInSnapshot
 *		Is the given XID visible according to the snapshot?
 *
 * This is the same as XidVisibleInSnapshot, but the caller knows that the
 * given XID committed. The only question is whether it's visible to our
 * snapshot or not.
 */
bool CommittedXidVisibleInSnapshot(TransactionId xid, Snapshot snapshot, Buffer buffer)
{
    CommitSeqNo csn;
    bool looped = false;
    TransactionId parentXid = InvalidTransactionId;

    if (!GTM_LITE_MODE || snapshot->gtm_snapshot_type == GTM_SNAPSHOT_TYPE_LOCAL) {
        /*
         * Make a quick range check to eliminate most XIDs without looking at the
         * CSN log.
         */
        if (TransactionIdPrecedes(xid, snapshot->xmin))
            return true;
    }

loop:
    if (ENABLE_DMS) {
        /* fetch TXN info locally if either reformer, original primary, or normal primary */
        if (SSCanFetchLocalSnapshotTxnRelatedInfo()) {
            csn = TransactionIdGetCommitSeqNo(xid, true, true, false, snapshot);
        } else {
            csn = SSTransactionIdGetCommitSeqNo(xid, true, true, false, snapshot, NULL);
        }
    } else {
        csn = TransactionIdGetCommitSeqNo(xid, true, true, false, snapshot);
    }

    if (COMMITSEQNO_IS_COMMITTING(csn)) {
        /* SS master node would've already sync-waited, so this should never happen */
        if (SS_STANDBY_MODE) {
            ereport(FATAL, (errmsg("SS xid %lu's csn %lu is still COMMITTING after Master txn waited.", xid, csn)));
        }
        if (looped) {
            ereport(WARNING,
                (errmsg("committed transaction id %lu's csn %lu"
                        "is changed to frozen after lockwait.",
                    xid,
                    csn)));
            CSNLogSetCommitSeqNo(xid, 0, NULL, COMMITSEQNO_FROZEN);
            SetLatestFetchState(xid, COMMITSEQNO_FROZEN);
            return true;
        } else {
            if (!COMMITSEQNO_IS_SUBTRANS(csn)) {
                /* If snapshotcsn lower than csn stored in csn log, don't need to wait. */
                CommitSeqNo latestCSN = GET_COMMITSEQNO(csn);
                if (latestCSN >= snapshot->snapshotcsn) {
                    ereport(DEBUG1,
                        (errmsg("snapshotcsn %lu lower than csn %lu"
                                " stored in csn log, don't need to sync wait, trx id %lu",
                            snapshot->snapshotcsn,
                            csn,
                            xid)));
                    return false;
                }
            } else {
                parentXid = (TransactionId)GET_PARENTXID(csn);
            }

            if (u_sess->attr.attr_common.xc_maintenance_mode || t_thrd.xact_cxt.bInAbortTransaction) {
                return false;
            }

            /* Wait for txn end and check again. */
            if (TransactionIdIsValid(parentXid))
                SyncWaitXidEnd(parentXid, buffer);
            else
                SyncWaitXidEnd(xid, buffer);
            looped = true;
            parentXid = InvalidTransactionId;
            goto loop;
        }
    } else if (!COMMITSEQNO_IS_COMMITTED(csn)) {
        ereport(WARNING,
            (errmsg("transaction/csn %lu/%lu was hinted as "
                    "committed, but was not marked as committed in "
                    "the transaction log",
                xid,
                csn)));
        /*
         * We have contradicting evidence on whether the transaction committed or
         * not. Let's assume that it did. That seems better than erroring out.
         */
        return true;
    }

    if (csn < snapshot->snapshotcsn)
        return true;
    else
        return false;
}

bool CommittedXidVisibleInDecodeSnapshot(TransactionId xid, Snapshot snapshot, Buffer buffer)
{
    CommitSeqNo csn;

    csn = TransactionIdGetCommitSeqNo(xid, true, true, false, snapshot);
    if (COMMITSEQNO_IS_COMMITTING(csn)) {
        return false;
    } else if (!COMMITSEQNO_IS_COMMITTED(csn)) {
        ereport(WARNING,
            (errmsg("transaction/csn %lu/%lu was hinted as "
                    "committed, but was not marked as committed in "
                    "the transaction log",
                xid, csn)));
        /*
         * We have contradicting evidence on whether the transaction committed or
         * not. Let's assume that it did. That seems better than erroring out.
         */
        return true;
    }

    if (csn < snapshot->snapshotcsn) {
        return true;
    } else {
        return false;
    }
}

/*
 * GetTransactionSnapshot
 *		Get the appropriate snapshot for a new query in a transaction.
 *
 * Note that the return value may point at static storage that will be modified
 * by future calls and by CommandCounterIncrement().  Callers should call
 * RegisterSnapshot or PushActiveSnapshot on the returned snap if it is to be
 * used very long.
 */
Snapshot GetTransactionSnapshot(bool force_local_snapshot)
{
    /*
     * Return historic snapshot if doing logical decoding. We'll never
     * need a non-historic transaction snapshot in this (sub-)transaction, so
     * there's no need to be careful to set one up for later calls to
     * GetTransactionSnapshot function.
     */
    if (HistoricSnapshotActive()) {
        Assert(!u_sess->utils_cxt.FirstSnapshotSet);
        return u_sess->utils_cxt.HistoricSnapshot;
    }

    /* First call in transaction? */
    if (!u_sess->utils_cxt.FirstSnapshotSet) {
        Assert(u_sess->utils_cxt.RegisteredSnapshots == 0);
        Assert(u_sess->utils_cxt.FirstXactSnapshot == NULL);

        /*
         * In transaction-snapshot mode, the first snapshot must live until
         * end of xact regardless of what the caller does with it, so we must
         * make a copy of it rather than returning CurrentSnapshotData
         * directly.  Furthermore, if we're running in serializable mode,
         * predicate.c needs to wrap the snapshot fetch in its own processing.
         */
        Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
        if (IsolationUsesXactSnapshot()) {
            /* First, create the snapshot in CurrentSnapshotData */
            if (IsolationIsSerializable()) {
                u_sess->utils_cxt.CurrentSnapshot =
                    GetSerializableTransactionSnapshot(u_sess->utils_cxt.CurrentSnapshotData);
            } else {
                u_sess->utils_cxt.CurrentSnapshot =
                    GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, false);
            }
            Assert(
                !(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
            /* Make a saved copy */
            u_sess->utils_cxt.CurrentSnapshot = CopySnapshot(u_sess->utils_cxt.CurrentSnapshot);

            u_sess->utils_cxt.FirstXactSnapshot = u_sess->utils_cxt.CurrentSnapshot;
            /* Mark it as "registered" in FirstXactSnapshot */
            u_sess->utils_cxt.FirstXactSnapshot->regd_count++;
            u_sess->utils_cxt.RegisteredSnapshots++;
        } else {
            u_sess->utils_cxt.CurrentSnapshot =
                GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, force_local_snapshot);
        }

        u_sess->utils_cxt.FirstSnapshotSet = true;
        return u_sess->utils_cxt.CurrentSnapshot;
    }

    if (IsolationUsesXactSnapshot()|| IS_EXRTO_STANDBY_READ) {
#ifdef PGXC
        /*
         * Consider this test case taken from portals.sql
         *
         * CREATE TABLE cursor (a int, b int) distribute by replication;
         * INSERT INTO cursor VALUES (10);
         * BEGIN;
         * SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
         * DECLARE c1 NO SCROLL CURSOR FOR SELECT * FROM cursor FOR UPDATE;
         * INSERT INTO cursor VALUES (2);
         * FETCH ALL FROM c1;
         * would result in
         * ERROR:  attempted to lock invisible tuple
         * because FETCH would be sent as a select to the remote nodes
         * with command id 0, whereas the command id would be 2
         * in the current snapshot.
         * (1 sent by Coordinator due to declare cursor &
         *  2 because of the insert inside the transaction)
         * The command id should therefore be updated in the
         * current snapshot.
         */
        if (IsConnFromCoord())
            SnapshotSetCommandId(GetCurrentCommandId(false));
#endif
        if (IS_EXRTO_STANDBY_READ) {
            t_thrd.pgxact->xmin = u_sess->utils_cxt.CurrentSnapshot->xmin;
            t_thrd.proc->exrto_min = u_sess->utils_cxt.CurrentSnapshot->read_lsn;
            t_thrd.proc->exrto_read_lsn = t_thrd.proc->exrto_min;
            t_thrd.proc->exrto_gen_snap_time = GetCurrentTimestamp();
        }
        return u_sess->utils_cxt.CurrentSnapshot;
    }
    Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
    u_sess->utils_cxt.CurrentSnapshot =
        GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, force_local_snapshot);
    return u_sess->utils_cxt.CurrentSnapshot;
}

void StreamTxnContextSetSnapShot(void* snapshotPtr)
{
    u_sess->utils_cxt.FirstSnapshotSet = true;
    Snapshot snapshot = (Snapshot)snapshotPtr;
    u_sess->utils_cxt.CurrentSnapshot = u_sess->utils_cxt.CurrentSnapshotData;
    u_sess->utils_cxt.CurrentSnapshot->xmin = snapshot->xmin;
    u_sess->utils_cxt.CurrentSnapshot->xmax = snapshot->xmax;
    u_sess->utils_cxt.CurrentSnapshot->timeline = snapshot->timeline;
    u_sess->utils_cxt.CurrentSnapshot->snapshotcsn = snapshot->snapshotcsn;
    u_sess->utils_cxt.CurrentSnapshot->read_lsn = snapshot->read_lsn;

    u_sess->utils_cxt.CurrentSnapshot->curcid = snapshot->curcid;

    /*
     * This is a new snapshot, so set both refcounts are zero, and mark it as
     * not copied in persistent memory.
     */
    u_sess->utils_cxt.CurrentSnapshot->active_count = 0;
    u_sess->utils_cxt.CurrentSnapshot->regd_count = 0;
    u_sess->utils_cxt.CurrentSnapshot->copied = false;
}

void StreamTxnContextSetMyPgXactXmin(TransactionId xmin)
{
    t_thrd.pgxact->xmin = xmin;
}

/*
 * GetLatestSnapshot
 *		Get a snapshot that is up-to-date as of the current instant,
 *		even if we are executing in transaction-snapshot mode.
 */
Snapshot GetLatestSnapshot(void)
{
    /*
     * So far there are no cases requiring support for GetLatestSnapshot()
     * during logical decoding, but it wouldn't be hard to add if
     * required.
     */
    Assert(!HistoricSnapshotActive());

    /* If first call in transaction, go ahead and set the xact snapshot */
    if (!u_sess->utils_cxt.FirstSnapshotSet)
        return GetTransactionSnapshot();

    Assert(!(u_sess->utils_cxt.SecondarySnapshot != NULL && u_sess->utils_cxt.SecondarySnapshot->user_data != NULL));

    u_sess->utils_cxt.SecondarySnapshot =
        GetSnapshotData(u_sess->utils_cxt.SecondarySnapshotData, false);

    return u_sess->utils_cxt.SecondarySnapshot;
}

Snapshot get_standby_snapshot()
{
    if (u_sess->utils_cxt.FirstSnapshotSet) {
        Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
        return u_sess->utils_cxt.CurrentSnapshot;
    }

    return GetTransactionSnapshot();
}

/*
 * GetCatalogSnapshot
 *      Get a snapshot that is sufficiently up-to-date for scan of the
 *      system catalog with the specified OID.
 */
Snapshot GetCatalogSnapshot()
{
    /*
     * Return historic snapshot if we're doing logical decoding, but
     * return a non-historic, snapshot if we temporarily are doing up2date
     * lookups.
     */
    if (HistoricSnapshotActive())
        return u_sess->utils_cxt.HistoricSnapshot;

    if (IS_EXRTO_RECOVERY_IN_PROGRESS && t_thrd.role != TRACK_STMT_WORKER && !dummyStandbyMode) {
        return get_standby_snapshot();
    }

    return SnapshotNow;
}

/*
 * get_toast_snapshot
 *      Get a snapshot that is sufficiently up-to-date for scan of the
 *      toast with the specified OID.
 */
Snapshot get_toast_snapshot()
{
    if (IS_EXRTO_RECOVERY_IN_PROGRESS && t_thrd.role != TRACK_STMT_WORKER && !dummyStandbyMode) {
        return get_standby_snapshot();
    }

    return SnapshotToast;
}

/*
 * SnapshotSetCommandId
 *		Propagate CommandCounterIncrement into the static snapshots, if set
 */
void SnapshotSetCommandId(CommandId curcid)
{
    if (!u_sess->utils_cxt.FirstSnapshotSet)
        return;

    if (u_sess->utils_cxt.CurrentSnapshot)
        u_sess->utils_cxt.CurrentSnapshot->curcid = curcid;
    if (u_sess->utils_cxt.SecondarySnapshot)
        u_sess->utils_cxt.SecondarySnapshot->curcid = curcid;

    if (SS_PRIMARY_MODE || SS_OFFICIAL_PRIMARY) {
        t_thrd.pgxact->cid = curcid;
    }
}

/*
 * SetTransactionSnapshot
 *		Set the transaction's snapshot from an imported MVCC snapshot.
 *
 * Note that this is very closely tied to GetTransactionSnapshot --- it
 * must take care of all the same considerations as the first-snapshot case
 * in GetTransactionSnapshot.
 */
void SetTransactionSnapshot(Snapshot sourcesnap, VirtualTransactionId *sourcevxid, ThreadId sourcepid)
{
    /* Caller should have checked this already */
    Assert(!u_sess->utils_cxt.FirstSnapshotSet);

    Assert(u_sess->utils_cxt.RegisteredSnapshots == 0);
    Assert(u_sess->utils_cxt.FirstXactSnapshot == NULL);
    Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
    /*
     * Even though we are not going to use the snapshot it computes, we must
     * call GetSnapshotData, for two reasons: (1) to be sure that
     * CurrentSnapshotData's XID arrays have been allocated, and (2) to update
     * RecentXmin and RecentGlobalXmin.  (We could alternatively include those
     * two variables in exported snapshot files, but it seems better to have
     * snapshot importers compute reasonably up-to-date values for them.)
     */
    u_sess->utils_cxt.CurrentSnapshot = GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, false);

    /*
     * Now copy appropriate fields from the source snapshot.
     */
    u_sess->utils_cxt.CurrentSnapshot->xmin = sourcesnap->xmin;
    u_sess->utils_cxt.CurrentSnapshot->xmax = sourcesnap->xmax;
    u_sess->utils_cxt.CurrentSnapshot->snapshotcsn = sourcesnap->snapshotcsn;
    u_sess->utils_cxt.CurrentSnapshot->timeline = sourcesnap->timeline;
    u_sess->utils_cxt.CurrentSnapshot->takenDuringRecovery = sourcesnap->takenDuringRecovery;
    /* NB: curcid should NOT be copied, it's a local matter */

    /*
     * Now we have to fix what GetSnapshotData did with MyPgXact->xmin and
     * TransactionXmin.  There is a race condition: to make sure we are not
     * causing the global xmin to go backwards, we have to test that the
     * source transaction is still running, and that has to be done
     * atomically. So let procarray.c do it.
     *
     * Note: in serializable mode, predicate.c will do this a second time. It
     * doesn't seem worth contorting the logic here to avoid two calls,
     * especially since it's not clear that predicate.c *must* do this.
     */

    /*
     * In transaction-snapshot mode, the first snapshot must live until end of
     * xact, so we must make a copy of it.	Furthermore, if we're running in
     * serializable mode, predicate.c needs to do its own processing.
     */
    if (IsolationUsesXactSnapshot()) {
        if (IsolationIsSerializable())
            SetSerializableTransactionSnapshot(u_sess->utils_cxt.CurrentSnapshot, sourcevxid, sourcepid);
        /* Make a saved copy */
        Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
        u_sess->utils_cxt.CurrentSnapshot = CopySnapshot(u_sess->utils_cxt.CurrentSnapshot);
        u_sess->utils_cxt.FirstXactSnapshot = u_sess->utils_cxt.CurrentSnapshot;
        /* Mark it as "registered" in FirstXactSnapshot */
        u_sess->utils_cxt.FirstXactSnapshot->regd_count++;
        u_sess->utils_cxt.RegisteredSnapshots++;
    }

    u_sess->utils_cxt.FirstSnapshotSet = true;
}

/*
 * CopySnapshot
 *		Copy the given snapshot.
 *
 * The copy is palloc'd in u_sess->top_transaction_mem_cxt and has initial refcounts set
 * to 0.  The returned snapshot has the copied flag set.
 */
Snapshot CopySnapshot(Snapshot snapshot)
{
    Snapshot newsnap;
    int rc = 0;

    Assert(snapshot != InvalidSnapshot);

    newsnap = (Snapshot)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, sizeof(SnapshotData));
    rc = memcpy_s(newsnap, sizeof(SnapshotData), snapshot, sizeof(SnapshotData));
    securec_check(rc, "", "");

    newsnap->regd_count = 0;
    newsnap->active_count = 0;
    newsnap->copied = true;
    newsnap->user_data = NULL;

    if (GTM_LITE_MODE && snapshot->prepared_array) { /* prepared_array is only defined for gtm_lite */
        Assert(snapshot->prepared_array_capacity > 0);
        size_t arraySize = sizeof(TransactionId) * snapshot->prepared_array_capacity;
        newsnap->prepared_array =
            (TransactionId *)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, arraySize);
        rc = memcpy_s(newsnap->prepared_array, arraySize, snapshot->prepared_array, arraySize);
        securec_check(rc, "", "");
    }
    return newsnap;
}

/*
 * CopySnapshot
 *		Copy the given snapshot.
 *
 * The copy is palloc'd in u_sess->top_transaction_mem_cxt and has initial refcounts set
 * to 0.  The returned snapshot has the copied flag set.
 */
Snapshot CopySnapshotByCurrentMcxt(Snapshot snapshot)
{
    Snapshot newsnap;
    errno_t rc;

    Assert(snapshot != InvalidSnapshot);

    newsnap = (Snapshot)palloc(sizeof(SnapshotData));
    rc = memcpy_s(newsnap, sizeof(SnapshotData), snapshot, sizeof(SnapshotData));
    securec_check(rc, "\0", "\0");

    newsnap->regd_count = 0;
    newsnap->active_count = 0;
    newsnap->copied = true;
    newsnap->user_data = NULL;

    if (GTM_LITE_MODE && snapshot->prepared_array) { /* prepared_array is only defined for gtm_lite */
        int arraySize = sizeof(TransactionId) * snapshot->prepared_array_capacity;
        newsnap->prepared_array = (TransactionId *)palloc(arraySize);
        rc = memcpy_s(newsnap->prepared_array, arraySize, snapshot->prepared_array, arraySize);
        securec_check(rc, "", "");
    }

    return newsnap;
}

/*
 * FreeSnapshot
 *		Free the memory associated with a snapshot.
 */
static void FreeSnapshot(Snapshot snapshot)
{
    Assert(snapshot->regd_count == 0);
    Assert(snapshot->active_count == 0);
    Assert(snapshot->copied);
    Assert(!(snapshot != NULL && snapshot->user_data != NULL));
    if (GTM_LITE_MODE && snapshot->prepared_array) {
        pfree_ext(snapshot->prepared_array);
    }

    pfree_ext(snapshot);
}


/*
 * FreeSnapshot
 *		Free the memory associated with snapshot members exclude snapshot struct.
 */
void FreeSnapshotDeepForce(Snapshot snap)
{
    if (snap->xip) {
        pfree_ext(snap->xip);
    }

    if (snap->subxip) {
        pfree_ext(snap->subxip);
    }

    if (snap->prepared_array) {
        pfree_ext(snap->prepared_array);
    }

    if (snap->user_data) {
        pfree_ext(snap->user_data);
    }

    pfree_ext(snap);
}

/*
 * PushActiveSnapshot
 *		Set the given snapshot as the current active snapshot
 *
 * If the passed snapshot is a statically-allocated one, or it is possibly
 * subject to a future command counter update, create a new long-lived copy
 * with active refcount=1.	Otherwise, only increment the refcount.
 */
void PushActiveSnapshot(Snapshot snap)
{
    ActiveSnapshotElt* newactive = NULL;

    Assert(snap != InvalidSnapshot);

    newactive = (ActiveSnapshotElt*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, sizeof(ActiveSnapshotElt));

    /*
     * Checking SecondarySnapshot is probably useless here, but it seems
     * better to be sure.
     */
    if (snap == u_sess->utils_cxt.CurrentSnapshot || snap == u_sess->utils_cxt.SecondarySnapshot || !snap->copied ||
        snap == u_sess->pgxc_cxt.gc_fdw_snapshot)
        newactive->as_snap = CopySnapshot(snap);
    else
        newactive->as_snap = snap;

    newactive->as_next = u_sess->utils_cxt.ActiveSnapshot;
    newactive->as_level = GetCurrentTransactionNestLevel();

    newactive->as_snap->active_count++;

    u_sess->utils_cxt.ActiveSnapshot = newactive;
}

/*
 * PushCopiedSnapshot
 *		As above, except forcibly copy the presented snapshot.
 *
 * This should be used when the ActiveSnapshot has to be modifiable, for
 * example if the caller intends to call UpdateActiveSnapshotCommandId.
 * The new snapshot will be released when popped from the stack.
 */
void PushCopiedSnapshot(Snapshot snapshot)
{
    PushActiveSnapshot(CopySnapshot(snapshot));
}

/*
 * UpdateActiveSnapshotCommandId
 *
 * Update the current CID of the active snapshot.  This can only be applied
 * to a snapshot that is not referenced elsewhere.
 */
void UpdateActiveSnapshotCommandId(void)
{
    Assert(u_sess->utils_cxt.ActiveSnapshot != NULL);
    Assert(u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count == 1);
    Assert(u_sess->utils_cxt.ActiveSnapshot->as_snap->regd_count == 0);

    u_sess->utils_cxt.ActiveSnapshot->as_snap->curcid = GetCurrentCommandId(false);
}

/*
 * PopActiveSnapshot
 *
 * Remove the topmost snapshot from the active snapshot stack, decrementing the
 * reference count, and free it if this was the last reference.
 */
void PopActiveSnapshot(void)
{
    /* 
     * In multi commit/rollback within stored procedure, the AciveSnapshot already poped.
     * Therefore, no need to pop the active snapshot. Otherwise it will cause seg falut.
     */
    if(!u_sess->utils_cxt.ActiveSnapshot) {
        return;
    }
    ActiveSnapshotElt* newstack = NULL;

    newstack = u_sess->utils_cxt.ActiveSnapshot->as_next;

    Assert(u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count > 0);

    u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count--;

    if (u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count == 0 &&
        u_sess->utils_cxt.ActiveSnapshot->as_snap->regd_count == 0)
        FreeSnapshot(u_sess->utils_cxt.ActiveSnapshot->as_snap);

    pfree_ext(u_sess->utils_cxt.ActiveSnapshot);
    u_sess->utils_cxt.ActiveSnapshot = newstack;

    SnapshotResetXmin();
}

/*
 * GetActiveSnapshot
 *		Return the topmost snapshot in the Active stack.
 */
Snapshot GetActiveSnapshot(void)
{
#ifdef PGXC
    /*
     * Check if topmost snapshot is null or not,
     * if it is, a new one will be taken from GTM.
     */
    if (!u_sess->utils_cxt.ActiveSnapshot && IS_PGXC_COORDINATOR && !IsConnFromCoord())
        return NULL;
#endif

    if (u_sess->utils_cxt.ActiveSnapshot == NULL) {
        ereport(ERROR,
            (errmodule(MOD_TRANS_SNAPSHOT),
                errcode(ERRCODE_INVALID_STATUS),
                errmsg("snapshot is not active")));
    }
    return u_sess->utils_cxt.ActiveSnapshot->as_snap;
}

/*
 * ActiveSnapshotSet
 *		Return whether there is at least one snapshot in the Active stack
 */
bool ActiveSnapshotSet(void)
{
    return u_sess->utils_cxt.ActiveSnapshot != NULL;
}

/*
 * RegisterSnapshot
 *		Register a snapshot as being in use by the current resource owner
 *
 * If InvalidSnapshot is passed, it is not registered.
 */
Snapshot RegisterSnapshot(Snapshot snapshot)
{
    if (snapshot == InvalidSnapshot)
        return InvalidSnapshot;

    return RegisterSnapshotOnOwner(snapshot, t_thrd.utils_cxt.CurrentResourceOwner);
}

/*
 * RegisterSnapshotOnOwner
 *		As above, but use the specified resource owner
 */
Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner)
{
    Snapshot snap;

    if (snapshot == InvalidSnapshot)
        return InvalidSnapshot;

    /* Static snapshot?  Create a persistent copy */
    snap = snapshot->copied ? snapshot : CopySnapshot(snapshot);

    /* and tell resowner.c about it */
    ResourceOwnerEnlargeSnapshots(owner);
    snap->regd_count++;
    ResourceOwnerRememberSnapshot(owner, snap);

    u_sess->utils_cxt.RegisteredSnapshots++;

    return snap;
}

/*
 * UnregisterSnapshot
 *
 * Decrement the reference count of a snapshot, remove the corresponding
 * reference from CurrentResourceOwner, and free the snapshot if no more
 * references remain.
 */
void UnregisterSnapshot(Snapshot snapshot)
{
    if (snapshot == NULL)
        return;

    UnregisterSnapshotFromOwner(snapshot, t_thrd.utils_cxt.CurrentResourceOwner);
}

/*
 * UnregisterSnapshotFromOwner
 *		As above, but use the specified resource owner
 */
void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner)
{
    if (snapshot == NULL)
        return;

    if (ResourceOwnerForgetSnapshot(owner, snapshot, true)) {
        Assert(u_sess->utils_cxt.RegisteredSnapshots > 0);
        Assert(snapshot->regd_count > 0);
        u_sess->utils_cxt.RegisteredSnapshots--;
        if (--snapshot->regd_count == 0 && snapshot->active_count == 0) {
            FreeSnapshot(snapshot);
            SnapshotResetXmin();
        }
    }
}

void RegisterStreamSnapshots()
{
    RegisterStreamSnapshot = true;
}

void ForgetRegisterStreamSnapshots()
{
    RegisterStreamSnapshot = false;
}

void UnRegisterStreamSnapshots()
{
    ForgetRegisterStreamSnapshots();
    SnapshotResetXmin();
}

/*
 * SnapshotResetXmin
 *
 * If there are no more snapshots, we can reset our PGXACT->xmin to InvalidXid.
 * Note we can do this without locking because we assume that storing an Xid
 * is atomic.
 */
static void SnapshotResetXmin(void)
{
    if (u_sess->utils_cxt.RegisteredSnapshots == 0 && u_sess->utils_cxt.ActiveSnapshot == NULL) {
        t_thrd.pgxact->xmin = InvalidTransactionId;
        t_thrd.proc->snapXmax = InvalidTransactionId;
        t_thrd.proc->snapCSN = InvalidCommitSeqNo;
        t_thrd.pgxact->csn_min = InvalidCommitSeqNo;
        t_thrd.pgxact->csn_dr = InvalidCommitSeqNo;

        t_thrd.proc->exrto_read_lsn = 0;
        t_thrd.proc->exrto_gen_snap_time = 0;
    }
}

/*
 * AtSubCommit_Snapshot
 */
void AtSubCommit_Snapshot(int level)
{
    ActiveSnapshotElt* active = NULL;

    /*
     * Relabel the active snapshots set in this subtransaction as though they
     * are owned by the parent subxact.
     */
    for (active = u_sess->utils_cxt.ActiveSnapshot; active != NULL; active = active->as_next) {
        if (active->as_level < level)
            break;
        active->as_level = level - 1;
    }
}

/*
 * AtSubAbort_Snapshot
 *		Clean up snapshots after a subtransaction abort
 */
void AtSubAbort_Snapshot(int level)
{
    /* Forget the active snapshots set by this subtransaction */
    while (u_sess->utils_cxt.ActiveSnapshot && u_sess->utils_cxt.ActiveSnapshot->as_level >= level) {
        ActiveSnapshotElt* next = NULL;

        next = u_sess->utils_cxt.ActiveSnapshot->as_next;

        /*
         * Decrement the snapshot's active count.  If it's still registered or
         * marked as active by an outer subtransaction, we can't free it yet.
         */
        Assert(u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count >= 1);
        u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count -= 1;

        if (u_sess->utils_cxt.ActiveSnapshot->as_snap->active_count == 0 &&
            u_sess->utils_cxt.ActiveSnapshot->as_snap->regd_count == 0)
            FreeSnapshot(u_sess->utils_cxt.ActiveSnapshot->as_snap);

        /* and free the stack element */
        pfree_ext(u_sess->utils_cxt.ActiveSnapshot);

        u_sess->utils_cxt.ActiveSnapshot = next;
    }

    SnapshotResetXmin();
}

static void free_snapshot_prepared_array(void)
{
    if (!GTM_LITE_MODE) {
        return;
    }
    int def_prep_array_num = get_snapshot_defualt_prepared_num();
    if (u_sess->utils_cxt.CurrentSnapshotData->prepared_array_capacity > def_prep_array_num) {
        pfree_ext(u_sess->utils_cxt.CurrentSnapshotData->prepared_array);
        u_sess->utils_cxt.CurrentSnapshotData->prepared_array_capacity = 0;
        u_sess->utils_cxt.CurrentSnapshotData->prepared_count = 0;
    }

    if (u_sess->utils_cxt.SecondarySnapshotData->prepared_array_capacity > def_prep_array_num) {
        pfree_ext(u_sess->utils_cxt.SecondarySnapshotData->prepared_array);
        u_sess->utils_cxt.SecondarySnapshotData->prepared_array_capacity = 0;
        u_sess->utils_cxt.SecondarySnapshotData->prepared_count = 0;
    }
}
/*
 * AtEOXact_Snapshot
 *		Snapshot manager's cleanup function for end of transaction
 */
void AtEOXact_Snapshot(bool isCommit)
{
    /*
     * In transaction-snapshot mode we must release our privately-managed
     * reference to the transaction snapshot.  We must decrement
     * RegisteredSnapshots to keep the check below happy.  But we don't bother
     * to do FreeSnapshot, for two reasons: the memory will go away with
     * u_sess->top_transaction_mem_cxt anyway, and if someone has left the snapshot
     * stacked as active, we don't want the code below to be chasing through a
     * dangling pointer.
     */
    if (u_sess->utils_cxt.FirstXactSnapshot != NULL) {
        Assert(u_sess->utils_cxt.FirstXactSnapshot->regd_count > 0);
        Assert(u_sess->utils_cxt.RegisteredSnapshots > 0);
        u_sess->utils_cxt.RegisteredSnapshots--;
    }
    u_sess->utils_cxt.FirstXactSnapshot = NULL;

    /*
     * If we exported any snapshots, clean them up.
     */
    if (u_sess->utils_cxt.exportedSnapshots != NIL) {
        ListCell   *lc;
        /*
         * Get rid of the files.  Unlink failure is only a WARNING because (1)
         * it's too late to abort the transaction, and (2) leaving a leaked
         * file around has little real consequence anyway.
         *
         * We also also need to remove the snapshots from RegisteredSnapshots
         * to prevent a warning below.
         *
         * As with the FirstXactSnapshot, we don't need to free resources of
         * the snapshot iself as it will go away with the memory context.
         */
        foreach(lc, u_sess->utils_cxt.exportedSnapshots) {
            ExportedSnapshot *esnap = (ExportedSnapshot *)lfirst(lc);

            if (unlink(esnap->snapfile))
                ereport(WARNING, (errmsg("could not unlink file \"%s\": %m", esnap->snapfile)));
        }

        /*
         * Note: you might be thinking "why do we have the exportedSnapshots
         * list at all?  All we need is a counter!".  You're right, but we do
         * it this way in case we ever feel like improving xmin management.
         */
        Assert(u_sess->utils_cxt.RegisteredSnapshots >= list_length(u_sess->utils_cxt.exportedSnapshots));
        u_sess->utils_cxt.RegisteredSnapshots -= list_length(u_sess->utils_cxt.exportedSnapshots);

        u_sess->utils_cxt.exportedSnapshots = NIL;
    }

    /* On commit, complain about leftover snapshots */
    if (isCommit) {
        ActiveSnapshotElt* active = NULL;

        if (u_sess->utils_cxt.RegisteredSnapshots != 0)
            ereport(WARNING,
                (errmsg(
                    "%d registered snapshots seem to remain after cleanup", u_sess->utils_cxt.RegisteredSnapshots)));

        /* complain about unpopped active snapshots */
        for (active = u_sess->utils_cxt.ActiveSnapshot; active != NULL; active = active->as_next)
            ereport(WARNING, (errmsg("snapshot still active")));
    }

    /* free static snapshotData prepared array memory in GTM Lite if needed */
    free_snapshot_prepared_array();

    /*
     * And reset our state.  We don't need to free the memory explicitly --
     * it'll go away with u_sess->top_transaction_mem_cxt.
     */
    u_sess->utils_cxt.ActiveSnapshot = NULL;
    u_sess->utils_cxt.RegisteredSnapshots = 0;

    Assert(!(u_sess->utils_cxt.CurrentSnapshot != NULL && u_sess->utils_cxt.CurrentSnapshot->user_data != NULL));
    Assert(!(u_sess->utils_cxt.SecondarySnapshot != NULL && u_sess->utils_cxt.SecondarySnapshot->user_data != NULL));

    u_sess->utils_cxt.CurrentSnapshot = NULL;
    u_sess->utils_cxt.SecondarySnapshot = NULL;

    u_sess->utils_cxt.FirstSnapshotSet = false;

    SnapshotResetXmin();
    t_thrd.proc->exrto_min = InvalidXLogRecPtr;
    if (IS_EXRTO_STANDBY_READ && t_thrd.proc->exrto_reload_cache) {
        t_thrd.proc->exrto_reload_cache = false;
        reset_invalidation_cache();
    }
}

/*
 * ExportSnapshot
 *		Export the snapshot to a file so that other backends can import it.
 *		Returns the token (the file name) that can be used to import this
 *		snapshot.
 */
char* ExportSnapshot(Snapshot snapshot, CommitSeqNo *snapshotCsn)
{
    TransactionId topXid;
    TransactionId* children = NULL;
    ExportedSnapshot *esnap;
    int nchildren;
    int addTopXid;
    StringInfoData buf;
    FILE* f = NULL;
    int i;
    MemoryContext oldcxt;
    char path[MAXPGPATH];
    char pathtmp[MAXPGPATH];
    int rc;

    /*
     * It's tempting to call RequireTransactionChain here, since it's not very
     * useful to export a snapshot that will disappear immediately afterwards.
     * However, we haven't got enough information to do that, since we don't
     * know if we're at top level or not.  For example, we could be inside a
     * plpgsql function that is going to fire off other transactions via
     * dblink.	Rather than disallow perfectly legitimate usages, don't make a
     * check.
     *
     * Also note that we don't make any restriction on the transaction's
     * isolation level; however, importers must check the level if they are
     * serializable.
     */

    /*
     * Get our transaction ID if there is one, to include in the snapshot.
     */
    topXid = GetTopTransactionIdIfAny();

    /*
     * We cannot export a snapshot from a subtransaction because there's no
     * easy way for importers to verify that the same subtransaction is still
     * running.
     */
    if (IsSubTransaction())
        ereport(
            ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("cannot export a snapshot from a subtransaction")));

    /*
     * We do however allow previous committed subtransactions to exist.
     * Importers of the snapshot must see them as still running, so get their
     * XIDs to add them to the snapshot.
     */
    nchildren = xactGetCommittedChildren(&children);

    /*
     * Generate file path for the snapshot.  We start numbering of snapshots
     * inside the transaction from 1.
     */
    rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/%08X-%08X-%d", SNAPSHOT_EXPORT_DIR,
        t_thrd.proc->backendId, t_thrd.proc->lxid, list_length(u_sess->utils_cxt.exportedSnapshots) + 1);
    securec_check_ss(rc, "", "");

    /*
     * Copy the snapshot into u_sess->top_transaction_mem_cxt, add it to the
     * exportedSnapshots list, and mark it pseudo-registered.  We do this to
     * ensure that the snapshot's xmin is honored for the rest of the
     * transaction.  (Right now, because SnapshotResetXmin is so stupid, this
     * is overkill; but later we might make that routine smarter.)
     */
    snapshot = CopySnapshot(snapshot);

    oldcxt = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
    esnap = (ExportedSnapshot *) palloc(sizeof(ExportedSnapshot));
    esnap->snapfile = pstrdup(path);
    esnap->snapshot = snapshot;
    u_sess->utils_cxt.exportedSnapshots = lappend(u_sess->utils_cxt.exportedSnapshots, esnap);
    (void)MemoryContextSwitchTo(oldcxt);

    snapshot->regd_count++;
    u_sess->utils_cxt.RegisteredSnapshots++;

    /*
     * Fill buf with a text serialization of the snapshot, plus identification
     * data about this transaction.  The format expected by ImportSnapshot is
     * pretty rigid: each line must be fieldname:value.
     */
    initStringInfo(&buf);

    appendStringInfo(&buf, "vxid: %d/" XID_FMT "\n", t_thrd.proc->backendId, t_thrd.proc->lxid);
    appendStringInfo(&buf, "pid:%lu\n", t_thrd.proc_cxt.MyProcPid);
    appendStringInfo(&buf, "dbid:%u\n", u_sess->proc_cxt.MyDatabaseId);
    appendStringInfo(&buf, "iso:%d\n", u_sess->utils_cxt.XactIsoLevel);
    appendStringInfo(&buf, "ro:%d\n", u_sess->attr.attr_common.XactReadOnly);

    appendStringInfo(&buf, "xmin:" XID_FMT "\n", snapshot->xmin);
    appendStringInfo(&buf, "xmax:" XID_FMT "\n", snapshot->xmax);

    appendStringInfo(&buf, "timeline:%u\n", snapshot->timeline);

    /*
     * We must include our own top transaction ID in the top-xid data, since
     * by definition we will still be running when the importing transaction
     * adopts the snapshot, but GetSnapshotData never includes our own XID in
     * the snapshot.  (There must, therefore, be enough room to add it.)
     *
     * However, it could be that our topXid is after the xmax, in which case
     * we shouldn't include it because xip[] members are expected to be before
     * xmax.  (We need not make the same check for subxip[] members, see
     * snapshot.h.)
     */
    addTopXid = (TransactionIdIsValid(topXid) && TransactionIdPrecedes(topXid, snapshot->xmax)) ? 1 : 0;
    if (addTopXid)
        appendStringInfo(&buf, "xip:" XID_FMT "\n", topXid);

    appendStringInfo(&buf, "snapshotcsn:" XID_FMT "\n", snapshot->snapshotcsn);
    if (snapshotCsn) {
        *snapshotCsn = snapshot->snapshotcsn;
    }

    /*
     * Similarly, we add our subcommitted child XIDs to the subxid data. Here,
     * we have to cope with possible overflow.
     */
    if (nchildren > GetMaxSnapshotSubxidCount())
        appendStringInfoString(&buf, "sof:1\n");
    else {
        appendStringInfoString(&buf, "sof:0\n");

        for (i = 0; i < nchildren; i++)
            appendStringInfo(&buf, "sxp:" XID_FMT "\n", children[i]);
    }
    appendStringInfo(&buf, "rec:%u\n", snapshot->takenDuringRecovery);

    /*
     * Now write the text representation into a file.  We first write to a
     * ".tmp" filename, and rename to final filename if no error.  This
     * ensures that no other backend can read an incomplete file
     * (ImportSnapshot won't allow it because of its valid-characters check).
     */
    rc = snprintf_s(pathtmp, sizeof(pathtmp), sizeof(pathtmp) - 1, "%s.tmp", path);
    securec_check_ss(rc, "", "");
    if (!(f = AllocateFile(pathtmp, PG_BINARY_W)))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", pathtmp)));

    if (fwrite(buf.data, buf.len, 1, f) != 1)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", pathtmp)));

    /* no fsync() since file need not survive a system crash */

    if (FreeFile(f))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", pathtmp)));

    /*
     * Now that we have written everything into a .tmp file, rename the file
     * to remove the .tmp suffix.
     */
    if (rename(pathtmp, path) < 0)
        ereport(
            ERROR, (errcode_for_file_access(), errmsg("could not rename file \"%s\" to \"%s\": %m", pathtmp, path)));

    /*
     * The basename of the file is what we return from pg_export_snapshot().
     * It's already in path in a textual format and we know that the path
     * starts with SNAPSHOT_EXPORT_DIR.  Skip over the prefix and the slash
     * and pstrdup it so as not to return the address of a local variable.
     */
    return pstrdup(path + strlen(SNAPSHOT_EXPORT_DIR) + 1);
}

/*
 * pg_export_snapshot
 *		SQL-callable wrapper for ExportSnapshot.
 */
Datum pg_export_snapshot(PG_FUNCTION_ARGS)
{
    char* snapshotName = NULL;

    snapshotName = ExportSnapshot(GetActiveSnapshot(), NULL);
    PG_RETURN_TEXT_P(cstring_to_text(snapshotName));
}

/*
 * pg_export_snapshot_and_csn
 *		SQL-callable wrapper for ExportSnapshot.
 *		Return snapshot name and csn.
 */
Datum pg_export_snapshot_and_csn(PG_FUNCTION_ARGS)
{
    char *snapshotName = NULL;

    Datum           values[2];
    bool            nulls[2] = {0};
    TupleDesc       tupdesc = NULL;
    HeapTuple       tuple = NULL;
    Datum           result;
    CommitSeqNo snapshotcsn = InvalidCommitSeqNo;
    char strCSN[MAX_ULONG_LENGTH] = {0};
    errno_t         errorno = EOK;

    /*
     * Construct a tuple descriptor for the result row.  This must match this
     * function's pg_proc entry!
     */
    tupdesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "snapshot_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "CSN", TEXTOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    snapshotName = ExportSnapshot(GetActiveSnapshot(), &snapshotcsn);
    errorno = snprintf_s(strCSN, MAX_ULONG_LENGTH, MAX_ULONG_LENGTH - 1, "%X", snapshotcsn);
    securec_check_ss(errorno, "", "");
    values[0] = CStringGetTextDatum(snapshotName);
    values[1] = CStringGetTextDatum(strCSN);
    nulls[0] = false;
    nulls[1] = false;

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    PG_RETURN_DATUM(result);
}

/*
 * Parsing subroutines for ImportSnapshot: parse a line with the given
 * prefix followed by a value, and advance *s to the next line.  The
 * filename is provided for use in error messages.
 */
static int parseIntFromText(const char* prefix, char** s, const char* filename)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    int val;

    if (strncmp(ptr, prefix, prefixlen) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr += prefixlen;
    if (sscanf_s(ptr, "%d", &val) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr = strchr(ptr, '\n');
    if (ptr == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    *s = ptr + 1;
    return val;
}

static TransactionId parseXidFromText(const char* prefix, char** s, const char* filename)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    TransactionId val;

    if (strncmp(ptr, prefix, prefixlen) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr += prefixlen;
    if (sscanf_s(ptr, XID_FMT, &val) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr = strchr(ptr, '\n');
    if (ptr == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    *s = ptr + 1;
    return val;
}

static GTM_Timeline parseTimelineFromText(const char* prefix, char** s, const char* filename)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    GTM_Timeline val;

    if (strncmp(ptr, prefix, prefixlen) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr += prefixlen;
    if (sscanf_s(ptr, "%u", &val) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr = strchr(ptr, '\n');
    if (ptr == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    *s = ptr + 1;
    return val;
}

static void parseVxidFromText(const char *prefix, char **s, const char *filename, VirtualTransactionId *vxid)
{
    char *ptr = *s;
    int prefixlen = strlen(prefix);

    if (strncmp(ptr, prefix, prefixlen) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr += prefixlen;
    if (sscanf_s(ptr, "%d/%u", &vxid->backendId, &vxid->localTransactionId) != 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    ptr = strchr(ptr, '\n');
    if (!ptr)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", filename)));
    *s = ptr + 1;
}

/*
 * ImportSnapshot
 *		Import a previously exported snapshot.	The argument should be a
 *		filename in SNAPSHOT_EXPORT_DIR.  Load the snapshot from that file.
 *		This is called by "SET TRANSACTION SNAPSHOT 'foo'".
 */
void ImportSnapshot(const char* idstr)
{
    char path[MAXPGPATH];
    FILE* f = NULL;
    struct stat stat_buf;
    char* filebuf = NULL;
    VirtualTransactionId src_vxid;
    ThreadId src_pid;
    Oid src_dbid;
    int src_isolevel;
    bool src_readonly = false;
    SnapshotData snapshot;

    /*
     * Must be at top level of a fresh transaction.  Note in particular that
     * we check we haven't acquired an XID --- if we have, it's conceivable
     * that the snapshot would show it as not running, making for very screwy
     * behavior.
     */
    if (u_sess->utils_cxt.FirstSnapshotSet || GetTopTransactionIdIfAny() != InvalidTransactionId || IsSubTransaction())
        ereport(ERROR,
            (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                errmsg("SET TRANSACTION SNAPSHOT must be called before any query")));

    /*
     * If we are in read committed mode then the next query would execute with
     * a new snapshot thus making this function call quite useless.
     */
    if (!IsolationUsesXactSnapshot())
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ")));

    /*
     * Verify the identifier: only 0-9, A-F and hyphens are allowed.  We do
     * this mainly to prevent reading arbitrary files.
     */
    if (strspn(idstr, "0123456789ABCDEF-") != strlen(idstr))
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid snapshot identifier: \"%s\"", idstr)));

    /* OK, read the file */
    int rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", SNAPSHOT_EXPORT_DIR, idstr);
    securec_check_ss(rc, "", "");

    f = AllocateFile(path, PG_BINARY_R);
    if (f == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid snapshot identifier: \"%s\"", idstr)));

    /* get the size of the file so that we know how much memory we need */
    if (fstat(fileno(f), &stat_buf))
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("could not stat file \"%s\": %m", path)));

    /* and read the file into a palloc'd string */
    filebuf = (char*)palloc(stat_buf.st_size + 1);
    if (fread(filebuf, stat_buf.st_size, 1, f) != 1)
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("could not read file \"%s\": %m", path)));

    filebuf[stat_buf.st_size] = '\0';

    (void)FreeFile(f);

    /*
     * Construct a snapshot struct by parsing the file content.
     */
    rc = memset_s(&snapshot, sizeof(SnapshotData), 0, sizeof(snapshot));
    securec_check(rc, "", "");

    parseVxidFromText("vxid:", &filebuf, path, &src_vxid);
    src_pid = parseIntFromText("pid:", &filebuf, path);
    /* we abuse parseXidFromText a bit here ... */
    src_dbid = parseXidFromText("dbid:", &filebuf, path);
    src_isolevel = parseIntFromText("iso:", &filebuf, path);
    src_readonly = parseIntFromText("ro:", &filebuf, path);

    snapshot.xmin = parseXidFromText("xmin:", &filebuf, path);
    snapshot.xmax = parseXidFromText("xmax:", &filebuf, path);
    /* should we use a new parsefunction for timeline */
    snapshot.timeline = parseTimelineFromText("timeline:", &filebuf, path);
    snapshot.snapshotcsn = parseXidFromText("snapshotcsn:", &filebuf, path);
    parseIntFromText("sof:", &filebuf, path);
    snapshot.takenDuringRecovery = parseIntFromText("rec:", &filebuf, path);

    /*
     * Do some additional sanity checking, just to protect ourselves.  We
     * don't trouble to check the array elements, just the most critical
     * fields.
     */
    if (!VirtualTransactionIdIsValid(src_vxid) || !OidIsValid(src_dbid) || !TransactionIdIsNormal(snapshot.xmin) ||
        !TransactionIdIsNormal(snapshot.xmax))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid snapshot data in file \"%s\"", path)));

    /*
     * If we're serializable, the source transaction must be too, otherwise
     * predicate.c has problems (SxactGlobalXmin could go backwards).  Also, a
     * non-read-only transaction can't adopt a snapshot from a read-only
     * transaction, as predicate.c handles the cases very differently.
     */
    if (IsolationIsSerializable()) {
        if (src_isolevel != XACT_SERIALIZABLE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("a serializable transaction cannot import a snapshot from a non-serializable transaction")));
        if (src_readonly && !u_sess->attr.attr_common.XactReadOnly)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("a non-read-only serializable transaction cannot import a snapshot from a read-only "
                           "transaction")));
    }

    /*
     * We cannot import a snapshot that was taken in a different database,
     * because vacuum calculates OldestXmin on a per-database basis; so the
     * source transaction's xmin doesn't protect us from data loss.  This
     * restriction could be removed if the source transaction were to mark its
     * xmin as being globally applicable.  But that would require some
     * additional syntax, since that has to be known when the snapshot is
     * initially taken.  (See pgsql-hackers discussion of 2011-10-21.)
     */
    if (src_dbid != u_sess->proc_cxt.MyDatabaseId)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot import a snapshot from a different database")));

    /* OK, install the snapshot */
    SetTransactionSnapshot(&snapshot, &src_vxid, src_pid);
}

/*
 * XactHasExportedSnapshots
 *		Test whether current transaction has exported any snapshots.
 */
bool XactHasExportedSnapshots(void)
{
    return (u_sess->utils_cxt.exportedSnapshots != NIL);
}

/*
 * DeleteAllExportedSnapshotFiles
 *		Clean up any files that have been left behind by a crashed backend
 *		that had exported snapshots before it died.
 *
 * This should be called during database startup or crash recovery.
 */
void DeleteAllExportedSnapshotFiles(void)
{
    char buf[MAXPGPATH];
    DIR* s_dir = NULL;
    struct dirent* s_de = NULL;
    int rc = 0;

    if (!(s_dir = AllocateDir(SNAPSHOT_EXPORT_DIR))) {
        /*
         * We really should have that directory in a sane cluster setup. But
         * then again if we don't, it's not fatal enough to make it FATAL.
         * Since we're running in the postmaster, LOG is our best bet.
         */
        ereport(LOG, (errmsg("could not open directory \"%s\": %m", SNAPSHOT_EXPORT_DIR)));
        return;
    }

    while ((s_de = ReadDir(s_dir, SNAPSHOT_EXPORT_DIR)) != NULL) {
        if (strcmp(s_de->d_name, ".") == 0 || strcmp(s_de->d_name, "..") == 0)
            continue;

        rc = snprintf_s(buf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", SNAPSHOT_EXPORT_DIR, s_de->d_name);
        securec_check_ss(rc, "", "");
        /* Again, unlink failure is not worthy of FATAL */
        if (unlink(buf))
            ereport(LOG, (errmsg("could not unlink file \"%s\": %m", buf)));
    }

    FreeDir(s_dir);
}

void StreamTxnContextSaveSnapmgr(void* stc)
{
    STCSaveElem(((StreamTxnContext*)stc)->RecentGlobalXmin, u_sess->utils_cxt.RecentGlobalXmin);
    STCSaveElem(((StreamTxnContext*)stc)->TransactionXmin, u_sess->utils_cxt.TransactionXmin);
    STCSaveElem(((StreamTxnContext*)stc)->RecentXmin, u_sess->utils_cxt.RecentXmin);
}

void StreamTxnContextRestoreSnapmgr(const void* stc)
{
    STCRestoreElem(((StreamTxnContext*)stc)->RecentGlobalXmin, u_sess->utils_cxt.RecentGlobalXmin);
    STCRestoreElem(((StreamTxnContext*)stc)->TransactionXmin, u_sess->utils_cxt.TransactionXmin);
    STCRestoreElem(((StreamTxnContext*)stc)->RecentXmin, u_sess->utils_cxt.RecentXmin);
}

bool ThereAreNoPriorRegisteredSnapshots(void)
{
    if (u_sess->utils_cxt.RegisteredSnapshots <= 1)
        return true;

    return false;
}

Snapshot PgFdwCopySnapshot(Snapshot snapshot)
{
    return CopySnapshot(snapshot);
}

/*
 * Setup a snapshot that replaces normal catalog snapshots that allows catalog
 * access to behave just like it did at a certain point in the past.
 *
 * Needed for logical decoding.
 */
void SetupHistoricSnapshot(Snapshot historic_snapshot, HTAB* tuplecids)
{
    Assert(historic_snapshot != NULL);

    /* setup the timetravel snapshot */
    u_sess->utils_cxt.HistoricSnapshot = historic_snapshot;

    /* setup (cmin, cmax) lookup hash */
    u_sess->utils_cxt.tuplecid_data = tuplecids;
}

/*
 * Make catalog snapshots behave normally again.
 */
void TeardownHistoricSnapshot(bool is_error)
{
    u_sess->utils_cxt.HistoricSnapshot = NULL;
    u_sess->utils_cxt.tuplecid_data = NULL;
}

bool HistoricSnapshotActive(void)
{
    return u_sess->utils_cxt.HistoricSnapshot != NULL;
}

HTAB* HistoricSnapshotGetTupleCids(void)
{
    Assert(HistoricSnapshotActive());
    return u_sess->utils_cxt.tuplecid_data;
}
