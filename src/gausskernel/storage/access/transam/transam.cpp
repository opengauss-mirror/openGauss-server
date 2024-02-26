/* -------------------------------------------------------------------------
 *
 * transam.cpp
 *	  openGauss transaction log interface routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/transam/transam.cpp
 *
 * NOTES
 *	  This file contains the high level access-method interface to the
 *	  transaction system.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/gtm.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/slru.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "replication/walreceiver.h"
#include "storage/procarray.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_common_attr.h"

#ifdef PGXC
#include "utils/builtins.h"
#endif

static CLogXidStatus TransactionLogFetch(TransactionId transactionId);

#ifdef PGXC
/* It is not really necessary to make it appear in header file */
Datum pgxc_is_committed(PG_FUNCTION_ARGS);
Datum pgxc_get_csn(PG_FUNCTION_ARGS);

#endif

/* ----------------------------------------------------------------
 *		openGauss log access method interface
 *
 *		TransactionLogFetch
 * ----------------------------------------------------------------
 */
void SetLatestFetchState(TransactionId transactionId, CommitSeqNo result)
{
    t_thrd.xact_cxt.latestFetchCSNXid = transactionId;
    t_thrd.xact_cxt.latestFetchCSN = result;
}

static CommitSeqNo GetCSNByCLog(TransactionId transactionId, bool isCommit)
{
    XLogRecPtr lsn;
    if (isCommit) {
        return COMMITSEQNO_FROZEN;
    }
    if (CLogGetStatus(transactionId, &lsn) == CLOG_XID_STATUS_COMMITTED) {
        return COMMITSEQNO_FROZEN;
    } else {
        return COMMITSEQNO_ABORTED;
    }
}

static CommitSeqNo FetchCSNFromCache(TransactionId transactionId, Snapshot snapshot, bool isMvcc)
{
    if (!TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchCSNXid)) {
        return InvalidCommitSeqNo;
    }

    if (snapshot == NULL) {
        goto TRY_USE_CACHE;
    }

    if (IsVersionMVCCSnapshot(snapshot) || (snapshot->satisfies == SNAPSHOT_DECODE_MVCC)) {
        return InvalidCommitSeqNo;
    }

    if (!isMvcc || GTM_LITE_MODE) {
        goto TRY_USE_CACHE;
    }

    /* frozen csn is not the true csn, only use frozen csn cache if we know xid is finished in my snapshot */
    if (IsMVCCSnapshot(snapshot) && !TransactionIdPrecedes(transactionId, snapshot->xmin) &&
        t_thrd.xact_cxt.cachedFetchCSN == COMMITSEQNO_FROZEN) {
        return InvalidCommitSeqNo;
    }

TRY_USE_CACHE:
    SetLatestFetchState(t_thrd.xact_cxt.cachedFetchCSNXid, t_thrd.xact_cxt.cachedFetchCSN);
    return t_thrd.xact_cxt.cachedFetchCSN;
}

/*
 * TransactionIdGetCommitSeqNo --- fetch CSN of specified transaction id
 */
CommitSeqNo TransactionIdGetCommitSeqNo(TransactionId transactionId, bool isCommit, bool isMvcc, bool isNest,
    Snapshot snapshot)
{
    CommitSeqNo result;
    TransactionId xid = InvalidTransactionId;
    int retry_times = 0;

    /*
     * Before going to the commit log manager, check our single item cache to
     * see if we didn't just check the transaction status a moment ago.
     */
    CommitSeqNo cacheCsn = FetchCSNFromCache(transactionId, snapshot, isMvcc);
    if (cacheCsn != InvalidCommitSeqNo) {
        return cacheCsn;
    }

    /*
     * Also, check to see if the transaction ID is a permanent one.
     */
    if (!TransactionIdIsNormal(transactionId)) {
        t_thrd.xact_cxt.latestFetchCSNXid = InvalidTransactionId;
        if (TransactionIdEquals(transactionId, BootstrapTransactionId) ||
            TransactionIdEquals(transactionId, FrozenTransactionId)) {
            return COMMITSEQNO_FROZEN;
        }
        return COMMITSEQNO_ABORTED;
    }
MemoryContext old = CurrentMemoryContext;
RETRY:
    /*
     * If the XID is older than RecentGlobalXmin, check the clog. Otherwise
     * check the csnlog.
     */
    if (snapshot != NULL && snapshot->satisfies == SNAPSHOT_DECODE_MVCC) {
        xid = GetReplicationSlotCatalogXmin();
    } else if (!isMvcc || GTM_LITE_MODE) {
        TransactionId recentGlobalXmin = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
        if (!TransactionIdIsValid(recentGlobalXmin)) {
            xid = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
        } else {
            xid = recentGlobalXmin;
        }
    } else if (snapshot != NULL && IsMVCCSnapshot(snapshot)) {
        xid = snapshot->xmin;
    } else {
        xid = u_sess->utils_cxt.RecentXmin;
    }

    Assert(TransactionIdIsValid(xid));
    if ((!IS_MULTI_DISASTER_RECOVER_MODE) && (snapshot == NULL || !IsVersionMVCCSnapshot(snapshot)) &&
        TransactionIdPrecedes(transactionId, xid)) {
        result = GetCSNByCLog(transactionId, isCommit);
    } else {
        uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
        /*
         * In gtm-lite mode, recentGlobalXmin acrroding to oldest csn, it can be updated when we read csn log.
         * Make sure that we restore the right hold interrupt count and release
         * all lock about csn log(control lock and page lock).
         * just retry one times, if we try to read csn log again, meaning any other problem happen, ereport as usual.
         */
        PG_TRY();
        {
            result = isNest ? CSNLogGetNestCommitSeqNo(transactionId) : CSNLogGetCommitSeqNo(transactionId);
        }
        PG_CATCH();
        {
            if ((IS_CN_DISASTER_RECOVER_MODE || IS_MULTI_DISASTER_RECOVER_MODE) &&
                t_thrd.xact_cxt.slru_errcause == SLRU_OPEN_FAILED)
            {
                (void)MemoryContextSwitchTo(old);
                FlushErrorState();
                t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
                result = GetCSNByCLog(transactionId, isCommit);
                ereport(LOG,
                        (errmsg("TransactionIdGetCommitSeqNo: "
                                "Treat CSN as frozen when csnlog file cannot be found for the given xid: %lu csn: %lu",
                                transactionId, result)));
            } else if ((GTM_LITE_MODE || (ENABLE_DMS && t_thrd.role == DMS_WORKER)) && retry_times == 0) {
                t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
                FlushErrorState();
                ereport(LOG, (errmsg("recentGlobalXmin has been updated, csn log may be truncated, try clog, xid"
                                     " %lu recentLocalXmin %lu.",
                                     xid, t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin)));
                retry_times++;
                (void)MemoryContextSwitchTo(old);
                goto RETRY;
            } else {
                PG_RE_THROW();
            }
        }
        PG_END_TRY();
    }
    if (SHOW_DEBUG_MESSAGE()) {
        ereport(DEBUG1,
                (errmsg("Get CSN xid %lu cur_xid %lu xid %lu result %lu iscommit %d, recentLocalXmin %lu, isMvcc :%d, "
                        "RecentXmin: %lu",
                        transactionId, GetCurrentTransactionIdIfAny(), xid, result, isCommit,
                        t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin, isMvcc, u_sess->utils_cxt.RecentXmin)));
    }

    /*
     * Cache it, but DO NOT cache status for unfinished transactions!
     * We only cache status that is guaranteed not to change.
     */
    if (COMMITSEQNO_IS_COMMITTED(result) || COMMITSEQNO_IS_ABORTED(result)) {
        t_thrd.xact_cxt.cachedFetchCSNXid = transactionId;
        t_thrd.xact_cxt.cachedFetchCSN = result;
    }

    t_thrd.xact_cxt.latestFetchCSNXid = transactionId;
    t_thrd.xact_cxt.latestFetchCSN = result;

    return result;
}

/*
 * TransactionIdGetCommitSeqNo --- fetch CSN of specified transaction id for gs_clean only
 */
static CommitSeqNo TransactionIdGetCommitSeqNoForGSClean(TransactionId transactionId)
{
    CommitSeqNo result;
    TransactionId xid;
    bool miss_global_xmin = false;

    /*
     * If the XID is older than RecentGlobalXmin, check the clog. Otherwise
     * check the csnlog.
     */
    Assert(TransactionIdIsNormal(transactionId));
    xid = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
    if (!TransactionIdIsValid(xid)) {
        /* Fetch the newest global xmin from gtm and use it. */
        TransactionId gtm_gloabl_xmin = InvalidTransactionId;

        if (TransactionIdIsValid(gtm_gloabl_xmin)) {
            (void)pg_atomic_compare_exchange_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin, &xid,
                                                 gtm_gloabl_xmin);
            xid = gtm_gloabl_xmin;
        } else {
            /*
             * When fail to get the newest global xmin from gtm, just use the
             * local xmin, gtm_free and gtm_lite mode and gtm fail case fall here.
             */
            xid = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
            ereport(DEBUG1, (errmsg("For gs_clean, fetch the recent global xmin %lu from recent local xmin.", xid)));
        }
    }

    Assert(TransactionIdIsValid(xid));
    if (TransactionIdPrecedes(transactionId, xid)) {
        if (miss_global_xmin) {
            /*
             * when we have no global xmin, we can not return 2 in gtm cluster
             * for gs_clean may use 2 to commit other prepare transactions
             * and there may be a old query is still running on that node,
             * which will cause inconsistency.
             */
            result = COMMITSEQNO_COMMIT_INPROGRESS;
        } else {
            result = COMMITSEQNO_FROZEN;
        }
    } else {
        result = CSNLogGetNestCommitSeqNo(transactionId);
    }

    return result;
}

/*
 * TransactionLogFetch --- fetch commit status of specified transaction id
 */
static CLogXidStatus TransactionLogFetch(TransactionId transactionId)
{
    CLogXidStatus xidstatus;
    XLogRecPtr xidlsn;

    /*
     * Before going to the commit log manager, check our single item cache to
     * see if we didn't just check the transaction status a moment ago.
     */
    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchXid)) {
        t_thrd.xact_cxt.latestFetchXid = t_thrd.xact_cxt.cachedFetchXid;
        t_thrd.xact_cxt.latestFetchXidStatus = t_thrd.xact_cxt.cachedFetchXidStatus;
        return t_thrd.xact_cxt.cachedFetchXidStatus;
    }

    /*
     * Also, check to see if the transaction ID is a permanent one.
     */
    if (!TransactionIdIsNormal(transactionId)) {
        t_thrd.xact_cxt.latestFetchXid = InvalidTransactionId;
        if (TransactionIdEquals(transactionId, BootstrapTransactionId))
            return CLOG_XID_STATUS_COMMITTED;
        if (TransactionIdEquals(transactionId, FrozenTransactionId))
            return CLOG_XID_STATUS_COMMITTED;
        return CLOG_XID_STATUS_ABORTED;
    }

    /*
     * Get the transaction status.
     */
    xidstatus = CLogGetStatus(transactionId, &xidlsn);
    /*
     * Cache it, but DO NOT cache status for unfinished or sub-committed
     * transactions!  We only cache status that is guaranteed not to change.
     */
    if (xidstatus != CLOG_XID_STATUS_IN_PROGRESS && xidstatus != CLOG_XID_STATUS_SUB_COMMITTED) {
        t_thrd.xact_cxt.cachedFetchXid = transactionId;
        t_thrd.xact_cxt.cachedFetchXidStatus = xidstatus;
        t_thrd.xact_cxt.cachedCommitLSN = xidlsn;
    }

    t_thrd.xact_cxt.latestFetchXid = transactionId;
    t_thrd.xact_cxt.latestFetchXidStatus = xidstatus;

    return xidstatus;
}

#ifdef PGXC
/*
 * For given Transaction ID, check if transaction is committed or aborted
 */
Datum pgxc_is_committed(PG_FUNCTION_ARGS)
{
    TransactionId tid = (TransactionId)PG_GETARG_TRANSACTIONID(0);
    CLogXidStatus xidstatus;

    if (TransactionIdFollows(tid, t_thrd.xact_cxt.ShmemVariableCache->nextXid))
        PG_RETURN_NULL();

    xidstatus = TransactionLogFetch(tid);
    if (xidstatus == CLOG_XID_STATUS_COMMITTED)
        PG_RETURN_BOOL(true);
    else if (xidstatus == CLOG_XID_STATUS_ABORTED)
        PG_RETURN_BOOL(false);
    else
        PG_RETURN_NULL();
}

/*
 * For given Transaction ID, return CSN value
 */
Datum pgxc_get_csn(PG_FUNCTION_ARGS)
{
    TransactionId tid = (TransactionId)PG_GETARG_TRANSACTIONID(0);
    CommitSeqNo xidCsn = InvalidCommitSeqNo;

    if (!TransactionIdFollows(tid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        if (u_sess->attr.attr_common.xc_maintenance_mode)
            xidCsn = TransactionIdGetCommitSeqNoForGSClean(tid);
        else
            xidCsn = TransactionIdGetCommitSeqNo(tid, false, false, true, NULL);
    }

    PG_RETURN_INT64(xidCsn);
}

#endif

/* ----------------------------------------------------------------
 *						Interface functions
 *
 *		TransactionIdDidCommit
 *		TransactionIdDidAbort
 *		========
 *		   these functions test the transaction status of
 *		   a specified transaction id.
 *
 *		TransactionIdCommitTree
 *		TransactionIdAsyncCommitTree
 *		TransactionIdAbortTree
 *		========
 *		   these functions set the transaction status of the specified
 *		   transaction tree.
 *
 * See also TransactionIdIsInProgress, which once was in this module
 * but now lives in procarray.c.
 * ----------------------------------------------------------------
 *
 *
 * TransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool TransactionIdDidCommit(TransactionId transactionId) /* true if given transaction committed */
{
    if (ENABLE_DMS) {
        /* fetch TXN info locally if either reformer, original primary, or normal primary */
        bool local_fetch = SSCanFetchLocalSnapshotTxnRelatedInfo();
        if (!local_fetch) {
            bool didCommit;
            SSTransactionIdDidCommit(transactionId, &didCommit);
            return didCommit;
        }
    }

    CLogXidStatus xidstatus;

    xidstatus = TransactionLogFetch(transactionId);
    /*
     * If it's marked committed, it's committed.
     */
    if (xidstatus == CLOG_XID_STATUS_COMMITTED)
        return true;

    /*
     * If it's marked subcommitted, we have to check the parent recursively.
     * However, if it's older than TransactionXmin, we can't look at
     * pg_subtrans; instead assume that the parent crashed without cleaning up
     * its children.
     *
     * Originally we Assert'ed that the result of SubTransGetParent was not
     * zero. However with the introduction of prepared transactions, there can
     * be a window just after database startup where we do not have complete
     * knowledge in pg_subtrans of the transactions after TransactionXmin.
     * StartupSUBTRANS() has ensured that any missing information will be
     * zeroed.	Since this case should not happen under normal conditions, it
     * seems reasonable to emit a WARNING for it.
     */
    if (xidstatus == CLOG_XID_STATUS_SUB_COMMITTED) {
        TransactionId parentXid = 0;
        CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;

        if (TransactionIdPrecedes(transactionId, u_sess->utils_cxt.TransactionXmin))
            return false;
        parentXid = SubTransGetParent(transactionId, &status, false);
        if (!TransactionIdIsValid(parentXid)) {
            if (status == CLOG_XID_STATUS_COMMITTED)
                return true;
            if (status == CLOG_XID_STATUS_ABORTED)
                return false;

            ereport(WARNING, (errmsg("no pg_subtrans entry for subcommitted XID %lu.", transactionId)));
            return false;
        }
        return TransactionIdDidCommit(parentXid);
    }

    /*
     * It's not committed.
     */
    return false;
}

/*
 * For ustore, clog is truncated based on globalRecycleXid. Therefore, we need to perform a quick check
 * first. If the transaction is smaller than globalRecycleXid, the transaction must be committed.
 *
 *      true iff given transaction committed
 */
bool UHeapTransactionIdDidCommit(TransactionId transactionId)
{
    if (transactionId == FrozenTransactionId) {
        return true;
    }
    if (TransactionIdIsNormal(transactionId) &&
        TransactionIdPrecedes(transactionId, pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid)) &&
        !RecoveryInProgress()) {
        Assert(TransactionIdDidCommit(transactionId));
        return true;
    }
    return TransactionIdDidCommit(transactionId);
}

/*
 * TransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool TransactionIdDidAbort(TransactionId transactionId) /* true if given transaction aborted */
{
    CLogXidStatus xidstatus;

    xidstatus = TransactionLogFetch(transactionId);
    /*
     * If it's marked aborted, it's aborted.
     */
    if (xidstatus == CLOG_XID_STATUS_ABORTED)
        return true;

    /*
     * If it's marked subcommitted, we have to check the parent recursively.
     * However, if it's older than TransactionXmin, we can't look at
     * pg_subtrans; instead assume that the parent crashed without cleaning up
     * its children.
     */
    if (xidstatus == CLOG_XID_STATUS_SUB_COMMITTED) {
        TransactionId parentXid;
        CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;

        if (TransactionIdPrecedes(transactionId, u_sess->utils_cxt.TransactionXmin))
            return true;
        parentXid = SubTransGetParent(transactionId, &status, false);
        if (!TransactionIdIsValid(parentXid)) {
            if (status == CLOG_XID_STATUS_COMMITTED)
                return false;
            if (status == CLOG_XID_STATUS_ABORTED)
                return true;

            /* see notes in TransactionIdDidCommit */
            ereport(WARNING, (errmsg("no pg_subtrans entry for subcommitted XID %lu.", transactionId)));
            return true;
        }
        return TransactionIdDidAbort(parentXid);
    }

    /*
     * It's not aborted.
     */
    return false;
}

/*
 * LatestFetchTransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 *		for subTransactionId will not very good
 */
bool LatestFetchTransactionIdDidAbort(TransactionId transactionId) /* true if given transaction aborted */
{
    CLogXidStatus xidstatus;

    if (!TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid) ||
        TransactionIdPrecedes(transactionId, t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid) ||
        (XACT_READ_UNCOMMITTED == u_sess->utils_cxt.XactIsoLevel || t_thrd.xact_cxt.useLocalSnapshot))
        return true;

    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.latestFetchXid))
        xidstatus = t_thrd.xact_cxt.latestFetchXidStatus;
    else
        return true;

    /*
     * If it's marked aborted, it's aborted.
     */
    if (xidstatus == CLOG_XID_STATUS_ABORTED || xidstatus == CLOG_XID_STATUS_SUB_COMMITTED)
        return true;

    /* If it is marked aborted by csn, it's aborted. */
    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.latestFetchCSNXid) &&
        COMMITSEQNO_IS_ABORTED(t_thrd.xact_cxt.latestFetchCSN))
        return true;

    /*
     * It's not aborted.
     */
    return false;
}

bool LatestFetchCSNDidAbort(TransactionId transactionId) /* true if given transaction aborted */
{
    CommitSeqNo csn;

    if (XACT_READ_UNCOMMITTED == u_sess->utils_cxt.XactIsoLevel || t_thrd.xact_cxt.useLocalSnapshot)
        return true;

    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.latestFetchCSNXid))
        csn = t_thrd.xact_cxt.latestFetchCSN;
    else
        return true;

    /*
     * If it's marked aborted, it's aborted.
     */
    if (COMMITSEQNO_IS_ABORTED(csn))
        return true;
    /*
     * It's not aborted.
     */
    return false;
}

/*
 * TransactionIdIsKnownCompleted
 *		True iff transaction associated with the identifier is currently
 *		known to have either committed or aborted.
 *
 * This does NOT look into pg_clog but merely probes our local cache
 * (and so it's not named TransactionIdDidComplete, which would be the
 * appropriate name for a function that worked that way).  The intended
 * use is just to short-circuit TransactionIdIsInProgress calls when doing
 * repeated heapam_visibility.c checks for the same XID.  If this isn't 
 * extremely fast then it will be counterproductive.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool TransactionIdIsKnownCompleted(TransactionId transactionId)
{
    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchXid) ||
        TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchCSNXid)) {
        /* If it's in the cache at all, it must be completed. */
        return true;
    }

    return false;
}

/*
 * TransactionIdCommitTree
 *		Marks the given transaction and children as committed
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * This commit operation is not guaranteed to be atomic, but if not, subxids
 * are correctly marked subcommit first.
 */
void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids, uint64 csn)
{
    TransactionIdAsyncCommitTree(xid, nxids, xids, InvalidXLogRecPtr, csn);
}

/*
 * TransactionIdAsyncCommitTree
 *		Same as above, but for async commits.  The commit record LSN is needed.
 */
void TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids, XLogRecPtr lsn, uint64 csn)
{
    /* update the Clog */
    CLogSetTreeStatus(xid, nxids, xids, CLOG_XID_STATUS_COMMITTED, lsn);

    /* set CSN log */
    if (RecoveryInProgress()) {
        /* reset CSN log at recovery */
        if (csn > 0)
            CSNLogSetCommitSeqNo(xid, nxids, xids, csn);
    } else {
        /* Set the true csn here just after the clog is set. */
        UpdateCSNLogAtTransactionEND(xid, nxids, xids, csn, true);
    }
}

/*
 * TransactionIdAbortTree
 *		Marks the given transaction and children as aborted.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * We don't need to worry about the non-atomic behavior, since any onlookers
 * will consider all the xacts as not-yet-committed anyway.
 */
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
{
    CLogSetTreeStatus(xid, nxids, xids, CLOG_XID_STATUS_ABORTED, InvalidXLogRecPtr);

    CSNLogSetCommitSeqNo(xid, nxids, xids, COMMITSEQNO_ABORTED);
}

/*
 * TransactionIdGetCommitLSN
 *
 * This function returns an LSN that is late enough to be able
 * to guarantee that if we flush up to the LSN returned then we
 * will have flushed the transaction's commit record to disk.
 *
 * The result is not necessarily the exact LSN of the transaction's
 * commit record!  For example, for long-past transactions (those whose
 * clog pages already migrated to disk), we'll return InvalidXLogRecPtr.
 * Also, because we group transactions on the same clog page to conserve
 * storage, we might return the LSN of a later transaction that falls into
 * the same group.
 */
XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid)
{
    XLogRecPtr result;

    /*
     * Currently, all uses of this function are for xids that were just
     * reported to be committed by TransactionLogFetch, so we expect that
     * checking TransactionLogFetch's cache will usually succeed and avoid an
     * extra trip to shared memory.
     */
    if (TransactionIdEquals(xid, t_thrd.xact_cxt.cachedFetchXid))
        return t_thrd.xact_cxt.cachedCommitLSN;

    /* Special XIDs are always known committed */
    if (!TransactionIdIsNormal(xid))
        return InvalidXLogRecPtr;

    /*
     * Get the transaction status.
     */
    (void)CLogGetStatus(xid, &result);

    return result;
}

/*
 * Returns the status of the tranaction.
 *
 * Note that this treats a a crashed transaction as still in-progress,
 * until it falls off the xmin horizon.
 */
TransactionIdStatus TransactionIdGetStatus(TransactionId transactionId)
{
    CommitSeqNo csn;

    csn = TransactionIdGetCommitSeqNo(transactionId, false, true, true, NULL);
    if (COMMITSEQNO_IS_COMMITTED(csn))
        return XID_COMMITTED;
    else if (COMMITSEQNO_IS_ABORTED(csn))
        return XID_ABORTED;
    else
        return XID_INPROGRESS;
}
