/* -------------------------------------------------------------------------
 *
 * csnlog.cpp
 *		Tracking Commit-Sequence-Numbers and in-progress subtransactions
 *
 * The pg_csnlog manager is a pg_clog-like manager that stores the commit
 * sequence number, or parent transaction Id, for each transaction.  It is
 * a fundamental part of MVCC.
 *
 * The csnlog serves two purposes:
 *
 * 1. While a transaction is in progress, it stores the parent transaction
 * Id for each in-progress subtransaction. A main transaction has a parent
 * of InvalidTransactionId, and each subtransaction has its immediate
 * parent. The tree can easily be walked from child to parent, but not in
 * the opposite direction.
 *
 * 2. After a transaction has committed, it stores the Commit Sequence
 * Number of the commit.
 *
 * We can use the same structure for both, because we don't care about the
 * parent-child relationships subtransaction after commit.
 *
 * This code is based on clog.c, but the robustness requirements
 * are completely different from pg_clog, because we only need to remember
 * pg_csnlog information for currently-open and recently committed
 * transactions.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/csnlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"
#include "storage/barrier.h"
#include "storage/procarray.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "replication/walreceiver.h"

/*
 * Defines for CSNLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in openGauss.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLOG page numbering also wraps around at 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE,
 * and CSNLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLOG (see CSNLOGPagePrecedes).
 *
 * We store the commit LSN for each xid
 */
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToCSNPage(xid) ((xid) / (TransactionId)CSNLOG_XACTS_PER_PAGE)
#define TransactionIdToCSNPgIndex(xid) ((xid) % (TransactionId)CSNLOG_XACTS_PER_PAGE)

#define MAX_CSNLOG_PAGE_NUMBER (TransactionIdToCSNPage(MaxTransactionId))

/* We allocate new log pages in batches, must be power of 2 */
#define BATCH_SIZE 32

#define CsnlogCtl(n) (&t_thrd.shemem_ptr_cxt.CsnlogCtlPtr[CSNBufHashPartition(n)])

#define CSN_LWLOCK_ACQUIRE(pageno, lockmode) ((void)LWLockAcquire(CSNBufMappingPartitionLock(pageno), lockmode))

#define CSN_LWLOCK_RELEASE(pageno) (LWLockRelease(CSNBufMappingPartitionLock(pageno)))

static int ZeroCSNLOGPage(int64 pageno);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId *subxids, CommitSeqNo csn, int64 pageno,
                                TransactionId topxid);
static bool CSNLogSetCSN(SlruCtl ctl, TransactionId xid, CommitSeqNo csn, int slotno);

static CommitSeqNo RecursiveGetCommitSeqNo(TransactionId xid);

/*
 * CSNLogSetCommitSeqNo
 *
 * Record the status and CSN of transaction entries in the commit log for a
 * transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be the
 * top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * InvalidCommitSeqNo for abort cases.
 *
 * Note: This doesn't guarantee atomicity. The caller can use the
 * COMMITSEQNO_COMMITTING special value for that.
 */
void CSNLogSetCommitSeqNo(TransactionId xid, int nsubxids, TransactionId *subxids, CommitSeqNo csn)
{
    int64 pageno;
    int i = 0;
    int offset = 0;
    TransactionId topxid = xid;

    /* for standby node, don't set invalid or abort csn mark. */
    if ((t_thrd.xact_cxt.useLocalSnapshot ||
         g_instance.attr.attr_storage.IsRoachStandbyCluster) &&
        csn <= COMMITSEQNO_ABORTED) {
        return;
    }

    if (csn == InvalidCommitSeqNo || xid == BootstrapTransactionId) {
        if (IsBootstrapProcessingMode())
            csn = COMMITSEQNO_FROZEN;
        else
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
                            errmsg("cannot mark transaction %lu committed without CSN %lu", xid, csn)));
    }

    /*
     * We set the status of parent transaction before the status of
     * child transactions, so that another process can correctly determine the
     * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
     */
    pageno = TransactionIdToCSNPage(xid);

    for (;;) {
        int num_on_page = 0;

        while (i < nsubxids && (int64)TransactionIdToCSNPage(subxids[i]) == pageno) {
            num_on_page++;
            i++;
        }

        CSNLogSetPageStatus(xid, num_on_page, subxids + offset, csn, pageno, topxid);
        if (i >= nsubxids) {
            break;
        }

        offset = i;
        pageno = TransactionIdToCSNPage(subxids[offset]);
        xid = InvalidTransactionId;
    }
    if (IS_MULTI_DISASTER_RECOVER_MODE && COMMITSEQNO_IS_COMMITTED(csn)) {
        UpdateXLogMaxCSN(csn);
    }
}

/**
 * @Description: Record the final state of transaction entries in the csn log for
 * 	all entries on a single page.  Atomic only on this page.
 *
 * 	Otherwise API is same as TransactionIdSetTreeStatus()
 * @in xid -  the transaction id
 * @in nsubxids -  the number of sub transactions
 * @in subxids - the array of sub transactions
 * @in csn - the csn to be set
 * @in pageno - the page number of xid
 * @in topxid - the top transaction id
 * @return -  no return
 */
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId *subxids, CommitSeqNo csn, int64 pageno,
                                TransactionId topxid)
{
    if (SS_STANDBY_MODE) {
        int tempdest = t_thrd.postgres_cxt.whereToSendOutput;
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("DMS standby can't set csnlog status")));
        t_thrd.postgres_cxt.whereToSendOutput = tempdest;
        return;
    }

    int slotno;
    int i;
    bool modified = false;
    bool commit_in_progress = ((csn & COMMITSEQNO_COMMIT_INPROGRESS) == COMMITSEQNO_COMMIT_INPROGRESS);
    bool retry = false;
    bool need_restart = false;
    Assert(csn <= COMMITSEQNO_SUBTRANS_BIT);

restart:
    PG_TRY();
    {
        /* lock is acquired by SimpleLruReadPage_ReadOnly */
        slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl(pageno), pageno, xid);
    }
    PG_CATCH();
    {
        /*
         * since redo will not concurrently happen, so no lock here.
         */
        if (!retry && RecoveryInProgress()) {
            ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("while build or rewind, page to access"
                                                                         " may cleaned before, reinit here.")));
            (void)ZeroCSNLOGPage(pageno);
            retry = true;
            need_restart = true;
            FlushErrorState();
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();

    if (need_restart) {
        need_restart = false;
        goto restart;
    }

    /* first the main transaction */
    if (TransactionIdIsValid(xid)) {
        if (CSNLogSetCSN(CsnlogCtl(pageno), xid, csn, slotno))
            modified = true;
    }

    /* then the Subtransactions, if needed ... */
    for (i = 0; i < nsubxids; i++) {
        CommitSeqNo set_csn = csn;
        Assert(CsnlogCtl(pageno)->shared->page_number[slotno] == (int64)TransactionIdToCSNPage(subxids[i]));

        if (commit_in_progress) {
            /* Reset Parent to Top Transaction xid */
            set_csn = COMMITSEQNO_SUBTRANS_BIT | topxid;
            /* Set Commit in progress */
            set_csn |= COMMITSEQNO_COMMIT_INPROGRESS;
        }

        if (CSNLogSetCSN(CsnlogCtl(pageno), subxids[i], set_csn, slotno))
            modified = true;
    }

    if (modified)
        CsnlogCtl(pageno)->shared->page_dirty[slotno] = true;

    CSN_LWLOCK_RELEASE(pageno);
}

/**
 * @Description: Record the parent of a subtransaction in the subtrans log.
 * 	In some cases we may need to overwrite an existing value.
 * @in xid - the transaction id
 * @in parent - the parent xid to be set
 * @return - no return
 */
void SubTransSetParent(TransactionId xid, TransactionId parent)
{
    if (SS_STANDBY_MODE) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("DMS standby can't set csnlog status")));
        return;
    }

    int64 pageno = TransactionIdToCSNPage(xid);
    int entryno = TransactionIdToCSNPgIndex(xid);
    int slotno;
    CommitSeqNo *ptr = NULL;
    CommitSeqNo newcsn;

    Assert(TransactionIdIsValid(parent));
    Assert(TransactionIdFollows(xid, parent));

    newcsn = COMMITSEQNO_SUBTRANS_BIT | parent;

    /*
     * Shared page access is enough to set the subtransaction parent.
     * It is set when the subtransaction is assigned an xid,
     * and can be read only later, after the subtransaction have modified
     * some tuples.
     */
    slotno = SimpleLruReadPage_ReadOnly(CsnlogCtl(pageno), pageno, xid);
    ptr = (CommitSeqNo *)CsnlogCtl(pageno)->shared->page_buffer[slotno];
    ptr += entryno;

    /*
     * It's possible we'll try to set the parent xid multiple times
     * but we shouldn't ever be changing the xid from one valid xid
     * to another valid xid, which would corrupt the data structure.
     */
    if (*ptr != newcsn && *ptr != (newcsn | COMMITSEQNO_COMMIT_INPROGRESS)) {
        *ptr = newcsn;
        CsnlogCtl(pageno)->shared->page_dirty[slotno] = true;
    }

    CSN_LWLOCK_RELEASE(pageno);
}

/**
 * @Description: Interrogate the parent of a transaction in the csnlog.
 * @in xid -  the transaction id
 * @out status - return commit/abort if csnlog has been set by top tx
 * @return - the parent xid of the input transaction
 */
TransactionId SubTransGetParent(TransactionId xid, CLogXidStatus *status, bool force_wait_parent)
{
    CommitSeqNo csn;

    csn = CSNLogGetCommitSeqNo(xid);
    if (COMMITSEQNO_IS_SUBTRANS(csn))
        return (TransactionId)GET_PARENTXID(csn);
    else {
        /* Check csn Log, maybe top trx has updated the csn. */
        if (COMMITSEQNO_IS_COMMITTED(csn))
            *status = CLOG_XID_STATUS_COMMITTED;
        if (COMMITSEQNO_IS_ABORTED(csn))
            *status = CLOG_XID_STATUS_ABORTED;

        /*
         * for lock wait condition, we search the proc array and try to find
         * the parent xid.
         */
        if (force_wait_parent) {
            return SubTransGetTopParentXidFromProcs(xid);
        }

        return InvalidTransactionId;
    }
}

/**
 * @Description: SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 *
 * @in xid -  the transaction id
 * @return - return the top parent transaction xid
 */
TransactionId SubTransGetTopmostTransaction(TransactionId xid)
{
    TransactionId parentXid = xid;
    TransactionId previousXid = xid;
    CLogXidStatus status = CLOG_XID_STATUS_IN_PROGRESS;

    /* Can't ask about stuff that might not be around anymore */
    Assert(TransactionIdFollowsOrEquals(xid, u_sess->utils_cxt.TransactionXmin));

    while (TransactionIdIsValid(parentXid)) {
        previousXid = parentXid;
        if (TransactionIdPrecedes(parentXid, u_sess->utils_cxt.TransactionXmin))
            break;
        parentXid = SubTransGetParent(parentXid, &status, false);
        /*
         * By convention the parent xid gets allocated first, so should
         * always precede the child xid. Anything else points to a corrupted
         * data structure that could lead to an infinite loop, so exit.
         */
        if (!TransactionIdPrecedes(parentXid, previousXid))
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("pg_csnlog contains invalid entry: xid %lu "
                                                              "points to parent xid %lu",
                                                              previousXid, parentXid)));
    }

    Assert(TransactionIdIsValid(previousXid));

    return previousXid;
}

/**
 * @Description: Sets the commit status of a single transaction.
 * Return true is page is modified.
 *
 * Must be called with CSNLogControlLock held
 * @in ctl -  the slru log control data
 * @in xid -  the transaction id
 * @in csn - the csn to be set
 * @in slotno - the slot id to set csn
 * @return -  return true if the csn is not aborted, else return false
 */
static bool CSNLogSetCSN(SlruCtl ctl, TransactionId xid, CommitSeqNo csn, int slotno)
{
    int entryno = TransactionIdToCSNPgIndex(xid);
    CommitSeqNo *ptr = NULL;
    CommitSeqNo value = 0;

    ptr = (CommitSeqNo *)(ctl->shared->page_buffer[slotno] + entryno * sizeof(CommitSeqNo));
    value = *ptr;

    /*
     * Two state changes don't allowed:
     * 1, ABORT to COMMIT
     * 2, COMMIT to ABORT
     * (Allow setting again to same value.)
     */
    if (((COMMITSEQNO_IS_COMMITTED(value) && COMMITSEQNO_IS_ABORTED(csn)) ||
         (COMMITSEQNO_IS_ABORTED(value) && COMMITSEQNO_IS_COMMITTED(csn)))) {
        elog(PANIC, "csn log state change from %lu to %lu is not allowed!", value, csn);
    }

    if (!COMMITSEQNO_IS_ABORTED(*ptr)) {
        *ptr = csn;
        return true;
    } else {
        return false;
    }
}

/**
 * @Description: Get the raw CSN value.
 * @in xid -  the transaction id
 * @return -  return the csn recorded for the input xid
 */
static CommitSeqNo InternalGetCommitSeqNo(TransactionId xid)
{
    int64 pageno = TransactionIdToCSNPage(xid);
    int entryno = TransactionIdToCSNPgIndex(xid);
    int slotno;
    CommitSeqNo csn;

    if (!TransactionIdIsNormal(xid)) {
        if (xid == InvalidTransactionId)
            return COMMITSEQNO_ABORTED;
        if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
            return COMMITSEQNO_FROZEN;
    }

    slotno = SimpleLruReadPage_ReadOnly_Locked(CsnlogCtl(pageno), pageno, xid);
    csn = *(CommitSeqNo *)(CsnlogCtl(pageno)->shared->page_buffer[slotno] + entryno * sizeof(CommitSeqNo));

    return csn;
}

/**
 * @Description: Interrogate the CSN of a transaction in the CSN log.
 * @in xid -  the transaction id
 * @return -  return the csn of the input transaction
 */
CommitSeqNo CSNLogGetCommitSeqNo(TransactionId xid)
{
    int64 pageno = TransactionIdToCSNPage(xid);
    CommitSeqNo csn;

    CSN_LWLOCK_ACQUIRE(pageno, LW_SHARED);
    csn = InternalGetCommitSeqNo(xid);
    CSN_LWLOCK_RELEASE(pageno);

    return csn;
}

/**
 * @Description: Interrogate the CSN of a transaction in the CSN log recursively.
 * @in xid -  the transaction id
 * @return -  return the csn of the top parent of the input transaction
 */
CommitSeqNo CSNLogGetNestCommitSeqNo(TransactionId xid)
{
    int64 pageno = TransactionIdToCSNPage(xid);
    CommitSeqNo csn;

    CSN_LWLOCK_ACQUIRE(pageno, LW_SHARED);
    csn = RecursiveGetCommitSeqNo(xid);
    CSN_LWLOCK_RELEASE(pageno);
    return csn;
}

/**
 * @Description: Interrogate the CSN of a transaction in the CSN log recursively.
 * @in xid -  the transaction id
 * @return -  return the csn of the top parent of the input transaction
 */
CommitSeqNo CSNLogGetDRCommitSeqNo(TransactionId xid)
{
    CommitSeqNo csn = InvalidCommitSeqNo;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    MemoryContext old = CurrentMemoryContext;
    PG_TRY();
    {
        csn = CSNLogGetCommitSeqNo(xid);
    }
    PG_CATCH();
    {
        if (t_thrd.xact_cxt.slru_errcause == SLRU_OPEN_FAILED) {
            MemoryContextSwitchTo(old);
            FlushErrorState();
            t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
            csn = InvalidCommitSeqNo;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    return csn;
}

/**
 * @Description: Determine the CSN of a transaction, walking the
 * subtransaction tree if needed
 * @in xid -  the transaction id
 * @return -  return csn of the input transaction id
 */
static CommitSeqNo RecursiveGetCommitSeqNo(TransactionId xid)
{
    CommitSeqNo csn;
    bool need_relock = false;

    csn = InternalGetCommitSeqNo(xid);
    if (COMMITSEQNO_IS_SUBTRANS(csn) && COMMITSEQNO_IS_COMMITTING(csn)) {
        TransactionId parentXid = GET_PARENTXID(csn);

        if (CSNBufHashPartition(TransactionIdToCSNPage(xid)) !=
            CSNBufHashPartition(TransactionIdToCSNPage(parentXid))) {
            CSN_LWLOCK_RELEASE(TransactionIdToCSNPage(xid));
            CSN_LWLOCK_ACQUIRE(TransactionIdToCSNPage(parentXid), LW_SHARED);
            need_relock = true;
        }

        csn = RecursiveGetCommitSeqNo(parentXid);

        if (need_relock) {
            CSN_LWLOCK_RELEASE(TransactionIdToCSNPage(parentXid));
            CSN_LWLOCK_ACQUIRE(TransactionIdToCSNPage(xid), LW_SHARED);
        }
    }

    return csn;
}

/**
 * @Description: Number of shared CSNLOG buffers.
 * @return -  return the number of shared CSNLOG buffers.
 */
Size CSNLOGShmemBuffers(void)
{
    return Min(256, Max(BATCH_SIZE, g_instance.attr.attr_storage.NBuffers / 512));
}

/**
 * @Description: Initialization of shared memory for CSNLOG
 * @return -  return the size of shared memory of csn log
 */
Size CSNLOGShmemSize(void)
{
    int i;
    Size sz = 0;

    for (i = 0; i < NUM_CSNLOG_PARTITIONS; i++) {
        sz += SimpleLruShmemSize(CSNLOGShmemBuffers(), 0);
    }

    return sz;
}

/**
 * @Description: Initialize the csnlog lru buffer
 * @return -  no return
 */
void CSNLOGShmemInit(void)
{
    int i;
    int rc = 0;
    char name[SLRU_MAX_NAME_LENGTH];

    MemoryContext old = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    t_thrd.shemem_ptr_cxt.CsnlogCtlPtr = (SlruCtlData*)palloc0(NUM_CSNLOG_PARTITIONS * sizeof(SlruCtlData));
    (void)MemoryContextSwitchTo(old);

    for (i = 0; i < NUM_CSNLOG_PARTITIONS; i++) {
        rc = sprintf_s(name, SLRU_MAX_NAME_LENGTH, "%s%d", "CSNLOG Ctl", i);
        securec_check_ss(rc, "\0", "\0");
        SimpleLruInit(CsnlogCtl(i), name, LWTRANCHE_CSNLOG_CTL, CSNLOGShmemBuffers(), 0,
                      CSNBufMappingPartitionLockByIndex(i), CSNLOGDIR, i);
    }
}

/**
 * @Description: This func must be called ONCE on system install.  It creates
 * the initial CSNLOG segment.  (The pg_csnlog directory is assumed to
 * have been created by initdb, and CSNLOGShmemInit must have been
 * called already.)
 * @return -  no return
 */
void BootStrapCSNLOG(void)
{
    int slotno;
    int64 pageno;

    /* Create and zero the first BATCH_SIZE page of the commit log */
    for (pageno = 0; pageno < BATCH_SIZE; pageno++) {
        CSN_LWLOCK_ACQUIRE(pageno, LW_EXCLUSIVE);
        slotno = ZeroCSNLOGPage(pageno);

        /* Make sure it's written out */
        SimpleLruWritePage(CsnlogCtl(pageno), slotno);
        if (CsnlogCtl(pageno)->shared->page_dirty[slotno])
            ereport(PANIC, (errmsg("Initialize the csn log failed.")));
        CSN_LWLOCK_RELEASE(pageno);
    }

    t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage = BATCH_SIZE - 1;
    t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage = 0;
}

/**
 * @Description: Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 * @in pageno -  the page of csn log to be zeroed
 * @return -  return the buffer slot for the page
 */
static int ZeroCSNLOGPage(int64 pageno)
{
    return SimpleLruZeroPage(CsnlogCtl(pageno), pageno);
}

/**
 * @Description: This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 * @in oldestActiveXID -  the oldest active transaction id
 * @return -  no return
 */
void StartupCSNLOG()
{
    int64 lastExtendPage = TransactionIdToCSNPage(t_thrd.xact_cxt.ShmemVariableCache->nextXid - 1);
    t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage = lastExtendPage;
    elog(LOG, "startup csnlog without extend at xid:%lu, pageno:%ld",
        t_thrd.xact_cxt.ShmemVariableCache->nextXid - 1, t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage);
    t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage = 0;
}

/**
 * @Description: This must be called ONCE during postmaster or standalone-backend shutdown
 * @return -  no return
 */
void ShutdownCSNLOG(void)
{
    int i;

    /*
     * Flush dirty CLOG pages to disk
     *
     * This is not actually necessary from a correctness point of view. We do
     * it merely as a debugging aid.
     */
    TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(false);

    for (i = 0; i < NUM_CSNLOG_PARTITIONS; i++)
        (void)SimpleLruFlush(CsnlogCtl(i), false);

    TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(false);
}

/**
 * @Description: Perform a checkpoint --- either during shutdown, or on-the-fly
 * @return -  no return
 */
void CheckPointCSNLOG(void)
{
    int i;
    int flush_num;
    TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_START(true);

    for (i = 0; i < NUM_CSNLOG_PARTITIONS; i++) {
        flush_num = SimpleLruFlush(CsnlogCtl(i), true);
        g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_csnlog_flush_num += flush_num;
    }
    TRACE_POSTGRESQL_CSNLOG_CHECKPOINT_DONE(true);
}

/**
 * @Description: Make sure that CSNLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 * @in newestXact -  the transaction id to be extend on csn log
 * @return -  no return
 */
void ExtendCSNLOG(TransactionId newestXact)
{
    int64 pageno;

    gstrace_entry(GS_TRC_ID_ExtendCSNLOG);
#ifdef PGXC
    int64 OldestUsedPage = 0;
    int64 lastExtendPage, newExtendPage;
    TransactionId lastExtendTransactionId;

    if (!TransactionIdIsValid(newestXact)) {
        gstrace_exit(GS_TRC_ID_ExtendCSNLOG);
        return;
    }

    /*
     * In PGXC, it may be that a node is not involved in a transaction,
     * and therefore will be skipped, so we need to detect this by using
     * the latest_page_number instead of the pg index.
     *
     * Also, there is a special case of when transactions wrap-around that
     * we need to detect.
     */
    pageno = TransactionIdToCSNPage(newestXact);
    /*
     * If the pageno is smaller than startExtendCSNLogpage(from gtm recent global xmin, or is 0), then
     *  the current transaction must be abortand no need to go on.
     */
    if (pageno < t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage) {
        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                        (errmsg("the current transaction %lu is smaller than gtm "
                                "recent global xmin, so no need to set csn log. pageno: %ld,"
                                " startExtendCSNLogPage: %ld",
                                newestXact, pageno, t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage))));
    }

    lastExtendPage = t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage;

    lastExtendTransactionId = (lastExtendPage + 1) * CSNLOG_XACTS_PER_PAGE - 1;
    if (TransactionIdPrecedes(lastExtendTransactionId, newestXact)) {
        if (TransactionIdIsNormal(u_sess->utils_cxt.g_GTM_Snapshot->sn_recent_global_xmin)) {
            /*
             * since the csnlog is only used for xid which is bigger than
             * recentGlobalXmin, so the pages before recentGlobalXmin
             * need not to be extended.
             */
            OldestUsedPage = TransactionIdToCSNPage(u_sess->utils_cxt.g_GTM_Snapshot->sn_recent_global_xmin);
            if (OldestUsedPage > lastExtendPage) {
                elog(LOG,
                     "extend csnlog from page no %ld according"
                     " to recent gloabl xmin %lu",
                     OldestUsedPage, u_sess->utils_cxt.g_GTM_Snapshot->sn_recent_global_xmin);
                CSN_LWLOCK_ACQUIRE(OldestUsedPage, LW_EXCLUSIVE);
                (void)ZeroCSNLOGPage(OldestUsedPage);
                t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage = OldestUsedPage;
                CSN_LWLOCK_RELEASE(OldestUsedPage);
                lastExtendPage = OldestUsedPage;
            }
        }

        /* The next BATCH_SIZE is the end page */
        newExtendPage = ((uint64)pageno + BATCH_SIZE) & (~(BATCH_SIZE - 1));
        Assert(newExtendPage <= (int64)MAX_CSNLOG_PAGE_NUMBER);

        /*
         * recheck lastExtendPage < newExtendPage, since the lastExtendPage may be changed
         * by OldestUsedPage in the code above
         */
        if (lastExtendPage >= newExtendPage) {
            ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                            (errmsg("The lastExtenPage %ld is larger than newExtendPage %ld,"
                                    "and the newestXact is %lu, the recent global xmin %lu",
                                    lastExtendPage, newExtendPage, newestXact,
                                    u_sess->utils_cxt.g_GTM_Snapshot->sn_recent_global_xmin))));
        }

        while (++lastExtendPage != newExtendPage) {
            CSN_LWLOCK_ACQUIRE(lastExtendPage, LW_EXCLUSIVE);
            (void)ZeroCSNLOGPage(lastExtendPage);
            CSN_LWLOCK_RELEASE(lastExtendPage);
        }

        t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage = newExtendPage - 1;

        elog(DEBUG1, "extend csnlog to xid:%lu, pageno:%ld", newestXact,
             t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage);
    }
#else
    /*
     * No work except at first XID of a page.
     */
    if (TransactionIdToCSNPgIndex(newestXact) != 0 && !TransactionIdEquals(newestXact, FirstNormalTransactionId)) {
        gstrace_exit(GS_TRC_ID_ExtendCSNLOG);
        return;
    }

    pageno = TransactionIdToCSNPage(newestXact);
    if (pageno % BATCH_SIZE) {
        gstrace_exit(GS_TRC_ID_ExtendCSNLOG);
        return;
    }

    /* Zero the page(without make an XLOG entry about it) */
    for (int64 i = pageno; i < pageno + BATCH_SIZE; i++) {
        CSN_LWLOCK_ACQUIRE(i, LW_EXCLUSIVE);
        (void)ZeroCSNLOGPage(i);
        CSN_LWLOCK_RELEASE(i);
    }
#endif
    gstrace_exit(GS_TRC_ID_ExtendCSNLOG);
}

/**
 * @Description: Remove all CSNLOG segments before the one holding the passed transaction ID
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 * @in oldestXact -  the oldest xmin
 * @return -  no return
 */
void TruncateCSNLOG(TransactionId oldestXact)
{
    int64 cutoffPage;
    TransactionId CatalogXmin = GetReplicationSlotCatalogXmin();
    if (CatalogXmin >= FirstNormalTransactionId) {
        oldestXact = Min(oldestXact, CatalogXmin);
    }
    u_sess->utils_cxt.RecentDataXmin = oldestXact;
    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.
     */
    cutoffPage = TransactionIdToCSNPage(oldestXact);

    SimpleLruTruncate(CsnlogCtl(0), cutoffPage, NUM_CSNLOG_PARTITIONS);

    elog(LOG, "truncate CSN log oldestXact %lu, next xid %lu", oldestXact, t_thrd.xact_cxt.ShmemVariableCache->nextXid);
}

void SSCSNLOGShmemClear(void)
{
    int i;
    int rc = 0;
    char name[SLRU_MAX_NAME_LENGTH];

    for (i = 0; i < NUM_CSNLOG_PARTITIONS; i++) {
        rc = sprintf_s(name, SLRU_MAX_NAME_LENGTH, "%s%d", "CSNLOG Ctl", i);
        securec_check_ss(rc, "\0", "\0");
        SimpleLruSetPageEmpty(CsnlogCtl(i), name, (int)LWTRANCHE_CSNLOG_CTL, (int)CSNLOGShmemBuffers(), 0,
            CSNBufMappingPartitionLockByIndex(i), CSNLOGDIR, i);
    }
}
