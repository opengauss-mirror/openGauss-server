/*
 * clog.cpp
 *		openGauss transaction-commit-log manager
 *
 * This module replaces the old "pg_log" access code, which treated pg_log
 * essentially like a relation, in that it went through the regular buffer
 * manager.  The problem with that was that there wasn't any good way to
 * recycle storage space for transactions so old that they'll never be
 * looked up again.  Now we use specialized access code so that the commit
 * log can be broken into relatively small, independent segments.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CLOG page is initialized to zeroes.	Other writes of CLOG come from
 * recording of transaction commit or abort in xact.c, which generates its
 * own XLOG records for these events and will re-perform the status update
 * on redo; so we need make no additional XLOG entry here.	For synchronous
 * transaction commits, the XLOG is guaranteed flushed through the XLOG commit
 * record before we are called to log a commit, so the WAL rule "write xlog
 * before data" is satisfied automatically.  However, for async commits we
 * must track the latest LSN affecting each CLOG page, so that we can flush
 * XLOG that far and satisfy the WAL rule.	We don't have to worry about this
 * for aborts (whether sync or async), since the post-crash assumption would
 * be that such transactions failed anyway.
 *
 * Portions Copyright (c) 2020, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/clog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/slru.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "storage/smgr/fd.h"
#include "storage/proc.h"
#ifdef USE_ASSERT_CHECKING
#include "utils/builtins.h"
#endif /* USE_ASSERT_CHECKING */
#ifdef ENABLE_UT
#define static
#endif /* USE_UT */

/* We store the latest async LSN for each group of transactions */
#define CLOG_XACTS_PER_LSN_GROUP 32 /* keep this a power of 2 */
#define CLOG_LSNS_PER_PAGE (CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP)

#define GetLSNIndex(slotno, xid) \
    ((slotno)*CLOG_LSNS_PER_PAGE + ((xid) % (TransactionId)CLOG_XACTS_PER_PAGE) / CLOG_XACTS_PER_LSN_GROUP)

#define ClogCtl(n) (&t_thrd.shemem_ptr_cxt.ClogCtl[CBufHashPartition(n)])
#define CLOG_BATCH_SIZE 32

/*
 * The number of subtransactions below which we consider to apply clog group
 * update optimization.  Testing reveals that the number higher than this can
 * hurt performance.
 */
#define THRESHOLD_SUBTRANS_CLOG_OPT 5

static int ZeroCLOGPage(int64 pageno, bool writeXlog);
static void WriteZeroPageXlogRec(int64 pageno);
static void WriteTruncateXlogRec(int64 pageno);
static void CLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId *subxids, CLogXidStatus status,
                              XLogRecPtr lsn, int64 pageno, bool all_xact_same_page);
static void set_status_by_pages(int nsubxids, TransactionId *subxids, CLogXidStatus status, XLogRecPtr lsn);
static void CLogSetStatusBit(TransactionId xid, CLogXidStatus status, XLogRecPtr lsn, int slotno);

static bool CLogGroupUpdateXidStatus(TransactionId xid, CLogXidStatus status, XLogRecPtr lsn, int64 pageno);
static void CLogSetPageStatusInternal(TransactionId xid, int nsubxids, const TransactionId *subxids,
                                      CLogXidStatus status, XLogRecPtr lsn, int64 pageno);
int FIT_lw_deadlock(int n_edges);

static inline char *GetTransactionStatus(CLogXidStatus status)
{
    if (status == CLOG_XID_STATUS_IN_PROGRESS)
        return "transaction in progress";
    else if (status == CLOG_XID_STATUS_COMMITTED)
        return "transaction committed";
    else if (status == CLOG_XID_STATUS_ABORTED)
        return "transaction aborted";
    else if (status == CLOG_XID_STATUS_SUB_COMMITTED)
        return "transaction subcommitted";
    else {
        ereport(PANIC, (errmsg("Incorrect xid status %d", status)));
        return "invalid status";
    }
}

/*
 * TransactionIdSetTreeStatus
 *
 * Record the final state of transaction entries in the commit log for
 * a transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be
 * the top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * lsn must be the WAL location of the commit record when recording an async
 * commit.	For a synchronous commit it can be InvalidXLogRecPtr, since the
 * caller guarantees the commit record is already flushed in that case.  It
 * should be InvalidXLogRecPtr for abort cases, too.
 *
 * In the commit case, atomicity is limited by whether all the subxids are in
 * the same CLOG page as xid.  If they all are, then the lock will be grabbed
 * only once, and the status will be set to committed directly.  Otherwise
 * we must
 *	 1. set sub-committed all subxids that are not on the same page as the
 *		main xid
 *	 2. atomically set committed the main xid and the subxids on the same page
 *	 3. go over the first bunch again and set them committed
 * Note that as far as concurrent checkers are concerned, main transaction
 * commit as a whole is still atomic.
 *
 * Example:
 *		TransactionId t commits and has subxids t1, t2, t3, t4
 *		t is on page p1, t1 is also on p1, t2 and t3 are on p2, t4 is on p3
 *		1. update pages2-3:
 *					page2: set t2,t3 as sub-committed
 *					page3: set t4 as sub-committed
 *		2. update page1:
 *					set t1 as sub-committed,
 *					then set t as committed,
                    then set t1 as committed
 *		3. update pages2-3:
 *					page2: set t2,t3 as committed
 *					page3: set t4 as committed
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; functions in transam.c are the intended callers.
 *
 * XXX Think about issuing FADVISE_WILLNEED on pages that we will need,
 * but aren't yet in cache, as well as hinting pages not to fall out of
 * cache yet.
 */
void CLogSetTreeStatus(TransactionId xid, int nsubxids, TransactionId *subxids, CLogXidStatus status, XLogRecPtr lsn)
{
    int64 pageno = (int64)TransactionIdToPage(xid); /* get page of parent */
    int i;

    if (SECUREC_UNLIKELY(!(status == CLOG_XID_STATUS_COMMITTED || status == CLOG_XID_STATUS_ABORTED)))
        ereport(PANIC, (errmsg("CLOG STATUS ERROR: xid %lu status %s", xid, GetTransactionStatus(status))));

    if (SHOW_DEBUG_MESSAGE()) {
        if (status == CLOG_XID_STATUS_COMMITTED)
            ereport(DEBUG1, (errmsg("Record transaction commit " XID_FMT, xid)));
        else
            ereport(DEBUG1, (errmsg("Record transaction abort " XID_FMT, xid)));
    }

    /*
     * See how many subxids, if any, are on the same page as the parent, if
     * any.
     */
    for (i = 0; i < nsubxids; i++) {
        if ((int64)TransactionIdToPage(subxids[i]) != pageno)
            break;
    }

    /*
     * Do all items fit on a single page?
     */
    if (i == nsubxids) {
        /*
         * Set the parent and all subtransactions in a single call
         */
        CLogSetPageStatus(xid, nsubxids, subxids, status, lsn, pageno, true);
    } else {
        int nsubxids_on_first_page = i;

        /*
         * If this is a commit then we care about doing this correctly (i.e.
         * using the subcommitted intermediate status).  By here, we know
         * we're updating more than one page of clog, so we must mark entries
         * that are *not* on the first page so that they show as subcommitted
         * before we then return to update the status to fully committed.
         *
         * To avoid touching the first page twice, skip marking subcommitted
         * for the subxids on that first page.
         */
        if (status == CLOG_XID_STATUS_COMMITTED)
            set_status_by_pages(nsubxids - nsubxids_on_first_page, subxids + nsubxids_on_first_page,
                                CLOG_XID_STATUS_SUB_COMMITTED, lsn);

        /*
         * Now set the parent and subtransactions on same page as the parent,
         * if any
         */
        pageno = (int64)TransactionIdToPage(xid);
        CLogSetPageStatus(xid, nsubxids_on_first_page, subxids, status, lsn, pageno, false);

        /*
         * Now work through the rest of the subxids one clog page at a time,
         * starting from the second page onwards, like we did above.
         */
        set_status_by_pages(nsubxids - nsubxids_on_first_page, subxids + nsubxids_on_first_page, status, lsn);
    }
}

/**
 * @Description: Record the final state of transaction entries in the commit log for
 *				all entries on a single page.  Atomic only on this page.
 * @in xid -  the transaction ID
 * @in nsubxids - the number of sub transactions of the current transaction
 * @in subxids - the array of sub transactions of the current transaction
 * @in status - the clog status to be set
 * @in lsn - the lsn for xlog record
 * @in pageno - the clog page no of the xid
 * @in all_xact_same_page - if the transaction and its subtransactions are in
 *						the same page.
 * @return -  no return
 */
static void CLogSetPageStatus(TransactionId xid, int nsubxids, TransactionId *subxids, CLogXidStatus status,
                              XLogRecPtr lsn, int64 pageno, bool all_xact_same_page)
{
    /* Can't use group update when PGPROC overflows. */
    StaticAssertStmt(THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS,
                     "group clog threshold less than PGPROC cached subxids");

    /*
     * When there is contention on ClogCtl(pageno)->shared->control_lock, we try to group multiple
     * updates; a single leader process will perform transaction status
     * updates for multiple backends so that the number of times
     * ClogCtl(pageno)->shared->control_lock needs to be acquired is reduced.
     *
     * For this optimization to be safe, the XID in MyPgXact and the subxids
     * in t_thrd.proc must be the same as the ones for which we're setting the
     * status.	Check that this is the case.
     *
     * For this optimization to be efficient, we shouldn't have too many
     * sub-XIDs and all of the XIDs for which we're adjusting clog should be
     * on the same page.  Check those conditions, too.
     */
    if (all_xact_same_page && xid == t_thrd.pgxact->xid && nsubxids <= THRESHOLD_SUBTRANS_CLOG_OPT &&
        nsubxids == t_thrd.pgxact->nxids &&
        memcmp(subxids, t_thrd.proc->subxids.xids, (size_t)(nsubxids * sizeof(TransactionId))) == 0) {
        /*
         * We don't try to do group update optimization if a process has
         * overflowed the subxids array in its PGPROC, since in that case we
         * don't have a complete list of XIDs for it.
         */
        Assert(THRESHOLD_SUBTRANS_CLOG_OPT <= PGPROC_MAX_CACHED_SUBXIDS);

        /*
         * If we can immediately acquire ClogCtl(pageno)->shared->control_lock, we update the status
         * of our own XID and release the lock.  If not, try use group XID
         * update.  If that doesn't work out, fall back to waiting for the
         * lock to perform an update for this transaction only.
         */
        if (LWLockConditionalAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE)) {
            /* Got the lock without waiting!  Do the update. */
            CLogSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
            LWLockRelease(ClogCtl(pageno)->shared->control_lock);
            return;
        } else if (CLogGroupUpdateXidStatus(xid, status, lsn, pageno)) {
            /* Group update mechanism has done the work. */
            return;
        }

        /* Fall through only if update isn't done yet. */
    }

    /* Group update not applicable, or couldn't accept this page number. */
    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);
    CLogSetPageStatusInternal(xid, nsubxids, subxids, status, lsn, pageno);
    LWLockRelease(ClogCtl(pageno)->shared->control_lock);
}

/**
 * @Description: Helper for TransactionIdSetTreeStatus: set the status for a bunch of
 * 	transactions, chunking in the separate CLOG pages involved. We never
 * 	pass the whole transaction tree to this function, only subtransactions
 * 	that are on different pages to the top level transaction id.
 * @in nsubxids - the number of sub transactions of the current transaction
 * @in subxids - the array of sub transactions of the current transaction
 * @in status - the clog status to be set
 * @in lsn - the lsn for xlog record
 * @return -  no return
 */
static void set_status_by_pages(int nsubxids, TransactionId *subxids, CLogXidStatus status, XLogRecPtr lsn)
{
    int64 pageno = (int64)TransactionIdToPage(subxids[0]);
    int offset = 0;
    int i = 0;

    Assert(nsubxids > 0); /* else the pageno fetch above is unsafe */

    while (i < nsubxids) {
        int num_on_page = 0;
        int64 nextpageno = 0;

        do {
            nextpageno = (int64)TransactionIdToPage(subxids[i]);
            if (nextpageno != pageno)
                break;
            num_on_page++;
            i++;
        } while (i < nsubxids);

        CLogSetPageStatus(InvalidTransactionId, num_on_page, subxids + offset, status, lsn, pageno, false);
        offset = i;
        pageno = nextpageno;
    }
}

/*
 * Record the final state of transaction entry in the commit log
 *
 * We don't do any locking here; caller must handle that.
 */
static void CLogSetPageStatusInternal(TransactionId xid, int nsubxids, const TransactionId *subxids,
                                      CLogXidStatus status, XLogRecPtr lsn, int64 pageno)
{
    int slotno;
    int i;

    if (!(status == CLOG_XID_STATUS_COMMITTED || status == CLOG_XID_STATUS_ABORTED ||
          (status == CLOG_XID_STATUS_SUB_COMMITTED && !TransactionIdIsValid(xid))))
        ereport(PANIC, (errmsg("CLOG PAGE STATUS ERROR: xid %lu status %s", xid, GetTransactionStatus(status))));

    Assert(LWLockHeldByMeInMode(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE));
    /*
     * If we're doing an async commit (ie, lsn is valid), then we must wait
     * for any active write on the page slot to complete.  Otherwise our
     * update could reach disk in that write, which will not do since we
     * mustn't let it reach disk until we've done the appropriate WAL flush.
     * But when lsn is invalid, it's OK to scribble on a page while it is
     * write-busy, since we don't care if the update reaches disk sooner than
     * we think.
     */
    slotno = SimpleLruReadPage(ClogCtl(pageno), pageno, XLogRecPtrIsInvalid(lsn), xid);

    /* Set the main transaction id, if any. */
    if (TransactionIdIsValid(xid)) {
        /* Subtransactions first, if needed ... */
        if (status == CLOG_XID_STATUS_COMMITTED) {
            for (i = 0; i < nsubxids; i++) {
                Assert(ClogCtl(pageno)->shared->page_number[slotno] == (int64)TransactionIdToPage(subxids[i]));
                CLogSetStatusBit(subxids[i], CLOG_XID_STATUS_SUB_COMMITTED, lsn, slotno);
            }
        }

        /* ... then the main transaction */
        CLogSetStatusBit(xid, status, lsn, slotno);
    }

    /* Set the subtransactions */
    for (i = 0; i < nsubxids; i++) {
        Assert(ClogCtl(pageno)->shared->page_number[slotno] == (int64)TransactionIdToPage(subxids[i]));
        CLogSetStatusBit(subxids[i], status, lsn, slotno);
    }

    ClogCtl(pageno)->shared->page_dirty[slotno] = true;
}

/*
 * When we cannot immediately acquire ClogCtl(pageno)->shared->control_lock in exclusive mode at
 * commit time, add ourselves to a list of processes that need their XIDs
 * status update.  The first process to add itself to the list will acquire
 * ClogCtl(pageno)->shared->control_lock in exclusive mode and set transaction status as required
 * on behalf of all group members.  This avoids a great deal of contention
 * around ClogCtl(pageno)->shared->control_lock when many processes are trying to commit at once,
 * since the lock need not be repeatedly handed off from one committing
 * process to the next.
 *
 * Returns true when transaction status has been updated in clog; returns
 * false if we decided against applying the optimization because the page
 * number we need to update differs from those processes already waiting.
 */
static bool CLogGroupUpdateXidStatus(TransactionId xid, CLogXidStatus status, XLogRecPtr lsn, int64 pageno)
{
    PGPROC *proc = t_thrd.proc;
    uint32 nextidx;
    uint32 wakeidx;

    /* We should definitely have an XID whose status needs to be updated. */
    Assert(TransactionIdIsValid(xid));

    /*
     * Add ourselves to the list of processes needing a group XID status
     * update.
     */
    proc->clogGroupMember = true;
    proc->clogGroupMemberXid = xid;
    proc->clogGroupMemberXidStatus = status;
    proc->clogGroupMemberPage = pageno;
    proc->clogGroupMemberLsn = lsn;

    nextidx = pg_atomic_read_u32(&g_instance.proc_base->clogGroupFirst);

    while (true) {
        /*
         * Add the proc to list, if the clog page where we need to update the
         * current transaction status is same as group leader's clog page.
         *
         * There is a race condition here, which is that after doing the below
         * check and before adding this proc's clog update to a group, the
         * group leader might have already finished the group update for this
         * page and becomes group leader of another group. This will lead to a
         * situation where a single group can have different clog page
         * updates.  This isn't likely and will still work, just maybe a bit
         * less efficiently.
         */
        if (nextidx != INVALID_PGPROCNO &&
            g_instance.proc_base_all_procs[nextidx]->clogGroupMemberPage != proc->clogGroupMemberPage) {
            proc->clogGroupMember = false;
            return false;
        }

        pg_atomic_write_u32(&proc->clogGroupNext, nextidx);

        if (pg_atomic_compare_exchange_u32(&g_instance.proc_base->clogGroupFirst, &nextidx, (uint32)proc->pgprocno))
            break;
    }

    /*
     * If the list was not empty, the leader will update the status of our
     * XID. It is impossible to have followers without a leader because the
     * first process that has added itself to the list will always have
     * nextidx as INVALID_PGPROCNO.
     */
    if (nextidx != INVALID_PGPROCNO) {
        int extraWaits = 0;

        /* Sleep until the leader updates our XID status. */
        for (;;) {
            /* acts as a read barrier */
            PGSemaphoreLock(&proc->sem, false);
            if (!proc->clogGroupMember)
                break;
            extraWaits++;
        }

        Assert(pg_atomic_read_u32(&proc->clogGroupNext) == INVALID_PGPROCNO);

        /* Fix semaphore count for any absorbed wakeups */
        while (extraWaits-- > 0)
            PGSemaphoreUnlock(&proc->sem);
        return true;
    }

    /* We are the leader.  Acquire the lock on behalf of everyone. */
    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);

    /*
     * Now that we've got the lock, clear the list of processes waiting for
     * group XID status update, saving a pointer to the head of the list.
     * Trying to pop elements one at a time could lead to an ABA problem.
     */
    nextidx = pg_atomic_exchange_u32(&g_instance.proc_base->clogGroupFirst, INVALID_PGPROCNO);

    /* Remember head of list so we can perform wakeups after dropping lock. */
    wakeidx = nextidx;

    /* Walk the list and update the status of all XIDs. */
    while (nextidx != INVALID_PGPROCNO) {
        proc = g_instance.proc_base_all_procs[nextidx];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[nextidx];

        CLogSetPageStatusInternal(proc->clogGroupMemberXid, pgxact->nxids, proc->subxids.xids,
                                  proc->clogGroupMemberXidStatus, proc->clogGroupMemberLsn, proc->clogGroupMemberPage);

        /* Move to next proc in list. */
        nextidx = pg_atomic_read_u32(&proc->clogGroupNext);
    }

    /* We're done with the lock now. */
    LWLockRelease(ClogCtl(pageno)->shared->control_lock);

    /*
     * Now that we've released the lock, go back and wake everybody up.  We
     * don't do this under the lock so as to keep lock hold times to a
     * minimum.
     */
    while (wakeidx != INVALID_PGPROCNO) {
        proc = g_instance.proc_base_all_procs[wakeidx];

        wakeidx = pg_atomic_read_u32(&proc->clogGroupNext);
        pg_atomic_write_u32(&proc->clogGroupNext, INVALID_PGPROCNO);

        /* ensure all previous writes are visible before follower continues. */
        pg_write_barrier();

        proc->clogGroupMember = false;

        if (proc != t_thrd.proc)
            PGSemaphoreUnlock(&proc->sem);
    }

    return true;
}

/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with ClogCtl(pageno)->shared->control_lock held
 */
static void CLogSetStatusBit(TransactionId xid, CLogXidStatus status, XLogRecPtr lsn, int slotno)
{
    int byteno = TransactionIdToByte(xid);
    int64 pageno = (int64)TransactionIdToPage(xid);
    uint32 bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
    unsigned char *byteptr = NULL;
    unsigned char byteval;
    unsigned char curval;

    byteptr = (unsigned char *)(ClogCtl(pageno)->shared->page_buffer[slotno] + byteno);
    curval = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

    /*
     * When replaying transactions during recovery we still need to perform
     * the two phases of subcommit and then commit. However, some transactions
     * are already correctly marked, so we just treat those as a no-op which
     * allows us to keep the following Assert as restrictive as possible.
     */
    if (t_thrd.xlog_cxt.InRecovery && status == CLOG_XID_STATUS_SUB_COMMITTED && curval == CLOG_XID_STATUS_COMMITTED)
        return;

    /*
     * Current state change should be from 0 or subcommitted to target state
     * or we should already be there when replaying changes during recovery.
     */
    if (!(curval == 0 || (curval == CLOG_XID_STATUS_SUB_COMMITTED && status != CLOG_XID_STATUS_IN_PROGRESS) ||
          curval == ((uint32)status) ||
          (curval == CLOG_XID_STATUS_COMMITTED && status == CLOG_XID_STATUS_SUB_COMMITTED)))
        ereport(PANIC, (errmsg("CLOG STATUS ERROR: xid: %lu input status %s, current status %s", xid,
                               GetTransactionStatus(status), GetTransactionStatus(curval))));

    /* note this assumes exclusive access to the clog page */
    byteval = *byteptr;
    byteval &= ~(((1U << (uint32)CLOG_BITS_PER_XACT) - 1) << bshift);
    byteval |= (((uint32)status) << bshift);
    *byteptr = byteval;

    /*
     * Update the group LSN if the transaction completion LSN is higher.
     *
     * Note: lsn will be invalid when supplied during InRecovery processing,
     * so we don't need to do anything special to avoid LSN updates during
     * recovery. After recovery completes the next clog change will set the
     * LSN correctly.
     */
    if (!XLogRecPtrIsInvalid(lsn)) {
        int lsnindex = (int)GetLSNIndex(slotno, xid);

        if (XLByteLT(ClogCtl(pageno)->shared->group_lsn[lsnindex], lsn))
            ClogCtl(pageno)->shared->group_lsn[lsnindex] = lsn;
    }
}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogFetch() in transam.c is the intended caller.
 */
CLogXidStatus CLogGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
    int64 pageno = (int64)TransactionIdToPage(xid);
    int byteno = TransactionIdToByte(xid);
    uint32 bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
    int slotno;
    int lsnindex;
    unsigned char *byteptr = NULL;
    CLogXidStatus status;

    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = SimpleLruReadPage_ReadOnly(ClogCtl(pageno), pageno, xid);
    byteptr = (unsigned char *)(ClogCtl(pageno)->shared->page_buffer[slotno] + byteno);

    status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

    lsnindex = (int)GetLSNIndex(slotno, xid);
    *lsn = ClogCtl(pageno)->shared->group_lsn[lsnindex];

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);

    return status;
}

/*
 * Number of shared CLOG buffers.
 *
@MDclog01 begin
+ * On larger multi-processor systems, it is possible to have many CLOG page
+ * requests in flight at one time which could lead to disk access for CLOG
+ * page if the required page is not found in memory.  Testing revealed that we
+ * can get the best performance by having 128 CLOG buffers, more than that it
+ * doesn't improve performance.
 *
 * Unconditionally keeping the number of CLOG buffers to 128 did not seem like
 * a good idea, because it would increase the minimum amount of shared memory
 * required to start, which could be a problem for people running very small
 * configurations.  The following formula seems to represent a reasonable
 * compromise: people with very low values for shared_buffers will get fewer
 * CLOG buffers as well, and everyone else will get 128.
@MDclog01 end
 *
 * It is likely that some further work will be needed here in future releases;
 * for example, on a 64-core server, the maximum number of CLOG requests that
 * can be simultaneously in flight will be even larger.  But that will
 * apparently require more than just changing the formula, so for now we take
 * the easy way out.
 */
Size CLOGShmemBuffers(void)
{
    return (Size)Min(256, Max(4, g_instance.attr.attr_storage.NBuffers / 512));
}

/*
 * Initialization of shared memory for CLOG
 */
Size CLOGShmemSize(void)
{
    int i = 0;
    Size sz = 0;

    for (i = 0; i < NUM_CLOG_PARTITIONS; i++) {
        sz += SimpleLruShmemSize((int)CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE);
    }

    return sz;
}

void CLOGShmemInit(void)
{
    int i = 0;
    int rc = 0;
    char name[SLRU_MAX_NAME_LENGTH];

    MemoryContext old = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    t_thrd.shemem_ptr_cxt.ClogCtl = (SlruCtlData*)palloc0(NUM_CLOG_PARTITIONS * sizeof(SlruCtlData));
    (void)MemoryContextSwitchTo(old);

    for (i = 0; i < NUM_CLOG_PARTITIONS; i++) {
        rc = sprintf_s(name, SLRU_MAX_NAME_LENGTH, "%s%d", "CLOG Ctl", i);
        securec_check_ss(rc, "", "");
        SimpleLruInit(ClogCtl(i), name, (int)LWTRANCHE_CLOG_CTL, (int)CLOGShmemBuffers(), CLOG_LSNS_PER_PAGE,
                      CBufMappingPartitionLockByIndex(i), "pg_clog");
    }
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CLOG segment.  (The CLOG directory is assumed to
 * have been created by initdb, and CLOGShmemInit must have been
 * called already.)
 */
void BootStrapCLOG(void)
{
    int slotno = 0;
    int64 pageno = 0;

    for (pageno = 0; pageno < CLOG_BATCH_SIZE; pageno++) {
        (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);
        slotno = ZeroCLOGPage(pageno, false);
        SimpleLruWritePage(ClogCtl(pageno), slotno);
        Assert(!ClogCtl(pageno)->shared->page_dirty[slotno]);
        LWLockRelease(ClogCtl(pageno)->shared->control_lock);
    }

    pageno = (int64)TransactionIdToPage(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);
    if (pageno >= CLOG_BATCH_SIZE) {
        /* Create and zero the first page of the commit log */
        slotno = ZeroCLOGPage(pageno, false);

        /* Make sure it's written out */
        SimpleLruWritePage(ClogCtl(pageno), slotno);
        Assert(!ClogCtl(pageno)->shared->page_dirty[slotno]);
    }
    LWLockRelease(ClogCtl(pageno)->shared->control_lock);
}

/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int ZeroCLOGPage(int64 pageno, bool writeXlog)
{
    int slotno;
    bool bZeroPage = true;

    slotno = SimpleLruZeroPage(ClogCtl(pageno), pageno, &bZeroPage);
    /*
     * If do not zero page in SimpleLruZeroPage, should not write XLOG.
     * Or the valid clog page might be wrongly zeroed when redo.
     */
    if (writeXlog && bZeroPage)
        WriteZeroPageXlogRec(pageno);

    return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 */
void StartupCLOG(void)
{
    TransactionId xid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    int64 pageno = (int64)TransactionIdToPage(xid);

    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);

    /*
     * Initialize our idea of the latest page number.
     */
    ClogCtl(pageno)->shared->latest_page_number = pageno;

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void TrimCLOG(void)
{
    TransactionId xid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    int64 pageno = (int64)TransactionIdToPage(xid);

    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);

    /*
     * Re-Initialize our idea of the latest page number.
     */
    ClogCtl(pageno)->shared->latest_page_number = pageno;

    /*
     * Zero out the remainder of the current clog page.  Under normal
     * circumstances it should be zeroes already, but it seems at least
     * theoretically possible that XLOG replay will have settled on a nextXID
     * value that is less than the last XID actually used and marked by the
     * previous database lifecycle (since subtransaction commit writes clog
     * but makes no WAL entry).  Let's just be safe. (We need not worry about
     * pages beyond the current one, since those will be zeroed when first
     * used.  For the same reason, there is no need to do anything when
     * nextXid is exactly at a page boundary; and it's likely that the
     * "current" page doesn't exist yet in that case.)
     */
    if (TransactionIdToPgIndex(xid) != 0) {
        int byteno = TransactionIdToByte(xid);
        uint32 bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
        int slotno;
        errno_t rc = EOK;
        unsigned char *byteptr = NULL;

        slotno = SimpleLruReadPage(ClogCtl(pageno), pageno, false, xid);
        byteptr = (unsigned char *)(ClogCtl(pageno)->shared->page_buffer[slotno] + byteno);

        /* Zero so-far-unused positions in the current byte */
        *byteptr &= (1U << bshift) - 1;
        /* Zero the rest of the page */
        rc = memset_s(byteptr + 1, (size_t)(BLCKSZ - byteno - 1), 0, (size_t)(BLCKSZ - byteno - 1));
        securec_check(rc, "", "");

        ClogCtl(pageno)->shared->page_dirty[slotno] = true;
    } else
        /* We need to set force_check_first_xid true, see slru.h */
        ClogCtl(pageno)->shared->force_check_first_xid = true;

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void ShutdownCLOG(void)
{
    /* Flush dirty CLOG pages to disk */
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(false);
    for (int i = 0; i < NUM_CLOG_PARTITIONS; i++) {
        (void)SimpleLruFlush(ClogCtl(i), false);
    }
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void CheckPointCLOG(void)
{
    /* Flush dirty CLOG pages to disk */
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_START(true);
    int flush_num = 0;
    for (int i = 0; i < NUM_CLOG_PARTITIONS; i++) {
        flush_num += SimpleLruFlush(ClogCtl(i), true);
    }
    g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_clog_flush_num += flush_num;
    TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE(true);
}

/*
 * Get the max page no of segment file.
 * Note that the caller must hold LW_EXCLUSIVE on ClogCtl(pageno)->shared->control_lock
 */
int ClogSegCurMaxPageNo(char *path, int64 pageno)
{
    /*
     * In a crash-and-restart situation, it's possible for us to receive
     * commands to set the commit status of transactions whose bits are in
     * already-truncated segments of the commit log (see notes in
     * SlruPhysicalWritePage).	Hence, if we are InRecovery, allow the case
     * where the file doesn't exist, and return -1 instead.
     */
    int fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        if (errno != ENOENT) {
            Assert(!t_thrd.xlog_cxt.InRecovery);
            LWLockRelease(ClogCtl(pageno)->shared->control_lock);
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Open file %s failed. %s\n", path, strerror(errno))));
        }

        ereport(LOG, (errmsg("file \"%s\" doesn't exist", path)));

        /*
         * for an existing clog segment file, its valid page number is between
         * 0 ~ SLRU_PAGES_PER_SEGMENT-1. becuase required clog file doesn't
         * exist, reutrn -1 instead of 0. see also the caller ClogPageHasBeenExtended().
         */
        return -1;
    }

    off_t endOffset = lseek(fd, 0, SEEK_END);
    if (endOffset < 0) {
        LWLockRelease(ClogCtl(pageno)->shared->control_lock);
        if (close(fd))
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Close file %s failed. %s\n", path, strerror(errno))));
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("seek file %s failed. %s\n", path, strerror(errno))));
    }
    if (close(fd)) {
        LWLockRelease(ClogCtl(pageno)->shared->control_lock);
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("Close file %s failed. %s\n", path, strerror(errno))));
    }

    return (int)(endOffset / BLCKSZ - 1);
}

/*
 * Check whether ClogPage has been extended
 * The caller must hold LW_EXCLUSIVE on ClogCtl(pageno)->shared->control_lock
 * true - means has been extended
 * false - means might be not extended or extended, not sure.
 */
bool ClogPageHasBeenExtended(int64 pageno, int64 &maxPageNoInSeg)
{
    SlruShared shared = ClogCtl(pageno)->shared;
    char path[MAXPGPATH];
    int64 rpageNo, segno;
    int64 segStartPageNo = 0;
    int64 segEndPageNo = 0;
    int64 maxPageNoInBuffer = -1;
    int rc = 0;

    maxPageNoInSeg = -1;
    segno = pageno / SLRU_PAGES_PER_SEGMENT;

    if (shared->latest_page_number == pageno && !shared->force_check_first_xid)
        return true;

    /*
     * See if CLOG page has been extended in buffer.
     *
     * Because future XID may be accessed, so SLRU_PAGE_READ_IN_PROGRESS doen't assure that
     * this page exists in disk.
     *
     * if this page has been assigned a buffer, and its status is normal (SLRU_PAGE_VALID)
     * or in IO progress of writing (SLRU_PAGE_WRITE_IN_PROGRESS), it has been extended in memory.
     */
    segStartPageNo = segno * SLRU_PAGES_PER_SEGMENT;
    segEndPageNo = segStartPageNo + SLRU_PAGES_PER_SEGMENT - 1;
    for (int i = 0; i < shared->num_slots; i++) {
        if (shared->page_number[i] == pageno) {
            if (shared->page_status[i] == SLRU_PAGE_VALID || shared->page_status[i] == SLRU_PAGE_WRITE_IN_PROGRESS)
                return true;
        } else if (shared->page_status[i] == SLRU_PAGE_VALID || shared->page_status[i] == SLRU_PAGE_WRITE_IN_PROGRESS) {
            if (shared->page_number[i] >= segStartPageNo && shared->page_number[i] <= segEndPageNo &&
                shared->page_number[i] > maxPageNoInBuffer) {
                maxPageNoInBuffer = shared->page_number[i];
            }
        }
    }

    /* If we can find large page number of the segno in clog shmem buffer, the page has been extended. */
    if (maxPageNoInBuffer >= pageno) {
        return true;
    }

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%04X%08X", (ClogCtl(pageno))->dir,
                    (uint32)((uint64)(segno) >> 32), (uint32)((segno) & (int64)0xFFFFFFFF));
    securec_check_ss(rc, "", "");
    rpageNo = ClogSegCurMaxPageNo(path, pageno);
    maxPageNoInSeg = rpageNo + segno * SLRU_PAGES_PER_SEGMENT;

    if (maxPageNoInBuffer > maxPageNoInSeg) {
        maxPageNoInSeg = maxPageNoInBuffer;
    }
    if (pageno > maxPageNoInSeg) {
        return false;
    }

    /* The page has been extended */
    return true;
}

/*
 * Make sure that CLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void ExtendCLOG(TransactionId newestXact, bool allowXlog)
{
    int64 pageno = 0;

#ifdef PGXC
    int64 maxPageNoInSeg = 0;
    /*
     * In PGXC, it may be that a node is not involved in a transaction,
     * and therefore will be skipped, so we need to detect this by using
     * the latest_page_number instead of the pg index.
     *
     * Also, there is a special case of when transactions wrap-around that
     * we need to detect.
     */
    pageno = (int64)TransactionIdToPage(newestXact);

    /*
     * Just do a check. In most case, it will return.
     * We don't acquire a LW_SHARD on ClogCtl(pageno)->shared->control_lock when do this check
     * in order to improve performance. It is safe because ClogPageHasBeenExtended will
     * recheck it when this check is not right.
     */
    if (ClogCtl(pageno)->shared->latest_page_number == pageno && !ClogCtl(pageno)->shared->force_check_first_xid)
        return;

    /*  Check whether this pageno has been extended */
    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);
    if (ClogPageHasBeenExtended(pageno, maxPageNoInSeg)) {
        if (ClogCtl(pageno)->shared->force_check_first_xid)
            ClogCtl(pageno)->shared->force_check_first_xid = false;

        LWLockRelease(ClogCtl(pageno)->shared->control_lock);
        return;
    }

    /* Now we need extend clog pages belong to this partition and from maxPageNoInSeg + 1 to pageno
     * for example, partitionId = 1, maxPageInSeg = 3 amd pageno = 256*3+1, then page 256*1+1, 256*2+1,
     * 256*3+1 will be extended, when page 256*3+1 in memory of partition1, then other partition cannot
     * see it, to extend clog independently, otherwise in disk, others see it as hole file, the pageno
     * smaller that 256*3+1 considered as extended, the file system make sure that hole in the log-file
     * can be read, get all 0.
     * even the extending over current file will be ok.
     */
    int64 partitionId = pageno % NUM_CLOG_PARTITIONS;
    int64 i = (maxPageNoInSeg + 1) / NUM_CLOG_PARTITIONS * NUM_CLOG_PARTITIONS + partitionId;
    if (i < (maxPageNoInSeg + 1)) {
        i += NUM_CLOG_PARTITIONS;
    }

    for (; i <= pageno; i += NUM_CLOG_PARTITIONS) {
        int64 tmp_max_segno = 0;
        if (!ClogPageHasBeenExtended(i, tmp_max_segno)) {
            (void)ZeroCLOGPage(i, !t_thrd.xlog_cxt.InRecovery && allowXlog);
        }
    }

    if (ClogCtl(pageno)->shared->force_check_first_xid)
        ClogCtl(pageno)->shared->force_check_first_xid = false;

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);

#else
    /*
     * No work except at first XID of a page.
     */
    if (TransactionIdToPgIndex(newestXact) != 0 && !TransactionIdEquals(newestXact, FirstNormalTransactionId))
        return;

    pageno = TransactionIdToPage(newestXact);

    (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);

    /* Zero the page and make an XLOG entry about it */
    ZeroCLOGPage(pageno, !t_thrd.xlog_cxt.InRecovery);

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);

#endif
}

/*
 * Remove all CLOG segments before the one holding the passed transaction ID
 *
 * Before removing any CLOG data, we must flush XLOG to disk, to ensure
 * that any recently-emitted HEAP_FREEZE records have reached disk; otherwise
 * a crash and restart might leave us with some unfrozen tuples referencing
 * removed CLOG data.  We choose to emit a special TRUNCATE XLOG record too.
 * Replaying the deletion from XLOG is not critical, since the files could
 * just as well be removed later, but doing so prevents a long-running hot
 * standby server from acquiring an unreasonably bloated CLOG directory.
 *
 * Since CLOG segments hold a large number of transactions, the opportunity to
 * actually remove a segment is fairly rare, and so it seems best not to do
 * the XLOG flush unless we have confirmed that there is a removable segment.
 */
void TruncateCLOG(TransactionId oldestXact)
{
    int64 cutoffPage;

    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.
     */
    cutoffPage = TransactionIdToPage(oldestXact);
    /* Check to see if there's any files that could be removed */
    if (!SlruScanDirectory(ClogCtl(cutoffPage), SlruScanDirCbReportPresence, &cutoffPage))
        return; /* nothing to remove */

    /* Write XLOG record and flush XLOG to disk */
    WriteTruncateXlogRec(cutoffPage);

    /* Now we can remove the old CLOG segment(s) */
    SimpleLruTruncate(ClogCtl(0), cutoffPage, NUM_CLOG_PARTITIONS);
    DeleteObsoleteTwoPhaseFile(cutoffPage);

    ereport(LOG, (errmsg("Truncate CLOG at xid %lu", oldestXact)));
}

/*
 * Write a ZEROPAGE xlog record
 */
static void WriteZeroPageXlogRec(int64 pageno)
{
    XLogBeginInsert();
    XLogRegisterData((char *)(&pageno), sizeof(int64));
    (void)XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
static void WriteTruncateXlogRec(int64 pageno)
{
    XLogRecPtr recptr;

    XLogBeginInsert();
    XLogRegisterData((char *)(&pageno), sizeof(int64));
    recptr = XLogInsert(RM_CLOG_ID, CLOG_TRUNCATE);
    XLogWaitFlush(recptr);
}

/*
 * CLOG resource manager's routines
 */
void clog_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    errno_t rc = EOK;

    /* Backup blocks are not used in clog records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == CLOG_ZEROPAGE) {
        int64 pageno;
        int slotno;

        rc = memcpy_s(&pageno, sizeof(int64), XLogRecGetData(record), sizeof(int64));
        securec_check(rc, "", "");

        (void)LWLockAcquire(ClogCtl(pageno)->shared->control_lock, LW_EXCLUSIVE);

        slotno = ZeroCLOGPage(pageno, false);
        SimpleLruWritePage(ClogCtl(pageno), slotno);
        Assert(!ClogCtl(pageno)->shared->page_dirty[slotno]);

        LWLockRelease(ClogCtl(pageno)->shared->control_lock);
    } else if (info == CLOG_TRUNCATE) {
        int64 pageno;

        rc = memcpy_s(&pageno, sizeof(int64), XLogRecGetData(record), sizeof(int64));
        securec_check(rc, "", "");

        /*
         * During XLOG replay, latest_page_number isn't set up yet; insert a
         * suitable value to bypass the sanity test in SimpleLruTruncate.
         */
        ClogCtl(pageno)->shared->latest_page_number = pageno;

        SimpleLruTruncate(ClogCtl(0), pageno, NUM_CLOG_PARTITIONS);
        DeleteObsoleteTwoPhaseFile(pageno);
    } else
        ereport(PANIC, (errmsg("clog_redo: unknown op code %u", (uint32)info)));
}

#ifdef USE_ASSERT_CHECKING

/*
 * @Description: read clog pages whose numbers are between clog_pgno and clog_pgno+count-1.
 *               clog_pgno must be >= 0.
 * @IN clog_pgno: which clog pageno to start reading
 * @IN count: how many pages to read
 * @Return: const value 0.
 * @See also:
 */
int FIT_clog_read_page(int64 clog_pgno, int count)
{
    if (clog_pgno < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("clog pageno should be >= 0")));
    }

    ereport(LOG, (errmsg("FIT: read clog %d pages from pageno %lu", count, clog_pgno)));

    for (int i = 0; i < count; ++i) {
        /* fetch one transaction id within page *clog_pgno* */
        TransactionId xid = ((TransactionId)clog_pgno) * CLOG_XACTS_PER_PAGE;
        xid += FirstNormalTransactionId;

        /* read this clog page */
        XLogRecPtr lsn;
        (void)CLogGetStatus(xid, &lsn);

        /* read next clog page */
        clog_pgno++;
    }
    return 0;
}

/*
 * @Description: extend clog page from clog_pgno to clog_pgno+count-1.
 * @IN clog_pgno: which clog pageno to start extending
 * @IN count: how many pages to extend
 * @Return: const value 0.
 * @See also:
 */
int FIT_clog_extend_page(int64 clog_pgno, int count)
{
    if (clog_pgno < 0) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("clog pageno should be >= 0")));
    }

    ereport(LOG, (errmsg("FIT: extend clog %d pages from pageno %lu", count, clog_pgno)));

    for (int i = 0; i < count; ++i) {
        /* fetch one transaction id within page *clog_pgno* */
        TransactionId xid = ((TransactionId)clog_pgno) * CLOG_XACTS_PER_PAGE;
        xid += FirstNormalTransactionId;

        /* extend this clog page */
        (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        ExtendCLOG(xid);
        LWLockRelease(XidGenLock);

        /* extend next clog page */
        clog_pgno++;
    }
    return 0;
}

int FIT_lw_deadlock(int n_edges)
{
    if (n_edges == 1) {
        (void)LWLockAcquire(OBSGetPathLock, LW_EXCLUSIVE);
        /* deadlock happens */
        (void)LWLockAcquire(OBSGetPathLock, LW_SHARED);
        LWLockRelease(OBSGetPathLock);
    }
    return 0;
}

/* ---------- description move FIT codes into signle file -------------- */
/*
 * @Description: convert a TEXT string into integer value
 * @IN arg: TEXT string
 * @Return: integer value
 * @See also:
 */
static inline int conv_text2int(text *arg)
{
    char *cstr = text_to_cstring(arg);
    int val = atoi(cstr);
    pfree_ext(cstr);
    return val;
}

/*
 * @Description: implement fault imjection for every type.
 * @IN arg1: input argument1
 * @IN arg2: input argument2
 * @IN arg3: input argument3
 * @IN arg4: input argument4
 * @IN arg5: input argument5
 * @IN fit_type: fault injection type
 * @Return: 0, sucess; otherwise failed.
 * @See also:
 */
static int gs_fault_inject_impl(int64 fit_type, text *arg1, text *arg2, text *arg3, text *arg4, text *arg5)
{
    int ret = 0;
    switch (fit_type) {
#ifdef FAULT_INJECTION_TEST
        case FIT_CLOG_EXTEND_PAGE:
            ret = FIT_clgaog_extend_page(conv_text2int(arg1), conv_text2int(arg2));
            break;
        case FIT_CLOG_READ_PAGE:
            ret = FIT_clog_read_page(conv_text2int(arg1), conv_text2int(arg2));
            break;
#endif /* FAULT_INJECTION_TEST */
        case FIT_LWLOCK_DEADLOCK:
            ret = FIT_lw_deadlock(conv_text2int(arg1));
            break;
        default:
            /* nothing to do */
            break;
    }
    return ret;
}

#endif

/* the type of each input arguments is defined by its fault type. */
Datum gs_fault_inject(PG_FUNCTION_ARGS)
{
#ifdef USE_ASSERT_CHECKING
    int ret = gs_fault_inject_impl(PG_GETARG_INT64(0), PG_GETARG_TEXT_P(1), PG_GETARG_TEXT_P(2), PG_GETARG_TEXT_P(3),
                                   PG_GETARG_TEXT_P(4), PG_GETARG_TEXT_P(5));
    PG_RETURN_INT64(ret);
#else
    ereport(WARNING, (errmsg("unsupported fault injection")));
    PG_RETURN_INT64(0);
#endif
}
