/* -------------------------------------------------------------------------
 *
 * varsup.cpp
 *	  openGauss OID & XID variables support routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/transam/varsup.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/distribute_test.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/gtm.h"
#include "storage/procarray.h"
#endif

/* Number of OIDs to prefetch (preallocate) per XLOG write */
#define VAR_OID_PREFETCH 8192
#define WHITEBOX_EXTEND_XID 100000

#ifdef PGXC /* PGXC_DATANODE */
/*
 * Set next transaction id to use
 */
void SetNextTransactionId(TransactionId xid, bool updateLatestCompletedXid)
{
    if (TransactionIdIsNormal(t_thrd.xact_cxt.next_xid) && module_logging_is_on(MOD_TRANS_XACT))
        ereport(LOG,
                (errmodule(MOD_TRANS_XACT), errmsg("[from \"g\" msg]setting xid = " XID_FMT ", old_value = " XID_FMT,
                                                   xid, t_thrd.xact_cxt.next_xid)));

    t_thrd.xact_cxt.next_xid = xid;

    /*
     * Use volatile pointer to prevent code rearrangement; other backends
     * could be examining my subxids info concurrently. Note we are assuming that
     * TransactionId and int fetch/store are atomic.
     */
    volatile PGXACT *mypgxact = t_thrd.pgxact;
    mypgxact->next_xid = t_thrd.xact_cxt.next_xid;
}

/*
 * Allow force of getting XID from GTM
 * Useful for explicit VACUUM (autovacuum already handled)
 */
void SetForceXidFromGTM(bool value)
{
    t_thrd.xact_cxt.force_get_xid_from_gtm = value;
}

/*
 * See if we should force using GTM
 * Useful for explicit VACUUM (autovacuum already handled)
 */
bool GetForceXidFromGTM(void)
{
    return t_thrd.xact_cxt.force_get_xid_from_gtm;
}
#endif /* PGXC */

#ifdef DEBUG
bool FastAdvanceXid(void)
{
    if (u_sess->attr.attr_storage.ustore_verify_level <= USTORE_VERIFY_COMPLETE) {
        return true;
    }
    t_thrd.xact_cxt.ShmemVariableCache->nextXid += WHITEBOX_EXTEND_XID;
    return true;
}
#endif

/*
 * Allocate the next XID for a new transaction or subtransaction.
 *
 * The new XID is also stored into MyPgXact before returning.
 *
 * Note: when this is called, we are actually already inside a valid
 * transaction, since XIDs are now not allocated until the transaction
 * does something.	So it is safe to do a database lookup if we want to
 * issue a warning about XID wrap.
 */
#ifdef PGXC
TransactionId GetNewTransactionId(bool isSubXact, TransactionState s)
#else
TransactionId GetNewTransactionId(bool isSubXact)
#endif
{
    /*
     * volatile xid is to prevent code rearrangement
     * during PG_TRY/PG_CATCH/PG_END_TRY
     */
    volatile TransactionId xid;

    /*
     * During bootstrap initialization, we return the special bootstrap
     * transaction id.
     */
    if (IsBootstrapProcessingMode()) {
        Assert(!isSubXact);
        t_thrd.pgxact->xid = BootstrapTransactionId;
        return BootstrapTransactionId;
    }

    /* safety check, we should never get this far in a HS slave */
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                        errmsg("cannot assign TransactionIds during recovery")));
    /* we are about to start streaming switch over, stop new xid to stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                errmsg("cannot assign TransactionIds during streaming disaster recovery")));
    }
    if (SSIsServerModeReadOnly()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
                        errmsg("cannot assign TransactionIds at Standby with DMS enabled")));
    }

    (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    xid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;

    /*
     * Check to see if it's safe to assign another XID.
     * If we're past xidVacLimit, start trying to force autovacuum cycles.
     */
    if (TransactionIdFollowsOrEquals(xid, t_thrd.xact_cxt.ShmemVariableCache->xidVacLimit)) {
        /*
         * For safety's sake, we release XidGenLock while sending signals,
         * warnings, etc.  This is not so much because we care about
         * preserving concurrency in this situation, as to avoid any
         * possibility of deadlock while doing get_database_name(). First,
         * copy all the shared values we'll need in this path.
         */
        LWLockRelease(XidGenLock);

        /*
         * To avoid swamping the postmaster with signals, we issue the autovac
         * request only once per 64K transaction starts.
         */
        if (IsUnderPostmaster && (xid % 65536) == 0)
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

        /* Re-acquire lock and start over */
        (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

        xid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    }

    /*
     * If we are allocating the first XID of a new page of the commit log,
     * zero out that commit-log page before returning. We must do this while
     * holding XidGenLock, else another xact could acquire and commit a later
     * XID before we zero the page.  Fortunately, a page of the commit log
     * holds 32K or more transactions, so we don't have to do this very often.
     *
     * Extend pg_subtrans too.
     */
    ExtendCLOG(xid);
    ExtendCSNLOG(xid);

    TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
#ifdef DEBUG
    FastAdvanceXid();
#endif
    /*
     * We must store the new XID into the shared ProcArray before releasing
     * XidGenLock.	This ensures that every active XID older than
     * latestCompletedXid is present in the ProcArray, which is essential for
     * correct OldestXmin tracking; see src/backend/access/transam/README.
     *
     * XXX by storing xid into MyPgXact without acquiring ProcArrayLock, we
     * are relying on fetch/store of an xid to be atomic, else other backends
     * might see a partially-set xid here.	But holding both locks at once
     * would be a nasty concurrency hit.  So for now, assume atomicity.
     *
     * Note that readers of PGXACT xid fields should be careful to fetch the
     * value only once, rather than assume they can read a value multiple
     * times and get the same answer each time.
     *
     * The same comments apply to the subxact xid count and overflow fields.
     *
     * A solution to the atomic-store problem would be to give each PGXACT its
     * own spinlock used only for fetching/storing that PGXACT's xid and
     * related fields.
     *
     * If there's no room to fit a subtransaction XID into PGPROC, set the
     * cache-overflowed flag instead.  This forces readers to look in
     * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
     * race-condition window, in that the new XID will not appear as running
     * until its parent link has been placed into pg_subtrans. However, that
     * will happen before anyone could possibly have a reason to inquire about
     * the status of the XID, so it seems OK.  (Snapshots taken during this
     * window *will* include the parent XID, so they will deliver the correct
     * answer later on when someone does have a reason to inquire.)
     */
    {
        /*
         * Use volatile pointer to prevent code rearrangement; other backends
         * could be examining my subxids info concurrently, and we don't want
         * them to see an invalid intermediate state, such as incrementing
         * nxids before filling the array entry.  Note we are assuming that
         * TransactionId and int fetch/store are atomic.
         */
        volatile PGPROC *myproc = t_thrd.proc;
        volatile PGXACT *mypgxact = t_thrd.pgxact;

        if (!isSubXact) {
            mypgxact->xid = xid;
        } else {
            int nxids = mypgxact->nxids;

            /* Allocate or realloc memory if needed */
            if (myproc->subxids.maxNumber == 0 || nxids >= myproc->subxids.maxNumber) {
                Assert(ProcSubXidCacheContext);
                if (myproc->subxids.maxNumber == 0) {
                    /* Init memory */
                    HOLD_INTERRUPTS();
                    MemoryContext oldContext = MemoryContextSwitchTo(ProcSubXidCacheContext);
                    myproc->subxids.xids = (TransactionId *)palloc(sizeof(TransactionId) * PGPROC_INIT_CACHED_SUBXIDS);
                    myproc->subxids.maxNumber = PGPROC_INIT_CACHED_SUBXIDS;
                    (void)MemoryContextSwitchTo(oldContext);
                    RESUME_INTERRUPTS();
                } else if (nxids >= myproc->subxids.maxNumber) {
                    int maxNumber = myproc->subxids.maxNumber * 2;
                    /* Realloc, use subxidsLock to protect subxids */
                    (void)LWLockAcquire(myproc->subxidsLock, LW_EXCLUSIVE);
                    myproc->subxids.xids = (TransactionId *)repalloc(myproc->subxids.xids,
                                                                     sizeof(TransactionId) * maxNumber);
                    myproc->subxids.maxNumber = maxNumber;
                    LWLockRelease(myproc->subxidsLock);
                }
            }

            myproc->subxids.xids[nxids] = xid;
            mypgxact->nxids = nxids + 1;
        }
    }
    /* when we use local snapshot, latestcomplete xid is very important for us. CHECK this here */
    if (TransactionIdFollowsOrEquals(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, xid))
        ereport(PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("GTM-FREE-MODE: latestCompletedXid %lu larger than next alloc xid %lu.",
                               t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, xid)));

    LWLockRelease(XidGenLock);
    return xid;
}

/*
 * Read nextXid but don't allocate it.
 */
TransactionId ReadNewTransactionId(void)
{
    TransactionId xid;

    (void)LWLockAcquire(XidGenLock, LW_SHARED);
    xid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    LWLockRelease(XidGenLock);

    return xid;
}

/*
 * Determine the last safe XID to allocate given the currently oldest
 * datfrozenxid (ie, the oldest XID that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 */
void SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid)
{
    TransactionId xidVacLimit;
    TransactionId curXid;

    Assert(TransactionIdIsNormal(oldest_datfrozenxid));

    /*
     * We'll start trying to force autovacuums when oldest_datfrozenxid gets
     * to be more than autovacuum_freeze_max_age transactions old.
     *
     * Note: autovacuum_freeze_max_age is a PGC_POSTMASTER parameter so that
     * we don't have to worry about dealing with on-the-fly changes in its
     * value.  It doesn't look practical to update shared state from a GUC
     * assign hook (too many processes would try to execute the hook,
     * resulting in race conditions as well as crashes of those not connected
     * to shared memory).  Perhaps this can be improved someday.
     */
    xidVacLimit = oldest_datfrozenxid + g_instance.attr.attr_storage.autovacuum_freeze_max_age;
    if (xidVacLimit < FirstNormalTransactionId)
        xidVacLimit += FirstNormalTransactionId;

    /* Grab lock for just long enough to set the new limit values */
    (void)LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    t_thrd.xact_cxt.ShmemVariableCache->oldestXid = oldest_datfrozenxid;
    t_thrd.xact_cxt.ShmemVariableCache->xidVacLimit = xidVacLimit;
    t_thrd.xact_cxt.ShmemVariableCache->oldestXidDB = oldest_datoid;
    curXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    LWLockRelease(XidGenLock);

    /*
     * If past the autovacuum force point, immediately signal an autovac
     * request.  The reason for this is that autovac only processes one
     * database per invocation.  Once it's finished cleaning up the oldest
     * database, it'll call here, and we'll signal the postmaster to start
     * another iteration immediately if there are still any old databases.
     */
    if (TransactionIdFollowsOrEquals(curXid, xidVacLimit) && IsUnderPostmaster && !t_thrd.xlog_cxt.InRecovery)
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
}

/*
 * GetNewObjectId -- allocate a new OID
 *
 * OIDs are generated by a cluster-wide counter.  Since they are only 32 bits
 * wide, counter wraparound will occur eventually, and therefore it is unwise
 * to assume they are unique unless precautions are taken to make them so.
 * Hence, this routine should generally not be used directly. The only
 * direct callers should be GetNewOid() and GetNewRelFileNode() in
 * catalog/catalog.c.
 *
 * IsToastRel: At present, this parameter might be useful only when called from
 * toast_save_dutam during inplace upgrade. See comments below.
 */
Oid GetNewObjectId(bool IsToastRel)
{
    Oid result;

    /* safety check, we should never get this far in a HS slave */
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION), errmsg("cannot assign OIDs during recovery")));

    if (SSIsServerModeReadOnly()) {
        ereport(ERROR, (errmsg("cannot assign OIDs at Standby with DMS enabled")));
    }
    /*
     * During inplace or online upgrade, if newly added system objects are
     * to be pinned, we set their oids by GUC parameters, such as
     * Inplace_upgrade_next_heap_pg_class_oid for a new catalog. If newly
     * added system objects are not to be pinned, e.g. system views, their oids
     * are assigned between FirstBootstrapObjectId and FirstNormalTransactionId.
     *
     * When getting new chunk_id for toast tuple, we turn back to global oid
     * assignment. InplaceUpgradeNextOid is not persistent and can easily wrap
     * around. This is OK for normal catalogs with oid indexes and SnapshotNow,
     * but is a disaster for toast tables with SnapshotToast.
     */
    if (u_sess->attr.attr_common.IsInplaceUpgrade && !IsToastRel) {
        if (t_thrd.xact_cxt.InplaceUpgradeNextOid >= FirstNormalObjectId)
            t_thrd.xact_cxt.InplaceUpgradeNextOid = FirstBootstrapObjectId;

        result = t_thrd.xact_cxt.InplaceUpgradeNextOid;
        t_thrd.xact_cxt.InplaceUpgradeNextOid++;
        return result;
    }

    (void)LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

    /*
     * Check for wraparound of the OID counter.  We *must* not return 0
     * (InvalidOid); and as long as we have to check that, it seems a good
     * idea to skip over everything below FirstNormalObjectId too. (This
     * basically just avoids lots of collisions with bootstrap-assigned OIDs
     * right after a wrap occurs, so as to avoid a possibly large number of
     * iterations in GetNewOid.)  Note we are relying on unsigned comparison.
     *
     * During initdb, we start the OID generator at FirstBootstrapObjectId, so
     * we only wrap if before that point when in bootstrap or standalone mode.
     * The first time through this routine after normal postmaster start, the
     * counter will be forced up to FirstNormalObjectId.  This mechanism
     * leaves the OIDs between FirstBootstrapObjectId and FirstNormalObjectId
     * available for automatic assignment during initdb, while ensuring they
     * will never conflict with user-assigned OIDs.
     */
    if (t_thrd.xact_cxt.ShmemVariableCache->nextOid < ((Oid)FirstNormalObjectId)) {
        if (IsPostmasterEnvironment) {
            /* wraparound, or first post-initdb assignment, in normal mode */
            t_thrd.xact_cxt.ShmemVariableCache->nextOid = FirstNormalObjectId;
            t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;
        } else {
            /* we may be bootstrapping, so don't enforce the full range */
            if (t_thrd.xact_cxt.ShmemVariableCache->nextOid < ((Oid)FirstBootstrapObjectId)) {
                /* wraparound in standalone mode (unlikely but possible) */
                t_thrd.xact_cxt.ShmemVariableCache->nextOid = FirstNormalObjectId;
                t_thrd.xact_cxt.ShmemVariableCache->oidCount = 0;
            }
        }
    }

    /* If we run out of logged for use oids then we must log more */
    if (t_thrd.xact_cxt.ShmemVariableCache->oidCount == 0) {
        XLogPutNextOid(t_thrd.xact_cxt.ShmemVariableCache->nextOid + VAR_OID_PREFETCH);
        t_thrd.xact_cxt.ShmemVariableCache->oidCount = VAR_OID_PREFETCH;
    }

    result = t_thrd.xact_cxt.ShmemVariableCache->nextOid;

    (t_thrd.xact_cxt.ShmemVariableCache->nextOid)++;
    (t_thrd.xact_cxt.ShmemVariableCache->oidCount)--;

    LWLockRelease(OidGenLock);

    return result;
}

/*
 * Check nextXid >= xidWarnLimit ?
 * The 64-bit xid feature does not require warnlimit, so set it to maximum xid.
 */
Datum pg_check_xidlimit(PG_FUNCTION_ARGS)
{
    TransactionId nextXid;
    TransactionId xidWarnLimit;

    /* Locking is probably not really necessary */
    nextXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    xidWarnLimit = MaxTransactionId; /* ShmemVariableCache->xidWarnLimit; */

    PG_RETURN_BOOL(TransactionIdFollowsOrEquals(nextXid, xidWarnLimit));
}

/*
 * Get the TransactionId information of ShmemVariableCache
 */
Datum pg_get_xidlimit(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    bool firstcall = false;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        firstcall = true;

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(7, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nextXid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "oldestXid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "xidVacLimit", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "xidWarnLimit", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "xidStopLimit", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "xidWrapLimit", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "oldestXidDB", OIDOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (firstcall) {
        Datum values[7];
        bool nulls[7];
        HeapTuple tuple;
        Datum result;
        errno_t rc;

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[0] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        values[1] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->oldestXid);
        values[2] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->xidVacLimit);
        values[3] = TransactionIdGetDatum(0);
        values[4] = TransactionIdGetDatum(0);
        values[5] = TransactionIdGetDatum(0);
        values[6] = ObjectIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->oldestXidDB);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * Get information of ShmemVariableCache
 */
Datum pg_get_variable_info(PG_FUNCTION_ARGS)
{
#define VARIABLE_INFO_ATTRS 11
    FuncCallContext *funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* Create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(VARIABLE_INFO_ATTRS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "nodeName", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "nextOid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "nextXid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "oldestXid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "xidVacLimit", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "oldestXidDB", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "lastExtendCSNLogpage", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "startExtendCSNLogpage", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "nextCommitSeqNo", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "latestCompletedXid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "startupMaxXid", XIDOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        /* Only one tuple */
        funcctx->max_calls = 1;

        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[VARIABLE_INFO_ATTRS];
        bool nulls[VARIABLE_INFO_ATTRS];
        HeapTuple tuple;
        Datum result;
        errno_t rc;

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[1] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextOid);
        values[2] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        values[3] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->oldestXid);
        values[4] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->xidVacLimit);
        values[5] = ObjectIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->oldestXidDB);
        values[6] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->lastExtendCSNLogpage);
        values[7] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->startExtendCSNLogpage);
        values[8] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
        values[9] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid);
        values[10] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->startupMaxXid);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

const unsigned NODE_XID_CSN_VIEW_COL_NUM = 3;

/* Get the head row of the view of nextXid and nextCSN */
TupleDesc get_xid_csn_view_frist_row()
{
    TupleDesc tupdesc = NULL;
    tupdesc = CreateTemplateTupleDesc(NODE_XID_CSN_VIEW_COL_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "next_xid", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "next_csn", XIDOID, -1, 0);
    return BlessTupleDesc(tupdesc);
}

/* Fetch the local nextXid and nextCSN values */
HeapTuple fetch_local_xid_csn_view_values(FuncCallContext *funcctx)
{
    Datum values[NODE_XID_CSN_VIEW_COL_NUM];
    bool nulls[NODE_XID_CSN_VIEW_COL_NUM] = {false};
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[1] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
    values[2] = TransactionIdGetDatum(t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
    return heap_form_tuple(funcctx->tuple_desc, values, nulls);
}

/* Fetch the remote nextXid and nextCSN values */
HeapTuple fetch_remote_view_values(FuncCallContext *funcctx, unsigned col_num)
{
    HeapTuple tuple = NULL;
    if (col_num == 0) {
        return tuple;
    }
    Datum values[col_num];
    bool nulls[col_num];
    Tuplestorestate *tupstore = ((TableDistributionInfo *)funcctx->user_fctx)->state->tupstore;
    TupleTableSlot *slot = ((TableDistributionInfo *)funcctx->user_fctx)->slot;

    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
        FreeParallelFunctionState(((TableDistributionInfo *)funcctx->user_fctx)->state);
        ExecDropSingleTupleTableSlot(slot);
        pfree_ext(funcctx->user_fctx);
        funcctx->user_fctx = NULL;
        return tuple;
    }

    for (unsigned int i = 0; i < col_num; i++) {
        values[i] = tableam_tslot_getattr(slot, i + 1, &nulls[i]);
    }
    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    (void)ExecClearTuple(slot);
    return tuple;
}

/* Get nextXid and nextCommitSeqNo of all nodes and provide a view */
Datum gs_get_next_xid_csn(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    /* get the first row of this view */
    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        funcctx->tuple_desc = get_xid_csn_view_frist_row();
        /* for coordinator, get a view of all nodes */
        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            funcctx->max_calls = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
            funcctx->user_fctx = get_remote_node_xid_csn(funcctx->tuple_desc);
        } else {
            /* for datanode, get a view of local */
            funcctx->max_calls = 1;
        }
        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    HeapTuple tuple = NULL;
    if (funcctx->call_cntr == 0) {
        /* get local xid and csn values */
        tuple = fetch_local_xid_csn_view_values(funcctx);
    } else if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && funcctx->user_fctx) {
        /* get remote xid and csn values */
        tuple = fetch_remote_view_values(funcctx, NODE_XID_CSN_VIEW_COL_NUM);
    }
    if (tuple) {
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}
