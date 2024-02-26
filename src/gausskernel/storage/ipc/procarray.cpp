/* -------------------------------------------------------------------------
 *
 * procarray.cpp
 *	  openGauss process array code.
 *
 *
 * This module maintains arrays of the PGPROC and PGXACT structures for all
 * active backends.  Although there are several uses for this, the principal
 * one is as a means of determining the set of currently running transactions.
 *
 * Because of various subtle race conditions it is critical that a backend
 * hold the correct locks while setting or clearing its MyPgXact->xid field.
 * See notes in src/backend/access/transam/README.
 *
 * The process arrays now also include structures representing prepared
 * transactions.  The xid and subxids fields of these are valid, as are the
 * myProcLocks lists.  They can be distinguished from regular backend PGPROCs
 * at need by checking for pid == 0.
 *
#ifdef PGXC
 * Vanilla PostgreSQL assumes maximum TransactinIds in any snapshot is
 * arrayP->maxProcs.  It does not apply to XC because XC's snapshot
 * should include XIDs running in other node, which may come at any
 * time.   This means that needed size of xip varies from time to time.
 *
 * This must be handled properly in all the functions in this module.
 *
 * The member max_xcnt was added as SnapshotData member to indicate the
 * real size of xip array.
 *
 * Here, the following assumption is made for SnapshotData struct throughout
 * this module.
 *
 * 1. xip member physical size is indicated by max_xcnt member.
 * 2. If max_xcnt == 0, it means that xip members is NULL, and vise versa.
 * 3. xip (and subxip) are allocated usign malloc() or realloc() directly.
 *
 * For Postgres-XC, there is some special handling for ANALYZE.
 * An XID for a local ANALYZE command will never involve other nodes.
 * Also, ANALYZE may run for a long time, affecting snapshot xmin values
 * on other nodes unnecessarily.  We want to exclude the XID
 * in global snapshots, but include it in local ones. As a result,
 * these are tracked in shared memory separately.
#endif
 *
 * During hot standby, we also keep a list of XIDs representing transactions
 * that are known to be running in the master (or more precisely, were running
 * as of the current point in the WAL stream).	This list is kept in the
 * KnownAssignedXids array, and is updated by watching the sequence of
 * arriving XIDs.  This is necessary because if we leave those XIDs out of
 * snapshots taken for standby queries, then they will appear to be already
 * complete, leading to MVCC failures.	Note that in hot standby, the PGPROC
 * array represents standby processes, which by definition are not running
 * transactions that have XIDs.
 *
 * It is perhaps possible for a backend on the master to terminate without
 * writing an abort record for its transaction.  While that shouldn't really
 * happen, it would tie up KnownAssignedXids indefinitely, so we protect
 * ourselves by pruning the array when a valid list of running XIDs arrives.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/procarray.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/extreme_rto/page_redo.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "catalog/catalog.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "gtm/gtm_txn.h"
#include "miscadmin.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/cfs_shrinker.h"
#include "storage/lmgr.h"
#include "storage/spin.h"
#include "threadpool/threadpool.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "utils/atomic.h"
#include "utils/distribute_test.h"
#include "access/heapam.h"
#ifdef PGXC
    #include "pgxc/pgxc.h"
    #include "access/gtm.h"
    #include "storage/ipc.h"
    #include "pgxc/nodemgr.h"
    #include"replication/walreceiver.h"
    /* PGXC_DATANODE */
    #include "postmaster/autovacuum.h"
    #include "postmaster/postmaster.h"
    #include "postmaster/twophasecleaner.h"
#endif
#include "gssignal/gs_signal.h"
#include "catalog/pg_control.h"
#include "pgstat.h"
#include "storage/lock/lwlock.h"
#include "threadpool/threadpool_sessctl.h"
#include "access/parallel_recovery/dispatcher.h"
#include "access/multi_redo_api.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"
#include "ddes/dms/ss_common_attr.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_reform_common.h"
#include "replication/ss_disaster_cluster.h"

#ifdef ENABLE_UT
    #define static
#endif /* USE_UT */



#ifdef XIDCACHE_DEBUG

    /* counters for XidCache measurement */
    static long xc_by_recent_xmin = 0;
    static long xc_by_known_xact = 0;
    static long xc_by_my_xact = 0;
    static long xc_by_latest_xid = 0;
    static long xc_by_main_xid = 0;
    static long xc_by_child_xid = 0;
    static long xc_by_known_assigned = 0;
    static long xc_no_overflow = 0;
    static long xc_slow_answer = 0;

    #define xc_by_recent_xmin_inc() (xc_by_recent_xmin++)
    #define xc_by_known_xact_inc() (xc_by_known_xact++)
    #define xc_by_my_xact_inc() (xc_by_my_xact++)
    #define xc_by_latest_xid_inc() (xc_by_latest_xid++)
    #define xc_by_main_xid_inc() (xc_by_main_xid++)
    #define xc_by_child_xid_inc() (xc_by_child_xid++)

    static void DisplayXidCache(void);
#else /* !XIDCACHE_DEBUG */

    #define xc_by_recent_xmin_inc() ((void)0)
    #define xc_by_known_xact_inc() ((void)0)
    #define xc_by_my_xact_inc() ((void)0)
    #define xc_by_latest_xid_inc() ((void)0)
    #define xc_by_main_xid_inc() ((void)0)
    #define xc_by_child_xid_inc() ((void)0)
#endif /* XIDCACHE_DEBUG */

#ifdef PGXC /* PGXC_DATANODE */

#define ADD_XMIN_TO_ARRAY(xmin) \
    if (xminArray != NULL) \
        (xminArray)[count] = xmin

void SetGlobalSnapshotData(
    TransactionId xmin, TransactionId xmax, uint64 csn, GTM_Timeline timeline, bool ss_need_sync_wait_all);
void UnsetGlobalSnapshotData(void);
static bool GetPGXCSnapshotData(Snapshot snapshot);
#ifdef ENABLE_MULTIPLE_NODES
    static bool GetSnapshotDataDataNode(Snapshot snapshot);
    static bool GetSnapshotDataCoordinator(Snapshot snapshot);
#endif
static void cleanSnapshot(Snapshot snapshot);
static void ResetProcXidCache(PGPROC* proc, bool needlock);

#endif

/* for local multi version snapshot */
void CalculateLocalLatestSnapshot(bool forceCalc);
static TransactionId GetMultiSnapshotOldestXmin();
static inline void ProcArrayEndTransactionInternal(PGPROC* proc, PGXACT* pgxact, TransactionId latestXid,
                                                   TransactionId* xid, uint32* nsubxids);

void XidCacheRemoveRunningXids(PGPROC* proc, PGXACT* pgxact);
void ProcArrayGroupClearXid(bool isSubTransaction, PGPROC* proc, TransactionId latestXid, 
                            TransactionId subTranactionXid, int nSubTransactionXids, 
                            TransactionId* subTransactionXids, TransactionId subTransactionLatestXid);

extern bool StreamTopConsumerAmI();

#define PROCARRAY_MAXPROCS      (g_instance.shmem_cxt.MaxBackends + \
    g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS)

/*
 * Report shared-memory space needed by CreateProcXactHashTable
 */
Size ProcXactHashTableShmemSize(void)
{
    return hash_estimate_size(PROCARRAY_MAXPROCS, sizeof(ProcXactLookupEntry));
}

void CreateProcXactHashTable(void)
{
    HASHCTL         info;

    info.keysize = sizeof(TransactionId);
    info.entrysize = sizeof(ProcXactLookupEntry);
    info.hash = tag_hash;
    info.num_partitions = NUM_PROCXACT_PARTITIONS; /* We only have 812 threads in current configuration */

    g_instance.ProcXactTable = ShmemInitHash("Proc Xact Lookup Table", PROCARRAY_MAXPROCS, PROCARRAY_MAXPROCS,
        &info, HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
}

/* @Return partition lock id. */
static LWLock *LockProcXactHashTablePartition(TransactionId xid, LWLockMode mode)
{
        uint32 hashValue = get_hash_value(g_instance.ProcXactTable, &xid);
        uint32 partition = hashValue & (NUM_PROCXACT_PARTITIONS - 1);
        uint32 lockid = (uint32)(FirstProcXactMappingLock + partition);
        LWLock* lock = &t_thrd.shemem_ptr_cxt.mainLWLockArray[lockid].lock;

        LWLockAcquire(lock, mode);

        return lock;
}

int
ProcXactHashTableLookup(TransactionId xid)
{
        /* Caller should make sure ProcArrayLock is held by it in share mode */
        ProcXactLookupEntry *result = NULL;
        bool found = false;

        LWLock* lock = LockProcXactHashTablePartition(xid, LW_SHARED);

        result = (ProcXactLookupEntry *) hash_search(g_instance.ProcXactTable, &xid, HASH_FIND, &found);

        LWLockRelease(lock);

        return found ? result->proc_id : InvalidProcessId;
}

void
ProcXactHashTableAdd(TransactionId xid, int procId)
{
        /* Caller should make sure ProcArrayLock is held by it in exclusive mode */
        bool found = true;

        LWLock* lock = LockProcXactHashTablePartition(xid, LW_EXCLUSIVE);

        ProcXactLookupEntry *result =
            (ProcXactLookupEntry *)hash_search(g_instance.ProcXactTable, &xid, HASH_ENTER, &found);

        LWLockRelease(lock);

        result->proc_id = procId;
}

void
ProcXactHashTableRemove(TransactionId xid)
{
        bool found = false;

        LWLock *lock = LockProcXactHashTablePartition(xid, LW_EXCLUSIVE);

        hash_search(g_instance.ProcXactTable, &xid, HASH_REMOVE, &found);

        LWLockRelease(lock);

        if (!found)
            ereport(WARNING, (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("transaction identifier %lu not exists in ProcXact hash table", xid)));
}

/*
 * Report shared-memory space needed by CreateSharedProcArray.
 */
Size ProcArrayShmemSize(void)
{
    Size size;

    /* Size of the ProcArray structure itself */
#define PROCARRAY_MAXPROCS (g_instance.shmem_cxt.MaxBackends + \
    g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS)

    size = offsetof(ProcArrayStruct, pgprocnos);
    size = add_size(size, mul_size(sizeof(int), PROCARRAY_MAXPROCS));

    /*
     * During Hot Standby processing we have a data structure called
     * KnownAssignedXids, created in shared memory. Local data structures are
     * also created in various backends during GetSnapshotData(),
     * TransactionIdIsInProgress() and GetRunningTransactionData(). All of the
     * main structures created in those functions must be identically sized,
     * since we may at times copy the whole of the data structures around. We
     * refer to this size as TOTAL_MAX_CACHED_SUBXIDS.
     *
     * Ideally we'd only create this structure if we were actually doing hot
     * standby in the current run, but we don't know that yet at the time
     * shared memory is being set up.
     */
#define TOTAL_MAX_CACHED_SUBXIDS ((PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS)

    if (g_instance.attr.attr_storage.EnableHotStandby) {
        size = add_size(size, mul_size(sizeof(TransactionId), TOTAL_MAX_CACHED_SUBXIDS));
        size = add_size(size, mul_size(sizeof(bool), TOTAL_MAX_CACHED_SUBXIDS));
    }

    return size;
}

/*
 * Initialize the shared PGPROC array during postmaster startup.
 */
void CreateSharedProcArray(void)
{
    /* Create or attach to the ProcArray shared structure */
    MemoryContext oldcontext = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    size_t array_size = offsetof(ProcArrayStruct, pgprocnos) + PROCARRAY_MAXPROCS * sizeof(int) + PG_CACHE_LINE_SIZE;
    if (g_instance.proc_array_idx == NULL) {
        g_instance.proc_array_idx = (ProcArrayStruct*)CACHELINEALIGN(palloc(array_size));
    }
    {
        /* We're the first - initialize. */
        g_instance.proc_array_idx->numProcs = 0;
        g_instance.proc_array_idx->maxProcs = PROCARRAY_MAXPROCS;
        g_instance.proc_array_idx->replication_slot_xmin = InvalidTransactionId;
        g_instance.proc_array_idx->replication_slot_catalog_xmin = InvalidTransactionId;
    }

    g_instance.proc_base_all_procs = g_instance.proc_base->allProcs;
    g_instance.proc_base_all_xacts = g_instance.proc_base->allPgXact;

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Add the specified PGPROC to the shared array.
 */
void ProcArrayAdd(PGPROC* proc)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index = 0;
    errno_t rc;
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];

    if (arrayP->numProcs >= arrayP->maxProcs) {
        /*
         * Ooops, no room.	(This really shouldn't happen, since there is a
         * fixed supply of PGPROC structs too, and so we should have failed
         * earlier.)
         */
        LWLockRelease(ProcArrayLock);
        ereport(FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("sorry, too many clients already")));
    }

    /*
     * Keep the procs array sorted by (PGPROC *) so that we can utilize
     * locality of references much better. This is useful while traversing the
     * ProcArray because there is a increased likelihood of finding the next
     * PGPROC structure in the cache.
     *
     * Since the occurrence of adding/removing a proc is much lower than the
     * access to the ProcArray itself, the overhead should be marginal
     */
    for (index = 0; index < arrayP->numProcs; index++) {
        /*
         * If we are the first PGPROC or if we have found our right position
         * in the array, break
         */
        if ((arrayP->pgprocnos[index] == -1) || (arrayP->pgprocnos[index] > proc->pgprocno))
            break;
    }

    rc = memmove_s(&arrayP->pgprocnos[index + 1],
                   PROCARRAY_MAXPROCS * sizeof(int),
                   &arrayP->pgprocnos[index],
                   (arrayP->numProcs - index) * sizeof(int));
    securec_check(rc, "\0", "\0");
    arrayP->pgprocnos[index] = proc->pgprocno;
    arrayP->numProcs++;

    if (TransactionIdIsValid(pgxact->xid)) {
        ProcXactHashTableAdd(pgxact->xid, proc->pgprocno);
    }

    LWLockRelease(ProcArrayLock);
}

/*
 * Remove the specified PGPROC from the shared array.
 *
 * When latestXid is a valid XID, we are removing a live 2PC gxact from the
 * array, and thus causing it to appear as "not running" anymore.  In this
 * case we must advance latestCompletedXid.  (This is essentially the same
 * as ProcArrayEndTransaction followed by removal of the PGPROC, but we take
 * the ProcArrayLock only once, and don't damage the content of the PGPROC;
 * twophase.c depends on the latter.)
 */
void ProcArrayRemove(PGPROC* proc, TransactionId latestXid)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];
    int index = 0;

    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    if (TransactionIdIsValid(latestXid)) {
        Assert(TransactionIdIsValid(pgxact->xid));

        /* Advance global latestCompletedXid while holding the lock */
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, latestXid))
            t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestXid;
    } else {
        if (IS_PGXC_DATANODE || !IsConnFromCoord()) {
            /* Shouldn't be trying to remove a live transaction here */
            Assert(!TransactionIdIsValid(pgxact->xid));
        }
    }

    /* Clear xid from ProcXactHashTable. We can ignore BootstrapTransactionId */
    if (TransactionIdIsNormal(pgxact->xid)) {
        ProcXactHashTableRemove(pgxact->xid);
    }

    for (index = 0; index < arrayP->numProcs; index++) {
        if (arrayP->pgprocnos[index] == proc->pgprocno) {
            /* Keep the PGPROC array sorted. See notes above */
            errno_t rc;
            rc = memmove_s(&arrayP->pgprocnos[index],
                           arrayP->numProcs * sizeof(int),
                           &arrayP->pgprocnos[index + 1],
                           (arrayP->numProcs - index - 1) * sizeof(int));
            securec_check(rc, "\0", "\0");
            arrayP->pgprocnos[arrayP->numProcs - 1] = -1; /* for debugging */
            arrayP->numProcs--;

            /* Calc new sanpshot. */
            if (TransactionIdIsValid(latestXid))
                CalculateLocalLatestSnapshot(false);
            LWLockRelease(ProcArrayLock);

            /* Free xid cache memory if needed, must after procarray remove */
            ResetProcXidCache(proc, true);
            proc->commitCSN = 0;
            pgxact->needToSyncXid = 0;
            return;
        }
    }

    /* Ooops */
    LWLockRelease(ProcArrayLock);

    ereport(LOG, (errmsg("failed to find proc in ProcArray")));
}

static void inline ProcArrayClearAutovacuum(PGXACT* pgxact)
{
    if (!IsAutoVacuumWorkerProcess() && IS_PGXC_DATANODE && !IS_SINGLE_NODE) {
        pgxact->vacuumFlags &= ~PROC_IS_AUTOVACUUM;
    }
}

/*
 * ProcArrayEndTransaction -- mark a transaction as no longer running
 *
 * This is used interchangeably for commit and abort cases.  The transaction
 * commit/abort must already be reported to WAL and pg_clog.
 *
 * proc is currently always t_thrd.proc, but we pass it explicitly for flexibility.
 * latestXid is the latest Xid among the transaction's main XID and
 * subtransactions, or InvalidTransactionId if it has no XID.  (We must ask
 * the caller to pass latestXid, instead of computing it from the PGPROC's
 * contents, because the subxid information in the PGPROC might be
 * incomplete.)
 */
void ProcArrayEndTransaction(PGPROC* proc, TransactionId latestXid, bool isCommit)
{
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];

#ifndef ENABLE_DISTRIBUTE_TEST
    if (ENABLE_WORKLOAD_CONTROL && WLMIsInfoInit()) {
        if (isCommit) {
            UpdateWlmCatalogInfoHash();
        } else {
            ResetWlmCatalogFlag();
        }
    }
#endif

    if (TransactionIdIsValid(latestXid)) {
        /*
         * We must lock ProcArrayLock while clearing our advertised XID, so
         * that we do not exit the set of "running" transactions while someone
         * else is taking a snapshot.  See discussion in
         * src/backend/access/transam/README.
         */
#ifdef PGXC
        /*
         * Remove this assertion. We have seen this failing because a ROLLBACK
         * statement may get canceled by a Coordinator, leading to recursive
         * abort of a transaction. This must be a openGauss issue, highlighted
         * by XC. See thread on hackers with subject "Canceling ROLLBACK
         * statement"
         */
#else
        Assert(TransactionIdIsValid(allPgXact[proc->pgprocno].xid));
#endif
        /*
         * If we can immediately acquire ProcArrayLock, we clear our own XID
         * and release the lock.  If not, use group XID clearing to improve
         * efficiency.
         */
        if (LWLockConditionalAcquire(ProcArrayLock, LW_EXCLUSIVE)) {
            TransactionId xid;
            uint32 nsubxids;

            ProcArrayEndTransactionInternal(proc, pgxact, latestXid, &xid, &nsubxids);
            CalculateLocalLatestSnapshot(false);
            LWLockRelease(ProcArrayLock);
        } else
            ProcArrayGroupClearXid(false, proc, latestXid, InvalidTransactionId, 0, NULL, InvalidTransactionId);
    } else {
        /*
         * If we have no XID, we don't need to lock, since we won't affect
         * anyone else's calculation of a snapshot.  We might change their
         * estimate of global xmin, but that's OK.
         */
        Assert(!TransactionIdIsValid(pgxact->xid));

        pgxact->handle = InvalidTransactionHandle;
        proc->lxid = InvalidLocalTransactionId;
        pgxact->next_xid = InvalidTransactionId;
        pgxact->xmin = InvalidTransactionId;
        proc->snapXmax = InvalidTransactionId;
        proc->snapCSN = InvalidCommitSeqNo;
        proc->exrto_read_lsn = 0;
        proc->exrto_min = 0;
        proc->exrto_gen_snap_time = 0;
        pgxact->csn_min = InvalidCommitSeqNo;
        pgxact->csn_dr = InvalidCommitSeqNo;
        /* must be cleared with xid/xmin: */
        pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
        ProcArrayClearAutovacuum(pgxact);
        pgxact->delayChkpt = false; /* be sure this is cleared in abort */
        proc->recoveryConflictPending = false;
        proc->commitCSN = 0;
        pgxact->needToSyncXid = 0;

        Assert(pgxact->nxids == 0);
    }

    /*
     * Reset isInResetUserName to false. isInResetUserName is set true in case 'O' so as to mask the log
     * in GetPGXCSnapshotData and GetSnapshotData.
     */
    t_thrd.postgres_cxt.isInResetUserName = false;
}

/*
 * Mark a write transaction as no longer running.
 *
 * We don't do any locking here; caller must handle that.
 */
static inline void ProcArrayEndTransactionInternal(PGPROC* proc, PGXACT* pgxact, TransactionId latestXid,
                                                   TransactionId* xid, uint32* nsubxids)
{
    /* Store xid and nsubxids to update csnlog */
    *xid = pgxact->xid;
    *nsubxids = pgxact->nxids;

    /* Clear xid from ProcXactHashTable. We can ignore BootstrapTransactionId */
    if (TransactionIdIsNormal(*xid)) {
        ProcXactHashTableRemove(*xid);
    }

    pgxact->handle = InvalidTransactionHandle;
    pgxact->xid = InvalidTransactionId;
    pgxact->next_xid = InvalidTransactionId;
    proc->lxid = InvalidLocalTransactionId;
    pgxact->xmin = InvalidTransactionId;
    proc->snapXmax = InvalidTransactionId;
    proc->snapCSN = InvalidCommitSeqNo;
    proc->exrto_read_lsn = 0;
    proc->exrto_min = 0;
    proc->exrto_gen_snap_time = 0;
    pgxact->csn_min = InvalidCommitSeqNo;
    pgxact->csn_dr = InvalidCommitSeqNo;
    /* must be cleared with xid/xmin: */
    pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
    ProcArrayClearAutovacuum(pgxact);
    pgxact->delayChkpt = false; /* be sure this is cleared in abort */
    proc->recoveryConflictPending = false;

    /* Clear the subtransaction-XID cache too while holding the lock */
    pgxact->nxids = 0;

    /* Also advance global latestCompletedXid while holding the lock */
    if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, latestXid))
        t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestXid;

    /* Clear commit csn after csn update */
    proc->commitCSN = 0;
    pgxact->needToSyncXid = 0;

    ResetProcXidCache(proc, true);
}

static inline void ProcInsertIntoGroup(PGPROC* proc, uint32* nextidx) {
    while (true) {
        *nextidx = pg_atomic_read_u32(&g_instance.proc_base->procArrayGroupFirst);
        pg_atomic_write_u32(&proc->procArrayGroupNext, *nextidx);

        if (pg_atomic_compare_exchange_u32(
                &g_instance.proc_base->procArrayGroupFirst, nextidx, (uint32)proc->pgprocno))
            break;
    }

}

static inline void ClearProcArrayGroupCache(PGPROC* proc) {
        proc->procArrayGroupMember = false;
        proc->procArrayGroupMemberXid = InvalidTransactionId;
        proc->procArrayGroupSubXactNXids = 0;
        proc->procArrayGroupSubXactXids = NULL;
        proc->procArrayGroupSubXactLatestXid = InvalidTransactionId;
}

static inline void SetProcArrayGroupCache(PGPROC* proc, TransactionId xid, int nxids,
                                          TransactionId* xids, TransactionId latestXid)
{
    proc->procArrayGroupMemberXid = xid;
    proc->procArrayGroupSubXactNXids = nxids;
    proc->procArrayGroupSubXactXids = xids;
    proc->procArrayGroupSubXactLatestXid = latestXid;
}

/*
 * ProcArrayGroupClearXid -- group XID clearing
 *
 * When we cannot immediately acquire ProcArrayLock in exclusive mode at
 * commit time, add ourselves to a list of processes that need their XIDs
 * cleared.  The first process to add itself to the list will acquire
 * ProcArrayLock in exclusive mode and perform ProcArrayEndTransactionInternal
 * for transaction group members and XidCacheRemoveRunningXids
 * for subtransaction group members. This avoids a great deal of contention
 * around ProcArrayLock when many processes are trying to commit at once,
 * since the lock need not be repeatedly handed off from one committing
 * process to the next.
 */
void ProcArrayGroupClearXid(bool isSubTransaction, PGPROC* proc, 
                            TransactionId latestXid, TransactionId subTranactionXid,
                            int nSubTransactionXids, TransactionId* subTransactionXids, 
                            TransactionId subTransactionLatestXid)
{
    uint32 nextidx;
    uint32 wakeidx;
    TransactionId xid[PROCARRAY_MAXPROCS];
    uint32 nsubxids[PROCARRAY_MAXPROCS];
    uint32 index = 0;
    bool groupMemberHasTransaction = false;

    /* We should definitely have an XID to clear. */
    /* Add ourselves to the list of processes needing a group XID clear. */
    proc->procArrayGroupMember = true;
    if (isSubTransaction) {
        SetProcArrayGroupCache(proc, subTranactionXid, nSubTransactionXids, subTransactionXids, subTransactionLatestXid);
    } else {
        SetProcArrayGroupCache(proc, latestXid, 0, NULL, InvalidTransactionId);
    }

    /* add current proc into ProcArrayGroup */
    ProcInsertIntoGroup(proc, &nextidx);

    /*
     * If the list was not empty, the leader will clear our XID.  It is
     * impossible to have followers without a leader because the first process
     * that has added itself to the list will always have nextidx as
     * INVALID_PGPROCNO.
     */
    if (nextidx != INVALID_PGPROCNO) {
        int extraWaits = 0;

        /* Sleep until the leader clears our XID. */
        for (;;) {
            /* acts as a read barrier */
            PGSemaphoreLock(&proc->sem, false);
            if (!proc->procArrayGroupMember)
                break;
            extraWaits++;
        }

        Assert(pg_atomic_read_u32(&proc->procArrayGroupNext) == INVALID_PGPROCNO);

        /* Fix semaphore count for any absorbed wakeups */
        while (extraWaits-- > 0)
            PGSemaphoreUnlock(&proc->sem);
        return;
    }

    /* We are the leader.  Acquire the lock on behalf of everyone. */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    /*
     * Now that we've got the lock, clear the list of processes waiting for
     * group XID clearing, saving a pointer to the head of the list.  Trying
     * to pop elements one at a time could lead to an ABA problem.
     */
    while (true) {
        nextidx = pg_atomic_read_u32(&g_instance.proc_base->procArrayGroupFirst);
        if (pg_atomic_compare_exchange_u32(&g_instance.proc_base->procArrayGroupFirst, &nextidx, INVALID_PGPROCNO))
            break;
    }

    /* Remember head of list so we can perform wakeups after dropping lock. */
    wakeidx = nextidx;

    /* Walk the list and clear all XIDs. */
    while (nextidx != INVALID_PGPROCNO) {
        PGPROC* proc_member = g_instance.proc_base_all_procs[nextidx];
        PGXACT* pgxact = &g_instance.proc_base_all_xacts[nextidx];
        ereport(DEBUG2, (errmsg("handle group member from procArrayGroup, procno = %u, "
            "procArrayGroupMemberXid = " XID_FMT ", "
            "procArrayGroupSubXactNXids = %d, "
            "procArrayGroupSubXactLatestXid = " XID_FMT ", "
            "procArrayGroupNext = %u", 
            proc_member->pgprocno, 
            proc_member->procArrayGroupMemberXid, 
            proc_member->procArrayGroupSubXactNXids, 
            proc_member->procArrayGroupSubXactLatestXid,
            proc_member->procArrayGroupNext)));

        /*
         * If the proc_member is a transaction, perform ProcArrayEndTransactionInternal
         * to clear its XID. If the proc_member is a subtransaction, 
         * perform XidCacheRemoveRunningXids to clear its XIDs and 
         * its committed subtransaction's XIDS.
         *
         * proc_member->procArrayGroupSubXactLatestXid !=0 when the group member 
         * is a subtransaction.
         */
        if (proc_member->procArrayGroupSubXactLatestXid != InvalidTransactionId) {
            XidCacheRemoveRunningXids(proc_member, pgxact);
        } else {
            groupMemberHasTransaction = true;
            ProcArrayEndTransactionInternal(
                proc_member, pgxact, proc_member->procArrayGroupMemberXid, &xid[index], &nsubxids[index]); 
        }

        /* Move to next proc in list. */
        nextidx = pg_atomic_read_u32(&proc_member->procArrayGroupNext);
        index++;
    }

    /* Already hold lock, caculate snapshot after last invocation, 
     * if there is at least one transaction in group.
     */
    if (groupMemberHasTransaction) {
        CalculateLocalLatestSnapshot(false);
    }

    /* We're done with the lock now. */
    LWLockRelease(ProcArrayLock);

    /*
     * Now that we've released the lock, go back and wake everybody up.  We
     * don't do this under the lock so as to keep lock hold times to a
     * minimum.  The system calls we need to perform to wake other processes
     * up are probably much slower than the simple memory writes we did while
     * holding the lock.
     */
    index = 0;
    while (wakeidx != INVALID_PGPROCNO) {
        PGPROC* proc_member = g_instance.proc_base_all_procs[wakeidx];

        wakeidx = pg_atomic_read_u32(&proc_member->procArrayGroupNext);
        pg_atomic_write_u32(&proc_member->procArrayGroupNext, INVALID_PGPROCNO);

        /* ensure all previous writes are visible before follower continues. */
        pg_write_barrier();

        ClearProcArrayGroupCache(proc_member);

        if (proc_member != t_thrd.proc)
            PGSemaphoreUnlock(&proc_member->sem);

        index++;
    }
}

/*
 * ProcArrayClearTransaction -- clear the transaction fields
 *
 * This is used after successfully preparing a 2-phase transaction.  We are
 * not actually reporting the transaction's XID as no longer running --- it
 * will still appear as running because the 2PC's gxact is in the ProcArray
 * too.  We just have to clear out our own PGXACT.
 */
void ProcArrayClearTransaction(PGPROC* proc)
{
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];

    /*
     * We can skip locking ProcArrayLock here, because this action does not
     * actually change anyone's view of the set of running XIDs: our entry is
     * duplicate with the gxact that has already been inserted into the
     * ProcArray.
     */
    pgxact->handle = InvalidTransactionHandle;
    pgxact->xid = InvalidTransactionId;
    pgxact->next_xid = InvalidTransactionId;
    proc->lxid = InvalidLocalTransactionId;
    pgxact->xmin = InvalidTransactionId;
    proc->snapXmax = InvalidTransactionId;
    proc->snapCSN = InvalidCommitSeqNo;
    proc->exrto_read_lsn = 0;
    proc->exrto_gen_snap_time = 0;
    pgxact->csn_min = InvalidCommitSeqNo;
    pgxact->csn_dr = InvalidCommitSeqNo;
    proc->recoveryConflictPending = false;

    /* redundant, but just in case */
    pgxact->vacuumFlags &= ~PROC_VACUUM_STATE_MASK;
    ProcArrayClearAutovacuum(pgxact);
    pgxact->delayChkpt = false;
    pgxact->needToSyncXid = 0;

    /* Clear the subtransaction-XID cache too */
    pgxact->nxids = 0;

    proc->exrto_min = 0;
    /* Free xid cache memory if needed */
    ResetProcXidCache(proc, true);
}

void UpdateCSNLogAtTransactionEND(
    TransactionId xid, int nsubxids, TransactionId* subXids, CommitSeqNo csn, bool isCommit)
{
    if (TransactionIdIsNormal(xid) && isCommit) {
        Assert(csn >= COMMITSEQNO_FROZEN);

        /* Update CSN log, stamp this XID (and sub-XIDs) with the CSN */
#ifdef ENABLE_MULTIPLE_NODES
        CSNLogSetCommitSeqNo(xid, nsubxids, subXids, csn);
#else
        CSNLogSetCommitSeqNo(xid, nsubxids, subXids, csn & ~COMMITSEQNO_COMMIT_INPROGRESS);
#endif
    }
}

/*
 * This is called in revorage stage, extend the CSN log page while doing
 * xact_redo if need, after the CSN log is initialized to latestObservedXid.
 */
void CSNLogRecordAssignedTransactionId(TransactionId newXid)
{
    if (TransactionIdFollows(newXid, t_thrd.storage_cxt.latestObservedXid)) {
        TransactionId next_expected_xid = t_thrd.storage_cxt.latestObservedXid;
        while (TransactionIdPrecedes(next_expected_xid, newXid)) {
            TransactionIdAdvance(next_expected_xid);
            ExtendCSNLOG(next_expected_xid);
        }
        Assert(next_expected_xid == newXid);

        /*
         * Now we can advance latestObservedXid
         */
        t_thrd.storage_cxt.latestObservedXid = newXid;

        if (t_thrd.xlog_cxt.standbyState <= STANDBY_INITIALIZED) {
            return;
        }

        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdFollowsOrEquals(next_expected_xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = next_expected_xid;
            TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
        }
        LWLockRelease(XidGenLock);
    }
}

/*
 * ProcArrayInitRecovery -- initialize recovery xid mgmt environment
 *
 * Remember up to where the startup process initialized the CLOG and subtrans
 * so we can ensure it's initialized gaplessly up to the point where necessary
 * while in recovery.
 */
void ProcArrayInitRecovery(TransactionId initializedUptoXID)
{
    Assert(t_thrd.xlog_cxt.standbyState == STANDBY_INITIALIZED);
    Assert(TransactionIdIsNormal(initializedUptoXID));

    /*
     * we set latestObservedXid to the xid SUBTRANS has been initialized upto,
     * so we can extend it from that point onwards in RecordKnownAssignedTransactionIds,
     * and when we get consistent in ProcArrayApplyRecoveryInfo().
     */
    t_thrd.storage_cxt.latestObservedXid = initializedUptoXID;
    TransactionIdRetreat(t_thrd.storage_cxt.latestObservedXid);
}

/*
 * GetRunningTransactionData -- returns information about running transactions.
 *
 * Similar to GetSnapshotData but returns more information. We include
 * all PGXACTs with an assigned TransactionId, even VACUUM processes.
 *
 * We acquire XidGenLock and ProcArrayLock, but the caller is responsible for
 * releasing them. Acquiring XidGenLock ensures that no new XIDs enter the proc
 * array until the caller has WAL-logged this snapshot, and releases the
 * lock. Acquiring ProcArrayLock ensures that no transactions commit until the
 * lock is released.

 *
 * The returned data structure is statically allocated; caller should not
 * modify it, and must not assume it is valid past the next call.
 *
 * This is never executed during recovery so there is no need to look at
 * KnownAssignedXids.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 *
 * Note that if any transaction has overflowed its cached subtransactions
 * then there is no real need include any subtransactions. That isn't a
 * common enough case to worry about optimising the size of the WAL record,
 * and we may wish to see that data for diagnostic purposes anyway.
 */
RunningTransactions GetRunningTransactionData(void)
{
    /* result workspace */
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    RunningTransactions CurrentRunningXacts = t_thrd.storage_cxt.CurrentRunningXacts;
    TransactionId latestCompletedXid;
    TransactionId oldestRunningXid;
    TransactionId* xids = NULL;
    int index;
    int count = 0;
    int subcount = 0;
    bool suboverflowed = false;
    int rc = 0;
    Assert(!RecoveryInProgress());

    /*
     * Allocating space for maxProcs xids is usually overkill; numProcs would
     * be sufficient.  But it seems better to do the malloc while not holding
     * the lock, so we can't look at numProcs.  Likewise, we allocate much
     * more subxip storage than is probably needed.
     *
     * Should only be allocated in bgwriter, since only ever executed during
     * checkpoints.
     */
    if (CurrentRunningXacts->xids == NULL) {
        /*
         * First call
         */
        CurrentRunningXacts->xids = (TransactionId*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
            (unsigned int)TOTAL_MAX_CACHED_SUBXIDS * sizeof(TransactionId));

        if (CurrentRunningXacts->xids == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    xids = CurrentRunningXacts->xids;

    /*
     * Ensure that no xids enter or leave the procarray while we obtain
     * snapshot.
     */
    LWLockAcquire(XidGenLock, LW_SHARED);
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    latestCompletedXid = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;

    oldestRunningXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;

    /* xmax is always latestCompletedXid + 1 */
    TransactionId xmax = latestCompletedXid;
    TransactionIdAdvance(xmax);
    TransactionId globalXmin = xmax;

    /*
     * Spin over procArray collecting all xids and subxids.
     */
    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId xid;
        int nxids;

        /* Update globalxmin to be the smallest valid xmin */
        xid = pgxact->xmin; /* fetch just once */

        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, globalXmin)) {
            globalXmin = xid;
        }

        /* Fetch xid just once - see GetNewTransactionId */
        xid = pgxact->xid;

        /*
         * We don't need to store transactions that don't have a TransactionId
         * yet because they will not show as running on a standby server.
         */
        if (!TransactionIdIsValid(xid))
            continue;

        xids[count++] = xid;

        if (TransactionIdPrecedes(xid, oldestRunningXid))
            oldestRunningXid = xid;

        /*
         * Save subtransaction XIDs. Other backends can't add or remove
         * entries while we're holding XidGenLock.
         */
        nxids = pgxact->nxids;

        if (nxids > 0) {
            if (nxids > PGPROC_MAX_CACHED_SUBXIDS)
                nxids = PGPROC_MAX_CACHED_SUBXIDS;

            rc = memcpy_s(&xids[count], nxids * sizeof(TransactionId), (void *)proc->subxids.xids,
                          nxids * sizeof(TransactionId));
            securec_check(rc, "\0", "\0");
            count += nxids;
            subcount += nxids;

            if (pgxact->nxids > PGPROC_MAX_CACHED_SUBXIDS)
                suboverflowed = true;

            /*
             * Top-level XID of a transaction is always less than any of its
             * subxids, so we don't need to check if any of the subxids are
             * smaller than oldestRunningXid
             */
        }
    }

    /*
     * Update globalxmin to include actual process xids.  This is a slightly
     * different way of computing it than GetOldestXmin uses, but should give
     * the same result.
     */
    if (TransactionIdPrecedes(oldestRunningXid, globalXmin)) {
        globalXmin = oldestRunningXid;
    }

    /*
     * It's important *not* to include the limits set by slots here because
     * snapbuild.c uses oldestRunningXid to manage its xmin horizon. If those
     * were to be included here the initial value could never increase because
     * of a circular dependency where slots only increase their limits when
     * running xacts increases oldestRunningXid and running xacts only
     * increases if slots do.
     */
    CurrentRunningXacts->xcnt = count;
    CurrentRunningXacts->subxid_overflow = suboverflowed;
    CurrentRunningXacts->nextXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    CurrentRunningXacts->oldestRunningXid = oldestRunningXid;
    CurrentRunningXacts->latestCompletedXid = latestCompletedXid;
    CurrentRunningXacts->globalXmin = globalXmin;

    Assert(TransactionIdIsValid(CurrentRunningXacts->nextXid));
    Assert(TransactionIdIsValid(CurrentRunningXacts->oldestRunningXid));
    Assert(TransactionIdIsNormal(CurrentRunningXacts->latestCompletedXid));
    /* We don't release the locks here, the caller is responsible for that */
    return CurrentRunningXacts;
}

/*
 * ProcArrayApplyRecoveryInfo -- apply recovery info about xids
 *
 * Takes us through 3 states: Initialized, Pending and Ready.
 * Normal case is to go all the way to Ready straight away, though there
 * are atypical cases where we need to take it in steps.
 *
 * Use the data about running transactions on master to create the initial
 * state of KnownAssignedXids. We also use these records to regularly prune
 * KnownAssignedXids because we know it is possible that some transactions
 * with FATAL errors fail to write abort records, which could cause eventual
 * overflow.
 *
 * See comments for LogStandbySnapshot().
 */
void ProcArrayApplyRecoveryInfo(RunningTransactions running)
{
    TransactionId nextXid;

    Assert(t_thrd.xlog_cxt.standbyState >= STANDBY_INITIALIZED);
    Assert(TransactionIdIsValid(running->nextXid));
    Assert(TransactionIdIsValid(running->oldestRunningXid));
    Assert(TransactionIdIsNormal(running->latestCompletedXid));

    /*
     * Remove stale locks, if any.
     *
     * Locks are always assigned to the toplevel xid so we don't need to care
     * about subxcnt/subxids (and by extension not about ->suboverflowed).
     */
    StandbyReleaseOldLocks(running->oldestRunningXid);

    /*
     * If our snapshot is already valid, nothing else to do...
     */
    if (t_thrd.xlog_cxt.standbyState == STANDBY_SNAPSHOT_READY)
        return;

    Assert(t_thrd.xlog_cxt.standbyState == STANDBY_INITIALIZED);

    /*
     * latestObservedXid is at least set to the point where CSNLOG was
     * started up to (c.f. ProcArrayInitRecovery()) or to the biggest xid
     * RecordKnownAssignedTransactionIds() was called for.  Initialize
     * subtrans from thereon, up to nextXid - 1.
     *
     * We need to duplicate parts of RecordKnownAssignedTransactionId() here,
     * because we've just added xids to the known assigned xids machinery that
     * haven't gone through RecordKnownAssignedTransactionId().
     */
    Assert(TransactionIdIsNormal(t_thrd.storage_cxt.latestObservedXid));
    TransactionIdAdvance(t_thrd.storage_cxt.latestObservedXid);
    while (TransactionIdPrecedes(t_thrd.storage_cxt.latestObservedXid, running->nextXid)) {
        ExtendCSNLOG(t_thrd.storage_cxt.latestObservedXid);
        TransactionIdAdvance(t_thrd.storage_cxt.latestObservedXid);
    }
    TransactionIdRetreat(t_thrd.storage_cxt.latestObservedXid); /* = running->nextXid - 1 */

    t_thrd.xlog_cxt.standbyState = STANDBY_SNAPSHOT_READY;
    MultiRedoUpdateStandbyState((HotStandbyState)t_thrd.xlog_cxt.standbyState);

    /*
     * If a transaction wrote a commit record in the gap between taking and
     * logging the snapshot then latestCompletedXid may already be higher than
     * the value from the snapshot, so check before we use the incoming value.
     */
    if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, running->latestCompletedXid))
        t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = running->latestCompletedXid;

    Assert(TransactionIdIsNormal(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid));

    /*
     * ShmemVariableCache->nextXid must be beyond any observed xid.
     *
     * We don't expect anyone else to modify nextXid, hence we don't need to
     * hold a lock while examining it.	We still acquire the lock to modify
     * it, though.
     */
    nextXid = t_thrd.storage_cxt.latestObservedXid;
    TransactionIdAdvance(nextXid);

    if (TransactionIdFollows(nextXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (TransactionIdFollows(nextXid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            t_thrd.xact_cxt.ShmemVariableCache->nextXid = nextXid;
        }
        LWLockRelease(XidGenLock);
    }

    Assert(TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->nextXid));
    ereport(trace_recovery(DEBUG1), (errmsg("recovery snapshots are now enabled")));
}

/*
 * TransactionIdIsActive -- is xid the top-level XID of an active backend?
 *
 * This differs from TransactionIdIsInProgress in that it ignores prepared
 * transactions, as well as transactions running on the master if we're in
 * hot standby.  Also, we ignore subtransactions since that's not needed
 * for current uses.
 */
bool TransactionIdIsActive(TransactionId xid)
{
    bool result = false;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int i;

    /*
     * Don't bother checking a transaction older than RecentXmin; it could not
     * possibly still be running.
     */
    if (TransactionIdPrecedes(xid, u_sess->utils_cxt.RecentXmin))
        return false;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (i = 0; i < arrayP->numProcs; i++) {
        int pgprocno = arrayP->pgprocnos[i];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId pxid;

        /* Fetch xid just once - see GetNewTransactionId */
        pxid = pgxact->xid;

        if (!TransactionIdIsValid(pxid))
            continue;

        if (proc->pid == 0)
            continue; /* ignore prepared transactions */

        if (TransactionIdEquals(pxid, xid)) {
            result = true;
            break;
        }
    }

    LWLockRelease(ProcArrayLock);

    return result;
}

/* Free xid cache memory if max number exceed PGPROC_MAX_CACHED_SUBXIDS */
static void ResetProcXidCache(PGPROC* proc, bool needlock)
{
    if (proc->subxids.maxNumber > PGPROC_INIT_CACHED_SUBXIDS) {
        /* Use subxidsLock to protect subxids */
        if (needlock)
            LWLockAcquire(proc->subxidsLock, LW_EXCLUSIVE);
        else
            HOLD_INTERRUPTS();

        proc->subxids.maxNumber = 0;
        pfree(proc->subxids.xids);
        proc->subxids.xids = NULL;

        if (needlock)
            LWLockRelease(proc->subxidsLock);
        else
            RESUME_INTERRUPTS();
    }
}

/* Free xidcache before proc exit */
void ProcSubXidCacheClean()
{
    if (t_thrd.proc && t_thrd.proc->subxids.maxNumber > PGPROC_INIT_CACHED_SUBXIDS) {
        /* Use subxidsLock to protect subxids */
        LWLockAcquire(t_thrd.proc->subxidsLock, LW_EXCLUSIVE);
        t_thrd.pgxact->nxids = 0;
        t_thrd.proc->subxids.maxNumber = 0;
        pfree(t_thrd.proc->subxids.xids);
        t_thrd.proc->subxids.xids = NULL;
        LWLockRelease(t_thrd.proc->subxidsLock);
    }
}

void InitProcSubXidCacheContext()
{
    if (ProcSubXidCacheContext == NULL) {
        ProcSubXidCacheContext = AllocSetContextCreate(g_instance.instance_context,
                                                       "ProcSubXidCacheContext",
                                                       ALLOCSET_DEFAULT_MINSIZE,
                                                       ALLOCSET_DEFAULT_INITSIZE,
                                                       ALLOCSET_DEFAULT_MAXSIZE,
                                                       SHARED_CONTEXT);
    }
}

/*
 * TransactionIdIsInProgress -- is given transaction running in some backend
 *
 * Aside from some shortcuts such as checking RecentXmin and our own Xid,
 * there are four possibilities for finding a running transaction:
 *
 * 1. The given Xid is a main transaction Id.  We will find this out cheaply
 * by looking at the PGXACT struct for each backend.
 *
 * 2. The given Xid is one of the cached subxact Xids in the PGPROC array.
 * We can find this out cheaply too.
 *
 * 3. In Hot Standby mode, we must search the KnownAssignedXids list to see
 * if the Xid is running on the master.
 *
 * 4. Search the SubTrans tree to find the Xid's topmost parent, and then see
 * if that is running according to PGXACT or KnownAssignedXids.  This is the
 * slowest way, but sadly it has to be done always if the others failed,
 * unless we see that the cached subxact sets are complete (none have
 * overflowed).
 *
 * ProcArrayLock has to be held while we do 1, 2, 3.  If we save the top Xids
 * while doing 1 and 3, we can release the ProcArrayLock while we do 4.
 * This buys back some concurrency (and we can't retrieve the main Xids from
 * PGXACT again anyway; see GetNewTransactionId).
 *
 * In MPPDB cluster environment, RecentXmin might not be the minimun xid, e.g.
 * 1. T1 starts at CN
 * 2. T2 starts at DN, gets RecentXmin from GTM, larger than T1 if GTM is
cleared up
 * 3. CN send T1 to DN
 * 4. T2 maybe set wrong tuple hints of T1 if it considered T1 is minor than
RecentXmin
 * as not in progress.
 * This scene using RecentXmin to shortcut might has wrong status of T1, then
wrong
 * infomask for tuple T1 dealt. So we will not shortut by RecentXmin by
default.
 * But if using MVCC snapshot, we confirm local snapshot will sync with GTM,
and make
 * sure RecentXmin is the minimun xid here. So we just shortcuts by checking
RecentXmin
 * in HeapTupleSatisfiesMVCC. But we keep assert checking every scene for
data consistency.
 */
bool TransactionIdIsInProgress(TransactionId xid, uint32* needSync, bool shortcutByRecentXmin,
                               bool bCareNextxid, bool isTopXact, bool checkLatestCompletedXid)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
#ifdef USE_ASSERT_CHECKING
    bool shortCutCheckRes = true;
#endif
    volatile int i = 0;
    volatile int j = 0;

    /*
     * Don't bother checking a transaction older than RecentXmin; it could not
     * possibly still be running.  (Note: in particular, this guarantees that
     * we reject InvalidTransactionId, FrozenTransactionId, etc as not
     * running.)
     *
     * Notes: our principle for distribute transaction is:
     * 	 We should treat gtm xact state as the global xact state, when local
    xact state
     * is not match with gtm xact, we block until they are match(
    SyncLocalXactsWithGTM).
     *
     * 	 So, the shortcut `RecentXmin' is not worth worried, because when it is
    assigned value
     * local must sync with gtm.
     */
    uint64 recycle_xid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    /* in hotstandby mode, the proc may being runnnig */
    if (RecoveryInProgress()) {
        recycle_xid = InvalidTransactionId;
    }
    if (shortcutByRecentXmin && TransactionIdPrecedes(xid, recycle_xid)) {
        xc_by_recent_xmin_inc();

        /*
         * As xc_maintenance_mode not sync local xacts with GTM for consistency,
         * Here we just check not in xc_maintenance_mode.
         */
        if (!u_sess->attr.attr_common.xc_maintenance_mode) {
#ifdef USE_ASSERT_CHECKING
            shortCutCheckRes = false;
#endif
        }

#ifdef USE_ASSERT_CHECKING
        /* fall through to do recheck */
#else
        return false;
#endif
    }

    /*
     * We may have just checked the status of this transaction, so if it is
     * already known to be completed, we can fall out without any access to
     * shared memory.
     */
    if (TransactionIdIsKnownCompleted(xid)) {
        xc_by_known_xact_inc();
        return false;
    }

    if (ENABLE_DMS) {
        /* fetch TXN info locally if either reformer, original primary, or normal primary */
        bool local_fetch = SSCanFetchLocalSnapshotTxnRelatedInfo();
        if (!local_fetch) {
            bool in_progress = true;
            SSTransactionIdIsInProgress(xid, &in_progress);
            return in_progress;
        }
    }

    /*
     * Also, we can handle our own transaction (and subtransactions) without
     * any access to shared memory.
     */
    if (TransactionIdIsCurrentTransactionId(xid)) {
        xc_by_my_xact_inc();
        Assert(shortCutCheckRes == true);
        return true;
    }

    if (!RecoveryInProgress()) {
        LWLockAcquire(ProcArrayLock, LW_SHARED);

        /*
         * Now that we have the lock, we can check latestCompletedXid; if the
         * target Xid is after that, it's surely still running.
         */
        if (checkLatestCompletedXid &&
            TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, xid)) {
            LWLockRelease(ProcArrayLock);
            xc_by_latest_xid_inc();

            /*
             * If xid < RecentXmin, xid should smaller than latestCompletedXid,
             * So shortCutCheckRes should be false. But for data replication,
             * page maybe faster than xlog, and tuple xid will be more than
             * latestCompletedXid after standby promote to primary. So the assert cannot
             * be always true, we will remove the assert. And it will not affect MVCC,
             * the xid should be aborted. Assert(shortCutCheckRes == true);
             */
            return true;
        }

        if (isTopXact && !bCareNextxid) {
            int procId = ProcXactHashTableLookup(xid);

            volatile PGXACT *pgxact = &g_instance.proc_base_all_xacts[procId];

            if (procId != InvalidProcessId) {
                if (needSync != NULL) {
                    *needSync = pgxact->needToSyncXid;
                }
                LWLockRelease(ProcArrayLock);
                return true;
            }

            LWLockRelease(ProcArrayLock);
        } else {
            /* No shortcuts, gotta grovel through the array */
            for (i = 0; i < arrayP->numProcs; i++) {
                int pgprocno = arrayP->pgprocnos[i];
                volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
                volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
                TransactionId pxid;

                /* Ignore my own proc --- dealt with it above */
                if (proc == t_thrd.proc)
                    continue;

                /* Fetch xid just once - see GetNewTransactionId */
                pxid = pgxact->xid;

                if (!TransactionIdIsValid(pxid)) {
                    if (bCareNextxid && TransactionIdIsValid(pgxact->next_xid))
                        pxid = pgxact->next_xid;
                    else
                        continue;
                }

                /*
                 * Step 1: check the main Xid
                 */
                if (TransactionIdEquals(pxid, xid)) {
                    if (needSync != NULL)
                        *needSync = pgxact->needToSyncXid;
                    LWLockRelease(ProcArrayLock);
                    xc_by_main_xid_inc();
                    Assert(shortCutCheckRes == true);
                    return true;
                }

                /*
                 * We can ignore main Xids that are younger than the target Xid, since
                 * the target could not possibly be their child.
                 */
                if (TransactionIdPrecedes(xid, pxid))
                    continue;

                /*
                 * Step 2: check the cached child-Xids arrays
                 */
                if (pgxact->nxids > 0) {
                    /* Use subxidsLock to protect subxids */
                    LWLockAcquire(proc->subxidsLock, LW_SHARED);
                    for (j = pgxact->nxids - 1; j >= 0; j--) {
                        /* Fetch xid just once - see GetNewTransactionId */
                        TransactionId cxid = proc->subxids.xids[j];

                        if (TransactionIdEquals(cxid, xid)) {
                            if (needSync != NULL)
                                *needSync = pgxact->needToSyncXid;
                            LWLockRelease(proc->subxidsLock);
                            LWLockRelease(ProcArrayLock);
                            xc_by_child_xid_inc();
                            Assert(shortCutCheckRes == true);
                            return true;
                        }
                    }
                    LWLockRelease(proc->subxidsLock);
                }
            }

            LWLockRelease(ProcArrayLock);
        }
    }
    /*
     * Step 3: in hot standby mode, check the CSN log.
     */
    if (RecoveryInProgress()) {
        CommitSeqNo csn;
        csn = TransactionIdGetCommitSeqNo(xid, false, false, true, NULL);
        if (COMMITSEQNO_IS_COMMITTED(csn) || COMMITSEQNO_IS_ABORTED(csn))
            return false;
        else
            return true;
    }

    return false;
}


/* Called by GetOldestXmin() */
static void UpdateRecentGlobalXmin(TransactionId currGlobalXmin, TransactionId result)
{
    if (module_logging_is_on(MOD_TRANS_SNAPSHOT))
        ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT), errmsg("recentGlobalXmin before update: currGlobalXmin = %lu",
                                                            currGlobalXmin)));
    while (TransactionIdFollows(result, currGlobalXmin)) {
        if (pg_atomic_compare_exchange_u64(
                &t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin, &currGlobalXmin, result)) {
            if (module_logging_is_on(MOD_TRANS_SNAPSHOT))
                ereport(LOG,
                        (errmodule(MOD_TRANS_SNAPSHOT), errmsg("recentGlobalXmin after update: %lu.", result)));
            break;
        }
    }
}

/*
 * GetOldestXmin -- returns oldest transaction that was running
 *					when any current transaction was started.
 *
 * If rel is NULL or a shared relation, all backends are considered, otherwise
 * only backends running in this database are considered.
 *
 * If ignoreVacuum is TRUE then backends with the PROC_IN_VACUUM flag set are
 * ignored.
 *
 * This is used by VACUUM to decide which deleted tuples must be preserved in
 * the passed in table. For shared relations backends in all databases must be
 * considered, but for non-shared relations that's not required, since only
 * backends in my own database could ever see the tuples in them. Also, we can
 * ignore concurrently running lazy VACUUMs because (a) they must be working
 * on other tables, and (b) they don't need to do snapshot-based lookups.
 *
 * This is also used to determine where to truncate pg_subtrans.  For that
 * backends in all databases have to be considered, so rel = NULL has to be
 * passed in.
 *
 * Note: we include all currently running xids in the set of considered xids.
 * This ensures that if a just-started xact has not yet set its snapshot,
 * when it does set the snapshot it cannot set xmin less than what we compute.
 * See notes in src/backend/access/transam/README.
 *
 * Note: despite the above, it's possible for the calculated value to move
 * backwards on repeated calls. The calculated value is conservative, so that
 * anything older is definitely not considered as running by anyone anymore,
 * but the exact value calculated depends on a number of things. For example,
 * if rel = NULL and there are no transactions running in the current
 * database, GetOldestXmin() returns latestCompletedXid. If a transaction
 * begins after that, its xmin will include in-progress transactions in other
 * databases that started earlier, so another call will return a lower value.
 * Nonetheless it is safe to vacuum a table in the current database with the
 * first result.  There are also replication-related effects: a walsender
 * process can set its xmin based on transactions that are no longer running
 * in the master but are still being replayed on the standby, thus possibly
 * making the GetOldestXmin reading go backwards.  In this case there is a
 * possibility that we lose data that the standby would like to have, but
 * there is little we can do about that --- data is only protected if the
 * walsender runs continuously while queries are executed on the standby.
 * (The Hot Standby code deals with such cases by failing standby queries
 * that needed to access already-removed data, so there's no integrity bug.)
 * The return value is also adjusted with vacuum_defer_cleanup_age, so
 * increasing that setting on the fly is another easy way to make
 * GetOldestXmin() move backwards, with no consequences for data integrity.
 */
TransactionId GetOldestXmin(Relation rel, bool bFixRecentGlobalXmin, bool bRecentGlobalXminNoCheck)
{
    TransactionId result = InvalidTransactionId;
    TransactionId currGlobalXmin;
    TransactionId replication_slot_xmin;
    volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;

    if (!bFixRecentGlobalXmin && TransactionIdIsNormal(u_sess->utils_cxt.RecentGlobalXmin) && !bRecentGlobalXminNoCheck)
        return u_sess->utils_cxt.RecentGlobalXmin;

    /* Fetch into local variable, don't need to hold ProcArrayLock */
    replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;

    if (!GTM_LITE_MODE) {
        /* Get recentLocalXmin from the latest snapshot */
        result = GetMultiSnapshotOldestXmin();

        if (bFixRecentGlobalXmin) {
            /* Fix recentGlobalXmin */
            if (!TransactionIdIsNormal(result) || TransactionIdFollows(result, u_sess->utils_cxt.RecentGlobalXmin))
                result = u_sess->utils_cxt.RecentGlobalXmin;

            /* Update recentGlobalXmin if needed */
            if (!u_sess->attr.attr_common.xc_maintenance_mode && !u_sess->utils_cxt.cn_xc_maintain_mode) {
                currGlobalXmin = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
                UpdateRecentGlobalXmin(currGlobalXmin, result);
            }
        } else if (!bRecentGlobalXminNoCheck) {
            /* Get recentGlobalXmin from ShmemVariableCache */
            currGlobalXmin = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
            if (TransactionIdIsNormal(currGlobalXmin) &&
                (!TransactionIdIsValid(result) || TransactionIdPrecedes(currGlobalXmin, result)))
                result = currGlobalXmin;
        }
    } else {
        /* directly fetch recentGlobalXmin from ShmemVariableCache */
        result = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
    }

    /* Update by vacuum_defer_cleanup_age */
    if (TransactionIdPrecedes(result, (uint64)u_sess->attr.attr_storage.vacuum_defer_cleanup_age)) {
        result = FirstNormalTransactionId;
    } else {
        result -= u_sess->attr.attr_storage.vacuum_defer_cleanup_age;
    }
    
    /* Check whether there's a replication slot requiring an older xmin. */
    if (TransactionIdIsNormal(replication_slot_xmin) && TransactionIdPrecedes(replication_slot_xmin, result))
        result = replication_slot_xmin;

    if (!TransactionIdIsNormal(result))
        result = FirstNormalTransactionId;
    /* fetch into volatile var while ProcArrayLock is held */
    replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;
    replication_slot_catalog_xmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;

    /*
     * Check whether there are replication slots requiring an older xmin.
     */
    if (TransactionIdIsValid(replication_slot_xmin) &&
        NormalTransactionIdPrecedes(replication_slot_xmin, result)) {
        result = replication_slot_xmin;
    }

    /*
     * After locks have been released and defer_cleanup_age has been applied,
     * check whether we need to back up further to make logical decoding
     * possible. We need to do so if we're computing the global limit (rel =
     * NULL) or if the passed relation is a catalog relation of some kind.
     */
    if ((rel != NULL && RelationIsAccessibleInLogicalDecoding(rel)) &&
        TransactionIdIsValid(replication_slot_catalog_xmin) &&
        NormalTransactionIdPrecedes(replication_slot_catalog_xmin, result))
        result = replication_slot_catalog_xmin;

    return result;
}

TransactionId GetGlobalOldestXmin()
{
    TransactionId result = InvalidTransactionId;

    /* directly fetch Global OldestXmin */
    result = GetMultiSnapshotOldestXmin();

    /* Update by vacuum_defer_cleanup_age */
    if (TransactionIdPrecedes(result, (uint64)u_sess->attr.attr_storage.vacuum_defer_cleanup_age)) {
        result = FirstNormalTransactionId;
    } else {
        result -= u_sess->attr.attr_storage.vacuum_defer_cleanup_age;
    }
    return result;
}

TransactionId GetOldestXminForUndo(TransactionId *recycleXmin)
{
    TransactionId oldestXmin = GetMultiSnapshotOldestXmin();
    *recycleXmin = oldestXmin;
    TransactionId globalRecycleXid = pg_atomic_read_u64(&g_instance.undo_cxt.globalRecycleXid);
    TransactionId xmin = InvalidTransactionId;
    if (ENABLE_TCAP_VERSION) {
        xmin = g_instance.flashback_cxt.globalOldestXminInFlashback;
        if (TransactionIdIsValid(xmin)) {
            *recycleXmin = (oldestXmin < xmin) ? oldestXmin : xmin;
        }
        if (unlikely(TransactionIdPrecedes(*recycleXmin, globalRecycleXid))) {
            *recycleXmin = globalRecycleXid;
        }
    }
    ereport(DEBUG1, (errmodule(MOD_UNDO),
        errmsg("recycleXmin is %lu, globalOldestXminInFlashback is %lu, oldestXmin is %lu.",
            *recycleXmin, g_instance.flashback_cxt.globalOldestXminInFlashback, oldestXmin)));
    return oldestXmin;
}

/*
 * GetMaxSnapshotXidCount -- get max size for snapshot XID array
 *
 * We have to export this for use by snapmgr.c.
 */
int GetMaxSnapshotXidCount(void)
{
    return g_instance.proc_array_idx->maxProcs;
}

/*
 * GetMaxSnapshotSubxidCount -- get max size for snapshot sub-XID array
 *
 * We have to export this for use by snapmgr.c.
 */
int GetMaxSnapshotSubxidCount(void)
{
    return TOTAL_MAX_CACHED_SUBXIDS;
}

/*
 * returns oldest transaction for catalog that was running when any current transaction was started.
 * take replication slot into consideration. please make sure it's safe to read replication_slot_catalog_xmin
 * before calling this func.
 */
TransactionId GetOldestCatalogXmin()
{
    TransactionId res = u_sess->utils_cxt.RecentGlobalXmin;
    TransactionId repSlotCatalogXmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;
    if (TransactionIdIsNormal(repSlotCatalogXmin) && TransactionIdPrecedes(repSlotCatalogXmin, res)) {
        return repSlotCatalogXmin;
    }
    return res;
}

#ifndef ENABLE_MULTIPLE_NODES
static void GroupGetSnapshotInternal(PGXACT* pgxact, Snapshot snapshot, TransactionId *xmin)
{
    if (!TransactionIdIsValid(pgxact->xmin)) {
        pgxact->xmin  = *xmin;
    }

    if (snapshot->takenDuringRecovery && TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin)) {
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin, *xmin)) {
            *xmin = t_thrd.xact_cxt.ShmemVariableCache->standbyXmin;
        }
        pgxact->xmin = *xmin;
    }

    snapshot->snapshotcsn = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
}

/*
 * GroupGetSnapshot -- group snapshot getting
 *
 * When we cannot immediately acquire ProcArrayLock in exclusive mode, add
 * ourselves to a list of processes that need to get snapshot.
 * The first process to add itself to the list will acquire ProcArrayLock
 * in exclusive mode and perform GroupGetSnapshotInternal on behalf of all
 * group members.  This avoids a great deal of contention around
 * ProcArrayLock when many processes are trying to get snapshot at once,
 * since the lock need not be repeatedly handed off from one process to the next.
 */
static void GroupGetSnapshot(PGPROC* proc)
{
    uint32 nextidx;
    uint32 wakeidx;
    TransactionId xmin;
    TransactionId xmax;
    TransactionId globalxmin;
    volatile TransactionId replication_slot_xmin = InvalidTransactionId;
    volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;
    bool clearGroup = false;

    HOLD_INTERRUPTS();

    /* Add ourselves to the list of processes needing to get snapshot. */
    proc->snapshotGroupMember = true;
    while (true) {
        nextidx = pg_atomic_read_u32(&g_instance.proc_base->snapshotGroupFirst);
        pg_atomic_write_u32(&proc->snapshotGroupNext, nextidx);

        /* Ensure all previous writes are visible before follower continues. */
        pg_memory_barrier();

        if (pg_atomic_compare_exchange_u32(
            &g_instance.proc_base->snapshotGroupFirst, &nextidx, (uint32)proc->pgprocno))
            break;
    }

    /*
     * If the list was not empty, the leader will get our snapshot. It is
     * impossible to have followers without a leader because the first process
     * that has added itself to the list will always have nextidx as
     * INVALID_PGPROCNO.
     */
    if (nextidx != INVALID_PGPROCNO) {
        int extraWaits = 0;

        /* Sleep until the leader gets our snapshot. */
        for (;;) {
            /* acts as a read barrier */
            PGSemaphoreLock(&proc->sem, false);
            if (!proc->snapshotGroupMember)
                break;
            extraWaits++;
        }

        Assert(pg_atomic_read_u32(&proc->snapshotGroupNext) == INVALID_PGPROCNO);

        /* Fix semaphore count for any absorbed wakeups */
        while (extraWaits-- > 0)
            PGSemaphoreUnlock(&proc->sem);

        /* in case of memory reordering in relaxed memory model like ARM */
        pg_memory_barrier();

        RESUME_INTERRUPTS();

        return;
    }
    RESUME_INTERRUPTS();

    /* We are the leader.  Acquire the lock on behalf of everyone. */
    bool retryGet = false;
RETRY_GET:
    if (retryGet) {
        if (InterruptPending) {
            clearGroup = true;
        }
        pg_usleep(100L);
    }
    if (!clearGroup) {
        XLogRecPtr redoEndLsn = GetXLogReplayRecPtr(NULL, NULL);
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        bool condition = (t_thrd.xact_cxt.ShmemVariableCache->standbyXmin <=
            t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin) &&
            (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn > redoEndLsn);
        if (condition) {
            LWLockRelease(ProcArrayLock);
            retryGet = true;
            goto RETRY_GET;
        }

        /*
         * Now that we've got the lock, clear the list of processes waiting for
         * group snapshot getting, saving a pointer to the head of the list. Trying
         * to pop elements one at a time could lead to an ABA problem.
         */
        while (true) {
            nextidx = pg_atomic_read_u32(&g_instance.proc_base->snapshotGroupFirst);
            if (pg_atomic_compare_exchange_u32(&g_instance.proc_base->snapshotGroupFirst, &nextidx, INVALID_PGPROCNO))
                break;
        }

        /* Remember head of list so we can perform wakeups after dropping lock. */
        wakeidx = nextidx;

        /* calculate the following infos after we have got ProcArrayLock. */
        /* xmax is always latestCompletedXid + 1 */
        xmax = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
        Assert(TransactionIdIsNormal(xmax));
        TransactionIdAdvance(xmax);

        /* initialize xmin calculation with xmax */
        globalxmin = xmin = xmax;

        /* fetch into volatile var while ProcArrayLock is held */
        replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;
        replication_slot_catalog_xmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;


        /* Walk the list and get all snapshots. */
        while (nextidx != INVALID_PGPROCNO) {
            PGPROC* procMember = g_instance.proc_base_all_procs[nextidx];
            PGXACT* pgxact = &g_instance.proc_base_all_xacts[nextidx];

            pg_memory_barrier();
            GroupGetSnapshotInternal(pgxact, procMember->snapshotGroup, &xmin);

            procMember->xminGroup = xmin;
            procMember->xmaxGroup = xmax;
            procMember->globalxminGroup = globalxmin;
            procMember->replicationSlotXminGroup = replication_slot_xmin;
            procMember->replicationSlotCatalogXminGroup = replication_slot_catalog_xmin;

            /* Move to next proc in list. */
            nextidx = pg_atomic_read_u32(&procMember->snapshotGroupNext);
        }

        /* We're done with the lock now. */
        LWLockRelease(ProcArrayLock);
    } else {
        /* clear the group, then process interrupt */
        while (true) {
            nextidx = pg_atomic_read_u32(&g_instance.proc_base->snapshotGroupFirst);
            if (pg_atomic_compare_exchange_u32(&g_instance.proc_base->snapshotGroupFirst, &nextidx, INVALID_PGPROCNO))
                break;
        }

        wakeidx = nextidx;
    }
    /*
     * Now that we've released the lock, go back and wake everybody up.  We
     * don't do this under the lock so as to keep lock hold times to a
     * minimum.  The system calls we need to perform to wake other processes
     * up are probably much slower than the simple memory writes we did while
     * holding the lock.
     */
    while (wakeidx != INVALID_PGPROCNO) {
        PGPROC* procMember = g_instance.proc_base_all_procs[wakeidx];

        wakeidx = pg_atomic_read_u32(&procMember->snapshotGroupNext);
        pg_atomic_write_u32(&procMember->snapshotGroupNext, INVALID_PGPROCNO);

        /* ensure all previous writes are visible before follower continues. */
        pg_memory_barrier();

        procMember->snapshotGroupMember = false;

        if (procMember != t_thrd.proc)
            PGSemaphoreUnlock(&procMember->sem);
    }
    if (clearGroup) {
        CHECK_FOR_INTERRUPTS();
    }
}

void AgentCopySnapshot(TransactionId *xmin, TransactionId *xmax, CommitSeqNo *snapcsn)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int* pgprocnos = arrayP->pgprocnos;
    int numProcs = arrayP->numProcs;
    volatile PGXACT* pgxact = NULL;
    PGPROC* proc = NULL;
    int pgprocno;
    int maxPgprocno;
    TransactionId pgprocXmin;
    TransactionId maxpgprocXmin;

    maxpgprocXmin = InvalidTransactionId;
    for (int index = 0; index < numProcs; index++) {
        pgprocno = pgprocnos[index];
        pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        /*
         * Backend is doing logical decoding which manages snapshot
         * separately, check below.
         */
        if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING) {
            continue;
        }

        if (pgxact == t_thrd.pgxact) {
            continue;
        }

        pgprocXmin = pgxact->xmin;
        /* get pgprocno with maximal xmin to reduce recovery conflict. */
        if (TransactionIdIsNormal(pgprocXmin) && TransactionIdPrecedes(maxpgprocXmin, pgprocXmin)) {
            maxpgprocXmin = pgprocXmin;
            maxPgprocno = pgprocno;
        }
    }

    if (TransactionIdIsValid(maxpgprocXmin)) {
        pgxact = &g_instance.proc_base_all_xacts[maxPgprocno];
        proc = g_instance.proc_base_all_procs[maxPgprocno];

        *xmin = pgxact->xmin;
        *xmax = proc->snapXmax;
        *snapcsn = proc->snapCSN;
    } else {
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        *snapcsn = InvalidCommitSeqNo;
    }
}
#endif

/*
 * GetSnapshotData -- returns information about running transactions.
 *
 * The returned snapshot includes xmin (lowest still-running xact ID),
 * xmax (highest completed xact ID + 1), and a list of running xact IDs
 * in the range xmin <= xid < xmax.  It is used as follows:
 *		All xact IDs < xmin are considered finished.
 *		All xact IDs >= xmax are considered still running.
 *		For an xact ID xmin <= xid < xmax, consult list to see whether
 *		it is considered running or not.
 * This ensures that the set of transactions seen as "running" by the
 * current xact will not change after it takes the snapshot.
 *
 * All running top-level XIDs are included in the snapshot, except for lazy
 * VACUUM processes.  We also try to include running subtransaction XIDs,
 * but since PGPROC has only a limited cache area for subxact XIDs, full
 * information may not be available.  If we find any overflowed subxid arrays,
 * we have to mark the snapshot's subxid data as overflowed, and extra work
 * *may* need to be done to determine what's running (see XidInMVCCSnapshot()
 * in heapam_visibility.c).
 *
 * We also update the following backend-global variables:
 *		TransactionXmin: the oldest xmin of any snapshot in use in the
 *			current transaction (this is the same as MyPgXact->xmin).
 *		RecentXmin: the xmin computed for the most recent snapshot.  XIDs
 *			older than this are known not running any more.
 *		RecentGlobalXmin: the global xmin (oldest TransactionXmin across all
 *			running transactions, except those running LAZY VACUUM).  This is
 *			the same computation done by GetOldestXmin(true, true).
 *     RecentGlobalDataXmin: the global xmin for non-catalog tables
 *         >= RecentGlobalXmin
 *
 * Note: this function should probably not be called with an argument that's
 * not statically allocated (see xip allocation below).
 */
#ifndef ENABLE_MULTIPLE_NODES
Snapshot GetSnapshotData(Snapshot snapshot, bool force_local_snapshot, bool forHSFeedBack)
#else
Snapshot GetSnapshotData(Snapshot snapshot, bool force_local_snapshot)
#endif
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    TransactionId xmin;
    TransactionId xmax;
    TransactionId globalxmin;
    int index;
    volatile TransactionId replication_slot_xmin = InvalidTransactionId;
    volatile TransactionId replication_slot_catalog_xmin = InvalidTransactionId;
    bool is_exec_cn = IS_PGXC_COORDINATOR && !IsConnFromCoord();
    bool is_exec_dn = IS_PGXC_DATANODE && !IsConnFromCoord() && !IsConnFromDatanode();
    WaitState oldStatus = STATE_WAIT_UNDEFINED;

    Assert(snapshot != NULL);

#ifdef PGXC /* PGXC_DATANODE */

    t_thrd.xact_cxt.useLocalSnapshot = false;

    if ((IS_MULTI_DISASTER_RECOVER_MODE && !is_exec_dn) ||
        (GTM_LITE_MODE &&
         ((is_exec_cn && !force_local_snapshot) ||                                        /* GTM_LITE exec cn */
          (!is_exec_cn && u_sess->utils_cxt.snapshot_source == SNAPSHOT_COORDINATOR)))) { /* GTM_LITE other node */
        /*
         * Obtain a global snapshot for a openGauss session
         * if possible. When not in postmaster environment, get local snapshot, --single mode e.g.
         */
        if (!useLocalXid) {
            if (!u_sess->attr.attr_common.xc_maintenance_mode && IsPostmasterEnvironment &&
                GetPGXCSnapshotData(snapshot)) {
                return snapshot;
            }
        }
    }

    /* first we try to get multiversion snapshot */
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE ||
        t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE) {
RETRY:
        if (GTM_LITE_MODE) {
            /* local snapshot, setup preplist array, must construct preplist before getting local snapshot */
            SetLocalSnapshotPreparedArray(snapshot);
            snapshot->gtm_snapshot_type = GTM_SNAPSHOT_TYPE_LOCAL;
        }

        Snapshot result;
        if (ENABLE_DMS) {
            /* fetch TXN info locally if either reformer, original primary, or normal primary */
            if (SSCanFetchLocalSnapshotTxnRelatedInfo()) {
                result = GetLocalSnapshotData(snapshot);
                snapshot->snapshotcsn = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
            } else {
                result = SSGetSnapshotData(snapshot);
                if (result == NULL) {
                    ereport(ERROR, (errmsg("failed to request snapshot as current node is in reform!")));
                }
            }
        } else {
            result = GetLocalSnapshotData(snapshot);
            snapshot->snapshotcsn = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
        }

        if (result) {
            if (GTM_LITE_MODE) {
                /* gtm lite check csn, if not pass, try to get local snapshot form multiversion again */
                CommitSeqNo return_csn = set_proc_csn_and_check("GetLocalSnapshotData", snapshot->snapshotcsn,
                                                                snapshot->gtm_snapshot_type, SNAPSHOT_DATANODE);
                if (!COMMITSEQNO_IS_COMMITTED(return_csn)) {
                    ereport(LOG,
                            (errcode(ERRCODE_SNAPSHOT_INVALID), errmsg("Retry to get local multiversion snapshot")));
                    goto RETRY;
                }
                u_sess->utils_cxt.RecentGlobalXmin = GetOldestXmin(NULL, true);
                u_sess->utils_cxt.RecentGlobalCatalogXmin = GetOldestCatalogXmin();
            }

            return result;
        }
    }
    /* For gtm-lite and gtm-free, use local snapshot */
    t_thrd.xact_cxt.useLocalSnapshot = true;

    /*
     * The codes below run when GetPGXCSnapshotData() couldn't get snapshot from
     * GTM.  So no data in snapshot will be used.
     */
    cleanSnapshot(snapshot);

#endif

    /* By here no available version for local snapshot
     *
     * It is sufficient to get shared lock on ProcArrayLock, even if we are
     * going to set MyPgXact->xmin.
     */
    snapshot->takenDuringRecovery = RecoveryInProgress();
    if (snapshot->takenDuringRecovery) {
        oldStatus = pgstat_report_waitstatus(STATE_STANDBY_GET_SNAPSHOT);
    }
    bool retry_get = false;
    uint64 retry_count = 0;
    const static uint64 WAIT_COUNT = 0x7FFFF;
    /* reset xmin before acquiring lwlock, in case blocking redo */
    t_thrd.pgxact->xmin = InvalidTransactionId;
RETRY_GET:
    if (snapshot->takenDuringRecovery && !StreamThreadAmI() && !IS_EXRTO_READ &&
        !u_sess->proc_cxt.clientIsCMAgent) {
        if (InterruptPending) {
            (void)pgstat_report_waitstatus(oldStatus);
        }
        if (retry_get) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(100L);
        }
        XLogRecPtr redoEndLsn = GetXLogReplayRecPtr(NULL, NULL);
        retry_count++;
        if ((retry_count & WAIT_COUNT) == WAIT_COUNT) {
            ereport(LOG, (errmsg("standbyRedoCleanupXmin = %ld, "
                                 "standbyRedoCleanupXminLsn = %ld, "
                                 "standbyXmin = %ld, redoEndLsn = %ld",
                                 t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin,
                                 t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn,
                                 t_thrd.xact_cxt.ShmemVariableCache->standbyXmin,
                                 redoEndLsn)));
        }
        if ((u_sess->proc_cxt.gsqlRemainCopyNum > 0 && retry_get)) {
            LWLockAcquire(ProcArrayLock, LW_SHARED);
            if ((t_thrd.xact_cxt.ShmemVariableCache->standbyXmin
                <= t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin)
                && (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn > redoEndLsn)) {
                /*
                 * If CM agent cannot get consistency snapshot immediately, try
                 * getting snapshot from other backends.
                 */
                AgentCopySnapshot(&xmin, &xmax, &snapshot->snapshotcsn);
                bool obtained = TransactionIdIsValid(xmin) && TransactionIdIsValid(xmax) &&
                    snapshot->snapshotcsn != InvalidCommitSeqNo;
                if (obtained) {
                    globalxmin = xmin;
                    /* fetch into volatile var while ProcArrayLock is held */
                    replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;
                    replication_slot_catalog_xmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;

                    if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
                        t_thrd.pgxact->handle = GetCurrentTransactionHandleIfAny();
                    }
                    t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = xmin;
                    LWLockRelease(ProcArrayLock);
                    u_sess->proc_cxt.gsqlRemainCopyNum--;
                    /* reuse the groupgetsnapshot logic to set snapshot and thread information. */
                    goto GROUP_GET_SNAPSHOT;
                }
                LWLockRelease(ProcArrayLock);
                retry_get = true;
                goto RETRY_GET;
            }
        } else if (LWLockConditionalAcquire(ProcArrayLock, LW_EXCLUSIVE)) {
            if ((t_thrd.xact_cxt.ShmemVariableCache->standbyXmin <=
                t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin) &&
                (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn > redoEndLsn) &&
                parallel_recovery::in_full_sync_dispatch()) {
                LWLockRelease(ProcArrayLock);
                retry_get = true;
                goto RETRY_GET;
            }
#ifndef ENABLE_MULTIPLE_NODES
        } else if (forHSFeedBack) {
            LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
            if ((t_thrd.xact_cxt.ShmemVariableCache->standbyXmin
                <= t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin)
                && (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn > redoEndLsn)) {
                LWLockRelease(ProcArrayLock);
                retry_get = true;
                goto RETRY_GET;
            }
        }
#endif
        else {
            if (!retry_get) {
                retry_get = true;
                goto RETRY_GET;
            }

            if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
                t_thrd.pgxact->handle = GetCurrentTransactionHandleIfAny();
            }
            t_thrd.proc->snapshotGroup = snapshot;

            /* ensure all previous writes are visible before setting snapshotGroup. */
            pg_memory_barrier();

            GroupGetSnapshot(t_thrd.proc);

            xmin = t_thrd.proc->xminGroup;
            xmax = t_thrd.proc->xmaxGroup;
            globalxmin = t_thrd.proc->globalxminGroup;
            replication_slot_xmin = t_thrd.proc->replicationSlotXminGroup;
            replication_slot_catalog_xmin = t_thrd.proc->replicationSlotCatalogXminGroup;
            u_sess->utils_cxt.TransactionXmin = t_thrd.pgxact->xmin;

            t_thrd.proc->snapshotGroup = NULL;
            t_thrd.proc->xminGroup = InvalidTransactionId;
            t_thrd.proc->xmaxGroup = InvalidTransactionId;
            t_thrd.proc->globalxminGroup = InvalidTransactionId;
            t_thrd.proc->replicationSlotXminGroup = InvalidTransactionId;
            t_thrd.proc->replicationSlotCatalogXminGroup = InvalidTransactionId;

            if (snapshot->snapshotcsn == 0) {
                retry_get = true;
                goto RETRY_GET;
            }
            goto GROUP_GET_SNAPSHOT;
        }
    } else {
        LWLockAcquire(ProcArrayLock, LW_SHARED);
    }
    /* xmax is always latestCompletedXid + 1 */
    xmax = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    Assert(TransactionIdIsNormal(xmax));
    TransactionIdAdvance(xmax);

    /* initialize xmin calculation with xmax */
    globalxmin = xmin = xmax;

    /*
     * If we're in recovery then snapshot data comes from a different place,
     * so decide which route we take before grab the lock. It is possible for
     * recovery to end before we finish taking snapshot, and for newly
     * assigned transaction ids to be added to the procarray. Xmax cannot
     * change while we hold ProcArrayLock, so those newly added transaction
     * ids would be filtered away, so we need not be concerned about them.
     */

#ifndef ENABLE_MULTIPLE_NODES
    if (!snapshot->takenDuringRecovery || forHSFeedBack) {
#else
    if (!snapshot->takenDuringRecovery) {
#endif
        int* pgprocnos = arrayP->pgprocnos;
        int numProcs;

        /*
         * Spin over procArray checking xid, xmin, and subxids.  The goal is
         * to gather all active xids, find the lowest xmin, and try to record
         * subxids.
         */
        numProcs = arrayP->numProcs;

        for (index = 0; index < numProcs; index++) {
            int pgprocno = pgprocnos[index];
            volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
            TransactionId xid = InvalidTransactionId;
            /*
             * Backend is doing logical decoding which manages xmin
             * separately, check below.
             */
            if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
                continue;

            /* Update globalxmin to be the smallest valid xmin, only Ignore procs running LAZY VACUUM xmin */
            if (!(pgxact->vacuumFlags & PROC_IN_VACUUM)) {
                xid = pgxact->xmin; /* fetch just once */
            }

            if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, globalxmin))
                globalxmin = xid;

            /* Fetch xid just once - see GetNewTransactionId */
            xid = pgxact->xid;

            /* If no XID assigned, use xid passed down from CN */
            if (!TransactionIdIsNormal(xid))
                xid = pgxact->next_xid;

            /*
             * If the transaction has no XID assigned, we can skip it; it
             * won't have sub-XIDs either.  If the XID is >= xmax, we can also
             * skip it; such transactions will be treated as running anyway
             * (and any sub-XIDs will also be >= xmax).
             */
            if (!TransactionIdIsNormal(xid) || !TransactionIdPrecedes(xid, xmax))
                continue;

            /*
             * We don't include our own XIDs (if any) in the snapshot, but we
             * must include them in xmin.
             */
            if (TransactionIdPrecedes(xid, xmin))
                xmin = xid;

            if (pgxact == t_thrd.pgxact)
                continue;
        }
    }

    /* fetch into volatile var while ProcArrayLock is held */
    replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;
    replication_slot_catalog_xmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;

    if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = xmin;
        t_thrd.pgxact->handle = GetCurrentTransactionHandleIfAny();
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (snapshot->takenDuringRecovery && TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin)) {
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin, xmin)) {
            xmin = t_thrd.xact_cxt.ShmemVariableCache->standbyXmin;
        }
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = xmin;
    }
#endif

    snapshot->snapshotcsn = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);

    if (GTM_LITE_MODE) {  /* gtm lite check csn, should always pass the check */
        (void)set_proc_csn_and_check("GetLocalSnapshotDataFromProc", snapshot->snapshotcsn,
                                     snapshot->gtm_snapshot_type, SNAPSHOT_LOCAL);
    }

    LWLockRelease(ProcArrayLock);

#ifndef ENABLE_MULTIPLE_NODES
GROUP_GET_SNAPSHOT:
#endif
    /* Save the xmax and csn, so that the CM agent can obtain them. */
    t_thrd.proc->snapXmax = xmax;
    t_thrd.proc->snapCSN = snapshot->snapshotcsn;

    /*
     * Update globalxmin to include actual process xids.  This is a slightly
     * different way of computing it than GetOldestXmin uses, but should give
     * the same result.
     */
    if (TransactionIdPrecedes(xmin, globalxmin)) {
        globalxmin = xmin;
    }

    /* When initdb we set vacuum_defer_cleanup_age to zero, so we can vacuum
        freeze three default database to avoid that localxid larger than GTM next_xid. */
    if (isSingleMode) {
        u_sess->attr.attr_storage.vacuum_defer_cleanup_age = 0;
    }

    /* Update global variables too */
    if (TransactionIdPrecedes(globalxmin, (uint64)u_sess->attr.attr_storage.vacuum_defer_cleanup_age)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    } else {
        u_sess->utils_cxt.RecentGlobalXmin = globalxmin - u_sess->attr.attr_storage.vacuum_defer_cleanup_age;
    }

    if (!TransactionIdIsNormal(u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    }

    /* Check whether there's a replication slot requiring an older xmin. */
    if (TransactionIdIsValid(replication_slot_xmin) &&
        TransactionIdPrecedes(replication_slot_xmin, u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = replication_slot_xmin;
    }

    /* Check whether there's a standby requiring an older xmin when dms is enabled. */
    if (SS_NORMAL_PRIMARY && SS_DISASTER_MAIN_STANDBY_NODE) {
        uint64 global_xmin = SSGetGlobalOldestXmin(u_sess->utils_cxt.RecentGlobalXmin);
        u_sess->utils_cxt.RecentGlobalXmin = global_xmin;
    }

    /* Non-catalog tables can be vacuumed if older than this xid */
    u_sess->utils_cxt.RecentGlobalDataXmin = u_sess->utils_cxt.RecentGlobalXmin;

    /*
     * Check whether there's a replication slot requiring an older catalog
     * xmin.
     */
    if (TransactionIdIsNormal(replication_slot_catalog_xmin) &&
        NormalTransactionIdPrecedes(replication_slot_catalog_xmin, u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = replication_slot_catalog_xmin;
    }
    u_sess->utils_cxt.RecentXmin = xmin;

#ifndef ENABLE_MULTIPLE_NODES
    if (forHSFeedBack) {
        u_sess->utils_cxt.RecentGlobalXmin = globalxmin;
    }
#endif

    snapshot->xmin = xmin;
    snapshot->xmax = xmax;
    snapshot->curcid = GetCurrentCommandId(false);

#ifdef PGXC

    if (!RecoveryInProgress()) {
        int errlevel = LOG;

        if (u_sess->attr.attr_common.xc_maintenance_mode || IsAutoVacuumLauncherProcess() || !IsNormalProcessingMode())
            errlevel = DEBUG1;

        /* Just ForeignScan runs in the compute pool, the snapshot and gxid is
         * not necessary. To avoid too much log, we set errlevel to DEBUG1. */
        if (IS_PGXC_COORDINATOR && (StreamTopConsumerAmI() || t_thrd.wlm_cxt.wlmalarm_dump_active))
            errlevel = DEBUG1;

        if (!GTM_FREE_MODE && !t_thrd.postgres_cxt.isInResetUserName)
            ereport(errlevel,
                    (errmsg("Local snapshot is built, xmin: %lu, xmax: %lu, "
                            "RecentGlobalXmin: %lu",
                            xmin,
                            xmax,
                            globalxmin)));
    }

#endif

    /*
     * This is a new snapshot, so set both refcounts are zero, and mark it as
     * not copied in persistent memory.
     */
    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->copied = false;

    if (snapshot->takenDuringRecovery) {
        if (IsDefaultExtremeRtoMode() && IS_EXRTO_STANDBY_READ) {
            exrto_read_snapshot(snapshot);
            if (t_thrd.proc->exrto_reload_cache) {
                t_thrd.proc->exrto_reload_cache = false;
                reset_invalidation_cache();
            }
            AcceptInvalidationMessages();
        }
        (void)pgstat_report_waitstatus(oldStatus);
    }

    return snapshot;
}

void exrto_get_snapshot_data(TransactionId &xmin, TransactionId &xmax, CommitSeqNo &snapshot_csn)
{
    LWLockAcquire(ProcArrayLock, LW_SHARED);
 
    /* xmax is always latest_completed_xid + 1 */
    xmax = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
 
    Assert(TransactionIdIsNormal(xmax));
    TransactionIdAdvance(xmax);
    /* initialize xmin calculation with xmax */
    xmin = xmax;
    if (TransactionIdIsValid(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin)) {
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->standbyXmin, xmin)) {
            xmin = t_thrd.xact_cxt.ShmemVariableCache->standbyXmin;
        }
    }
 
    LWLockRelease(ProcArrayLock);
    snapshot_csn = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
}

/*
 * ProcArrayInstallImportedXmin -- install imported xmin into MyPgXact->xmin
 *
 * This is called when installing a snapshot imported from another
 * transaction.  To ensure that OldestXmin doesn't go backwards, we must
 * check that the source transaction is still running, and we'd better do
 * that atomically with installing the new xmin.
 *
 * Returns TRUE if successful, FALSE if source xact is no longer running.
 */
bool ProcArrayInstallImportedXmin(TransactionId xmin, VirtualTransactionId *sourcevxid)
{
    bool result = false;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;

    Assert(TransactionIdIsNormal(xmin));

    if (!sourcevxid)
        return false;

    /* Get lock so source xact can't end while we're doing this */
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId xid;

        /* We are only interested in the specific virtual transaction. */
        if (proc->backendId != sourcevxid->backendId)
            continue;
        if (proc->lxid != sourcevxid->localTransactionId)
            continue;

        /*
         * We check the transaction's database ID for paranoia's sake: if it's
         * in another DB then its xmin does not cover us.  Caller should have
         * detected this already, so we just treat any funny cases as
         * "transaction not found".
         */
        if (proc->databaseId != u_sess->proc_cxt.MyDatabaseId)
            continue;

        /*
         * Likewise, let's just make real sure its xmin does cover us.
         */
        xid = pgxact->xmin; /* fetch just once */

        if (!TransactionIdIsNormal(xid) || !TransactionIdPrecedesOrEquals(xid, xmin))
            continue;

        /*
         * We're good.  Install the new xmin.  As in GetSnapshotData, set
         * TransactionXmin too.  (Note that because snapmgr.c called
         * GetSnapshotData first, we'll be overwriting a valid xmin here, so
         * we don't check that.)
         */
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = xmin;

        result = true;
        break;
    }

    LWLockRelease(ProcArrayLock);

    return result;
}

typedef struct GTM_RunningXacts {
    int cur_index;
} GTM_RunningXacts;

Datum pg_get_running_xacts(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    GTM_RunningXacts* status = NULL;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pg_prepared_xacts view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(10, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "handle", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "gxid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "state", INT1OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "node", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "xmin", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "vacuum", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "timeline", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "prepare_xid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "next_xid", XIDOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Collect all the 2PC status information that we will format and send
         * out as a result set.
         */
        status = (GTM_RunningXacts*)palloc(sizeof(GTM_RunningXacts));
        status->cur_index = 0;
        funcctx->user_fctx = (void*)status;

        MemoryContextSwitchTo(oldcontext);

        /*
         * Ensure that no xids enter or leave the procarray while we obtain
         * snapshot.
         */
        LWLockAcquire(ProcArrayLock, LW_SHARED);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (GTM_RunningXacts*)funcctx->user_fctx;

    while (status->cur_index < arrayP->numProcs) {
        int pgprocno = arrayP->pgprocnos[status->cur_index++];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        Datum values[10];
        bool nulls[10];
        HeapTuple tuple;
        Datum result;

        /* Skip self */
        if (pgxact == t_thrd.pgxact)
            continue;

        /*
         * Form tuple with appropriate data.
         */
        errno_t ret = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(ret, "\0", "\0");
        ret = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ret, "\0", "\0");

        values[0] = Int32GetDatum(pgxact->handle);
        values[1] = TransactionIdGetDatum(pgxact->xid);

        if (TransactionIdIsPrepared(pgxact->xid))
            values[2] = Int8GetDatum(GTM_TXN_PREPARED);
        else
            values[2] = Int8GetDatum(GTM_TXN_STARTING);

        values[3] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[4] = TransactionIdGetDatum(pgxact->xmin);

        if (pgxact->vacuumFlags & PROC_IN_VACUUM)
            values[5] = BoolGetDatum(true);
        else
            values[5] = BoolGetDatum(false);

        values[6] = Int64GetDatum(get_controlfile_timeline());
        values[7] = TransactionIdGetDatum(pgxact->prepare_xid);
        values[8] = Int64GetDatum(proc->pid);
        values[9] = TransactionIdGetDatum(pgxact->next_xid);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    LWLockRelease(ProcArrayLock);
    SRF_RETURN_DONE(funcctx);
}

/*
 * Similar to GetSnapshotData but returns just oldestActiveXid. We include
 * all PGXACTs with an assigned TransactionId, even VACUUM processes.
 * We look at all databases, though there is no need to include WALSender
 * since this has no effect on hot standby conflicts.
 *
 * This is never executed during recovery so there is no need to look at
 * KnownAssignedXids.
 *
 * We don't worry about updating other counters, we want to keep this as
 * simple as possible and leave GetSnapshotData() as the primary code for
 * that bookkeeping.
 */
TransactionId GetOldestActiveTransactionId(TransactionId *globalXmin)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    TransactionId oldestRunningXid;
    int index;

    /* xmax is always latestCompletedXid + 1 */
    TransactionId xmax = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    Assert(TransactionIdIsNormal(xmax));
    TransactionIdAdvance(xmax);
    TransactionId xmin = xmax;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    /*
     * It's okay to read nextXid without acquiring XidGenLock because (1) we
     * assume TransactionIds can be read atomically and (2) we don't care if
     * we get a slightly stale value.  It can't be very stale anyway, because
     * the LWLockAcquire above will have done any necessary memory
     * interlocking.
     */
    oldestRunningXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;

    /*
     * Spin over procArray collecting all xids and subxids.
     */
    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId xid;

        /* Update globalxmin to be the smallest valid xmin */
        xid = pgxact->xmin; /* fetch just once */

        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, xmin))
            xmin = xid;

        /* Fetch xid just once - see GetNewTransactionId */
        xid = pgxact->xid;

        if (!TransactionIdIsNormal(xid))
            continue;

        if (TransactionIdPrecedes(xid, oldestRunningXid))
            oldestRunningXid = xid;

        /*
         * Top-level XID of a transaction is always less than any of its
         * subxids, so we don't need to check if any of the subxids are
         * smaller than oldestRunningXid
         */
    }

    LWLockRelease(ProcArrayLock);

    /*
     * Update globalxmin to include actual process xids.  This is a slightly
     * different way of computing it than GetOldestXmin uses, but should give
     * the same result.
     */
    if (TransactionIdPrecedes(oldestRunningXid, xmin)) {
        xmin = oldestRunningXid;
    }
    *globalXmin = xmin;
    if (IS_EXRTO_STANDBY_READ) {
        ereport(LOG, (errmsg("proc_array_get_oldest_active_transaction_id: global_xmin = %lu", *globalXmin)));
    }
    return oldestRunningXid;
}

/*
 * GetOldestSafeDecodingTransactionId -- lowest xid not affected by vacuum
 *
 * Returns the oldest xid that we can guarantee not to have been affected by
 * vacuum, i.e. no rows >= that xid have been vacuumed away unless the
 * transaction aborted. Note that the value can (and most of the time will) be
 * much more conservative than what really has been affected by vacuum, but we
 * currently don't have better data available.
 *
 * This is useful to initalize the cutoff xid after which a new changeset
 * extraction replication slot can start decoding changes.
 *
 * Must be called with ProcArrayLock held either shared or exclusively,
 * although most callers will want to use exclusive mode since it is expected
 * that the caller will immediately use the xid to peg the xmin horizon.
 */
TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    TransactionId oldestSafeXid;
    int index;
    bool recovery_in_progress = RecoveryInProgress();

    Assert(LWLockHeldByMe(ProcArrayLock));

    /*
     * Acquire XidGenLock, so no transactions can acquire an xid while we're
     * running. If no transaction with xid were running concurrently a new xid
     * could influence the the RecentXmin et al.
     *
     * We initialize the computation to nextXid since that's guaranteed to be
     * a safe, albeit pessimal, value.
     */
    LWLockAcquire(XidGenLock, LW_SHARED);
    oldestSafeXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;

    /*
     * If there's already a slot pegging the xmin horizon, we can start with
     * that value, it's guaranteed to be safe since it's computed by this
     * routine initially and has been enforced since.  We can always use the
     * slot's general xmin horizon, but the catalog horizon is only usable
     * when we only catalog data is going to be looked at.
     */
    if (TransactionIdIsValid(g_instance.proc_array_idx->replication_slot_xmin) &&
        TransactionIdPrecedes(g_instance.proc_array_idx->replication_slot_xmin, oldestSafeXid))
        oldestSafeXid = g_instance.proc_array_idx->replication_slot_xmin;

    if (catalogOnly && TransactionIdIsValid(g_instance.proc_array_idx->replication_slot_catalog_xmin) &&
        TransactionIdPrecedes(g_instance.proc_array_idx->replication_slot_catalog_xmin, oldestSafeXid))
        oldestSafeXid = g_instance.proc_array_idx->replication_slot_catalog_xmin;

    /*
     * If we're not in recovery, we walk over the procarray and collect the
     * lowest xid. Since we're called with ProcArrayLock held and have
     * acquired XidGenLock, no entries can vanish concurrently, since
     * PGXACT->xid is only set with XidGenLock held and only cleared with
     * ProcArrayLock held.
     *
     * In recovery we can't lower the safe value besides what we've computed
     * above, so we'll have to wait a bit longer there. We unfortunately can
     * *not* use KnownAssignedXidsGetOldestXmin() since the KnownAssignedXids
     * machinery can miss values and return an older value than is safe.
     */
    if (!recovery_in_progress) {
        /*
         * Spin over procArray collecting all min(PGXACT->xid)
         */
        for (index = 0; index < arrayP->numProcs; index++) {
            int pgprocno = arrayP->pgprocnos[index];
            volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
            TransactionId xid;

            /* Fetch xid just once - see GetNewTransactionId */
            xid = pgxact->xid;

            if (!TransactionIdIsNormal(xid))
                continue;

            if (TransactionIdPrecedes(xid, oldestSafeXid))
                oldestSafeXid = xid;
        }
    }

    LWLockRelease(XidGenLock);

    return oldestSafeXid;
}

/*
 * GetVirtualXIDsDelayingChkpt -- Get the XIDs of transactions that are
 * delaying checkpoint because they have critical actions in progress.
 *
 * Constructs an array of VXIDs of transactions that are currently in commit
 * critical sections, as shown by having delayChkpt set in their PGXACT.
 *
 * Returns a palloc'd array that should be freed by the caller.
 * *nvxids is the number of valid entries.
 *
 * Note that because backends set or clear delayChkpt without holding any lock,
 * the result is somewhat indeterminate, but we don't really care.  Even in
 * a multiprocessor with delayed writes to shared memory, it should be certain
 * that setting of delayChkpt will propagate to shared memory when the backend
 * takes a lock, so we cannot fail to see an virtual xact as delayChkpt if
 * it's already inserted its commit record.  Whether it takes a little while
 * for clearing of delayChkpt to propagate is unimportant for correctness.
 */
VirtualTransactionId* GetVirtualXIDsDelayingChkpt(int* nvxids)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;

    /* allocate what's certainly enough result space */
    VirtualTransactionId* vxids = (VirtualTransactionId*)palloc(sizeof(VirtualTransactionId) * arrayP->maxProcs);

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        if (pgxact->delayChkpt) {
            VirtualTransactionId vxid;

            GET_VXID_FROM_PGPROC(vxid, *proc);
            if (VirtualTransactionIdIsValid(vxid))
                vxids[count++] = vxid;
        }
    }

    LWLockRelease(ProcArrayLock);

    *nvxids = count;
    return vxids;
}

/*
 * HaveVirtualXIDsDelayingChkpt -- Are any of the specified VXIDs delaying?
 *
 * This is used with the results of GetVirtualXIDsDelayingChkpt to see if any
 * of the specified VXIDs are still in critical sections of code.
 *
 * Note: this is O(N^2) in the number of vxacts that are/were delaying, but
 * those numbers should be small enough for it not to be a problem.
 */
bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId* vxids, int nvxids)
{
    bool result = false;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        VirtualTransactionId vxid;

        GET_VXID_FROM_PGPROC(vxid, *proc);

        if (pgxact->delayChkpt && VirtualTransactionIdIsValid(vxid)) {
            int i;

            for (i = 0; i < nvxids; i++) {
                if (VirtualTransactionIdEquals(vxid, vxids[i])) {
                    result = true;
                    break;
                }
            }
            if (result) {
                break;
            }
        }
    }

    LWLockRelease(ProcArrayLock);

    return result;
}

/*
 * BackendPidGetProc -- get a backend's PGPROC given its PID
 *
 * Returns NULL if not found.  Note that it is up to the caller to be
 * sure that the question remains meaningful for long enough for the
 * answer to be used ...
 */
PGPROC* BackendPidGetProc(ThreadId pid)
{
    PGPROC* result = NULL;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;

    if (pid == 0) /* never match dummy PGPROCs */
        return NULL;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        PGPROC* proc = g_instance.proc_base_all_procs[arrayP->pgprocnos[index]];

        if (proc->pid == pid) {
            result = proc;
            break;
        }
    }

    LWLockRelease(ProcArrayLock);

    return result;
}

/*
 * BackendXidGetPid -- get a backend's pid given its XID
 *
 * Returns 0 if not found or it's a prepared transaction.  Note that
 * it is up to the caller to be sure that the question remains
 * meaningful for long enough for the answer to be used ...
 *
 * Only main transaction Ids are considered.  This function is mainly
 * useful for determining what backend owns a lock.
 *
 * Beware that not every xact has an XID assigned.	However, as long as you
 * only call this using an XID found on disk, you're safe.
 */
int BackendXidGetPid(TransactionId xid)
{
    int result = 0;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;

    if (xid == InvalidTransactionId) /* never match invalid xid */
        return 0;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        if (pgxact->xid == xid) {
            result = proc->pid;
            break;
        }
    }

    LWLockRelease(ProcArrayLock);

    return result;
}

/*
 * IsBackendPid -- is a given pid a running backend
 */
bool IsBackendPid(ThreadId pid)
{
    return (BackendPidGetProc(pid) != NULL);
}

/*
 * GetCurrentVirtualXIDs -- returns an array of currently active VXIDs.
 *
 * The array is palloc'd. The number of valid entries is returned into *nvxids.
 *
 * The arguments allow filtering the set of VXIDs returned.  Our own process
 * is always skipped.  In addition:
 *	If limitXmin is not InvalidTransactionId, skip processes with
 *		xmin > limitXmin.
 *	If excludeXmin0 is true, skip processes with xmin = 0.
 *	If allDbs is false, skip processes attached to other databases.
 *	If excludeVacuum isn't zero, skip processes for which
 *		(vacuumFlags & excludeVacuum) is not zero.
 *
 * Note: the purpose of the limitXmin and excludeXmin0 parameters is to
 * allow skipping backends whose oldest live snapshot is no older than
 * some snapshot we have.  Since we examine the procarray with only shared
 * lock, there are race conditions: a backend could set its xmin just after
 * we look.  Indeed, on multiprocessors with weak memory ordering, the
 * other backend could have set its xmin *before* we look.	We know however
 * that such a backend must have held shared ProcArrayLock overlapping our
 * own hold of ProcArrayLock, else we would see its xmin update.  Therefore,
 * any snapshot the other backend is taking concurrently with our scan cannot
 * consider any transactions as still running that we think are committed
 * (since backends must hold ProcArrayLock exclusive to commit).
 */
VirtualTransactionId* GetCurrentVirtualXIDs(
    TransactionId limitXmin, bool excludeXmin0, bool allDbs, int excludeVacuum, int* nvxids)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;

    /* allocate what's certainly enough result space */
    VirtualTransactionId* vxids = (VirtualTransactionId*)palloc(sizeof(VirtualTransactionId) * arrayP->maxProcs);

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        if (proc == t_thrd.proc)
            continue;

        if (excludeVacuum & pgxact->vacuumFlags)
            continue;

        if (allDbs || proc->databaseId == u_sess->proc_cxt.MyDatabaseId) {
            /* Fetch xmin just once - might change on us */
            TransactionId pxmin = pgxact->xmin;

            if (excludeXmin0 && !TransactionIdIsValid(pxmin))
                continue;

            /*
             * InvalidTransactionId precedes all other XIDs, so a proc that
             * hasn't set xmin yet will not be rejected by this test.
             */
            if (!TransactionIdIsValid(limitXmin) || TransactionIdPrecedesOrEquals(pxmin, limitXmin)) {
                VirtualTransactionId vxid;

                GET_VXID_FROM_PGPROC(vxid, *proc);

                if (VirtualTransactionIdIsValid(vxid))
                    vxids[count++] = vxid;
            }
        }
    }

    LWLockRelease(ProcArrayLock);

    *nvxids = count;
    return vxids;
}

void UpdateCleanUpInfo(TransactionId limitXmin, XLogRecPtr lsn)
{
    if (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin < limitXmin) {
        t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin = limitXmin;
        const int xid_gap = 10000000;
        if (limitXmin > t_thrd.xact_cxt.ShmemVariableCache->standbyXmin + xid_gap) {
            ereport(LOG, (errmsg("limitXmin = %ld, standbyRedoCleanupXmin = %ld, "
                                 "lsn = %ld, standbyRedoCleanupXminLsn = %ld, "
                                 "standbyXmin = %ld",
                                 limitXmin, t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin,
                                 lsn, t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn,
                                 t_thrd.xact_cxt.ShmemVariableCache->standbyXmin)));
        }
    }

    if (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn < lsn) {
        t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn = lsn;
    }
}

/*
 * GetConflictingVirtualXIDs -- returns an array of currently active VXIDs.
 *
 * Usage is limited to conflict resolution during recovery on standby servers.
 * limitXmin is supplied as either latestRemovedXid, or InvalidTransactionId
 * in cases where we cannot accurately determine a value for latestRemovedXid.
 *
 * If limitXmin is InvalidTransactionId then we want to kill everybody,
 * so we're not worried if they have a snapshot or not, nor does it really
 * matter what type of lock we hold.
 *
 * All callers that are checking xmins always now supply a valid and useful
 * value for limitXmin. The limitXmin is always lower than the lowest
 * numbered KnownAssignedXid that is not already a FATAL error. This is
 * because we only care about cleanup records that are cleaning up tuple
 * versions from committed transactions. In that case they will only occur
 * at the point where the record is less than the lowest running xid. That
 * allows us to say that if any backend takes a snapshot concurrently with
 * us then the conflict assessment made here would never include the snapshot
 * that is being derived. So we take LW_SHARED on the ProcArray and allow
 * concurrent snapshots when limitXmin is valid. We might
 * think about adding Assert(limitXmin < lowest(KnownAssignedXids))
 * but that would not be true in the case of FATAL errors lagging in array,
 * but we already know those are bogus anyway, so we skip that test.
 *
 * If dbOid is valid we skip backends attached to other databases.
 *
 * Be careful to *not* pfree the result from this function. We reuse
 * this array sufficiently often that we use malloc for the result.
 */
VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid, XLogRecPtr lsn,
                                                CommitSeqNo limitXminCSN, TransactionId* xminArray)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;
    int index;

    /*
     * If first time through, get workspace to remember main XIDs in. We
     * malloc it permanently to avoid repeated palloc/pfree overhead. Allow
     * result space, remembering room for a terminator.
     */
    if (t_thrd.storage_cxt.proc_vxids == NULL) {
        t_thrd.storage_cxt.proc_vxids = (VirtualTransactionId*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
            sizeof(VirtualTransactionId) * (unsigned int)(arrayP->maxProcs + 1));

        if (t_thrd.storage_cxt.proc_vxids == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        /* Exclude prepared transactions and Statement flush thread */
        if (proc->pid == 0 || (OidIsValid(dbOid) && proc->databaseId != dbOid) ||
            strcmp((const char*)(proc->myProgName), "Statement flush thread") == 0) {
            continue;
        }

#ifndef ENABLE_MULTIPLE_NODES
        /* Fetch xmin just once - can't change on us, but good coding */
        TransactionId pxmin = pgxact->xmin;

        /*
         * We ignore an invalid pxmin because this means that backend has
         * no snapshot and cannot get another one while we hold exclusive
         * lock.
         */
        if (!TransactionIdIsValid(limitXmin) ||
            (TransactionIdIsValid(pxmin) && !TransactionIdFollows(pxmin, limitXmin))) {
            VirtualTransactionId vxid;

            GET_VXID_FROM_PGPROC(vxid, *proc);

            if (VirtualTransactionIdIsValid(vxid)) {
                ADD_XMIN_TO_ARRAY(pxmin);
                t_thrd.storage_cxt.proc_vxids[count++] = vxid;
            }
        }
#else
        if (!IS_MULTI_DISASTER_RECOVER_MODE) {
            break;
        }
        CommitSeqNo xact_csn = pgxact->csn_dr;
        if (!TransactionIdIsValid(limitXmin) || (limitXminCSN >= xact_csn && xact_csn != InvalidCommitSeqNo)) {
            VirtualTransactionId vxid;
            GET_VXID_FROM_PGPROC(vxid, *proc);

            if (VirtualTransactionIdIsValid(vxid)) {
                t_thrd.storage_cxt.proc_vxids[count++] = vxid;
            }
        }
#endif
    }
#ifndef ENABLE_MULTIPLE_NODES
    UpdateCleanUpInfo(limitXmin, lsn);
#endif
    LWLockRelease(ProcArrayLock);

    /* add the terminator */
    t_thrd.storage_cxt.proc_vxids[count].backendId = InvalidBackendId;
    t_thrd.storage_cxt.proc_vxids[count].localTransactionId = InvalidLocalTransactionId;
    ADD_XMIN_TO_ARRAY(InvalidTransactionId);

    return t_thrd.storage_cxt.proc_vxids;
}

/*
 * CancelVirtualTransaction - used in recovery conflict processing
 *
 * Returns pid of the process signaled, or 0 if not found.
 */
ThreadId CancelVirtualTransaction(const VirtualTransactionId& vxid, ProcSignalReason sigmode)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;
    ThreadId pid = 0;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        VirtualTransactionId procvxid;

        GET_VXID_FROM_PGPROC(procvxid, *proc);

        if (procvxid.backendId == vxid.backendId && procvxid.localTransactionId == vxid.localTransactionId) {
            proc->recoveryConflictPending = true;
            pid = proc->pid;

            if (pid != 0) {
                /*
                 * Kill the pid if it's still here. If not, that's what we
                 * wanted so ignore any errors.
                 */
                (void)SendProcSignal(pid, sigmode, vxid.backendId);
            }

            break;
        }
    }

    LWLockRelease(ProcArrayLock);

    return pid;
}

bool proc_array_cancel_conflicting_proc(
    TransactionId latest_removed_xid, XLogRecPtr truncate_redo_lsn, bool reach_max_check_times)
{
    ProcArrayStruct* proc_array = g_instance.proc_array_idx;
    bool conflict = false;

    LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (int index = 0; index < proc_array->numProcs; index++) {
        int pg_proc_no = proc_array->pgprocnos[index];
        PGPROC* pg_proc = g_instance.proc_base_all_procs[pg_proc_no];
        PGXACT* pg_xact = &g_instance.proc_base_all_xacts[pg_proc_no];
        XLogRecPtr read_lsn = pg_proc->exrto_min;
        TransactionId pxmin = pg_xact->xmin;

        if (pg_proc->pid == 0 || XLogRecPtrIsInvalid(read_lsn)) {
            continue;
        }

        Assert(!(pg_xact->vacuumFlags & PROC_IN_VACUUM));
        /*
         * Backend is doing logical decoding which manages xmin
         * separately, check below.
         */
        if (pg_xact->vacuumFlags & PROC_IN_LOGICAL_DECODING) {
            continue;
        }

        /* cancel query when its xmin < latest_removed_xid */
        if (TransactionIdPrecedesOrEquals(pxmin, latest_removed_xid) ||
            (truncate_redo_lsn != InvalidXLogRecPtr && XLByteLT(read_lsn, truncate_redo_lsn))) {
            conflict = true;
            pg_proc->recoveryConflictPending = true;
            if (pg_proc->pid != 0) {
                /*
                 * Kill the pid if it's still here. If not, that's what we
                 * wanted so ignore any errors.
                 */
                (void)SendProcSignal(pg_proc->pid, PROCSIG_RECOVERY_CONFLICT_SNAPSHOT, pg_proc->backendId);
                /*
                 * Wait a little bit for it to die so that we avoid flooding
                 * an unresponsive backend when system is heavily loaded.
                 */
                ereport(LOG,
                    (errmsg(EXRTOFORMAT("cancel thread while "
                                        "redo truncate (lsn: %08X/%08X, latest_removed_xid: %lu), thread id = %lu, "
                                        "read_lsn: %08X/%08X, xmin: %lu"),
                        (uint32)(truncate_redo_lsn >> UINT64_HALF),
                        (uint32)truncate_redo_lsn,
                        latest_removed_xid,
                        pg_proc->pid,
                        (uint32)(read_lsn >> UINT64_HALF),
                        (uint32)read_lsn,
                        pxmin)));
                pg_usleep(5000L);
            }
        }
        if (reach_max_check_times) {
            ereport(WARNING, (
                errmsg("can not cancel thread while redo truncate, thread id = %lu", pg_proc->pid)));
        }
    }
    LWLockRelease(ProcArrayLock);

    return conflict;
}

/*
 * MinimumActiveBackends --- count backends (other than myself) that are
 *		in active transactions.  Return true if the count exceeds the
 *		minimum threshold passed.  This is used as a heuristic to decide if
 *		a pre-XLOG-flush delay is worthwhile during commit.
 *
 * Do not count backends that are blocked waiting for locks, since they are
 * not going to get to run until someone else commits.
 */
bool MinimumActiveBackends(int min)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;
    int index;

    /* Quick short-circuit if no minimum is specified */
    if (min == 0) {
        return true;
    }

    /*
     * Note: for speed, we don't acquire ProcArrayLock.  This is a little bit
     * bogus, but since we are only testing fields for zero or nonzero, it
     * should be OK.  The result is only used for heuristic purposes anyway...
     */
    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        /*
         * Since we're not holding a lock, need to check that the pointer is
         * valid. Someone holding the lock could have incremented numProcs
         * already, but not yet inserted a valid pointer to the array.
         *
         * If someone just decremented numProcs, 'proc' could also point to a
         * PGPROC entry that's no longer in the array. It still points to a
         * PGPROC struct, though, because freed PGPROC entries just go to the
         * free list and are recycled. Its contents are nonsense in that case,
         * but that's acceptable for this function.
         */
        if (pgprocno == -1) {
            continue; /* do not count deleted entries */
        }

        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        if (proc == t_thrd.proc) {
            continue; /* do not count myself */
        }

        if (pgxact->xid == InvalidTransactionId) {
            continue; /* do not count if no XID assigned */
        }

        if (proc->pid == 0) {
            continue; /* do not count prepared xacts */
        }

        if (proc->waitLock != NULL) {
            continue; /* do not count if blocked on a lock */
        }

        count++;

        if (count >= min) {
            break;
        }
    }

    return count >= min;
}

/*
 * CountDBBackends:
 * The purpose is to collect statistics on the number of current connections.
 *
 * 1.In case of thread pool mode, active and inactive threads are counted through interface CountDBSessions.
 * 2.In case of none thread pool mode, the number of threads to be prepared and the number of active backend threads
 *   need to be collected with the help of global variable g_instance.proc_base_all_procs.
 */
int CountDBBackends(Oid database_oid)
{
    const int MAXAUTOVACPIDS = 10; /* max autovacs to SIGTERM per iteration */
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index, pgprocno, num_connections;
    int num_autovacs = 0;
    int num_wdrxdbs = 0;
    int num_backends = 0;
    int num_prepared = 0;

    CHECK_FOR_INTERRUPTS();
    /* Under thread pool mode, active and inactive threads are counted. */
    if (ENABLE_THREAD_POOL) {
        num_connections = g_threadPoolControler->GetSessionCtrl()->CountDBSessions(database_oid);
    } else {
        LWLockAcquire(ProcArrayLock, LW_SHARED);

        for (index = 0; index < arrayP->numProcs; index++) {
            pgprocno = arrayP->pgprocnos[index];
            volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
            volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
            volatile PgBackendStatus* beentry = pgstat_get_backend_single_entry(proc->sessionid);

            if (proc->databaseId != database_oid) {
                continue;
            }

            if (proc->pid == 0) {
                num_prepared++;
            } else {
                /* Internal threads are not counted. Function: autovacuum */
                if ((pgxact->vacuumFlags & PROC_IS_AUTOVACUUM) && num_autovacs < MAXAUTOVACPIDS) {
                    num_autovacs++;
                    continue;
                }

                /* Internal threads are not counted. Function: cross-database query */
                if (beentry != NULL && strcmp(beentry->st_appname, "WDRXdb") == 0 &&
                    num_wdrxdbs < MAXAUTOVACPIDS) {
                    num_wdrxdbs++;
                    continue;
                }

                num_backends++;
            }
        }

        LWLockRelease(ProcArrayLock);
        num_connections = num_backends + num_prepared;
    }

    if (ENABLE_THREAD_POOL) {
        ereport(DEBUG5, (errmsg("count backend connections in threadpool mode, num_connections[%d].)",
                                num_connections)));
    } else {
        ereport(DEBUG5, (errmsg("count backend connections in none-threadpool mode, num_backends[%d], "
                                "num_prepared[%d], num_autovacs[%d], num_wdrxdbs[%d].)",
                                num_backends, num_prepared, num_autovacs, num_wdrxdbs)));
    }

    return num_connections;
}

/*
 * CountDBActiveBackends
 * The purpose is to count active backends that are using specified database which used for clearing links
 * in the redo scenario.
 */
int CountDBActiveBackends(Oid database_oid)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

        if (proc->pid == 0)
            continue; /* do not count prepared xacts */

        if (!OidIsValid(database_oid) || proc->databaseId == database_oid)
            count++;
    }

    LWLockRelease(ProcArrayLock);

    return count;
}

/*
 * CancelDBBackends --- cancel backends that are using specified database
 */
void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;
    ThreadId pid = 0;

    /* tell all backends to die */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

        if (databaseid == InvalidOid || proc->databaseId == databaseid) {
            VirtualTransactionId procvxid;

            GET_VXID_FROM_PGPROC(procvxid, *proc);

            proc->recoveryConflictPending = conflictPending;
            pid = proc->pid;

            if (pid != 0) {
                /*
                 * Kill the pid if it's still here. If not, that's what we
                 * wanted so ignore any errors.
                 */
                (void)SendProcSignal(pid, sigmode, procvxid.backendId);
            }
        }
    }

    LWLockRelease(ProcArrayLock);
}

static bool ValidDBoidAndUseroid(Oid databaseOid, Oid userOid, volatile PGPROC* proc)
{
    /*
     * Thread are 3 situation in CLEAN CONNECTION:
     * 1. Only database, for example: CLEAN CONNECTION TO ALL FORCE FOR DATABASE xxx;
     * 2. Only user, for example: CLEAN CONNECTION TO ALL FORCE TO USER xxx;
     * 3. Both database and user, for example: CLEAN CONNECTION TO ALL FORCE FOR DATABASE xxx TO USER xxx;
     */
    if (((databaseOid != InvalidOid) && (userOid == InvalidOid) && (proc->databaseId == databaseOid))
        || ((databaseOid == InvalidOid) && (userOid != InvalidOid) && (proc->roleId == userOid))
        || ((proc->databaseId == databaseOid) && (proc->roleId == userOid))) {
        return true;
    }
    return false;
}

int CountSingleNodeActiveBackends(Oid databaseOid, Oid userOid)
{
    if ((databaseOid == InvalidOid) && (userOid == InvalidOid)) {
        ereport(WARNING,
            (errmsg("DB oid and user oid are all Invalid (may be NULL). Shut down clean activite sessions.")));
        return 0;
    }
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int count = 0;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

        if (proc->pid == 0)
            continue; /* do not count prepared xacts */

        if (ValidDBoidAndUseroid(databaseOid, userOid, proc)) {
            count++;
        }
    }

    LWLockRelease(ProcArrayLock);

    return count;
}

/*
 * CancelSingleNodeBackends --- cancel backends in single node by database oid or user oid
 */
void CancelSingleNodeBackends(Oid databaseOid, Oid userOid, ProcSignalReason sigmode, bool conflictPending)
{
    if ((databaseOid == InvalidOid) && (userOid == InvalidOid)) {
        ereport(WARNING,
            (errmsg("DB oid and user oid are all Invalid (may be NULL). Shut down clean activite sessions.")));
        return;
    }
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;
    ThreadId pid = 0;

    /* tell all backends to die */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

        if (ValidDBoidAndUseroid(databaseOid, userOid, proc)) {
            VirtualTransactionId procvxid;

            GET_VXID_FROM_PGPROC(procvxid, *proc);

            proc->recoveryConflictPending = conflictPending;
            pid = proc->pid;

            if (pid != 0) {
                /*
                 * Kill the pid if it's still here. If not, that's what we
                 * wanted so ignore any errors.
                 */
                (void)SendProcSignal(pid, sigmode, procvxid.backendId);
            }
        }
    }

    LWLockRelease(ProcArrayLock);
}


LWLock *RoleidPartitionLock(uint32 hashCode)
{
    int id = FirstSessRoleIdLock + hashCode % NUM_SESSION_ROLEID_PARTITIONS;
    return GetMainLWLockByIndex(id);
}

/* Init RoleId HashTable */
void InitRoleIdHashTable()
{
    HASHCTL hctl;
    errno_t rc = 0;

    MemoryContext context = AllocSetContextCreate(g_instance.instance_context,
                                                  "RoleIdHashtblContext",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);

    rc = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(rc, "", "");

    hctl.keysize = sizeof(Oid);
    hctl.entrysize = sizeof(RoleIdHashEntry);
    hctl.hash = oid_hash;
    hctl.hcxt = context;
    hctl.num_partitions = NUM_SESSION_ROLEID_PARTITIONS;

    g_instance.roleid_cxt.roleid_table = HeapMemInitHash("Roleid map",
        INIT_ROLEID_HASHTBL,
        MAX_ROLEID_HASHTBL,
        &hctl,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);

}
/* get the RoleId Count. */
int GetRoleIdCount(Oid roleoid)
{
    bool found = false;
    uint32 hashCode = 0;
    volatile int64 roleNum = 0;
    RoleIdHashEntry *entry = NULL;

    hashCode = oid_hash(&roleoid, sizeof(Oid));
    LWLock *lock = RoleidPartitionLock(hashCode);

    (void)LWLockAcquire(lock, LW_SHARED);

    entry = (RoleIdHashEntry *)hash_search(g_instance.roleid_cxt.roleid_table, (void*)&roleoid, HASH_FIND, &found);

    if (!found) {
        roleNum = 0;
    } else {
        roleNum = entry->roleNum;
    }

    LWLockRelease(lock);

    return roleNum;
}

int IncreaseUserCount(Oid roleoid)
{
    bool found = false;
    uint32 hashCode = 0;
    volatile int64 roleNum = 0;
    RoleIdHashEntry *entry = NULL;

    if(roleoid == 0) {
        return 0;
    }

    hashCode = oid_hash(&roleoid, sizeof(Oid));
    LWLock *lock = RoleidPartitionLock(hashCode);

    (void)LWLockAcquire(lock, LW_EXCLUSIVE);

    entry = (RoleIdHashEntry *)hash_search(g_instance.roleid_cxt.roleid_table, (void*)&roleoid, HASH_ENTER, &found);

    if (!found) {
        entry->roleNum = 1;
    } else {
        entry->roleNum++;
    }

    roleNum = entry->roleNum;
    LWLockRelease(lock);
    return roleNum;
}

int DecreaseUserCount(Oid roleoid)
{
    bool found = false;
    uint32 hashCode = 0;
    volatile int64 roleNum = 0;
    RoleIdHashEntry *entry = NULL;

    if(roleoid == 0) {
        return 0;
    }
    hashCode = oid_hash(&roleoid, sizeof(Oid));
    LWLock *lock = RoleidPartitionLock(hashCode);

    (void)LWLockAcquire(lock, LW_EXCLUSIVE);

    entry = (RoleIdHashEntry *)hash_search(g_instance.roleid_cxt.roleid_table, (void*)&roleoid, HASH_FIND, &found);

    if (found) {
        entry->roleNum--;
        roleNum = entry->roleNum;
        if (entry->roleNum == 0) {
            (void)hash_search(g_instance.roleid_cxt.roleid_table, (void*)&roleoid, HASH_REMOVE, &found);
        }
    }

    LWLockRelease(lock);
    return roleNum;
}

/*
 * CountUserBackends --- count backends that are used by specified user
 */
int CountUserBackends(Oid roleid)
{
    
    int count = 0;
    if (!ENABLE_THREAD_POOL) {
        ProcArrayStruct* arrayP = g_instance.proc_array_idx;
        LWLockAcquire(ProcArrayLock, LW_SHARED);

        for (int index = 0; index < arrayP->numProcs; index++) {
            int pgprocno = arrayP->pgprocnos[index];
            volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

            if (proc->pid == 0)
                continue; /* do not count prepared xacts */

            if (proc->roleId == roleid && (t_thrd.role != STREAM_WORKER))
                count++;
        }

        LWLockRelease(ProcArrayLock);
    } else {
        count = GetRoleIdCount(roleid);
    }

    return count;
}

/*
 * CountOtherDBBackends -- check for other backends running in the given DB
 *
 * If there are other backends in the DB, we will wait a maximum of 5 seconds
 * for them to exit.  Autovacuum backends are encouraged to exit early by
 * sending them SIGTERM, but normal user backends are just waited for.
 *
 * The current backend is always ignored; it is caller's responsibility to
 * check whether the current backend uses the given DB, if it's important.
 *
 * Returns TRUE if there are (still) other backends in the DB, FALSE if not.
 * Also, *nbackends and *nprepared are set to the number of other backends
 * and prepared transactions in the DB, respectively.
 *
 * This function is used to interlock DROP DATABASE and related commands
 * against there being any active backends in the target DB --- dropping the
 * DB while active backends remain would be a Bad Thing.  Note that we cannot
 * detect here the possibility of a newly-started backend that is trying to
 * connect to the doomed database, so additional interlocking is needed during
 * backend startup.  The caller should normally hold an exclusive lock on the
 * target DB before calling this, which is one reason we mustn't wait
 * indefinitely.
 */
bool CountOtherDBBackends(Oid databaseId, int* nbackends, int* nprepared)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;

#define MAXAUTOVACPIDS 10 /* max autovacs to SIGTERM per iteration */
    ThreadId autovac_pids[MAXAUTOVACPIDS];
    ThreadId wdrxdb_pids[MAXAUTOVACPIDS];
    int tries;

    if (ENABLE_DMS && SS_PRIMARY_MODE) {
        bool ret = SSCheckDbBackendsFromAllStandby(databaseId);
        if (ret) {
            *nbackends = *nprepared = 0;
            return true;
        }
    }
    
    /* 50 tries with 100ms sleep between tries makes 5 sec total wait */
    for (tries = 0; tries < 50; tries++) {
        int nworkers = 0;
        int nautovacs = 0;
        int nwdrxdbs = 0;
        int index;

        CHECK_FOR_INTERRUPTS();

        *nbackends = *nprepared = 0;

        LWLockAcquire(ProcArrayLock, LW_SHARED);

        for (index = 0; index < arrayP->numProcs; index++) {
            int pgprocno = arrayP->pgprocnos[index];
            volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
            volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
            volatile PgBackendStatus* beentry = pgstat_get_backend_single_entry(proc->sessionid);

            if (proc->databaseId != databaseId)
                continue;

            if (proc == t_thrd.proc)
                continue;

            if (proc->pid == 0)
                (*nprepared)++;
            else {
                (*nbackends)++;

                if (ENABLE_THREAD_POOL && proc->sessionid > 0) {
                    nworkers++;
                }

                if ((pgxact->vacuumFlags & PROC_IS_AUTOVACUUM) && nautovacs < MAXAUTOVACPIDS) {
                    autovac_pids[nautovacs++] = proc->pid;
                }
                if (!ENABLE_THREAD_POOL && beentry != NULL && strcmp(beentry->st_appname, "WDRXdb") == 0 &&
                    nwdrxdbs < MAXAUTOVACPIDS) {
                    wdrxdb_pids[nwdrxdbs++] = proc->pid;
                    ereport(LOG, (errmsg("WDRXdb sessionid (beentry sessionid): %lu", beentry->st_sessionid)));
                    ereport(LOG, (errmsg("WDRXdb thread id (beentry st_tid): %d", beentry->st_tid)));
                }
            }
        }

        /* Under thread pool mode, we also need to count inactive sessions that are detached from worker threads */
        if (ENABLE_THREAD_POOL) {
            *nbackends -= nworkers;
            *nbackends += g_threadPoolControler->GetSessionCtrl()->CountDBSessions(databaseId);
        }

        LWLockRelease(ProcArrayLock);

        if (*nbackends == 0 && *nprepared == 0) {
            return false; /* no conflicting backends, so done */
        }

        /*
         * Send SIGTERM to any conflicting autovacuums before sleeping. We
         * postpone this step until after the loop because we don't want to
         * hold ProcArrayLock while issuing kill(). We have no idea what might
         * block kill() inside the kernel...
         */
        for (index = 0; index < nautovacs; index++) {
            gs_signal_send(autovac_pids[index], SIGTERM); /* ignore any error */
        }
        for (index = 0; index < nwdrxdbs; index++) {
            gs_signal_send(wdrxdb_pids[index], SIGTERM);
            gs_signal_send(wdrxdb_pids[index], SIGUSR2);
            ereport(LOG, (errmsg("WDRXdb thread pid: %lu is killed(proc->pid)", wdrxdb_pids[index])));
        }

        /* sleep, then try again */
        pg_usleep(100 * 1000L); /* 100ms */
    }

    return true; /* timed out, still conflicts */
}

#ifdef PGXC
/*
 * ReloadConnInfoOnBackends -- reload connection information for all the backends
 */
void ReloadConnInfoOnBackends(void)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int index;
    ThreadId pid = 0;

    /* tell all backends to reload except this one who already reloaded */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        VirtualTransactionId vxid;
        GET_VXID_FROM_PGPROC(vxid, *proc);

        if (proc == t_thrd.proc)
            continue; /* do not do that on myself */

        if (proc->pid == 0)
            continue; /* useless on prepared xacts */
        
        if (pgxact->vacuumFlags & PROC_IN_VACUUM)
            continue; /* ignore vacuum processes */
        
        if (EnableGlobalSysCache()) {
            /* syscache is on thread in gsc mode. when enable thread pool,
             * even the thread does not connect to a database, we still need send signal to it */
            if (!OidIsValid(proc->databaseId) && !ENABLE_THREAD_POOL) {
                continue;
            }
        } else {
            if (!OidIsValid(proc->databaseId)) {
                continue;
            }
            if (ENABLE_THREAD_POOL && proc->sessionid > 0) {
                continue;
            }
        }

        pid = proc->pid;
        /*
         * Send the reload signal if backend still exists
         */
        (void)SendProcSignal(pid, PROCSIG_PGXCPOOL_RELOAD, vxid.backendId);
    }

    LWLockRelease(ProcArrayLock);

    if (ENABLE_THREAD_POOL) {
        g_threadPoolControler->GetSessionCtrl()->HandlePoolerReload();
    }
}
#endif

char dump_memory_context_name[MEMORY_CONTEXT_NAME_LEN];

/*
 * DumpMemoryCtxOnBackend -- dump memory context on some backend
 */
void DumpMemoryCtxOnBackend(ThreadId tid, const char* mem_ctx)
{
    int ret;
    errno_t ss_rc = EOK;

    if (strlen(mem_ctx) >= MEMORY_CONTEXT_NAME_LEN) {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("The name of memory context is too long(>=%dbytes)", MEMORY_CONTEXT_NAME_LEN)));
        return;
    }

    ss_rc = memset_s(dump_memory_context_name, MEMORY_CONTEXT_NAME_LEN, 0, MEMORY_CONTEXT_NAME_LEN);
    securec_check(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(dump_memory_context_name, MEMORY_CONTEXT_NAME_LEN, mem_ctx);
    securec_check(ss_rc, "\0", "\0");

    LWLockAcquire(ProcArrayLock, LW_SHARED);
    ret = SendProcSignal(tid, PROCSIG_MEMORYCONTEXT_DUMP, InvalidBackendId);
    LWLockRelease(ProcArrayLock);
    if (ret)
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("Fail to send signal to backend(tid:%lu).", (unsigned long)tid)));
}

/*
 * ProcArraySetReplicationSlotXmin
 *
 * Install limits to future computations of the xmin horizon to prevent vacuum
 * and HOT pruning from removing affected rows still needed by clients with
 * replicaton slots.
 */
void ProcArraySetReplicationSlotXmin(TransactionId xmin, TransactionId catalog_xmin, bool already_locked)
{
    Assert(!already_locked || LWLockHeldByMe(ProcArrayLock));

    if (!already_locked)
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    if (xmin == InvalidTransactionId || TransactionIdPrecedes(g_instance.proc_array_idx->replication_slot_xmin, xmin)) {
        g_instance.proc_array_idx->replication_slot_xmin = xmin;
    }
    if (catalog_xmin == InvalidTransactionId ||
        TransactionIdPrecedes(g_instance.proc_array_idx->replication_slot_catalog_xmin, catalog_xmin)) {
        g_instance.proc_array_idx->replication_slot_catalog_xmin = catalog_xmin;
    }

    if (!already_locked)
        LWLockRelease(ProcArrayLock);
}

/*
 * GetReplicationSlotCatalogXmin
 * 
 * Return replication_slot_catalog_xmin.
 */
TransactionId GetReplicationSlotCatalogXmin() {
    return g_instance.proc_array_idx->replication_slot_catalog_xmin;
}

/*
 * ProcArrayGetReplicationSlotXmin
 *
 * Return the current slot xmin limits. That's useful to be able to remove
 * data that's older than those limits.
 */
void ProcArrayGetReplicationSlotXmin(TransactionId* xmin, TransactionId* catalog_xmin)
{
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    if (xmin != NULL)
        *xmin = g_instance.proc_array_idx->replication_slot_xmin;

    if (catalog_xmin != NULL)
        *catalog_xmin = g_instance.proc_array_idx->replication_slot_catalog_xmin;

    LWLockRelease(ProcArrayLock);
}

/*
 * XidCacheRemoveRunningXids
 *
 * Remove a bunch of TransactionIds from the list of known-running
 * subtransactions for my backend.	Both the specified xid and those in
 * the xids[] array (of length nxids) are removed from the subxids cache.
 * latestXid must be the latest XID among the group. We should store the
 * required parameters into proc before performing XidCacheRemoveRunningXids,
 * including subtransaction xid, the number of committed subtransaction, 
 * committed substransaction list, the latestXid between its xid and its
 * committed subtransactions'.
 * 
 * We don't do any locking here; caller must get the procArrayLock before
 * perform XidCacheRemoveRunningXids.
 */
void XidCacheRemoveRunningXids(PGPROC* proc, PGXACT* pgxact)
{
    int i, j;
    TransactionId xid = proc->procArrayGroupMemberXid;
    int nxids = proc->procArrayGroupSubXactNXids;
    TransactionId* xids = proc->procArrayGroupSubXactXids;
    TransactionId latestXid = proc->procArrayGroupSubXactLatestXid;

    Assert(TransactionIdIsValid(xid));

    /*
     * Under normal circumstances xid and xids[] will be in increasing order,
     * as will be the entries in subxids.  Scan backwards to avoid O(N^2)
     * behavior when removing a lot of xids.
     */
    for (i = nxids - 1; i >= 0; i--) {
        TransactionId anxid = xids[i];

        for (j = pgxact->nxids - 1; j >= 0; j--) {
            if (TransactionIdEquals(proc->subxids.xids[j], anxid)) {
                proc->subxids.xids[j] = proc->subxids.xids[pgxact->nxids - 1];
                pgxact->nxids--;
                break;
            }
        }

        /*
         * Ordinarily we should have found it, unless the cache has
         * overflowed. However it's also possible for this routine to be
         * invoked multiple times for the same subtransaction, in case of an
         * error during AbortSubTransaction.  So instead of Assert, emit a
         * debug warning.
         */
        if (j < 0)
            ereport(WARNING, (errmsg("did not find subXID " XID_FMT " in t_thrd.proc", anxid)));
    }

    for (j = pgxact->nxids - 1; j >= 0; j--) {
        if (TransactionIdEquals(proc->subxids.xids[j], xid)) {
            proc->subxids.xids[j] = proc->subxids.xids[pgxact->nxids - 1];
            pgxact->nxids--;
            break;
        }
    }

    /* Ordinarily we should have found it, unless the cache has overflowed */
    if (j < 0)
        ereport(WARNING, (errmsg("did not find subXID " XID_FMT " in t_thrd.proc", xid)));

    /* Also advance global latestCompletedXid while holding the lock */
    if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid, latestXid))
        t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid = latestXid;

}

#ifdef XIDCACHE_DEBUG

/*
 * Print stats about effectiveness of XID cache
 */
static void DisplayXidCache(void)
{
    fprintf(stderr,
            "XidCache: xmin: %ld, known: %ld, myxact: %ld, latest: %ld, mainxid: %ld, childxid: %ld, knownassigned: %ld, "
            "nooflo: %ld, slow: %ld\n",
            xc_by_recent_xmin,
            xc_by_known_xact,
            xc_by_my_xact,
            xc_by_latest_xid,
            xc_by_main_xid,
            xc_by_child_xid,
            xc_by_known_assigned,
            xc_no_overflow,
            xc_slow_answer);
}
#endif /* XIDCACHE_DEBUG */

#ifdef PGXC
/*
 * Store snapshot data received from the Coordinator
 */
void SetGlobalSnapshotData(
    TransactionId xmin, TransactionId xmax, uint64 csn, GTM_Timeline timeline, bool ss_need_sync_wait_all)
{
    u_sess->utils_cxt.snapshot_source = SNAPSHOT_COORDINATOR;
    u_sess->utils_cxt.g_GTM_Snapshot->sn_xmin = u_sess->utils_cxt.gxmin = xmin;
    u_sess->utils_cxt.g_GTM_Snapshot->sn_xmax = u_sess->utils_cxt.gxmax = xmax;
    u_sess->utils_cxt.g_GTM_Snapshot->sn_recent_global_xmin = u_sess->utils_cxt.RecentGlobalXmin;
    u_sess->utils_cxt.g_GTM_Snapshot->csn = u_sess->utils_cxt.g_snapshotcsn = csn;
    u_sess->utils_cxt.GtmTimeline = timeline;
    u_sess->utils_cxt.snapshot_need_sync_wait_all = ss_need_sync_wait_all;

    if (module_logging_is_on(MOD_TRANS_SNAPSHOT)) {
        ereport(LOG,
                (errmodule(MOD_TRANS_SNAPSHOT),
                 errmsg("global snapshot info from CN: gxmin: " XID_FMT ", gxmax: " XID_FMT ", gscn: %lu,"
                        "RecentGlobalXmin: %lu, cn_xc_maintain_mode: %s.",
                        u_sess->utils_cxt.gxmin,
                        u_sess->utils_cxt.gxmax,
                        u_sess->utils_cxt.g_snapshotcsn,
                        u_sess->utils_cxt.RecentGlobalXmin,
                        u_sess->utils_cxt.cn_xc_maintain_mode ? "on" : "off")));
    }
}

/*
 * Store snapshot data received from the Coordinator
 */
void SetGlobalSnapshotDataNode(TransactionId xmin, TransactionId xmax, uint64 csn, GTM_Timeline timeline)
{
    u_sess->utils_cxt.snapshot_source = SNAPSHOT_DATANODE;
    u_sess->utils_cxt.gxmin = xmin;
    u_sess->utils_cxt.gxmax = xmax;
    u_sess->utils_cxt.g_snapshotcsn = csn;
    u_sess->utils_cxt.GtmTimeline = timeline;

    ereport(DEBUG1,
            (errmsg("global snapshot info: gxmin: " XID_FMT ", gxmax: " XID_FMT ", gscn: %lu",
                    u_sess->utils_cxt.gxmin,
                    u_sess->utils_cxt.gxmax,
                    u_sess->utils_cxt.g_snapshotcsn)));
}

/*
 * Force Datanode to use local snapshot data
 */
void UnsetGlobalSnapshotData(void)
{
    u_sess->utils_cxt.snapshot_source = SNAPSHOT_UNDEFINED;
    u_sess->utils_cxt.gxmin = InvalidTransactionId;
    u_sess->utils_cxt.gxmax = InvalidTransactionId;
    u_sess->utils_cxt.g_snapshotcsn = 0;
    u_sess->utils_cxt.GtmTimeline = InvalidTransactionTimeline;
    u_sess->utils_cxt.is_autovacuum_snapshot = false;

    ereport(DEBUG1, (errmsg("unset snapshot info")));
}

/*
 * Entry of snapshot obtention for openGauss node
 * returns information about running transactions.
 * The returned snapshot includes xmin (lowest still-running xact ID),
 * xmax (highest completed xact ID + 1), and a list of running xact IDs
 * in the range xmin <= xid < xmax.  It is used as follows:
 *		All xact IDs < xmin are considered finished.
 *		All xact IDs >= xmax are considered still running.
 *		For an xact ID xmin <= xid < xmax, consult list to see whether
 *		it is considered running or not.
 * This ensures that the set of transactions seen as "running" by the
 * current xact will not change after it takes the snapshot.
 *
 * We also update the following backend-global variables:
 *		TransactionXmin: the oldest xmin of any snapshot in use in the
 *			current transaction (this is the same as MyPgXact->xmin).
 *		RecentXmin: the xmin computed for the most recent snapshot.  XIDs
 *			older than this are known not running any more.
 *		RecentGlobalXmin: the global xmin (oldest TransactionXmin across all
 *			running transactions).  This is
 *			the same computation done by GetOldestXmin(true, true).
 */
static bool GetPGXCSnapshotData(Snapshot snapshot)
{
#ifdef ENABLE_MULTIPLE_NODES
    /*
     * If this node is in recovery phase,
     * snapshot has to be taken directly from WAL information.
     */
    if (!IS_MULTI_DISASTER_RECOVER_MODE && RecoveryInProgress())
        return false;

    /*
     * The typical case is that the local Coordinator passes down the snapshot to the
     * remote nodes to use, while it itself obtains it from GTM. Autovacuum processes
     * need however to connect directly to GTM themselves to obtain XID and snapshot
     * information for autovacuum worker threads.
     * A vacuum analyze uses a special function to get a transaction ID and signal
     * GTM not to include this transaction ID in snapshot.
     * A vacuum worker starts as a normal transaction would.
     */
    if ((IS_PGXC_DATANODE || IsConnFromCoord() || IsAutoVacuumWorkerProcess() || GetForceXidFromGTM()) &&
        IsNormalProcessingMode()) {
        if (GetSnapshotDataDataNode(snapshot))
            return true;

        /* else fallthrough */
    } else if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && IsNormalProcessingMode()) {
        /* Snapshot has ever been received from remote Coordinator */
        if (GetSnapshotDataCoordinator(snapshot))
            return true;

        /* else fallthrough */
    }

    /*
     * If we have no snapshot, we will use a local one.
     * If we are in normal mode, we output a warning though.
     * We currently fallback and use a local one at initdb time,
     * as well as when a new connection occurs.
     * This is also the case for autovacuum launcher.
     *
     * IsPostmasterEnvironment - checks for initdb
     * IsNormalProcessingMode() - checks for new connections
     * IsAutoVacuumLauncherProcess - checks for autovacuum launcher process
     */
    if (IS_PGXC_DATANODE && !isRestoreMode && u_sess->utils_cxt.snapshot_source == SNAPSHOT_UNDEFINED &&
        IsPostmasterEnvironment && IsNormalProcessingMode() && !IsAutoVacuumLauncherProcess()) {
        if (!t_thrd.postgres_cxt.isInResetUserName)
            ereport(WARNING, (errmsg("Do not have a GTM snapshot available")));
    }

    return false;
#else
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
#endif /* ENABLE_MULTIPLE_NODES */
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Get snapshot data for Datanode
 * This is usually passed down from the Coordinator
 *
 * returns whether or not to return immediately with snapshot
 */
static bool GetSnapshotDataDataNode(Snapshot snapshot)
{
    Assert(IS_PGXC_DATANODE || IsConnFromCoord() || IsAutoVacuumWorkerProcess() || GetForceXidFromGTM());

    /*
     * Fallback to general case if Datanode is accessed directly by an application
     */
    if (IsPGXCNodeXactDatanodeDirect())
        return GetSnapshotDataCoordinator(snapshot);

    if (IsAutoVacuumWorkerProcess() || GetForceXidFromGTM()) {
        GTM_Snapshot gtm_snapshot;
        ereport(DEBUG1,
                (errmsg("Getting snapshot for autovacuum. Current XID = " XID_FMT, GetCurrentTransactionIdIfAny())));
        gtm_snapshot = IS_MULTI_DISASTER_RECOVER_MODE ? GetSnapshotGTMDR() : GetSnapshotGTMLite();

        if (!gtm_snapshot) {
            if (g_instance.status > NoShutdown) {
                if (module_logging_is_on(MOD_TRANS_SNAPSHOT)) {
                    ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT), errmsg("Shut down, could not obtain snapshot")));
                }
                return false;
            } else {
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("GTM error, could not obtain snapshot")));
            }
        } else {
            *u_sess->utils_cxt.g_GTM_Snapshot = *gtm_snapshot;
            u_sess->utils_cxt.snapshot_source = SNAPSHOT_DIRECT;
            snapshot->gtm_snapshot_type = IsAutoVacuumWorkerProcess() ? GTM_SNAPSHOT_TYPE_AUTOVACUUM : GTM_SNAPSHOT_TYPE_GLOBAL;
            /* only use gtm csn */
            Snapshot ret;
            ret = GetLocalSnapshotData(snapshot);
            Assert(ret != NULL);

            snapshot->snapshotcsn = set_proc_csn_and_check("GetSnapshotDataDataNodeDirectGTM",
                gtm_snapshot->csn, snapshot->gtm_snapshot_type, SNAPSHOT_DIRECT);
            u_sess->utils_cxt.g_GTM_Snapshot->csn = snapshot->snapshotcsn;
            u_sess->utils_cxt.RecentGlobalXmin = GetOldestXmin(NULL, true);
            u_sess->utils_cxt.RecentGlobalCatalogXmin = GetOldestCatalogXmin();
            return true;
        }
    }

    if (GTM_LITE_MODE && u_sess->utils_cxt.snapshot_source == SNAPSHOT_COORDINATOR) {
        TransactionId save_recentglobalxmin = u_sess->utils_cxt.RecentGlobalXmin;
        snapshot->gtm_snapshot_type =
            u_sess->utils_cxt.is_autovacuum_snapshot ? GTM_SNAPSHOT_TYPE_AUTOVACUUM : GTM_SNAPSHOT_TYPE_GLOBAL;
        if (IS_MULTI_DISASTER_RECOVER_MODE) {
            snapshot->snapshotcsn = u_sess->utils_cxt.g_snapshotcsn;
            t_thrd.pgxact->csn_dr = snapshot->snapshotcsn;
            pg_memory_barrier();
            CommitSeqNo lastReplayedConflictCSN = (CommitSeqNo)pg_atomic_read_u64(
                &(g_instance.comm_cxt.predo_cxt.last_replayed_conflict_csn));
            if (lastReplayedConflictCSN != 0 && snapshot->snapshotcsn - 1 <= lastReplayedConflictCSN) {
                ereport(ERROR, (errmsg("gtm csn small: gtm csn %lu, lastReplayedConflictCSN %lu",
                    snapshot->snapshotcsn, lastReplayedConflictCSN)));
            }
            LWLockAcquire(XLogMaxCSNLock, LW_SHARED);
            if (t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN + 1 < snapshot->snapshotcsn) {
                ereport(ERROR, (errmsg("dn data invisible: local csn %lu, gtm snapshotcsn %lu",
                    t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN, snapshot->snapshotcsn)));
            }
            LWLockRelease(XLogMaxCSNLock);
        } else {
            /* only use gtm csn */
            Snapshot ret;
            ret = GetLocalSnapshotData(snapshot);
            Assert(ret != NULL);
            snapshot->snapshotcsn = u_sess->utils_cxt.g_snapshotcsn;
            (void)set_proc_csn_and_check("GetSnapshotDataDataNodeFromCN", snapshot->snapshotcsn,
                                         snapshot->gtm_snapshot_type, SNAPSHOT_COORDINATOR);
            /* reset RecentGlobalXmin */
            u_sess->utils_cxt.RecentGlobalXmin = save_recentglobalxmin;
            /* too late to check and set */
        }
        return true;
    }

    return false;
}

/*
 * Get snapshot data for Coordinator
 * It will later be passed down to Datanodes
 *
 * returns whether or not to return immediately with snapshot
 */
static bool GetSnapshotDataCoordinator(Snapshot snapshot)
{
    GTM_Snapshot gtm_snapshot;

    Assert(IS_PGXC_COORDINATOR || IsPGXCNodeXactDatanodeDirect());

    /* Log some information about snapshot obtention */
    if (IsAutoVacuumWorkerProcess()) {
        ereport(DEBUG1,
                (errmsg("Getting snapshot for autovacuum. Current XID = " XID_FMT, GetCurrentTransactionIdIfAny())));
    } else {
        ereport(DEBUG1, (errmsg("Getting snapshot. Current XID = " XID_FMT, GetCurrentTransactionIdIfAny())));
    }

    gtm_snapshot = IS_MULTI_DISASTER_RECOVER_MODE ? GetSnapshotGTMDR() : GetSnapshotGTMLite();

    if (!gtm_snapshot) {
        if (g_instance.status > NoShutdown) {
            return false;
        } else {
            /* error level degrade when in AbortTransaction procedure */
            ereport(t_thrd.xact_cxt.bInAbortTransaction ? WARNING : ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("GTM error, could not obtain snapshot XID = " XID_FMT, GetCurrentTransactionIdIfAny())));
        }
    } else {
        snapshot->gtm_snapshot_type = GTM_SNAPSHOT_TYPE_GLOBAL;
        *u_sess->utils_cxt.g_GTM_Snapshot = *gtm_snapshot;
        if (IS_MULTI_DISASTER_RECOVER_MODE) {
            snapshot->snapshotcsn = gtm_snapshot->csn;
            t_thrd.pgxact->csn_dr = snapshot->snapshotcsn;
            LWLockAcquire(XLogMaxCSNLock, LW_SHARED);
            if (t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN + 1 < snapshot->snapshotcsn) {
                ereport(ERROR, (errmsg("cn data invisible: local csn %lu, gtm snapshotcsn %lu", t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN, snapshot->snapshotcsn)));
            }
            LWLockRelease(XLogMaxCSNLock);
        } else {
            /* only use gtm csn */
            Snapshot ret;
            ret = GetLocalSnapshotData(snapshot);
            Assert(ret != NULL);

            snapshot->snapshotcsn = set_proc_csn_and_check("GetSnapshotDataCoordinator", gtm_snapshot->csn,
                                                           snapshot->gtm_snapshot_type, SNAPSHOT_DIRECT);

            u_sess->utils_cxt.g_GTM_Snapshot->csn = snapshot->snapshotcsn;
            u_sess->utils_cxt.RecentGlobalXmin = GetOldestXmin(NULL, true);
            u_sess->utils_cxt.RecentGlobalCatalogXmin = GetOldestCatalogXmin();
        }
        if (module_logging_is_on(MOD_TRANS_SNAPSHOT)) {
            ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT),
                          errmsg("CN gets snapshot from gtm_snapshot, csn = %lu.", snapshot->snapshotcsn)));
        }
        return true;
    }

    return false;
}

void proc_cancel_invalid_gtm_lite_conn()
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int i;
    GtmHostIndex hostindex = GTM_HOST_INVAILD;
    GtmHostIndex my_gtmhost = InitGTM(false);

    ereport(LOG, (errmsg("GTMLite: canceling stale GTM connections, new GTM host index: %d.", my_gtmhost)));

    Assert(my_gtmhost == t_thrd.proc->my_gtmhost);

    LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (i = 0; i < arrayP->numProcs; i++) {
        int pgprocno = arrayP->pgprocnos[i];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];

        if (proc == NULL || (proc->pid == 0)) {
            continue;
        }

        Assert(proc->myProgName != NULL);

        /* skip non-postgres threads */
        if (strcmp((const char*)(proc->myProgName), "postgres") != 0) {
            continue;
        }

        hostindex = (GtmHostIndex)pg_atomic_fetch_add_u32((volatile uint32*)&proc->my_gtmhost, 0);

        ereport(DEBUG1, (errmsg("current GTM hostindex %d, thread id: %lu, thread GTM hostindex: %d", my_gtmhost,
            proc->pid, hostindex)));

        if (hostindex == GTM_HOST_INVAILD) {
            continue;
        }

        if (my_gtmhost != hostindex) {
            (void)pg_atomic_exchange_u32(&proc->signal_cancel_gtm_conn_flag, HOST2FLAG(my_gtmhost));
            if (gs_signal_send(proc->pid, SIGUSR2)) {
                ereport(WARNING, (errmsg("GTMLite: could not send signal to thread %lu: %m", proc->pid)));
                (void)pg_atomic_exchange_u32(&proc->signal_cancel_gtm_conn_flag, 0);
            } else {
                ereport(LOG, (errmsg("GTMLite: success to send SIGUSR2 to openGauss thread: %lu.", proc->pid)));
            }
        }
    }
    LWLockRelease(ProcArrayLock);
}

#endif /* ENABLE_MULTIPLE_NODES */

/* Cleanup the snapshot */
static void cleanSnapshot(Snapshot snapshot)
{
    snapshot->snapshotcsn = 0;
    snapshot->xmin = snapshot->xmax = InvalidTransactionId;
    snapshot->timeline = InvalidTransactionTimeline;
}

#endif /* PGXC */

TransactionId GetGlobal2pcXmin()
{
    TransactionId golabl_2pc_xmin = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    int ii = 0;
    int* pgprocnos = arrayP->pgprocnos;
    int numProcs;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    numProcs = arrayP->numProcs;

    for (ii = 0; ii < numProcs; ii++) {
        int pgprocno = pgprocnos[ii];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        TransactionId xid = pgxact->xid;
        TransactionId prepare_xid = pgxact->prepare_xid;

        if (proc->pid == 0)
            continue; /* ignore prepared transactions */

        ereport(DEBUG5, (errmsg("Active transaction: xid: " XID_FMT " ,prepare_xid: " XID_FMT, xid, prepare_xid)));

        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, golabl_2pc_xmin)) {
            golabl_2pc_xmin = xid;
        }
        if (TransactionIdIsNormal(prepare_xid) && TransactionIdPrecedes(prepare_xid, golabl_2pc_xmin)) {
            golabl_2pc_xmin = prepare_xid;
        }
    }

    LWLockRelease(ProcArrayLock);

    return golabl_2pc_xmin;
}

/*
 * Wait for the transaction which modify the tuple to finish.
 * First release the buffer lock. After waiting, re-acquire the buffer lock.
 */
void SyncWaitXidEnd(TransactionId xid, Buffer buffer, const Snapshot snapshot)
{
    if (!BufferIsValid(buffer)) {
        /* Wait local transaction finish */
        SyncLocalXidWait(xid, snapshot);
        return;
    }

    BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
    LWLockMode mode = GetHeldLWLockMode(bufHdr->content_lock);

    Assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    /* Release buffer lock */
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    /* Wait local transaction finish */
    SyncLocalXidWait(xid, snapshot);
    /* Re-acqure buffer lock, need transform lwlock mode to buffer lock mode */
    LockBuffer(buffer, mode == LW_EXCLUSIVE ? BUFFER_LOCK_EXCLUSIVE : BUFFER_LOCK_SHARE);
}


/*
 * Wait local transaction finish, if transaction wait time exceed transaction_sync_naptime, call gs_clean.
 */
void SyncLocalXidWait(TransactionId xid, const Snapshot snapshot)
{
    ReleaseAllGSCRdConcurrentLock();
    
    int64 remainingNapTime = (int64)u_sess->attr.attr_common.transaction_sync_naptime * 1000000; /* us */
    int64 remainingTimeout = (int64)u_sess->attr.attr_common.transaction_sync_timeout * 1000000; /* us */
    const int64 sleepTime = 1000;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    gstrace_entry(GS_TRC_ID_SyncLocalXidWait);
    while (!ConditionalXactLockTableWait(xid, snapshot)) {
        /* type of transaction id is same as node id, reuse the second param for waited transaction id */
        pgstat_report_waitstatus_xid(STATE_WAIT_XACTSYNC, xid);

        if (u_sess->attr.attr_common.transaction_sync_naptime && remainingNapTime <= 0 && twoPhaseCleanerProc) {
            ereport(LOG,
                    (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                     errmsg("wait transaction sync time would exceed %d s, "
                            "call gs_clean to clean reserved prepared transactions.",
                            u_sess->attr.attr_common.transaction_sync_naptime)));
            CHECK_FOR_INTERRUPTS();
            /* call gs_clean */
            bSyncXactsCallGsclean = true;
            SetLatch(&twoPhaseCleanerProc->procLatch);
            /* sleep 0.1s, wait gs_clean process */
            pg_usleep(100 * sleepTime);
            remainingNapTime = (int64)u_sess->attr.attr_common.transaction_sync_naptime * 1000000; /* us */
        }

        if (u_sess->attr.attr_common.transaction_sync_timeout && remainingTimeout <= 0) {
            (void)pgstat_report_waitstatus(oldStatus);
            ereport(ERROR,
                    (errcode(ERRCODE_LOCK_WAIT_TIMEOUT),
                     errmsg("wait transaction %lu sync time exceed %d s.",
                            xid,
                            u_sess->attr.attr_common.transaction_sync_timeout)));
        }

        if (g_instance.status > NoShutdown || g_instance.demotion > NoDemote) {
            ereport(FATAL,
                    (errcode(ERRCODE_ADMIN_SHUTDOWN),
                     errmsg("terminating SyncLocalXactsWithGTM process due to administrator command")));
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(sleepTime); /* 1ms */
        remainingNapTime = remainingNapTime - sleepTime;
        remainingTimeout = remainingTimeout - sleepTime;
    }
    (void)pgstat_report_waitstatus(oldStatus);
    gstrace_exit(GS_TRC_ID_SyncLocalXidWait);
}

void PrintCurrentSnapshotInfo(int logelevel, TransactionId xid, Snapshot snapshot, const char* action)
{
    if (snapshot) {
        StringInfoData snapshot_str;
        initStringInfo(&snapshot_str);

        appendStringInfo(&snapshot_str,
                         "snapshot xmin: %lu, xmax: %lu, csn: %lu, "
                         "recentGlobalXmin: %lu",
                         snapshot->xmin,
                         snapshot->xmax,
                         snapshot->snapshotcsn,
                         pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin));

        ereport(logelevel,
                (errmsg("[%s] xtuplexid= %lu, [MVCCSanpshot] %s", action ? action : "no aciton", xid, snapshot_str.data)));

        pfree(snapshot_str.data);
        snapshot_str.data = NULL;
    } else
        ereport(logelevel, (errmsg("[%s] tuplexid = %lu", action ? action : "no aciton", xid)));
}

/*
 * cache line size in bytes
 */
#define CACHE_LINE_SZ 64

/*
 * partition reference count to groups of threads to reduce contention
 */
#define NREFCNT 1

/*
 * atomic increment
 */
#define atomic_inc(ptr) __sync_add_and_fetch(ptr, 1)

/*
 * atomic decrement
 */
#define atomic_dec(ptr) __sync_sub_and_fetch(ptr, 1)

/*
 * cache-line aligned reference counter
 */
typedef struct _ref_cnt {
    unsigned count;
    unsigned pad[CACHE_LINE_SZ / sizeof(unsigned) - sizeof(unsigned)];
} ref_cnt_t;


/* snapxid structure to hold the values computed at a commit time */
#ifdef __aarch64__

/* the offset of ref_cnt in the struct _snapxid. */
#define REF_CNT_OFFSET 36

typedef struct _snapxid {
    TransactionId xmin;
    TransactionId xmax;
    CommitSeqNo snapshotcsn;
    TransactionId localxmin; /* the latest xmin in local node, update at transaction end. */
    bool takenDuringRecovery;
    char padding[PG_CACHE_LINE_SIZE - REF_CNT_OFFSET];
} snapxid_t;

#else

typedef struct _snapxid {
    TransactionId xmin;
    TransactionId xmax;
    CommitSeqNo snapshotcsn;
    TransactionId localxmin; /* the latest xmin in local node, update at transaction end. */
    bool takenDuringRecovery;
    ref_cnt_t ref_cnt[NREFCNT];
} snapxid_t;

#endif

/*
 * the snapshot ring buffer
 */
static snapxid_t* g_snap_buffer = NULL;       /* the ring buffer for snapxids */
static snapxid_t* g_snap_buffer_copy = NULL;  /* the ring buffer for AtProcExit */
static size_t g_bufsz = 0;
static bool g_snap_assigned = false;  /* true if current snap valid */

#define SNAP_SZ sizeof(snapxid_t)  /* size of snapxid_t */
#define MaxNumSnapVersion 64       /* max version number */

/*
 * get pointer to snapxid_t entry in specified index in ring buffer
 */
static inline snapxid_t* SNAPXID_AT(size_t i)
{
    return (snapxid_t*)(((char*)g_snap_buffer) + SNAP_SZ * i);
}

/*
 * get offset in bytes of snapxid_t entry in ring buffer
 */
static inline size_t SNAPXID_OFFSET(snapxid_t* x)
{
    return (((char*)x) - ((char*)g_snap_buffer));
}

/*
 * get index of snapxid_t entry in ring buffer
 */
static inline size_t SNAPXID_INDEX(snapxid_t* x)
{
    return (SNAPXID_OFFSET(x) / SNAP_SZ);
}

/*
 * points to most recently computed snapshot
 */
static volatile snapxid_t* g_snap_current = NULL;

/*
 * points to next available slot in snapshot ring buffer
 */
static volatile snapxid_t* g_snap_next = NULL;

/*
 * Report shared-memory space needed by CreateSharedRingBuffer.
 */
Size RingBufferShmemSize(void)
{
#ifdef __aarch64__
    return mul_size(MaxNumSnapVersion, SNAP_SZ) + PG_CACHE_LINE_SIZE;
#else
    return mul_size(MaxNumSnapVersion, SNAP_SZ);
#endif
}

/*
 * Initialize the shared Snapshot Ring Buffer during postmaster startup.
 */
void CreateSharedRingBuffer(void)
{
    bool found = false;

#ifdef __aarch64__
    /* Create or attach to the ProcArray shared structure. */
    g_snap_buffer = (snapxid_t*)CACHELINEALIGN(ShmemInitStruct("Snapshot Ring Buffer", RingBufferShmemSize(), &found));
#else
    /* Create or attach to the ProcArray shared structure. */
    g_snap_buffer = (snapxid_t*)ShmemInitStruct("Snapshot Ring Buffer", RingBufferShmemSize(), &found);
#endif

    if (!found) {
        /* Initialize if we're the first. */
        g_bufsz = MaxNumSnapVersion;
        g_snap_current = SNAPXID_AT(0);
        g_snap_next = SNAPXID_AT(1);
        g_snap_buffer_copy = g_snap_buffer;
        errno_t rc = memset_s(g_snap_buffer, RingBufferShmemSize(), 0, RingBufferShmemSize());
        securec_check(rc, "\0", "\0");
    }
}

#ifdef __aarch64__

/*
 * increment reference count of snapshot
 */
static void IncrRefCount(snapxid_t* s)
{
    t_thrd.proc->snap_refcnt_bitmap |= 1 << (SNAPXID_INDEX(s) % 64);
    pg_write_barrier();
}

/*
 * decrement reference count of snapshot
 */
static void DecrRefCount(snapxid_t* s)
{
    t_thrd.proc->snap_refcnt_bitmap &= ~(1 << (SNAPXID_INDEX(s) % 64));
    pg_write_barrier();
}

/*
 * test for zero reference count of snapshot
 */
static int IsZeroRefCount(snapxid_t* s)
{
    uint64 bitmap = 1 << (SNAPXID_INDEX(s) % 64);
    for (int i = 0; i < g_instance.proc_array_idx->numProcs; i++) {
        if (g_instance.proc_base_all_procs[g_instance.proc_array_idx->pgprocnos[i]]->snap_refcnt_bitmap & bitmap) {
            return 0;
        }
    }
    return 1;
}

#else

/*
 * increment reference count of snapshot
 */
static void IncrRefCount(snapxid_t* s)
{
    const int wh = 0;
    atomic_inc(&s->ref_cnt[wh].count);
}

/*
 * decrement reference count of snapshot
 */
static void DecrRefCount(snapxid_t* s)
{
    const int wh = 0;
    atomic_dec(&s->ref_cnt[wh].count);
}

/*
 * test for zero reference count of snapshot
 */
static int IsZeroRefCount(snapxid_t* s)
{
    int i;
    for (i = 0; i < NREFCNT; ++i) {
        if (s->ref_cnt[i].count) {
            return 0;
        }
    }
    return 1;
}

#endif

/* snapxid to be held off to the next commit */
static inline snapxid_t* GetNextSnapXid()
{
    return g_snap_buffer ? (snapxid_t*)g_snap_next : NULL;
}

const static int SNAP_ERROR_COUNT = 256;
/*
 * update the current snapshot pointer find the next available slot for the next pointer
 */
static void SetNextSnapXid()
{
    if (g_snap_buffer != NULL) {
        g_snap_current = g_snap_next;
        pg_write_barrier();
        g_snap_assigned = true;
        snapxid_t* ret = (snapxid_t*)g_snap_current;
        size_t idx = SNAPXID_INDEX(ret);
        int nofindCount = 0;
loop:
        do {
            ++idx;
            /* if wrap-around, take start from head to find free slot */
            if (idx == g_bufsz)
                idx = 0;
            ret = SNAPXID_AT(idx);
            if (IsZeroRefCount(ret)) {
                g_snap_next = ret;
                return;
            }
            nofindCount++;
        } while (ret != g_snap_next);
        /* we alloc sufficient space for local snapshot , overflow should not happen here */
        ereport(WARNING, (errmsg("snapshot ring buffer overflow.")));
        if (nofindCount >= SNAP_ERROR_COUNT) {
            ereport(PANIC, (errcode(ERRCODE_LOG), errmsg("Can not get an available snapshot slot")));
        }
        /* try to find available slot */
        goto loop;
    }
}

/*
 * just a wrapper to pass __snap_current to GetSnapshotData
 */
static snapxid_t* GetCurrentSnapXid()
{
    snapxid_t* x = (snapxid_t*)g_snap_current;
    IncrRefCount(x);
    return x;
}

/*
 * release snapshot data (decrement reference count)
 */
static void ReleaseSnapXid(snapxid_t* snapshot)
{
    DecrRefCount(snapshot);
}

#ifdef USE_ASSERT_CHECKING
/* add assert information for refcount of snapshot */
class AutoSnapId {
public:
    AutoSnapId()
        : m_count(1)
    {}

    ~AutoSnapId()
    {
        if (m_count > 0) {
            ereport(PANIC, (errcode(ERRCODE_LOG),
                errmsg("snapshot refcount leak, must be zero")));
        }
    }

    void decr()
    {
        m_count = 0;
    }

public:
    int m_count;
};
#endif

Snapshot GetLocalSnapshotData(Snapshot snapshot)
{
    /* if first here, fallback to original code */
    if (!g_snap_assigned || (g_snap_buffer == NULL)) {
        ereport(DEBUG1, (errmsg("Falling back to origin GetSnapshotData: not assigned yet or during shutdown\n")));
        return NULL;
    }
    pg_read_barrier();
    HOLD_INTERRUPTS();
    /* 1. increase ref-count of current snapshot in ring buffer */
    snapxid_t* snapxid = GetCurrentSnapXid();

#ifdef USE_ASSERT_CHECKING
    AutoSnapId snapid;
#endif

    /* save use_data for release */
    snapshot->user_data = snapxid;

    /* 2. copy from pre-computed snapshot arrays into return param snapshot */
    snapshot->takenDuringRecovery = snapxid->takenDuringRecovery;

    TransactionId replication_slot_xmin = g_instance.proc_array_idx->replication_slot_xmin;

    if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = snapxid->xmin;
        t_thrd.pgxact->handle = GetCurrentTransactionHandleIfAny();
    }

    if (TransactionIdPrecedes(snapxid->localxmin, (uint64)u_sess->attr.attr_storage.vacuum_defer_cleanup_age)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    } else {
        u_sess->utils_cxt.RecentGlobalXmin = snapxid->localxmin - u_sess->attr.attr_storage.vacuum_defer_cleanup_age;
    }

    if (!TransactionIdIsNormal(u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    }

    if (TransactionIdIsNormal(replication_slot_xmin) &&
        TransactionIdPrecedes(replication_slot_xmin, u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = replication_slot_xmin;
    }

    u_sess->utils_cxt.RecentGlobalCatalogXmin = GetOldestCatalogXmin();
    u_sess->utils_cxt.RecentXmin = snapxid->xmin;
    snapshot->xmin = snapxid->xmin;
    snapshot->xmax = snapxid->xmax;
    snapshot->snapshotcsn = snapxid->snapshotcsn;
    snapshot->curcid = GetCurrentCommandId(false);

    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->copied = false;
    /* Non-catalog tables can be vacuumed if older than this xid */
    u_sess->utils_cxt.RecentGlobalDataXmin = u_sess->utils_cxt.RecentGlobalXmin;

    ReleaseSnapXid(snapxid);
    snapshot->user_data = NULL;

#ifdef USE_ASSERT_CHECKING
    snapid.decr();
#endif

    RESUME_INTERRUPTS();

    return snapshot;
}

#define MAX_PENDING_SNAPSHOT_CNT 1000
#define CALC_SNAPSHOT_TIMEOUT (1 * 1000)

void forward_recent_global_xmin(void)
{
    (void)LWLockAcquire(CsnMinLock, LW_EXCLUSIVE);
    /*
     * check and update recentGlobalXmin, get a snapshot,
     * the csn of xid preceed recentLocalXmin, must smaller than nextCommitSeqNo.
     */
    if (t_thrd.xact_cxt.ShmemVariableCache->keep_csn <= t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min) {
        if (module_logging_is_on(MOD_TRANS_SNAPSHOT))
            ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT),
                          errmsg("update recentGlobalXmin, from  %lu to %lu. keep_xmin from %lu to %lu, "
                                 "keep_csn from %lu to %lu.",
                                 t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin, t_thrd.xact_cxt.ShmemVariableCache->keep_xmin,
                                 t_thrd.xact_cxt.ShmemVariableCache->keep_xmin, t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin,
                                 t_thrd.xact_cxt.ShmemVariableCache->keep_csn,
                                 t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo)));
        t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin = t_thrd.xact_cxt.ShmemVariableCache->keep_xmin;
        t_thrd.xact_cxt.ShmemVariableCache->keep_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
        t_thrd.xact_cxt.ShmemVariableCache->keep_csn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    }
    LWLockRelease(CsnMinLock);
}

static void init_shmem_csn_cleanup_instr(void)
{
    (void)LWLockAcquire(CsnMinLock, LW_EXCLUSIVE);
    /* make sure cutoff_csn_min is small enough when after redo to avoid false positive invalid snapshot */
    t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min = COMMITSEQNO_FIRST_NORMAL + 1;
    t_thrd.xact_cxt.ShmemVariableCache->keep_csn = COMMITSEQNO_FIRST_NORMAL + 1;
    t_thrd.xact_cxt.ShmemVariableCache->keep_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin;
    t_thrd.xact_cxt.ShmemVariableCache->local_csn_min = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    LWLockRelease(CsnMinLock);
}

static bool ForceCalculateSnapshotXmin(bool forceCalc)
{
    return (!u_sess->attr.attr_storage.enable_defer_calculate_snapshot || forceCalc);
}

void CalculateLocalLatestSnapshot(bool forceCalc)
{
    /*
     * 1. copy current snapshot data to next
     * 2. follow same line as original ProcArrayEndTransactionInternal
     * 3. generate new snapshot, based on code in GetSnapshotData_Orig()
     * 4. add new snapshot to ring buffer (lock-free)
     * 5. advance ring-buffer current snapshot pointer.
     */
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;
    TransactionId xmin;
    TransactionId xmax;
    TransactionId globalxmin;
    int index;
    Timestamp currentTimeStamp;
    static Timestamp snapshotTimeStamp = 0;
    static uint32 snapshotPendingCnt = 0;

    snapxid_t* snapxid = GetNextSnapXid();
    if (snapxid == NULL) {
        ereport(LOG, (errmsg("Skipping generation of new snapshot: ring buffer not active (during shutdown)\n")));
        return;
    }

    /* xmax is always latestCompletedXid + 1 */
    xmax = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    Assert(TransactionIdIsNormal(xmax));
    TransactionIdAdvance(xmax);

    /*
     * We calculate xmin under the fllowing conditions:
     * 1. we always calculate snapshot if disable defer_calculate_snpshot.
     * 2. we didn't calculate snapshot for GTM_MAX_PENDING_SNAPSHOT_CNT times.
     * 3. we didn't calculate snapshot for GTM_CALC_SNAPSHOT_TIMEOUT seconds.
     */
    currentTimeStamp = GetCurrentTimestamp();
    if (ForceCalculateSnapshotXmin(forceCalc) || ((++snapshotPendingCnt == MAX_PENDING_SNAPSHOT_CNT) ||
        (TimestampDifferenceExceeds(snapshotTimeStamp, currentTimeStamp, CALC_SNAPSHOT_TIMEOUT)))) {
        pg_read_barrier();
        snapshotPendingCnt = 0;
        snapshotTimeStamp = currentTimeStamp;

        /* initialize xmin calculation with xmax */
        globalxmin = xmin = xmax;

        /* Also need to include other snapshot xmin */
        if (g_snap_buffer != NULL) {
            TransactionId minXmin = ((snapxid_t*)g_snap_current)->xmin;
            if (!TransactionIdIsValid(minXmin))
                minXmin = globalxmin;
            for (size_t idx = 0; idx < g_bufsz; idx++) {
                snapxid_t* ret = NULL;

                ret = SNAPXID_AT(idx);
                if (!IsZeroRefCount(ret) && TransactionIdIsValid(ret->xmin)) {
                    if (TransactionIdPrecedes(ret->xmin, minXmin)) {
                        minXmin = ret->xmin;
                    }
                }
            }
            if (TransactionIdPrecedes(minXmin, globalxmin))
                globalxmin = minXmin;
        }

        int* pgprocnos = arrayP->pgprocnos;
        int numProcs;

        /*
         * Spin over procArray checking xid, xmin, and subxids.  The goal is
         * to gather all active xids, find the lowest xmin, and try to record
         * subxids. Also need include myself.
         */
        numProcs = arrayP->numProcs;

        for (index = 0; index < numProcs; index++) {
            int pgprocno = pgprocnos[index];
            volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
            TransactionId xid = InvalidTransactionId;

            /*
             * Backend is doing logical decoding which manages xmin
             * separately, check below.
             */
            if (pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING)
                continue;

            /* Update globalxmin to be the smallest valid xmin, only Ignore procs running LAZY VACUUM xmin */
            if (!(pgxact->vacuumFlags & PROC_IN_VACUUM)) {
                xid = pgxact->xmin; /* fetch just once */
            }

            if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, globalxmin))
                globalxmin = xid;

            /* Fetch xid just once - see GetNewTransactionId */
            xid = pgxact->xid;

            /* If no XID assigned, use xid passed down from CN */
            if (!TransactionIdIsNormal(xid))
                xid = pgxact->next_xid;

            /*
             * If the transaction has no XID assigned, we can skip it; it
             * won't have sub-XIDs either.  If the XID is >= xmax, we can also
             * skip it; such transactions will be treated as running anyway
             * (and any sub-XIDs will also be >= xmax).
             */
            if (!TransactionIdIsNormal(xid) || !TransactionIdPrecedes(xid, xmax))
                continue;

            /*
             * We don't include our own XIDs (if any) in the snapshot, but we
             * must include them in xmin.
             * Not true any more in this function.
             */
            if (TransactionIdPrecedes(xid, xmin))
                xmin = xid;
        }

        /*
         * Update globalxmin to include actual process xids.  This is a slightly
         * different way of computing it than GetOldestXmin uses, but should give
         * the same result. GTM-FREE-MODE always use recentLocalXmin.
         */
        if (TransactionIdPrecedes(xmin, globalxmin))
            globalxmin = xmin;

        if (ENABLE_DMS && SS_PRIMARY_MODE) {
            SSUpdateNodeOldestXmin(SS_MY_INST_ID, globalxmin);
            globalxmin = SSGetGlobalOldestXmin(globalxmin);
            if (ENABLE_SS_BCAST_SNAPSHOT) {
                SSSendLatestSnapshotToStandby(xmin, xmax, t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
            }
        }

        t_thrd.xact_cxt.ShmemVariableCache->xmin = xmin;
        t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin = globalxmin;
        if (GTM_FREE_MODE) {
            t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin = globalxmin;
        }
    } else if (ENABLE_SS_BCAST_SNAPSHOT && SS_PRIMARY_MODE) {
        SSSendLatestSnapshotToStandby(t_thrd.xact_cxt.ShmemVariableCache->xmin, xmax,
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo);
    }

    if (GTM_LITE_MODE) {
        currentTimeStamp = GetCurrentTimestamp();
        if (forceCalc) {  /* means first time here; */
            init_shmem_csn_cleanup_instr();
            forward_recent_global_xmin();
        }
    }

    snapxid->xmin = t_thrd.xact_cxt.ShmemVariableCache->xmin;
    snapxid->xmax = xmax;
    snapxid->localxmin = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
    snapxid->snapshotcsn = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    snapxid->takenDuringRecovery = RecoveryInProgress();

    pg_write_barrier();

    ereport(DEBUG1, (errmsg("Generated snapshot in ring buffer slot %lu\n", SNAPXID_INDEX(snapxid))));
    SetNextSnapXid();
}

/*
 * Return the minimal xmin in all the valid snapshot versions.
 */
static TransactionId GetMultiSnapshotOldestXmin()
{
    return ((snapxid_t*)g_snap_current)->localxmin;
}

void ProcArrayResetXmin(PGPROC* proc)
{
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];

    /*
     * Note we can do this without locking because we assume that storing an Xid
     * is atomic.
     */
    pgxact->xmin = InvalidTransactionId;
}

/* return global csn from GTM */
CommitSeqNo GetCommitCsn()
{
    return t_thrd.proc->commitCSN;
}

void setCommitCsn(uint64 commit_csn)
{
    t_thrd.proc->commitCSN = commit_csn;
}

/**
 * @Description: Return the parent xid of the given sub xid.
 *
 * @in xid -  the sub transaction id
 * @return -  return invlid transactionid if not found, otherwise
 * 		return the parent xid.
 */
TransactionId SubTransGetTopParentXidFromProcs(TransactionId xid)
{
    ProcArrayStruct* arrayP = g_instance.proc_array_idx;

    LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (int i = 0; i < arrayP->numProcs; i++) {
        int pgprocno = arrayP->pgprocnos[i];
        volatile PGPROC* proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId pxid;

        /* Fetch xid just once - see GetNewTransactionId */
        pxid = pgxact->xid;

        /*
         * search the sub xids, return the top parent xid when match.
         */
        if (pgxact->nxids > 0) {
            /* Use subxidsLock to protect subxids */
            LWLockAcquire(proc->subxidsLock, LW_SHARED);
            for (int j = pgxact->nxids - 1; j >= 0; j--) {
                TransactionId cxid = proc->subxids.xids[j];

                if (TransactionIdEquals(cxid, xid)) {
                    /* when found, release the lock and return the parent xid. */
                    LWLockRelease(proc->subxidsLock);
                    LWLockRelease(ProcArrayLock);
                    return pxid;
                }
            }
            LWLockRelease(proc->subxidsLock);
        }
    }

    LWLockRelease(ProcArrayLock);

    return InvalidTransactionId;
}

Datum pgxc_gtm_snapshot_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));
    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext* funcctx = NULL;
    /* check gtm mode, only gtm support this function */
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("unsupported function or view in %s mode.", GTM_LITE_MODE ? "GTM-Lite" : "GTM-Free")));
    SRF_RETURN_DONE(funcctx);
#endif
}

/*
 * @Description: check whether csn is valid, if valid, set it to pgxact.
 *
 * @in func -  the function which need to check and set csn
 * @in csn_min -  the csn to check
 * @in snapshot_type -  the type of snapshot
 * @in from -  where the snapshot from
 *
 * @return -  return csn_min set in pgxact,
 *            or InvalidCommitSeqNo if csn_min is invalid and from the local multi snapshot
 */
CommitSeqNo
set_proc_csn_and_check(const char* func, CommitSeqNo csn_min, GTM_SnapshotType gtm_snapshot_type, SnapshotSource from)
{
    if (u_sess->attr.attr_common.xc_maintenance_mode || u_sess->utils_cxt.cn_xc_maintain_mode ||
        IsAutoVacuumWorkerProcess() || gtm_snapshot_type == GTM_SNAPSHOT_TYPE_AUTOVACUUM) {
        return csn_min;
    }
    if (!COMMITSEQNO_IS_COMMITTED(csn_min))
        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                        errmsg("Snapshot is invalid, snaphot type %s, snapshot csn: %lu.",
                               transfer_snapshot_type(gtm_snapshot_type), csn_min)));

    LWLockAcquire(CsnMinLock, LW_SHARED);

    /* make sure the received csn from gtm is not small than local_csn_min */
    CommitSeqNo local_csn_min = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->local_csn_min);
    if (from == SNAPSHOT_DIRECT && csn_min < local_csn_min) {
        csn_min = local_csn_min;
    }

    CommitSeqNo cutoff_csn_min = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->cutoff_csn_min);
    if (csn_min < cutoff_csn_min) {
        if (from == SNAPSHOT_DATANODE) {
            LWLockRelease(CsnMinLock);
            return InvalidCommitSeqNo;
        }
        LWLockRelease(CsnMinLock);
        ereport(ERROR,
                (errcode(ERRCODE_SNAPSHOT_INVALID),
                 errmsg("Snapshot is invalid, this is a safe error, snapshot too old."),
                 errdetail("Snaphot type %s csn %lu is lower than cutoff_csn_min %lu in %s.",
                           transfer_snapshot_type(gtm_snapshot_type), csn_min, cutoff_csn_min, func),
                 errhint("This is a safe error report, will not impact "
                         "data consistency, retry your query if needed.")));
    } else {
        ereport(DEBUG1, (errmsg("try to set my proc csn from %lu to %lu.",
                                t_thrd.pgxact->csn_min, csn_min)));
    }

    t_thrd.pgxact->csn_min = csn_min;
    LWLockRelease(CsnMinLock);

    return t_thrd.pgxact->csn_min;
}

Datum get_gtm_lite_status(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext* funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));
    SRF_RETURN_DONE(funcctx);
#else
#define GTM_LITE_STATUS_ATTRS 2

    /* check gtm mode, gtm-free unsupport this function */
    if (GTM_FREE_MODE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unsupported function or view in GTM-FREE mode.")));
    }

    FuncCallContext* funcctx = NULL;
    GTMLite_Status gtm_status = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(GTM_LITE_STATUS_ATTRS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "backup_xid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "csn", XIDOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = 1;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[GTM_LITE_STATUS_ATTRS];
        bool nulls[GTM_LITE_STATUS_ATTRS];
        HeapTuple tuple;
        errno_t rc = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check_c(rc, "\0", "\0");

        gtm_status = GetGTMLiteStatus();
        if (!gtm_status) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                            errmsg("GTM error, could not obtain snapshot_status, please check GTM is running or failovering.")));
        }
        values[0] = TransactionIdGetDatum(gtm_status->backup_xid);
        values[1] = TransactionIdGetDatum(gtm_status->csn);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE(funcctx);
    }
#endif
}

const char* transfer_snapshot_type(GTM_SnapshotType gtm_snap_type)
{
    if (gtm_snap_type == GTM_SNAPSHOT_TYPE_UNDEFINED) {
        return "UNDEFINED";
    } else if (gtm_snap_type == GTM_SNAPSHOT_TYPE_LOCAL) {
        return "LOCAL";
    } else if (gtm_snap_type == GTM_SNAPSHOT_TYPE_GLOBAL) {
        return "GLOBAL";
    } else if (gtm_snap_type == GTM_SNAPSHOT_TYPE_AUTOVACUUM) {
        return "AUTOVACUUM";
    }
    return "UnKnown";
}

/*
 * search all active backend to get oldest frozenxid
 * for global temp table.
 */
TransactionId ListAllThreadGttFrozenxids(int maxSize, ThreadId *pids, TransactionId *xids, int *n)
{
    ProcArrayStruct *arrayP = g_instance.proc_array_idx;
    TransactionId result = InvalidTransactionId;
    int index;
    int flags = 0;
    int i = 0;

    if (g_instance.attr.attr_storage.max_active_gtt <= 0)
        return 0;

    if (maxSize > 0) {
        Assert(pids);
        Assert(xids);
        Assert(n);
        *n = 0;
    }

    if (RecoveryInProgress() || SSIsServerModeReadOnly())
        return InvalidTransactionId;

    flags |= PROC_IS_AUTOVACUUM;
    flags |= PROC_IN_LOGICAL_DECODING;

    LWLockAcquire(ProcArrayLock, LW_SHARED);
    if (maxSize > 0 && maxSize < arrayP->numProcs) {
        LWLockRelease(ProcArrayLock);
        elog(ERROR, "pids, xids array size is not enough for list all gtt frozenxids.");
    }

    for (index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        volatile PGPROC *proc = g_instance.proc_base_all_procs[pgprocno];
        volatile PGXACT *pgxact = &g_instance.proc_base_all_xacts[pgprocno];

        if (pgxact->vacuumFlags & flags)
            continue;

        if (proc->databaseId == u_sess->proc_cxt.MyDatabaseId &&
            TransactionIdIsNormal(proc->gtt_session_frozenxid)) {
            if (result == InvalidTransactionId)
                result = proc->gtt_session_frozenxid;
            else if (TransactionIdPrecedes(proc->gtt_session_frozenxid, result))
                result = proc->gtt_session_frozenxid;

            if (maxSize > 0) {
                pids[i] = proc->pid;
                xids[i] = proc->gtt_session_frozenxid;
                i++;
            }
        }
    }
    LWLockRelease(ProcArrayLock);
    if (maxSize > 0) {
        *n = i;
    }
    return result;
}

CommitSeqNo
calculate_local_csn_min()
{
    /* acquire procarray and csnmin lock, and there is no deadlock */
    LWLockAcquire(ProcArrayLock, LW_SHARED);
    ProcArrayStruct *arrayP = g_instance.proc_array_idx;
    int *pgprocnos = arrayP->pgprocnos;
    int num_procs = arrayP->numProcs;
    CommitSeqNo local_csn_min = t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo;
    LWLockAcquire(CsnMinLock, LW_EXCLUSIVE);
    for (int index = 0; index < num_procs; index++) {
        int pgprocno = pgprocnos[index];
        volatile PGXACT *pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        /*
        * Ignore procs doing logical decoding which manages xmin
        * separately or running LAZY VACUUM
        */
        if ((pgxact->vacuumFlags & PROC_IN_LOGICAL_DECODING) || (pgxact->vacuumFlags & PROC_IN_VACUUM)) {
            continue;
        }

        CommitSeqNo current_csn = pgxact->csn_min;  /* fetch the csn min */
        if (COMMITSEQNO_IS_COMMITTED(current_csn) && current_csn < local_csn_min) {
            local_csn_min = current_csn;
        }
    }
    LWLockRelease(CsnMinLock);
    LWLockRelease(ProcArrayLock);
    return local_csn_min;
}
/*
 * Updates the maximum value for CSN read from XLog
 */

void UpdateXLogMaxCSN(CommitSeqNo xlogCSN)
{
    if (xlogCSN > t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN) {
        LWLockAcquire(XLogMaxCSNLock, LW_EXCLUSIVE);
        if (xlogCSN > t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN) {
            t_thrd.xact_cxt.ShmemVariableCache->xlogMaxCSN = xlogCSN;
        }
        LWLockRelease(XLogMaxCSNLock);
    }
}

/* Get the current oldestxmin, as there may be no transaction or no finished one */
void GetOldestGlobalProcXmin(TransactionId *globalProcXmin)
{
    TransactionId globalxmin = MaxTransactionId;
    ProcArrayStruct *arrayP = g_instance.proc_array_idx;
    int *pgprocnos = arrayP->pgprocnos;
    int numProcs = arrayP->numProcs;
    (void)LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (int index = 0; index < numProcs; index++) {
        int pgprocno = pgprocnos[index];
        volatile PGXACT* pgxact = &g_instance.proc_base_all_xacts[pgprocno];
        TransactionId xid;
        if (pgxact->vacuumFlags & PROC_IN_VACUUM)
            continue;

        xid = pgxact->xmin;

        if (TransactionIdIsNormal(xid) && TransactionIdPrecedesOrEquals(xid, globalxmin)) {
            globalxmin = xid;
            *globalProcXmin = globalxmin;
        }
    }
    LWLockRelease(ProcArrayLock);
}
