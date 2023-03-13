/*
 *
 * twophase.cpp
 *		Two-phase commit support functions.
 *
 * Portions Copyright (c) 2020, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *		src/gausskernel/storage/access/transam/twophase.cpp
 *
 * NOTES
 *		Each global transaction is associated with a global transaction
 *		identifier (GID). The client assigns a GID to a openGauss
 *		transaction with the PREPARE TRANSACTION command.
 *
 *		We keep all active global transactions in a shared memory array.
 *		When the PREPARE TRANSACTION command is issued, the GID is
 *		reserved for the transaction in the array. This is done before
 *		a WAL entry is made, because the reservation checks for duplicate
 *		GIDs and aborts the transaction if there already is a global
 *		transaction in prepared state with the same GID.
 *
 *		A global transaction (gxact) also has dummy PGXACT and PGPROC; this is
 *		what keeps the XID considered running by TransactionIdIsInProgress.
 *		It is also convenient as a PGPROC to hook the gxact's locks to.
 *
 *		Information to recover prepared transactions in case of crash is
 *		now stored in WAL for the common case. In some cases there will be
 *		an extended period between preparing a GXACT and commit/abort, in
 *		which case we need to separately record prepared transaction data
 *		in permanent storage. This includes locking information, pending
 *		notifications etc. All that state information is written to the
 *		per-transaction state file in the pg_twophase directory.
 *		All prepared transactions will be written prior to shutdown.
 *
 *		Life track of state data is following:
 *
 *		* On PREPARE TRANSACTION backend writes state data only to the WAL and
 *		  stores pointer to the start of the WAL record in
 *		  gxact->prepare_start_lsn.
 *		* If COMMIT occurs before checkpoint then backend reads data from WAL
 *		  using prepare_start_lsn.
 *		* On checkpoint state data copied to files in pg_twophase directory and
 *		  fsynced
 *		* If COMMIT happens after checkpoint then backend reads state data from
 *		  files
 *
 *		During replay and replication, TwoPhaseState also holds information
 *		about active prepared transactions that haven't been moved to disk yet.
 *
 *		Replay of twophase records happens by the following rules:
 *
 *		* At the beginning of recovery, pg_twophase is scanned once, filling
 *		  TwoPhaseState with entries marked with gxact->inredo and
 *		  gxact->ondisk.  Two-phase file data older than the XID horizon of
 *		  the redo position are discarded.
 *		* On PREPARE redo, the transaction is added to TwoPhaseState->prepXacts.
 *		  gxact->inredo is set to true for such entries.
 *		* On Checkpoint we iterate through TwoPhaseState->prepXacts entries
 *		  that have gxact->inredo set and are behind the redo_horizon. We
 *		  save them to disk and then switch gxact->ondisk to true.
 *		* On COMMIT/ABORT we delete the entry from TwoPhaseState->prepXacts.
 *		  If gxact->ondisk is true, the corresponding entry from the disk
 *		  is additionally deleted.
 *		* The function named RecoverPreparedTransactions()
 *        and the function named StandbyRecoverPreparedTransactions()
 *		  and the function named PrescanPreparedTransactions() have been modified
 *        to go through gxact->inredo entries that have not made it to disk.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/htup.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/cstore_am.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "commands/tablespace.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "replication/datasyncrep.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "utils/distribute_test.h"
#include "access/tableam.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif
#include "instruments/instr_statement.h"
#include "storage/file/fio_device.h"

/*
 * Directory where Two-phase commit files reside within PGDATA
 */
#define TWOPHASE_DIR (g_instance.datadir_cxt.twophaseDir)

int PendingPreparedXactsCount = 0;

/*
 * This struct describes one global transaction that is in prepared state
 * or attempting to become prepared.
 *
 * The lifecycle of a global transaction is:
 *
 * 1. After checking that the requested GID is not in use, set up an
 * entry in the TwoPhaseState->prepXacts array with the correct XID and GID,
 * with locking_xid = my own XID and valid = false.
 *
 * 2. After successfully completing prepare, set valid = true and enter the
 * referenced PGPROC into the global ProcArray.
 *
 * 3. To begin COMMIT PREPARED or ROLLBACK PREPARED, check that the entry
 * is valid and its locking_xid is no longer active, then store my current
 * XID into locking_xid.  This prevents concurrent attempts to commit or
 * rollback the same prepared xact.
 *
 * 4. On completion of COMMIT PREPARED or ROLLBACK PREPARED, remove the entry
 * from the ProcArray and the TwoPhaseState->prepXacts array and return it to
 * the freelist.
 *
 * Note that if the preparing transaction fails between steps 1 and 2, the
 * entry will remain in prepXacts until recycled.  We can detect recyclable
 * entries by checking for valid = false and locking_xid no longer active.
 *
 * typedef struct GlobalTransactionData *GlobalTransaction appears in
 * twophase.h
 */
static void RecordTransactionCommitPrepared(TransactionId xid, int nchildren, TransactionId *children, int nrels,
                                            ColFileNode *rels, int ninvalmsgs, SharedInvalidationMessage *invalmsgs,
                                            int nlibrary, char *librarys, int libraryLen, bool initfileinval);
static void RecordTransactionAbortPrepared(TransactionId xid, int nchildren, TransactionId *children, int nrels,
                                           ColFileNode *rels, int nlibrary, char *library, int libraryLen);
static void ProcessRecords(char *bufptr, TransactionId xid, const TwoPhaseCallback callbacks[]);
static void RemoveGXact(GlobalTransaction gxact);
static int CopyPreparedTransactionList(TransactionId **prepared_xid_list, int partId);
static int GetPreparedTransactionList(GlobalTransaction *gxacts);
static void XlogReadTwoPhaseData(XLogRecPtr lsn, char **buf, int *len);
static void CloseTwoPhaseXlogFile();
static char *ProcessTwoPhaseBuffer(TransactionId xid, XLogRecPtr prepare_start_lsn, bool fromdisk, bool setParent,
                                   bool setNextXid);
static void MarkAsPreparingGuts(GTM_TransactionHandle handle, GlobalTransaction gxact, TransactionId xid,
                                const char *gid, TimestampTz prepared_at, Oid owner, Oid databaseid, uint64 sessionid);
static void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning);
static void RecreateTwoPhaseFile(TransactionId xid, void *content, int len);
static Datum get_my_node_name();

extern bool find_tmptable_cache_key(Oid relNode);

/*
 * Initialization of shared memory
 */
Size TwoPhaseShmemSize(void)
{
    Size size;
    /* Need the fixed struct, the array of pointers, and the GTD structs */
    size = offsetof(TwoPhaseStateData, prepXacts);
    size = add_size(size, mul_size(g_instance.attr.attr_storage.max_prepared_xacts, sizeof(GlobalTransaction)));
    size = MAXALIGN(size);
    size = add_size(size, mul_size(g_instance.attr.attr_storage.max_prepared_xacts, sizeof(GlobalTransactionData)));

    return size;
}

void TwoPhaseShmemInitOnePart(int index)
{
    const int TWOPHASE_MAX_NAME_LENGTH = 64;
    bool found = false;
    int rc = 0;
    char name[TWOPHASE_MAX_NAME_LENGTH];
    rc = sprintf_s(name, TWOPHASE_MAX_NAME_LENGTH, "%s%d", "Prepared Transaction Table ", index);
    securec_check_ss(rc, "\0", "\0");
    TwoPhaseState(index) = (TwoPhaseStateData *)ShmemInitStruct(name, TwoPhaseShmemSize(), &found);
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(index);
    int numPreparedXacts = g_instance.attr.attr_storage.max_prepared_xacts;
    if (!IsUnderPostmaster) {
        GlobalTransaction gxacts;

        Assert(!found);
        currentStatePtr->freeGXacts = NULL;
        currentStatePtr->numPrepXacts = 0;

        /*
         * Initialize the linked list of free GlobalTransactionData structs
         */
        gxacts =
            (GlobalTransaction)((char *)currentStatePtr +
                                MAXALIGN(offsetof(TwoPhaseStateData, prepXacts) +
                                         sizeof(GlobalTransaction) * numPreparedXacts));
        for (int i = 0; i < numPreparedXacts; i++) {
            /* insert into linked list */
            gxacts[i].next = currentStatePtr->freeGXacts;
            currentStatePtr->freeGXacts = &gxacts[i];

            /* associate it with a PGPROC assigned by InitProcGlobal */
            gxacts[i].pgprocno = g_instance.proc_preparexact_base[i + index * numPreparedXacts]->pgprocno;

            /*
             * Assign a unique ID for each dummy proc, so that the range of
             * dummy backend IDs immediately follows the range of normal
             * backend IDs. We don't dare to assign a real backend ID to dummy
             * procs, because prepared transactions don't take part in cache
             * invalidation like a real backend ID would imply, but having a
             * unique ID for them is nevertheless handy. This arrangement
             * allows you to allocate an array of size (g_instance.shmem_cxt.MaxBackends +
             * g_instance.attr.attr_storage.max_prepared_xacts + 1), and have a slot for every backend and
             * prepared transaction. Currently multixact.c uses that
             * technique.
             */
            gxacts[i].dummyBackendId = g_instance.shmem_cxt.MaxBackends + 1 + i + index * numPreparedXacts;
        }
        if (GTM_LITE_MODE) {
            Size size = mul_size(numPreparedXacts, sizeof(TransactionId));

            for (int i = 0; i < MAX_PREP_XACT_VERSIONS; i++) {
                currentStatePtr->validPrepXids[i].refCount = 0;
                currentStatePtr->validPrepXids[i].numPrepXid = 0;
                currentStatePtr->validPrepXids[i].isOverflow = false;
                currentStatePtr->validPrepXids[i].validPrepXid = (TransactionId *)palloc0(size);
            }
            currentStatePtr->currPrepXid = &(currentStatePtr->validPrepXids[0]);
            currentStatePtr->nextPrepXid = &(currentStatePtr->validPrepXids[1]);
        }
    } else {
        Assert(found);
    }
}

void TwoPhaseShmemInit(void)
{
    int i;
    MemoryContext old = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    t_thrd.xact_cxt.TwoPhaseState = 
        (TwoPhaseStateData**)palloc0(NUM_TWOPHASE_PARTITIONS * sizeof(TwoPhaseStateData*));
    (void)MemoryContextSwitchTo(old);
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TwoPhaseShmemInitOnePart(i);
    }
}

/* atomic action for preplist ring buffer */
static void IncrPrepXidRef(ValidPrepXid prepXid)
{
    pg_atomic_fetch_add_u32(&(prepXid->refCount), 1);
}

static void DecrPrepXidRef(ValidPrepXid prepXid)
{
    pg_atomic_fetch_sub_u32(&(prepXid->refCount), 1);
}

static bool IsPrepXidZeroRef(ValidPrepXid prepXid)
{
    return (prepXid->refCount == 0);
}

static inline uint32 GetPrepXidIndex(ValidPrepXid prepXid, TwoPhaseStateData *currentStatePtr)
{
    ValidPrepXid prepXidBase = &(currentStatePtr->validPrepXids[0]);
    return prepXid - prepXidBase;
}

static inline ValidPrepXid GetNextPrepXid(TwoPhaseStateData *currentStatePtr)
{
    return currentStatePtr->nextPrepXid;
}

/*
 * Call this function while holding the EXCLUSIVE TwoPhaseStateLock
 * Publishes the changes made to nextPrepXid by setting currPrepXid to point to it
 * and looks for a new victim to evict from the prepared ring buffer
 * for the next transaction by sets nextPrepXid pointing to it.
 */
static void SetNextPrepXid(TwoPhaseStateData *currentStatePtr)
{
    ValidPrepXid tmp = currentStatePtr->currPrepXid;
    /* Changes to nextPrepXid are published */
    currentStatePtr->currPrepXid = currentStatePtr->nextPrepXid;
    ValidPrepXid curr = currentStatePtr->currPrepXid;

    uint32 idx = GetPrepXidIndex(curr, currentStatePtr);

LOOP:
    do {
        /* if wrap-around, take start from ring buffer head to find free slot */
        if (++idx == MAX_PREP_XACT_VERSIONS) {
            idx = 0;
        }
        curr = &(currentStatePtr->validPrepXids[idx]);

        if (curr != tmp && curr != currentStatePtr->currPrepXid && IsPrepXidZeroRef(curr)) {
            currentStatePtr->nextPrepXid = curr;
            currentStatePtr->nextPrepXid->numPrepXid = 0;
            return;
        }
    } while (curr != currentStatePtr->nextPrepXid);

    ereport(WARNING,
            (errmsg("prepared transactions ring buffer overflow, max_prepared_xact_version is: %d. "
                "try it again.", MAX_PREP_XACT_VERSIONS)));

    goto LOOP;
}

static ValidPrepXid GetCurrPrepXid(TwoPhaseStateData *currentStatePtr)
{
    ValidPrepXid prepXid = currentStatePtr->currPrepXid;
    IncrPrepXidRef(prepXid);
    return prepXid;
}

static void ReleasePrepXid(ValidPrepXid prepXid)
{
    DecrPrepXidRef(prepXid);
}

static int IsPrepXidValid(ValidPrepXid prepXid, TransactionId xid)
{
    for (unsigned int i = 0; i < prepXid->numPrepXid; i++) {
        if (prepXid->validPrepXid[i] == xid) {
            return i;
        }
    }
    return -1;
}

/* if the prepared xid list is overflow, we need copy the list from
 * two-phase state struct
 */
static void MarkPrepXidPrepareListOverflow(ValidPrepXid current, ValidPrepXid next, TwoPhaseStateData *currentStatePtr)
{
    /* current points to the current valid prepared xid list,
     * and next points the next valid prepared xid list
     */
    if (current == NULL || next == NULL) {
        ereport(ERROR,
                (errmsg("the current valid prepared xid list or the next valid prepared list should not be NULL")));
        return;
    }
    int maxPrepareXacts = g_instance.attr.attr_storage.max_prepared_xacts;
    int numXacts = currentStatePtr->numPrepXacts;
    if (numXacts == 0) {
        /* the two phase state struct is empty,
         * so release the current valid prepared list, switch to the next
         * change the over flag to false
         */
        next->numPrepXid = 0;
        next->isOverflow = false;
        ReleasePrepXid(current);
        SetNextPrepXid(currentStatePtr);
        return;
    }
    int ntotalxids = numXacts;
    int i = 0;
    for (i = 0; i < numXacts; i++) {
        /* fetch each txn in the two phase state
         * count the sub txns.
         */
        GlobalTransaction gxact = currentStatePtr->prepXacts[i];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
        /* if the trx has been marked 'invalid',  skip it */
        if (!gxact->valid) {
            continue;
        }
        ntotalxids += pgxact->nxids;
    }
    if (ntotalxids > maxPrepareXacts) {
        /* if the total xid number is bigger than maxPrepareXacts,
         * then the next valid should have the overflow flag.
         */
        next->isOverflow = true;
    } else {
        int index = 0;
        ntotalxids = numXacts;
        for (i = 0; i < numXacts; i++) {
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];
            PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
            PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
            /* if the trx has been marked 'invalid',  skip it */
            if (!gxact->valid) {
                continue;
            }
            *(next->validPrepXid + index) = gxact->xid;
            index++;
            if (pgxact->nxids > 0) {
                LWLockAcquire(proc->subxidsLock, LW_SHARED);
                if (proc->subxids.xids != NULL) {
                    Size subXidSize = mul_size(pgxact->nxids, sizeof(TransactionId));
                    errno_t rc = memcpy_s(next->validPrepXid + index, subXidSize, proc->subxids.xids, subXidSize);
                    securec_check(rc, "", "");
                    index += pgxact->nxids;
                    ntotalxids += pgxact->nxids;
                }
                LWLockRelease(proc->subxidsLock);
            }
        }
        next->isOverflow = false;
    }
    next->numPrepXid = ntotalxids;
    ReleasePrepXid(current);
    SetNextPrepXid(currentStatePtr);
    return;
}

/*
 * Call this function while holding the EXCLUSIVE TwoPhaseStateLock
 * Inserts xid to the currPrepXid by creating a new prepared list
 * and sets currPrepXid pointing to it.
 */
static void MarkPrepXidValid(const GlobalTransaction gxact)
{
    TransactionId xid = gxact->xid;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    /* Get latest prepared list, in case we need to copy */
    ValidPrepXid curr = GetCurrPrepXid(currentStatePtr);
    /* Get our victim for eviction */
    ValidPrepXid next = GetNextPrepXid(currentStatePtr);
    PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
    int max_prepare_xacts = g_instance.attr.attr_storage.max_prepared_xacts;
    if (curr->isOverflow) {
        MarkPrepXidPrepareListOverflow(curr, next, currentStatePtr);
        return;
    }
    if (curr->numPrepXid > 0) {
        /* Copy over the prepared list from the latest one */
        Size size = mul_size(curr->numPrepXid, sizeof(TransactionId));
        errno_t rc = memcpy_s(next->validPrepXid, size, curr->validPrepXid, size);
        securec_check(rc, "", "");
        next->numPrepXid = curr->numPrepXid;
    }
    ReleasePrepXid(curr);

    /* Add entry to the end of prepared list */
    if (next->numPrepXid + 1 > (unsigned int)max_prepare_xacts) {
        next->isOverflow = true;
    } else {
        next->validPrepXid[next->numPrepXid] = xid;
    }
    /* increase the xids list counter */
    next->numPrepXid++;
    /* Add the sub xids to prepared list */
    if (next->numPrepXid + pgxact->nxids > (unsigned int)max_prepare_xacts) {
        next->isOverflow = true;
        next->numPrepXid = next->numPrepXid + pgxact->nxids;
    } else {
        int i = 0;
        for (i = 0; i < pgxact->nxids; i++) {
            next->validPrepXid[next->numPrepXid++] = proc->subxids.xids[i];
        }
    }
    ereport(DEBUG1, (errmsg("Generated prepared list in slot %u. Add new transactoin id %lu to prepared list!",
                            GetPrepXidIndex(next, currentStatePtr), xid)));
    /* Publish our changes and find a new victim for eviction */
    SetNextPrepXid(currentStatePtr);
}

/*
 * Call this function while holding the EXCLUSIVE TwoPhaseStateLock
 * Removes xid from the currPrepXid by creating a new prepared list
 * and sets currPrepXid pointing to it.
 */
static void MarkPrepXidInvalid(const GlobalTransaction gxact, const TransactionId *subxids, const int nsubxids)
{
    TransactionId xid = gxact->xid;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    /* Get latest prepared list, in case we need to copy */
    ValidPrepXid curr = GetCurrPrepXid(currentStatePtr);
    /* Get our victim for eviction */
    ValidPrepXid next = GetNextPrepXid(currentStatePtr);
    if (curr->isOverflow) {
        MarkPrepXidPrepareListOverflow(curr, next, currentStatePtr);
        return;
    }
    int idx = IsPrepXidValid(curr, xid);
    if (idx == -1) {
        ereport(DEBUG1, (errmsg("Transaction Id %lu has already been marked as invalid!", xid)));
        ReleasePrepXid(curr);
        return;
    }

    if (curr->numPrepXid > 0) {
        /* Copy over the prepared list from the latest one */
        Size size = mul_size(curr->numPrepXid, sizeof(TransactionId));
        errno_t rc = memcpy_s(next->validPrepXid, size, curr->validPrepXid, size);
        securec_check(rc, "", "");
        next->numPrepXid = curr->numPrepXid;
    }
    ReleasePrepXid(curr);

    /* Find the entry and delete it */
    next->numPrepXid--;
    next->validPrepXid[idx] = next->validPrepXid[next->numPrepXid];

    /* delete sub xids of this xact from prepared list */
    int i = 0;
    for (i = 0; i < nsubxids; i++) {
        TransactionId subxid = subxids[i];
        int index = IsPrepXidValid(next, subxid);
        if (index != -1) {
            next->numPrepXid--;
            next->validPrepXid[index] = next->validPrepXid[next->numPrepXid];
        }
    }
    ereport(DEBUG1, (errmsg("Generated prepared list in slot %u. Remove transactoin id %lu from prepared list!",
                            GetPrepXidIndex(next, currentStatePtr), xid)));
    /* Publish our changes and find a new victim for eviction */
    SetNextPrepXid(currentStatePtr);
}

/*
 * Exit hook to unlock the global transaction entry we're working on.
 */
static void AtProcExit_Twophase(int code, Datum arg)
{
    /* same logic as abort */
    AtAbort_Twophase();
}

/*
 * Abort hook to unlock the global transaction entry we're working on.
 */
void AtAbort_Twophase(void)
{
    if (t_thrd.xact_cxt.MyLockedGxact == NULL) {
        return;
    }

    /*
     * What to do with the locked global transaction entry?  If we were in
     * the process of preparing the transaction, but haven't written the WAL
     * record and state file yet, the transaction must not be considered as
     * prepared.  Likewise, if we are in the process of finishing an
     * already-prepared transaction, and fail after having already written
     * the 2nd phase commit or rollback record to the WAL, the transaction
     * should not be considered as prepared anymore.  In those cases, just
     * remove the entry from shared memory.
     *
     * Otherwise, the entry must be left in place so that the transaction
     * can be finished later, so just unlock it.
     *
     * If we abort during prepare, after having written the WAL record, we
     * might not have transfered all locks and other state to the prepared
     * transaction yet.  Likewise, if we abort during commit or rollback,
     * after having written the WAL record, we might not have released
     * all the resources held by the transaction yet.  In those cases, the
     * in-memory state can be wrong, but it's too late to back out.
     */
    TransactionId xid = t_thrd.xact_cxt.MyLockedGxact->xid;
    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    if (!t_thrd.xact_cxt.MyLockedGxact->valid) {
        RemoveGXact(t_thrd.xact_cxt.MyLockedGxact);
    } else {
        t_thrd.xact_cxt.MyLockedGxact->locking_backend = InvalidBackendId;
    }
    TWOPAHSE_LWLOCK_RELEASE(xid);
    t_thrd.xact_cxt.MyLockedGxact = NULL;
}

/*
 * This is called after we have finished transfering state to the prepared
 * PGXACT entry.
 */
void PostPrepare_Twophase()
{
    TransactionId xid = t_thrd.xact_cxt.MyLockedGxact->xid;
    Assert(TransactionIdIsValid(xid));
    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    t_thrd.xact_cxt.MyLockedGxact->locking_backend = InvalidBackendId;
    TWOPAHSE_LWLOCK_RELEASE(xid);

    t_thrd.xact_cxt.MyLockedGxact = NULL;
}

/*
 * MarkAsPreparing
 *		Reserve the GID for the given transaction.
 */
GlobalTransaction MarkAsPreparing(GTM_TransactionHandle handle, TransactionId xid, const char *gid,
                                  TimestampTz prepared_at, Oid owner, Oid databaseid, uint64 sessionid)
{
    GlobalTransaction gxact;
    int i;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    if (strlen(gid) >= GIDSIZE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("transaction identifier \"%s\" is too long", gid)));
    }

    /* fail immediately if feature is disabled */
    if (g_instance.attr.attr_storage.max_prepared_xacts == 0) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("prepared transactions are disabled"),
                        errhint("Set max_prepared_transactions to a nonzero value.")));
    }

    /* on first call, register the exit hook */
    if (!t_thrd.xact_cxt.twophaseExitRegistered) {
        on_shmem_exit(AtProcExit_Twophase, 0);
        t_thrd.xact_cxt.twophaseExitRegistered = true;
    }

    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    /* Check for conflicting GID */
    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        gxact = currentStatePtr->prepXacts[i];
        if (strcmp(gxact->gid, gid) == 0) {
            TWOPAHSE_LWLOCK_RELEASE(xid);
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("transaction identifier \"%s\" is already in use", gid)));
        }
    }

    /* Get a free gxact from the freelist */
    if (currentStatePtr->freeGXacts == NULL) {
        TWOPAHSE_LWLOCK_RELEASE(xid);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("maximum number of prepared transactions reached"),
                        errhint("Increase max_prepared_transactions (currently %d).",
                                g_instance.attr.attr_storage.max_prepared_xacts)));
    }
    gxact = currentStatePtr->freeGXacts;
    currentStatePtr->freeGXacts = gxact->next;

    MarkAsPreparingGuts(handle, gxact, xid, gid, prepared_at, owner, databaseid, sessionid);

    gxact->ondisk = false;

    /* And insert it into the active array */
    Assert(currentStatePtr->numPrepXacts < g_instance.attr.attr_storage.max_prepared_xacts);
    currentStatePtr->prepXacts[currentStatePtr->numPrepXacts++] = gxact;

    TWOPAHSE_LWLOCK_RELEASE(xid);

    return gxact;
}

/*
 * MarkAsPreparingGuts
 *
 * This uses a gxact struct and puts it into the active array.
 * NOTE: this is also used when reloading a gxact after a crash; so avoid
 * assuming that we can use very much backend context.
 *
 * Note: This function should be called with appropriate locks held.
 */
static void MarkAsPreparingGuts(GTM_TransactionHandle handle, GlobalTransaction gxact, TransactionId xid,
                                const char *gid, TimestampTz prepared_at, Oid owner, Oid databaseid, uint64 sessionid)
{
    PGPROC *proc = NULL;
    PGXACT *pgxact = NULL;
    int i;
    errno_t rc = 0;

    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(TwoPhaseStateMappingPartitionLock(xid)));

    Assert(gxact != NULL);
    proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];

    /* Reset the sub xids' memory in the last dummy proc */
    if (proc->subxids.maxNumber > 0) {
        proc->subxids.maxNumber = 0;
        pfree(proc->subxids.xids);
        proc->subxids.xids = NULL;
    }

    /* backup and restore LWLock pointers */
    LWLock *bakBackendLock = proc->backendLock;
    LWLock *bakSubxidsLock = proc->subxidsLock;

    /* Initialize the PGPROC entry */
    rc = memset_s(proc, sizeof(PGPROC), 0, sizeof(PGPROC));
    securec_check_c(rc, "", "");

    proc->backendLock = bakBackendLock;
    proc->subxidsLock = bakSubxidsLock;

    proc->pgprocno = gxact->pgprocno;
    SHMQueueElemInit(&(proc->links));
    proc->waitStatus = STATUS_OK;
    /* We set up the gxact's VXID as InvalidBackendId/XID */
    proc->lxid = (LocalTransactionId)xid;
    pgxact->handle = handle;
    pgxact->xid = xid;
    pgxact->xmin = InvalidTransactionId;
    proc->snapXmax = InvalidTransactionId;
    proc->snapCSN = InvalidCommitSeqNo;
    pgxact->csn_min = InvalidCommitSeqNo;
    pgxact->csn_dr = InvalidCommitSeqNo;
    pgxact->delayChkpt = false;
    pgxact->vacuumFlags = 0;
    proc->pid = 0;
    proc->backendId = InvalidBackendId;
    proc->databaseId = databaseid;
    proc->roleId = owner;
    proc->sessionid = sessionid;    /* record it as blocking session id */
    proc->lwWaiting = false;
    proc->lwWaitMode = 0;
    proc->waitLock = NULL;
    proc->waitProcLock = NULL;
    proc->blockProcLock = NULL;
    for (i = 0; i < NUM_LOCK_PARTITIONS; i++) {
        SHMQueueInit(&(proc->myProcLocks[i]));
    }
    pgxact->nxids = 0;

    gxact->prepared_at = prepared_at;
    gxact->xid = xid;
    gxact->owner = owner;
    gxact->locking_backend = AmStartupProcess() ? InvalidBackendId : t_thrd.proc_cxt.MyBackendId;
    gxact->valid = false;
    gxact->inredo = false;
    rc = strcpy_s(gxact->gid, GIDSIZE, gid);
    securec_check_c(rc, "", "");

    /*
     * Remember that we have this GlobalTransaction entry locked for us.
     * If we abort after this, we must release it.
     */
    t_thrd.xact_cxt.MyLockedGxact = gxact;
}

/*
 * GXactLoadSubxactData
 *
 * If the transaction being persisted had any subtransactions, this must
 * be called before MarkAsPrepared() to load information into the dummy
 * PGPROC.
 */
static void GXactLoadSubxactData(GlobalTransaction gxact, int nsubxacts, TransactionId *children)
{
    PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];

    /* We need no extra lock since the GXACT isn't valid yet */
    if (nsubxacts > 0) {
        errno_t retno;
        MemoryContext oldContext;

        /* Allocate or realloc memory if needed. */
        Assert(ProcSubXidCacheContext);
        HOLD_INTERRUPTS();
        oldContext = MemoryContextSwitchTo(ProcSubXidCacheContext);
        if (proc->subxids.maxNumber == 0) {
            /* Init memory */
            int maxNumber = Max(nsubxacts, PGPROC_INIT_CACHED_SUBXIDS);
            proc->subxids.xids = (TransactionId *)palloc(sizeof(TransactionId) * maxNumber);
            proc->subxids.maxNumber = maxNumber;
        } else if (nsubxacts >= proc->subxids.maxNumber) {
            /* Realloc */
            int maxNumber = Max(nsubxacts, proc->subxids.maxNumber * 2);
            proc->subxids.xids = (TransactionId *)repalloc(proc->subxids.xids, sizeof(TransactionId) * maxNumber);
            proc->subxids.maxNumber = maxNumber;
        }

        MemoryContextSwitchTo(oldContext);
        RESUME_INTERRUPTS();

        retno = memcpy_s(proc->subxids.xids, nsubxacts * sizeof(TransactionId), children,
                         nsubxacts * sizeof(TransactionId));
        securec_check(retno, "\0", "\0");

        pgxact->nxids = nsubxacts;
    }
}

/*
 * MarkAsPrepared
 *		Mark the GXACT as fully valid, and enter it into the global ProcArray.
 *
 * lock_held indicates whether caller already holds TwoPhaseStateLock.
 */
static void MarkAsPrepared(GlobalTransaction gxact, bool lock_held)
{
    TransactionId xid = gxact->xid;
    Assert(TransactionIdIsValid(xid));
    /* Lock here may be overkill, but I'm not convinced of that ... */
    if (!lock_held) {
        TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    }
    Assert(!gxact->valid);
    gxact->valid = true;

    if (GTM_LITE_MODE) {
        MarkPrepXidValid(gxact);
    }
    if (!lock_held) {
        TWOPAHSE_LWLOCK_RELEASE(xid);
    }

    /* for left two phase transaction, need to sync when satisfyNow */
    g_instance.proc_base_all_xacts[gxact->pgprocno].needToSyncXid |= SNAPSHOT_NOW_NEED_SYNC;

    /*
     * Put it into the global ProcArray so TransactionIdIsInProgress considers
     * the XID as still running.
     */
    ProcArrayAdd(g_instance.proc_base_all_procs[gxact->pgprocno]);
}

/*
 * LockGXact
 *		Locate the prepared transaction and mark it busy for COMMIT or PREPARE.
 */
static GlobalTransaction LockGXact(const char *gid, Oid user)
{
    int i = 0;
    int j = 0;

    /* on first call, register the exit hook */
    if (!t_thrd.xact_cxt.twophaseExitRegistered) {
        on_shmem_exit(AtProcExit_Twophase, 0);
        t_thrd.xact_cxt.twophaseExitRegistered = true;
    }
    /* loop each twophase state to find one */
    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
retry:
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_EXCLUSIVE);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];
            PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];

            /* Ignore not-yet-valid GIDs */
            if (!gxact->valid || strcmp(gxact->gid, gid) != 0) {
                continue;
            }

            /* Found it, but has someone else got it locked? */
            if (gxact->locking_backend != InvalidBackendId) {
                TWOPAHSE_LWLOCK_RELEASE(j);
                if (!u_sess->attr.attr_common.xc_maintenance_mode) {
                    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                                    errmsg("prepared transaction with identifier \"%s\" is busy", gid)));
                } else {
                    goto retry;
                }
            }
            /* Database Security:  Support separation of privilege. */
            if (user != gxact->owner && !systemDBA_arg(user)) {
                TWOPAHSE_LWLOCK_RELEASE(j);
                ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("permission denied to finish prepared transaction"),
                                errhint("Must be system admin or the user that prepared the transaction.")));
            }

            /*
             * Note: it probably would be possible to allow committing from
             * another database; but at the moment NOTIFY is known not to work and
             * there may be some other issues as well.	Hence disallow until
             * someone gets motivated to make it work.
             */
            if (u_sess->proc_cxt.MyDatabaseId != proc->databaseId) {
                TWOPAHSE_LWLOCK_RELEASE(j);
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("prepared transaction belongs to another database"),
                        errhint("Connect to the database where the transaction was prepared to finish it.")));
            }

            /* OK for me to lock it */
            gxact->locking_backend = t_thrd.proc_cxt.MyBackendId;
            t_thrd.xact_cxt.MyLockedGxact = gxact;

            TWOPAHSE_LWLOCK_RELEASE(j);

            return gxact;
        }

        TWOPAHSE_LWLOCK_RELEASE(j);
    }
#ifdef PGXC
    /*
     * In PGXC, if u_sess->attr.attr_common.xc_maintenance_mode is on, COMMIT/ROLLBACK PREPARED may be issued to the
     * node where the given xid does not exist.
     */
    if (!u_sess->attr.attr_common.xc_maintenance_mode) {
#endif
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("prepared transaction with identifier \"%s\" does not exist", gid)));
#ifdef PGXC
    }
#endif

    /* NOTREACHED */
    return NULL;
}

/*
 * RemoveGXact
 *		Remove the prepared transaction from the shared memory array.
 *
 * NB: caller should have already removed it from ProcArray
 */
static void RemoveGXact(GlobalTransaction gxact)
{
    int i = 0;
    TransactionId xid = gxact->xid;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);

    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(TwoPhaseStateMappingPartitionLock(xid)));

    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        if (gxact == currentStatePtr->prepXacts[i]) {
            /* remove from the active array */
            currentStatePtr->numPrepXacts--;
            currentStatePtr->prepXacts[i] =
                currentStatePtr->prepXacts[currentStatePtr->numPrepXacts];

            /* and put it back in the freelist */
            gxact->next = currentStatePtr->freeGXacts;
            currentStatePtr->freeGXacts = gxact;
            return;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("failed to find transaction in GlobalTransaction array")));
}

/*
 * TransactionIdIsPrepared
 *		True iff transaction associated with the identifier is prepared
 *		for two-phase commit
 *
 * Note: only gxacts marked "valid" are considered; but notice we do not
 * check the locking status.
 *
 */
bool TransactionIdIsPrepared(TransactionId xid)
{
    bool result = false;
    int i = 0;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);

    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_SHARED);

    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        GlobalTransaction gxact = currentStatePtr->prepXacts[i];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];

        if (gxact->valid && pgxact->xid == xid) {
            result = true;
            break;
        }
    }

    TWOPAHSE_LWLOCK_RELEASE(xid);

    return result;
}

/* returns the smallest power of 2 that is greater than a 32-bit integer n */
static int nextPowerOf2(int n)
{
    if (n <= 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Invalid prepared list lenth: %d", n)));
    }
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    return n;
}

/* or a num refer to max_prepared_xacts_num */
const int defualt_prepared_num = 128;
int get_snapshot_defualt_prepared_num(void)
{
    return defualt_prepared_num;
}

static void AllocPrepareListMemory(Snapshot snapshot, int numPrepXids, ValidPrepXid prepXid)
{
    TransactionId *oldPtr = NULL;
    int oldCount = 0;
    errno_t rc = EOK;

    /*
     * since we use static snapshot, don't palloc each time,
     * just free if it exceed defalut bound 128, see AtEOXact_Snapshot
     */
    MemoryContext oldContext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    int upper_bound = nextPowerOf2(numPrepXids);
    int newCapacity = (upper_bound > defualt_prepared_num) ? upper_bound : defualt_prepared_num;
    if (newCapacity > snapshot->prepared_array_capacity) {
        oldPtr = snapshot->prepared_array;
        oldCount = snapshot->prepared_count;
        snapshot->prepared_array = NULL;
        snapshot->prepared_array_capacity = 0;
        snapshot->prepared_count = 0;
    }
    if (snapshot->prepared_array == NULL) {
        snapshot->prepared_array = (TransactionId *)palloc0_noexcept(sizeof(TransactionId) * newCapacity);
        if (snapshot->prepared_array == NULL) {
            pfree_ext(oldPtr);
            if (prepXid != NULL) {
                ReleasePrepXid(prepXid);
            }
            MemoryContextSwitchTo(oldContext);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
        snapshot->prepared_array_capacity = newCapacity;
        /* restore old xid list */
        if (oldPtr != NULL && oldCount > 0) {
            Size oldSize = mul_size(oldCount, sizeof(TransactionId));
            rc = memcpy_s(snapshot->prepared_array, oldSize, oldPtr, oldSize);
            if (rc != EOK && prepXid != NULL) {
                /* release refcount before ereport */
                ReleasePrepXid(prepXid);
            }
            securec_check(rc, "", "");
            snapshot->prepared_count = oldCount;
            oldCount = 0;
        }
        pfree_ext(oldPtr);
    }
    MemoryContextSwitchTo(oldContext);
    return;
}

/*
 * SetLocalSnapshotPreparedArray
 *          At the time of snapshot, get prepared list.
 */
void SetLocalSnapshotPreparedArray(Snapshot snapshot)
{
    /* reset snapshot preapred count */
    snapshot->prepared_count = 0;
    /* for abort transaction, no need to get prepared array */
    if (t_thrd.xact_cxt.bInAbortTransaction) {
        return;
    }
    int i = 0;
    int numTotalPrepXids = 0;
    errno_t rc = EOK;
    bool isOverflow = false;
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(i);
        ValidPrepXid prepXid = GetCurrPrepXid(currentStatePtr);
        int numPrepXid = prepXid->numPrepXid;
        if (numPrepXid == 0) {
            ReleasePrepXid(prepXid);
            continue;
        }
        isOverflow = prepXid->isOverflow;
        TransactionId *prepared_xid_list = NULL;
        /* whether the prepare_list is overflow or not */
        if (isOverflow) {
            /* if the prepared list is overflow, then try to get the two phase state */
            ReleasePrepXid(prepXid);
            ereport(DEBUG1, (errmsg("prepare list is overflow because of too many sub transactions")));
            numPrepXid = CopyPreparedTransactionList(&prepared_xid_list, i);
            if (numPrepXid == 0) {
                continue;
            }
        }

        AllocPrepareListMemory(snapshot, numTotalPrepXids + numPrepXid, isOverflow ? NULL : prepXid);

        Size size = mul_size(numPrepXid, sizeof(TransactionId));
        if (!isOverflow) {     
            rc = memcpy_s(snapshot->prepared_array + numTotalPrepXids, size, prepXid->validPrepXid, size);
            if (rc != EOK) {
                /* release refcount before ereport */
                ReleasePrepXid(prepXid);
            }
        } else {
            rc = memcpy_s(snapshot->prepared_array + numTotalPrepXids, size, prepared_xid_list, size);
            pfree(prepared_xid_list);
        }
        securec_check(rc, "", "");

        /* update prepare list count */
        numTotalPrepXids += numPrepXid;
        snapshot->prepared_count = numTotalPrepXids;
        if (!isOverflow) {
            ReleasePrepXid(prepXid);
        }
    }
}

/* Called only in StartupXLog, LWLock is not needed. */
int GetPendingXactCount(void)
{
    int i;
    int res = 0;
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(i);
        res += currentStatePtr->numPrepXacts;
    }
    return res;
}

/*
 * Returns an array of all prepared transactions for the user-level
 * function pg_prepared_xact.
 *
 * The returned array and all its elements are copies of internal data
 * structures, to minimize the time we need to hold the TwoPhaseStateLock.
 *
 * WARNING -- we return even those transactions that are not fully prepared
 * yet.  The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
static int GetPreparedTransactionList(GlobalTransaction *gxacts)
{
    GlobalTransaction array = NULL;
    int num = 0;
    int indexNum = 0;
    int i = 0;
    errno_t rc = 0;
    int j = 0;

    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_SHARED);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        num += currentStatePtr->numPrepXacts;
    }

    if (num > 0) {
        array = (GlobalTransaction)palloc(sizeof(GlobalTransactionData) * num);
        *gxacts = array;

        for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
            TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
            for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
                rc = memcpy_s(array + indexNum + i, sizeof(GlobalTransactionData),
                    currentStatePtr->prepXacts[i], sizeof(GlobalTransactionData));
                securec_check(rc, "", "");
            }
            indexNum += currentStatePtr->numPrepXacts;
        }
    }

    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_RELEASE(j);
    }

    if (num == 0) {
        *gxacts = NULL;
    }
    return num;
}

/*
 * Returns an array of all prepared xid list (include sub transactions) for the user-level
 * function pg_prepared_xact.
 *
 * The returned array and all its elements are copies of internal data
 * structures, to minimize the time we need to hold the TwoPhaseStateLock.
 *
 * WARNING -- we return even those transactions that are not fully prepared
 * yet.  The caller should filter them out if he doesn't want them.
 *
 * The returned array is palloc'd.
 */
static int CopyPreparedTransactionList(TransactionId **prepared_xid_list, int partId)
{
    TransactionId *array;
    int num = 0;
    int i = 0;
    errno_t rc = 0;
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(partId);
    TWOPAHSE_LWLOCK_ACQUIRE(partId, LW_SHARED);
    if (currentStatePtr->numPrepXacts == 0) {
        TWOPAHSE_LWLOCK_RELEASE(partId);
        *prepared_xid_list = NULL;
        return 0;
    }
    num = currentStatePtr->numPrepXacts;
    int nxids = num;
    for (i = 0; i < num; i++) {
        GlobalTransaction gxact = currentStatePtr->prepXacts[i];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
        /* if the trx has been marked 'invalid',  skip it */
        if (!gxact->valid) {
            continue;
        }
        nxids += pgxact->nxids;
    }
    Size size = mul_size(nxids, sizeof(TransactionId));
    array = (TransactionId *)palloc0(size);
    *prepared_xid_list = array;
    int index = 0;
    nxids = num;
    for (i = 0; i < num; i++) {
        GlobalTransaction gxact = currentStatePtr->prepXacts[i];
        PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
        /* if the trx has been marked 'invalid',  skip it */
        if (!gxact->valid) {
            continue;
        }
        *(array + index) = gxact->xid;
        index++;
        if (pgxact->nxids > 0) {
            LWLockAcquire(proc->subxidsLock, LW_SHARED);
            if (proc->subxids.xids != NULL) {
                Size subXidSize = mul_size(pgxact->nxids, sizeof(TransactionId));
                rc = memcpy_s(array + index, subXidSize, proc->subxids.xids, subXidSize);
                securec_check(rc, "", "");
                index += pgxact->nxids;
                nxids += pgxact->nxids;
            }
            LWLockRelease(proc->subxidsLock);
        }
    }
    TWOPAHSE_LWLOCK_RELEASE(partId);
    return nxids;
}

/* Working status for pg_prepared_xact */
typedef struct {
    GlobalTransaction array;
    int ngxacts;
    int currIdx;
} Working_State;

/*
 * get_prepared_pending_xid
 *
 * This function is return the nextxid when recovery done
 */
Datum get_prepared_pending_xid(PG_FUNCTION_ARGS)
{
#define MAX_XID_LEN 128
    char xid_need_recovery[MAX_XID_LEN] = {0};

    TransactionId global_2pc_xmin = GetGlobal2pcXmin();
    errno_t rc = EOK;

    rc = snprintf_s(xid_need_recovery, MAX_XID_LEN, MAX_XID_LEN - 1, XID_FMT, global_2pc_xmin);
    securec_check_ss(rc, "", "");

    PG_RETURN_TEXT_P(cstring_to_text(xid_need_recovery));
}

/*
 * build tupdesc for result tuples used in pg_prepared_xact and get_local_prepared_xact
 */
static void build_prepared_xact_tuple_desc(FuncCallContext *funcctx, bool with_node_name)
{
    TupleDesc tupdesc;

    if (!with_node_name) {
        tupdesc = CreateTemplateTupleDesc(5, false);
    } else {
        tupdesc = CreateTemplateTupleDesc(6, false);
    }
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "transaction", XIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "gid", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "prepared", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "ownerid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "dbid", OIDOID, -1, 0);
    if (with_node_name) {
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "node_name", TEXTOID, -1, 0);
    }

    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
}

/*
 * pg_prepared_xact
 *		Produce a view with one row per prepared transaction.
 *
 * This function is here so we don't have to export the
 * GlobalTransactionData struct definition.
 */
Datum pg_prepared_xact(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    Working_State *status = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pg_prepared_xacts view in system_views.sql */
        build_prepared_xact_tuple_desc(funcctx, false);

        /*
         * Collect all the 2PC status information that we will format and send
         * out as a result set.
         */
        status = (Working_State *)palloc(sizeof(Working_State));
        funcctx->user_fctx = (void *)status;

        status->ngxacts = GetPreparedTransactionList(&status->array);
        status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (Working_State *)funcctx->user_fctx;

    while (status->array != NULL && status->currIdx < status->ngxacts) {
        GlobalTransaction gxact = &status->array[status->currIdx++];
        PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
        Datum values[5];
        bool nulls[5];
        HeapTuple tuple;
        Datum result;
        errno_t rc = EOK;

        if (!gxact->valid) {
            continue;
        }

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "", "");

        values[0] = TransactionIdGetDatum(pgxact->xid);
        values[1] = CStringGetTextDatum(gxact->gid);
        values[2] = TimestampTzGetDatum(gxact->prepared_at);
        values[3] = ObjectIdGetDatum(gxact->owner);
        values[4] = ObjectIdGetDatum(proc->databaseId);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

static Datum get_my_node_name()
{
    if (g_instance.attr.attr_common.PGXCNodeName != NULL && g_instance.attr.attr_common.PGXCNodeName[0] != '\0') {
        return CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    } else {
        return CStringGetTextDatum("not define");
    }
}

/*
 * pg_prepared_xact with node_name, in case that
 * drop pg_prepared_xact and not create the new one during
 * upgrading, gs_clean cannot work and cannot clean up
 * two phase state, so we use version 2 here.
 */
Datum get_local_prepared_xact(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    Working_State *status = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        build_prepared_xact_tuple_desc(funcctx, true);

        /*
         * Collect all the 2PC status information that we will format and send
         * out as a result set.
         */
        status = (Working_State *)palloc(sizeof(Working_State));
        funcctx->user_fctx = (void *)status;

        status->ngxacts = GetPreparedTransactionList(&status->array);
        status->currIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (Working_State *)funcctx->user_fctx;

    while (status->array != NULL && status->currIdx < status->ngxacts) {
        GlobalTransaction gxact = &status->array[status->currIdx++];
        PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
        Datum values[6];
        bool nulls[6];
        HeapTuple tuple;
        Datum result;
        errno_t rc = EOK;

        if (!gxact->valid) {
            continue;
        }

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "", "");

        values[0] = TransactionIdGetDatum(pgxact->xid);
        values[1] = CStringGetTextDatum(gxact->gid);
        values[2] = TimestampTzGetDatum(gxact->prepared_at);
        values[3] = ObjectIdGetDatum(gxact->owner);
        values[4] = ObjectIdGetDatum(proc->databaseId);
        values[5] = get_my_node_name();

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

#ifdef ENABLE_MULTIPLE_NODES
static TableDistributionInfo *fetch_remote_prepared_xacts(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo *distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo *)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
                     "select                                                         "
                     "p.transaction, p.gid, p.prepared, u.rolname AS owner,          "
                     "d.datname AS database, p.node_name                             "
                     "FROM get_local_prepared_xact() p                               "
                     "LEFT JOIN pg_authid u ON p.ownerid = u.oid                     "
                     "LEFT JOIN pg_database d ON p.dbid = d.oid                      ");
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}
#endif

Datum get_remote_prepared_xacts(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    FuncCallContext *funcctx = NULL;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported view in single node mode.")));

    SRF_RETURN_DONE(funcctx);
#else
    FuncCallContext *funcctx = NULL;
    Datum values[6];
    bool nulls[6] = { false, false, false, false, false, false };
    HeapTuple tuple;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples. */
        TupleDesc tupdesc = CreateTemplateTupleDesc(6, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "transaction", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "gid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "prepared", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "owner", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "database", NAMEOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "node_name", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->max_calls = u_sess->pgxc_cxt.NumCoords + u_sess->pgxc_cxt.NumDataNodes;

        /* the main call for get table distribution. */
        funcctx->user_fctx = fetch_remote_prepared_xacts(funcctx->tuple_desc);

        MemoryContextSwitchTo(oldcontext);

        if (funcctx->user_fctx == NULL) {
            SRF_RETURN_DONE(funcctx);
        }
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->user_fctx != NULL) {
        Tuplestorestate *tupstore = ((TableDistributionInfo *)funcctx->user_fctx)->state->tupstore;
        TupleTableSlot *slot = ((TableDistributionInfo *)funcctx->user_fctx)->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(((TableDistributionInfo *)funcctx->user_fctx)->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(funcctx->user_fctx);
            SRF_RETURN_DONE(funcctx);
        }
        for (int i = 0; i < 6; i++) {
            values[i] = tableam_tslot_getattr(slot, (i + 1), &nulls[i]);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        ExecClearTuple(slot);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
#endif
}

/* Working status for pg_parse_clog */
typedef struct {
    uint32 cur_index;
    uint32 nxid;
} ParseClog_State;

/*
 * pg_parse_clog
 *		parse the clog to get the status of xid.
 *
 * This function is here so we don't have to export the
 * GlobalTransactionData struct definition.
 */
Datum pg_parse_clog(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx = NULL;
    ParseClog_State *status = NULL;

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
        tupdesc = CreateTemplateTupleDesc(2, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "xid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "status", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Collect all the 2PC status information that we will format and send
         * out as a result set.
         */
        status = (ParseClog_State *)palloc(sizeof(ParseClog_State));
        funcctx->user_fctx = (void *)status;

        status->nxid = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
        status->cur_index = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    status = (ParseClog_State *)funcctx->user_fctx;

    while (status->cur_index < status->nxid) {
        char *TransactionStatusIndex[] = { "INPROGRESS", "COMMITTED", "ABORTED", "SUBCOMMITTED" };
        XLogRecPtr xidlsn;
        Datum values[2];
        bool nulls[2];
        HeapTuple tuple;
        Datum result;
        int CurXidStatus;
        errno_t rc;

        CurXidStatus = CLogGetStatus(status->cur_index, &xidlsn);

        if (CurXidStatus > 3 || CurXidStatus < 0) {
            continue;
        }

        /*
         * Form tuple with appropriate data.
         */
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check_c(rc, "", "");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check_c(rc, "", "");

        values[0] = TransactionIdGetDatum(status->cur_index);
        values[1] = CStringGetTextDatum(TransactionStatusIndex[CurXidStatus]);

        status->cur_index++;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * TwoPhaseGetGXact
 *		Get the GlobalTransaction struct for a prepared transaction
 *		specified by XID
 */
static GlobalTransaction TwoPhaseGetGXact(TransactionId xid)
{
    GlobalTransaction result = NULL;
    int i;

    /*
     * During a recovery, COMMIT PREPARED, or ABORT PREPARED, we'll be called
     * repeatedly for the same XID.  We can save work with a simple cache.
     */
    if (xid == t_thrd.xact_cxt.cached_xid) {
        return t_thrd.xact_cxt.cached_gxact;
    }

    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_SHARED);

    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        GlobalTransaction gxact = currentStatePtr->prepXacts[i];
        PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];

        if (pgxact->xid == xid) {
            result = gxact;
            break;
        }
    }

    TWOPAHSE_LWLOCK_RELEASE(xid);

    if (result == NULL) { /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("failed to find GlobalTransaction for xid %lu", xid)));
    }

    t_thrd.xact_cxt.cached_xid = xid;
    t_thrd.xact_cxt.cached_gxact = result;

    return result;
}

/*
 * TwoPhaseGetDummyBackendId
 *		Get the dummy backend ID for prepared transaction specified by XID
 *
 * Dummy backend IDs are similar to real backend IDs of real backends.
 * They start at g_instance.shmem_cxt.MaxBackends + 1, and are unique across all currently active
 * real backends and prepared transactions.
 */
BackendId TwoPhaseGetDummyBackendId(TransactionId xid)
{
    GlobalTransaction gxact = TwoPhaseGetGXact(xid);

    return gxact->dummyBackendId;
}

/*
 * TwoPhaseGetDummyProc
 *		Get the PGPROC that represents a prepared transaction specified by XID
 */
PGPROC *TwoPhaseGetDummyProc(TransactionId xid)
{
    GlobalTransaction gxact = TwoPhaseGetGXact(xid);

    return g_instance.proc_base_all_procs[gxact->pgprocno];
}

/************************************************************************/
/* State file support													 */
/************************************************************************/

/*
 * Header for each record in a state file
 *
 * NOTE: len counts only the rmgr data, not the TwoPhaseRecordOnDisk header.
 * The rmgr data will be stored starting on a MAXALIGN boundary.
 */
typedef struct TwoPhaseRecordOnDisk {
    uint32 len;          /* length of rmgr data */
    TwoPhaseRmgrId rmid; /* resource manager for this record */
    uint16 info;         /* flag bits for use by rmgr */
} TwoPhaseRecordOnDisk;

/*
 * Append a block of data to records data structure.
 *
 * NB: each block is padded to a MAXALIGN multiple.  This must be
 * accounted for when the file is later read!
 *
 * The data is copied, so the caller is free to modify it afterwards.
 */
static void save_state_data(const void *data, uint32 len)
{
    uint32 padlen = MAXALIGN(len);
    errno_t rc = EOK;

    if (padlen > t_thrd.xact_cxt.records.bytes_free) {
        t_thrd.xact_cxt.records.tail->next = (StateFileChunk *)palloc0(sizeof(StateFileChunk));
        t_thrd.xact_cxt.records.tail = t_thrd.xact_cxt.records.tail->next;
        t_thrd.xact_cxt.records.tail->len = 0;
        t_thrd.xact_cxt.records.tail->next = NULL;
        t_thrd.xact_cxt.records.num_chunks++;

        t_thrd.xact_cxt.records.bytes_free = Max(padlen, 512);
        t_thrd.xact_cxt.records.tail->data = (char *)palloc(t_thrd.xact_cxt.records.bytes_free);
    }

    rc = memcpy_s(((char *)t_thrd.xact_cxt.records.tail->data) + t_thrd.xact_cxt.records.tail->len,
                  t_thrd.xact_cxt.records.bytes_free, data, len);
    securec_check(rc, "\0", "\0");
    t_thrd.xact_cxt.records.tail->len += padlen;
    t_thrd.xact_cxt.records.bytes_free -= padlen;
    t_thrd.xact_cxt.records.total_len += padlen;
}

/*
 * Start preparing a state file.
 *
 * Initializes data structure and inserts the 2PC file header record.
 */
void StartPrepare(GlobalTransaction gxact)
{
    PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
    TransactionId xid = pgxact->xid;
    TwoPhaseFileHeaderNew hdr_new;
    TransactionId *children = NULL;
    ColFileNode *commitrels = NULL;
    ColFileNode *abortrels = NULL;
    char *commitLibrary = NULL;
    char *abortLibrary = NULL;
    int commitLibraryLen = 0;
    int abortLibraryLen = 0;
    errno_t errorno = EOK;
    SharedInvalidationMessage *invalmsgs = NULL;

    /* Initialize linked list */
    t_thrd.xact_cxt.records.head = (StateFileChunk *)palloc0(sizeof(StateFileChunk));
    t_thrd.xact_cxt.records.head->len = 0;
    t_thrd.xact_cxt.records.head->next = NULL;

    t_thrd.xact_cxt.records.bytes_free = Max(sizeof(TwoPhaseFileHeaderNew), 512);
    t_thrd.xact_cxt.records.head->data = (char *)palloc(t_thrd.xact_cxt.records.bytes_free);

    t_thrd.xact_cxt.records.tail = t_thrd.xact_cxt.records.head;
    t_thrd.xact_cxt.records.num_chunks = 1;

    t_thrd.xact_cxt.records.total_len = 0;

    hdr_new.ncommitrels_temp = 0;
    hdr_new.nabortrels_temp = 0;

    hdr_new.hdr.total_len = 0; /* EndPrepare will fill this in */
    hdr_new.hdr.xid = xid;
    hdr_new.hdr.database = proc->databaseId;
    hdr_new.hdr.prepared_at = gxact->prepared_at;
    hdr_new.hdr.owner = gxact->owner;
    hdr_new.hdr.nsubxacts  = xactGetCommittedChildren(&children);
    hdr_new.hdr.ncommitrels = smgrGetPendingDeletes(true, &commitrels, false, &hdr_new.ncommitrels_temp);
    hdr_new.hdr.nabortrels = smgrGetPendingDeletes(false, &abortrels, false, &hdr_new.nabortrels_temp);
    hdr_new.hdr.ninvalmsgs = xactGetCommittedInvalidationMessages(&invalmsgs, &hdr_new.hdr.initfileinval);
    hdr_new.hdr.ncommitlibrarys = libraryGetPendingDeletes(true, &commitLibrary, &commitLibraryLen);
    hdr_new.hdr.nabortlibrarys = libraryGetPendingDeletes(false, &abortLibrary, &abortLibraryLen);

    errorno = strncpy_s(hdr_new.hdr.gid, GIDSIZE, gxact->gid, GIDSIZE - 1);
    securec_check(errorno, "", "");

    /* Create header */
    if (t_thrd.proc->workingVersionNum >= PAGE_COMPRESSION_VERSION) {
        hdr_new.hdr.magic = TWOPHASE_MAGIC_COMPRESSION;
        save_state_data(&hdr_new, sizeof(TwoPhaseFileHeaderNew));
    } else if (t_thrd.proc->workingVersionNum >= TWOPHASE_FILE_VERSION) {
        hdr_new.hdr.magic = TWOPHASE_MAGIC_NEW;
        save_state_data(&hdr_new, sizeof(TwoPhaseFileHeaderNew));
    } else {
        hdr_new.hdr.magic = TWOPHASE_MAGIC;
        save_state_data(&hdr_new.hdr, sizeof(TwoPhaseFileHeader));
    }

    /*
     * Add the additional info about subxacts, deletable files and cache
     * invalidation messages.
     */
    if (hdr_new.hdr.nsubxacts > 0) {
        save_state_data(children, hdr_new.hdr.nsubxacts * sizeof(TransactionId));
        /* While we have the child-xact data, stuff it in the gxact too */
        GXactLoadSubxactData(gxact, hdr_new.hdr.nsubxacts, children);
    }
    if (hdr_new.hdr.ncommitrels > 0) {
        void *registerData = commitrels;
        uint32 size = (uint32)(hdr_new.hdr.ncommitrels * sizeof(ColFileNode));
        if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
            /* commitrels will be free in ConvertToOldColFileNode */
            registerData = (void *)ConvertToOldColFileNode(commitrels, hdr_new.hdr.ncommitrels);
            size = hdr_new.hdr.ncommitrels * sizeof(ColFileNodeRel);
        }
        save_state_data(registerData, size);
        pfree(registerData);
        commitrels = NULL;
    }
    if (hdr_new.hdr.nabortrels > 0) {
        void *registerData = abortrels;
        uint32 size = (uint32)(hdr_new.hdr.nabortrels * sizeof(ColFileNode));
        if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
            /* commitrels will be free in ConvertToOldColFileNode */
            registerData = (void *)ConvertToOldColFileNode(abortrels, hdr_new.hdr.nabortrels);
            size = hdr_new.hdr.nabortrels * sizeof(ColFileNodeRel);
        }
        save_state_data(registerData, size);
        pfree(registerData);
        abortrels = NULL;
    }
    if (hdr_new.hdr.ninvalmsgs > 0) {
        save_state_data(invalmsgs, hdr_new.hdr.ninvalmsgs * sizeof(SharedInvalidationMessage));
        pfree(invalmsgs);
        invalmsgs = NULL;
    }
    if (hdr_new.hdr.ncommitlibrarys > 0) {
        save_state_data(commitLibrary, commitLibraryLen);
        pfree(commitLibrary);
        commitLibrary = NULL;
    }
    if (hdr_new.hdr.nabortlibrarys > 0) {
        save_state_data(abortLibrary, abortLibraryLen);
        pfree(abortLibrary);
        abortLibrary = NULL;
    }
}

/*
 * Finish preparing state data and writing it to WAL.
 */
void EndPrepare(GlobalTransaction gxact)
{
    TwoPhaseFileHeader *hdr = NULL;
    StateFileChunk *record = NULL;

    /* Add the end sentinel to the list of 2PC records */
    RegisterTwoPhaseRecord(TWOPHASE_RM_END_ID, 0, NULL, 0);

    /* Go back and fill in total_len in the file header record */
    hdr = (TwoPhaseFileHeader *)t_thrd.xact_cxt.records.head->data;
    Assert(hdr->magic == TWOPHASE_MAGIC || hdr->magic == TWOPHASE_MAGIC_NEW ||
               hdr->magic == TWOPHASE_MAGIC_COMPRESSION);
    hdr->total_len = t_thrd.xact_cxt.records.total_len + sizeof(pg_crc32);

    /*
     * If the data size exceeds MaxAllocSize, we won't be able to read it in
     * ReadTwoPhaseFile. Check for that now, rather than fail in the case
     * where we write data to file and then re-read at commit time.
     */
    if (hdr->total_len > MaxAllocSize) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("two-phase state file maximum length exceeded")));
    }

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(CN_LOCAL_PREPARED_CLOG_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("GTM_TEST: write prepare transaction " XID_FMT " clog failed", hdr->xid)));
    }
#endif
    /* Wait data replicate */
    if (!IsInitdb && !g_instance.attr.attr_storage.enable_mix_replication) {
        if (g_instance.attr.attr_storage.max_wal_senders > 0) {
            DataSndWakeup();
        }

        /* wait for the data synchronization */
        WaitForDataSync();
        Assert(BCMArrayIsEmpty());
    }

    /*
     * Now writing 2PC state data to WAL. We let the WAL's CRC protection
     * cover us, so no need to calculate a separate CRC.
     *
     * We have to set delayChkpt here, too; otherwise a checkpoint starting
     * immediately after the WAL record is inserted could complete without
     * fsync'ing our state file.  (This is essentially the same kind of race
     * condition as the COMMIT-to-clog-write case that RecordTransactionCommit
     * uses delayChkpt for; see notes there.)
     *
     * We save the PREPARE record's location in the gxact for later use by
     * CheckPointTwoPhase.
     */
    XLogEnsureRecordSpace(0, t_thrd.xact_cxt.records.num_chunks);

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(CN_LOCAL_PREPARED_XLOG_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("GTM_TEST: write prepare transaction " XID_FMT " xlog failed", hdr->xid)));
    }

    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, gxact->gid, WHITEBOX_DEFAULT, 0.002)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errcode(ERRCODE_IO_ERROR), errmsg("WHITE_BOX TEST  %s: write prepare transaction xlog failed",
                                                   g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    START_CRIT_SECTION();

    t_thrd.pgxact->delayChkpt = true;

    XLogBeginInsert();
    for (record = t_thrd.xact_cxt.records.head; record != NULL; record = record->next) {
        XLogRegisterData(record->data, record->len);
    }

    gxact->prepare_end_lsn = XLogInsert(RM_XACT_ID, XLOG_XACT_PREPARE);
    XLogWaitFlush(gxact->prepare_end_lsn);

    /* If we crash now, we have prepared: WAL replay will fix things */
    /* Store record's start location to read that later on Commit */
    gxact->prepare_start_lsn = t_thrd.xlog_cxt.ProcLastRecPtr;

    /*
     * Mark the prepared transaction as valid.	As soon as xact.c marks
     * MyPgXact as not running our XID (which it will do immediately after
     * this function returns), others can commit/rollback the xact.
     *
     * NB: a side effect of this is to make a dummy ProcArray entry for the
     * prepared XID.  This must happen before we clear the XID from MyPgXact,
     * else there is a window where the XID is not running according to
     * TransactionIdIsInProgress, and onlookers would be entitled to assume
     * the xact crashed.  Instead we have a window where the same XID appears
     * twice in ProcArray, which is OK.
     */
    MarkAsPrepared(gxact, false);
    t_thrd.xact_cxt.needRemoveTwophaseState = true;

    /*
     * Now we can mark ourselves as out of the commit critical section: a
     * checkpoint starting after this will certainly see the gxact as a
     * candidate for fsyncing.
     */
    t_thrd.pgxact->delayChkpt = false;

    /*
     * Remember that we have this GlobalTransaction entry locked for us.  If
     * we crash after this point, it's too late to abort, but we must unlock
     * it so that the prepared transaction can be committed or rolled back.
     */
    t_thrd.xact_cxt.MyLockedGxact = gxact;

    END_CRIT_SECTION();

    /*
     * Wait for synchronous replication, if required.
     *
     * Note that at this stage we have marked the prepare, but still show as
     * running in the procarray (twice!) and continue to hold locks.
     */
    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(gxact->prepare_end_lsn);
        } else {
#ifndef ENABLE_MULTIPLE_NODES
            if (g_instance.attr.attr_storage.enable_save_confirmed_lsn) {
                t_thrd.proc->syncSetConfirmedLSN = t_thrd.xlog_cxt.ProcLastRecPtr;
            }
#endif
            SyncWaitRet stopWatiRes = SyncRepWaitForLSN(gxact->prepare_end_lsn, false);
#ifdef ENABLE_MULTIPLE_NODES
            /* In distribute prepare phase, repsync failed participant rasie error to coordinator */
            if (stopWatiRes == STOP_WAIT) {
                ereport(ERROR, (errmodule(MODE_REPSYNC), errmsg("Fail to sync prepare transaction to standbys.")));
            }
#endif
            if (module_logging_is_on(MODE_REPSYNC) && IS_PGXC_DATANODE) {
                ereport(LOG, (errmodule(MODE_REPSYNC), errmsg("prepare xid: %lu, gxid: %s, end_lsn: %lu,"
                    " sync_commit_guc: %d, sync_names: %s, stop wait reason: %s",
                    gxact->xid, gxact->gid, gxact->prepare_end_lsn, u_sess->attr.attr_storage.guc_synchronous_commit,
                    (SyncStandbysDefined() ? u_sess->attr.attr_storage.SyncRepStandbyNames : "not defined"),
                    SyncWaitRetDesc[stopWatiRes])));
            }
            g_instance.comm_cxt.localinfo_cxt.set_term = true;
        }
    }

    t_thrd.xact_cxt.records.tail = t_thrd.xact_cxt.records.head = NULL;
    t_thrd.xact_cxt.records.num_chunks = 0;
}

/*
 * Register a 2PC record to be written to state file.
 */
void RegisterTwoPhaseRecord(TwoPhaseRmgrId rmid, uint16 info, const void *data, uint32 len)
{
    TwoPhaseRecordOnDisk record;

    record.rmid = rmid;
    record.info = info;
    record.len = len;
    save_state_data(&record, sizeof(TwoPhaseRecordOnDisk));
    if (len > 0) {
        save_state_data(data, len);
    }
}

/*
 * Read and validate the state file for xid.
 *
 * If it looks OK (has a valid magic number and CRC), return the palloc'd
 * contents of the file.  Otherwise return NULL.
 */
static char *ReadTwoPhaseFile(TransactionId xid, bool give_warnings)
{
    char path[MAXPGPATH];
    char *buf = NULL;
    TwoPhaseFileHeader *hdr = NULL;
    int fd;
    struct stat stat;
    uint32 crc_offset;
    pg_crc32 calc_crc, file_crc;
    errno_t rc;

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X", TWOPHASE_DIR, (uint32)(xid >> 32), (uint32)xid);
    securec_check_ss(rc, "", "");

    fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        if (give_warnings) {
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not open two-phase state file \"%s\": %m", path)));
        }
        return NULL;
    }

    /*
     * Check file length.  We can determine a lower bound pretty easily. We
     * set an upper bound to avoid palloc() failure on a corrupt file, though
     * we can't guarantee that we won't get an out of memory error anyway,
     * even on a valid file.
     */
    if (fstat(fd, &stat)) {
        int save_errno = errno;
        close(fd);
        if (give_warnings) {
            errno = save_errno;
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not stat two-phase state file \"%s\": %m", path)));
        }
        return NULL;
    }

    if (stat.st_size < (long int)(MAXALIGN(sizeof(TwoPhaseFileHeader)) + MAXALIGN(sizeof(TwoPhaseRecordOnDisk)) +
                                  sizeof(pg_crc32)) ||
        stat.st_size > (long int)MaxAllocSize) {
        close(fd);
        return NULL;
    }

    crc_offset = stat.st_size - sizeof(pg_crc32);
    if (crc_offset != MAXALIGN(crc_offset)) {
        close(fd);
        return NULL;
    }

    /*
     * OK, slurp in the file.
     */
    buf = (char *)palloc(stat.st_size);

    pgstat_report_waitevent(WAIT_EVENT_TWOPHASE_FILE_READ);
    if (read(fd, buf, stat.st_size) != stat.st_size) {
        int save_errno = errno;
        close(fd);
        if (give_warnings) {
            errno = save_errno;
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not read two-phase state file \"%s\": %m", path)));
        }
        pfree(buf);
        buf = NULL;
        return NULL;
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    close(fd);

    hdr = (TwoPhaseFileHeader *)buf;
    if ((hdr->magic != TWOPHASE_MAGIC && hdr->magic != TWOPHASE_MAGIC_NEW &&
        hdr->magic != TWOPHASE_MAGIC_COMPRESSION) ||
        hdr->total_len != stat.st_size) {
        pfree(buf);
        buf = NULL;
        return NULL;
    }

    INIT_CRC32(calc_crc);
    COMP_CRC32(calc_crc, buf, crc_offset);
    FIN_CRC32(calc_crc);

    file_crc = *((pg_crc32 *)(buf + crc_offset));

    if (!EQ_CRC32(calc_crc, file_crc)) {
        pfree(buf);
        buf = NULL;
        return NULL;
    }

    return buf;
}
static void CloseTwoPhaseXlogFile()
{
    if (t_thrd.xlog_cxt.sendFile != -1) {
        (void)close(t_thrd.xlog_cxt.sendFile);
        t_thrd.xlog_cxt.sendFile = -1;
    }
}
/*
 * Reads 2PC data from xlog. During checkpoint this data will be moved to
 * twophase files and ReadTwoPhaseFile should be used instead.
 *
 * Note clearly that this function can access WAL during normal operation,
 * similarly to the way WALSender or Logical Decoding would do.
 *
 */
static void XlogReadTwoPhaseData(XLogRecPtr lsn, char **buf, int *len)
{
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = NULL;
    char *errormsg = NULL;
    errno_t rc = 0;

    xlogreader = XLogReaderAllocate(&read_local_xlog_page, NULL);
    if (xlogreader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor.")));
    }

    record = XLogReadRecord(xlogreader, lsn, &errormsg);
    if (record == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not read two-phase state from xlog at %X/%X, errormsg: %s",
                                                   (uint32)(lsn >> 32), (uint32)lsn, errormsg ? errormsg : " ")));
    }

    if (XLogRecGetRmid(xlogreader) != RM_XACT_ID ||
        (XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_XACT_PREPARE) {
        ereport(ERROR,
                (errcode_for_file_access(), errmsg("expected two-phase state data is not present in xlog at %X/%X",
                                                   (uint32)(lsn >> 32), (uint32)lsn)));
    }

    if (len != NULL) {
        *len = XLogRecGetDataLen(xlogreader);
    }

    *buf = (char *)palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
    rc = memcpy_s(*buf, sizeof(char) * XLogRecGetDataLen(xlogreader), XLogRecGetData(xlogreader),
                  sizeof(char) * XLogRecGetDataLen(xlogreader));
    securec_check_c(rc, "", "");

    XLogReaderFree(xlogreader);
}

/*
 * Confirms an xid is prepared, during recovery
 */
bool StandbyTransactionIdIsPrepared(TransactionId xid)
{
    char *buf = NULL;
    TwoPhaseFileHeader *hdr = NULL;
    bool result = false;

    Assert(TransactionIdIsValid(xid));

    if (g_instance.attr.attr_storage.max_prepared_xacts <= 0) {
        return false; /* nothing to do */
    }

    /* Read and validate file */
    buf = ReadTwoPhaseFile(xid, false);
    if (buf == NULL) {
        return false;
    }

    /* Check header also */
    hdr = (TwoPhaseFileHeader *)buf;
    result = TransactionIdEquals(hdr->xid, xid);
    pfree(buf);
    buf = NULL;

    return result;
}

void DropBufferForDelRelinXlogUsingScan(ColFileNode *delrels, int ndelrels)
{
    if (SECUREC_LIKELY(ndelrels <= 0)) {
        return;
    }

    int i;
    int rnode_len = 0;
    RelFileNode rnodes[DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD];
    Assert(ndelrels <= DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD);
    for (i = 0; i < ndelrels; ++i) {
        ColFileNode colFileNode;
        ColFileNode *colFileNodeRel = delrels + i;
        ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        if (!IsValidColForkNum(colFileNode.forknum)&& IsSegmentFileNode(colFileNode.filenode)) {
            rnodes[rnode_len++] = colFileNode.filenode;
        }
    }

    DropRelFileNodeAllBuffersUsingScan(rnodes, rnode_len);
}

void DropBufferForDelRelsinXlogUsingHash(ColFileNode *delrels, int ndelrels)
{
    HTAB *relfilenode_hashtbl = relfilenode_hashtbl_create();

    int enter_cnt = 0;
    bool found = false;
    int i;
    for (i = 0; i < ndelrels; ++i) {
        ColFileNode colFileNode;
        ColFileNode *colFileNodeRel = delrels + i;
        ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        if (!IsValidColForkNum(colFileNode.forknum) && IsSegmentFileNode(colFileNode.filenode)) {
            if (relfilenode_hashtbl != NULL) {
                hash_search(relfilenode_hashtbl, &(colFileNode.filenode), HASH_ENTER, &found);
            }
            if (!found) {
                enter_cnt++;
            }
        }
    }

    /* At least one filenode founded */
    if (enter_cnt > 0) {
        DropRelFileNodeAllBuffersUsingHash(relfilenode_hashtbl);
    }
    hash_destroy(relfilenode_hashtbl);
    relfilenode_hashtbl = NULL;
}

bool relsContainsSegmentTable(ColFileNode *delrels, int ndelrels)
{
    bool found = false;
    for (int i = 0; i < ndelrels; ++i) {
        ColFileNode colFileNode;
        ColFileNode *colFileNodeRel = delrels + i;
        ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        if (!IsValidColForkNum(colFileNode.forknum) && IsSegmentFileNode(colFileNode.filenode)) {
            found = true;
            return found;
        }
    }
    return found;
}

inline void FreeOldColFileNode(bool compression, ColFileNode *commitRels, int32 commitRelCount, ColFileNode *abortRels,
                               int32 abortRelCount)
{
    if (unlikely((long)!compression)) {
        if (commitRelCount > 0 && commitRels != NULL) {
            pfree(commitRels);
        }
        if (abortRelCount > 0 && abortRels != NULL) {
            pfree(abortRels);
        }
    }
}

/*
 * FinishPreparedTransaction: execute COMMIT PREPARED or ROLLBACK PREPARED
 */
void FinishPreparedTransaction(const char *gid, bool isCommit)
{
    TransactionId xid;
    PGXACT *pgxact = NULL;
    TwoPhaseFileHeader *hdr = NULL;
    char *bufptr = NULL;
    TransactionId *children = NULL;
    ColFileNode *commitrels = NULL;
    ColFileNode *abortrels = NULL;
    char *commitLibrary = NULL;
    char *abortLibrary = NULL;
    SharedInvalidationMessage *invalmsgs = NULL;
    char *buf = NULL;
    GlobalTransaction gxact = NULL;
    int commitLibraryLen = 0;
    int abortLibraryLen = 0;
    PGPROC *proc = NULL;
    TransactionId latestXid;
    int ndelrels;
    int ndelrels_temp = 0;
    int i;
    ColFileNode *delrels = NULL;
    bool need_remove = false;
    MemoryContext current_context = CurrentMemoryContext;

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(DN_COMMIT_PREPARED_FAILED, twophase_default_error_emit) ||
        TEST_STUB(DN_ABORT_PREPARED_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
                (errcode(ERRCODE_OPERATE_FAILED),
                 errmsg("GTM_TEST  %s: %s failed", g_instance.attr.attr_common.PGXCNodeName, gid)));
    }

    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, gid, WHITEBOX_DEFAULT, 0.002)) {
        ereport(LOG, (errmsg("WHITE_BOX TEST  %s: finish prepared transaction phase1 failed",
                             g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    /*
     * Validate the GID, and lock the GXACT to ensure that two backends do not
     * try to commit the same GID at once.
     */
    gxact = LockGXact(gid, GetUserId());
#ifdef PGXC
    /*
     * LockGXact returns NULL if this node does not contain given two-phase
     * TXN.  This can happen when COMMIT/ROLLBACK PREPARED is issued at
     * the originating Coordinator for cleanup.
     * In this case, no local handling is needed.   Only report to GTM
     * is needed and this has already been handled in FinishRemotePreparedTransaction().
     *
     * Second predicate may not be necessary.   It is just in case.
     */
    if (gxact == NULL && u_sess->attr.attr_common.xc_maintenance_mode) {
        return;
    }
#endif
    Assert(gxact != NULL);
    proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
    xid = pgxact->xid;

    instr_stmt_report_txid(xid);

    int nsubxids = 0;
    TransactionId *subXids = NULL;
    if (GTM_LITE_MODE) {
        nsubxids = pgxact->nxids;
        /* use top transaction memory conext */
        MemoryContext oldContext = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
        if (pgxact->nxids > 0) {
            Size size = mul_size(nsubxids, sizeof(TransactionId));
            subXids = (TransactionId *)palloc0(size);
            errno_t rc = memcpy_s(subXids, size, proc->subxids.xids, size);
            securec_check(rc, "", "");
        }
        MemoryContextSwitchTo(oldContext);
    }

    if (module_logging_is_on(MOD_TRANS_XACT)) {
        ereport(LOG, (errmodule(MOD_TRANS_XACT), errmsg("Node  %s: xid restore from dummy proc is %lu",
                                                        g_instance.attr.attr_common.PGXCNodeName, xid)));
    }

    /*
     * Read and validate 2PC state data.
     * State data will typically be stored in WAL files if the LSN is after the
     * last checkpoint record, or moved to disk if for some reason they have
     * lived for a long time.
     */
    if (gxact->ondisk) {
        buf = ReadTwoPhaseFile(xid, true);
        if (buf == NULL) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read two-phase state file.")));
        }
    } else {
        XlogReadTwoPhaseData(gxact->prepare_start_lsn, &buf, NULL);
        CloseTwoPhaseXlogFile();
    }

    /*
     * Disassemble the header area
     */
    hdr = (TwoPhaseFileHeader *)buf;
    if (hdr->magic == TWOPHASE_MAGIC_NEW || hdr->magic == TWOPHASE_MAGIC_COMPRESSION) {
        /* get num of deleted temp table */
        if (isCommit) {
            ndelrels_temp = ((TwoPhaseFileHeaderNew*)hdr)->ncommitrels_temp;
        } else {
            ndelrels_temp = ((TwoPhaseFileHeaderNew*)hdr)->nabortrels_temp;
        }
        bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeaderNew));
    } else {
        bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeader));
    }

    bool compression = hdr->magic == TWOPHASE_MAGIC_COMPRESSION;
    Assert(TransactionIdEquals(hdr->xid, xid));
    children = (TransactionId *)bufptr;
    bufptr += MAXALIGN(hdr->nsubxacts * sizeof(TransactionId));
    commitrels = (ColFileNode *)bufptr;
    if (unlikely((long)((!compression) && (hdr->ncommitrels > 0)))) {
        ColFileNodeRel *colFileNodeRel = (ColFileNodeRel *)(void *)bufptr;
        commitrels = (ColFileNode *)palloc0((uint32)hdr->ncommitrels * (uint32)sizeof(ColFileNode));
        for (int j = 0; j < hdr->ncommitrels; j++) {
            ColFileNodeCopy(&commitrels[j], &colFileNodeRel[j]);
        }
    }
    bufptr += MAXALIGN(hdr->ncommitrels * (int32)SIZE_OF_COLFILENODE(compression));
    abortrels = (ColFileNode *)bufptr;
    if (unlikely((long)((!compression) && (hdr->nabortrels > 0)))) {
        ColFileNodeRel *colFileNodeRel = (ColFileNodeRel *)(void *)bufptr;
        abortrels = (ColFileNode *)palloc0((uint32)hdr->nabortrels * (uint32)sizeof(ColFileNode));
        for (int j = 0; j < hdr->nabortrels; j++) {
            ColFileNodeCopy(&abortrels[j], &colFileNodeRel[j]);
        }
    }
    bufptr += MAXALIGN(hdr->nabortrels * (int32)SIZE_OF_COLFILENODE(compression));
    invalmsgs = (SharedInvalidationMessage *)bufptr;
    bufptr += MAXALIGN(hdr->ninvalmsgs * sizeof(SharedInvalidationMessage));

    if (hdr->ncommitlibrarys > 0) {
        commitLibraryLen = read_library(bufptr, hdr->ncommitlibrarys);
        commitLibrary = bufptr;
        bufptr += MAXALIGN((uint32)commitLibraryLen);
    }

    if (hdr->nabortlibrarys > 0) {
        abortLibraryLen = read_library(bufptr, hdr->nabortlibrarys);
        abortLibrary = bufptr;
        bufptr += MAXALIGN((uint32)abortLibraryLen);
    }

    /* compute latestXid among all children */
    latestXid = TransactionIdLatest(xid, hdr->nsubxacts, children);

    /*
     * The order of operations here is critical: make the XLOG entry for
     * commit or abort, then mark the transaction committed or aborted in
     * pg_clog, then remove its PGPROC from the global ProcArray (which means
     * TransactionIdIsInProgress will stop saying the prepared xact is in
     * progress), then run the post-commit or post-abort callbacks. The
     * callbacks will release the locks the transaction held.
     */
    if (isCommit) {
        /*
         * Set CSN before abort in case of gtm free
         */
        if (useLocalXid || !IsPostmasterEnvironment || GTM_FREE_MODE) {
            SetXact2CommitInProgress(xid, 0);
            setCommitCsn(getLocalNextCSN());
        }

#ifdef ENABLE_MOT
        CallXactCallbacks(XACT_EVENT_COMMIT_PREPARED);
#endif
        /* Set dummy proc need sync, not current work thread */
        pgxact->needToSyncXid |= SNAPSHOT_UPDATE_NEED_SYNC;

        RecordTransactionCommitPrepared(xid, hdr->nsubxacts, children, hdr->ncommitrels,
            commitrels, hdr->ninvalmsgs, invalmsgs, hdr->ncommitlibrarys, commitLibrary,
            commitLibraryLen, hdr->initfileinval);

#ifdef ENABLE_MOT
        /* Release MOT locks */
        CallXactCallbacks(XACT_EVENT_END_TRANSACTION);
#endif
    } else {
#ifdef ENABLE_MOT
        CallXactCallbacks(XACT_EVENT_ROLLBACK_PREPARED);
#endif

        RecordTransactionAbortPrepared(xid, hdr->nsubxacts, children, hdr->nabortrels,
            abortrels, hdr->nabortlibrarys, abortLibrary, abortLibraryLen);
    }

    ProcArrayRemove(proc, latestXid);

    /*
     * In case we fail while running the callbacks, mark the gxact invalid so
     * no one else will try to commit/rollback, and so it will be recycled
     * if we fail after this point.  It is still locked by our backend so it
     * won't go away yet.
     *
     * (We assume it's safe to do this without taking TwoPhaseStateLock.)
     */
    gxact->valid = false;

    /* Prevent cancel/die interrupt while cleaning up */
    HOLD_INTERRUPTS();
    int saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    /*
     * We have to remove any files that were supposed to be dropped. For
     * consistency with the regular xact.c code paths, must do this before
     * releasing locks, so do it before running the callbacks.
     *
     * NB: this code knows that we couldn't be dropping any temp rels ...
     */
    if (isCommit) {
        delrels = commitrels;
        ndelrels = hdr->ncommitrels;
    } else {
        delrels = abortrels;
        ndelrels = hdr->nabortrels;
    }

    /* CREATE TABlE for dfs table:
     * commit: isCommit == true, ncommitrels=0, nabortrels=1,
     *         1st ncommitrels traverse do nothing because of ncommitrels=0;
     *         2nd nabortrels traverse do nothing  because !isCommit is false;
     *         The mapper file is dropped in 2nd traverse.
     *
     * abort : isCommit == false, ncommitrels=0, nabortrels=1,
     *         1st ncommitrels traverse do nothing because of ncommitrels=0;
     *         2nd nabortrels traverse drop hdfs directory because !isCommit is true;
     *		   The mapper file is dropped in 2nd traverse
     *
     * DROP TABLE for dfs table:
     * commit: isCommit == true, ncommitrels=1, nabortrels=0,
     *         1st ncommitrels traverse drop hdfs directory because isCommit is true;
     *         2nd nabortrels traverse do nothing because of nabortrels=0;
     *         The mapper file is dropped in 1st traverse
     *
     * abort : isCommit == false, ncommitrels=1, nabortrels=0,
     *         1st ncommitrels traverse do nothing because the isCommit is false;
     *         2nd nabortrels traverse do nothing because of nabortrels=0;
     *         The mapper file is dropped in 1st traverse
     *
     * TRUNCATE TABLE for dfs table:
     * commit: isCommit == true, ncommitrels>1, nabortrels=0,
     *         1st ncommitrels traverse drop hdfs directory because isCommit is true;
     *         2nd nabortrels traverse do nothing because of nabortrels=0;
     *         The mapper file is dropped in 1st traverse
     *
     * abort : isCommit == false, ncommitrels>1, nabortrels=0,
     *         1st ncommitrels traverse do nothing because !isCommit is false;
     *         2nd nabortrels traverse do nothing because of nabortrels=0;
     *         The mapper file is dropped in 1st traverse
     *
     */
    /*
     * whatever commit or abort, it's always necessary to drop the mapper files.
     */

    /*
     * At present, after the commited gxact are moved out of procarray, we
     * simply recycle it in Atabort_Twophase if an ERROR occurs. The remaining
     * cleaning-up are not finished: locks are not released, SI messages are
     * not sent, and so on.
     * We could release all the related resouces in Atabort_Twophase. However,
     * deciding when to do so and how to do so could be a little complicated.
     * The same ERROR during cleaning could be recursively thrown out, leading to
     * PANIC in the end.
     * Instead, we degrade ERROR for non-critical problems and force a critical
     * section while doing pending deletes and dealing with contents in shared
     * memory, such as SI messages, user info hash tab and locks.
     *
     * Notes: for non-critical operations inside try/catch, please make sure
     * that no FATAL error might be triggered!
     */
    PG_TRY();
    {
        /* first loop to handle commitrels */
        for (i = 0; i < hdr->ncommitrels; i++) {
            ColFileNode colFileNode;
            ColFileNode *colFileNodeRel = commitrels + i;
            ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
        }
        /* second loop to handle abortrels */
        for (i = 0; i < hdr->nabortrels; i++) {
            ColFileNode colFileNode;
            ColFileNode *colFileNodeRel = abortrels + i;
            ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
            Assert(!IsValidPaxDfsForkNum(colFileNode.forknum));
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(current_context);
        ErrorData *edata = CopyErrorData();
        FlushErrorState();

        ereport(LOG,
                (errmsg("Failed before releasing resources during commit prepared transaction: %s", edata->message)));
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
    }
    PG_END_TRY();

    /*
     * RowRelationDoDeleteFiles will generate xlog if the storage type is segment-page.
     * We can not enlarge record space once we entring the critical section. Thus do it here.
     * Currently, a segment unlink xlog touching at most 4 buffers, and about 30 XLogRecData.
     * But it's harmless to allocate more.
     */
    XLogEnsureRecordSpace(8, 50);

    START_CRIT_SECTION();

    if (isCommit && (commitLibrary != NULL)) {
        parseAndRemoveLibrary(commitLibrary, hdr->ncommitlibrarys);
    }

    if (!isCommit && (abortLibrary != NULL)) {
        parseAndRemoveLibrary(abortLibrary, hdr->nabortlibrarys);
    }
    if (ndelrels != 0) {
        t_thrd.pgxact->delayChkpt = true;
    }
    if (relsContainsSegmentTable(delrels, ndelrels)) {
        if (IS_DEL_RELS_OVER_HASH_THRESHOLD(ndelrels)) {
            DropBufferForDelRelsinXlogUsingHash(delrels, ndelrels);
        } else {
            DropBufferForDelRelinXlogUsingScan(delrels, ndelrels);
        }
    }
    push_unlink_rel_to_hashtbl(delrels, ndelrels);

    ColMainFileNodesCreate();

    for (i = 0; i < ndelrels; i++) {
        ColFileNode colFileNode;
        ColFileNode *colFileNodeRel = delrels + i;
        ColFileNodeFullCopy(&colFileNode, colFileNodeRel);

        if (!IsValidColForkNum(colFileNode.forknum)) {
            RowRelationDoDeleteFiles(colFileNode.filenode, InvalidBackendId, colFileNode.ownerid);
        } else {
            ColumnRelationDoDeleteFiles(&colFileNode.filenode, colFileNode.forknum, InvalidBackendId,
                                        colFileNode.ownerid);
        }
    }
    ColMainFileNodesDestroy();
    if (ndelrels != 0) {
        t_thrd.pgxact->delayChkpt = false;
    }

    /*
     * Handle cache invalidation messages.
     *
     * Relcache init file invalidation requires processing both before and
     * after we send the SI messages. See AtEOXact_Inval()
     */
    if (hdr->initfileinval) {
        RelationCacheInitFilePreInvalidate();
    }
    SendSharedInvalidMessages(invalmsgs, hdr->ninvalmsgs);
    if (hdr->initfileinval) {
        RelationCacheInitFilePostInvalidate();
    }

    /*
     * if transaction is to commit, update user and resource pool hash table,
     * otherwise, reset those flags set for updating hash table
     */
    if (isCommit) {
        UpdateWlmCatalogInfoHash();
    } else {
        ResetWlmCatalogFlag();
    }

    /* And now do the callbacks */
    if (isCommit) {
        ProcessRecords(bufptr, xid, g_twophase_postcommit_callbacks);
    } else {
        ProcessRecords(bufptr, xid, g_twophase_postabort_callbacks);
    }

    PredicateLockTwoPhaseFinish(xid, isCommit);
    if (IS_PGXC_DATANODE || IsConnFromCoord()) {
        u_sess->storage_cxt.twoPhaseCommitInProgress = false;
    }

    END_CRIT_SECTION();

    /* Count the prepared xact as committed or aborted */
    AtEOXact_PgStat(isCommit);

    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    /* we need hold 2PC lock when check gxact->ondisk in case that checkpoint thread change it */
    need_remove = gxact->ondisk;

    /* only can do this after commit csn++ and remove proc */
    if (GTM_LITE_MODE) {
        MarkPrepXidInvalid(gxact, subXids, nsubxids);
    }
    RemoveGXact(gxact);
    TWOPAHSE_LWLOCK_RELEASE(xid);

    if (GTM_LITE_MODE) {
        /* free sub xids array */
        if (subXids != NULL) {
            pfree_ext(subXids);
        }
    }

    /*
     * And now we can clean up any files we may have left.
     */
    if (need_remove) {
        RemoveTwoPhaseFile(xid, true);
    }

    t_thrd.xact_cxt.MyLockedGxact = NULL;
    closeAllVfds();

    RESUME_INTERRUPTS();
    FreeOldColFileNode(compression, commitrels, hdr->ncommitrels, abortrels, hdr->nabortrels);
    pfree(buf);
    buf = NULL;
}

/*
 * @Description: Parse library name from library, skip int byte.
 * @in library: Source string.
 * @in overLen: String over length.
 */
void parseAndRemoveLibrary(char *library, int nlibrary)
{
    char *ptr = library;
    int nlib = nlibrary;
    char *filename = NULL;
    errno_t rc = 0;

    /* Function delete_file_handle have change to process member, here need add lock. */
    AutoMutexLock libraryLock(&dlerror_lock);
    libraryLock.lock();

    while (nlib > 0) {
        int libraryLen = 0;
        rc = memcpy_s(&libraryLen, sizeof(int), ptr, sizeof(int));
        securec_check_c(rc, "", "");
        ptr += sizeof(int);

        filename = (char *)palloc0(libraryLen + 1);
        /* Get library file name. */
        rc = memcpy_s(filename, libraryLen + 1, ptr, libraryLen);
        securec_check_c(rc, "", "");
        ptr += libraryLen;

        delete_file_handle(filename);
        removeLibrary(filename);
        pfree(filename);
        filename = NULL;
        nlib--;
    }

    libraryLock.unLock();
}

/*
 * Scan 2PC state data in memory and call the indicated callbacks for each 2PC record.
 */
static void ProcessRecords(char *bufptr, TransactionId xid, const TwoPhaseCallback callbacks[])
{
    for (;;) {
        TwoPhaseRecordOnDisk *record = (TwoPhaseRecordOnDisk *)bufptr;

        if (record->rmid > TWOPHASE_RM_MAX_ID) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid twophase resource manager lock id")));
        }
        if (record->rmid == TWOPHASE_RM_END_ID) {
            break;
        }

        bufptr += MAXALIGN(sizeof(TwoPhaseRecordOnDisk));

        if (callbacks[record->rmid] != NULL) {
            callbacks[record->rmid](xid, record->info, (void *)bufptr, record->len);
        }

        bufptr += MAXALIGN(record->len);
    }
}

void DeleteObsoleteTwoPhaseFile(int64 pageno)
{
    DIR *cldir = NULL;
    struct dirent *clde = NULL;
    int i;
    int64 cutoffPage = pageno;
    cutoffPage -= cutoffPage % SLRU_PAGES_PER_SEGMENT;
    TransactionId cutoffXid = (TransactionId)CLOG_XACTS_PER_PAGE * cutoffPage;
    cldir = AllocateDir(TWOPHASE_DIR);
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TWOPAHSE_LWLOCK_ACQUIRE(i, LW_EXCLUSIVE);
    }
    while ((clde = ReadDir(cldir, TWOPHASE_DIR)) != NULL) {
        if (strlen(clde->d_name) == 16 && strspn(clde->d_name, "0123456789ABCDEF") == 16) {
            TransactionId xid = (TransactionId)pg_strtouint64(clde->d_name, NULL, 16);
            if (xid < cutoffXid) {
#ifdef USE_ASSERT_CHECKING
               int elevel = PANIC;
#else
               int elevel = WARNING;
#endif
               ereport(elevel, (errmsg("twophase file %lu is older than clog truncate xid :%lu", xid, cutoffXid)));
               RemoveTwoPhaseFile(xid, true);
            }
        }
    }
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TWOPAHSE_LWLOCK_RELEASE(i);
    }
    FreeDir(cldir);
}

/*
 * Remove the 2PC file for the specified XID.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
static void RemoveTwoPhaseFile(TransactionId xid, bool giveWarning)
{
    char path[MAXPGPATH];
    errno_t rc = EOK;

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X", TWOPHASE_DIR, (uint32)(xid >> 32), (uint32)xid);
    securec_check_ss(rc, "", "");
    if (unlink(path)) {
        if (errno != ENOENT || giveWarning) {
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not remove two-phase state file \"%s\": %m", path)));
        }
    }
}

/*
 * Recreates a state file. This is used in WAL replay and during
 * checkpoint creation.
 *
 * Note: content and len don't include CRC.
 */
static void RecreateTwoPhaseFile(TransactionId xid, void *content, int len)
{
    char path[MAXPGPATH];
    pg_crc32 statefile_crc;
    int fd;
    errno_t rc = EOK;

    /* Recompute CRC */
    INIT_CRC32(statefile_crc);
    COMP_CRC32(statefile_crc, content, len);
    FIN_CRC32(statefile_crc);

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X", TWOPHASE_DIR, (uint32)(xid >> 32), (uint32)xid);
    securec_check_ss(rc, "", "");

    fd = BasicOpenFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not recreate two-phase state file \"%s\": %m", path)));
    }

    /* Write content and CRC */
    pgstat_report_waitevent(WAIT_EVENT_TWOPHASE_FILE_WRITE);
    if (write(fd, content, len) != len) {
        int save_errno = errno;
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write two-phase state file: %m")));
    }
    if (write(fd, &statefile_crc, sizeof(pg_crc32)) != sizeof(pg_crc32)) {
        int save_errno = errno;
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write two-phase state file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    /*
     * We must fsync the file because the end-of-replay checkpoint will not do
     * so, there being no GXACT in shared memory yet to tell it to.
     */
    pgstat_report_waitevent(WAIT_EVENT_TWOPHASE_FILE_SYNC);
    if (pg_fsync(fd) != 0) {
        int save_errno = errno;
        close(fd);
        errno = save_errno;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fsync two-phase state file: %m")));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd) != 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close two-phase state file: %m")));
    }
}

/*
 * CheckPointTwoPhase -- handle 2PC component of checkpointing.
 *
 * We must fsync the state file of any GXACT that is valid or has been
 * generated during redo and has a PREPARE LSN <= the checkpoint's redo
 * horizon.  (If the gxact isn't valid yet, has not been generated in
 * redo, or has a later LSN, this checkpoint is not responsible for
 * fsyncing it.)
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because GXACTs ordinarily have short lifespans, and so it is quite
 * possible that GXACTs that were valid at checkpoint start will no longer
 * exist if we wait a little bit. With typical checkpoint settings this
 * will be about 3 minutes for an online checkpoint, so as a result we
 * we expect that there will be no GXACTs that need to be copied to disk.
 *
 * If a GXACT remains valid across multiple checkpoints, it will already
 * be on disk so we don't bother to repeat that write.
 */
void CheckPointTwoPhase(XLogRecPtr redo_horizon)
{
    int i, j;
    int serialized_xacts = 0;
    /*
     * We don't want to hold the TwoPhaseStateLock while doing I/O, so we grab
     * it just long enough to make a list of the XIDs that require fsyncing,
     * and then do the I/O afterwards.
     *
     * This approach creates a race condition: someone else could delete a
     * GXACT between the time we release TwoPhaseStateLock and the time we try
     * to open its state file.	We handle this by special-casing ENOENT
     * failures: if we see that, we verify that the GXACT is no longer valid,
     * and if so ignore the failure.
     */
    if (g_instance.attr.attr_storage.max_prepared_xacts <= 0) {
        return; /* nothing to do */
    }

    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_START();

    /*
     * We are expecting there to be zero GXACTs that need to be
     * copied to disk, so we perform all I/O while holding
     * TwoPhaseStateLock for simplicity. This prevents any new xacts
     * from preparing while this occurs, which shouldn't be a problem
     * since the presence of long-lived prepared xacts indicates the
     * transaction manager isn't active.
     *
     * It's also possible to move I/O out of the lock, but on
     * every error we should check whether somebody commited our
     * transaction in different backend. Let's leave this optimisation
     * for future, if somebody will spot that this place cause
     * bottleneck.
     *
     * Note that it isn't possible for there to be a GXACT with
     * a prepare_end_lsn set prior to the last checkpoint yet
     * is marked invalid, because of the efforts with delayChkpt.
     */
    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_SHARED);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
            /* Note that we are using gxact not pgxact so this works in recovery also */
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];

            if ((gxact->valid || gxact->inredo) && !gxact->ondisk && XLByteLE(gxact->prepare_end_lsn, redo_horizon)) {
                char *buf = NULL;
                int len;
                XlogReadTwoPhaseData(gxact->prepare_start_lsn, &buf, &len);
                RecreateTwoPhaseFile(gxact->xid, buf, len);
                gxact->ondisk = true;
                gxact->prepare_start_lsn = InvalidXLogRecPtr;
                gxact->prepare_end_lsn = InvalidXLogRecPtr;
                pfree(buf);
                buf = NULL;
                serialized_xacts++;
            }
        }
        TWOPAHSE_LWLOCK_RELEASE(j);
    }
    CloseTwoPhaseXlogFile();
    g_instance.ckpt_cxt_ctl->ckpt_view.ckpt_twophase_flush_num += serialized_xacts;
    TRACE_POSTGRESQL_TWOPHASE_CHECKPOINT_DONE();

    if (u_sess->attr.attr_common.log_checkpoints && serialized_xacts > 0) {
        ereport(LOG, (errmsg("%d two-phase state files were written "
                             "for long-running prepared transactions",
                             serialized_xacts)));
    }
}

/*
 * restoreTwoPhaseData
 *
 * Scan pg_twophase and fill TwoPhaseState depending on the on-disk data.
 * This is called once at the beginning of recovery, saving any extra
 * lookups in the future.  Two-phase files that are newer than the
 * minimum XID horizon are discarded on the way.
 */
void restoreTwoPhaseData(void)
{
    DIR *cldir = NULL;
    struct dirent *clde = NULL;
    int i;

    cldir = AllocateDir(TWOPHASE_DIR);
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TWOPAHSE_LWLOCK_ACQUIRE(i, LW_EXCLUSIVE);
    }
    while ((clde = ReadDir(cldir, TWOPHASE_DIR)) != NULL) {
        if (strlen(clde->d_name) == 16 && strspn(clde->d_name, "0123456789ABCDEF") == 16) {
            TransactionId xid;
            char *buf = NULL;

            xid = (TransactionId)pg_strtouint64(clde->d_name, NULL, 16);

            buf = ProcessTwoPhaseBuffer(xid, InvalidXLogRecPtr, true, false, false);
            if (buf == NULL) {
                continue;
            }

            PrepareRedoAdd(buf, InvalidXLogRecPtr, InvalidXLogRecPtr);
        }
    }
    for (i = 0; i < NUM_TWOPHASE_PARTITIONS; i++) {
        TWOPAHSE_LWLOCK_RELEASE(i);
    }
    FreeDir(cldir);
}

/*
 * PrescanPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and determine the range
 * of valid XIDs present.  This is run during database startup, after we
 * have completed reading WAL.  ShmemVariableCache->nextXid has been set to
 * one more than the highest XID for which evidence exists in WAL.
 *
 * We throw away any prepared xacts with main XID beyond nextXid --- if any
 * are present, it suggests that the DBA has done a PITR recovery to an
 * earlier point in time without cleaning out pg_twophase.	We dare not
 * try to recover such prepared xacts since they likely depend on database
 * state that doesn't exist now.
 *
 * However, we will advance nextXid beyond any subxact XIDs belonging to
 * valid prepared xacts.  We need to do this since subxact commit doesn't
 * write a WAL entry, and so there might be no evidence in WAL of those
 * subxact XIDs.
 *
 * Our other responsibility is to determine and return the oldest valid XID
 * among the prepared xacts (if none, return ShmemVariableCache->nextXid).
 * This is needed to synchronize pg_subtrans startup properly.
 *
 * If xids_p and nxids_p are not NULL, pointer to a palloc'd array of all
 * top-level xids is stored in *xids_p. The number of entries in the array
 * is returned in *nxids_p.
 */
TransactionId PrescanPreparedTransactions(TransactionId **xids_p, int *nxids_p)
{
    TransactionId origNextXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    TransactionId result = origNextXid;
    TransactionId *xids = NULL;
    int nxids = 0;
    int allocsize = 0;
    int i, j;

    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_EXCLUSIVE);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
            TransactionId xid;
            char *buf = NULL;
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];

            Assert(gxact->inredo);

            xid = gxact->xid;

            buf = ProcessTwoPhaseBuffer(xid, gxact->prepare_start_lsn, gxact->ondisk, false, true);

            if (buf == NULL) {
                continue;
            }

            /*
             * OK, we think this file is valid.  Incorporate xid into the
             * running-minimum result.
             */
            if (TransactionIdPrecedes(xid, result)) {
                result = xid;
            }

            if (xids_p != NULL) {
                if (nxids == allocsize) {
                    if (nxids == 0) {
                        allocsize = 10;
                        xids = (TransactionId *)palloc(allocsize * sizeof(TransactionId));
                    } else {
                        allocsize = allocsize * 2;
                        xids = (TransactionId *)repalloc(xids, allocsize * sizeof(TransactionId));
                    }
                }
                xids[nxids++] = xid;
            }

            pfree(buf);
            buf = NULL;
        }
        TWOPAHSE_LWLOCK_RELEASE(j);
    }
    CloseTwoPhaseXlogFile();

    if (xids_p != NULL) {
        *xids_p = xids;
        *nxids_p = nxids;
    }

    return result;
}

/*
 * StandbyRecoverPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and setup all the required
 * information to allow standby queries to treat prepared transactions as still
 * active.
 *
 * This is never called at the end of recovery - we use
 * RecoverPreparedTransactions() at that point.
 *
 * The lack of calls to SubTransSetParent() calls here is by design;
 * those calls are made by RecoverPreparedTransactions() at the end of recovery
 * for those xacts that need this.
 */
void StandbyRecoverPreparedTransactions(void)
{
    int i, j;

    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_EXCLUSIVE);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
            TransactionId xid;
            char *buf = NULL;
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];

            Assert(gxact->inredo);

            xid = gxact->xid;

            buf = ProcessTwoPhaseBuffer(xid, gxact->prepare_start_lsn, gxact->ondisk, true, false);
            if (buf != NULL) {
                pfree(buf);
                buf = NULL;
            }
        }
        TWOPAHSE_LWLOCK_RELEASE(j);
    }
    CloseTwoPhaseXlogFile();
}

/*
 * RecoverPreparedTransactions
 *
 * Scan the shared memory entries of TwoPhaseState and reload the state for
 * each prepared transaction (reacquire locks, etc).
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 *
 * At the end of recovery the way we take snapshots will change. We now need
 * to mark all running transactions with their full SubTransSetParent() info
 * to allow normal snapshots to work correctly if snapshots overflow.
 * We do this here because by definition prepared transactions are the only
 * type of write transaction still running, so this is necessary and
 * complete.
 */
void RecoverPreparedTransactions(void)
{
    int i, j;
    /* Find recentGlobalXmin form  TwoPhaseState when recovery finish */
    TransactionId prepare_xmin = InvalidTransactionId;

    for (j = 0; j < NUM_TWOPHASE_PARTITIONS; j++) {
        TWOPAHSE_LWLOCK_ACQUIRE(j, LW_EXCLUSIVE);
        TwoPhaseStateData *currentStatePtr = TwoPhaseState(j);
        int prepareXactNumber = currentStatePtr->numPrepXacts;
        for (i = 0; i < prepareXactNumber; i++) {
            TransactionId xid;
            char *buf = NULL;
            GlobalTransaction gxact = currentStatePtr->prepXacts[i];
            char *bufptr = NULL;
            TwoPhaseFileHeader *hdr = NULL;
            TransactionId *subxids = NULL;

            xid = gxact->xid;

            /*
             * Reconstruct subtrans state for the transaction --- needed
             * because pg_subtrans is not preserved over a restart.  Note that
             * we are linking all the subtransactions directly to the
             * top-level XID; there may originally have been a more complex
             * hierarchy, but there's no need to restore that exactly.
             * It's possible that SubTransSetParent has been set before, if
             * the prepared transaction generated xid assignment records.
             */
            buf = ProcessTwoPhaseBuffer(xid, gxact->prepare_start_lsn, gxact->ondisk, true, false);
            if (buf == NULL) {
                continue;
            }

            ereport(LOG, (errmsg("recovering prepared transaction " XID_FMT " from shared memory", xid)));

            hdr = (TwoPhaseFileHeader *)buf;
            Assert(TransactionIdEquals(hdr->xid, xid));
            bool compressMagic = hdr->magic == TWOPHASE_MAGIC_COMPRESSION;
            int hdrSize = (hdr->magic == TWOPHASE_MAGIC_NEW || compressMagic) ?
                sizeof(TwoPhaseFileHeaderNew) : sizeof(TwoPhaseFileHeader);
            bufptr = buf + MAXALIGN(hdrSize);
            subxids = (TransactionId *)bufptr;
            bufptr += MAXALIGN(hdr->nsubxacts * sizeof(TransactionId));
            bufptr += MAXALIGN((uint32)hdr->ncommitrels * SIZE_OF_COLFILENODE(compressMagic));
            bufptr += MAXALIGN((uint32)hdr->nabortrels * SIZE_OF_COLFILENODE(compressMagic));
            bufptr += MAXALIGN((uint32)hdr->ninvalmsgs * sizeof(SharedInvalidationMessage));

            if (hdr->ncommitlibrarys > 0) {
                int commitLibraryLen = read_library(bufptr, hdr->ncommitlibrarys);
                bufptr += MAXALIGN((uint32)commitLibraryLen);
            }

            if (hdr->nabortlibrarys > 0) {
                int abortLibraryLen = read_library(bufptr, hdr->nabortlibrarys);
                bufptr += MAXALIGN((uint32)abortLibraryLen);
            }

            /*
             * Recreate its GXACT and dummy PGPROC. But, check whether
             * it was added in redo and already has a shmem entry for
             * it.
             */
            MarkAsPreparingGuts(InvalidTransactionHandle, gxact, xid, hdr->gid, hdr->prepared_at, hdr->owner,
                                hdr->database, 0);

            /* recovered, so reset the flag for entries generated by redo */
            gxact->inredo = false;

            GXactLoadSubxactData(gxact, hdr->nsubxacts, subxids);
            MarkAsPrepared(gxact, true);

            if (!TransactionIdIsValid(prepare_xmin)) {
                prepare_xmin = xid;
            } else if (TransactionIdPrecedes(xid, prepare_xmin)) {
                prepare_xmin = xid;
            }

            TWOPAHSE_LWLOCK_RELEASE(j);

            /*
             * Recover other state (notably locks) using resource managers.
             */
            ProcessRecords(bufptr, xid, g_twophase_recover_callbacks);

            /*
             * Release locks held by the standby process after we process each
             * prepared transaction. As a result, we don't need too many
             * additional locks at any one time.
             */
            if (InHotStandby) {
                StandbyReleaseLockTree(xid, hdr->nsubxacts, subxids);
            }

            /*
             * We're done with recovering this transaction. Clear
             * MyLockedGxact, like we do in PrepareTransaction() during normal
             * operation.
             */
            PostPrepare_Twophase();

            pfree(buf);
            buf = NULL;

            TWOPAHSE_LWLOCK_ACQUIRE(j, LW_EXCLUSIVE);
        }

        TWOPAHSE_LWLOCK_RELEASE(j);
    }
    CloseTwoPhaseXlogFile();

    if (TransactionIdIsValid(prepare_xmin)) {
        if (TransactionIdPrecedes(prepare_xmin, t_thrd.xact_cxt.ShmemVariableCache->xmin)) {
            t_thrd.xact_cxt.ShmemVariableCache->xmin = prepare_xmin;
            TransactionIdRetreat(prepare_xmin);
            if (TransactionIdPrecedes(prepare_xmin, t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin)) {
                t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin = prepare_xmin;
            }
        }
    }
}

/*
 * ProcessTwoPhaseBuffer
 *
 * Given a transaction id, read it either from disk or read it directly
 * via shmem xlog record pointer using the provided "prepare_start_lsn".
 *
 * If setParent is true, then use the overwriteOK parameter to set up
 * subtransaction parent linkages.
 *
 * If setParent is true, set up subtransaction parent linkages.
 */
static char *ProcessTwoPhaseBuffer(TransactionId xid, XLogRecPtr prepare_start_lsn, bool fromdisk, bool setParent,
                                   bool setNextXid)
{
    TransactionId origNextXid = t_thrd.xact_cxt.ShmemVariableCache->nextXid;
    TransactionId *subxids = NULL;
    char *buf = NULL;
    TwoPhaseFileHeader *hdr = NULL;
    int i;

    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(TwoPhaseStateMappingPartitionLock(xid)));
    if (!fromdisk) {
        Assert(prepare_start_lsn != 0);
    }

    /* Reject XID if too new */
    if (TransactionIdFollowsOrEquals(xid, origNextXid)) {
        if (fromdisk) {
            ereport(WARNING, (errmsg("removing future two-phase state file for \"" XID_FMT "\"", xid)));
            RemoveTwoPhaseFile(xid, true);
        } else {
            ereport(WARNING, (errmsg("removing future two-phase state from memory for \"" XID_FMT "\"", xid)));
            PrepareRedoRemove(xid, true);
        }
        return NULL;
    }

    /* Already processed? */
    bool xid_processed = TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid);
    if (xid_processed) {
        if (fromdisk) {
            ereport(WARNING, (errmsg("removing stale two-phase state file for \"" XID_FMT "\"", xid)));
            RemoveTwoPhaseFile(xid, true);
        } else {
            ereport(WARNING, (errmsg("removing stale two-phase state from shared memory for \"" XID_FMT "\"", xid)));
            PrepareRedoRemove(xid, true);
        }
        return NULL;
    }

    if (fromdisk) {
        /* Read and validate file */
        buf = ReadTwoPhaseFile(xid, true);
        if (buf == NULL) {
            ereport(WARNING, (errmsg("removing corrupt two-phase state file for \"" XID_FMT "\"", xid)));
            RemoveTwoPhaseFile(xid, true);
            return NULL;
        }
    } else {
        /* Read xlog data */
        XlogReadTwoPhaseData(prepare_start_lsn, &buf, NULL);
    }

    /* Deconstruct header */
    hdr = (TwoPhaseFileHeader *)buf;
    if (!TransactionIdEquals(hdr->xid, xid)) {
        if (fromdisk) {
            ereport(WARNING, (errmsg("removing corrupt two-phase state file for \"" XID_FMT "\"", xid)));
            RemoveTwoPhaseFile(xid, true);
        } else {
            ereport(WARNING, (errmsg("removing corrupt two-phase state from memory for \"" XID_FMT "\"", xid)));
            PrepareRedoRemove(xid, true);
        }
        pfree(buf);
        buf = NULL;
        return NULL;
    }

    /*
     * Examine subtransaction XIDs ... they should all follow main
     * XID, and they may force us to advance nextXid.
     */
    bool compressMagic = hdr->magic == TWOPHASE_MAGIC_COMPRESSION;
    int hdrSize = (hdr->magic == TWOPHASE_MAGIC_NEW || compressMagic) ?
        sizeof(TwoPhaseFileHeaderNew) : sizeof(TwoPhaseFileHeader);
    subxids = (TransactionId *)(buf + MAXALIGN(hdrSize));
    for (i = 0; i < hdr->nsubxacts; i++) {
        TransactionId subxid = subxids[i];

        Assert(TransactionIdFollows(subxid, xid));

        /* update nextXid if needed */
        if (setNextXid && TransactionIdFollowsOrEquals(subxid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
            /*
             * We don't expect anyone else to modify nextXid, hence we don't
             * need to hold a lock while examining it.  We still acquire the
             * lock to modify it, though, so we recheck.
             */
            LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
            if (TransactionIdFollowsOrEquals(subxid, t_thrd.xact_cxt.ShmemVariableCache->nextXid)) {
                t_thrd.xact_cxt.ShmemVariableCache->nextXid = subxid;
                TransactionIdAdvance(t_thrd.xact_cxt.ShmemVariableCache->nextXid);
            }
            LWLockRelease(XidGenLock);
        }

        if (setParent) {
            SubTransSetParent(subxid, xid);
        }
    }

    return buf;
}

/*
 *	RecordTransactionCommitPrepared
 *
 * This is basically the same as RecordTransactionCommit: in particular,
 * we must set the inCommit flag to avoid a race condition.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the commit record.
 */
static void RecordTransactionCommitPrepared(TransactionId xid, int nchildren, TransactionId *children, int nrels,
                                            ColFileNode *rels, int ninvalmsgs, SharedInvalidationMessage *invalmsgs,
                                            int nlibrary, char *librarys, int libraryLen, bool initfileinval)
{
    xl_xact_commit_prepared xlrec;
    XLogRecPtr recptr;
    XLogRecPtr globalDelayDDLLSN = InvalidXLogRecPtr;

    /*
     * Check that we haven't commited halfway through RecordTransactionAbortPrepared.
     */
    if (TransactionIdDidAbort(xid)) {
        ereport(PANIC, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("cannot commit prepared transaction %lu, it was already aborted", xid)));
    }

    START_CRIT_SECTION();

    /* See notes in RecordTransactionCommit */
    t_thrd.pgxact->delayChkpt = true;
    UpdateNextMaxKnownCSN(GetCommitCsn());

    /* Emit the XLOG commit record */
    xlrec.crec.csn = GetCommitCsn();
    xlrec.xid = xid;
    xlrec.crec.xact_time = GetCurrentTimestamp();
    xlrec.crec.xinfo = initfileinval ? XACT_COMPLETION_UPDATE_RELCACHE_FILE : 0;
#ifdef ENABLE_MOT
    if (IsMOTEngineUsed() || IsMixedEngineUsed()) {
        xlrec.crec.xinfo |= XACT_MOT_ENGINE_USED;
    }
#endif
    xlrec.crec.nmsgs = 0;
    xlrec.crec.nrels = nrels;
    xlrec.crec.nsubxacts = nchildren;
    xlrec.crec.nmsgs = ninvalmsgs;
    xlrec.crec.nlibrary = nlibrary;
    xlrec.crec.dbId = u_sess->proc_cxt.MyDatabaseId;
    xlrec.crec.tsId = u_sess->proc_cxt.MyDatabaseTableSpace;

    XLogBeginInsert();
    XLogRegisterData((char *)(&xlrec), MinSizeOfXactCommitPrepared);

    /* dump rels to delete */
    if (nrels > 0) {
        if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
            XLogRegisterData((char *)ConvertToOldColFileNode(rels, nrels, false),
                             (int)(nrels * sizeof(ColFileNodeRel)));
        } else {
            XLogRegisterData((char *)rels, nrels * sizeof(ColFileNode));
        }
        LWLockAcquire(DelayDDLLock, LW_SHARED);
    }

    /* dump committed child Xids */
    if (nchildren > 0) {
        XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));
    }

    /* dump cache invalidation messages */
    if (ninvalmsgs > 0) {
        XLogRegisterData((char *)invalmsgs, ninvalmsgs * sizeof(SharedInvalidationMessage));
    }

#ifndef ENABLE_MULTIPLE_NODES
    XLogRegisterData((char *) &u_sess->utils_cxt.RecentXmin, sizeof(TransactionId));
#endif

    if (nlibrary > 0) {
        XLogRegisterData((char *)librarys, libraryLen);
    }

    /* we allow filtering by xacts */
    XLogIncludeOrigin();

    int1 info = XLOG_XACT_COMMIT_PREPARED;
    if (t_thrd.proc->workingVersionNum >= PAGE_COMPRESSION_VERSION) {
        info |= XLR_REL_COMPRESS;
    }
    recptr = XLogInsert(RM_XACT_ID, (uint8)info);

    if (nrels > 0) {
        globalDelayDDLLSN = GetDDLDelayStartPtr();

        if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, recptr)) {
            t_thrd.xact_cxt.xactDelayDDL = true;
        } else {
            t_thrd.xact_cxt.xactDelayDDL = false;
        }

        LWLockRelease(DelayDDLLock);
    }

    /*
     * We don't currently try to sleep before flush here ... nor is there any
     * support for async commit of a prepared xact (the very idea is probably
     * a contradiction)
     */
    /* Flush XLOG to disk */
    XLogWaitFlush(recptr);

    /*
     * Wake up all walsenders to send WAL up to the COMMIT PREPARED record
     * immediately if replication is enabled
     */
    if (g_instance.attr.attr_storage.max_wal_senders > 0) {
        WalSndWakeup();
    }

    /* Mark the transaction committed in pg_clog */
    TransactionIdCommitTree(xid, nchildren, children, GetCommitCsn());

    /* Checkpoint can proceed now */
    t_thrd.pgxact->delayChkpt = false;

    END_CRIT_SECTION();
}

/*
 *	RecordTransactionAbortPrepared
 *
 * This is basically the same as RecordTransactionAbort.
 *
 * We know the transaction made at least one XLOG entry (its PREPARE),
 * so it is never possible to optimize out the abort record.
 */
static void RecordTransactionAbortPrepared(TransactionId xid, int nchildren, TransactionId *children, int nrels,
                                           ColFileNode *rels, int nlibrary, char *library, int libraryLen)
{
    xl_xact_abort_prepared xlrec;
    XLogRecPtr recptr;
    XLogRecPtr globalDelayDDLLSN = InvalidXLogRecPtr;

    /*
     * Catch the scenario where we aborted partway through
     * RecordTransactionCommitPrepared ...
     */
    if (TransactionIdDidCommit(xid)) {
        ereport(PANIC, (errmsg("cannot abort prepared transaction %lu, it was already committed", xid)));
    }

    START_CRIT_SECTION();

    /* Emit the XLOG abort record */
    xlrec.xid = xid;
    xlrec.arec.xact_time = GetCurrentTimestamp();
    xlrec.arec.nrels = nrels;
    xlrec.arec.nsubxacts = nchildren;
    xlrec.arec.nlibrary = nlibrary;

    XLogBeginInsert();
    XLogRegisterData((char *)(&xlrec), MinSizeOfXactAbortPrepared);

    /* dump rels to delete */
    if (nrels > 0) {
        if (unlikely((long)(t_thrd.proc->workingVersionNum < PAGE_COMPRESSION_VERSION))) {
            XLogRegisterData((char *)ConvertToOldColFileNode(rels, nrels, false),
                             (int)(nrels * sizeof(ColFileNodeRel)));
        } else {
            XLogRegisterData((char *)rels, nrels * sizeof(ColFileNode));
        }
        LWLockAcquire(DelayDDLLock, LW_SHARED);
    }

    /* dump committed child Xids */
    if (nchildren > 0) {
        XLogRegisterData((char *)children, nchildren * sizeof(TransactionId));
    }

    if (nlibrary > 0) {
        XLogRegisterData((char *)library, libraryLen);
    }

    int1 info = XLOG_XACT_ABORT_PREPARED;
    if (t_thrd.proc->workingVersionNum >= PAGE_COMPRESSION_VERSION) {
        info |= XLR_REL_COMPRESS;
    }
    recptr = XLogInsert(RM_XACT_ID, (uint8)info);

    if (nrels > 0) {
        globalDelayDDLLSN = GetDDLDelayStartPtr();

        if (!XLogRecPtrIsInvalid(globalDelayDDLLSN) && XLByteLT(globalDelayDDLLSN, recptr)) {
            t_thrd.xact_cxt.xactDelayDDL = true;
        } else {
            t_thrd.xact_cxt.xactDelayDDL = false;
        }

        LWLockRelease(DelayDDLLock);
    }

    /* Always flush, since we're about to remove the 2PC state file */
    XLogWaitFlush(recptr);

    /*
     * Wake up all walsenders to send WAL up to the ABORT PREPARED record
     * immediately if replication is enabled
     */
    if (g_instance.attr.attr_storage.max_wal_senders > 0) {
        WalSndWakeup();
    }

    /*
     * Mark the transaction aborted in clog.  This is not absolutely necessary
     * but we may as well do it while we are here.
     */
    TransactionIdAbortTree(xid, nchildren, children);

    END_CRIT_SECTION();
}

/*
 * PrepareRedoAdd
 *
 * Store pointers to the start/end of the WAL record along with the xid in
 * a gxact entry in shared memory TwoPhaseState structure.  If caller
 * specifies InvalidXLogRecPtr as WAL position to fetch the two-phase
 * data, the entry is marked as located on disk.
 */
void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
    TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *)buf;
    const char *gid = NULL;
    GlobalTransaction gxact;
    errno_t rc = 0;

    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(TwoPhaseStateMappingPartitionLock(hdr->xid)));
    Assert(RecoveryInProgress());

    gid = hdr->gid;

    /* make sure no same xid in the TwoPhaseState */
    TwoPhaseStateData *currentStatePtr = TwoPhaseState(hdr->xid);
    for (int i = 0; i < currentStatePtr->numPrepXacts; i++) {
        gxact = currentStatePtr->prepXacts[i];

        if (gxact->xid == hdr->xid) {
            Assert(gxact->inredo);
            ereport(LOG, (errmsg("2PC data xid : " XID_FMT " has already existed", gxact->xid)));
            return;
        }
    }

    /*
     * Reserve the GID for the given transaction in the redo code path.
     *
     * This creates a gxact struct and puts it into the active array.
     *
     * In redo, this struct is mainly used to track PREPARE/COMMIT entries
     * in shared memory. Hence, we only fill up the bare minimum contents here.
     * The gxact also gets marked with gxact->inredo set to true to indicate
     * that it got added in the redo phase
     */
    /* Get a free gxact from the freelist */
    if (currentStatePtr->freeGXacts == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("maximum number of prepared transactions reached"),
                        errhint("Increase max_prepared_transactions (currently %d).",
                                g_instance.attr.attr_storage.max_prepared_xacts)));
    }
    gxact = currentStatePtr->freeGXacts;
    currentStatePtr->freeGXacts = gxact->next;

    gxact->prepared_at = hdr->prepared_at;
    gxact->prepare_start_lsn = start_lsn;
    gxact->prepare_end_lsn = end_lsn;
    gxact->xid = hdr->xid;
    gxact->owner = hdr->owner;
    gxact->locking_backend = InvalidBackendId;
    gxact->valid = false;
    gxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
    gxact->inredo = true; /* yes, added in redo */
    rc = strcpy_s(gxact->gid, GIDSIZE, gid);
    securec_check_c(rc, "\0", "\0");

    /* And insert it into the active array */
    Assert(currentStatePtr->numPrepXacts < g_instance.attr.attr_storage.max_prepared_xacts);
    currentStatePtr->prepXacts[currentStatePtr->numPrepXacts++] = gxact;

    ereport(DEBUG2, (errmsg("Adding 2PC data " XID_FMT " to shared memory", gxact->xid)));
}

/*
 * PrepareRedoRemove
 *
 * Remove the corresponding gxact entry from TwoPhaseState. Also
 * remove the 2PC file if a prepared transaction was saved via
 * an earlier checkpoint.
 *
 * Caller must hold TwoPhaseStateLock in exclusive mode, because TwoPhaseState
 * is updated.
 */
void PrepareRedoRemove(TransactionId xid, bool giveWarning)
{
    GlobalTransaction gxact = NULL;
    int i;
    bool found = false;

    /* unfortunately we can't check if the lock is held exclusively */
    Assert(LWLockHeldByMe(TwoPhaseStateMappingPartitionLock(xid)));

    Assert(RecoveryInProgress());

    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        gxact = currentStatePtr->prepXacts[i];

        if (gxact->xid == xid) {
            Assert(gxact->inredo);
            found = true;
            break;
        }
    }

    /*
     * Just leave if there is nothing, this is expected during WAL replay.
     */
    if (!found) {
        return;
    }

    /*
     * And now we can clean up any files we may have left.
     */
    ereport(DEBUG2, (errmsg("removing 2PC data for transaction " XID_FMT, xid)));
    if (gxact->ondisk) {
        RemoveTwoPhaseFile(xid, giveWarning);
    }
    RemoveGXact(gxact);

    return;
}

void RecoverPrepareTransactionCSNLog(char *buf)
{
    TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *)buf;

    if (TransactionIdIsNormal(hdr->xid)) {
        bool compressMagic = hdr->magic == TWOPHASE_MAGIC_COMPRESSION;
        int hdrSize = (hdr->magic == TWOPHASE_MAGIC_NEW || compressMagic) ?
            sizeof(TwoPhaseFileHeaderNew) : sizeof(TwoPhaseFileHeader);
        TransactionId *sub_xids = (TransactionId *)(buf + MAXALIGN(hdrSize));
        ExtendCsnlogForSubtrans(hdr->xid, hdr->nsubxacts, sub_xids);
        CSNLogSetCommitSeqNo(hdr->xid, hdr->nsubxacts, sub_xids,
                             COMMITSEQNO_COMMIT_INPROGRESS);
    }
}

void RemoveStaleTwophaseState(TransactionId xid)
{
    GlobalTransaction gxact = NULL;
    int i;
    bool found = false;
    TransactionId latestCompletedXid;
    ereport(WARNING,
            (errmsg("removing 2PC stale state and stale proc for transaction %lu after prepare failed.", xid)));

    TwoPhaseStateData *currentStatePtr = TwoPhaseState(xid);
    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_SHARED);
    for (i = 0; i < currentStatePtr->numPrepXacts; i++) {
        gxact = currentStatePtr->prepXacts[i];

        if (gxact->xid == xid) {
            found = true;
            break;
        }
    }
    TWOPAHSE_LWLOCK_RELEASE(xid);

    /*
     * Just leave if there is nothing without holding any locks.
     */
    if (!found) {
        return;
    }
    int nsubxids = 0;
    TransactionId *subXids = NULL;
    PGPROC *proc = g_instance.proc_base_all_procs[gxact->pgprocno];
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[gxact->pgprocno];
    if (GTM_LITE_MODE) {
        nsubxids = pgxact->nxids;
        /* Top memory context or abort memory context */
        if (pgxact->nxids > 0) {
            MemoryContext oldContext =
                MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
            Size size = mul_size(pgxact->nxids, sizeof(TransactionId));
            subXids = (TransactionId *)palloc0(size);
            errno_t rc = memcpy_s(subXids, size, proc->subxids.xids, size);
            securec_check(rc, "", "");
            MemoryContextSwitchTo(oldContext);
        }
    }

    /*
     * Remove procarray first , don't hold 2pc lock to avoid deadlock
     * Should not advance latestCompletedXid, let recordtransactionabort
     * do it. Use 32 atomic read to get latestCompletedXid. And just pass
     * it to ProcArrayRemove. Don't calculate new local snapshot.
     */
    latestCompletedXid = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    ProcArrayRemove(proc, latestCompletedXid);

    /*
     * And now we can clean up any files we may have left.
     */
    TWOPAHSE_LWLOCK_ACQUIRE(xid, LW_EXCLUSIVE);
    if (gxact->ondisk) {
        RemoveTwoPhaseFile(xid, true);
    }

    if (GTM_LITE_MODE) {
        MarkPrepXidInvalid(gxact, subXids, nsubxids);
    }
    RemoveGXact(gxact);
    TWOPAHSE_LWLOCK_RELEASE(xid);
    if (GTM_LITE_MODE && (subXids != NULL)) {
        pfree_ext(subXids);
    }
    ereport(WARNING, (errmsg("finish remove stale state after prepare failed.")));

    return;
}

