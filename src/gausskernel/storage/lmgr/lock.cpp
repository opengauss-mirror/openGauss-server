/* -------------------------------------------------------------------------
 *
 * lock.cpp
 *	  openGauss primary lock mechanism
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/lock.cpp
 *
 * NOTES
 *	  A lock table is a shared memory hash table.  When
 *	  a process tries to acquire a lock of a type that conflicts
 *	  with existing locks, it is put to sleep using the routines
 *	  in storage/lmgr/proc.c.
 *
 *	  For the most part, this code should be invoked via lmgr.c
 *	  or another lock-management module, not directly.
 *
 *	Interface provided here: InitLocks(), GetLocksMethodTable(),
 *	Interface provided here: LockAcquire(), LockRelease(), LockReleaseAll(),
 *	Interface provided here: LockCheckConflicts(), GrantLock()
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "executor/exec/execStream.h"
#include "instruments/instr_event.h"
#include "instruments/instr_statement.h"
#include "ddes/dms/ss_dms_bufmgr.h"

#define NLOCKENTS()                                           \
    mul_size(g_instance.attr.attr_storage.max_locks_per_xact, \
             add_size(g_instance.shmem_cxt.MaxBackends,       \
             g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS))

/*
 * Data structures defining the semantics of the standard lock methods.
 *
 * The conflict table defines the semantics of the various lock modes.
 */
static const LOCKMASK LockConflicts[] = {
    0,

    /* AccessShareLock */
    (1 << AccessExclusiveLock),

    /* RowShareLock */
    (1 << ExclusiveLock) | (1 << AccessExclusiveLock),

    /* RowExclusiveLock */
    (1 << ShareLock) | (1 << ShareRowExclusiveLock) | (1 << ExclusiveLock) | (1 << AccessExclusiveLock),

    /* ShareUpdateExclusiveLock */
    (1 << ShareUpdateExclusiveLock) | (1 << ShareLock) | (1 << ShareRowExclusiveLock) | (1 << ExclusiveLock) |
    (1 << AccessExclusiveLock),

    /* ShareLock */
    (1 << RowExclusiveLock) | (1 << ShareUpdateExclusiveLock) | (1 << ShareRowExclusiveLock) | (1 << ExclusiveLock) |
    (1 << AccessExclusiveLock),

    /* ShareRowExclusiveLock */
    (1 << RowExclusiveLock) | (1 << ShareUpdateExclusiveLock) | (1 << ShareLock) | (1 << ShareRowExclusiveLock) |
    (1 << ExclusiveLock) | (1 << AccessExclusiveLock),

    /* ExclusiveLock */
    (1 << RowShareLock) | (1 << RowExclusiveLock) | (1 << ShareUpdateExclusiveLock) | (1 << ShareLock) |
    (1 << ShareRowExclusiveLock) | (1 << ExclusiveLock) | (1 << AccessExclusiveLock),

    /* AccessExclusiveLock */
    (1 << AccessShareLock) | (1 << RowShareLock) | (1 << RowExclusiveLock) | (1 << ShareUpdateExclusiveLock) |
    (1 << ShareLock) | (1 << ShareRowExclusiveLock) | (1 << ExclusiveLock) | (1 << AccessExclusiveLock)
};

/* Names of lock modes, for debug printouts */
static const char *const lock_mode_names[] = {
    "INVALID",   "AccessShareLock",       "RowShareLock",  "RowExclusiveLock",   "ShareUpdateExclusiveLock",
    "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"
};

#ifndef LOCK_DEBUG
    static bool Dummy_trace = false;
#endif

static const LockMethodData default_lockmethod = { AccessExclusiveLock, /* highest valid lock mode number */
                                                   LockConflicts,
                                                   lock_mode_names,
#ifdef LOCK_DEBUG
                                                   &Trace_locks
#else
                                                   &Dummy_trace
#endif
                                                };

static const LockMethodData user_lockmethod = { AccessExclusiveLock, /* highest valid lock mode number */
                                                LockConflicts,
                                                lock_mode_names,
#ifdef LOCK_DEBUG
                                                &Trace_userlocks
#else
                                                &Dummy_trace
#endif
                                            };

/*
 * map from lock method id to the lock table data structures
 */
static const LockMethod LockMethods[] = {NULL, &default_lockmethod, &user_lockmethod};
static bool FastPathGrantRelationLock(const FastPathTag &tag, LOCKMODE lockmode);
static bool FastPathUnGrantRelationLock(const FastPathTag &tag, LOCKMODE lockmode);
static bool FastPathTransferRelationLocks(LockMethod lockMethodTable, const LOCKTAG *locktag, uint32 hashcode);
static PROCLOCK *FastPathGetRelationLockEntry(LOCALLOCK *locallock);
static LockAcquireResult LockAcquireExtendedXC(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock,
                                               bool dontWait, bool reportMemoryError, bool only_increment,
                                               bool allow_con_update = false, int waitSec = 0);
static bool SSDmsLockAcquire(LOCALLOCK *locallock, bool dontWait, int waitSec);
static void SSDmsLockRelease(LOCALLOCK *locallock);

#if defined(LOCK_DEBUG) || defined(USE_ASSERT_CHECKING)

/* ------
 * The following configuration options are available for lock debugging:
 *
 *	   TRACE_LOCKS		-- give a bunch of output what's going on in this file
 *	   TRACE_USERLOCKS	-- same but for user locks
 *	   TRACE_LOCK_OIDMIN-- do not trace locks for tables below this oid
 *						   (use to avoid output on system tables)
 *	   TRACE_LOCK_TABLE -- trace locks on this table (oid) unconditionally
 *	   DEBUG_DEADLOCKS	-- currently dumps locks at untimely occasions ;)
 *
 * Furthermore, but in storage/lmgr/lwlock.c:
 *	   TRACE_LWLOCKS	-- trace lightweight locks (pretty useless)
 *
 * Define LOCK_DEBUG at compile time to get all these enabled.
 * --------
 */
inline static bool LOCK_DEBUG_ENABLED(const LOCKTAG *tag)
{
#ifdef USE_ASSERT_CHECKING
    /* enable all user tables to debug */
    return (u_sess->attr.attr_storage.Debug_deadlocks && (Oid)tag->locktag_field2 >= (Oid)FirstNormalObjectId);
#else /* LOCK_DEBUG */
    return (*(LockMethods[tag->locktag_lockmethodid]->trace_flag) &&
            ((Oid)tag->locktag_field2 >= (Oid)u_sess->attr.attr_storage.Trace_lock_oidmin)) ||
           (u_sess->attr.attr_storage.Trace_lock_table &&
            (tag->locktag_field2 == u_sess->attr.attr_storage.Trace_lock_table));
#endif
}

inline static void LOCK_PRINT(const char *where, const LOCK *lock, LOCKMODE type)
{
    if (LOCK_DEBUG_ENABLED(&lock->tag))
        ereport(LOG,
                (errmsg("%s: lock id(%u,%u,%u,%u,%u,%u,%u) grantMask(%x) "
                        "req(%d,%d,%d,%d,%d,%d,%d)=%d "
                        "grant(%d,%d,%d,%d,%d,%d,%d)=%d wait(%d) type(%s)",
                        where,
                        lock->tag.locktag_field1,
                        lock->tag.locktag_field2,
                        lock->tag.locktag_field5,
                        lock->tag.locktag_field3,
                        lock->tag.locktag_field4,
                        lock->tag.locktag_type,
                        lock->tag.locktag_lockmethodid,
                        lock->grantMask,
                        lock->requested[1],
                        lock->requested[2],
                        lock->requested[3],
                        lock->requested[4],
                        lock->requested[5],
                        lock->requested[6],
                        lock->requested[7],
                        lock->nRequested,
                        lock->granted[1],
                        lock->granted[2],
                        lock->granted[3],
                        lock->granted[4],
                        lock->granted[5],
                        lock->granted[6],
                        lock->granted[7],
                        lock->nGranted,
                        lock->waitProcs.size,
                        LockMethods[LOCK_LOCKMETHOD(*lock)]->lockModeNames[type])));
}

inline static void PROCLOCK_PRINT(const char *where, const PROCLOCK *proclockP)
{
    if (LOCK_DEBUG_ENABLED(&proclockP->tag.myLock->tag))
        ereport(LOG, (errmsg("%s: proclock(%p) lock(%p) method(%u) proc(%p) hold(%x)", where, proclockP,
                             proclockP->tag.myLock, PROCLOCK_LOCKMETHOD(*(proclockP)), proclockP->tag.myProc,
                             (int)proclockP->holdMask)));
}

inline static void SSLOCK_PRINT(const char *where, const LOCALLOCK *locallock, LOCKMODE type)
{
    if (LOCK_DEBUG_ENABLED(&locallock->tag.lock))
        ereport(LOG,
                (errmsg("%s: SSlock id(%u,%u,%u,%u,%u,%u,%u) type(%s)",
                        where,
                        locallock->tag.lock.locktag_field1,
                        locallock->tag.lock.locktag_field2,
                        locallock->tag.lock.locktag_field5,
                        locallock->tag.lock.locktag_field3,
                        locallock->tag.lock.locktag_field4,
                        locallock->tag.lock.locktag_type,
                        locallock->tag.lock.locktag_lockmethodid,
                        LockMethods[SSLOCK_LOCKMETHOD(*locallock)]->lockModeNames[type])));
}
#else /* not LOCK_DEBUG */

#define LOCK_PRINT(where, lock, type)
#define PROCLOCK_PRINT(where, proclockP)
#define SSLOCK_PRINT(where, lock, type)
#endif /* not LOCK_DEBUG */

static uint32 proclock_hash(const void *key, Size keysize);
static void RemoveLocalLock(LOCALLOCK *locallock);
static PROCLOCK *SetupLockInTable(LockMethod lockMethodTable, PGPROC *proc, const LOCKTAG *locktag, uint32 hashcode,
                                  LOCKMODE lockmode);
static void GrantLockLocal(LOCALLOCK *locallock, ResourceOwner owner);
static void BeginStrongLockAcquire(LOCALLOCK *locallock, uint32 fasthashcode);
static void FinishStrongLockAcquire(void);
static void WaitOnLock(LOCALLOCK *locallock, ResourceOwner owner, bool allow_con_update, int waitSec);
static void ReleaseLockIfHeld(LOCALLOCK *locallock, bool sessionLock);
static bool UnGrantLock(LOCK *lock, LOCKMODE lockmode, PROCLOCK *proclock, LockMethod lockMethodTable);
static void CleanUpLock(LOCK *lock, PROCLOCK *proclock, LockMethod lockMethodTable, uint32 hashcode, bool wakeupNeeded);
static void LockRefindAndRelease(LockMethod lockMethodTable, PGPROC *proc, LOCKTAG *locktag, LOCKMODE lockmode,
                                 bool decrement_strong_lock_count);

/*
 * InitLocks -- Initialize the lock manager's data structures.
 *
 * This is called from CreateSharedMemoryAndSemaphores(), which see for
 * more comments.  In the normal postmaster case, the shared hash tables
 * are created here, as well as a locallock hash table that will remain
 * unused and empty in the postmaster itself.  Backends inherit the pointers
 * to the shared tables via fork(), and also inherit an image of the locallock
 * hash table, which they proceed to use.  In the EXEC_BACKEND case, each
 * backend re-executes this code to obtain pointers to the already existing
 * shared hash tables and to create its locallock hash table.
 */
void InitLocks(void)
{
    HASHCTL info;
    int hash_flags;
    long init_table_size, max_table_size;
    bool found = false;

    /*
     * Compute init/max size to request for lock hashtables.  Note these
     * calculations must agree with LockShmemSize!
     */
    max_table_size = NLOCKENTS();
    init_table_size = max_table_size / 2;

    /*
     * Allocate hash table for LOCK structs.  This stores per-locked-object
     * information.
     */
    errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "", "");
    info.keysize = sizeof(LOCKTAG);
    info.entrysize = sizeof(LOCK);
    info.hash = tag_hash;
    info.num_partitions = NUM_LOCK_PARTITIONS;
    hash_flags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);

    t_thrd.storage_cxt.LockMethodLockHash = ShmemInitHash("LOCK hash", init_table_size, max_table_size, &info,
                                                          hash_flags);

    /* Assume an average of 2 holders per lock */
    max_table_size *= 2;
    init_table_size *= 2;

    /*
     * Allocate hash table for PROCLOCK structs.  This stores
     * per-lock-per-holder information.
     */
    info.keysize = sizeof(PROCLOCKTAG);
    info.entrysize = sizeof(PROCLOCK);
    info.hash = proclock_hash;
    info.num_partitions = NUM_LOCK_PARTITIONS;
    hash_flags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);

    t_thrd.storage_cxt.LockMethodProcLockHash = ShmemInitHash("PROCLOCK hash", init_table_size, max_table_size, &info,
                                                              hash_flags);

    /*
     * Allocate fast-path structures.
     */
    t_thrd.storage_cxt.FastPathStrongRelationLocks =
        (FastPathStrongRelationLockData *)ShmemInitStruct("Fast Path Strong Relation Lock Data",
                                                          sizeof(FastPathStrongRelationLockData), &found);
    if (!found)
        SpinLockInit(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);

    /*
     * Allocate non-shared hash table for LOCALLOCK structs.  This stores lock
     * counts and resource owner information.
     *
     * The non-shared table could already exist in this process (this occurs
     * when the postmaster is recreating shared memory after a backend crash).
     * If so, delete and recreate it.  (We could simply leave it, since it
     * ought to be empty in the postmaster, but for safety let's zap it.)
     */
    if (t_thrd.storage_cxt.LockMethodLocalHash)
        hash_destroy(t_thrd.storage_cxt.LockMethodLocalHash);

    info.keysize = sizeof(LOCALLOCKTAG);
    info.entrysize = sizeof(LOCALLOCK);
    info.hash = tag_hash;
    hash_flags = (HASH_ELEM | HASH_FUNCTION);

    t_thrd.storage_cxt.LockMethodLocalHash = hash_create("LOCALLOCK hash", 16, &info, hash_flags);
}

/*
 * Given two lock modes, return whether they would conflict.
 */
    bool
DoLockModesConflict(LOCKMODE mode1, LOCKMODE mode2)
{
    LockMethod      lockMethodTable = LockMethods[DEFAULT_LOCKMETHOD];

    if (lockMethodTable->conflictTab[mode1] & LOCKBIT_ON(mode2))
        return true;

    return false;
}

/*
 * Fetch the lock method table associated with a given lock
 */
LockMethod GetLocksMethodTable(const LOCK *lock)
{
    LOCKMETHODID lockmethodid = LOCK_LOCKMETHOD(*lock);

    Assert(lockmethodid > 0 && lockmethodid < lengthof(LockMethods));
    return LockMethods[lockmethodid];
}

/*
 * Compute the hash code associated with a LOCKTAG.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per function, and then pass it around as needed.  Aside from
 * passing the hashcode to hash_search_with_hash_value(), we can extract
 * the lock partition number from the hashcode.
 */
uint32 LockTagHashCode(const LOCKTAG *locktag)
{
    return get_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (const void *)locktag);
}

/*
 * Compute the hash code associated with a PROCLOCKTAG.
 *
 * Because we want to use just one set of partition locks for both the
 * LOCK and PROCLOCK hash tables, we have to make sure that PROCLOCKs
 * fall into the same partition number as their associated LOCKs.
 * dynahash.c expects the partition number to be the low-order bits of
 * the hash code, and therefore a PROCLOCKTAG's hash code must have the
 * same low-order bits as the associated LOCKTAG's hash code.  We achieve
 * this with this specialized hash function.
 */
static uint32 proclock_hash(const void *key, Size keysize)
{
    const PROCLOCKTAG *proclocktag = (const PROCLOCKTAG *)key;
    Datum procptr;

    Assert(keysize == sizeof(PROCLOCKTAG));

    /* Look into the associated LOCK object, and compute its hash code */
    uint32 lockhash = LockTagHashCode(&proclocktag->myLock->tag);

    /*
     * To make the hash code also depend on the PGPROC, we xor the proc
     * struct's address into the hash code, left-shifted so that the
     * partition-number bits don't change.  Since this is only a hash, we
     * don't care if we lose high-order bits of the address; use an
     * intermediate variable to suppress cast-pointer-to-int warnings.
     */
    procptr = PointerGetDatum(proclocktag->myProc);
    lockhash ^= ((uint32)procptr) << LOG2_NUM_LOCK_PARTITIONS;

    return lockhash;
}

/*
 * Compute the hash code associated with a PROCLOCKTAG, given the hashcode
 * for its underlying LOCK.
 *
 * We use this just to avoid redundant calls of LockTagHashCode().
 */
static inline uint32 ProcLockHashCode(const PROCLOCKTAG *proclocktag, uint32 hashcode)
{
    uint32 lockhash = hashcode;

    /*
     * This must match proclock_hash()!
     */
    Datum procptr = PointerGetDatum(proclocktag->myProc);
    lockhash ^= ((uint32)procptr) << LOG2_NUM_LOCK_PARTITIONS;

    return lockhash;
}

/*
 * LockHasWaiters -- look up 'locktag' and check if releasing this
 *		lock would wake up other processes waiting for it.
 */
bool LockHasWaiters(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LOCALLOCKTAG localtag;
    bool hasWaiters = false;

    CHECK_LOCKMETHODID(lockmethodid);
    LockMethod lockMethodTable = LockMethods[lockmethodid];
    CHECK_LOCKMODE(lockmode, lockMethodTable);

#ifdef LOCK_DEBUG
    if (LOCK_DEBUG_ENABLED(locktag)) {
        ereport(LOG, (errmsg("LockHasWaiters: lock [%u,%u] %s", locktag->locktag_field1, locktag->locktag_field2,
                             lockMethodTable->lockModeNames[lockmode])));
    }
#endif

    /*
     * Find the LOCALLOCK entry for this lock and lockmode
     */
    errno_t rc = memset_s(&localtag, sizeof(localtag), 0, sizeof(localtag)); /* must clear padding */
    securec_check(rc, "", "");
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    LOCALLOCK *locallock =
        (LOCALLOCK *)hash_search(t_thrd.storage_cxt.LockMethodLocalHash, (void *)&localtag, HASH_FIND, NULL);
    /*
     * let the caller print its own error message, too. Do not ereport(ERROR).
     */
    if ((locallock == NULL) || locallock->nLocks <= 0) {
        ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        return false;
    }

    /*
     * Check the shared lock table.
     */
    LWLock *partitionLock = LockHashPartitionLock(locallock->hashcode);

    LWLockAcquire(partitionLock, LW_SHARED);

    /*
     * We don't need to re-find the lock or proclock, since we kept their
     * addresses in the locallock table, and they couldn't have been removed
     * while we were holding a lock on them.
     */
    LOCK *lock = locallock->lock;
    LOCK_PRINT("LockHasWaiters: found", lock, lockmode);
    PROCLOCK *proclock = locallock->proclock;
    PROCLOCK_PRINT("LockHasWaiters: found", proclock);

    /*
     * Double-check that we are actually holding a lock of the type we want to
     * release.
     */
    if (!(proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode))) {
        PROCLOCK_PRINT("LockHasWaiters: WRONGTYPE", proclock);
        LWLockRelease(partitionLock);
        ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        RemoveLocalLock(locallock);
        return false;
    }

    /*
     * Do the checking.
     */
    if ((lockMethodTable->conflictTab[lockmode] & lock->waitMask) != 0)
        hasWaiters = true;

    LWLockRelease(partitionLock);

    return hasWaiters;
}

/*
 * LockAcquire -- Check for lock conflicts, sleep if conflict found,
 *		set lock if/when no conflicts.
 *
 * Inputs:
 *	locktag: unique identifier for the lockable object
 *	lockmode: lock mode to acquire
 *	sessionLock: if true, acquire lock for session not current transaction
 *	dontWait: if true, don't wait to acquire lock
 *
 * Returns one of:
 *		LOCKACQUIRE_NOT_AVAIL		lock not available, and dontWait=true
 *		LOCKACQUIRE_OK				lock successfully acquired
 *		LOCKACQUIRE_ALREADY_HELD	incremented count for lock already held
 *
 * In the normal case where dontWait=false and the caller doesn't need to
 * distinguish a freshly acquired lock from one already taken earlier in
 * this same transaction, there is no need to examine the return value.
 *
 * Side Effects: The lock is acquired and recorded in lock tables.
 *
 * NOTE: if we wait for the lock, there is no way to abort the wait
 * short of aborting the transaction.
 */
LockAcquireResult LockAcquire(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock, bool dontWait,
                              bool allow_con_update, int waitSec)
{
    return LockAcquireExtended(locktag, lockmode, sessionLock, dontWait, true, allow_con_update, waitSec);
}

#ifdef PGXC
/*
 * LockIncrementIfExists - Special purpose case of LockAcquire().
 * This checks if there is already a reference to the lock. If yes,
 * increments it, and returns true. If not, just returns back false.
 * Effectively, it never creates a new lock.
 */
bool LockIncrementIfExists(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    int ret = LockAcquireExtendedXC(locktag, lockmode, sessionLock, true, /* never wait */
        true, true);

    return (ret == LOCKACQUIRE_ALREADY_HELD);
}
#endif

/*
 * LockAcquireExtended - allows us to specify additional options
 *
 * reportMemoryError specifies whether a lock request that fills the
 * lock table should generate an ERROR or not. This allows a priority
 * caller to note that the lock table is full and then begin taking
 * extreme action to reduce the number of other lock holders before
 * retrying the action.
 */
LockAcquireResult LockAcquireExtended(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock, bool dontWait,
                                      bool reportMemoryError, bool allow_con_update, int waitSec)
{
    return LockAcquireExtendedXC(locktag, lockmode, sessionLock, dontWait, reportMemoryError, false, allow_con_update,
                                 waitSec);
}

/*
 * return true if proc is a redistribuition one
 */
bool IsOtherProcRedistribution(PGPROC *otherProc)
{
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[otherProc->pgprocno];
    bool isRedis = false;

    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    if (pgxact->vacuumFlags & PROC_IS_REDIST) {
        isRedis = true;
    }
    LWLockRelease(ProcArrayLock);

    return isRedis;
}

/*
 * when query run as stream mode, the topConsumer and producer thread hold differnt
 * Procs, but we treat them as one transaction
 */
inline bool IsInSameTransaction(PGPROC *proc1, PGPROC *proc2)
{
    return u_sess->stream_cxt.global_obj == NULL ? false
            : u_sess->stream_cxt.global_obj->inNodeGroup(proc1->pid, proc2->pid);
}

static bool IsPrepareXact(const PGPROC *proc)
{
    PGXACT *pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];
    if (pgxact->prepare_xid != InvalidTransactionId) {
        ereport(DEBUG1, (errmsg("IsPrepareXact skip process %lu", proc->pid)));
        return true;
    }
    return false;
}

void CancelConflictLockWaiter(PROCLOCK *proclock, LOCK *lock, LockMethod lockMethodTable, LOCKMODE lockmode)
{
    PROC_QUEUE *waitQueue = &(lock->waitProcs);
    PGPROC *proc = (PGPROC *)waitQueue->links.prev;
    PGPROC *leader1 = (t_thrd.proc->lockGroupLeader == NULL) ? t_thrd.proc : t_thrd.proc->lockGroupLeader;

    /* do nothing if we are not in lock cancle mode */
    if (!t_thrd.xact_cxt.enable_lock_cancel || lockmode < ExclusiveLock) {
        return;
    }
    for (int j = 0; j < waitQueue->size; j++) {
        bool conflictLocks =
            ((lockMethodTable->conflictTab[lockmode] & LOCKBIT_ON((unsigned int)proc->waitLockMode)) != 0);
        PGPROC *leader2 = (proc->lockGroupLeader == NULL) ? proc : proc->lockGroupLeader;
        bool isSameTrans = ((StreamTopConsumerAmI() || StreamThreadAmI()) && IsInSameTransaction(proc, t_thrd.proc)) ||
            (leader1 == leader2);
        /* send term to waitqueue proc while conflict and not in a stream or lock group */
        if (conflictLocks && !isSameTrans && !IsPrepareXact(proc) &&
            proc->pid != 0 && gs_signal_send(proc->pid, SIGTERM) < 0) {
            /* Just a warning to allow multiple callers */
            ereport(WARNING, (errmsg("could not send signal to process %lu: %m", proc->pid)));
        }
        proc = (PGPROC *)proc->links.prev;
    }
}

void CancelConflictLockHolder(PROCLOCK *proclock, LOCK *lock, LockMethod lockMethodTable, LOCKMODE lockmode)
{
    SHM_QUEUE *otherProcLocks = &(lock->procLocks);
    PROCLOCK *otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, otherProcLocks, offsetof(PROCLOCK, lockLink));
    PGPROC *leader1 = (t_thrd.proc->lockGroupLeader == NULL) ? t_thrd.proc : t_thrd.proc->lockGroupLeader;

    /* do nothing if we are not in lock cancle mode */
    if (!t_thrd.xact_cxt.enable_lock_cancel || lockmode < ExclusiveLock) {
        return;
    }
    while (otherProcLock != NULL) {
        if (otherProcLock->tag.myProc->pid != t_thrd.proc->pid && otherProcLock->tag.myProc->pid != 0) {
            bool conflictLocks = ((lockMethodTable->conflictTab[lockmode] & otherProcLock->holdMask) != 0);
            PGPROC *leader2 = (otherProcLock->tag.myProc->lockGroupLeader == NULL) ?
                otherProcLock->tag.myProc : otherProcLock->tag.myProc->lockGroupLeader;
            bool isSameTrans = ((StreamTopConsumerAmI() || StreamThreadAmI()) &&
                IsInSameTransaction(otherProcLock->tag.myProc, t_thrd.proc)) || (leader1 == leader2);
            /* send term to holder proc while conflict and not in a stream or lock group */
            if (conflictLocks && !isSameTrans && !IsPrepareXact(otherProcLock->tag.myProc) &&
                otherProcLock->tag.myProc->pid != 0 && gs_signal_send(otherProcLock->tag.myProc->pid, SIGTERM) < 0) {
                /* Just a warning to allow multiple callers */
                ereport(WARNING,
                    (errmsg("could not send signal to process %lu: %m", otherProcLock->tag.myProc->pid)));
            }
        }
        /* otherProcLock iterate to next. */
        otherProcLock =
            (PROCLOCK *)SHMQueueNext(otherProcLocks, &otherProcLock->lockLink, offsetof(PROCLOCK, lockLink));
    }
}

/*
 * LockAcquireExtendedXC - additional parameter only_increment. This is XC
 * specific. Check comments for the function LockIncrementIfExists()
 */
static LockAcquireResult LockAcquireExtendedXC(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock,
                                               bool dontWait, bool reportMemoryError, bool only_increment,
                                               bool allow_con_update, int waitSec)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCALLOCKTAG localtag;
    LOCALLOCK *locallock = NULL;
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    bool found = false;
    ResourceOwner owner = NULL;
    uint32 hashcode;
    LWLock *partitionLock = NULL;
    int status;
    bool log_lock = false;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    CHECK_LOCKMODE(lockmode, lockMethodTable);

    if (RecoveryInProgress() && !t_thrd.xlog_cxt.InRecovery &&
        (locktag->locktag_type == LOCKTAG_OBJECT || locktag->locktag_type == LOCKTAG_RELATION) &&
        lockmode > RowExclusiveLock)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot acquire lock mode %s on database objects while recovery is in progress",
                               lockMethodTable->lockModeNames[lockmode]),
                        errhint("Only RowExclusiveLock or less can be acquired on database objects during recovery.")));

#ifdef LOCK_DEBUG
    if (LOCK_DEBUG_ENABLED(locktag))
        ereport(LOG, (errmsg("LockAcquire: lock [%u,%u] %s", locktag->locktag_field1, locktag->locktag_field2,
                             lockMethodTable->lockModeNames[lockmode])));
#endif
    instr_stmt_report_lock(LOCK_START, lockmode, locktag);

    /* Identify owner for lock */
    if (!sessionLock) {
        owner = t_thrd.utils_cxt.CurrentResourceOwner;
    }

    /*
     * Find or create a LOCALLOCK entry for this lock and lockmode
     */
    errno_t rc = memset_s(&localtag, sizeof(localtag), 0, sizeof(localtag)); /* must clear padding */
    securec_check(rc, "", "");
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = (LOCALLOCK *)hash_search(t_thrd.storage_cxt.LockMethodLocalHash, (void *)&localtag, HASH_ENTER, &found);

    /*
     * if it's a new locallock object, initialize it
     */
    START_CRIT_SECTION();
    if (!found) {
        locallock->lock = NULL;
        locallock->proclock = NULL;
        locallock->hashcode = LockTagHashCode(&(localtag.lock));
        locallock->nLocks = 0;
        locallock->numLockOwners = 0;
        locallock->maxLockOwners = 8;
        locallock->holdsStrongLockCount = FALSE;
        locallock->ssLock = FALSE;
        locallock->lockOwners = NULL; /* in case next line fails */
        locallock->lockOwners =
            (LOCALLOCKOWNER *)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                 locallock->maxLockOwners * sizeof(LOCALLOCKOWNER));
    } else {
        if (locallock->lockOwners == NULL) {
            ereport(PANIC, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Failed to allocate memory for lockOwners.")));
        }

        /* Make sure there will be room to remember the lock */
        if (locallock->numLockOwners >= locallock->maxLockOwners) {
            int newsize = locallock->maxLockOwners * 2;

            locallock->lockOwners = (LOCALLOCKOWNER *)repalloc(locallock->lockOwners, newsize * sizeof(LOCALLOCKOWNER));
            locallock->maxLockOwners = newsize;
        }
    }
    hashcode = locallock->hashcode;
    END_CRIT_SECTION();

    /*
     * If we already hold the lock, we can just increase the count locally.
     */
    if (locallock->nLocks > 0) {
        GrantLockLocal(locallock, owner);
        instr_stmt_report_lock(LOCK_END, lockmode);
        return LOCKACQUIRE_ALREADY_HELD;
#ifdef PGXC
    } else if (only_increment) {
        instr_stmt_report_lock(LOCK_END, NoLock);
        /* User does not want to create new lock if it does not already exist */
        return LOCKACQUIRE_NOT_AVAIL;
#endif
    }
    /*
     * Emit a WAL record if acquisition of this lock needs to be replayed in a
     * standby server. Only AccessExclusiveLocks can conflict with lock types
     * that read-only transactions can acquire in a standby server.
     *
     * Make sure this definition matches the one in GetRunningTransactionLocks().
     *
     * First we prepare to log, then after lock acquired we issue log record.
     */
    if (!SS_SINGLE_CLUSTER && lockmode >= AccessExclusiveLock &&
        (locktag->locktag_type == LOCKTAG_RELATION || locktag->locktag_type == LOCKTAG_PARTITION ||
         locktag->locktag_type == LOCKTAG_PARTITION_SEQUENCE || locktag->locktag_type == LOCKTAG_OBJECT) &&
        !RecoveryInProgress() && XLogStandbyInfoActive()) {
        LogAccessExclusiveLockPrepare();
        log_lock = true;
    }

    /* First we try to get dms lock in shared storage mode */
    if (ENABLE_DMS && (locktag->locktag_type < (uint8)LOCKTAG_PAGE || locktag->locktag_type == (uint8)LOCKTAG_OBJECT) &&
        !RecoveryInProgress()) {
        bool ret = SSDmsLockAcquire(locallock, dontWait, waitSec);
        if (!ret) {
            instr_stmt_report_lock(LOCK_END, NoLock);
            return LOCKACQUIRE_NOT_AVAIL;
        }
    }
    /*
     * Attempt to take lock via fast path, if eligible.  But if we remember
     * having filled up the fast path array, we don't attempt to make any
     * further use of it until we release some locks.  It's possible that some
     * other backend has transferred some of those locks to the shared hash
     * table, leaving space free, but it's not worth acquiring the LWLock just
     * to check.  It's also possible that we're acquiring a second or third
     * lock type on a relation we have already locked using the fast-path, but
     * for now we don't worry about that case either.
     */
    if (EligibleForRelationFastPath(locktag, lockmode) &&
        (uint32)t_thrd.storage_cxt.FastPathLocalUseCount < FP_LOCK_SLOTS_PER_BACKEND) {
        uint32 fasthashcode = FastPathStrongLockHashPartition(hashcode);
        bool acquired = false;

        /*
         * LWLockAcquire acts as a memory sequencing point, so it's safe to
         * assume that any strong locker whose increment to
         * FastPathStrongRelationLocks->counts becomes visible after we test
         * it has yet to begin to transfer fast-path locks.
         */
        LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);
        if (t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode] != 0)
            acquired = false;
        else {
            FastPathTag tag = { locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3 };
            acquired = FastPathGrantRelationLock(tag, lockmode);
        }

        LWLockRelease(t_thrd.proc->backendLock);
        if (acquired) {
            /*
             * The locallock might contain stale pointers to some old shared
             * objects; we MUST reset these to null before considering the
             * lock to be acquired via fast-path.
             */
            locallock->lock = NULL;
            locallock->proclock = NULL;
            GrantLockLocal(locallock, owner);
            instr_stmt_report_lock(LOCK_END, lockmode);
            return LOCKACQUIRE_OK;
        }
    }

    /*
     * If this lock could potentially have been taken via the fast-path by
     * some other backend, we must (temporarily) disable further use of the
     * fast-path for this lock tag, and migrate any locks already taken via
     * this method to the main lock table.
     */
    if (ConflictsWithRelationFastPath(locktag, lockmode)) {
        uint32 fasthashcode = FastPathStrongLockHashPartition(hashcode);

        BeginStrongLockAcquire(locallock, fasthashcode);
        if (!FastPathTransferRelationLocks(lockMethodTable, locktag, hashcode)) {
            AbortStrongLockAcquire();
            SSDmsLockRelease(locallock);
            instr_stmt_report_lock(LOCK_END, NoLock);
            if (reportMemoryError)
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                    errhint("You might need to increase max_locks_per_transaction.")));
            else
                return LOCKACQUIRE_NOT_AVAIL;
        }
    }

    /*
     * We didn't find the lock in our LOCALLOCK table, and we didn't manage to
     * take it via the fast-path, either, so we've got to mess with the shared
     * lock table.
     */
    partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Find or create lock and proclock entries with this tag
     *
     * Note: if the locallock object already existed, it might have a pointer
     * to the lock already ... but we should not assume that that pointer is
     * valid, since a lock object with zero hold and request counts can go
     * away anytime.  So we have to use SetupLockInTable() to recompute the
     * lock and proclock pointers, even if they're already set.
     */
    proclock = SetupLockInTable(lockMethodTable, t_thrd.proc, locktag, hashcode, lockmode);
    if (proclock == NULL) {
        AbortStrongLockAcquire();
        LWLockRelease(partitionLock);
        SSDmsLockRelease(locallock);
        instr_stmt_report_lock(LOCK_END, NoLock);
        if (reportMemoryError)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                            errhint("You might need to increase max_locks_per_transaction.")));
        else
            return LOCKACQUIRE_NOT_AVAIL;
    }
    locallock->proclock = proclock;
    lock = proclock->tag.myLock;
    locallock->lock = lock;

    /*
     * If lock requested conflicts with locks requested by waiters, must join
     * wait queue.	Otherwise, check for conflict with already-held locks.
     * (That's last because most complex check.)
     */
    if (lockMethodTable->conflictTab[lockmode] & lock->waitMask) {
        status = STATUS_FOUND;

        /*
         * Here we check the waitQueue backwardly, and find the first waiting proc whose requesting lock
         * conflicted with lock requested by us. Then we record the proc information for deacklock report.
         * Notice: Lock we requesetd may conflict many locks requested by waiters, we just choice one
         * for report.
         */
        PGPROC *proc = NULL;
        PROC_QUEUE *waitQueue = &(lock->waitProcs);
        int j;
        proc = (PGPROC *)waitQueue->links.prev;
        for (j = 0; j < waitQueue->size; j++) {
            if (lockMethodTable->conflictTab[lockmode] & LOCKBIT_ON((unsigned int)proc->waitLockMode)) {
                ProcBlockerUpdate(proclock->tag.myProc,
                    proc->waitProcLock, lockMethodTable->lockModeNames[proc->waitLockMode], false);
                break;
            }
            proc = (PGPROC *)proc->links.prev;
        }

        /* above must be a normal break. */
        Assert(!t_thrd.storage_cxt.conflicting_lock_by_holdlock);

        /*
         * Before we give up we have to check a special case.
         * In the event of drop/truncate table acquiring exclusive lock
         * while there is a redistribution proc in the lock->waitProcs,
         * we takes high priority from redistribution and cancel it first
         * so we will get the lock when it is our turn and no need to wait
         * forever for the redistribuition proc finish
         */
        if (u_sess->catalog_cxt.redistribution_cancelable && lockmode == AccessExclusiveLock) {
            PROC_QUEUE *wait_queue = &(lock->waitProcs);
            int queue_size = wait_queue->size;
            PGPROC *otherProc = (PGPROC *)wait_queue->links.next;

            /*
             * Traverse the waitQueue and find the redistribuition proc if any
             */
            while (queue_size-- > 0) {
                if (IsOtherProcRedistribution(otherProc)) {
                    /*
                     * There should be only one redistribuition proc and
                     * blocking_redistribution_proc has not been set yet.
                     */
                    Assert(t_thrd.storage_cxt.blocking_redistribution_proc == NULL);
                    t_thrd.storage_cxt.blocking_redistribution_proc = otherProc;
                    status = STATUS_FOUND_NEED_CANCEL;
                    break;
                }
                otherProc = (PGPROC *)otherProc->links.next;
            }
        }
    } else {
        status = LockCheckConflicts(lockMethodTable, lockmode, lock, proclock, t_thrd.proc);
    }

    if (status == STATUS_OK) {
        /* No conflict with held or previously requested locks */
        GrantLock(lock, proclock, lockmode);
        GrantLockLocal(locallock, owner);
    } else {
        Assert(status == STATUS_FOUND || status == STATUS_FOUND_NEED_CANCEL);

        /*
         * We can't acquire the lock immediately.  If caller specified no
         * blocking, remove useless table entries and return NOT_AVAIL without
         * waiting.
         */
        if (dontWait) {
            AbortStrongLockAcquire();
            if (proclock->holdMask == 0) {
                uint32 proclock_hashcode;

                proclock_hashcode = ProcLockHashCode(&proclock->tag, hashcode);
                SHMQueueDelete(&proclock->lockLink);
                SHMQueueDelete(&proclock->procLink);
                if (!hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&(proclock->tag),
                                                 proclock_hashcode, HASH_REMOVE, NULL)) {
                    ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proclock table corrupted")));
                }
            } else {
                PROCLOCK_PRINT("LockAcquire: NOWAIT", proclock);
            }
            lock->nRequested--;
            lock->requested[lockmode]--;
            LOCK_PRINT("LockAcquire: conditional lock failed", lock, lockmode);
            Assert((lock->nRequested > 0) && (lock->requested[lockmode] >= 0));
            Assert(lock->nGranted <= lock->nRequested);
            LWLockRelease(partitionLock);
            if (locallock->nLocks == 0) {
                RemoveLocalLock(locallock);
            }
            instr_stmt_report_lock(LOCK_END, NoLock);
            return LOCKACQUIRE_NOT_AVAIL;
        }

        if (status == STATUS_FOUND_NEED_CANCEL) {
            CancelBlockedRedistWorker(lock, lockmode);
            u_sess->catalog_cxt.redistribution_cancelable = false;
        } else {
            /* do blocker cancle if needed */
            CancelConflictLockWaiter(proclock, lock, lockMethodTable, lockmode);
            CancelConflictLockHolder(proclock, lock, lockMethodTable, lockmode);
        }

        /*
         * Set bitmask of locks this process already holds on this object.
         */
        t_thrd.proc->heldLocks = proclock->holdMask;

        /*
         * Sleep till someone wakes me up.
         */
        TRACE_POSTGRESQL_LOCK_WAIT_START(locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3,
                                         locktag->locktag_field4, locktag->locktag_type, lockmode);

        WaitOnLock(locallock, owner, allow_con_update, waitSec);

        TRACE_POSTGRESQL_LOCK_WAIT_DONE(locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3,
                                        locktag->locktag_field4, locktag->locktag_type, lockmode);

        /*
         * NOTE: do not do any material change of state between here and
         * return.	All required changes in locktable state must have been
         * done when the lock was granted to us --- see notes in WaitOnLock.
         *
         * Check the proclock entry status, in case something in the ipc
         * communication doesn't work correctly.
         */
        if (!(proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode))) {
            AbortStrongLockAcquire();
            PROCLOCK_PRINT("LockAcquire: INCONSISTENT", proclock);
            LOCK_PRINT("LockAcquire: INCONSISTENT", lock, lockmode);
            /* Should we retry ? */
            LWLockRelease(partitionLock);
            SSDmsLockRelease(locallock);
            instr_stmt_report_lock(LOCK_END, NoLock);
            ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("LockAcquire failed")));
        }
        PROCLOCK_PRINT("LockAcquire: granted", proclock);
        LOCK_PRINT("LockAcquire: granted", lock, lockmode);
    }

    /*
     * Lock state is fully up-to-date now; if we error out after this, no
     * special error cleanup is required.
     */
    FinishStrongLockAcquire();

    LWLockRelease(partitionLock);

    /*
     * Emit a WAL record if acquisition of this lock need to be replayed in a
     * standby server.
     */
    if (log_lock) {
        /*
         * Decode the locktag back to the original values, to avoid sending
         * lots of empty bytes with every message.	See lock.h to check how a
         * locktag is defined for LOCKTAG_RELATION
         */
        uint32 seq = InvalidOid;
        if (locktag->locktag_type == LOCKTAG_PARTITION || locktag->locktag_type == LOCKTAG_PARTITION_SEQUENCE ||
            locktag->locktag_type == LOCKTAG_OBJECT) {
            seq = locktag->locktag_field3;
        }
        LogAccessExclusiveLock(locktag->locktag_field1, locktag->locktag_field2, seq);
    }

    instr_stmt_report_lock(LOCK_END, lockmode);
    return LOCKACQUIRE_OK;
}

/*
 * Find or create LOCK and PROCLOCK objects as needed for a new lock
 * request.
 *
 * Returns the PROCLOCK object, or NULL if we failed to create the objects
 * for lack of shared memory.
 *
 * The appropriate partition lock must be held at entry, and will be
 * held at exit.
 */
static PROCLOCK *SetupLockInTable(LockMethod lockMethodTable, PGPROC *proc, const LOCKTAG *locktag, uint32 hashcode,
                                  LOCKMODE lockmode)
{
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    PROCLOCKTAG proclocktag;
    uint32 proclock_hashcode;
    bool found = false;
    errno_t rc = EOK;

    /*
     * Find or create a lock with this tag.
     */
    lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (const void *)locktag, hashcode,
                                               HASH_ENTER_NULL, &found);
    if (lock == NULL)
        return NULL;

    /*
     * if it's a new lock object, initialize it
     */
    if (!found) {
        lock->grantMask = 0;
        lock->waitMask = 0;
        SHMQueueInit(&(lock->procLocks));
        ProcQueueInit(&(lock->waitProcs));
        lock->nRequested = 0;
        lock->nGranted = 0;
        rc = memset_s(lock->requested, sizeof(lock->requested), 0, sizeof(lock->requested));
        securec_check(rc, "", "");
        rc = memset_s(lock->granted, sizeof(lock->granted), 0, sizeof(lock->granted));
        securec_check(rc, "", "");
        LOCK_PRINT("LockAcquire: new", lock, lockmode);
    } else {
        LOCK_PRINT("LockAcquire: found", lock, lockmode);
        Assert((lock->nRequested >= 0) && (lock->requested[lockmode] >= 0));
        Assert((lock->nGranted >= 0) && (lock->granted[lockmode] >= 0));
        Assert(lock->nGranted <= lock->nRequested);
    }

    /*
     * Create the hash key for the proclock table.
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc;

    proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

    /*
     * Find or create a proclock entry with this tag
     */
    proclock = (PROCLOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&proclocktag,
                                                       proclock_hashcode, HASH_ENTER_NULL, &found);
    if (proclock == NULL) {
        /* Ooops, not enough shmem for the proclock */
        if (lock->nRequested == 0) {
            /*
             * There are no other requestors of this lock, so garbage-collect
             * the lock object.  We *must* do this to avoid a permanent leak
             * of shared memory, because there won't be anything to cause
             * anyone to release the lock object later.
             */
            Assert(SHMQueueEmpty(&(lock->procLocks)));
            if (!hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)&(lock->tag), hashcode,
                                             HASH_REMOVE, NULL))
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("lock table corrupted")));
        }
        return NULL;
    }

    /*
     * If new, initialize the new entry
     */
    if (!found) {
        uint32 partition = LockHashPartition(hashcode);

        /*
         * It might seem unsafe to access proclock->groupLeader without a
         * lock, but it's not really.  Either we are initializing a proclock
         * on our own behalf, in which case our group leader isn't changing
         * because the group leader for a process can only ever be changed by
         * the process itself; or else we are transferring a fast-path lock to
         * the main lock table, in which case that process can't change it's
         * lock group leader without first releasing all of its locks (and in
         * particular the one we are currently transferring).
         */
        proclock->groupLeader = (proc->lockGroupLeader != NULL) ? proc->lockGroupLeader : proc;

        proclock->holdMask = 0;
        proclock->releaseMask = 0;
        /* Add proclock to appropriate lists */
        SHMQueueInsertBefore(&lock->procLocks, &proclock->lockLink);
        SHMQueueInsertBefore(&(proc->myProcLocks[partition]), &proclock->procLink);
        PROCLOCK_PRINT("LockAcquire: new", proclock);
    } else {
        PROCLOCK_PRINT("LockAcquire: found", proclock);
        Assert((proclock->holdMask & ~lock->grantMask) == 0);

#ifdef CHECK_DEADLOCK_RISK

        /*
         * Issue warning if we already hold a lower-level lock on this object
         * and do not hold a lock of the requested level or higher. This
         * indicates a deadlock-prone coding practice (eg, we'd have a
         * deadlock if another backend were following the same code path at
         * about the same time).
         *
         * This is not enabled by default, because it may generate log entries
         * about user-level coding practices that are in fact safe in context.
         * It can be enabled to help find system-level problems.
         *
         * XXX Doing numeric comparison on the lockmodes is a hack; it'd be
         * better to use a table.  For now, though, this works.
         */
        {
            int i;

            for (i = lockMethodTable->numLockModes; i > 0; i--) {
                if (proclock->holdMask & LOCKBIT_ON(i)) {
                    if (i >= (int)lockmode)
                        break; /* safe: we have a lock >= req level */
                    ereport(LOG,
                            (errmsg("deadlock risk: raising lock level"
                                    " from %s to %s on object %u/%u/%u",
                                    lockMethodTable->lockModeNames[i], lockMethodTable->lockModeNames[lockmode],
                                    lock->tag.locktag_field1, lock->tag.locktag_field2, lock->tag.locktag_field3)));
                    break;
                }
            }
        }
#endif /* CHECK_DEADLOCK_RISK */
    }

    /*
     * lock->nRequested and lock->requested[] count the total number of
     * requests, whether granted or waiting, so increment those immediately.
     * The other counts don't increment till we get the lock.
     */
    lock->nRequested++;
    lock->requested[lockmode]++;
    Assert((lock->nRequested > 0) && (lock->requested[lockmode] > 0));

    /*
     * We shouldn't already hold the desired lock; else locallock table is
     * broken.
     */
    if (proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("lock %s on object %u/%u/%u is already held", lockMethodTable->lockModeNames[lockmode],
                               lock->tag.locktag_field1, lock->tag.locktag_field2, lock->tag.locktag_field3)));
    }

    return proclock;
}

/*
 * Subroutine to free a locallock entry
 */
static void RemoveLocalLock(LOCALLOCK *locallock)
{
    HOLD_INTERRUPTS();
    locallock->numLockOwners = 0;
    if (locallock->lockOwners)
        pfree(locallock->lockOwners);
    locallock->lockOwners = NULL;
    RESUME_INTERRUPTS();
    if (locallock->holdsStrongLockCount) {
        uint32 fasthashcode;

        fasthashcode = FastPathStrongLockHashPartition(locallock->hashcode);

        SpinLockAcquire(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
        Assert(t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode] > 0);
        t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode]--;
        locallock->holdsStrongLockCount = FALSE;
        SpinLockRelease(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
    }
    if (!hash_search(t_thrd.storage_cxt.LockMethodLocalHash, (void *)&(locallock->tag), HASH_REMOVE, NULL))
        ereport(WARNING, (errmsg("locallock table corrupted")));

    SSDmsLockRelease(locallock);
}

inline  bool IsInSameLockGroup(const PROCLOCK *proclock1, const PROCLOCK *proclock2)
{
    Assert(proclock1->groupLeader != t_thrd.proc || t_thrd.proc->lockGroupLeader != NULL);
    return proclock1 != proclock2 && proclock1->groupLeader == proclock2->groupLeader;
}

/*
 * LockCheckConflicts -- test whether requested lock conflicts
 *		with those already granted
 *
 * Returns STATUS_FOUND if conflict, STATUS_OK if no conflict.
 *
 * NOTES:
 *		Here's what makes this complicated: one process's locks don't
 * conflict with one another, no matter what purpose they are held for
 * (eg, session and transaction locks do not conflict).
 * So, we must subtract off our own locks when determining whether the
 * requested new lock conflicts with those already held.
 */
int LockCheckConflicts(LockMethod lockMethodTable, LOCKMODE lockmode, LOCK *lock, PROCLOCK *proclock, PGPROC *proc)
{
    int numLockModes = lockMethodTable->numLockModes;
    LOCKMASK myLocks;
    LOCKMASK otherLocks;
    int i;
    LOCKMASK conflictLocks = 0;

    /*
     * first check for global conflicts: If no locks conflict with my request,
     * then I get the lock.
     *
     * Checking for conflict: lock->grantMask represents the types of
     * currently held locks.  conflictTable[lockmode] has a bit set for each
     * type of lock that conflicts with request.   Bitwise compare tells if
     * there is a conflict.
     */
    if (!(lockMethodTable->conflictTab[lockmode] & lock->grantMask)) {
        PROCLOCK_PRINT("LockCheckConflicts: no conflict", proclock);
        return STATUS_OK;
    }

    /*
     * Rats.  Something conflicts.	But it could still be my own lock. We have
     * to construct a conflict mask that does not reflect our own locks, but
     * only lock types held by other processes.
     */
    myLocks = proclock->holdMask;
    otherLocks = 0;

    bool inLockGroup = (proclock->groupLeader != t_thrd.proc || t_thrd.proc->lockGroupLeader != NULL);
    for (i = 1; i <= numLockModes; i++) {
        int myHolding = (myLocks & LOCKBIT_ON((unsigned int)i)) ? 1 : 0;

        /*
         * When query runs as Streaming mode, the consumer thread and produce
         * thread is in one transaction, but these threads use differnt procs.
         * We need treat these procs as one proc
         */
        if (StreamTopConsumerAmI() || StreamThreadAmI() || inLockGroup) {
            SHM_QUEUE *otherProcLocks = &(lock->procLocks);
            PROCLOCK *otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, otherProcLocks,
                                                               offsetof(PROCLOCK, lockLink));
            while (otherProcLock != NULL) {
                if ((!inLockGroup && IsInSameTransaction(otherProcLock->tag.myProc, proc))
                    || (inLockGroup && IsInSameLockGroup(proclock, otherProcLock))) {
                    if (otherProcLock->holdMask & LOCKBIT_ON((unsigned int)i))
                        ++myHolding;
                }
                otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, &otherProcLock->lockLink,
                                                         offsetof(PROCLOCK, lockLink));
            }
        }
        if (lock->granted[i] > myHolding)
            otherLocks |= LOCKBIT_ON((unsigned int)i);
    }

    /* find the conflicting thread and lts lock mode. */
    conflictLocks = 0;
    SHM_QUEUE *otherProcLocks = &(lock->procLocks);
    PROCLOCK *otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, otherProcLocks, offsetof(PROCLOCK, lockLink));
    while (otherProcLock != NULL) {
        if (otherProcLock->tag.myProc->pid != proc->pid) {
            conflictLocks = (lockMethodTable->conflictTab[lockmode] & otherProcLock->holdMask);

            /* lock conflicts and not is a stream group */
            if (conflictLocks && (inLockGroup || !IsInSameTransaction(otherProcLock->tag.myProc, proc)) && 
                (!inLockGroup || !IsInSameLockGroup(proclock, otherProcLock))) {
                int lock_idx = 1;
                for (; lock_idx <= numLockModes; lock_idx++) {
                    if (conflictLocks & LOCKBIT_ON((unsigned int)lock_idx))
                        break;
                }
                /* must be normal break; */
                Assert(lock_idx <= numLockModes);

                /* remember the conficting thread, for output by ProcSleep when DS_LOCK_TIMEOUT. */
                ProcBlockerUpdate(proc, otherProcLock, lockMethodTable->lockModeNames[lock_idx], true);

                /* just find one, and stop. */
                break;
            }
        }

        /* otherProcLock iterate to next. */
        otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, &otherProcLock->lockLink,
                                                 offsetof(PROCLOCK, lockLink));
    }

    /*
     * Check if we could cancel the redistribution proc when we are doing
     * drop/truncate tables. We only can cancel it if there is a redis proc
     * blocking us from locking the object. Drop/truncate table always
     * acquire for lockmode=8 lock, which should conflict with all other holdMask.
     *
     * Do not want to mix the code below in above loops
     */
    if (u_sess->catalog_cxt.redistribution_cancelable && lockmode == AccessExclusiveLock) {
        otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, otherProcLocks, offsetof(PROCLOCK, lockLink));
        while (otherProcLock != NULL) {
            /*
             * A proce can never block itself
             * lockmode 8 conflicts with all other proc hold mask (this condition is given and can be removed)
             * otherProc is redistribuition
             */
            if (otherProcLock->tag.myProc->pid != proc->pid &&
                (lockMethodTable->conflictTab[lockmode] & otherProcLock->holdMask) &&
                IsOtherProcRedistribution(otherProcLock->tag.myProc)) {
                t_thrd.storage_cxt.blocking_redistribution_proc = otherProcLock->tag.myProc;
            }

            otherProcLock = (PROCLOCK *)SHMQueueNext(otherProcLocks, &otherProcLock->lockLink,
                                                     offsetof(PROCLOCK, lockLink));
        }
    }

    /*
     * now check again for conflicts.  'otherLocks' describes the types of
     * locks held by other processes.  If one of these conflicts with the kind
     * of lock that I want, there is a conflict and I have to sleep.
     */
    if (!(lockMethodTable->conflictTab[lockmode] & otherLocks)) {
        /* no conflict. OK to get the lock */
        PROCLOCK_PRINT("LockCheckConflicts: resolved", proclock);
        return STATUS_OK;
    } else if (t_thrd.storage_cxt.blocking_redistribution_proc) {
        if (IsOtherProcRedistribution(t_thrd.storage_cxt.blocking_redistribution_proc)) {
            PROCLOCK_PRINT("LockCheckConflicts: conflicting will cancel redistribution xact.", proclock);
            return STATUS_FOUND_NEED_CANCEL;
        } else {
            t_thrd.storage_cxt.blocking_redistribution_proc = NULL;
        }
    }

    PROCLOCK_PRINT("LockCheckConflicts: conflicting", proclock);
    return STATUS_FOUND;
}

/*
 * GrantLock -- update the lock and proclock data structures to show
 *		the lock request has been granted.
 *
 * NOTE: if proc was blocked, it also needs to be removed from the wait list
 * and have its waitLock/waitProcLock fields cleared.  That's not done here.
 *
 * NOTE: the lock grant also has to be recorded in the associated LOCALLOCK
 * table entry; but since we may be awaking some other process, we can't do
 * that here; it's done by GrantLockLocal, instead.
 */
void GrantLock(LOCK *lock, PROCLOCK *proclock, LOCKMODE lockmode)
{
    lock->nGranted++;
    lock->granted[lockmode]++;
    lock->grantMask |= LOCKBIT_ON((unsigned int)lockmode);
    if (lock->granted[lockmode] == lock->requested[lockmode]) {
        lock->waitMask &= LOCKBIT_OFF((unsigned int)lockmode);
    }
    proclock->holdMask |= LOCKBIT_ON((unsigned int)lockmode);
    LOCK_PRINT("GrantLock", lock, lockmode);
    Assert((lock->nGranted > 0) && (lock->granted[lockmode] > 0));
    Assert(lock->nGranted <= lock->nRequested);
}

/*
 * UnGrantLock -- opposite of GrantLock.
 *
 * Updates the lock and proclock data structures to show that the lock
 * is no longer held nor requested by the current holder.
 *
 * Returns true if there were any waiters waiting on the lock that
 * should now be woken up with ProcLockWakeup.
 */
static bool UnGrantLock(LOCK *lock, LOCKMODE lockmode, PROCLOCK *proclock, LockMethod lockMethodTable)
{
    bool wakeupNeeded = false;

    Assert((lock->nRequested > 0) && (lock->requested[lockmode] > 0));
    Assert((lock->nGranted > 0) && (lock->granted[lockmode] > 0));
    Assert(lock->nGranted <= lock->nRequested);

    /*
     * fix the general lock stats
     */
    lock->nRequested--;
    lock->requested[lockmode]--;
    lock->nGranted--;
    lock->granted[lockmode]--;

    if (lock->granted[lockmode] == 0) {
        /* change the conflict mask.  No more of this lock type. */
        lock->grantMask &= LOCKBIT_OFF((unsigned int)lockmode);
    }

    LOCK_PRINT("UnGrantLock: updated", lock, lockmode);

    /*
     * We need only run ProcLockWakeup if the released lock conflicts with at
     * least one of the lock types requested by waiter(s).	Otherwise whatever
     * conflict made them wait must still exist.  NOTE: before MVCC, we could
     * skip wakeup if lock->granted[lockmode] was still positive. But that's
     * not true anymore, because the remaining granted locks might belong to
     * some waiter, who could now be awakened because he doesn't conflict with
     * his own locks.
     */
    if (lockMethodTable->conflictTab[lockmode] & lock->waitMask) {
        wakeupNeeded = true;
    }

    /*
     * Now fix the per-proclock state.
     */
    proclock->holdMask &= LOCKBIT_OFF((unsigned int)lockmode);
    PROCLOCK_PRINT("UnGrantLock: updated", proclock);

    return wakeupNeeded;
}

/*
 * CleanUpLock -- clean up after releasing a lock.	We garbage-collect the
 * proclock and lock objects if possible, and call ProcLockWakeup if there
 * are remaining requests and the caller says it's OK.  (Normally, this
 * should be called after UnGrantLock, and wakeupNeeded is the result from
 * UnGrantLock.)
 *
 * The appropriate partition lock must be held at entry, and will be
 * held at exit.
 */
static void CleanUpLock(LOCK *lock, PROCLOCK *proclock, LockMethod lockMethodTable, uint32 hashcode, bool wakeupNeeded)
{
    /*
     * If this was my last hold on this lock, delete my entry in the proclock
     * table.
     */
    if (proclock->holdMask == 0) {
        uint32 proclock_hashcode;

        PROCLOCK_PRINT("CleanUpLock: deleting", proclock);
        SHMQueueDelete(&proclock->lockLink);
        SHMQueueDelete(&proclock->procLink);
        proclock_hashcode = ProcLockHashCode(&proclock->tag, hashcode);
        if (!hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&(proclock->tag),
                                         proclock_hashcode, HASH_REMOVE, NULL)) {
            ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proclock table corrupted")));
        }
    }

    if (lock->nRequested == 0) {
        /*
         * The caller just released the last lock, so garbage-collect the lock
         * object.
         */
        LOCK_PRINT("CleanUpLock: deleting", lock, 0);
        Assert(SHMQueueEmpty(&(lock->procLocks)));
        if (!hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)&(lock->tag), hashcode,
                                         HASH_REMOVE, NULL))
            ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("lock table corrupted")));
    } else if (wakeupNeeded) {
        /* There are waiters on this lock, so wake them up. */
        ProcLockWakeup(lockMethodTable, lock, proclock);
    }
}

/*
 * GrantLockLocal -- update the locallock data structures to show
 *		the lock request has been granted.
 *
 * We expect that LockAcquire made sure there is room to add a new
 * ResourceOwner entry.
 */
static void GrantLockLocal(LOCALLOCK *locallock, ResourceOwner owner)
{
    LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
    int i;

    Assert(locallock->numLockOwners < locallock->maxLockOwners);
    /* Count the total */
    locallock->nLocks++;
    /* Count the per-owner lock */
    for (i = 0; i < locallock->numLockOwners; i++) {
        if (lockOwners[i].owner == owner) {
            lockOwners[i].nLocks++;
            return;
        }
    }
    lockOwners[i].owner = owner;
    lockOwners[i].nLocks = 1;
    locallock->numLockOwners++;
}

/*
 * BeginStrongLockAcquire - inhibit use of fastpath for a given LOCALLOCK,
 * and arrange for error cleanup if it fails
 */
static void BeginStrongLockAcquire(LOCALLOCK *locallock, uint32 fasthashcode)
{
    Assert(t_thrd.storage_cxt.StrongLockInProgress == NULL);
    Assert(locallock->holdsStrongLockCount == FALSE);

    /*
     * Adding to a memory location is not atomic, so we take a spinlock to
     * ensure we don't collide with someone else trying to bump the count at
     * the same time.
     *
     * XXX: It might be worth considering using an atomic fetch-and-add
     * instruction here, on architectures where that is supported.
     */
    SpinLockAcquire(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
    t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode]++;
    locallock->holdsStrongLockCount = TRUE;
    t_thrd.storage_cxt.StrongLockInProgress = locallock;
    SpinLockRelease(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
}

/*
 * FinishStrongLockAcquire - cancel pending cleanup for a strong lock
 * acquisition once it's no longer needed
 */
static void FinishStrongLockAcquire(void)
{
    t_thrd.storage_cxt.StrongLockInProgress = NULL;
}

/*
 * AbortStrongLockAcquire - undo strong lock state changes performed by
 * BeginStrongLockAcquire.
 */
void AbortStrongLockAcquire(void)
{
    uint32 fasthashcode;
    LOCALLOCK *locallock = t_thrd.storage_cxt.StrongLockInProgress;

    if (locallock == NULL)
        return;

    fasthashcode = FastPathStrongLockHashPartition(locallock->hashcode);
    Assert(locallock->holdsStrongLockCount == TRUE);
    SpinLockAcquire(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
    Assert(t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode] > 0);
    t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode]--;
    locallock->holdsStrongLockCount = FALSE;
    t_thrd.storage_cxt.StrongLockInProgress = NULL;
    SpinLockRelease(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
}

/*
 * GrantAwaitedLock -- call GrantLockLocal for the lock we are doing
 *		WaitOnLock on.
 *
 * proc.c needs this for the case where we are booted off the lock by
 * timeout, but discover that someone granted us the lock anyway.
 *
 * We could just export GrantLockLocal, but that would require including
 * resowner.h in lock.h, which creates circularity.
 */
void GrantAwaitedLock(void)
{
    GrantLockLocal(t_thrd.storage_cxt.awaitedLock, t_thrd.storage_cxt.awaitedOwner);
}

static void ReportWaitLockInfo(const LOCALLOCK *locallock)
{
    uint32 wait_event_info = PG_WAIT_LOCK | locallock->tag.lock.locktag_type;
    PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    pgstat_increment_changecount_before(beentry);
    /*
     * Since this is a four-byte field which is always read and written as
     * four-bytes, updates are atomic.
     */
    uint32 old_wait_event_info = beentry->st_waitevent;
    beentry->st_waitevent = wait_event_info;

    if (u_sess->attr.attr_common.enable_instr_track_wait &&
        wait_event_info != WAIT_EVENT_END) {
        beentry->waitInfo.event_info.start_time = GetCurrentTimestamp();
    } else if (u_sess->attr.attr_common.enable_instr_track_wait &&
               old_wait_event_info != WAIT_EVENT_END &&
               wait_event_info == WAIT_EVENT_END) {
        TimestampTz currentTime = GetCurrentTimestamp();
        int64 duration =
            currentTime - beentry->waitInfo.event_info.start_time;
        UpdateWaitEventStat(&beentry->waitInfo, old_wait_event_info, duration, currentTime);
        beentry->waitInfo.event_info.start_time = 0;
    }

    if ((wait_event_info & 0xFF000000) == PG_WAIT_LOCK
        && locallock->proclock->tag.myProc->blockProcLock != NULL) {
        pgstat_report_blocksid(&t_thrd,
            locallock->proclock->tag.myProc->blockProcLock->tag.myProc->sessionid);
    }
    errno_t rc = memcpy_s(&beentry->locallocktag, sizeof(LOCALLOCKTAG),
                          &locallock->tag, sizeof(LOCALLOCKTAG));
    securec_check(rc, "\0", "\0");
    pgstat_increment_changecount_after(beentry);
}

/*
 * WaitOnLock -- wait to acquire a lock
 *
 * Caller must have set t_thrd.proc->heldLocks to reflect locks already held
 * on the lockable object by this process.
 *
 * The appropriate partition lock must be held at entry.
 */
static void WaitOnLock(LOCALLOCK *locallock, ResourceOwner owner, bool allow_con_update, int waitSec)
{
    LOCKMETHODID lockmethodid = LOCALLOCK_LOCKMETHOD(*locallock);
    LockMethod lockMethodTable = LockMethods[lockmethodid];
    char *volatile new_status = NULL;

    LOCK_PRINT("WaitOnLock: sleeping on lock", locallock->lock, locallock->tag.mode);
    instr_stmt_report_lock(LOCK_WAIT_START, locallock->tag.mode, &locallock->tag.lock);

    /* Report change to waiting status */
    if (u_sess->attr.attr_common.update_process_title) {
        const char *old_status = NULL;
        int len;
        errno_t errorno = EOK;

        old_status = get_ps_display(&len);
        new_status = (char *)palloc(len + PRINT_WAIT_LENTH);
        if (len > 0) {
            errorno = memcpy_s(new_status, len + PRINT_WAIT_LENTH, old_status, len);
            securec_check(errorno, "\0", "\0");
        }
        errorno = strcpy_s(new_status + len, PRINT_WAIT_LENTH, " waiting");
        securec_check(errorno, "\0", "\0");
        set_ps_display(new_status, false);
        new_status[len] = '\0'; /* truncate off " waiting" */
    }
    ReportWaitLockInfo(locallock);

    t_thrd.storage_cxt.awaitedLock = locallock;
    t_thrd.storage_cxt.awaitedOwner = owner;

    /*
     * NOTE: Think not to put any shared-state cleanup after the call to
     * ProcSleep, in either the normal or failure path.  The lock state must
     * be fully set by the lock grantor, or by CheckDeadLock if we give up
     * waiting for the lock.  This is necessary because of the possibility
     * that a cancel/die interrupt will interrupt ProcSleep after someone else
     * grants us the lock, but before we've noticed it. Hence, after granting,
     * the locktable state must fully reflect the fact that we own the lock;
     * we can't do additional work on return.
     *
     * We can and do use a PG_TRY block to try to clean up after failure, but
     * this still has a major limitation: elog(FATAL) can occur while waiting
     * (eg, a "die" interrupt), and then control won't come back here. So all
     * cleanup of essential state should happen in LockErrorCleanup, not here.
     * We can use PG_TRY to clear the "waiting" status flags, since doing that
     * is unimportant if the process exits.
     */
    PG_TRY();
    {
        if (ProcSleep(locallock, lockMethodTable, allow_con_update, waitSec) != STATUS_OK) {
            /*
             * We failed as a result of a deadlock, see CheckDeadLock(). Quit
             * now.
             */
            t_thrd.storage_cxt.awaitedLock = NULL;
            LOCK_PRINT("WaitOnLock: aborting on lock", locallock->lock, locallock->tag.mode);
            pgstat_report_wait_lock_failed(PG_WAIT_LOCK | locallock->tag.lock.locktag_type);
            LWLockRelease(LockHashPartitionLock(locallock->hashcode));

            /*
             * Now that we aren't holding the partition lock, we can give an
             * error report including details about the detected deadlock.
             */
            DeadLockReport();
            /* not reached */
        }
    }
    PG_CATCH();
    {
        /* In this path, awaitedLock remains set until LockErrorCleanup
         *
         * Report change to non-waiting status
         */
        if (t_thrd.storage_cxt.deadlock_state == DS_LOCK_TIMEOUT) {
            pgstat_report_wait_lock_failed(PG_WAIT_LOCK | locallock->tag.lock.locktag_type);
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
        pgstat_report_blocksid(&t_thrd, 0);
        if (u_sess->attr.attr_common.update_process_title) {
            set_ps_display(new_status, false);
            pfree(new_status);
        }
        SSDmsLockRelease(locallock);
        instr_stmt_report_lock(LOCK_WAIT_END);
        instr_stmt_report_lock(LOCK_END, NoLock);

        /* and propagate the error */
        PG_RE_THROW();
    }
    PG_END_TRY();

    t_thrd.storage_cxt.awaitedLock = NULL;

    /* Report change to non-waiting status */
    pgstat_report_waitevent(WAIT_EVENT_END);
    pgstat_report_blocksid(&t_thrd, 0);
    if (u_sess->attr.attr_common.update_process_title) {
        set_ps_display(new_status, false);
        pfree(new_status);
    }

    instr_stmt_report_lock(LOCK_WAIT_END);
    LOCK_PRINT("WaitOnLock: wakeup on lock", locallock->lock, locallock->tag.mode);
}

/*
 * Remove a proc from the wait-queue it is on (caller must know it is on one).
 * This is only used when the proc has failed to get the lock, so we set its
 * waitStatus to STATUS_ERROR.
 *
 * Appropriate partition lock must be held by caller.  Also, caller is
 * responsible for signaling the proc if needed.
 *
 * NB: this does not clean up any locallock object that may exist for the lock.
 */
void RemoveFromWaitQueue(PGPROC *proc, uint32 hashcode)
{
    LOCK *waitLock = proc->waitLock;
    PROCLOCK *proclock = proc->waitProcLock;
    LOCKMODE lockmode = proc->waitLockMode;
    LOCKMETHODID lockmethodid = LOCK_LOCKMETHOD(*waitLock);

    /* Make sure proc is waiting */
    Assert(proc->waitStatus == STATUS_WAITING);
    Assert(proc->links.next != NULL);
    Assert(waitLock);
    Assert(waitLock->waitProcs.size > 0);
    Assert(lockmethodid > 0 && lockmethodid < lengthof(LockMethods));

    /* Remove proc from lock's wait queue */
    SHMQueueDelete(&(proc->links));
    waitLock->waitProcs.size--;

    /* Undo increments of request counts by waiting process */
    Assert(waitLock->nRequested > 0);
    Assert(waitLock->nRequested > proc->waitLock->nGranted);
    waitLock->nRequested--;
    Assert(waitLock->requested[lockmode] > 0);
    waitLock->requested[lockmode]--;
    /* don't forget to clear waitMask bit if appropriate */
    if (waitLock->granted[lockmode] == waitLock->requested[lockmode]) {
        waitLock->waitMask &= LOCKBIT_OFF((unsigned int)lockmode);
    }

    /* Clean up the proc's own state, and pass it the ok/fail signal */
    proc->waitLock = NULL;
    proc->waitProcLock = NULL;
    proc->waitStatus = STATUS_ERROR;

    /*
     * Delete the proclock immediately if it represents no already-held locks.
     * (This must happen now because if the owner of the lock decides to
     * release it, and the requested/granted counts then go to zero,
     * LockRelease expects there to be no remaining proclocks.) Then see if
     * any other waiters for the lock can be woken up now.
     */
    CleanUpLock(waitLock, proclock, LockMethods[lockmethodid], hashcode, true);
}

/*
 * LockRelease -- look up 'locktag' and release one 'lockmode' lock on it.
 *		Release a session lock if 'sessionLock' is true, else release a
 *		regular transaction lock.
 *
 * Side Effects: find any waiting processes that are now wakable,
 *		grant them their requested locks and awaken them.
 *		(We have to grant the lock here to avoid a race between
 *		the waking process and any new process to
 *		come along and request the lock.)
 */
bool LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCALLOCKTAG localtag;
    LOCALLOCK *locallock = NULL;
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    LWLock *partitionLock = NULL;
    bool wakeupNeeded = false;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    CHECK_LOCKMODE(lockmode, lockMethodTable);

#ifdef LOCK_DEBUG
    if (LOCK_DEBUG_ENABLED(locktag))
        ereport(LOG, (errmsg("LockRelease: lock [%u,%u] %s", locktag->locktag_field1, locktag->locktag_field2,
                             lockMethodTable->lockModeNames[lockmode])));
#endif

    /*
     * Find the LOCALLOCK entry for this lock and lockmode
     */
    errno_t rc = memset_s(&localtag, sizeof(localtag), 0, sizeof(localtag)); /* must clear padding */
    securec_check(rc, "", "");
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = (LOCALLOCK *)hash_search(t_thrd.storage_cxt.LockMethodLocalHash, (void *)&localtag, HASH_FIND, NULL);
    /*
     * let the caller print its own error message, too. Do not ereport(ERROR).
     */
    if ((locallock == NULL) || locallock->nLocks <= 0) {
        ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        return FALSE;
    }

    /*
     * Decrease the count for the resource owner.
     */
    {
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        ResourceOwner owner;
        int i;

        /* Identify owner for lock */
        if (sessionLock)
            owner = NULL;
        else
            owner = t_thrd.utils_cxt.CurrentResourceOwner;

        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == owner) {
                Assert(lockOwners[i].nLocks > 0);
                if (--lockOwners[i].nLocks == 0) {
                    /* compact out unused slot */
                    locallock->numLockOwners--;
                    if (i < locallock->numLockOwners)
                        lockOwners[i] = lockOwners[locallock->numLockOwners];
                }
                break;
            }
        }
        if (i < 0) {
            /* don't release a lock belonging to another owner */
            ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
            return FALSE;
        }
    }

    /*
     * Decrease the total local count.	If we're still holding the lock, we're
     * done.
     */
    locallock->nLocks--;

    if (locallock->nLocks > 0) {
        instr_stmt_report_lock(LOCK_RELEASE, lockmode, locktag);
        return TRUE;
    }

    /* Attempt fast release of any lock eligible for the fast path. */
    if (EligibleForRelationFastPath(locktag, lockmode) && t_thrd.storage_cxt.FastPathLocalUseCount > 0) {
        bool released = false;
        FastPathTag tag = { locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3 };

        /*
         * We might not find the lock here, even if we originally entered it
         * here.  Another backend may have moved it to the main table.
         */
        LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);
        released = FastPathUnGrantRelationLock(tag, lockmode);
        LWLockRelease(t_thrd.proc->backendLock);
        if (released) {
            RemoveLocalLock(locallock);
            instr_stmt_report_lock(LOCK_RELEASE, lockmode, locktag);
            return TRUE;
        }
    }

    /*
     * Otherwise we've got to mess with the shared lock table.
     */
    partitionLock = LockHashPartitionLock(locallock->hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Normally, we don't need to re-find the lock or proclock, since we kept
     * their addresses in the locallock table, and they couldn't have been
     * removed while we were holding a lock on them.  But it's possible that
     * the lock was taken fast-path and has since been moved to the main hash
     * table by another backend, in which case we will need to look up the
     * objects here.  We assume the lock field is NULL if so.
     */
    lock = locallock->lock;
    if (lock == NULL) {
        PROCLOCKTAG proclocktag;

        Assert(EligibleForRelationFastPath(locktag, lockmode));
        lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (const void *)locktag,
                                                   locallock->hashcode, HASH_FIND, NULL);
        if (lock == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("failed to re-find shared lock object")));
        }
        locallock->lock = lock;

        proclocktag.myLock = lock;
        proclocktag.myProc = t_thrd.proc;
        locallock->proclock = (PROCLOCK *)hash_search(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&proclocktag,
                                                      HASH_FIND, NULL);
        if (!locallock->proclock) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("failed to re-find shared proclock object")));
        }
    }
    LOCK_PRINT("LockRelease: found", lock, lockmode);
    proclock = locallock->proclock;
    PROCLOCK_PRINT("LockRelease: found", proclock);

    /*
     * Double-check that we are actually holding a lock of the type we want to
     * release.
     */
    if (!(proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode))) {
        PROCLOCK_PRINT("LockRelease: WRONGTYPE", proclock);
        LWLockRelease(partitionLock);
        ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        RemoveLocalLock(locallock);
        return FALSE;
    }

    /*
     * Do the releasing.  CleanUpLock will waken any now-wakable waiters.
     */
    wakeupNeeded = UnGrantLock(lock, lockmode, proclock, lockMethodTable);

    CleanUpLock(lock, proclock, lockMethodTable, locallock->hashcode, wakeupNeeded);

    LWLockRelease(partitionLock);
    instr_stmt_report_lock(LOCK_RELEASE, lockmode, locktag);

    RemoveLocalLock(locallock);

    return TRUE;
}

/*
 * LockReleaseAll -- Release all locks of the specified lock method that
 *		are held by the current process.
 *
 * Well, not necessarily *all* locks.  The available behaviors are:
 *		allLocks == true: release all locks including session locks.
 *		allLocks == false: release all non-session locks.
 */
void LockReleaseAll(LOCKMETHODID lockmethodid, bool allLocks)
{
    HASH_SEQ_STATUS status;
    LockMethod lockMethodTable;
    int i, numLockModes;
    LOCALLOCK *locallock = NULL;
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    int partition;
    bool have_fast_path_lwlock = false;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];

#ifdef LOCK_DEBUG
    if (*(lockMethodTable->trace_flag))
        ereport(LOG, (errmsg("LockReleaseAll: lockmethod=%d", lockmethodid)));
#endif

    /*
     * Get rid of our fast-path VXID lock, if appropriate.	Note that this is
     * the only way that the lock we hold on our own VXID can ever get
     * released: it is always and only released when a toplevel transaction
     * ends.
     */
    if ((lockmethodid == DEFAULT_LOCKMETHOD) && (t_thrd.role != PAGEREDO)) {
        VirtualXactLockTableCleanup();
    }

    numLockModes = lockMethodTable->numLockModes;

    u_sess->storage_cxt.holdSessionLock[lockmethodid - 1] = false;

    /*
     * First we run through the locallock table and get rid of unwanted
     * entries, then we scan the process's proclocks and get rid of those. We
     * do this separately because we may have multiple locallock entries
     * pointing to the same proclock, and we daren't end up with any dangling
     * pointers.  Fast-path locks are cleaned up during the locallock table
     * scan, though.
     */
    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        /*
         * If the LOCALLOCK entry is unused, we must've run out of shared
         * memory while trying to set up this lock.  Just forget the local
         * entry.
         */
        if (locallock->nLocks == 0) {
            RemoveLocalLock(locallock);
            continue;
        }

        /* Ignore items that are not of the lockmethod to be removed */
        if (LOCALLOCK_LOCKMETHOD(*locallock) != lockmethodid)
            continue;

        /*
         * If we are asked to release all locks, we can just zap the entry.
         * Otherwise, must scan to see if there are session locks. We assume
         * there is at most one lockOwners entry for session locks.
         */
        if (!allLocks) {
            LOCALLOCKOWNER *lockOwners = locallock->lockOwners;

            /* If it's above array position 0, move it down to 0 */
            for (i = locallock->numLockOwners - 1; i > 0; i--) {
                if (lockOwners[i].owner == NULL) {
                    lockOwners[0] = lockOwners[i];
                    break;
                }
            }

            if (locallock->numLockOwners > 0 && lockOwners[0].owner == NULL && lockOwners[0].nLocks > 0) {
                /* Fix the locallock to show just the session locks */
                locallock->nLocks = lockOwners[0].nLocks;
                locallock->numLockOwners = 1;
                u_sess->storage_cxt.holdSessionLock[lockmethodid - 1] = true;
                /* We aren't deleting this locallock, so done */
                continue;
            }
        }

        /*
         * If the lock or proclock pointers are NULL, this lock was taken via
         * the relation fast-path (and is not known to have been transferred).
         */
        if (locallock->proclock == NULL || locallock->lock == NULL) {
            LOCKMODE lockmode = locallock->tag.mode;
            FastPathTag tag = { locallock->tag.lock.locktag_field1, locallock->tag.lock.locktag_field2,
                                locallock->tag.lock.locktag_field3
                            };

            /* Verify that a fast-path lock is what we've got. */
            if (!EligibleForRelationFastPath(&locallock->tag.lock, lockmode))
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("locallock table corrupted")));

            /*
             * If we don't currently hold the LWLock that protects our
             * fast-path data structures, we must acquire it before attempting
             * to release the lock via the fast-path.  We will continue to
             * hold the LWLock until we're done scanning the locallock table,
             * unless we hit a transferred fast-path lock.  (XXX is this
             * really such a good idea?  There could be a lot of entries ...)
             */
            if (!have_fast_path_lwlock) {
                LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);
                have_fast_path_lwlock = true;
            }

            /* Attempt fast-path release. */
            if (FastPathUnGrantRelationLock(tag, lockmode)) {
                instr_stmt_report_lock(LOCK_RELEASE, lockmode, &locallock->tag.lock);
                RemoveLocalLock(locallock);
                continue;
            }

            /*
             * Our lock, originally taken via the fast path, has been
             * transferred to the main lock table.	That's going to require
             * some extra work, so release our fast-path lock before starting.
             */
            LWLockRelease(t_thrd.proc->backendLock);
            have_fast_path_lwlock = false;

            /*
             * Now dump the lock.  We haven't got a pointer to the LOCK or
             * PROCLOCK in this case, so we have to handle this a bit
             * differently than a normal lock release.	Unfortunately, this
             * requires an extra LWLock acquire-and-release cycle on the
             * partitionLock, but hopefully it shouldn't happen often.
             */
            LockRefindAndRelease(lockMethodTable, t_thrd.proc, &locallock->tag.lock, lockmode, false);
            RemoveLocalLock(locallock);
            continue;
        }

        /* Mark the proclock to show we need to release this lockmode */
        if (locallock->nLocks > 0)
            locallock->proclock->releaseMask |= LOCKBIT_ON((unsigned int)locallock->tag.mode);

        /* And remove the locallock hashtable entry */
        RemoveLocalLock(locallock);
    }

    /* Done with the fast-path data structures */
    if (have_fast_path_lwlock)
        LWLockRelease(t_thrd.proc->backendLock);

    /*
     * Now, scan each lock partition separately.
     */
    for (partition = 0; partition < NUM_LOCK_PARTITIONS; partition++) {
        LWLock *partitionLock = GetMainLWLockByIndex(FirstLockMgrLock + partition);
        SHM_QUEUE *procLocks = &(t_thrd.proc->myProcLocks[partition]);

        proclock = (PROCLOCK *)SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, procLink));
        if (proclock == NULL)
            continue; /* needn't examine this partition */

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        while (proclock != NULL) {
            bool wakeupNeeded = false;
            PROCLOCK *nextplock = NULL;

            /* Get link first, since we may unlink/delete this proclock */
            nextplock = (PROCLOCK *)SHMQueueNext(procLocks, &proclock->procLink, offsetof(PROCLOCK, procLink));

            Assert(proclock->tag.myProc == t_thrd.proc);

            lock = proclock->tag.myLock;

            /* Ignore items that are not of the lockmethod to be removed */
            if (LOCK_LOCKMETHOD(*lock) != lockmethodid)
                goto next_item;

            /*
             * In allLocks mode, force release of all locks even if locallock
             * table had problems
             */
            if (allLocks) {
                proclock->releaseMask = proclock->holdMask;
            } else if (unlikely((proclock->releaseMask & ~proclock->holdMask) != 0)) {
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proclock table corrupted")));
            }

            /*
             * Ignore items that have nothing to be released, unless they have
             * holdMask == 0 and are therefore recyclable
             */
            if (proclock->releaseMask == 0 && proclock->holdMask != 0)
                goto next_item;

            PROCLOCK_PRINT("LockReleaseAll", proclock);
            LOCK_PRINT("LockReleaseAll", lock, 0);
            Assert(lock->nRequested >= 0);
            Assert(lock->nGranted >= 0);
            Assert(lock->nGranted <= lock->nRequested);
            Assert((proclock->holdMask & ~lock->grantMask) == 0);

            /*
             * Release the previously-marked lock modes
             */
            for (i = 1; i <= numLockModes; i++) {
                if (proclock->releaseMask & LOCKBIT_ON((unsigned int)i)) {
                    wakeupNeeded |= UnGrantLock(lock, i, proclock, lockMethodTable);
                    instr_stmt_report_lock(LOCK_RELEASE, i, &lock->tag);
                }
            }
            Assert((lock->nRequested >= 0) && (lock->nGranted >= 0));
            Assert(lock->nGranted <= lock->nRequested);
            LOCK_PRINT("LockReleaseAll: updated", lock, 0);

            proclock->releaseMask = 0;

            /* CleanUpLock will wake up waiters if needed. */
            CleanUpLock(lock, proclock, lockMethodTable, LockTagHashCode(&lock->tag), wakeupNeeded);

next_item:
            proclock = nextplock;
        } /* loop over PROCLOCKs within this partition */

        LWLockRelease(partitionLock);
    } /* loop over partitions */

#ifdef LOCK_DEBUG
    if (*(lockMethodTable->trace_flag))
        ereport(LOG, (errmsg("LockReleaseAll done")));
#endif
}

/* check fastpath bit num */
void Check_FastpathBit()
{
    uint32 f;
    bool leaked = false;
    for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
        if (FAST_PATH_GET_BITS(t_thrd.proc, f) != 0) {
            Assert(0);
            leaked = true;
        }
    }
    /* reset fastpath bit num and use count, also report leak */
    t_thrd.storage_cxt.FastPathLocalUseCount = 0;
    FAST_PATH_SET_LOCKBITS_ZERO(t_thrd.proc);
    if (leaked == true)
        ereport(WARNING, (errmsg("Fast path bit num leak.")));
}

/*
 * LockReleaseSession -- Release all session locks of the specified lock method
 *		that are held by the current process.
 */
void LockReleaseSession(LOCKMETHODID lockmethodid)
{
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock = NULL;

    CHECK_LOCKMETHODID(lockmethodid);

    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        /* Ignore items that are not of the specified lock method */
        if (LOCALLOCK_LOCKMETHOD(*locallock) != lockmethodid)
            continue;

        ReleaseLockIfHeld(locallock, true);
    }
}

/*
 * LockReleaseCurrentOwner
 *		Release all locks belonging to CurrentResourceOwner
 */
void LockReleaseCurrentOwner(void)
{
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock = NULL;

    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        ReleaseLockIfHeld(locallock, false);
    }
}

/* clear all info of CurrentResourceOwner in locallock and release the lock if no other owners. */
void ReleaseLockIfHeld(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCALLOCKTAG localtag;
    LOCALLOCK *locallock = NULL;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    CHECK_LOCKMODE(lockmode, lockMethodTable);

#ifdef LOCK_DEBUG
    if (LOCK_DEBUG_ENABLED(locktag))
        ereport(LOG, (errmsg("ReleaseLockIfHeld: lock [%u,%u] %s", locktag->locktag_field1, locktag->locktag_field2,
                             lockMethodTable->lockModeNames[lockmode])));
#endif

    /*
     * Find the LOCALLOCK entry for this lock and lockmode
     */
    errno_t rc = memset_s(&localtag, sizeof(localtag), 0, sizeof(localtag)); /* must clear padding */
    securec_check(rc, "", "");
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = (LOCALLOCK *)hash_search(t_thrd.storage_cxt.LockMethodLocalHash, (void *)&localtag, HASH_FIND, NULL);
    if ((locallock == NULL) || locallock->nLocks <= 0) {
        ereport(LOG, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        return;
    }

    ReleaseLockIfHeld(locallock, sessionLock);
}

/*
 * ReleaseLockIfHeld
 *		Release any session-level locks on this lockable object if sessionLock
 *		is true; else, release any locks held by CurrentResourceOwner.
 *
 * It is tempting to pass this a ResourceOwner pointer (or NULL for session
 * locks), but without refactoring LockRelease() we cannot support releasing
 * locks belonging to resource owners other than CurrentResourceOwner.
 * If we were to refactor, it'd be a good idea to fix it so we don't have to
 * do a hashtable lookup of the locallock, too.  However, currently this
 * function isn't used heavily enough to justify refactoring for its
 * convenience.
 */
static void ReleaseLockIfHeld(LOCALLOCK *locallock, bool sessionLock)
{
    ResourceOwner owner;
    LOCALLOCKOWNER *lockOwners = NULL;
    int i;

    /* Identify owner for lock (must match LockRelease!) */
    if (sessionLock)
        owner = NULL;
    else
        owner = t_thrd.utils_cxt.CurrentResourceOwner;

    /* Scan to see if there are any locks belonging to the target owner */
    lockOwners = locallock->lockOwners;
    for (i = locallock->numLockOwners - 1; i >= 0; i--) {
        if (lockOwners[i].owner == owner) {
            Assert(lockOwners[i].nLocks > 0);
            if (lockOwners[i].nLocks < locallock->nLocks) {
                /*
                 * We will still hold this lock after forgetting this
                 * ResourceOwner.
                 */
                locallock->nLocks -= lockOwners[i].nLocks;
                /* compact out unused slot */
                locallock->numLockOwners--;
                if (i < locallock->numLockOwners)
                    lockOwners[i] = lockOwners[locallock->numLockOwners];
            } else {
                Assert(lockOwners[i].nLocks == locallock->nLocks);
                /* We want to call LockRelease just once */
                lockOwners[i].nLocks = 1;
                locallock->nLocks = 1;
                if (!LockRelease(&locallock->tag.lock, locallock->tag.mode, sessionLock))
                    ereport(WARNING, (errmsg("ReleaseLockIfHeld: failed?\?")));
            }
            break;
        }
    }
}

/*
 * LockReassignCurrentOwner
 *		Reassign all locks belonging to CurrentResourceOwner to belong
 *		to its parent resource owner
 */
void LockReassignCurrentOwner(void)
{
    ResourceOwner parent = ResourceOwnerGetParent(t_thrd.utils_cxt.CurrentResourceOwner);
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock = NULL;
    LOCALLOCKOWNER *lockOwners = NULL;

    Assert(parent != NULL);

    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        int i;
        int ic = -1;
        int ip = -1;

        /*
         * Scan to see if there are any locks belonging to current owner or
         * its parent
         */
        lockOwners = locallock->lockOwners;
        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == t_thrd.utils_cxt.CurrentResourceOwner)
                ic = i;
            else if (lockOwners[i].owner == parent)
                ip = i;
        }

        if (ic < 0)
            continue; /* no current locks */

        if (ip < 0) {
            /* Parent has no slot, so just give it child's slot */
            lockOwners[ic].owner = parent;
        } else {
            /* Merge child's count with parent's */
            lockOwners[ip].nLocks += lockOwners[ic].nLocks;
            /* compact out unused slot */
            locallock->numLockOwners--;
            if (ic < locallock->numLockOwners)
                lockOwners[ic] = lockOwners[locallock->numLockOwners];
        }
    }
}

/*
 * FastPathGrantRelationLock
 *		Grant lock using per-backend fast-path array, if there is space.
 */
static bool FastPathGrantRelationLock(const FastPathTag &tag, LOCKMODE lockmode)
{
    uint32 f;
    uint32 unused_slot = FP_LOCK_SLOTS_PER_BACKEND;

    /* Scan for existing entry for this relid, remembering empty slot. */
    for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
        if (FAST_PATH_GET_BITS(t_thrd.proc, f) == 0)
            unused_slot = f;
        else if (FAST_PATH_TAG_EQUALS(t_thrd.proc->fpRelId[f], tag)) {
            Assert(!FAST_PATH_CHECK_LOCKMODE(t_thrd.proc, f, lockmode));
            FAST_PATH_SET_LOCKMODE(t_thrd.proc, f, lockmode);
            return true;
        }
    }

    /* If no existing entry, use any empty slot. */
    if (unused_slot < FP_LOCK_SLOTS_PER_BACKEND) {
        t_thrd.proc->fpRelId[unused_slot] = tag;
        FAST_PATH_SET_LOCKMODE(t_thrd.proc, unused_slot, lockmode);
        ++t_thrd.storage_cxt.FastPathLocalUseCount;
        return true;
    }

    /* No existing entry, and no empty slot. */
    return false;
}

/*
 * FastPathUnGrantRelationLock
 *		Release fast-path lock, if present.  Update backend-private local
 *		use count, while we're at it.
 */
static bool FastPathUnGrantRelationLock(const FastPathTag &tag, LOCKMODE lockmode)
{
    uint32 f;
    bool result = false;

    t_thrd.storage_cxt.FastPathLocalUseCount = 0;
    for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
        if (FAST_PATH_TAG_EQUALS(t_thrd.proc->fpRelId[f], tag) && FAST_PATH_CHECK_LOCKMODE(t_thrd.proc, f, lockmode)) {
            Assert(!result);
            FAST_PATH_CLEAR_LOCKMODE(t_thrd.proc, f, lockmode);
            result = true;
            /* we continue iterating so as to update FastPathLocalUseCount */
        }
        if (FAST_PATH_GET_BITS(t_thrd.proc, f) != 0)
            ++t_thrd.storage_cxt.FastPathLocalUseCount;
    }
    return result;
}

/*
 * FastPathTransferRelationLocks
 *		Transfer locks matching the given lock tag from per-backend fast-path
 *		arrays to the shared hash table.
 *
 * Returns true if successful, false if ran out of shared memory.
 */
static bool FastPathTransferRelationLocks(LockMethod lockMethodTable, const LOCKTAG *locktag, uint32 hashcode)
{
    LWLock *partitionLock = LockHashPartitionLock(hashcode);
    FastPathTag tag = { locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3 };
    uint32 i;

    /*
     * Every PGPROC that can potentially hold a fast-path lock is present in
     * g_instance.proc_base->allProcs.  Prepared transactions are not, but any
     * outstanding fast-path locks held by prepared transactions are
     * transferred to the main lock table.
     */
    for (i = 0; i < g_instance.proc_base->allNonPreparedProcCount; i++) {
        PGPROC *proc = g_instance.proc_base_all_procs[i];
        uint32 f;

        LWLockAcquire(proc->backendLock, LW_EXCLUSIVE);

        for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
            uint32 lockmode;

            /* Look for an allocated slot matching the given relid. */
            if (!FAST_PATH_TAG_EQUALS(tag, proc->fpRelId[f]) || FAST_PATH_GET_BITS(proc, f) == 0)
                continue;

            /* Find or create lock object. */
            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            for (lockmode = FAST_PATH_LOCKNUMBER_OFFSET;
                 lockmode < FAST_PATH_LOCKNUMBER_OFFSET + FAST_PATH_BITS_PER_SLOT; ++lockmode) {
                PROCLOCK *proclock = NULL;

                if (!FAST_PATH_CHECK_LOCKMODE(proc, f, lockmode))
                    continue;
                proclock = SetupLockInTable(lockMethodTable, proc, locktag, hashcode, lockmode);
                if (proclock == NULL) {
                    LWLockRelease(partitionLock);
                    return false;
                }
                GrantLock(proclock->tag.myLock, proclock, lockmode);
                FAST_PATH_CLEAR_LOCKMODE(proc, f, lockmode);
            }
            LWLockRelease(partitionLock);

            /* No need to examine remaining slots. */
            break;
        }
        LWLockRelease(proc->backendLock);
    }
    return true;
}

/*
 * FastPathGetLockEntry
 *		Return the PROCLOCK for a lock originally taken via the fast-path,
 *		transferring it to the primary lock table if necessary.
 */
static PROCLOCK *FastPathGetRelationLockEntry(LOCALLOCK *locallock)
{
    LockMethod lockMethodTable = LockMethods[DEFAULT_LOCKMETHOD];
    LOCKTAG *locktag = &locallock->tag.lock;
    PROCLOCK *proclock = NULL;
    LWLock *partitionLock = LockHashPartitionLock(locallock->hashcode);
    FastPathTag tag = { locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3 };
    uint32 f;

    LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);

    for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
        uint32 lockmode;

        /* Look for an allocated slot matching the given relid. */
        if (!FAST_PATH_TAG_EQUALS(tag, t_thrd.proc->fpRelId[f]) || FAST_PATH_GET_BITS(t_thrd.proc, f) == 0)
            continue;

        /* If we don't have a lock of the given mode, forget it! */
        lockmode = locallock->tag.mode;
        if (!FAST_PATH_CHECK_LOCKMODE(t_thrd.proc, f, lockmode))
            break;

        /* Find or create lock object. */
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        proclock = SetupLockInTable(lockMethodTable, t_thrd.proc, locktag, locallock->hashcode, lockmode);
        if (proclock == NULL) {
            LWLockRelease(partitionLock);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                            errhint("You might need to increase max_locks_per_transaction.")));
        }
        GrantLock(proclock->tag.myLock, proclock, lockmode);
        FAST_PATH_CLEAR_LOCKMODE(t_thrd.proc, f, lockmode);

        LWLockRelease(partitionLock);

        /* No need to examine remaining slots. */
        break;
    }

    LWLockRelease(t_thrd.proc->backendLock);

    /* Lock may have already been transferred by some other backend. */
    if (proclock == NULL) {
        LOCK *lock = NULL;
        PROCLOCKTAG proclocktag;
        uint32 proclock_hashcode;

        LWLockAcquire(partitionLock, LW_SHARED);

        lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)locktag,
                                                   locallock->hashcode, HASH_FIND, NULL);
        if (lock == NULL)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("failed to re-find shared lock object")));

        proclocktag.myLock = lock;
        proclocktag.myProc = t_thrd.proc;

        proclock_hashcode = ProcLockHashCode(&proclocktag, locallock->hashcode);
        proclock = (PROCLOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash,
                                                           (void *)&proclocktag, proclock_hashcode, HASH_FIND, NULL);
        if (proclock == NULL)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("failed to re-find shared proclock object")));
        LWLockRelease(partitionLock);
    }

    return proclock;
}

/*
 * GetLockConflicts
 *		Get an array of VirtualTransactionIds of xacts currently holding locks
 *		that would conflict with the specified lock/lockmode.
 *		xacts merely awaiting such a lock are NOT reported.
 *
 * The result array is palloc'd and is terminated with an invalid VXID.
 *
 * Of course, the result could be out of date by the time it's returned,
 * so use of this function has to be thought about carefully.
 *
 * Note we never include the current xact's vxid in the result array,
 * since an xact never blocks itself.  Also, prepared transactions are
 * ignored, which is a bit more debatable but is appropriate for current
 * uses of the result.
 */
VirtualTransactionId *GetLockConflicts(const LOCKTAG *locktag, LOCKMODE lockmode)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCK *lock = NULL;
    LOCKMASK conflictMask;
    SHM_QUEUE *procLocks = NULL;
    PROCLOCK *proclock = NULL;
    uint32 hashcode;
    LWLock *partitionLock = NULL;
    int count = 0;
    int fast_count = 0;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    CHECK_LOCKMODE(lockmode, lockMethodTable);

    /*
     * Allocate memory to store results, and fill with InvalidVXID.  We only
     * need enough space for g_instance.shmem_cxt.MaxBackends + a terminator, since prepared xacts
     * don't count. InHotStandby allocate once in t_thrd.top_mem_cxt.
     */
    if (InHotStandby) {
        if (t_thrd.storage_cxt.lock_vxids == NULL)
            t_thrd.storage_cxt.lock_vxids = (VirtualTransactionId *)MemoryContextAlloc(
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                sizeof(VirtualTransactionId) * (g_instance.shmem_cxt.MaxBackends + 1));
    } else
        t_thrd.storage_cxt.lock_vxids =
            (VirtualTransactionId *)palloc0(sizeof(VirtualTransactionId) * (g_instance.shmem_cxt.MaxBackends + 1));

    /* Compute hash code and partiton lock, and look up conflicting modes. */
    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);
    conflictMask = lockMethodTable->conflictTab[lockmode];

    /*
     * Fast path locks might not have been entered in the primary lock table.
     * If the lock we're dealing with could conflict with such a lock, we must
     * examine each backend's fast-path array for conflicts.
     */
    if (ConflictsWithRelationFastPath(locktag, lockmode)) {
        int i;
        FastPathTag tag = { locktag->locktag_field1, locktag->locktag_field2, locktag->locktag_field3 };
        VirtualTransactionId vxid;

        /*
         * Iterate over relevant PGPROCs.  Anything held by a prepared
         * transaction will have been transferred to the primary lock table,
         * so we need not worry about those.  This is all a bit fuzzy, because
         * new locks could be taken after we've visited a particular
         * partition, but the callers had better be prepared to deal with that
         * anyway, since the locks could equally well be taken between the
         * time we return the value and the time the caller does something
         * with it.
         */
        for (i = 0; (unsigned int)(i) < g_instance.proc_base->allNonPreparedProcCount; i++) {
            PGPROC *proc = g_instance.proc_base_all_procs[i];
            uint32 f;

            /* A backend never blocks itself */
            if (proc == t_thrd.proc)
                continue;

            LWLockAcquire(proc->backendLock, LW_SHARED);

            for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++) {
                uint32 lockmask;

                /* Look for an allocated slot matching the given relid. */
                if (!FAST_PATH_TAG_EQUALS(tag, proc->fpRelId[f]))
                    continue;
                lockmask = FAST_PATH_GET_BITS(proc, f);
                if (!lockmask)
                    continue;
                lockmask <<= FAST_PATH_LOCKNUMBER_OFFSET;

                /*
                 * There can only be one entry per relation, so if we found it
                 * and it doesn't conflict, we can skip the rest of the slots.
                 */
                if ((lockmask & conflictMask) == 0)
                    break;

                /* Conflict! */
                GET_VXID_FROM_PGPROC(vxid, *proc);

                /*
                 * If we see an invalid VXID, then either the xact has already
                 * committed (or aborted), or it's a prepared xact.  In either
                 * case we may ignore it.
                 */
                if (VirtualTransactionIdIsValid(vxid))
                    t_thrd.storage_cxt.lock_vxids[count++] = vxid;
                break;
            }

            LWLockRelease(proc->backendLock);
        }
    }

    /* Remember how many fast-path conflicts we found. */
    fast_count = count;

    /*
     * Look up the lock object matching the tag.
     */
    LWLockAcquire(partitionLock, LW_SHARED);

    lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (const void *)locktag, hashcode,
                                               HASH_FIND, NULL);
    if (lock == NULL) {
        /*
         * If the lock object doesn't exist, there is nothing holding a lock
         * on this lockable object.
         */
        LWLockRelease(partitionLock);
        t_thrd.storage_cxt.lock_vxids[count].backendId = InvalidBackendId;
        t_thrd.storage_cxt.lock_vxids[count].localTransactionId = InvalidLocalTransactionId;
        return t_thrd.storage_cxt.lock_vxids;
    }

    /* Examine each existing holder (or awaiter) of the lock. */
    procLocks = &(lock->procLocks);

    proclock = (PROCLOCK *)SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, lockLink));

    while (proclock != NULL) {
        if (conflictMask & proclock->holdMask) {
            PGPROC *proc = proclock->tag.myProc;

            /* A backend never blocks itself */
            if (proc != t_thrd.proc) {
                VirtualTransactionId vxid;

                GET_VXID_FROM_PGPROC(vxid, *proc);

                /*
                 * If we see an invalid VXID, then either the xact has already
                 * committed (or aborted), or it's a prepared xact.  In either
                 * case we may ignore it.
                 */
                if (VirtualTransactionIdIsValid(vxid)) {
                    int i;

                    /* Avoid duplicate entries. */
                    for (i = 0; i < fast_count; ++i)
                        if (VirtualTransactionIdEquals(t_thrd.storage_cxt.lock_vxids[i], vxid))
                            break;
                    if (i >= fast_count)
                        t_thrd.storage_cxt.lock_vxids[count++] = vxid;
                }
            }
        }

        proclock = (PROCLOCK *)SHMQueueNext(procLocks, &proclock->lockLink, offsetof(PROCLOCK, lockLink));
    }

    LWLockRelease(partitionLock);

    if (count > g_instance.shmem_cxt.MaxBackends) /* should never happen */
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("too many conflicting locks found")));

    t_thrd.storage_cxt.lock_vxids[count].backendId = InvalidBackendId;
    t_thrd.storage_cxt.lock_vxids[count].localTransactionId = InvalidLocalTransactionId;
    return t_thrd.storage_cxt.lock_vxids;
}

/*
 * Find a lock in the shared lock table and release it.  It is the caller's
 * responsibility to verify that this is a sane thing to do.  (For example, it
 * would be bad to release a lock here if there might still be a LOCALLOCK
 * object with pointers to it.)
 *
 * We currently use this in two situations: first, to release locks held by
 * prepared transactions on commit (see lock_twophase_postcommit); and second,
 * to release locks taken via the fast-path, transferred to the main hash
 * table, and then released (see LockReleaseAll).
 */
static void LockRefindAndRelease(LockMethod lockMethodTable, PGPROC *proc, LOCKTAG *locktag, LOCKMODE lockmode,
                                 bool decrement_strong_lock_count)
{
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    PROCLOCKTAG proclocktag;
    uint32 hashcode;
    uint32 proclock_hashcode;
    LWLock *partitionLock = NULL;
    bool wakeupNeeded = false;

    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Re-find the lock object (it had better be there).
     */
    lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)locktag, hashcode,
                                               HASH_FIND, NULL);
    if (lock == NULL)
        ereport(PANIC, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("failed to re-find shared lock object")));

    /*
     * Re-find the proclock object (ditto).
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc;

    proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

    proclock = (PROCLOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&proclocktag,
                                                       proclock_hashcode, HASH_FIND, NULL);
    if (proclock == NULL)
        ereport(PANIC, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("failed to re-find shared proclock object")));

    /*
     * Double-check that we are actually holding a lock of the type we want to
     * release.
     */
    if (!(proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode))) {
        PROCLOCK_PRINT("lock_twophase_postcommit: WRONGTYPE", proclock);
        LWLockRelease(partitionLock);
        ereport(WARNING, (errmsg("you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode])));
        return;
    }

    /*
     * Do the releasing.  CleanUpLock will waken any now-wakable waiters.
     */
    wakeupNeeded = UnGrantLock(lock, lockmode, proclock, lockMethodTable);

    CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeupNeeded);

    LWLockRelease(partitionLock);
    instr_stmt_report_lock(LOCK_RELEASE, lockmode, locktag);

    /*
     * Decrement strong lock count.  This logic is needed only for 2PC.
     * locktag is more reliable than &lock->tag, which is volatile in case of high concurrence.
     */
    if (decrement_strong_lock_count && ConflictsWithRelationFastPath(locktag, lockmode)) {
        uint32 fasthashcode = FastPathStrongLockHashPartition(hashcode);

        SpinLockAcquire(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
        Assert(t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode] > 0);
        t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode]--;
        SpinLockRelease(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
    }
}

/*
 * AtPrepare_Locks
 *		Do the preparatory work for a PREPARE: make 2PC state file records
 *		for all locks currently held.
 *
 * Session-level locks are ignored, as are VXID locks.
 *
 * There are some special cases that we error out on: we can't be holding any
 * locks at both session and transaction level (since we must either keep or
 * give away the PROCLOCK object), and we can't be holding any locks on
 * temporary objects (since that would mess up the current backend if it tries
 * to exit before the prepared xact is committed).
 */
void AtPrepare_Locks(void)
{
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock = NULL;
    errno_t errorno = EOK;
    /*
     * For the most part, we don't need to touch shared memory for this ---
     * all the necessary state information is in the locallock table.
     * Fast-path locks are an exception, however: we move any such locks to
     * the main table before allowing PREPARE TRANSACTION to succeed.
     */
    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        TwoPhaseLockRecord record;
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        bool haveSessionLock = false;
        bool haveXactLock = false;
        int i;

        /*
         * Ignore VXID locks.  We don't want those to be held by prepared
         * transactions, since they aren't meaningful after a restart.
         */
        if (locallock->tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
            continue;

        /* Ignore it if we don't actually hold the lock */
        if (locallock->nLocks <= 0)
            continue;

        /* Scan to see whether we hold it at session or transaction level */
        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == NULL)
                haveSessionLock = true;
            else
                haveXactLock = true;
        }

        /* Ignore it if we have only session lock */
        if (!haveXactLock) {
            if (haveSessionLock) {
                LOCALLOCK *tmplock = NULL;
                HASH_SEQ_STATUS tmpstatus;

                /* See if we have transaction-level lock with the same locktag (not care lockmode) */
                hash_seq_init(&tmpstatus, t_thrd.storage_cxt.LockMethodLocalHash);
                while ((tmplock = (LOCALLOCK *)hash_seq_search(&tmpstatus)) != NULL) {
                    if (memcmp(&(locallock->tag.lock), &(tmplock->tag.lock), sizeof(LOCKTAG)) == 0) {
                        lockOwners = tmplock->lockOwners;
                        for (i = tmplock->numLockOwners - 1; i >= 0; i--) {
                            if (lockOwners[i].owner != NULL)
                                haveXactLock = true;
                        }
                    }
                }
            }

            if (!haveXactLock)
                continue;
        }

        /*
         * If we have both session- and transaction-level locks, fail.	This
         * should never happen with regular locks, since we only take those at
         * session level in some special operations like VACUUM.  It's
         * possible to hit this with advisory locks, though.
         *
         * It would be nice if we could keep the session hold and give away
         * the transactional hold to the prepared xact.  However, that would
         * require two PROCLOCK objects, and we cannot be sure that another
         * PROCLOCK will be available when it comes time for PostPrepare_Locks
         * to do the deed.	So for now, we error out while we can still do so
         * safely.
         */
        if (haveSessionLock)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot PREPARE while holding both session-level and transaction-level locks on the same "
                            "object")));

        /*
         * If the local lock was taken via the fast-path, we need to move it
         * to the primary lock table, or just get a pointer to the existing
         * primary lock table entry if by chance it's already been
         * transferred.
         */
        if (locallock->proclock == NULL) {
            locallock->proclock = FastPathGetRelationLockEntry(locallock);
            locallock->lock = locallock->proclock->tag.myLock;
        }

        /*
         * Arrange to not release any strong lock count held by this lock
         * entry.  We must retain the count until the prepared transaction is
         * committed or rolled back.
         */
        locallock->holdsStrongLockCount = FALSE;

        /*
         * Create a 2PC record.
         */
        errorno = memcpy_s(&(record.locktag), sizeof(LOCKTAG), &(locallock->tag.lock), sizeof(LOCKTAG));
        securec_check(errorno, "\0", "\0");
        record.lockmode = locallock->tag.mode;

        RegisterTwoPhaseRecord(TWOPHASE_RM_LOCK_ID, 0, &record, sizeof(TwoPhaseLockRecord));
    }
}

/*
 * PostPrepare_Locks
 *		Clean up after successful PREPARE
 *
 * Here, we want to transfer ownership of our locks to a dummy PGPROC
 * that's now associated with the prepared transaction, and we want to
 * clean out the corresponding entries in the LOCALLOCK table.
 *
 * Note: by removing the LOCALLOCK entries, we are leaving dangling
 * pointers in the transaction's resource owner.  This is OK at the
 * moment since resowner.c doesn't try to free locks retail at a toplevel
 * transaction commit or abort.  We could alternatively zero out nLocks
 * and leave the LOCALLOCK entries to be garbage-collected by LockReleaseAll,
 * but that probably costs more cycles.
 */
void PostPrepare_Locks(TransactionId xid)
{
    PGPROC *newproc = TwoPhaseGetDummyProc(xid);
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock = NULL;
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    PROCLOCKTAG proclocktag;
    bool found = false;
    int partition;

    /* Can't prepare a lock group follower. */
    Assert(t_thrd.proc->lockGroupLeader == NULL ||
           t_thrd.proc->lockGroupLeader == t_thrd.proc);

    /* This is a critical section: any error means big trouble */
    START_CRIT_SECTION();

    /*
     * First we run through the locallock table and get rid of unwanted
     * entries, then we scan the process's proclocks and transfer them to the
     * target proc.
     *
     * We do this separately because we may have multiple locallock entries
     * pointing to the same proclock, and we daren't end up with any dangling
     * pointers.
     */
    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        bool haveSessionLock = false;
        bool haveXactLock = false;
        int i;

        if (locallock->proclock == NULL || locallock->lock == NULL) {
            /*
             * We must've run out of shared memory while trying to set up this
             * lock.  Just forget the local entry.
             */
            Assert(locallock->nLocks == 0);
            RemoveLocalLock(locallock);
            continue;
        }

        /* Ignore VXID locks */
        if (locallock->tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
            continue;

        /* Scan to see whether we hold it at session or transaction level */
        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == NULL)
                haveSessionLock = true;
            else
                haveXactLock = true;
        }

        /* Ignore it if we have only session lock */
        if (!haveXactLock)
            continue;

        /* This can't happen, because we already checked it */
        if (haveSessionLock)
            ereport(PANIC,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot PREPARE while holding both session-level and transaction-level locks on the same "
                            "object")));

        /* Mark the proclock to show we need to release this lockmode */
        if (locallock->nLocks > 0)
            locallock->proclock->releaseMask |= LOCKBIT_ON((unsigned int)locallock->tag.mode);

        /* And remove the locallock hashtable entry */
        RemoveLocalLock(locallock);
    }

    /*
     * Now, scan each lock partition separately.
     */
    for (partition = 0; partition < NUM_LOCK_PARTITIONS; partition++) {
        LWLock *partitionLock = GetMainLWLockByIndex(FirstLockMgrLock + partition);
        SHM_QUEUE *procLocks = &(t_thrd.proc->myProcLocks[partition]);

        proclock = (PROCLOCK *)SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, procLink));
        if (proclock == NULL)
            continue; /* needn't examine this partition */

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        while (proclock != NULL) {
            PROCLOCK *nextplock = NULL;
            LOCKMASK holdMask;
            PROCLOCK *newproclock = NULL;

            /* Get link first, since we may unlink/delete this proclock */
            nextplock = (PROCLOCK *)SHMQueueNext(procLocks, &proclock->procLink, offsetof(PROCLOCK, procLink));

            Assert(proclock->tag.myProc == t_thrd.proc);

            lock = proclock->tag.myLock;

            /* Ignore VXID locks */
            if (lock->tag.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
                goto next_item;

            PROCLOCK_PRINT("PostPrepare_Locks", proclock);
            LOCK_PRINT("PostPrepare_Locks", lock, 0);
            Assert(lock->nRequested >= 0);
            Assert(lock->nGranted >= 0);
            Assert(lock->nGranted <= lock->nRequested);
            Assert((proclock->holdMask & ~lock->grantMask) == 0);

            /* Ignore it if nothing to release (must be a session lock) */
            if (proclock->releaseMask == 0)
                goto next_item;

            /* Else we should be releasing all locks */
            if (proclock->releaseMask != proclock->holdMask)
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("we seem to have dropped a bit somewhere")));

            holdMask = proclock->holdMask;

            /*
             * We cannot simply modify proclock->tag.myProc to reassign
             * ownership of the lock, because that's part of the hash key and
             * the proclock would then be in the wrong hash chain.	So, unlink
             * and delete the old proclock; create a new one with the right
             * contents; and link it into place.  We do it in this order to be
             * certain we won't run out of shared memory (the way dynahash.c
             * works, the deleted object is certain to be available for
             * reallocation).
             */
            SHMQueueDelete(&proclock->lockLink);
            SHMQueueDelete(&proclock->procLink);
            if (!hash_search(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&(proclock->tag), HASH_REMOVE, NULL))
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proclock table corrupted")));

            /*
             * Create the hash key for the new proclock table.
             */
            proclocktag.myLock = lock;
            proclocktag.myProc = newproc;

            /*
             * Update groupLeader pointer to point to the new proc.  (We'd
             * better not be a member of somebody else's lock group!)
             */
            Assert(proclock->groupLeader == proclock->tag.myProc);
            proclock->groupLeader = newproc;

            newproclock = (PROCLOCK *)hash_search(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&proclocktag,
                                                  HASH_ENTER_NULL, &found);
            if (newproclock == NULL)
                ereport(PANIC, /* should not happen */
                        (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                         errdetail("Not enough memory for reassigning the prepared transaction's locks.")));

            /*
             * If new, initialize the new entry
             */
            if (!found) {
                newproclock->holdMask = 0;
                newproclock->releaseMask = 0;
                /* Add new proclock to appropriate lists */
                SHMQueueInsertBefore(&lock->procLocks, &newproclock->lockLink);
                if (unlikely(newproc == NULL)) {
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("newproc should not be null")));
                }
                SHMQueueInsertBefore(&(newproc->myProcLocks[partition]), &newproclock->procLink);
                PROCLOCK_PRINT("PostPrepare_Locks: new", newproclock);
            } else {
                PROCLOCK_PRINT("PostPrepare_Locks: found", newproclock);
                Assert((newproclock->holdMask & ~lock->grantMask) == 0);
            }

            /*
             * Pass over the identified lock ownership.
             */
            Assert((newproclock->holdMask & holdMask) == 0);
            newproclock->holdMask |= holdMask;

next_item:
            proclock = nextplock;
        } /* loop over PROCLOCKs within this partition */

        LWLockRelease(partitionLock);
    } /* loop over partitions */

    END_CRIT_SECTION();
}

/*
 * Estimate shared-memory space used for lock tables
 */
Size LockShmemSize(void)
{
    Size size = 0;
    long max_table_size;

    /* lock hash table */
    max_table_size = NLOCKENTS();
    size = add_size(size, hash_estimate_size(max_table_size, sizeof(LOCK)));

    /* proclock hash table */
    max_table_size *= 2;
    size = add_size(size, hash_estimate_size(max_table_size, sizeof(PROCLOCK)));

    /*
     * Since NLOCKENTS is only an estimate, add 10% safety margin.
     */
    size = add_size(size, size / 10);

    return size;
}

/*
 * GetLockStatusData - Return a summary of the lock manager's internal
 * status, for use in a user-level reporting function.
 *
 * The return data consists of an array of PROCLOCK objects, with the
 * associated PGPROC and LOCK objects for each.  Note that multiple
 * copies of the same PGPROC and/or LOCK objects are likely to appear.
 * It is the caller's responsibility to match up duplicates if wanted.
 *
 * The design goal is to hold the LWLocks for as short a time as possible;
 * thus, this function simply makes a copy of the necessary data and releases
 * the locks, allowing the caller to contemplate and format the data for as
 * long as it pleases.
 */
LockData *GetLockStatusData(void)
{
    LockData *data = NULL;
    PROCLOCK *proclock = NULL;
    HASH_SEQ_STATUS seqstat;
    int els;
    int el;
    int i;

    data = (LockData *)palloc(sizeof(LockData));

    /* Guess how much space we'll need. */
    els = g_instance.shmem_cxt.MaxBackends;
    el = 0;
    data->locks = (LockInstanceData *)palloc(sizeof(LockInstanceData) * els);

    /*
     * First, we iterate through the per-backend fast-path arrays, locking
     * them one at a time.	This might produce an inconsistent picture of the
     * system state, but taking all of those LWLocks at the same time seems
     * impractical (in particular, note MAX_SIMUL_LWLOCKS).  It shouldn't
     * matter too much, because none of these locks can be involved in lock
     * conflicts anyway - anything that might must be present in the main lock
     * table.
     */
    for (i = 0; (unsigned int)(i) < g_instance.proc_base->allNonPreparedProcCount; ++i) {
        PGPROC *proc = g_instance.proc_base_all_procs[i];
        uint32 f;

        LWLockAcquire(proc->backendLock, LW_SHARED);

        for (f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; ++f) {
            LockInstanceData *instance = NULL;
            uint32 lockbits = FAST_PATH_GET_BITS(proc, f);
            /* Skip unallocated slots. */
            if (!lockbits)
                continue;

            if (el >= els) {
                els += g_instance.shmem_cxt.MaxBackends;
                data->locks = (LockInstanceData *)repalloc(data->locks, sizeof(LockInstanceData) * els);
            }

            instance = &data->locks[el];
            if (proc->fpRelId[f].partitionid != InvalidOid)
                SET_LOCKTAG_PARTITION(instance->locktag, proc->databaseId, proc->fpRelId[f].relid,
                                      proc->fpRelId[f].partitionid);
            else
                SET_LOCKTAG_RELATION(instance->locktag, proc->databaseId, proc->fpRelId[f].relid);

            instance->holdMask = lockbits << FAST_PATH_LOCKNUMBER_OFFSET;
            instance->waitLockMode = NoLock;
            instance->backend = proc->backendId;
            instance->lxid = proc->lxid;
            instance->pid = proc->pid;
            instance->globalSessionId = proc->globalSessionId;
            instance->sessionid = proc->sessionid;
            instance->fastpath = true;

            el++;
        }

        if (proc->fpVXIDLock) {
            VirtualTransactionId vxid;
            LockInstanceData *instance = NULL;

            if (el >= els) {
                els += g_instance.shmem_cxt.MaxBackends;
                data->locks = (LockInstanceData *)repalloc(data->locks, sizeof(LockInstanceData) * els);
            }

            vxid.backendId = proc->backendId;
            vxid.localTransactionId = proc->fpLocalTransactionId;

            instance = &data->locks[el];
            SET_LOCKTAG_VIRTUALTRANSACTION(instance->locktag, vxid);
            instance->holdMask = LOCKBIT_ON(ExclusiveLock);
            instance->waitLockMode = NoLock;
            instance->backend = proc->backendId;
            instance->lxid = proc->lxid;
            instance->pid = proc->pid;
            instance->sessionid = proc->sessionid;
            instance->globalSessionId = proc->globalSessionId;
            instance->fastpath = true;

            el++;
        }

        LWLockRelease(proc->backendLock);
    }

    /*
     * Next, acquire lock on the entire shared lock data structure.  We do
     * this so that, at least for locks in the primary lock table, the state
     * will be self-consistent.
     *
     * Since this is a read-only operation, we take shared instead of
     * exclusive lock.	There's not a whole lot of point to this, because all
     * the normal operations require exclusive lock, but it doesn't hurt
     * anything either. It will at least allow two backends to do
     * GetLockStatusData in parallel.
     *
     * Must grab LWLocks in partition-number order to avoid LWLock deadlock.
     */
    for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
        LWLockAcquire(GetMainLWLockByIndex(FirstLockMgrLock + i), LW_SHARED);

    /* Now we can safely count the number of proclocks */
    data->nelements = el + hash_get_num_entries(t_thrd.storage_cxt.LockMethodProcLockHash);
    if (data->nelements > els) {
        els = data->nelements;
        data->locks = (LockInstanceData *)repalloc(data->locks, sizeof(LockInstanceData) * els);
    }

    /* Now scan the tables to copy the data */
    hash_seq_init(&seqstat, t_thrd.storage_cxt.LockMethodProcLockHash);

    while ((proclock = (PROCLOCK *)hash_seq_search(&seqstat))) {
        PGPROC *proc = proclock->tag.myProc;
        LOCK *lock = proclock->tag.myLock;
        LockInstanceData *instance = &data->locks[el];
        errno_t errorno = EOK;

        errorno = memcpy_s(&instance->locktag, sizeof(LOCKTAG), &lock->tag, sizeof(LOCKTAG));
        securec_check(errorno, "\0", "\0");
        instance->holdMask = proclock->holdMask;
        if (proc->waitLock == proclock->tag.myLock)
            instance->waitLockMode = proc->waitLockMode;
        else
            instance->waitLockMode = NoLock;
        instance->backend = proc->backendId;
        instance->lxid = proc->lxid;
        instance->pid = proc->pid;
        instance->sessionid = proc->sessionid;
        instance->globalSessionId = proc->globalSessionId;
        instance->fastpath = false;

        el++;
    }

    /*
     * And release locks.  We do this in reverse order for two reasons: (1)
     * Anyone else who needs more than one of the locks will be trying to lock
     * them in increasing order; we don't want to release the other process
     * until it can get all the locks it needs. (2) This avoids O(N^2)
     * behavior inside LWLockRelease.
     */
    for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
        LWLockRelease(GetMainLWLockByIndex(FirstLockMgrLock + i));

    Assert(el == data->nelements);

    return data;
}

/*
 * Returns a list of currently held AccessExclusiveLocks, for use by
 * LogStandbySnapshot(). The result is a palloc'd array,
 * with the number of elements returned into *nlocks.
 *
 * XXX This currently takes a lock on all partitions of the lock table,
 * but it's possible to do better.  By reference counting locks and storing
 * the value in the ProcArray entry for each backend we could tell if any
 * locks need recording without having to acquire the partition locks and
 * scan the lock table.  Whether that's worth the additional overhead
 * is pretty dubious though.
 */
xl_standby_lock *GetRunningTransactionLocks(int *nlocks)
{
    xl_standby_lock *accessExclusiveLocks = NULL;
    PROCLOCK *proclock = NULL;
    HASH_SEQ_STATUS seqstat;
    int i;
    int index;
    int els;

    /*
     * Acquire lock on the entire shared lock data structure.
     *
     * Must grab LWLocks in partition-number order to avoid LWLock deadlock.
     */
    for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
        LWLockAcquire(GetMainLWLockByIndex(FirstLockMgrLock + i), LW_SHARED);

    /* Now we can safely count the number of proclocks */
    els = hash_get_num_entries(t_thrd.storage_cxt.LockMethodProcLockHash);
    /*
     * Allocating enough space for all locks in the lock table is overkill,
     * but it's more convenient and faster than having to enlarge the array.
     */
    if (els > 0) {
        accessExclusiveLocks = (xl_standby_lock *)palloc(els * sizeof(xl_standby_lock));
    } else {
        accessExclusiveLocks = (xl_standby_lock *)palloc(sizeof(xl_standby_lock));
    }

    /* Now scan the tables to copy the data */
    hash_seq_init(&seqstat, t_thrd.storage_cxt.LockMethodProcLockHash);

    /*
     * If lock is a currently granted AccessExclusiveLock then it will have
     * just one proclock holder, so locks are never accessed twice in this
     * particular case. Don't copy this code for use elsewhere because in the
     * general case this will give you duplicate locks when looking at
     * non-exclusive lock types.
     */
    index = 0;
    while ((proclock = (PROCLOCK *)hash_seq_search(&seqstat))) {
        /* make sure this definition matches the one used in LockAcquire */
        if ((proclock->holdMask & LOCKBIT_ON(AccessExclusiveLock)) &&
            proclock->tag.myLock->tag.locktag_type == LOCKTAG_RELATION) {
            PGPROC *proc = proclock->tag.myProc;
            PGXACT *pgxact = &g_instance.proc_base_all_xacts[proc->pgprocno];
            LOCK *lock = proclock->tag.myLock;
            TransactionId xid = pgxact->xid;

            /*
             * Don't record locks for transactions if we know they have
             * already issued their WAL record for commit but not yet released
             * lock. It is still possible that we see locks held by already
             * complete transactions, if they haven't yet zeroed their xids.
             */
            if (!TransactionIdIsValid(xid))
                continue;

            accessExclusiveLocks[index].xid = xid;
            accessExclusiveLocks[index].dbOid = lock->tag.locktag_field1;
            accessExclusiveLocks[index].relOid = lock->tag.locktag_field2;

            index++;
        }
    }

    if (index > els)
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("GetRunningTransactionLocks: index = %d els = %d", index, els)));

    /*
     * And release locks.  We do this in reverse order for two reasons: (1)
     * Anyone else who needs more than one of the locks will be trying to lock
     * them in increasing order; we don't want to release the other process
     * until it can get all the locks it needs. (2) This avoids O(N^2)
     * behavior inside LWLockRelease.
     */
    for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
        LWLockRelease(GetMainLWLockByIndex(FirstLockMgrLock + i));

    *nlocks = index;
    return accessExclusiveLocks;
}

/* Provide the textual name of any lock mode */
const char *GetLockmodeName(LOCKMETHODID lockmethodid, LOCKMODE mode)
{
    Assert(lockmethodid > 0 && lockmethodid < lengthof(LockMethods));
    Assert(mode > 0 && mode <= LockMethods[lockmethodid]->numLockModes);
    return LockMethods[lockmethodid]->lockModeNames[mode];
}

#if defined(LOCK_DEBUG) || defined(USE_ASSERT_CHECKING)

/*
 * Dump all locks in the given proc's myProcLocks lists.
 *
 * Caller is responsible for having acquired appropriate LWLocks.
 */
void DumpLocks(PGPROC *proc)
{
    SHM_QUEUE *procLocks = NULL;
    PROCLOCK *proclock = NULL;
    LOCK *lock = NULL;
    int i;

    if (proc == NULL)
        return;

    if (proc->waitLock)
        LOCK_PRINT("DumpLocks: waiting on", proc->waitLock, 0);

    for (i = 0; i < NUM_LOCK_PARTITIONS; i++) {
        procLocks = &(proc->myProcLocks[i]);

        proclock = (PROCLOCK *)SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, procLink));

        while (proclock != NULL) {
            Assert(proclock->tag.myProc == proc);

            lock = proclock->tag.myLock;

            PROCLOCK_PRINT("DumpLocks", proclock);
            LOCK_PRINT("DumpLocks", lock, 0);

            proclock = (PROCLOCK *)SHMQueueNext(procLocks, &proclock->procLink, offsetof(PROCLOCK, procLink));
        }
    }
}

/*
 * Dump all lmgr locks.
 *
 * Caller is responsible for having acquired appropriate LWLocks.
 */
void DumpAllLocks(void)
{
    PGPROC *proc = NULL;
    PROCLOCK *proclock = NULL;
    LOCK *lock = NULL;
    HASH_SEQ_STATUS status;

    /* set print flag */
    u_sess->attr.attr_storage.Debug_deadlocks = true;

    proc = t_thrd.proc;

    if ((proc != NULL) && proc->waitLock)
        LOCK_PRINT("DumpAllLocks: waiting on", proc->waitLock, 0);

    hash_seq_init(&status, t_thrd.storage_cxt.LockMethodProcLockHash);

    while ((proclock = (PROCLOCK *)hash_seq_search(&status)) != NULL) {
        PROCLOCK_PRINT("DumpAllLocks", proclock);

        lock = proclock->tag.myLock;
        if (lock != NULL)
            LOCK_PRINT("DumpAllLocks", lock, 0);
        else
            ereport(LOG, (errmsg("DumpAllLocks: proclock->tag.myLock = NULL")));
    }

    /* reset print flag */
    u_sess->attr.attr_storage.Debug_deadlocks = false;
}
#endif /* LOCK_DEBUG */

/*
 * LOCK 2PC resource manager's routines
 *
 *
 * Re-acquire a lock belonging to a transaction that was prepared.
 *
 * Because this function is run at db startup, re-acquiring the locks should
 * never conflict with running transactions because there are none.  We
 * assume that the lock state represented by the stored 2PC files is legal.
 *
 * When switching from Hot Standby mode to normal operation, the locks will
 * be already held by the startup process. The locks are acquired for the new
 * procs without checking for conflicts, so we don't get a conflict between the
 * startup process and the dummy procs, even though we will momentarily have
 * a situation where two procs are holding the same AccessExclusiveLock,
 * which isn't normally possible because the conflict. If we're in standby
 * mode, but a recovery snapshot hasn't been established yet, it's possible
 * that some but not all of the locks are already held by the startup process.
 *
 * This approach is simple, but also a bit dangerous, because if there isn't
 * enough shared memory to acquire the locks, an error will be thrown, which
 * is promoted to FATAL and recovery will abort, bringing down postmaster.
 * A safer approach would be to transfer the locks like we do in
 * AtPrepare_Locks, but then again, in hot standby mode it's possible for
 * read-only backends to use up all the shared lock memory anyway, so that
 * replaying the WAL record that needs to acquire a lock will throw an error
 * and PANIC anyway.
 */
void lock_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    TwoPhaseLockRecord *rec = (TwoPhaseLockRecord *)recdata;
    PGPROC *proc = TwoPhaseGetDummyProc(xid);
    LOCKTAG *locktag = NULL;
    LOCKMODE lockmode;
    LOCKMETHODID lockmethodid;
    LOCK *lock = NULL;
    PROCLOCK *proclock = NULL;
    PROCLOCKTAG proclocktag;
    bool found = false;
    uint32 hashcode;
    uint32 proclock_hashcode;
    int partition;
    LWLock *partitionLock = NULL;
    LockMethod lockMethodTable;
    errno_t rc = EOK;

    Assert(len == sizeof(TwoPhaseLockRecord));
    locktag = &rec->locktag;
    lockmode = rec->lockmode;
    lockmethodid = locktag->locktag_lockmethodid;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];

    hashcode = LockTagHashCode(locktag);
    partition = LockHashPartition(hashcode);
    partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Find or create a lock with this tag.
     */
    lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)locktag, hashcode,
                                               HASH_ENTER_NULL, &found);
    if (lock == NULL) {
        LWLockRelease(partitionLock);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                        errhint("You might need to increase max_locks_per_transaction.")));
    }

    /*
     * if it's a new lock object, initialize it
     */
    if (!found) {
        lock->grantMask = 0;
        lock->waitMask = 0;
        SHMQueueInit(&(lock->procLocks));
        ProcQueueInit(&(lock->waitProcs));
        lock->nRequested = 0;
        lock->nGranted = 0;
        rc = memset_s(lock->requested, sizeof(lock->requested), 0, sizeof(lock->requested));
        securec_check(rc, "", "");
        rc = memset_s(lock->granted, sizeof(lock->granted), 0, sizeof(lock->granted));
        securec_check(rc, "", "");
        LOCK_PRINT("lock_twophase_recover: new", lock, lockmode);
    } else {
        LOCK_PRINT("lock_twophase_recover: found", lock, lockmode);
        Assert((lock->nRequested >= 0) && (lock->requested[lockmode] >= 0));
        Assert((lock->nGranted >= 0) && (lock->granted[lockmode] >= 0));
        Assert(lock->nGranted <= lock->nRequested);
    }

    /*
     * Create the hash key for the proclock table.
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc;

    proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

    /*
     * Find or create a proclock entry with this tag
     */
    proclock = (PROCLOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodProcLockHash, (void *)&proclocktag,
                                                       proclock_hashcode, HASH_ENTER_NULL, &found);
    if (proclock == NULL) {
        /* Ooops, not enough shmem for the proclock */
        if (lock->nRequested == 0) {
            /*
             * There are no other requestors of this lock, so garbage-collect
             * the lock object.  We *must* do this to avoid a permanent leak
             * of shared memory, because there won't be anything to cause
             * anyone to release the lock object later.
             */
            Assert(SHMQueueEmpty(&(lock->procLocks)));
            if (!hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (void *)&(lock->tag), hashcode,
                                             HASH_REMOVE, NULL))
                ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("lock table corrupted")));
        }
        LWLockRelease(partitionLock);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                        errhint("You might need to increase max_locks_per_transaction.")));
    }

    /*
     * If new, initialize the new entry
     */
    if (!found) {
        Assert(proc->lockGroupLeader == NULL);
        proclock->groupLeader = proc;
        proclock->holdMask = 0;
        proclock->releaseMask = 0;
        /* Add proclock to appropriate lists */
        SHMQueueInsertBefore(&lock->procLocks, &proclock->lockLink);
        if (unlikely(proc == NULL)) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("proc should not be null")));
        }
        SHMQueueInsertBefore(&(proc->myProcLocks[partition]), &proclock->procLink);
        PROCLOCK_PRINT("lock_twophase_recover: new", proclock);
    } else {
        PROCLOCK_PRINT("lock_twophase_recover: found", proclock);
        Assert((proclock->holdMask & ~lock->grantMask) == 0);
    }

    /*
     * lock->nRequested and lock->requested[] count the total number of
     * requests, whether granted or waiting, so increment those immediately.
     */
    lock->nRequested++;
    lock->requested[lockmode]++;
    Assert((lock->nRequested > 0) && (lock->requested[lockmode] > 0));

    /*
     * We shouldn't already hold the desired lock.
     */
    if (proclock->holdMask & LOCKBIT_ON((unsigned int)lockmode)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("lock %s on object %u/%u/%u is already held", lockMethodTable->lockModeNames[lockmode],
                               lock->tag.locktag_field1, lock->tag.locktag_field2, lock->tag.locktag_field3)));
    }

    /*
     * We ignore any possible conflicts and just grant ourselves the lock. Not
     * only because we don't bother, but also to avoid deadlocks when
     * switching from standby to normal mode. See function comment.
     */
    GrantLock(lock, proclock, lockmode);

    /*
     * Bump strong lock count, to make sure any fast-path lock requests won't
     * be granted without consulting the primary lock table.
     */
    if (ConflictsWithRelationFastPath(&lock->tag, lockmode)) {
        uint32 fasthashcode = FastPathStrongLockHashPartition(hashcode);

        SpinLockAcquire(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
        t_thrd.storage_cxt.FastPathStrongRelationLocks->count[fasthashcode]++;
        SpinLockRelease(&t_thrd.storage_cxt.FastPathStrongRelationLocks->mutex);
    }

    LWLockRelease(partitionLock);
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Find and release the lock indicated by the 2PC record.
 */
void lock_twophase_postcommit(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    TwoPhaseLockRecord *rec = (TwoPhaseLockRecord *)recdata;
    PGPROC *proc = TwoPhaseGetDummyProc(xid);
    LOCKTAG *locktag = NULL;
    LOCKMETHODID lockmethodid;
    LockMethod lockMethodTable;

    Assert(len == sizeof(TwoPhaseLockRecord));
    locktag = &rec->locktag;
    lockmethodid = locktag->locktag_lockmethodid;

    CHECK_LOCKMETHODID(lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];

    LockRefindAndRelease(lockMethodTable, proc, locktag, rec->lockmode, true);
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * This is actually just the same as the COMMIT case.
 */
void lock_twophase_postabort(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
    lock_twophase_postcommit(xid, info, recdata, len);
}

/*
 *		VirtualXactLockTableInsert
 *
 *		Take vxid lock via the fast-path.  There can't be any pre-existing
 *		lockers, as we haven't advertised this vxid via the ProcArray yet.
 *
 *		Since t_thrd.proc->fpLocalTransactionId will normally contain the same data
 *		as t_thrd.proc->lxid, you might wonder if we really need both.  The
 *		difference is that t_thrd.proc->lxid is set and cleared unlocked, and
 *		examined by procarray.c, while fpLocalTransactionId is protected by
 *		backendLock and is used only by the locking subsystem.	Doing it this
 *		way makes it easier to verify that there are no funny race conditions.
 *
 *		We don't bother recording this lock in the local lock table, since it's
 *		only ever released at the end of a transaction.  Instead,
 *		LockReleaseAll() calls VirtualXactLockTableCleanup().
 */
void VirtualXactLockTableInsert(const VirtualTransactionId &vxid)
{
    Assert(VirtualTransactionIdIsValid(vxid));

    LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);

    Assert(t_thrd.proc->backendId == vxid.backendId);
    Assert(t_thrd.proc->fpLocalTransactionId == InvalidLocalTransactionId);
    Assert(t_thrd.proc->fpVXIDLock == false);

    t_thrd.proc->fpVXIDLock = true;
    t_thrd.proc->fpLocalTransactionId = vxid.localTransactionId;

    LWLockRelease(t_thrd.proc->backendLock);
}

/*
 *		VirtualXactLockTableCleanup
 *
 *		Check whether a VXID lock has been materialized; if so, release it,
 *		unblocking waiters.
 */
void VirtualXactLockTableCleanup()
{
    bool fastpath = false;
    LocalTransactionId lxid;

    Assert(t_thrd.proc->backendId != InvalidBackendId);

    /*
     * Clean up shared memory state.
     */
    LWLockAcquire(t_thrd.proc->backendLock, LW_EXCLUSIVE);

    fastpath = t_thrd.proc->fpVXIDLock;
    lxid = t_thrd.proc->fpLocalTransactionId;
    t_thrd.proc->fpVXIDLock = false;
    t_thrd.proc->fpLocalTransactionId = InvalidLocalTransactionId;

    LWLockRelease(t_thrd.proc->backendLock);

    /*
     * If fpVXIDLock has been cleared without touching fpLocalTransactionId,
     * that means someone transferred the lock to the main lock table.
     */
    if (!fastpath && LocalTransactionIdIsValid(lxid)) {
        VirtualTransactionId vxid;
        LOCKTAG locktag;

        vxid.backendId = t_thrd.proc_cxt.MyBackendId;
        vxid.localTransactionId = lxid;
        SET_LOCKTAG_VIRTUALTRANSACTION(locktag, vxid);

        LockRefindAndRelease(LockMethods[DEFAULT_LOCKMETHOD], t_thrd.proc, &locktag, ExclusiveLock, false);
    }
}

/*
 *		VirtualXactLock
 *
 * If wait = true, wait until the given VXID has been released, and then
 * return true.
 *
 * If wait = false, just check whether the VXID is still running, and return
 * true or false.
 */
bool VirtualXactLock(const VirtualTransactionId &vxid, bool wait)
{
    LOCKTAG tag;
    PGPROC *proc = NULL;

    Assert(VirtualTransactionIdIsValid(vxid));

    SET_LOCKTAG_VIRTUALTRANSACTION(tag, vxid);

    /*
     * If a lock table entry must be made, this is the PGPROC on whose behalf
     * it must be done.  Note that the transaction might end or the PGPROC
     * might be reassigned to a new backend before we get around to examining
     * it, but it doesn't matter.  If we find upon examination that the
     * relevant lxid is no longer running here, that's enough to prove that
     * it's no longer running anywhere.
     */
    proc = BackendIdGetProc(vxid.backendId);
    if (proc == NULL)
        return true;

    /*
     * We must acquire this lock before checking the backendId and lxid
     * against the ones we're waiting for.  The target backend will only set
     * or clear lxid while holding this lock.
     */
    LWLockAcquire(proc->backendLock, LW_EXCLUSIVE);

    /* If the transaction has ended, our work here is done. */
    if (proc->backendId != vxid.backendId || proc->fpLocalTransactionId != vxid.localTransactionId) {
        LWLockRelease(proc->backendLock);
        return true;
    }

    /*
     * If we aren't asked to wait, there's no need to set up a lock table
     * entry.  The transaction is still in progress, so just return false.
     */
    if (!wait) {
        LWLockRelease(proc->backendLock);
        return false;
    }

    /*
     * OK, we're going to need to sleep on the VXID.  But first, we must set
     * up the primary lock table entry, if needed (ie, convert the proc's
     * fast-path lock on its VXID to a regular lock).
     */
    if (proc->fpVXIDLock) {
        PROCLOCK *proclock = NULL;
        uint32 hashcode;
        LWLock *partitionLock = NULL;

        hashcode = LockTagHashCode(&tag);

        partitionLock = LockHashPartitionLock(hashcode);
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        proclock = SetupLockInTable(LockMethods[DEFAULT_LOCKMETHOD], proc, &tag, hashcode, ExclusiveLock);
        if (proclock == NULL) {
            LWLockRelease(partitionLock);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory"),
                            errhint("You might need to increase max_locks_per_transaction.")));
        }
        GrantLock(proclock->tag.myLock, proclock, ExclusiveLock);

        LWLockRelease(partitionLock);

        proc->fpVXIDLock = false;
    }

    /* Done with proc->fpLockBits */
    LWLockRelease(proc->backendLock);

    /* Time to wait. */
    (void)LockAcquire(&tag, ShareLock, false, false);

    (void)LockRelease(&tag, ShareLock, false);
    return true;
}

/*
 * LockWaiterCount
 *
 * Find the number of lock requester on this locktag
 */
int LockWaiterCount(const LOCKTAG *locktag)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LOCK *lock = NULL;
    bool found = false;
    uint32 hashcode;
    LWLock *partitionLock = NULL;
    int waiters = 0;

    CHECK_LOCKMETHODID(lockmethodid);

    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    lock = (LOCK *)hash_search_with_hash_value(t_thrd.storage_cxt.LockMethodLockHash, (const void *)locktag, hashcode,
                                               HASH_FIND, &found);
    if (found) {
        Assert(lock != NULL);
        waiters = lock->nRequested;
    }
    LWLockRelease(partitionLock);

    return waiters;
}

static bool SSDmsLockAcquire(LOCALLOCK *locallock, bool dontWait, int waitSec)
{
    dms_context_t dms_ctx;
    dms_drlatch_t dlatch;
    bool ret = true;
    bool timeout = true;
    bool skipAcquire = false;
    int waitMilliSec = 0;
    int needWaitMilliSec = 0;
    TimestampTz startTime;
    LOCKMODE lockmode;

    InitDmsContext(&dms_ctx);
    TransformLockTagToDmsLatch(&dlatch, locallock->tag.lock);

    needWaitMilliSec = (waitSec == 0) ? u_sess->attr.attr_storage.LockWaitTimeout : waitSec * SEC2MILLISEC;
    startTime = GetCurrentTimestamp();
    lockmode = locallock->tag.mode;

    PG_TRY();
    {
        do {
            if (lockmode < AccessExclusiveLock && SS_NORMAL_STANDBY) {
                ret = dms_latch_timed_s(&dms_ctx, &dlatch, SS_ACQUIRE_LOCK_DO_NOT_WAIT, (unsigned char)false);
            } else if (lockmode >= AccessExclusiveLock && SS_NORMAL_PRIMARY) {
                ret = dms_latch_timed_x(&dms_ctx, &dlatch, SS_ACQUIRE_LOCK_DO_NOT_WAIT);
            } else {
                // skip if lockmode do not meet ss lock request, or openGauss in reform process
                skipAcquire = true;
                break;
            }

            // acquire failed, and need wait
            if (!(dontWait || ret)) {
                CHECK_FOR_INTERRUPTS();

                // first entry
                if (waitMilliSec == 0) {
                    SSLOCK_PRINT("WaitOnLock: sleeping on ss_lock start", locallock, lockmode);
                    instr_stmt_report_lock(LOCK_WAIT_START, lockmode, &locallock->tag.lock);
                }

                waitMilliSec = ComputeTimeStamp(startTime);
                timeout = (waitMilliSec >= needWaitMilliSec);
                if (timeout) {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_WAIT_TIMEOUT),
                        errmsg("SSLock wait timeout after %d ms", waitMilliSec)));
                }
                pg_usleep(SS_ACQUIRE_LOCK_RETRY_INTERVAL * MILLISEC2MICROSEC);  // 50 ms
            }
        } while (!(timeout || ret));
    }
    PG_CATCH();
    {
        instr_stmt_report_lock(LOCK_WAIT_END);
        instr_stmt_report_lock(LOCK_END, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (waitMilliSec != 0) {
        instr_stmt_report_lock(LOCK_WAIT_END);
        SSLOCK_PRINT("WaitOnLock: sleeping on ss_lock end", locallock, locallock->tag.mode);
    }

    locallock->ssLock = ret && !skipAcquire;
    return ret;
}

static void SSDmsLockRelease(LOCALLOCK *locallock)
{
    dms_context_t dms_ctx;
    dms_drlatch_t dlatch;

    if (!locallock->ssLock) {
        return;
    }

    InitDmsContext(&dms_ctx);
    TransformLockTagToDmsLatch(&dlatch, locallock->tag.lock);

    locallock->ssLock = FALSE;
    dms_unlatch(&dms_ctx, &dlatch);
}
