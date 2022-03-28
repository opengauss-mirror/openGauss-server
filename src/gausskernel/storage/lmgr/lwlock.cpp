/* -------------------------------------------------------------------------
 *
 * lwlock.cpp
 *     Lightweight lock manager
 *
 * Lightweight locks are intended primarily to provide mutual exclusion of
 * access to shared-memory data structures.  Therefore, they offer both
 * exclusive and shared lock modes (to support read/write and read-only
 * access to a shared object). There are few other frammishes.  User-level
 * locking should be done with the full lock manager --- which depends on
 * LWLocks to protect its shared state.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/lmgr/lwlock.cpp
 *
 * NOTES:
 *
 * This used to be a pretty straight forward reader-writer lock
 * implementation, in which the internal state was protected by a
 * spinlock. Unfortunately the overhead of taking the spinlock proved to be
 * too high for workloads/locks that were taken in shared mode very
 * frequently. Often we were spinning in the (obviously exclusive) spinlock,
 * while trying to acquire a shared lock that was actually free.
 *
 * Thus a new implementation was devised that provides wait-free shared lock
 * acquisition for locks that aren't exclusively locked.
 *
 * The basic idea is to have a single atomic variable 'lockcount' instead of
 * the formerly separate shared and exclusive counters and to use atomic
 * operations to acquire the lock. That's fairly easy to do for plain
 * rw-spinlocks, but a lot harder for something like LWLocks that want to wait
 * in the OS.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the lockcount
 * variable. For exclusive lock we swap in a sentinel value
 * (LW_VAL_EXCLUSIVE), for shared locks we count the number of holders.
 *
 * To release the lock we use an atomic decrement to release the lock. If the
 * new value is zero (we get that atomically), we know we can/have to release
 * waiters.
 *
 * Obviously it is important that the sentinel value for exclusive locks
 * doesn't conflict with the maximum number of possible share lockers -
 * luckily MAX_BACKENDS makes that easily possible.
 *
 *
 * The attentive reader might have noticed that naively doing the above has a
 * glaring race condition: We try to lock using the atomic operations and
 * notice that we have to wait. Unfortunately by the time we have finished
 * queuing, the former locker very well might have already finished it's
 * work. That's problematic because we're now stuck waiting inside the OS.
 *
 * To mitigate those races we use a two phased attempt at locking:
 *   Phase 1: Try to do it atomically, if we succeed, nice
 *   Phase 2: Add ourselves to the waitqueue of the lock
 *   Phase 3: Try to grab the lock again, if we succeed, remove ourselves from
 *            the queue
 *   Phase 4: Sleep till wake-up, goto Phase 1
 *
 * This protects us against the problem from above as nobody can release too
 *    quick, before we're queued, since after Phase 2 we're already queued.
 * -------------------------------------------------------------------------
 */
#include "storage/dfs/dfscache_mgr.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "commands/async.h"
#include "commands/copy.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock_be.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/lock/s_lock.h"
#include "storage/spin.h"
#include "storage/cucache_mgr.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "instruments/instr_event.h"
#include "instruments/instr_statement.h"
#include "tsan_annotation.h"

#ifndef MAX
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#endif

#define LW_FLAG_HAS_WAITERS ((uint32)1 << 30)
#define LW_FLAG_RELEASE_OK ((uint32)1 << 29)
#define LW_FLAG_LOCKED ((uint32)1 << 28)

#define LW_VAL_EXCLUSIVE ((uint32)1 << 24)
#define LW_VAL_SHARED 1

#define LW_LOCK_MASK ((uint32)((1 << 25) - 1))
#ifdef LOCK_DEBUG
    /* Must be greater than MAX_BACKENDS - which is 2^23-1, so we're fine. */
    #define LW_SHARED_MASK ((uint32)(1 << 23))
#endif

#define LWLOCK_TRANCHE_SIZE 128

const char **LWLockTrancheArray = NULL;
int LWLockTranchesAllocated = 0;

/*
 * The array MainLWLockNames represents the name of individual locks
 * for LWLock in src/include/storage/lwlocknames.h.
 * The definition is moved to lwlocknames.cpp, which is generated through generate-lwlocknames.pl in Makefile
 *
 *
 * The array BuiltinTrancheNames represents LWLock tranche' names
 * for BuiltinTrancheIds in src/include/storage/lock/lwlock.h.
 * The order should be consistent with the order of BuiltinTrancheIds.
 */
static const char *BuiltinTrancheNames[] = {
    "BufMappingLock",
    "LockMgrLock",
    "PredicateLockMgrLock",
    "OperatorRealTLock",
    "OperatorHistLock",
    "SessionRealTLock",
    "SessionHistLock",
    "InstanceRealTLock",
    "CacheSlotMappingLock",
    "CSNBufMappingLock",
    "CLogBufMappingLock",
    "UniqueSQLMappingLock",
    "InstrUserLockId",
    "GPCMappingLock",
    "UspagrpMappingLock", 
    "ProcXactMappingLock",
    "ASPMappingLock",
    "GlobalSeqLock",
    "GlobalWorkloadLock",
    "NormalizedSqlLock",
    "StartBlockMappingLock",
    "BufferIOLock",
    "BufferContentLock",
    "UndoPerZoneLock",
    "UndoSpaceLock",
    "DataCacheLock",
    "MetaCacheLock",
    "PGPROCLock",
    "ReplicationSlotLock",
    "Async Ctl",
    "CLOG Ctl",
    "CSNLOG Ctl",
    "MultiXactOffset Ctl",
    "MultiXactMember Ctl",
    "OldSerXid SLRU Ctl",
    "WALInsertLock",
    "DoubleWriteLock",
    "DWSingleFlushFirstLock",
    "DWSingleFlushSecondLock",
    "DWSingleFlushSecondBufTagLock",
    "RestartPointQueueLock",
    "PruneDirtyQueueLock",
    "UnlinkRelHashTblLock",
    "UnlinkRelForkHashTblLock",
    "LWTRANCHE_ACCOUNT_TABLE",
    "GeneralExtendedLock",
    "MPFLLOCK",
    "GlobalTempTableControl", /* LWTRANCHE_GTT_CTL */
    "PLdebugger",
    "NGroupMappingLock",
    "MatviewSeqnoLock",
    "IOStatLock",
    "WALFlushWait",
    "WALBufferInitWait",
    "WALInitSegment",
    "SegmentHeadPartitionLock",
    "TwoPhaseStatePartLock",
    "RoleIdPartLock",
    "PgwrSyncQueueLock",
    "BarrierHashTblLock",
    "PageRepairHashTblLock",
    "FileRepairHashTblLock",
    "ReplicationOriginSlotLock",
    "AuditIndextblLock"
};

static void RegisterLWLockTranches(void);
static void InitializeLWLocks(int numLocks);

#ifdef LWLOCK_STATS
typedef struct lwlock_stats_key {
    int tranche;
    void *instance;
} lwlock_stats_key;

typedef struct lwlock_stats {
    lwlock_stats_key key;
    int sh_acquire_count;
    int ex_acquire_count;
    int block_count;
    int dequeue_self_count;
    int spin_delay_count;
} lwlock_stats;

static HTAB *lwlock_stats_htab;
static lwlock_stats lwlock_stats_dummy;
#endif

#ifdef LOCK_DEBUG
bool Trace_lwlocks = false;

inline static void PRINT_LWDEBUG(const char *where, LWLock *lock, LWLockMode mode)
{
    /* hide statement & context here, otherwise the log is just too verbose */
    if (Trace_lwlocks) {
        uint32 state = pg_atomic_read_u32(&lock->state);
        ereport(LOG, (errhidestmt(true), errhidecontext(true),
                      errmsg("%d: %s(%s): excl %u shared %u haswaiters %u waiters %u rOK %d",
                             t_thrd.proc_cxt.MyProcPid, where, T_NAME(lock), !!(state & LW_VAL_EXCLUSIVE),
                             state & LW_SHARED_MASK, !!(state & LW_FLAG_HAS_WAITERS),
                             pg_atomic_read_u32(&lock->nwaiters), !!(state & LW_FLAG_RELEASE_OK))));
    }
}

inline static void LOG_LWDEBUG(const char *where, LWLock *lock, const char *msg)
{
    /* hide statement & context here, otherwise the log is just too verbose */
    if (Trace_lwlocks) {
        ereport(LOG,
                (errhidestmt(true), errhidecontext(true), errmsg("%s(%s): %s", where, T_NAME(lock), msg)));
    }
}

#else /* not LOCK_DEBUG */
#define PRINT_LWDEBUG(a, b, c) ((void)0)
#define LOG_LWDEBUG(a, b, c) ((void)0)
#endif /* LOCK_DEBUG */

#ifdef LWLOCK_STATS

static void init_lwlock_stats(void);
static void print_lwlock_stats(int code, Datum arg);
static lwlock_stats* get_lwlock_stats_entry(LWLock* lockid);

static void init_lwlock_stats(void)
{
    HASHCTL ctl;
    static MemoryContext lwlock_stats_cxt = NULL;
    static bool exit_registered = false;

    if (lwlock_stats_cxt != NULL) {
        MemoryContextDelete(lwlock_stats_cxt);
    }

    /*
     * The LWLock stats will be updated within a critical section, which
     * requires allocating new hash entries. Allocations within a critical
     * section are normally not allowed because running out of memory would
     * lead to a PANIC, but LWLOCK_STATS is debugging code that's not normally
     * turned on in production, so that's an acceptable risk. The hash entries
     * are small, so the risk of running out of memory is minimal in practice.
     */
    lwlock_stats_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "LWLock stats", ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextAllowInCriticalSection(lwlock_stats_cxt, true);

    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(lwlock_stats_key);
    ctl.entrysize = sizeof(lwlock_stats);
    ctl.hcxt = lwlock_stats_cxt;
    lwlock_stats_htab = hash_create("lwlock stats", 16384, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    if (!exit_registered) {
        on_shmem_exit(print_lwlock_stats, 0);
        exit_registered = true;
    }
}

static void print_lwlock_stats(int code, Datum arg)
{
    HASH_SEQ_STATUS scan;
    lwlock_stats *lwstats = NULL;

    hash_seq_init(&scan, lwlock_stats_htab);

    /* Grab an LWLock to keep different backends from mixing reports */
    LWLockAcquire(GetMainLWLockByIndex(0), LW_EXCLUSIVE);

    while ((lwstats = (lwlock_stats*)hash_seq_search(&scan)) != NULL) {
        fprintf(stderr,
                "PID %d lwlock %s: shacq %u exacq %u blk %u spindelay %u dequeue self %u\n",
                t_thrd.proc_cxt.MyProcPid,
                LWLockTrancheArray[lwstats->key.tranche]->name,
                lwstats->sh_acquire_count,
                lwstats->ex_acquire_count,
                lwstats->block_count,
                lwstats->spin_delay_count,
                lwstats->dequeue_self_count);
    }

    LWLockRelease(GetMainLWLockByIndex(0));
}

static lwlock_stats *get_lwlock_stats_entry(LWLock *lock)
{
    lwlock_stats_key key;
    lwlock_stats *lwstats = NULL;
    bool found = false;

    /*
     * During shared memory initialization, the hash table doesn't exist yet.
     * Stats of that phase aren't very interesting, so just collect operations
     * on all locks in a single dummy entry.
     */
    if (lwlock_stats_htab == NULL) {
        return &lwlock_stats_dummy;
    }

    /* Fetch or create the entry. */
    key.tranche = lock->tranche;
    key.instance = lock;
    lwstats = hash_search(lwlock_stats_htab, &key, HASH_ENTER, &found);
    if (!found) {
        lwstats->sh_acquire_count = 0;
        lwstats->ex_acquire_count = 0;
        lwstats->block_count = 0;
        lwstats->dequeue_self_count = 0;
        lwstats->spin_delay_count = 0;
    }
    return lwstats;
}
#endif /* LWLOCK_STATS */

/*
 * Compute number of LWLocks to allocate.
 */
int NumLWLocks(void)
{
    int numLocks;
    uint32 maxConn = g_instance.attr.attr_network.MaxConnections;
    uint32 maxThreadNum = 0;
    if (ENABLE_THREAD_POOL) {
        maxThreadNum = g_threadPoolControler->GetThreadNum();
    }
    uint32 numLockFactor = 4;

    /*
     * Possibly this logic should be spread out among the affected modules,
     * the same way that shmem space estimation is done.  But for now, there
     * are few enough users of LWLocks that we can get away with just keeping
     * the knowledge here.
     */
    /* Predefined LWLocks */
    numLocks = (int)NumFixedLWLocks;

    /* bufmgr.c needs two for each shared buffer */
    numLocks += 2 * TOTAL_BUFFER_NUM;

    /* each zone owns undo space lock */
    numLocks += MAX(maxConn, maxThreadNum) * numLockFactor * UNDO_ZONE_LOCK;

    /* cucache_mgr.cpp CU Cache calculates its own requirements */
    numLocks += DataCacheMgrNumLocks();

    /* dfscache_mgr.cpp Meta data Cache calculates its own requirements */
    numLocks += MetaCacheMgrNumLocks();

    /* proc.c needs one for each backend or auxiliary process. For prepared xacts,
     * backendLock is actually not allocated. */
    numLocks += (2 * GLOBAL_ALL_PROCS - g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS);

    /* clog.c needs one per CLOG buffer */
    numLocks += CLOGShmemBuffers();

    /* csnlog.c needs one per CLOG buffer */
    numLocks += NUM_CSNLOG_PARTITIONS * CSNLOGShmemBuffers();

    /* clog.c needs one per CLOG buffer */
    numLocks += NUM_CLOG_PARTITIONS * CLOGShmemBuffers();

    /* multixact.c needs two SLRU areas */
    numLocks += NUM_MXACTOFFSET_BUFFERS + NUM_MXACTMEMBER_BUFFERS;

    /* async.c needs one per Async buffer */
    numLocks += NUM_ASYNC_BUFFERS;

    /* predicate.c needs one per old serializable xid buffer */
    numLocks += NUM_OLDSERXID_BUFFERS;

    /* slot.c needs one for each slot */
    numLocks += g_instance.attr.attr_storage.max_replication_slots;

    /* double write.c needs flush lock */
    numLocks += 1;   /* dw batch flush lock */
    numLocks += 3;  /* dw single flush pos lock (two version) + second version buftag page lock */

    /* for materialized view */
    numLocks += 1;

    /* for WALFlushWait lock, WALBufferInitWait lock and WALInitSegment lock */
    numLocks += 3;
    
    /* for recovery state queue */
    numLocks += 1;

    /* for prune dirty queue */
    numLocks += 1;

    /* for unlink rel hashtbl, one is for all fork relation hashtable, one is for one fork relation hash table */
    numLocks += 2;

    /* for incre ckpt sync request queue lock */
    numLocks +=1;
    /* for page repair hash table and file repair hash table */
    numLocks += 2;

    /* for barrier preparse hashtbl */
    numLocks += 1;

    /*
     * Add any requested by loadable modules; for backwards-compatibility
     * reasons, allocate at least NUM_USER_DEFINED_LWLOCKS of them even if
     * there are no explicit requests.
     */
    t_thrd.storage_cxt.lock_addin_request_allowed = false;
    numLocks += Max(t_thrd.storage_cxt.lock_addin_request, NUM_USER_DEFINED_LWLOCKS);

    return numLocks;
}

/*
 * RequestAddinLWLocks
 *		Request that extra LWLocks be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.	Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void RequestAddinLWLocks(int n)
{
    if (IsUnderPostmaster || !t_thrd.storage_cxt.lock_addin_request_allowed) {
        return; /* too late */
    }
    t_thrd.storage_cxt.lock_addin_request += n;
}

/*
 * Compute shmem space needed for LWLocks.
 */
Size LWLockShmemSize(void)
{
    Size size;
    int numLocks = NumLWLocks();

    /* Space for the LWLock array. */
    size = mul_size(numLocks, sizeof(LWLockPadded));

    /* Space for dynamic allocation counter, plus room for alignment. */
    size = add_size(size, 3 * sizeof(int) + LWLOCK_PADDED_SIZE);

    return size;
}

/*
 * Allocate shmem space for LWLocks and initialize the locks.
 */
void CreateLWLocks(void)
{
    int numLocks = NumLWLocks();
    Size spaceLocks = LWLockShmemSize();
    int *LWLockCounter = NULL;
    char *ptr = NULL;

    StaticAssertExpr(LW_VAL_EXCLUSIVE > (uint32)MAX_BACKENDS, "MAX_BACKENDS too big for lwlock.cpp");
    /* Allocate space */
    ptr = (char *)ShmemAlloc(spaceLocks);

    /* Leave room for dynamic allocation counter */
    ptr += 2 * sizeof(int);

    /* Ensure desired alignment of LWLock array */
    ptr += LWLOCK_PADDED_SIZE - ((uintptr_t)ptr) % LWLOCK_PADDED_SIZE;

    t_thrd.shemem_ptr_cxt.mainLWLockArray = (LWLockPadded *)ptr;
    /*
     * Initialize the dynamic-allocation counter, which is stored just before
     * the first LWLock.
     */
    LWLockCounter = (int *)((char *)t_thrd.shemem_ptr_cxt.mainLWLockArray - 2 * sizeof(int));
    LWLockCounter[0] = (int)NumFixedLWLocks;
    LWLockCounter[1] = numLocks;

    InitializeLWLocks(numLocks);
    RegisterLWLockTranches();
}

/*
 * Initialize LWLocks that are fixed and those belonging to named tranches.
 */
static void InitializeLWLocks(int numLocks)
{
    int id;
    LWLockPadded *lock = t_thrd.shemem_ptr_cxt.mainLWLockArray;

    /* Initialize all individual LWLocks in main array */
    for (id = 0; id < NUM_INDIVIDUAL_LWLOCKS; id++, lock++) {
        LWLockInitialize(&lock->lock, id);
    }

    Assert((lock - t_thrd.shemem_ptr_cxt.mainLWLockArray) == NUM_INDIVIDUAL_LWLOCKS);

    for (id = 0; id < NUM_BUFFER_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_BUFMAPPING);
    }

    for (id = 0; id < NUM_LOCK_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_LOCK_MANAGER);
    }

    for (id = 0; id < NUM_PREDICATELOCK_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_PREDICATE_LOCK_MANAGER);
    }

    for (id = 0; id < NUM_OPERATOR_REALTIME_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_OPERATOR_REAL_TIME);
    }

    for (id = 0; id < NUM_OPERATOR_HISTORY_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_OPERATOR_HISTORY);
    }

    for (id = 0; id < NUM_SESSION_REALTIME_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_SESSION_REAL_TIME);
    }

    for (id = 0; id < NUM_SESSION_HISTORY_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_SESSION_HISTORY);
    }

    for (id = 0; id < NUM_INSTANCE_REALTIME_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_INSTANCE_REAL_TIME);
    }

    for (id = 0; id < NUM_CACHE_BUFFER_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_CACHE_SLOT_MAPPING);
    }

    for (id = 0; id < NUM_CSNLOG_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_CSN_BUFMAPPING);
    }

    for (id = 0; id < NUM_CLOG_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_CLOG_BUFMAPPING);
    }

    for (id = 0; id < NUM_UNIQUE_SQL_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_UNIQUE_SQLMAPPING);
    }

    for (id = 0; id < NUM_INSTR_USER_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_INSTR_USER);
    }

    for (id = 0; id < NUM_GPC_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_GPC_MAPPING);
    }

    for (id = 0; id < NUM_UNIQUE_SQL_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_ASP_MAPPING);
    }

    for (id = 0; id < NUM_GS_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_GlobalSeq);
    }

    for (id = 0; id < NUM_GWC_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_GWC_MAPPING);
    }

    for (id = 0; id < NUM_NORMALIZED_SQL_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_NORMALIZED_SQL);
    }

    for (id = 0; id < NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_MPFL);
    }

    for (id = 0; id < NUM_NGROUP_INFO_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_NGROUP_MAPPING);
    }

    for (id = 0; id < NUM_IO_STAT_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_IO_STAT);
    }

    for (id = 0; id < NUM_PROCXACT_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_PROC_XACT_MAPPING);
    }

    for (id = 0; id < NUM_STARTBLOCK_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_START_BLOCK_MAPPING);
    }

    for (id = 0; id < NUM_SEGMENT_HEAD_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_SEGHEAD_PARTITION);
    }

    for (id = 0; id < NUM_TWOPHASE_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_TWOPHASE_STATE);
    }
    
    for (id = 0; id < NUM_SESSION_ROLEID_PARTITIONS; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_ROLEID_PARTITION);
    }
    Assert((lock - t_thrd.shemem_ptr_cxt.mainLWLockArray) == NumFixedLWLocks);

    for (id = NumFixedLWLocks; id < numLocks; id++, lock++) {
        LWLockInitialize(&lock->lock, LWTRANCHE_UNKNOWN);
    }
}

const char *GetBuiltInTrancheName(int trancheId)
{
    Assert(trancheId >= NUM_INDIVIDUAL_LWLOCKS);
    Assert(trancheId < LWTRANCHE_NATIVE_TRANCHE_NUM);
    int offset = trancheId - NUM_INDIVIDUAL_LWLOCKS;
    Assert(offset < (int)(sizeof(BuiltinTrancheNames) / sizeof(BuiltinTrancheNames[0])));
    return BuiltinTrancheNames[offset];
}

/*
 * Return an identifier for an LWLock based on the wait class and event.
 */
const char *GetLWLockIdentifier(uint32 classId, uint16 eventId)
{
    Assert(classId == PG_WAIT_LWLOCK);

    if (eventId >= LWLockTranchesAllocated || LWLockTrancheArray[eventId] == NULL) {
        return "extension";
    }

    return LWLockTrancheArray[eventId];
}

void DumpLWLockInfo()
{
    static const int NUM_COUNTERS_LWLOCKARRAY = 2;
    int* LWLockCounter = (int*) ((char*)t_thrd.shemem_ptr_cxt.mainLWLockArray - NUM_COUNTERS_LWLOCKARRAY * sizeof(int));
    SpinLockAcquire(t_thrd.shemem_ptr_cxt.ShmemLock);
    ereport(INFO,
            (errmsg("dumpLWLockInfo LWLockCounter: %d, %d.", LWLockCounter[0], LWLockCounter[1])));
    for (int i = 0; i < LWLockCounter[0]; i++) {
        LWLock* lock = &t_thrd.shemem_ptr_cxt.mainLWLockArray[i].lock;
        ereport(INFO,
                (errmsg("dumpLWLockInfo LWLock: %d, %d, %s.", i, lock->tranche, LWLockTrancheArray[lock->tranche])));
    }
    SpinLockRelease(t_thrd.shemem_ptr_cxt.ShmemLock);

    for (int i = 0; i < LWLockTranchesAllocated; i++) {
        ereport(INFO, (errmsg("dumpLWLockInfo LWLockTranche: %d, %s.", i, LWLockTrancheArray[i])));
    }
}

/*
 * Register a tranche ID in the lookup table for the current process.  This
 * routine will save a pointer to the tranche name passed as an argument,
 * so the name should be allocated in a backend-lifetime context
 * (t_thrd.top_mem_cxt, static variable, or similar).
 */
void LWLockRegisterTranche(int tranche_id, const char *tranche_name)
{
    Assert(LWLockTrancheArray != NULL);

    if (tranche_id >= LWLockTranchesAllocated) {
        int i = LWLockTranchesAllocated;
        int j = LWLockTranchesAllocated;

        while (i <= tranche_id) {
            i *= 2;
        }

        LWLockTrancheArray = (const char **)repalloc(LWLockTrancheArray, i * sizeof(char *));
        LWLockTranchesAllocated = i;
        while (j < LWLockTranchesAllocated) {
            LWLockTrancheArray[j++] = NULL;
        }
    }

    LWLockTrancheArray[tranche_id] = tranche_name;
}

/*
 * Register tranches for fixed LWLocks.
 */
static void RegisterLWLockTranches(void)
{
    uint64 i;
    int trancheId;
    Size builtInTrancheNum;

    if (LWLockTrancheArray == NULL) {
        LWLockTranchesAllocated = LWLOCK_TRANCHE_SIZE;
        LWLockTrancheArray = (const char **)MemoryContextAllocZero(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), LWLockTranchesAllocated * sizeof(char *));
    }

    /*
     * Make sure the size of MainLWLockNames equals to NUM_INDIVIDUAL_LWLOCKS.
     * They are added in lwlocknames.txt, which will be used to generate lwlocknames.h/cpp automatically.
     */
    for (i = 0, trancheId = 0; i < NUM_INDIVIDUAL_LWLOCKS; i++, trancheId++) {
        LWLockRegisterTranche(trancheId, MainLWLockNames[i]);
    }

    builtInTrancheNum = sizeof(BuiltinTrancheNames) / sizeof(BuiltinTrancheNames[0]);
    Assert((Size)(LWTRANCHE_NATIVE_TRANCHE_NUM - NUM_INDIVIDUAL_LWLOCKS) == builtInTrancheNum);
    for (i = 0; i < builtInTrancheNum; i++, trancheId++) {
        LWLockRegisterTranche(trancheId, BuiltinTrancheNames[i]);
    }
}

/*
 * LWLockAssign - assign a dynamically-allocated LWLock number
 *
 * We interlock this using the same spinlock that is used to protect
 * ShmemAlloc().  Interlocking is not really necessary during postmaster
 * startup, but it is needed if any user-defined code tries to allocate
 * LWLocks after startup.
 */
LWLock *LWLockAssign(int trancheId)
{
    LWLock *result = NULL;

    /* use volatile pointer to prevent code rearrangement */
    volatile int *LWLockCounter = NULL;

    LWLockCounter = (int *)((char *)t_thrd.shemem_ptr_cxt.mainLWLockArray - 2 * sizeof(int));
    SpinLockAcquire(t_thrd.shemem_ptr_cxt.ShmemLock);
    if (LWLockCounter[0] >= LWLockCounter[1]) {
        SpinLockRelease(t_thrd.shemem_ptr_cxt.ShmemLock);
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("no more LWLocks available")));
    }
    result = &t_thrd.shemem_ptr_cxt.mainLWLockArray[LWLockCounter[0]++].lock;
    SpinLockRelease(t_thrd.shemem_ptr_cxt.ShmemLock);
    result->tranche = trancheId;
    return result;
}

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
void LWLockInitialize(LWLock *lock, int tranche_id)
{
    pg_atomic_init_u32(&lock->state, LW_FLAG_RELEASE_OK);

    /* ENABLE_THREAD_CHECK only, Register RWLock in Tsan */
    TsAnnotateRWLockCreate(&lock->rwlock);
    TsAnnotateRWLockCreate(&lock->listlock);

#ifdef LOCK_DEBUG
    pg_atomic_init_u32(&lock->nwaiters, 0);
#endif
    lock->tranche = tranche_id;
    dlist_init(&lock->waiters);
}

static void LWThreadSuicide(PGPROC *proc, int extraWaits, LWLock *lock, LWLockMode mode)
{
    if (!proc->lwIsVictim) {
        return;
    }

    Assert(false); /* for debug */

    /* allow LWLockRelease to release waiters again. */
    pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

    /* Fix the process wait semaphore's count for any absorbed wakeups. */
    while (extraWaits-- > 0) {
        PGSemaphoreUnlock(&proc->sem);
    }
    instr_stmt_report_lock(LWLOCK_WAIT_END);
    LWLockReportWaitFailed(lock);
    ereport(FATAL, (errmsg("force thread %lu to exit because of lwlock deadlock", proc->pid),
                    errdetail("Lock Info: (%s), mode %d", T_NAME(lock), mode)));
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode. This function will not block waiting for a lock to become free - that's the
 * callers job.
 * Returns true if the lock isn't free and we need to wait.
 */
static bool LWLockAttemptLock(LWLock *lock, LWLockMode mode)
{
    uint32 old_state;

    AssertArg(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    /*
     * Read once outside the loop, later iterations will get the newer value
     * via compare & exchange.
     */
    old_state = pg_atomic_read_u32(&lock->state);

    /* loop until we've determined whether we could acquire the lock or not */
    while (true) {
        uint32 desired_state;
        bool lock_free = false;

        desired_state = old_state;

        if (mode == LW_EXCLUSIVE) {
            lock_free = ((old_state & LW_LOCK_MASK) == 0);
            if (lock_free) {
                desired_state += LW_VAL_EXCLUSIVE;
            }
        } else {
            lock_free = ((old_state & LW_VAL_EXCLUSIVE) == 0);
            if (lock_free) {
                desired_state += LW_VAL_SHARED;
            }
        }

        /*
         * Attempt to swap in the state we are expecting. If we didn't see
         * lock to be free, that's just the old value. If we saw it as free,
         * we'll attempt to mark it acquired. The reason that we always swap
         * in the value is that this doubles as a memory barrier. We could try
         * to be smarter and only swap in values if we saw the lock as free,
         * but benchmark haven't shown it as beneficial so far.
         *
         * Retry if the value changed since we last looked at it.
         */
        if (pg_atomic_compare_exchange_u32(&lock->state, &old_state, desired_state)) {
            if (lock_free) {
                /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
                 * thread after got the lock */
                if (desired_state & LW_VAL_EXCLUSIVE) {
                    TsAnnotateRWLockAcquired(&lock->rwlock, 1);
                } else {
                    TsAnnotateRWLockAcquired(&lock->rwlock, 0);
                }

                /* Great! Got the lock. */
#ifdef LOCK_DEBUG
                if (mode == LW_EXCLUSIVE) {
                    lock->owner = t_thrd.proc;
                }
#endif
                return false;
            } else {
                return true; /* someobdy else has the lock */
            }
        }
    }
}

/*
 * Lock the LWLock's wait list against concurrent activity.
 *
 * NB: even though the wait list is locked, non-conflicting lock operations
 * may still happen concurrently.
 *
 * Time spent holding mutex should be short!
 */
static void LWLockWaitListLock(LWLock *lock)
{
    uint32 old_state;
#ifdef LWLOCK_STATS
    lwlock_stats *lwstats = NULL;
    uint32 delays = 0;

    lwstats = get_lwlock_stats_entry(lock);
#endif

    while (true) {
        /* always try once to acquire lock directly */
        old_state = pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_LOCKED);
        if (!(old_state & LW_FLAG_LOCKED)) {
            break; /* got lock */
        }

        /* and then spin without atomic operations until lock is released */
        {
#ifndef ENABLE_THREAD_CHECK
            SpinDelayStatus delayStatus = init_spin_delay((void *)&lock->state);
#endif

            while (old_state & LW_FLAG_LOCKED) {
#ifndef ENABLE_THREAD_CHECK
                perform_spin_delay(&delayStatus);
#endif
                old_state = pg_atomic_read_u32(&lock->state);
            }
#ifdef LWLOCK_STATS
            delays += delayStatus.delays;
#endif
#ifndef ENABLE_THREAD_CHECK
            finish_spin_delay(&delayStatus);
#endif
        }

        /*
         * Retry. The lock might obviously already be re-acquired by the time
         * we're attempting to get it again.
         */
    }

#ifdef LWLOCK_STATS
    delays += delayStatus.delays;
#endif

    /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
     * thread after got the lock */
    TsAnnotateRWLockAcquired(&lock->listlock, 1);
}

/*
 * Unlock the LWLock's wait list.
 *
 * Note that it can be more efficient to manipulate flags and release the
 * locks in a single atomic operation.
 */
static void LWLockWaitListUnlock(LWLock *lock)
{
    uint32 old_state PG_USED_FOR_ASSERTS_ONLY;

    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TsAnnotateRWLockReleased(&lock->listlock, 1);

    old_state = pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_LOCKED);
    Assert(old_state & LW_FLAG_LOCKED);
}

/*
 * Wakeup all the lockers that currently have a chance to acquire the lock.
 */
static void LWLockWakeup(LWLock *lock)
{
    bool new_release_ok = true;
    bool wokeup_somebody = false;
    dlist_head wakeup;
    dlist_mutable_iter iter;

    dlist_init(&wakeup);

    /* lock wait list while collecting backends to wake up */
    LWLockWaitListLock(lock);

    dlist_foreach_modify(iter, &lock->waiters)
    {
        PGPROC *waiter = dlist_container(PGPROC, lwWaitLink, iter.cur);
        if (wokeup_somebody && waiter->lwWaitMode == LW_EXCLUSIVE) {
            continue;
        }

        dlist_delete(&waiter->lwWaitLink);
        dlist_push_tail(&wakeup, &waiter->lwWaitLink);

        if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE) {
            /* Prevent additional wakeups until retryer gets to run. Backends
             * that are just waiting for the lock to become free don't retry
             * automatically. */
            new_release_ok = false;
            /* Don't wakeup (further) exclusive locks. */
            wokeup_somebody = true;
        }

        /*
         * Once we've woken up an exclusive lock, there's no point in waking
         * up anybody else.
         */
        if (waiter->lwWaitMode == LW_EXCLUSIVE) {
            break;
        }
    }

    Assert(dlist_is_empty(&wakeup) || (pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS));

    /* unset required flags, and release lock, in one fell swoop */
    {
        uint32 old_state;
        uint32 desired_state;

        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->listlock, 1);

        old_state = pg_atomic_read_u32(&lock->state);
        while (true) {
            desired_state = old_state;

            /* compute desired flags */
            if (new_release_ok) {
                desired_state |= LW_FLAG_RELEASE_OK;
            } else {
                desired_state &= ~LW_FLAG_RELEASE_OK;
            }

            if (dlist_is_empty(&wakeup)) {
                desired_state &= ~LW_FLAG_HAS_WAITERS;
            }

            desired_state &= ~LW_FLAG_LOCKED;  // release lock
            if (pg_atomic_compare_exchange_u32(&lock->state, &old_state, desired_state)) {
                break;
            }
        }
    }

    /* Awaken any waiters I removed from the queue. */
    dlist_foreach_modify(iter, &wakeup)
    {
        PGPROC *waiter = dlist_container(PGPROC, lwWaitLink, iter.cur);

        LOG_LWDEBUG("LWLockRelease", lock, "release waiter");
        dlist_delete(&waiter->lwWaitLink);
        /*
         * Guarantee that lwWaiting being unset only becomes visible once the
         * unlink from the link has completed. Otherwise the target backend
         * could be woken up for other reason and enqueue for a new lock - if
         * that happens before the list unlink happens, the list would end up
         * being corrupted.
         *
         * The barrier pairs with the LWLockWaitListLock() when enqueing for
         * another lock.
         */
        pg_write_barrier();

        /* ENABLE_THREAD_CHECK only, waiter->lwWaiting should not be reported race  */
        TsAnnotateBenignRaceSized(&waiter->lwWaiting, sizeof(waiter->lwWaiting));

        waiter->lwWaiting = false;
        PGSemaphoreUnlock(&waiter->sem);
    }
}

/*
 * Add ourselves to the end of the queue.
 *
 * NB: Mode can be LW_WAIT_UNTIL_FREE here!
 */
static void LWLockQueueSelf(LWLock *lock, LWLockMode mode)
{
    /*
     * If we don't have a PGPROC structure, there's no way to wait. This
     * should never occur, since t_thrd.proc should only be null during shared
     * memory initialization.
     */
    if (t_thrd.proc == NULL) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("cannot wait without a PGPROC structure")));
    }

    if (t_thrd.proc->lwWaiting) {
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("queueing for lock while waiting on another one")));
    }

    LWLockWaitListLock(lock);

    /* setting the flag is protected by the spinlock */
    pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_HAS_WAITERS);

    t_thrd.proc->lwWaiting = true;
    t_thrd.proc->lwWaitMode = mode;

    /* LW_WAIT_UNTIL_FREE waiters are always at the front of the queue */
    if (mode == LW_WAIT_UNTIL_FREE) {
        dlist_push_head(&lock->waiters, &t_thrd.proc->lwWaitLink);
    } else {
        dlist_push_tail(&lock->waiters, &t_thrd.proc->lwWaitLink);
    }

    /* Can release the mutex now */
    LWLockWaitListUnlock(lock);

#ifdef LOCK_DEBUG
    pg_atomic_fetch_add_u32(&lock->nwaiters, 1);
#endif
}

/*
 * Remove ourselves from the waitlist.
 *
 * This is used if we queued ourselves because we thought we needed to sleep
 * but, after further checking, we discovered that we don't actually need to
 * do so. Returns false if somebody else already has woken us up, otherwise
 * returns true.
 */
static void LWLockDequeueSelf(LWLock *lock, LWLockMode mode)
{
    bool found = false;
    dlist_mutable_iter iter;

#ifdef LWLOCK_STATS
    lwlock_stats *lwstats = NULL;

    lwstats = get_lwlock_stats_entry(lock);

    lwstats->dequeue_self_count++;
#endif

    LWLockWaitListLock(lock);

    /*
     * Can't just remove ourselves from the list, but we need to iterate over
     * all entries as somebody else could have unqueued us.
     */
    dlist_foreach_modify(iter, &lock->waiters)
    {
        PGPROC *proc = dlist_container(PGPROC, lwWaitLink, iter.cur);
        if (proc == t_thrd.proc) {
            found = true;
            dlist_delete(&proc->lwWaitLink);
            break;
        }
    }

    if (dlist_is_empty(&lock->waiters) && (pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS) != 0) {
        pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_HAS_WAITERS);
    }

    /* XXX: combine with fetch_and above? */
    LWLockWaitListUnlock(lock);

    /* clear waiting state again, nice for debugging */
    if (found) {
        t_thrd.proc->lwWaiting = false;
    } else {
        int extraWaits = 0;

        /*
         * Somebody else dequeued us and has or will wake us up. Deal with the
         * superflous absorption of a wakeup.
         *
         * Reset releaseOk if somebody woke us before we removed ourselves -
         * they'll have set it to false. */
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

        /*
         * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
         * get reset at some inconvenient point later. Most of the time this
         * will immediately return. */
        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            PGSemaphoreLock(&t_thrd.proc->sem, false);
            if (!t_thrd.proc->lwWaiting) {
                LWThreadSuicide(t_thrd.proc, extraWaits, lock, mode);
                break;
            }
            extraWaits++;
        }

        /*
         * Fix the process wait semaphore's count for any absorbed wakeups.
         */
        while (extraWaits-- > 0) {
            PGSemaphoreUnlock(&t_thrd.proc->sem);
        }
    }

#ifdef LOCK_DEBUG
    {
        /* not waiting anymore */
        uint32 nwaiters = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);
        Assert(nwaiters < MAX_BACKENDS);
    }
#endif
}

/*
 * Does the lwlock in its current state need to wait for the variable value to
 * change?
 *
 * If we don't need to wait, and it's because the value of the variable has
 * changed, store the current value in newval.
 *
 * *result is set to true if the lock was free, and false otherwise.
 */
static bool LWLockConflictsWithVar(LWLock *lock, uint64 *valptr, uint64 oldval, uint64 *newval, bool *result)
{
    bool mustwait = false;
    uint64 value;

    /*
     * Test first to see if it the slot is free right now.
     *
     * XXX: the caller uses a spinlock before this, so we don't need a memory
     * barrier here as far as the current usage is concerned.  But that might
     * not be safe in general.
     */
    mustwait = (pg_atomic_read_u32(&lock->state) & LW_VAL_EXCLUSIVE) != 0;
    if (!mustwait) {
        *result = true;
        return false;
    }

    *result = false;

    /*
     * Read value using the lwlock's wait list lock, as we can't generally
     * rely on atomic 64 bit reads/stores. On platforms with a way to
     * do atomic 64 bit reads/writes the spinlock should be optimized away.
     */
    LWLockWaitListLock(lock);
    value = *valptr;
    LWLockWaitListUnlock(lock);

    if (value != oldval) {
        mustwait = false;
        *newval = value;
    } else {
        mustwait = true;
    }

    return mustwait;
}

const float NEED_UPDATE_LOCKID_QUEUE_SLOT = 0.6;

/*
 * LWLockAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, sleep until it is.
 *
 * Side effect: cancel/die interrupts are held off until lock release.
 */
bool LWLockAcquire(LWLock *lock, LWLockMode mode, bool need_update_lockid)
{
    PGPROC *proc = t_thrd.proc;
    bool result = true;
    int extraWaits = 0;
#ifdef LWLOCK_STATS
    lwlock_stats *lwstats = NULL;

    lwstats = get_lwlock_stats_entry(lock);
#endif

    AssertArg(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquire", lock, mode);

#ifdef LWLOCK_STATS
    /* Count lock acquisition attempts */
    if (mode == LW_EXCLUSIVE) {
        lwstats->ex_acquire_count++;
    } else {
        lwstats->sh_acquire_count++;
    }
#endif /* LWLOCK_STATS */

    /*
     * We can't wait if we haven't got a PGPROC.  This should only occur
     * during bootstrap or shared memory initialization.  Put an Assert here
     * to catch unsafe coding practices.
     */
    Assert(!(proc == NULL && IsUnderPostmaster));

    /* Ensure we will have room to remember the lock */
    if (t_thrd.storage_cxt.num_held_lwlocks >= MAX_SIMUL_LWLOCKS) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("too many LWLocks taken")));
    }

    remember_lwlock_acquire(lock);

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /*
     * Loop here to try to acquire lock after each time we are signaled by
     * LWLockRelease.
     *
     * NOTE: it might seem better to have LWLockRelease actually grant us the
     * lock, rather than retrying and possibly having to go back to sleep. But
     * in practice that is no good because it means a process swap for every
     * lock acquisition when two or more processes are contending for the same
     * lock.  Since LWLocks are normally used to protect not-very-long
     * sections of computation, a process needs to be able to acquire and
     * release the same lock many times during a single CPU time slice, even
     * in the presence of contention.  The efficiency of being able to do that
     * outweighs the inefficiency of sometimes wasting a process dispatch
     * cycle because the lock is not free when a released waiter finally gets
     * to run.	See pgsql-hackers archives for 29-Dec-01.
     */
    for (;;) {
        bool mustwait = false;

        /*
         * Try to grab the lock the first time, we're not in the waitqueue
         * yet/anymore.
         */
        mustwait = LWLockAttemptLock(lock, mode);
        if (!mustwait) {
            /* XXX: remove before commit? */
            LOG_LWDEBUG("LWLockAcquire", lock, "immediately acquired lock");
            break; /* got the lock */
        }

        instr_stmt_report_lock(LWLOCK_WAIT_START, mode, NULL, lock->tranche);
        pgstat_report_waitevent(PG_WAIT_LWLOCK | lock->tranche);
        /*
         * Ok, at this point we couldn't grab the lock on the first try. We
         * cannot simply queue ourselves to the end of the list and wait to be
         * woken up because by now the lock could long have been released.
         * Instead add us to the queue and try to grab the lock again. If we
         * succeed we need to revert the queuing and be happy, otherwise we
         * recheck the lock. If we still couldn't grab it, we know that the
         * other lock will see our queue entries when releasing since they
         * existed before we checked for the lock.
         */
        /* add to the queue */
        LWLockQueueSelf(lock, mode);

        mustwait = LWLockAttemptLock(lock, mode);
        /* ok, grabbed the lock the second time round, need to undo queueing */
        if (!mustwait) {
            LOG_LWDEBUG("LWLockAcquire", lock, "acquired, undoing queue");

            LWLockDequeueSelf(lock, mode);
            pgstat_report_waitevent(WAIT_EVENT_END);
            instr_stmt_report_lock(LWLOCK_WAIT_END);
            break;
        }

        if (need_update_lockid &&
            get_dirty_page_num() >= g_instance.ckpt_cxt_ctl->dirty_page_queue_size * NEED_UPDATE_LOCKID_QUEUE_SLOT) {
            update_wait_lockid(lock);
        }
        /*
         * Wait until awakened.
         *
         * Since we share the process wait semaphore with the regular lock
         * manager and ProcWaitForSignal, and we may need to acquire an LWLock
         * while one of those is pending, it is possible that we get awakened
         * for a reason other than being signaled by LWLockRelease. If so,
         * loop back and wait again.  Once we've gotten the LWLock,
         * re-increment the sema by the number of additional signals received,
         * so that the lock manager or signal manager will see the received
         * signal when it next waits.
         */
        LOG_LWDEBUG("LWLockAcquire", lock, "waiting");

#ifdef LWLOCK_STATS
        lwstats->block_count++;
#endif
        TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);
        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            PGSemaphoreLock(&proc->sem, false);
            if (!proc->lwWaiting) {
                if (!proc->lwIsVictim) {
                    break;
                }
                /* allow LWLockRelease to release waiters again. */
                pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);
                LWThreadSuicide(proc, extraWaits, lock, mode);
            }
            extraWaits++;
        }

        /* Retrying, allow LWLockRelease to release waiters again. */
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

#ifdef LOCK_DEBUG
        {
            /* not waiting anymore */
            uint32 nwaiters = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);
            Assert(nwaiters < MAX_BACKENDS);
        }
#endif
        TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
        pgstat_report_waitevent(WAIT_EVENT_END);
        instr_stmt_report_lock(LWLOCK_WAIT_END);

        LOG_LWDEBUG("LWLockAcquire", lock, "awakened");

        /* Now loop back and try to acquire lock again. */
        result = false;
    }

    TRACE_POSTGRESQL_LWLOCK_ACQUIRE(T_NAME(lock), mode);

    forget_lwlock_acquire();

    /* Add lock to list of locks held by this backend */
    t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks].lock = lock;
    t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks++].mode = mode;

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while (extraWaits-- > 0) {
        PGSemaphoreUnlock(&proc->sem);
    }

    return result;
}

/*
 * LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, return FALSE with no side-effects.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
    bool mustwait = false;

    AssertArg(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockConditionalAcquire", lock, mode);

    /* Ensure we will have room to remember the lock */
    if (t_thrd.storage_cxt.num_held_lwlocks >= MAX_SIMUL_LWLOCKS) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("too many LWLocks taken")));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /* Check for the lock */
    mustwait = LWLockAttemptLock(lock, mode);
    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();
        LOG_LWDEBUG("LWLockConditionalAcquire", lock, "failed");
        TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(T_NAME(lock), mode);
    } else {
        /* Add lock to list of locks held by this backend */
        t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks].lock = lock;
        t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks++].mode = mode;
        TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(T_NAME(lock), mode);
    }
    return !mustwait;
}

/*
 * LWLockAcquireOrWait - Acquire lock, or wait until it's free
 *
 * The semantics of this function are a bit funky.	If the lock is currently
 * free, it is acquired in the given mode, and the function returns true.  If
 * the lock isn't immediately free, the function waits until it is released
 * and returns false, but does not acquire the lock.
 *
 * This is currently used for WALWriteLock: when a backend flushes the WAL,
 * holding WALWriteLock, it can flush the commit records of many other
 * backends as a side-effect.  Those other backends need to wait until the
 * flush finishes, but don't need to acquire the lock anymore.  They can just
 * wake up, observe that their records have already been flushed, and return.
 */
bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode)
{
    PGPROC *proc = t_thrd.proc;
    bool mustwait = false;
    int extraWaits = 0;
#ifdef LWLOCK_STATS
    lwlock_stats *lwstats = NULL;
    lwstats = get_lwlock_stats_entry(lock);
#endif

    Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquireOrWait", lock, mode);

    /* Ensure we will have room to remember the lock */
    if (t_thrd.storage_cxt.num_held_lwlocks >= MAX_SIMUL_LWLOCKS) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("too many LWLocks taken")));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    mustwait = LWLockAttemptLock(lock, mode);
    if (mustwait) {
        instr_stmt_report_lock(LWLOCK_WAIT_START, mode, NULL, lock->tranche);
        pgstat_report_waitevent(PG_WAIT_LWLOCK | lock->tranche);
        TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);

        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);
        mustwait = LWLockAttemptLock(lock, mode);
        if (mustwait) {
            /*
             * Wait until awakened.  Like in LWLockAcquire, be prepared for bogus
             * wakups, because we share the semaphore with ProcWaitForSignal.
             */
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "waiting");

#ifdef LWLOCK_STATS
            lwstats->block_count++;
#endif
            remember_lwlock_acquire(lock);

            for (;;) {
                /* "false" means cannot accept cancel/die interrupt here. */
                PGSemaphoreLock(&proc->sem, false);
                if (!proc->lwWaiting) {
                    if (!proc->lwIsVictim) {
                        break;
                    }
                    LWThreadSuicide(proc, extraWaits, lock, mode);
                }
                extraWaits++;
            }

            forget_lwlock_acquire();

#ifdef LOCK_DEBUG
            {
                /* not waiting anymore */
                uint32 nwaiters = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);
                Assert(nwaiters < MAX_BACKENDS);
            }
#endif
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "awakened");
        } else {
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "acquired, undoing queue");

            /*
             * Got lock in the second attempt, undo queueing. We need to
             * treat this as having successfully acquired the lock, otherwise
             * we'd not necessarily wake up people we've prevented from
             * acquiring the lock.
             */
            LWLockDequeueSelf(lock, mode);
        }
        TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
        pgstat_report_waitevent(WAIT_EVENT_END);
        instr_stmt_report_lock(LWLOCK_WAIT_END);
    }

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while (extraWaits-- > 0) {
        PGSemaphoreUnlock(&proc->sem);
    }

    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "failed");
        TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE_FAIL(T_NAME(lock), mode);
    } else {
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "succeeded");
        /* Add lock to list of locks held by this backend */
        t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks].lock = lock;
        t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks++].mode = mode;
        TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE(T_NAME(lock), mode);
    }

    return !mustwait;
}

/*
 * LWLockWaitForVar - Wait until lock is free, or a variable is updated.
 * If the lock is held and *valptr equals oldval, waits until the lock is
 * either freed, or the lock holder updates *valptr by calling
 * LWLockUpdateVar.  If the lock is free on exit (immediately or after
 * waiting), returns true.  If the lock is still held, but *valptr no longer
 * matches oldval, returns false and sets *newval to the current value in *valptr.
 *
 * It's possible that the lock holder releases the lock, but another backend
 * acquires it again before we get a chance to observe that the lock was
 * momentarily released.  We wouldn't need to wait for the new lock holder,
 * but we cannot distinguish that case, so we will have to wait.
 *
 * Note: this function ignores shared lock holders; if the lock is held
 * in shared mode, returns 'true'.
 */
bool LWLockWaitForVar(LWLock *lock, uint64 *valptr, uint64 oldval, uint64 *newval)
{
    PGPROC *proc = t_thrd.proc;
    int extraWaits = 0;
    bool result = false;
#ifdef LWLOCK_STATS
    lwlock_stats *lwstats = NULL;
#endif

#ifdef LWLOCK_STATS
    lwstats = get_lwlock_stats_entry(lock);
#endif /* LWLOCK_STATS */

    PRINT_LWDEBUG("LWLockWaitForVar", lock, LW_WAIT_UNTIL_FREE);

    /*
     * Lock out cancel/die interrupts while we sleep on the lock.  There is no
     * cleanup mechanism to remove us from the wait queue if we got
     * interrupted.
     */
    HOLD_INTERRUPTS();

    /*
     * Loop here to check the lock's status after each time we are signaled.
     */
    for (;;) {
        bool mustwait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &result);
        if (!mustwait) {
            break; /* the lock was free or value didn't match */
        }

        /*
         * Add myself to wait queue. Note that this is racy, somebody else
         * could wakeup before we're finished queuing. NB: We're using nearly
         * the same twice-in-a-row lock acquisition protocol as
         * LWLockAcquire(). Check its comments for details. The only
         * difference is that we also have to check the variable's values when
         * checking the state of the lock.
         */
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        /*
         * Set RELEASE_OK flag, to make sure we get woken up as soon as the
         * lock is released.
         */
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

        /*
         * We're now guaranteed to be woken up if necessary. Recheck the lock
         * and variables state.
         */
        mustwait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &result);
        /* Ok, no conflict after we queued ourselves. Undo queueing. */
        if (!mustwait) {
            LOG_LWDEBUG("LWLockWaitForVar", lock, "free, undoing queue");

            LWLockDequeueSelf(lock, LW_WAIT_UNTIL_FREE);
            break;
        }

        /*
         * Wait until awakened.
         *
         * Since we share the process wait semaphore with the regular lock
         * manager and ProcWaitForSignal, and we may need to acquire an LWLock
         * while one of those is pending, it is possible that we get awakened
         * for a reason other than being signaled by LWLockRelease. If so,
         * loop back and wait again.  Once we've gotten the LWLock,
         * re-increment the sema by the number of additional signals received,
         * so that the lock manager or signal manager will see the received
         * signal when it next waits.
         */
        LOG_LWDEBUG("LWLockWaitForVar", lock, "waiting");

#ifdef LWLOCK_STATS
        lwstats->block_count++;
#endif

        TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), LW_EXCLUSIVE);

        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            PGSemaphoreLock(&proc->sem, false);
            if (!proc->lwWaiting) {
                LWThreadSuicide(proc, extraWaits, lock, LW_WAIT_UNTIL_FREE);
                break;
            }
            extraWaits++;
        }
#ifdef LOCK_DEBUG
        {
            /* not waiting anymore */
            uint32 nwaiters PG_USED_FOR_ASSERTS_ONLY = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);

            Assert(nwaiters < MAX_BACKENDS);
        }
#endif

        TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), LW_EXCLUSIVE);

        LOG_LWDEBUG("LWLockWaitForVar", lock, "awakened");

        /* Now loop back and check the status of the lock again. */
    }

    TRACE_POSTGRESQL_LWLOCK_ACQUIRE(T_NAME(lock), LW_EXCLUSIVE);

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while (extraWaits-- > 0) {
        PGSemaphoreUnlock(&proc->sem);
    }

    /*
     * Now okay to allow cancel/die interrupts.
     */
    RESUME_INTERRUPTS();

    return result;
}

/*
 * LWLockUpdateVar - Update a variable and wake up waiters atomically
 *
 * Sets *valptr to 'val', and wakes up all processes waiting for us with
 * LWLockWaitForVar().  Setting the value and waking up the processes happen
 * atomically so that any process calling LWLockWaitForVar() on the same lock
 * is guaranteed to see the new value, and act accordingly.
 *
 * The caller must be holding the lock in exclusive mode.
 */
void LWLockUpdateVar(LWLock *lock, uint64 *valptr, uint64 val)
{
    dlist_head wakeup;
    dlist_mutable_iter iter;

    PRINT_LWDEBUG("LWLockUpdateVar", lock, LW_EXCLUSIVE);

    dlist_init(&wakeup);

    LWLockWaitListLock(lock);

    Assert(pg_atomic_read_u32(&lock->state) & LW_VAL_EXCLUSIVE);

    /* Update the lock's value */
    *valptr = val;

    /*
     * See if there are any LW_WAIT_UNTIL_FREE waiters that need to be woken
     * up. They are always in the front of the queue. */
    dlist_foreach_modify(iter, &lock->waiters)
    {
        PGPROC *waiter = dlist_container(PGPROC, lwWaitLink, iter.cur);

        if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE) {
            break;
        }

        dlist_delete(&waiter->lwWaitLink);
        dlist_push_tail(&wakeup, &waiter->lwWaitLink);
    }

    /* We are done updating shared state of the lock itself. */
    LWLockWaitListUnlock(lock);

    /* Awaken any waiters I removed from the queue. */
    dlist_foreach_modify(iter, &wakeup) {
        PGPROC* waiter = dlist_container(PGPROC, lwWaitLink, iter.cur);

        dlist_delete(&waiter->lwWaitLink);
        /* check comment in LWLockWakeup() about this barrier */
        pg_write_barrier();
        waiter->lwWaiting = false;
        PGSemaphoreUnlock(&waiter->sem);
    }
}

/*
 * LWLockRelease - release a previously acquired lock
 */
void LWLockRelease(LWLock *lock)
{
    LWLockMode mode = LW_EXCLUSIVE;
    uint32 oldstate;
    bool check_waiters = false;
    int i;

    /* Remove lock from list of locks held.  Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards. */
    for (i = t_thrd.storage_cxt.num_held_lwlocks; --i >= 0;) {
        if (lock == t_thrd.storage_cxt.held_lwlocks[i].lock) {
            mode = t_thrd.storage_cxt.held_lwlocks[i].mode;
            break;
        }
    }
    if (i < 0) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("lock %s is not held", T_NAME(lock))));
    }
    t_thrd.storage_cxt.num_held_lwlocks--;
    for (; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        t_thrd.storage_cxt.held_lwlocks[i] = t_thrd.storage_cxt.held_lwlocks[i + 1];
    }

    PRINT_LWDEBUG("LWLockRelease", lock, mode);

    /*
     * Release my hold on lock, after that it can immediately be acquired by
     * others, even if we still have to wakeup other waiters. */
    if (mode == LW_EXCLUSIVE) {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->rwlock, 1);
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);
    } else {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->rwlock, 0);
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);
    }

    /* nobody else can have that kind of lock */
    Assert(!(oldstate & LW_VAL_EXCLUSIVE));

    /* We're still waiting for backends to get scheduled, don't wake them up again. */
    check_waiters =
        ((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK))
        && ((oldstate & LW_LOCK_MASK) == 0);
    /* As waking up waiters requires the spinlock to be acquired, only do so
     * if necessary. */
    if (check_waiters) {
        /* XXX: remove before commit? */
        LOG_LWDEBUG("LWLockRelease", lock, "releasing waiters");
        LWLockWakeup(lock);
    }

    TRACE_POSTGRESQL_LWLOCK_RELEASE(T_NAME(lock));

    /* Now okay to allow cancel/die interrupts. */
    RESUME_INTERRUPTS();
}

/*
 * LWLockReleaseClearVar - release a previously acquired lock, reset variable
 */
void LWLockReleaseClearVar(LWLock *lock, uint64 *valptr, uint64 val)
{
    LWLockWaitListLock(lock);

    /*
     * Set the variable's value before releasing the lock, that prevents race
     * a race condition wherein a new locker acquires the lock, but hasn't yet
     * set the variables value.
     */
    *valptr = val;
    LWLockWaitListUnlock(lock);

    LWLockRelease(lock);
}

/*
 * LWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail LWLockRelease calls is that t_thrd.int_cxt.InterruptHoldoffCount is
 * unchanged by this operation.  This is necessary since t_thrd.int_cxt.InterruptHoldoffCount
 * has been set to an appropriate level earlier in error recovery. We could
 * decrement it below zero if we allow it to drop for each released lock!
 */
void LWLockReleaseAll(void)
{
    int index = t_thrd.storage_cxt.num_held_lwlocks - 1;
    while (index >= 0) {
        // SwitchoverLockHolder never release switchover lock in LWLockReleaseAll
        if (t_thrd.storage_cxt.isSwitchoverLockHolder && (g_instance.archive_obs_cxt.in_switchover ||
                g_instance.streaming_dr_cxt.isInSwitchover) &&
                (t_thrd.storage_cxt.held_lwlocks[index].lock == HadrSwitchoverLock)) {
            index--;
            continue;
        }
        HOLD_INTERRUPTS(); /* match the upcoming RESUME_INTERRUPTS */

        LWLockRelease(t_thrd.storage_cxt.held_lwlocks[index].lock);
        index--;
    }
}

/*
 * LWLockHeldByMe - test whether my process currently holds a lock
 *
 * This is meant as debug support only.  We currently do not distinguish
 * whether the lock is held shared or exclusive.
 */
bool LWLockHeldByMe(LWLock *lock)
{
    for (int i = 0; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        if (t_thrd.storage_cxt.held_lwlocks[i].lock == lock) {
            return true;
        }
    }
    return false;
}

/*
 * LWLockHeldByMeInMode - test whether my process holds a lock in given mode
 *
 * This is meant as debug support only.
 */
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
    for (int i = 0; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        if (t_thrd.storage_cxt.held_lwlocks[i].lock == lock && t_thrd.storage_cxt.held_lwlocks[i].mode == mode) {
            return true;
        }
    }
    return false;
}

/* reset a lwlock */
void LWLockReset(LWLock *lock)
{
    pg_atomic_init_u32(&lock->state, LW_FLAG_RELEASE_OK);

    /* ENABLE_THREAD_CHECK only */
    TsAnnotateRWLockDestroy(&lock->listlock);
    TsAnnotateRWLockCreate(&lock->listlock);
    TsAnnotateRWLockDestroy(&lock->rwlock);
    TsAnnotateRWLockCreate(&lock->rwlock);

#ifdef LOCK_DEBUG
    pg_atomic_init_u32(&lock->nwaiters, 0);
    lock->owner = NULL;
#endif
    dlist_init(&lock->waiters);
}

/*
 * @Description: obtain ownership of a lock acquired by another thread
 * This function should only be used when a lock is acquired in one thread
 * context and released within another. The lock is unaffected, it remains
 * held.  All that changes is the book keeping, the thread calling this
 * routine becomes responsible for releasing the lock.
 */
void LWLockOwn(LWLock *lock)
{
    uint32 expected_state;

    /* Ensure we will have room to remember the lock */
    if (t_thrd.storage_cxt.num_held_lwlocks >= MAX_SIMUL_LWLOCKS) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("too many LWLocks taken")));
    }

    /* Ensure that lock is held */
    expected_state = pg_atomic_read_u32(&lock->state);
    if (!((expected_state & LW_LOCK_MASK) > 0 || (expected_state & LW_VAL_EXCLUSIVE) > 0)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("lock %s is not held", T_NAME(lock))));
    }

    t_thrd.storage_cxt.held_lwlocks[t_thrd.storage_cxt.num_held_lwlocks++].lock = lock;

    HOLD_INTERRUPTS();
}

/*
 * @Description: LWLockDisown - Disown the lock acquired by this thread that it has no  intention of releasing.
 * This function should only be used when a lock is acquired in one thread
 * context and released within another. Disowning the lock prevents the thread
 * that acquired it from releasing it.
 *
 * This is necessary in cases where the lock must be held beyond the life of
 * the thread that acquired it (when another thread will be releasing it).
 * @Param[IN] lockid: lock id
 * @See also:
 */
void LWLockDisown(LWLock *lock)
{
    uint32 expected_state;
    int i;

    /* Ensure that lock is held */
    expected_state = pg_atomic_read_u32(&lock->state);
    if (!((expected_state & LW_LOCK_MASK) > 0 || (expected_state & LW_VAL_EXCLUSIVE) > 0)) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("lock %s is not held", T_NAME(lock))));
    }

    for (i = t_thrd.storage_cxt.num_held_lwlocks; --i >= 0;) {
        if (lock == t_thrd.storage_cxt.held_lwlocks[i].lock) {
            break;
        }
    }

    if (i < 0) {
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("lock %s is not held", T_NAME(lock))));
    }

    t_thrd.storage_cxt.num_held_lwlocks--;
    for (; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        t_thrd.storage_cxt.held_lwlocks[i] = t_thrd.storage_cxt.held_lwlocks[i + 1];
    }

    RESUME_INTERRUPTS();
}

/* get the address of num_held_lwlocks */
int *get_held_lwlocks_num(void)
{
    return &t_thrd.storage_cxt.num_held_lwlocks;
}

/* get the max number of held lwlocks */
uint32 get_held_lwlocks_maxnum(void)
{
    return (uint32)MAX_SIMUL_LWLOCKS;
}

/* get lwlocks now held */
void *get_held_lwlocks(void)
{
    return (void *)t_thrd.storage_cxt.held_lwlocks;
}

#define COPY_LWLOCK_HANDLE(src, dst) do { \
        (dst)->lock_addr.lock = (src)->lock; \
        (dst)->lock_sx = (src)->mode;        \
        ++(src);                             \
        ++(dst);                             \
    } while (0)

/* copy lwlocks from heldlocks to dst whose size is at least num_heldlocks */
void copy_held_lwlocks(void *heldlocks, lwlock_id_mode *dst, int num_heldlocks)
{
    LWLockHandle *src = (LWLockHandle *)heldlocks;
    const int nloops = (num_heldlocks / 4);
    const int nlefts = (num_heldlocks % 4);

    /* extending loops */
    for (int i = 0; i < nloops; ++i) {
        COPY_LWLOCK_HANDLE(src, dst);
        COPY_LWLOCK_HANDLE(src, dst);
        COPY_LWLOCK_HANDLE(src, dst);
        COPY_LWLOCK_HANDLE(src, dst);
    }
    for (int i = 0; i < nlefts; ++i) {
        COPY_LWLOCK_HANDLE(src, dst);
    }
}

/* wake up the victim thread, and force it to exit */
void wakeup_victim(LWLock *lock, ThreadId victim_tid)
{
    dlist_mutable_iter iter = { NULL, NULL, NULL };
    PGPROC *victim = NULL;

    LWLockWaitListLock(lock);

    dlist_foreach_modify(iter, &lock->waiters)
    {
        PGPROC *waiter = dlist_container(PGPROC, lwWaitLink, iter.cur);
        if (victim_tid == waiter->pid) {
            dlist_delete(&waiter->lwWaitLink);
            victim = waiter;
            break;
        }
    }

    /* update flag LW_FLAG_HAS_WAITERS before waking up victim */
    if (dlist_is_empty(&lock->waiters) && (pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS) != 0) {
        pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_HAS_WAITERS);
    }

    LWLockWaitListUnlock(lock);

    if (victim != NULL) {
        ereport(LOG, (errmsg("victim(lock %s, thread %lu) found, wake up it", T_NAME(lock), victim_tid)));
        /* wake up this victim */
        pg_write_barrier();
        victim->lwWaiting = false;
        victim->lwIsVictim = true;
        PGSemaphoreUnlock(&(victim->sem));
    } else {
        /* print a LOG message */
        ereport(LOG, (errmsg("victim(lock %s, thread %lu) not found, maybe lwlock deadlock disappear", T_NAME(lock),
                             victim_tid)));
    }
}

void print_leak_warning_at_commit()
{
    /* LWLock must be released before commit */
    for (int i = 0; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        ereport(WARNING,
                (errmsg("LWLock %s is held when commit transaction", T_NAME(t_thrd.storage_cxt.held_lwlocks[i].lock))));
    }
}

LWLockMode GetHeldLWLockMode(LWLock *lock)
{
    for (int i = 0; i < t_thrd.storage_cxt.num_held_lwlocks; i++) {
        if (t_thrd.storage_cxt.held_lwlocks[i].lock == lock) {
            return t_thrd.storage_cxt.held_lwlocks[i].mode;
        }
    }

    ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
             errmsg("lock %s is not held", T_NAME(lock))));

    return (LWLockMode)0; /* keep compiler silence */
}

static int FindLWLockPartIndex(const char* name)
{
    int i;
    for (i = 0; i < LWLOCK_PART_KIND; i++) {
        if (pg_strcasecmp(name, LWLockPartInfo[i].name) == 0) {
            return i;
        }
    }
    return -1;
}

static bool CheckAndSetLWLockPartNum(const char* input)
{
    const int pairNum = 2;
    /* Do str copy and remove space. */
    char* strs = TrimStr(input);
    if (strs == NULL || strs[0] == '\0') {
        return false;
    }
    const char* delim = "=";
    List *res = NULL;
    char* nextToken = NULL;

    /* Get name and num */
    char* token = strtok_s(strs, delim, &nextToken);
    while (token != NULL) {
        res = lappend(res, TrimStr(token));
        token = strtok_s(NULL, delim, &nextToken);
    }
    pfree(strs);
    if (res->length != pairNum) {
        list_free_deep(res);
        return false;
    }
    int index = FindLWLockPartIndex((char*)linitial(res));
    if (index != -1) {
        if (!StrToInt32((char*)lsecond(res), &g_instance.attr.attr_storage.num_internal_lock_partitions[index])) {
            ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                errmsg("num_internal_lock_partitions attr has invalid lwlock num:%s.", (char*)lsecond(res))));
        }
        list_free_deep(res);
        return true;
    } else {
        ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
            errmsg("num_internal_lock_partitions attr has invalid lwlock name: %s.", (char*)linitial(res))));
        return false; /* keep compiler silence */
    }
}

void SetLWLockPartDefaultNum(void)
{
    int i;
    for (i = 0; i < LWLOCK_PART_KIND; i++) {
        g_instance.attr.attr_storage.num_internal_lock_partitions[i] = LWLockPartInfo[i].defaultNumPartition;
    }
}

void CheckAndSetLWLockPartInfo(const List* res)
{
    ListCell* cell = NULL;
    foreach (cell, res) {
        char* input = (char*)lfirst(cell);
        if (!CheckAndSetLWLockPartNum(input)) {
            ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                errmsg("num_internal_lock_partitions attr has invalid input syntax.")));
        }
    }
}

void CheckLWLockPartNumRange(void)
{
    int i;
    for (i = 0; i < LWLOCK_PART_KIND; i++) {
        if (g_instance.attr.attr_storage.num_internal_lock_partitions[i] < LWLockPartInfo[i].minNumPartition ||
            g_instance.attr.attr_storage.num_internal_lock_partitions[i] > LWLockPartInfo[i].maxNumPartition) {
            ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                errmsg("Invalid attribute for internal lock partitions."),
                errdetail("Current %s lock partition num %d is out of range [%d, %d].",
                    LWLockPartInfo[i].name, g_instance.attr.attr_storage.num_internal_lock_partitions[i],
                    LWLockPartInfo[i].minNumPartition, LWLockPartInfo[i].maxNumPartition)));
        }
    }
}

