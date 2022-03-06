/* -------------------------------------------------------------------------
 *
 * proc.h
 *	  per-process shared memory data structures
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/proc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _PROC_H_
#define _PROC_H_

#include "access/clog.h"
#include "access/xlog.h"
#include "datatype/timestamp.h"
#include "storage/latch.h"
#include "storage/lock/lock.h"
#include "storage/lock/pg_sema.h"
#include "threadpool/threadpool.h"
#include "replication/dataqueuedefs.h"
#include "utils/syscall_lock.h"
#include "pgtime.h"
#include "gtm/gtm_c.h"
#include "alarm/alarm.h"
#include "utils/atomic.h"
#include "access/multi_redo_settings.h"


/*
 * Each backend advertises up to PGPROC_MAX_CACHED_SUBXIDS TransactionIds
 * for non-aborted subtransactions of its current top transaction.	These
 * have to be treated as running XIDs by other backends.
 *
 * We also keep track of whether the cache overflowed (ie, the transaction has
 * generated at least one subtransaction that didn't fit in the cache).
 * If none of the caches have overflowed, we can assume that an XID that's not
 * listed anywhere in the PGPROC array is not a running transaction.  Else we
 * have to look at pg_subtrans.
 */
#define PGPROC_MAX_CACHED_SUBXIDS 64 /* XXX guessed-at value */

#define PGPROC_INIT_CACHED_SUBXIDS 64

struct XidCache {
    int maxNumber;       /* max number of sub xids */
    TransactionId* xids; /* pointer to sub xids, memory allocate in g_instance.instance_context */
};

/* Flags for PGXACT->vacuumFlags */
#define PROC_IS_AUTOVACUUM 0x01         /* is it an autovac worker? */
#define PROC_IN_VACUUM 0x02             /* currently running lazy vacuum */
#define PROC_IN_ANALYZE 0x04            /* currently running analyze */
/* flags reset at EOXact */
#define PROC_VACUUM_STATE_MASK (PROC_IN_VACUUM | PROC_IN_ANALYZE)
/*
 * Flags reused to mark data redistribution xact at online expansion time
 * we do not want to introduce a new field in PGXACT for data
 * redistribution which increases the sizeof(PGXACT) and possiblely make
 * it not fit into CPU cacheline. Please see the comments below for PGXACT
 */
#define PROC_IS_REDIST 0x10           /* currently running data redistribution */
#define PROC_IN_LOGICAL_DECODING 0x20 /* currently doing logical decoding */

#define XACT_NOT_IN_USE 0
#define XACT_IN_USE 1

/*
 * We allow a small number of "weak" relation locks (AccesShareLock,
 * RowShareLock, RowExclusiveLock) to be recorded in the PGPROC structure
 * rather than the main lock table.  This eases contention on the lock
 * manager LWLocks.  See storage/lmgr/README for additional details.
 */
#define FP_LOCK_SLOTS_PER_BACKEND ((uint32)g_instance.attr.attr_storage.num_internal_lock_partitions[FASTPATH_PART])
#define FP_LOCK_SLOTS_PER_LOCKBIT 20
#define FP_LOCKBIT_NUM (((FP_LOCK_SLOTS_PER_BACKEND - 1) / FP_LOCK_SLOTS_PER_LOCKBIT) + 1)
#define FAST_PATH_SET_LOCKBITS_ZERO(proc)                       \
    do {                                                        \
        for (uint32 _idx = 0; _idx < FP_LOCKBIT_NUM; _idx++) {  \
            (proc)->fpLockBits[_idx] = 0;                       \
        }                                                       \
    } while (0)



typedef struct FastPathTag {
    uint32 dbid;
    uint32 relid;
    uint32 partitionid;
} FastPathTag;

#define FAST_PATH_TAG_EQUALS(tag1, tag2) \
    (((tag1).dbid == (tag2).dbid) && ((tag1).relid == (tag2).relid) && ((tag1).partitionid == (tag2).partitionid))

/*
 * An invalid pgprocno.  Must be larger than the maximum number of PGPROC
 * structures we could possibly have.  See comments for MAX_BACKENDS.
 */
#define INVALID_PGPROCNO PG_INT32_MAX

/*
 * Each backend has a PGPROC struct in shared memory.  There is also a list of
 * currently-unused PGPROC structs that will be reallocated to new backends.
 *
 * links: list link for any list the PGPROC is in.	When waiting for a lock,
 * the PGPROC is linked into that lock's waitProcs queue.  A recycled PGPROC
 * is linked into ProcGlobal's freeProcs list.
 *
 * Note: twophase.c also sets up a dummy PGPROC struct for each currently
 * prepared transaction.  These PGPROCs appear in the ProcArray data structure
 * so that the prepared transactions appear to be still running and are
 * correctly shown as holding locks.  A prepared transaction PGPROC can be
 * distinguished from a real one at need by the fact that it has pid == 0.
 * The semaphore and lock-activity fields in a prepared-xact PGPROC are unused,
 * but its myProcLocks[] lists are valid.
 */
struct PGPROC {
    /* proc->links MUST BE FIRST IN STRUCT (see ProcSleep,ProcWakeup,etc) */
    SHM_QUEUE links; /* list link if process is in a list */

    PGSemaphoreData sem; /* ONE semaphore to sleep on */
    int waitStatus;      /* STATUS_WAITING, STATUS_OK or STATUS_ERROR */
                         /* STATUS_BLOCKING for update block session */

    Latch procLatch; /* generic latch for process */

    LocalTransactionId lxid; /* local id of top-level transaction currently
                              * being executed by this proc, if running;
                              * else InvalidLocalTransactionId */
    ThreadId pid;            /* Backend's process ID; 0 if prepared xact */
    /*
     * session id in mySessionMemoryEntry
     * stream works share SessionMemoryEntry with their parent sessions,
     * so sessMemorySessionid is their parent's as well.
     */
    ThreadId sessMemorySessionid;
    uint64 sessionid; /* if not zero, session id in thread pool*/
    GlobalSessionId globalSessionId;
    int logictid;     /*logic thread id*/
    TransactionId gtt_session_frozenxid;    /* session level global temp table relfrozenxid */

    int pgprocno;
    int nodeno;

    /* These fields are zero while a backend is still starting up: */
    BackendId backendId; /* This backend's backend ID (if assigned) */
    Oid databaseId;      /* OID of database this backend is using */
    Oid roleId;          /* OID of role using this backend */

    /* Backend or session working version number. */
    uint32 workingVersionNum;

    /*
     * While in hot standby mode, shows that a conflict signal has been sent
     * for the current transaction. Set/cleared while holding ProcArrayLock,
     * though not required. Accessed without lock, if needed.
     */
    bool recoveryConflictPending;

    /* Info about LWLock the process is currently waiting for, if any. */
    bool lwWaiting;        /* true if waiting for an LW lock */
    uint8 lwWaitMode;      /* lwlock mode being waited for */
    bool lwIsVictim;       /* force to give up LWLock acquire */
    dlist_node lwWaitLink; /* next waiter for same LW lock */

    /* Info about lock the process is currently waiting for, if any. */
    /* waitLock and waitProcLock are NULL if not currently waiting. */
    LOCK* waitLock;         /* Lock object we're sleeping on ... */
    PROCLOCK* waitProcLock; /* Per-holder info for awaited lock */
    LOCKMODE waitLockMode;  /* type of lock we're waiting for */
    LOCKMASK heldLocks;     /* bitmask for lock types already held on this
                             * lock object by this backend */

    /*
     * Info to allow us to wait for synchronous replication, if needed.
     * waitLSN is InvalidXLogRecPtr if not waiting; set only by user backend.
     * syncRepState must not be touched except by owning process or WALSender.
     * syncRepLinks used only while holding SyncRepLock.
     */
    XLogRecPtr waitLSN;     /* waiting for this LSN or higher */
    int syncRepState;       /* wait state for sync rep */
    bool syncRepInCompleteQueue; /* waiting in complete queue */
    SHM_QUEUE syncRepLinks; /* list link if process is in syncrep queue */

    XLogRecPtr waitPaxosLSN;    /* waiting for this LSN or higher on paxos callback */
    int syncPaxosState;  /* wait state for sync paxos, reuse syncRepState defines */
    SHM_QUEUE syncPaxosLinks;  /* list link if process is in syncpaxos queue */

    DataQueuePtr waitDataSyncPoint; /* waiting for this data sync point */
    int dataSyncRepState;           /* wait state for data sync rep */
    SHM_QUEUE dataSyncRepLinks;     /* list link if process is in datasyncrep queue */

    MemoryContext topmcxt; /*top memory context of this backend.*/
    char myProgName[64];
    pg_time_t myStartTime;
    syscalllock deleMemContextMutex;

    /* Support for group XID clearing. */
    /* true, if member of ProcArray group waiting for XID clear */
    bool procArrayGroupMember;
    /* next ProcArray group member waiting for XID clear */
    pg_atomic_uint32 procArrayGroupNext;
    /*
     * latest transaction id among the transaction's main XID and
     * subtransactions
     */
    TransactionId procArrayGroupMemberXid;

    /* commit sequence number send down */
    CommitSeqNo commitCSN;

    /* Support for group transaction status update. */
    bool clogGroupMember;                   /* true, if member of clog group */
    pg_atomic_uint32 clogGroupNext;         /* next clog group member */
    TransactionId clogGroupMemberXid;       /* transaction id of clog group member */
    CLogXidStatus clogGroupMemberXidStatus; /* transaction status of clog
                                             * group member */
    int64 clogGroupMemberPage;              /* clog page corresponding to
                                             * transaction id of clog group member */
    XLogRecPtr clogGroupMemberLsn;          /* WAL location of commit record for clog
                                             * group member */
#ifdef __aarch64__
    /* Support for group xlog insert. */
    bool xlogGroupMember;
    pg_atomic_uint32 xlogGroupNext;
    XLogRecData* xlogGrouprdata;
    XLogRecPtr xlogGroupfpw_lsn;
    XLogRecPtr* xlogGroupProcLastRecPtr;
    XLogRecPtr* xlogGroupXactLastRecEnd;
    void* xlogGroupCurrentTransactionState;
    XLogRecPtr* xlogGroupRedoRecPtr;
    void* xlogGroupLogwrtResult;
    XLogRecPtr xlogGroupReturntRecPtr;
    TimeLineID xlogGroupTimeLineID;
    bool* xlogGroupDoPageWrites;
    bool xlogGroupIsFPW;
    uint64 snap_refcnt_bitmap;
#endif

    LWLock* subxidsLock;
    struct XidCache subxids; /* cache for subtransaction XIDs */

    volatile GtmHostIndex my_gtmhost;
    GtmHostIndex suggested_gtmhost;
    pg_atomic_uint32 signal_cancel_gtm_conn_flag;

    /* Per-backend LWLock.	Protects fields below. */
    LWLock* backendLock; /* protects the fields below */

    /* Lock manager data, recording fast-path locks taken by this backend. */
    uint64 *fpLockBits;                             /* lock modes held for each fast-path slot */
    FastPathTag *fpRelId;                           /* slots for rel oids */
    bool fpVXIDLock;                                /* are we holding a fast-path VXID lock? */
    LocalTransactionId fpLocalTransactionId;        /* lxid for fast-path VXID
                                                     * lock */
    /* The proc which block cur proc */
    PROCLOCK* blockProcLock;

    /*
     * A knl_thrd_context pointer to find this proc's t_thrd.
     *
     * NOTE: Only be valid/used for lock waiter now. There is no concurrency risk for other lock
     * releaser, who must be holding the sepcified partition lock of lockmgr's shared hash table
     * to traverse the lock wait queue, to visit this thread since the lock wait queue would be
     * destoryed before thread exits.
     */
    void *waitLockThrd;

    char *dw_unaligned_buf;
    char *dw_buf;
    volatile bool flush_new_dw;
    volatile int32 dw_pos;

    /*
     * Support for lock groups.  Use LockHashPartitionLockByProc on the group
     * leader to get the LWLock protecting these fields.
     */
    PGPROC     *lockGroupLeader;    /* lock group leader, if I'm a member */
    dlist_head  lockGroupMembers;   /* list of members, if I'm a leader */
    dlist_node  lockGroupLink;  /* my member link, if I'm a member */

    /*
     * All PROCLOCK objects for locks held or awaited by this backend are
     * linked into one of these lists, according to the partition number of
     * their lock.
     */
    SHM_QUEUE myProcLocks[1];
};

/* NOTE: "typedef struct PGPROC PGPROC" appears in storage/lock/lock.h. */

/* the offset of the last padding if exists*/
#define PGXACT_PAD_OFFSET 55

/*
 * Prior to PostgreSQL 9.2, the fields below were stored as part of the
 * PGPROC.	However, benchmarking revealed that packing these particular
 * members into a separate array as tightly as possible sped up GetSnapshotData
 * considerably on systems with many CPU cores, by reducing the number of
 * cache lines needing to be fetched.  Thus, think very carefully before adding
 * anything else here.
 */
typedef struct PGXACT {
    GTM_TransactionHandle handle; /* txn handle in GTM */
    TransactionId xid;            /* id of top-level transaction currently being
                                   * executed by this proc, if running and XID
                                   * is assigned; else InvalidTransactionId */
    TransactionId prepare_xid;

    TransactionId xmin;     /* minimal running XID as it was when we were
                             * starting our xact, excluding LAZY VACUUM:
                             * vacuum must not remove tuples deleted by
                             * xid >= xmin ! */
    CommitSeqNo csn_min;    /* local csn min */
    CommitSeqNo csn_dr;
    TransactionId next_xid; /* xid sent down from CN */
    int nxids;              /* use int replace uint8, avoid overflow when sub xids >= 256 */
    uint8 vacuumFlags;      /* vacuum-related flags, see above */

    uint32 needToSyncXid; /* At GTM mode, there's a window between CSN log set and proc array remove.
                         * In this window, we can get CSN but TransactionIdIsInProgress returns true,
                         * So we need to sync at this window.
                         */
    bool delayChkpt;    /* true if this proc delays checkpoint start;
                         * previously called InCommit */
#ifdef __aarch64__
    char padding[PG_CACHE_LINE_SIZE - PGXACT_PAD_OFFSET]; /*padding to 128 bytes*/
#endif
} PGXACT;

/* the offset of the last padding if exists*/
#define PROC_HDR_PAD_OFFSET 112

/*
 * There is one ProcGlobal struct for the whole database cluster.
 */
typedef struct PROC_HDR {
    /* Array of PGPROC structures (not including dummies for prepared txns) */
    PGPROC** allProcs;
    /* Array of PGXACT structures (not including dummies for prepared txns) */
    PGXACT* allPgXact;
    /* Length of allProcs array */
    uint32 allProcCount;
    /* Length of all non-prepared Procs */
    uint32		allNonPreparedProcCount;
    /* Head of list of free PGPROC structures */
    PGPROC* freeProcs;
    /* Head of list of external's free PGPROC structures */
    PGPROC* externalFreeProcs;
    /* Head of list of autovacuum's free PGPROC structures */
    PGPROC* autovacFreeProcs;
    /* Head of list of cm agent's free PGPROC structures */
    PGPROC* cmAgentFreeProcs;
    /* Head of list of pg_job's free PGPROC structures */
    PGPROC* pgjobfreeProcs;
	/* Head of list of bgworker free PGPROC structures */
    PGPROC* bgworkerFreeProcs;
    /* First pgproc waiting for group XID clear */
    pg_atomic_uint32 procArrayGroupFirst;
    /* First pgproc waiting for group transaction status update */
    pg_atomic_uint32 clogGroupFirst;
    /* WALWriter process's latch */
    Latch* walwriterLatch;
    /* WALWriterAuxiliary process's latch */
    Latch* walwriterauxiliaryLatch;
    /* Checkpointer process's latch */
    Latch* checkpointerLatch;
    /* pagewriter main process's latch */
    Latch* pgwrMainThreadLatch;
    /* BCMWriter process's latch */
    Latch* cbmwriterLatch;
    volatile Latch* ShareStoragexlogCopyerLatch;
    volatile Latch* BarrierPreParseLatch;
    /* Current shared estimate of appropriate spins_per_delay value */
    int spins_per_delay;
    /* The proc of the Startup process, since not in ProcArray */
    PGPROC* startupProc;
    ThreadId startupProcPid;
    /* Buffer id of the buffer that Startup process waits for pin on, or -1 */
    int startupBufferPinWaitBufId;
#ifdef __aarch64__
    char pad[PG_CACHE_LINE_SIZE - PROC_HDR_PAD_OFFSET];
#endif
} PROC_HDR;

/*
 * We set aside some extra PGPROC structures for auxiliary processes,
 * ie things that aren't full-fledged backends but need shmem access.
 *
 * Background writer, checkpointer and WAL writer run during normal operation.
 * Startup process and WAL receiver also consume 2 slots, but WAL writer is
 * launched only after startup has exited, so we only need 4 slots.
 *
 * PGXC needs another slot for the pool manager process
 */
const int MAX_PAGE_WRITER_THREAD_NUM = 16;

#ifndef ENABLE_LITE_MODE
const int MAX_COMPACTION_THREAD_NUM = 100;
#else
const int MAX_COMPACTION_THREAD_NUM = 10;
#endif

/* number of multi auxiliary threads. */
#define NUM_MULTI_AUX_PROC \
    (MAX_PAGE_WRITER_THREAD_NUM + \
     MAX_RECOVERY_THREAD_NUM + \
     g_instance.shmem_cxt.ThreadPoolGroupNum + \
     MAX_COMPACTION_THREAD_NUM \
    )

#define NUM_AUXILIARY_PROCS (NUM_SINGLE_AUX_PROC + NUM_MULTI_AUX_PROC) 

/* max number of CMA's connections */
#define NUM_CMAGENT_PROCS (10)
/* buffer length of information when no free proc available for cm_agent */
#define CONNINFOLEN (64)

/* max number of DCF call back threads */
#define NUM_DCF_CALLBACK_PROCS \
        (g_instance.attr.attr_storage.dcf_attr.enable_dcf ? \
        g_instance.attr.attr_storage.dcf_attr.dcf_max_workers : 0)

#define GLOBAL_ALL_PROCS \
    (g_instance.shmem_cxt.MaxBackends + \
     NUM_CMAGENT_PROCS + NUM_AUXILIARY_PROCS + NUM_DCF_CALLBACK_PROCS + \
     (g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS))

#define GLOBAL_MAX_SESSION_NUM (2 * g_instance.shmem_cxt.MaxBackends)
#define GLOBAL_RESERVE_SESSION_NUM (g_instance.shmem_cxt.MaxReserveBackendId)
#define MAX_SESSION_SLOT_COUNT (GLOBAL_MAX_SESSION_NUM + GLOBAL_RESERVE_SESSION_NUM)
#define MAX_BACKEND_SLOT \
    (g_instance.attr.attr_common.enable_thread_pool ? MAX_SESSION_SLOT_COUNT : g_instance.shmem_cxt.MaxBackends)
#define MAX_SESSION_TIMEOUT 24 * 60 * 60 /* max session timeout value. */

#define BackendStatusArray_size (MAX_BACKEND_SLOT + NUM_AUXILIARY_PROCS)

#define GSC_MAX_BACKEND_SLOT (g_instance.shmem_cxt.MaxBackends + MAX_SESSION_SLOT_COUNT)

extern AlarmCheckResult ConnectionOverloadChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);

/*
 * Function Prototypes
 */
extern int ProcGlobalSemas(void);
extern Size ProcGlobalShmemSize(void);
extern void InitNuma(void);
extern void InitProcGlobal(void);
extern void InitProcess(void);
extern void InitProcessPhase2(void);
extern void InitAuxiliaryProcess(void);

extern int GetAuxProcEntryIndex(int baseIdx);
extern void PublishStartupProcessInformation(void);

extern bool HaveNFreeProcs(int n);
extern void ProcReleaseLocks(bool isCommit);
extern int GetUsedConnectionCount(void);
extern int GetUsedInnerToolConnCount(void);

extern void ProcQueueInit(PROC_QUEUE* queue);
extern int ProcSleep(LOCALLOCK* locallock, LockMethod lockMethodTable, bool allow_con_update, int waitSec);
extern PGPROC* ProcWakeup(PGPROC* proc, int waitStatus);
extern void ProcLockWakeup(LockMethod lockMethodTable, LOCK* lock, const PROCLOCK* proclock = NULL);
extern void ProcBlockerUpdate(PGPROC *waiterProc, PROCLOCK *blockerProcLock, const char* lockMode, bool isLockHolder);
extern bool IsWaitingForLock(void);
extern void LockErrorCleanup(void);

extern void ProcWaitForSignal(void);
extern void ProcSendSignal(ThreadId pid);

extern TimestampTz GetStatementFinTime();

extern bool enable_sig_alarm(int delayms, bool is_statement_timeout);
extern bool enable_lockwait_sig_alarm(int delayms);
extern bool enable_session_sig_alarm(int delayms);
extern bool disable_session_sig_alarm(void);

extern bool disable_sig_alarm(bool is_statement_timeout);
extern bool pause_sig_alarm(bool is_statement_timeout);
extern bool resume_sig_alarm(bool is_statement_timeout);
extern void handle_sig_alarm(SIGNAL_ARGS);

extern bool enable_standby_sig_alarm(TimestampTz now, TimestampTz fin_time, bool deadlock_only);
extern bool disable_standby_sig_alarm(void);
extern void handle_standby_sig_alarm(SIGNAL_ARGS);

extern ThreadId getThreadIdFromLogicThreadId(int logictid);
extern int getLogicThreadIdFromThreadId(ThreadId tid);

extern bool IsRedistributionWorkerProcess(void);
extern void PgStatCMAThreadStatus();

void CancelBlockedRedistWorker(LOCK* lock, LOCKMODE lockmode);

extern void BecomeLockGroupLeader(void);
extern void BecomeLockGroupMember(PGPROC *leader);

static inline bool TransactionIdOlderThanAllUndo(TransactionId xid)
{
    uint64 cutoff = pg_atomic_read_u64(&g_instance.undo_cxt.oldestXidInUndo);
    return xid < cutoff;
}
static inline bool TransactionIdOlderThanFrozenXid(TransactionId xid)
{
    uint64 cutoff = pg_atomic_read_u64(&g_instance.undo_cxt.oldestFrozenXid);
    return xid < cutoff;
}

extern int GetThreadPoolStreamProcNum(void);

#endif /* PROC_H */
