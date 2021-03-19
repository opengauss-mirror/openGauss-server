/* -------------------------------------------------------------------------
 *
 * lwlock.h
 *	  Lightweight lock manager
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lock/lwlock.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LWLOCK_H
#define LWLOCK_H

#include "lib/ilist.h"
#include "storage/lock/s_lock.h"
#include "utils/atomic.h"
#include "gs_thread.h"

typedef volatile uint32 pg_atomic_uint32;
typedef volatile uint64 pg_atomic_uint64;

/* Names for fixed lwlocks and NUM_INDIVIDUAL_LWLOCKS */
#ifndef WIN32
#include "storage/lwlocknames.h"
#endif
extern const char *const MainLWLockNames[];

/*
 * It's a bit odd to declare NUM_BUFFER_PARTITIONS and NUM_LOCK_PARTITIONS
 * here, but we need them to figure out offsets within MainLWLockArray, and having
 * this file include lock.h or bufmgr.h would be backwards.
 */

/* Number of partitions of the shared operator-level statistics hashtable */
#define NUM_OPERATOR_REALTIME_PARTITIONS 32

#define NUM_OPERATOR_HISTORY_PARTITIONS 64

/* Number of partitions of the shared buffer mapping hashtable */
#define NUM_BUFFER_PARTITIONS 4096

/* change 1024 ->2048 for two cache(compress data cache and meta cache) */
#define NUM_CACHE_BUFFER_PARTITIONS 2048

/* Number of partitions of the shared session-level statistics hashtable */
#define NUM_SESSION_REALTIME_PARTITIONS 32

#define NUM_SESSION_HISTORY_PARTITIONS 64

/* Number of partitions of the shared instance statistics hashtable */
#define NUM_INSTANCE_REALTIME_PARTITIONS 32

/* CSN log partitions */
#define NUM_CSNLOG_PARTITIONS 512

/* Clog partitions */
#define NUM_CLOG_PARTITIONS 256

/* Number of partitions the shared lock tables are divided into */
#define LOG2_NUM_LOCK_PARTITIONS 4
#define NUM_LOCK_PARTITIONS (1 << LOG2_NUM_LOCK_PARTITIONS)

/* Number of partitions the shared predicate lock tables are divided into */
#define LOG2_NUM_PREDICATELOCK_PARTITIONS 4
#define NUM_PREDICATELOCK_PARTITIONS (1 << LOG2_NUM_PREDICATELOCK_PARTITIONS)

/* Number of partions the shared unique SQL hashtable */
#define NUM_UNIQUE_SQL_PARTITIONS 64

/* Number of partions the shared instr user hashtable */
#define NUM_INSTR_USER_PARTITIONS 64

/* Number of partions the global plan cache hashtable */
#define NUM_GPC_PARTITIONS 128

/* Number of partions normalized query hashtable */
#define NUM_NORMALIZED_SQL_PARTITIONS 64

/* Number of partions the max page flush lsn file */
#define NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS 1024

/* Number of partions the ngroup info hash table */
#define NUM_NGROUP_INFO_PARTITIONS  256

/* Number of partions the io state hashtable */
#define NUM_IO_STAT_PARTITIONS 128

/* Number of partions the global sequence hashtable */
#define NUM_GS_PARTITIONS 1024

#ifdef WIN32
#define NUM_INDIVIDUAL_LWLOCKS           100
#endif
/* 
 * WARNING---Please keep the order of LWLockTrunkOffset and BuiltinTrancheIds consistent!!! 
*/

/* Offsets for various chunks of preallocated lwlocks in main array. */
enum LWLockTrunkOffset {
    FirstBufMappingLock = NUM_INDIVIDUAL_LWLOCKS,
    FirstLockMgrLock = FirstBufMappingLock + NUM_BUFFER_PARTITIONS,
    FirstPredicateLockMgrLock = FirstLockMgrLock + NUM_LOCK_PARTITIONS,
    FirstOperatorRealTLock = FirstPredicateLockMgrLock+ NUM_PREDICATELOCK_PARTITIONS,
    FirstOperatorHistLock = FirstOperatorRealTLock + NUM_OPERATOR_REALTIME_PARTITIONS,
    FirstSessionRealTLock = FirstOperatorHistLock + NUM_OPERATOR_HISTORY_PARTITIONS,
    FirstSessionHistLock = FirstSessionRealTLock + NUM_SESSION_REALTIME_PARTITIONS,
    FirstInstanceRealTLock = FirstSessionHistLock + NUM_SESSION_HISTORY_PARTITIONS,
    /* Cache Mgr lock IDs */
    FirstCacheSlotMappingLock = FirstInstanceRealTLock + NUM_INSTANCE_REALTIME_PARTITIONS,
    FirstCSNBufMappingLock = FirstCacheSlotMappingLock + NUM_CACHE_BUFFER_PARTITIONS,
    FirstCBufMappingLock = FirstCSNBufMappingLock + NUM_CSNLOG_PARTITIONS,
    /* Instrumentaion */
    FirstUniqueSQLMappingLock = FirstCBufMappingLock + NUM_CLOG_PARTITIONS,
    FirstInstrUserLock = FirstUniqueSQLMappingLock + NUM_UNIQUE_SQL_PARTITIONS,
    /* global plan cache */
    FirstGPCMappingLock = FirstInstrUserLock + NUM_INSTR_USER_PARTITIONS,
    FirstGPCPrepareMappingLock = FirstGPCMappingLock + NUM_GPC_PARTITIONS,
    /* ASP */
    FirstASPMappingLock = FirstGPCPrepareMappingLock + NUM_GPC_PARTITIONS,
    /* global sequence */
    FirstGlobalSeqLock = FirstASPMappingLock + NUM_UNIQUE_SQL_PARTITIONS,

    FirstNormalizedSqlLock = FirstGlobalSeqLock + NUM_GS_PARTITIONS,
    FirstMPFLLock = FirstNormalizedSqlLock + NUM_NORMALIZED_SQL_PARTITIONS,

    FirstNGroupMappingLock = FirstMPFLLock + NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS,
    FirstIOStatLock = FirstNGroupMappingLock + NUM_NGROUP_INFO_PARTITIONS,

    /* must be last: */
    NumFixedLWLocks = FirstIOStatLock + NUM_IO_STAT_PARTITIONS
};

/*
 * WARNING----Please keep BuiltinTrancheIds and BuiltinTrancheNames consistent!!!
 *
 * Every tranche ID less than NUM_INDIVIDUAL_LWLOCKS is reserved; also,
 * we reserve additional tranche IDs for builtin tranches not included in
 * the set of individual LWLocks.  A call to LWLockNewTrancheId(to be added in future) will never
 * return a value less than LWTRANCHE_NATIVE_TRANCHE_NUM.
 */
enum BuiltinTrancheIds
{
    LWTRANCHE_BUFMAPPING = NUM_INDIVIDUAL_LWLOCKS,
    LWTRANCHE_LOCK_MANAGER,
    LWTRANCHE_PREDICATE_LOCK_MANAGER,
    LWTRANCHE_OPERATOR_REAL_TIME,
    LWTRANCHE_OPERATOR_HISTORY,
    LWTRANCHE_SESSION_REAL_TIME,
    LWTRANCHE_SESSION_HISTORY,
    LWTRANCHE_INSTANCE_REAL_TIME,
    LWTRANCHE_CACHE_SLOT_MAPPING,
    LWTRANCHE_CSN_BUFMAPPING,
    LWTRANCHE_CLOG_BUFMAPPING,
    LWTRANCHE_UNIQUE_SQLMAPPING,
    LWTRANCHE_INSTR_USER,
    LWTRANCHE_GPC_MAPPING,
    LWTRANCHE_GPC_PREPARE_MAPPING,
    LWTRANCHE_ASP_MAPPING,
    LWTRANCHE_GlobalSeq, 
    LWTRANCHE_NORMALIZED_SQL,
    LWTRANCHE_BUFFER_IO_IN_PROGRESS,
    LWTRANCHE_BUFFER_CONTENT,
    LWTRANCHE_DATA_CACHE,
    LWTRANCHE_META_CACHE,
    LWTRANCHE_PROC,
    LWTRANCHE_REPLICATION_SLOT,
    LWTRANCHE_ASYNC_CTL,
    LWTRANCHE_CLOG_CTL,
    LWTRANCHE_CSNLOG_CTL,
    LWTRANCHE_MULTIXACTOFFSET_CTL,
    LWTRANCHE_MULTIXACTMEMBER_CTL,
    LWTRANCHE_OLDSERXID_SLRU_CTL,
    LWTRANCHE_WAL_INSERT,
    LWTRANCHE_DOUBLE_WRITE,
    LWTRANCHE_DW_SINGLE_POS,
    LWTRANCHE_DW_SINGLE_WRITE,
    LWTRANCHE_REDO_POINT_QUEUE,
    LWTRANCHE_PRUNE_DIRTY_QUEUE,
    LWTRANCHE_ACCOUNT_TABLE,
    LWTRANCHE_EXTEND, // For general 3rd plugin
    LWTRANCHE_MPFL,
    LWTRANCHE_GTT_CTL, // For GTT
    LWTRANCHE_PLDEBUG, // For Pldebugger
    LWTRANCHE_NGROUP_MAPPING,    
    LWTRANCHE_MATVIEW_SEQNO,
    LWTRANCHE_IO_STAT,
    LWTRANCHE_WAL_FLUSH_WAIT,
    LWTRANCHE_WAL_BUFFER_INIT_WAIT,
    LWTRANCHE_WAL_INIT_SEGMENT,
    /*
     * Each trancheId above should have a corresponding item in BuiltinTrancheNames;
     */
    LWTRANCHE_NATIVE_TRANCHE_NUM,
    LWTRANCHE_UNKNOWN = 65535
};

#ifndef cpu_relax
// New lock
#if  defined(__x86_64__) || defined(__x86__)
#define cpu_relax() asm volatile("pause\n": : :"memory")
#else                                               // some web says yield:::memory
#define cpu_relax() asm volatile("" : : : "memory") // equivalent to "rep; nop"
#endif
#endif

typedef enum LWLockMode {
    LW_EXCLUSIVE,
    LW_SHARED,
    LW_WAIT_UNTIL_FREE /* A special mode used in PGPROC->lwlockMode,
                        * when waiting for lock to become free. Not
                        * to be used as LWLockAcquire argument */
} LWLockMode;

/* To avoid pointer misuse during hash search, we wrapper the LWLock* in the following structure. */
struct LWLock;
typedef struct {
    LWLock* lock;
} LWLockAddr;

typedef struct {
    LWLockAddr lock_addr;
    LWLockMode lock_sx;
} lwlock_id_mode;

struct PGPROC;

typedef struct LWLock {
    uint16      tranche;            /* tranche ID */
    pg_atomic_uint32 state; /* state of exlusive/nonexclusive lockers */
    dlist_head waiters;     /* list of waiting PGPROCs */
#ifdef LOCK_DEBUG
    pg_atomic_uint32 nwaiters; /* number of waiters */
    struct PGPROC* owner;      /* last exlusive owner of the lock */
#endif
#ifdef ENABLE_THREAD_CHECK
    pg_atomic_uint32 rwlock;
    pg_atomic_uint32 listlock;
#endif
} LWLock;

/*
 * All the LWLock structs are allocated as an array in shared memory.
 * (LWLockIds are indexes into the array.)	We force the array stride to
 * be a power of 2, which saves a few cycles in indexing, but more
 * importantly also ensures that individual LWLocks don't cross cache line
 * boundaries.	This reduces cache contention problems, especially on AMD
 * Opterons.  (Of course, we have to also ensure that the array start
 * address is suitably aligned.)
 *
 * On a 32-bit platforms a LWLock will these days fit into 16 bytes, but since
 * that didn't use to be the case and cramming more lwlocks into a cacheline
 * might be detrimental performancewise we still use 32 byte alignment
 * there. So, both on 32 and 64 bit platforms, it should fit into 32 bytes
 * unless slock_t is really big.  We allow for that just in case.
 *
 * Add even more padding so that each LWLock takes up an entire cache line;
 * this is useful, for example, in the main LWLock array, where the overall number of
 * locks is small but some are heavily contended.
 *
 * In future, the LWLock should be devided into two parts: One uses LWLockPadded,
 * the other uses LWLockMinimallyPadded. This should relax the requirement for the single global array.
 */
#ifdef __aarch64__
#define LWLOCK_PADDED_SIZE PG_CACHE_LINE_SIZE
#else
#define LWLOCK_PADDED_SIZE (sizeof(LWLock) <= 32 ? 32 : 64)
#endif

typedef union LWLockPadded {
    LWLock lock;
    char pad[LWLOCK_PADDED_SIZE];
} LWLockPadded;

extern PGDLLIMPORT LWLockPadded *MainLWLockArray;

/*
 * We use this structure to keep track of locked LWLocks for release
 * during error recovery.  The maximum size could be determined at runtime
 * if necessary, but it seems unlikely that more than a few locks could
 * ever be held simultaneously.
 */
#define MAX_SIMUL_LWLOCKS 4224

/* struct representing the LWLocks we're holding */
typedef struct LWLockHandle {
    LWLock* lock;
    LWLockMode mode;
} LWLockHandle;

#define GetMainLWLockByIndex(i) \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[i].lock)

extern void DumpLWLockInfo();
extern LWLock* LWLockAssign(int trancheId);
extern void LWLockInitialize(LWLock* lock, int tranche_id);
extern bool LWLockAcquire(LWLock* lock, LWLockMode mode, bool need_update_lockid = false);
extern bool LWLockConditionalAcquire(LWLock* lock, LWLockMode mode);
extern bool LWLockAcquireOrWait(LWLock* lock, LWLockMode mode);
extern void LWLockRelease(LWLock* lock);
extern void LWLockReleaseClearVar(LWLock* lock, uint64* valptr, uint64 val);
extern void LWLockReleaseAll(void);
extern bool LWLockHeldByMe(LWLock* lock);
extern bool LWLockHeldByMeInMode(LWLock* lock, LWLockMode mode);
extern void LWLockReset(LWLock* lock);

extern void LWLockOwn(LWLock* lock);
extern void LWLockDisown(LWLock* lock);

extern bool LWLockWaitForVar(LWLock* lock, uint64* valptr, uint64 oldval, uint64* newval);
extern void LWLockUpdateVar(LWLock* lock, uint64* valptr, uint64 value);

extern int NumLWLocks(void);
extern Size LWLockShmemSize(void);
extern void CreateLWLocks(void);

extern void RequestAddinLWLocks(int n);
extern const char* GetBuiltInTrancheName(int trancheId);

/*
 * There is another, more flexible method of obtaining lwlocks. First, call
 * LWLockNewTrancheId just once to obtain a tranche ID; this allocates from
 * a shared counter.  Next, each individual process using the tranche should
 * call LWLockRegisterTranche() to associate that tranche ID with a name.
 * Finally, LWLockInitialize should be called just once per lwlock, passing
 * the tranche ID as an argument.
 *
 * It may seem strange that each process using the tranche must register it
 * separately, but dynamic shared memory segments aren't guaranteed to be
 * mapped at the same address in all coordinating backends, so storing the
 * registration in the main shared memory segment wouldn't work for that case.
 */
extern void LWLockRegisterTranche(int tranche_id, const char *tranche_name);

extern void wakeup_victim(LWLock *lock, ThreadId victim_tid);
extern int *get_held_lwlocks_num(void);
extern uint32 get_held_lwlocks_maxnum(void);
extern void* get_held_lwlocks(void);
extern void copy_held_lwlocks(void* heldlocks, lwlock_id_mode* dst, int num_heldlocks);
extern const char* GetLWLockIdentifier(uint32 classId, uint16 eventId);
extern LWLockMode GetHeldLWLockMode(LWLock* lock);

extern const char** LWLockTrancheArray;
extern int LWLockTranchesAllocated;

#define T_NAME(lock) \
    ((lock) ? (LWLockTrancheArray[(lock)->tranche]) : (""))

/*
 * Peviously, we used an enum type called LWLockId to refer
 * to LWLocks.  New code should instead use LWLock *.  However, for the
 * convenience of third-party code, we include the following typedef.
 */
//typedef LWLock *LWLockId; // Uncomment it later. Now should disable to find bugs

#endif   /* LWLOCK_H */
