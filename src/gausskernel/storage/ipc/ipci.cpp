/* -------------------------------------------------------------------------
 *
 * ipci.cpp
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/ipci.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/dfs/dfs_insert.h"
#include "access/xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "access/double_write.h"
#include "catalog/storage_gtt.h"
#include "commands/tablespace.h"
#include "commands/async.h"
#include "commands/matview.h"
#include "foreign/dummyserver.h"
#include "job/job_scheduler.h"
#include "miscadmin.h"
#include "pgstat.h"
#ifdef PGXC
    #include "pgxc/nodemgr.h"
#endif
#include "postmaster/autovacuum.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "postmaster/startup.h"
#include "replication/heartbeat.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "storage/buf/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "storage/cstore/cstorealloc.h"
#include "storage/cucache_mgr.h"
#include "storage/dfs/dfs_connector.h"
#include "utils/memprot.h"
#ifdef ENABLE_MULTIPLE_NODES
    #include "tsdb/cache/queryid_cachemgr.h"
#endif   /* ENABLE_MULTIPLE_NODES */


/* we use semaphore not LWLOCK, because when thread InitGucConfig, it does not get a t_thrd.proc */
pthread_mutex_t gLocaleMutex = PTHREAD_MUTEX_INITIALIZER;

extern void SetShmemCxt(void);

/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.	Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void RequestAddinShmemSpace(Size size)
{
    if (IsUnderPostmaster || !t_thrd.storage_cxt.addin_request_allowed)
        return; /* too late */
    t_thrd.storage_cxt.total_addin_request = add_size(t_thrd.storage_cxt.total_addin_request, size);
}

/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 *
 * If "makePrivate" is true then we only need private memory, not shared
 * memory.	This is true for a standalone backend, false for a postmaster.
 */
void CreateSharedMemoryAndSemaphores(bool makePrivate, int port)
{
    if (!IsUnderPostmaster) {
        InitNuma();

        /* Set max backends and thread pool group number before alloc share memory array. */
        SetShmemCxt();

        PGShmemHeader* seghdr = NULL;
        Size size;
        int numSemas;

        /*
         * Size of the Postgres shared-memory block is estimated via
         * moderately-accurate estimates for the big hogs, plus 100K for the
         * stuff that's too small to bother with estimating.
         *
         * We take some care during this phase to ensure that the total size
         * request doesn't overflow size_t.  If this gets through, we don't
         * need to be so careful during the actual allocation phase.
         */
        size = 100000;
        size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE, sizeof(ShmemIndexEnt)));
        size = add_size(size, BufferShmemSize());
        size = add_size(size, ReplicationSlotsShmemSize());
        size = add_size(size, LockShmemSize());
        size = add_size(size, PredicateLockShmemSize());
        size = add_size(size, ProcGlobalShmemSize());
        size = add_size(size, XLOGShmemSize());
        size = add_size(size, XLogStatShmemSize());
        size = add_size(size, CLOGShmemSize());
        size = add_size(size, CSNLOGShmemSize());
        size = add_size(size, TwoPhaseShmemSize());
        size = add_size(size, MultiXactShmemSize());
        size = add_size(size, LWLockShmemSize());
        size = add_size(size, ProcArrayShmemSize());
        size = add_size(size, RingBufferShmemSize());
        size = add_size(size, BackendStatusShmemSize());
        size = add_size(size, sessionTimeShmemSize());
        size = add_size(size, sessionStatShmemSize());
        size = add_size(size, sessionMemoryShmemSize());
        size = add_size(size, SInvalShmemSize());
        size = add_size(size, PMSignalShmemSize());
        size = add_size(size, ProcSignalShmemSize());
        size = add_size(size, CheckpointerShmemSize());
        size = add_size(size, AutoVacuumShmemSize());
        size = add_size(size, WalSndShmemSize());
        size = add_size(size, WalRcvShmemSize());
        size = add_size(size, DataSndShmemSize());
        size = add_size(size, DataRcvShmemSize());
        /* DataSenderQueue, DataWriterQueue has the same size, WalDataWriterQueue is deleted */
        size = add_size(size, mul_size(2, DataQueueShmemSize()));
        size = add_size(size, CBMShmemSize());
        size = add_size(size, HaShmemSize());
        size = add_size(size, NotifySignalShmemSize());
        size = add_size(size, JobInfoShmemSize());
        size = add_size(size, BTreeShmemSize());
        size = add_size(size, SyncScanShmemSize());
        size = add_size(size, AsyncShmemSize());
        size = add_size(size, active_gtt_shared_hash_size());
#ifdef PGXC
        size = add_size(size, NodeTablesShmemSize());
#endif

        size = add_size(size, TableSpaceUsageManager::ShmemSize());
        size = add_size(size, LsnXlogFlushChkShmemSize());
        size = add_size(size, heartbeat_shmem_size());
        size = add_size(size, MatviewShmemSize());

        /* freeze the addin request size and include it */
        t_thrd.storage_cxt.addin_request_allowed = false;
        size = add_size(size, t_thrd.storage_cxt.total_addin_request);

        /* might as well round it off to a multiple of a typical page size */
        size = add_size(size, 8192 - (size % 8192));

        ereport(DEBUG3, (errmsg("invoking IpcMemoryCreate(size=%lu)", (unsigned long)size)));

        /* Initialize the Memory Protection feature */
        gs_memprot_init(size);

        /*
         * Create the shmem segment
         */
        seghdr = PGSharedMemoryCreate(size, makePrivate, port);

        InitShmemAccess(seghdr);

        /*
         * Create semaphores
         */
        numSemas = ProcGlobalSemas();
        numSemas += SpinlockSemas();
        numSemas += XLogSemas();

#ifdef ENABLED_DEBUG_SYNC
        numSemas += 1; /* For debug sync handling */
#endif

        numSemas += 1; /* for locale concurrency control */

        PGReserveSemaphores(numSemas, port);
    } else {
        /*
         * We are reattaching to an existing shared memory segment. This
         * should only be reached in the EXEC_BACKEND case, and even then only
         * with makePrivate == false.
         */
#ifdef EXEC_BACKEND
        Assert(!makePrivate);
#else
        ereport(PANIC, (errmsg("should be attached to shared memory already")));
#endif
    }

    /*
     * Set up shared memory allocation mechanism
     */
    if (!IsUnderPostmaster)
        InitShmemAllocation();

    /*
     * Now initialize LWLocks, which do shared memory allocation and are
     * needed for InitShmemIndex.
     */
    if (!IsUnderPostmaster)
        CreateLWLocks();

    /*
     * Set up shmem.c index hashtable
     */
    InitShmemIndex();

    /*
     * Set up xlog, clog, and buffers
     */
    XLOGShmemInit();
    XLogStatShmemInit();

    {
        CLOGShmemInit();
        CSNLOGShmemInit();
        MultiXactShmemInit();
        InitBufferPool();
        /* global temporay table */
        active_gtt_shared_hash_init();
        /*
         * Set up lock manager
         */
        InitLocks();

        /*
         * Set up predicate lock manager
         */
        InitPredicateLocks();
    }

    /*
     * Set up process table
     */
    if (!IsUnderPostmaster) {
        InitProcGlobal();
        CreateSharedProcArray();
    }

    CreateSharedRingBuffer();
    CreateSharedBackendStatus();
    sessionTimeShmemInit();
    sessionStatShmemInit();
    sessionMemoryShmemInit();

    {
        TwoPhaseShmemInit();
    }

    /*
     * Set up shared-inval messaging
     */
    CreateSharedInvalidationState();

    /*
     * Set up interprocess signaling mechanisms
     */
    PMSignalShmemInit();
    ProcSignalShmemInit();

    {
        CheckpointerShmemInit();
        CBMShmemInit();
        AutoVacuumShmemInit();
    }
    ReplicationSlotsShmemInit();
    WalSndShmemInit();
    /*
    * Set up WAL semaphores. This must be done after WalSndShmemInit().
    */
    if (!IsUnderPostmaster) {
        InitWalSemaphores();
    }
    WalRcvShmemInit();
    DataSndShmemInit();
    DataRcvShmemInit();
    DataSenderQueueShmemInit();
    DataWriterQueueShmemInit();
    HaShmemInit();
    heartbeat_shmem_init();
    MatviewShmemInit();

    {
        NotifySignalShmemInit();

        JobInfoShmemInit();
        /*
         * Set up other modules that need some shared memory space
         */
        BTreeShmemInit();
        SyncScanShmemInit();
        AsyncShmemInit();

#ifdef PGXC
        NodeTablesShmemInit();
#endif
    }

    /*
     * Set up tablespace usage information management struct
     */
    TableSpaceUsageManager::Init();

    /*
     * Set up thread shared fd cache
     */
    InitDataFileIdCache();

    /*
     * Set up CStoreSpaceAllocator
     */
    CStoreAllocator::InitColSpaceCache();

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * Set up TableStatusCache
     */
    if (g_instance.attr.attr_common.enable_tsdb) {
        Tsdb::TableStatus::GetInstance().init();
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    /*
     * Set up DfsConnector cache
     */
    dfs::InitOBSConnectorCacheLock();

    /* set up Dummy server cache */
    InitDummyServrCache();

    /*
     * Set up dfs space cache hash table.
     */
    DfsInsert::InitDfsSpaceCache();

    LsnXlogFlushChkShmInit();   

    if (g_instance.ckpt_cxt_ctl->prune_queue_lock == NULL) {
        g_instance.ckpt_cxt_ctl->prune_queue_lock = LWLockAssign(LWTRANCHE_PRUNE_DIRTY_QUEUE);
    }
    if (g_instance.pid_cxt.PageWriterPID == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);
        g_instance.pid_cxt.PageWriterPID =
            (ThreadId*)palloc0(sizeof(ThreadId) * g_instance.attr.attr_storage.pagewriter_thread_num);

        int thread_num = g_instance.attr.attr_storage.bgwriter_thread_num;
        thread_num = thread_num > 0 ? thread_num : 1;
        g_instance.pid_cxt.CkptBgWriterPID = (ThreadId*)palloc0(sizeof(ThreadId) * thread_num);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (g_instance.attr.attr_storage.enableIncrementalCheckpoint &&
        g_instance.ckpt_cxt_ctl->page_writer_procs.writer_proc == NULL) {
        incre_ckpt_pagewriter_cxt_init();
    }
    if (g_instance.attr.attr_storage.enableIncrementalCheckpoint &&
        g_instance.bgwriter_cxt.bgwriter_procs == NULL) {
        incre_ckpt_bgwriter_cxt_init();
    }

    if (g_instance.attr.attr_storage.enableIncrementalCheckpoint &&
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.ckpt_rec_queue == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.recovery_queue_lock = LWLockAssign(LWTRANCHE_REDO_POINT_QUEUE);
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.ckpt_rec_queue =
            (CheckPointItem*)palloc0(sizeof(CheckPointItem) * RESTART_POINT_QUEUE_LEN);
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.start = 0;
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.end = 0;
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (g_instance.ckpt_cxt_ctl->ckpt_redo_state.recovery_queue_lock == NULL) {
        g_instance.ckpt_cxt_ctl->ckpt_redo_state.recovery_queue_lock = LWLockAssign(LWTRANCHE_REDO_POINT_QUEUE);
    }

    /*
     * Now give loadable modules a chance to set up their shmem allocations
     */
    if (t_thrd.storage_cxt.shmem_startup_hook)
        t_thrd.storage_cxt.shmem_startup_hook();
}

