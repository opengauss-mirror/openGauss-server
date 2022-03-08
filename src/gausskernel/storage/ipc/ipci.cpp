/* -------------------------------------------------------------------------
 *
 * ipci.cpp
 *	  openGauss inter-process communication initialization code.
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
#include "catalog/pg_uid_fn.h"
#include "catalog/storage_gtt.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/knl_undoworker.h"
#include "access/ustore/knl_undorequest.h"
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
#include "postmaster/bgworker.h"
#include "postmaster/pagewriter.h"
#include "postmaster/pagerepair.h"
#include "postmaster/postmaster.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/rbcleaner.h"
#include "replication/slot.h"
#include "postmaster/startup.h"
#include "replication/heartbeat.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/dataqueue.h"
#include "replication/origin.h"
#include "replication/logicallauncher.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/smgr/segment.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#include "storage/cstore/cstorealloc.h"
#include "storage/cucache_mgr.h"
#include "storage/dfs/dfs_connector.h"
#include "storage/xlog_share_storage/xlog_share_storage.h"
#include "utils/memprot.h"
#include "pgaudit.h"
#ifdef ENABLE_MULTIPLE_NODES
    #include "tsdb/cache/queryid_cachemgr.h"
#endif   /* ENABLE_MULTIPLE_NODES */

#include "replication/dcf_replication.h"
#include "commands/verify.h"

/* we use semaphore not LWLOCK, because when thread InitGucConfig, it does not get a t_thrd.proc */
pthread_mutex_t gLocaleMutex = PTHREAD_MUTEX_INITIALIZER;

extern void SetShmemCxt(void);
#ifdef ENABLE_MULTIPLE_NODES
extern void InitDisasterCache();
#endif

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

Size ComputeTotalSizeOfShmem()
{
        Size size;
        /*
         * Size of the openGauss shared-memory block is estimated via
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
#ifndef ENABLE_MULTIPLE_NODES
        size = add_size(size, ReplicationOriginShmemSize());
        size = add_size(size, ApplyLauncherShmemSize());
#endif
        size = add_size(size, LockShmemSize());
        size = add_size(size, PredicateLockShmemSize());
        size = add_size(size, ProcGlobalShmemSize());
        size = add_size(size, XLOGShmemSize());
        size = add_size(size, XLogStatShmemSize());
        size = add_size(size, CLOGShmemSize());
        size = add_size(size, CSNLOGShmemSize());
        size = add_size(size, mul_size(NUM_TWOPHASE_PARTITIONS, TwoPhaseShmemSize()));
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
        size = add_size(size, PageWriterShmemSize());
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
        size = add_size(size, AsyncRollbackHashShmemSize());
        size = add_size(size, UndoWorkerShmemSize());
        size = add_size(size, TxnSnapCapShmemSize());
        size = add_size(size, RbCleanerShmemSize());

#ifdef PGXC
        size = add_size(size, NodeTablesShmemSize());
#endif

        size = add_size(size, TableSpaceUsageManager::ShmemSize());
        size = add_size(size, LsnXlogFlushChkShmemSize());
        size = add_size(size, heartbeat_shmem_size());
        size = add_size(size, MatviewShmemSize());
#ifndef ENABLE_MULTIPLE_NODES
        if(g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            size = add_size(size, DcfContextShmemSize());
        }
#endif
        size = add_size(size, CalShareStorageCtlSize());
        /* freeze the addin request size and include it */
        t_thrd.storage_cxt.addin_request_allowed = false;
        size = add_size(size, t_thrd.storage_cxt.total_addin_request);

        /* might as well round it off to a multiple of a typical page size */
        size = add_size(size, 8192 - (size % 8192));
        return size;
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
        int numSemas;
        Size size = ComputeTotalSizeOfShmem();
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

    if (!IsUnderPostmaster)
    {
        /*
         * Set up shared memory allocation mechanism
         */
        InitShmemAllocation();

        /*
         * Now initialize LWLocks, which do shared memory allocation and are
         * needed for InitShmemIndex.
         */
        CreateLWLocks();
    }

    /*
     * Set up shmem.c index hashtable
     */
    InitShmemIndex();

#ifdef DEBUG_UHEAP
        UHeapSchemeInit();
#endif

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
        InitBgworkerGlobal();
        CreateSharedProcArray();
        CreateProcXactHashTable();
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
        TxnSnapCapShmemInit();
        RbCleanerShmemInit();
    }
    ReplicationSlotsShmemInit();
#ifndef ENABLE_MULTIPLE_NODES
    ReplicationOriginShmemInit();
    ApplyLauncherShmemInit();
#endif
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
    AsyncRollbackHashShmemInit();
    UndoWorkerShmemInit();
    heartbeat_shmem_init();
    MatviewShmemInit();
#ifndef ENABLE_MULTIPLE_NODES
    if(g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        DcfContextShmemInit();
    }
#endif

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
    InitUidCache();

    /*
     * Set up seg spc cache
     */
    InitSegSpcCache();

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_DISASTER_RECOVER_MODE) {
        InitDisasterCache();
    }
#endif

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

#ifndef ENABLE_LITE_MODE
    /*
     * Set up DfsConnector cache
     */
    dfs::InitOBSConnectorCacheLock();
#endif

    /* set up Dummy server cache */
    InitDummyServrCache();

    /*
     * Set up dfs space cache hash table.
     */
    DfsInsert::InitDfsSpaceCache();

    LsnXlogFlushChkShmInit();

    PageRepairHashTblInit();
    FileRepairHashTblInit();
    initRepairBadBlockStat();

    if (g_instance.ckpt_cxt_ctl->prune_queue_lock == NULL) {
        g_instance.ckpt_cxt_ctl->prune_queue_lock = LWLockAssign(LWTRANCHE_PRUNE_DIRTY_QUEUE);
    }
    if (g_instance.pid_cxt.PageWriterPID == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.increCheckPoint_context);
        int thread_num = g_instance.attr.attr_storage.pagewriter_thread_num + 1;
        g_instance.pid_cxt.PageWriterPID = (ThreadId*)palloc0(sizeof(ThreadId) * thread_num);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (g_instance.pid_cxt.PgAuditPID == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.audit_cxt.global_audit_context);
        int thread_num = g_instance.attr.attr_security.audit_thread_num;
        g_instance.pid_cxt.PgAuditPID = (ThreadId*)palloc0(sizeof(ThreadId) * thread_num);
        if (g_instance.audit_cxt.index_file_lock == NULL) {
            g_instance.audit_cxt.index_file_lock = LWLockAssign(LWTRANCHE_AUDIT_INDEX_WAIT);
        }
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (ENABLE_INCRE_CKPT) {
        PageWriterSyncShmemInit();
    }
    if (ENABLE_INCRE_CKPT && g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc == NULL) {
        incre_ckpt_pagewriter_cxt_init();
    }

    if (ENABLE_INCRE_CKPT && g_instance.ckpt_cxt_ctl->ckpt_redo_state.ckpt_rec_queue == NULL) {
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

