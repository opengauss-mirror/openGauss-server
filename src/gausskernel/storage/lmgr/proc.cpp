/* -------------------------------------------------------------------------
 *
 * proc.cpp
 *	  routines to manage per-process shared memory data structure
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/lmgr/proc.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 * Interface (a):
 *		ProcSleep(), ProcWakeup(),
 *		ProcQueueAlloc() -- create a shm queue for sleeping processes
 *		ProcQueueInit() -- create a queue without allocing memory
 *
 * Waiting for a lock causes the backend to be put to sleep.  Whoever releases
 * the lock wakes the process up again (and gives it an error code so it knows
 * whether it was awoken on an error condition).
 *
 * Interface (b):
 *
 * ProcReleaseLocks -- frees the locks associated with current transaction
 *
 * ProcKill -- destroys the shared memory state (and locks)
 * associated with the process.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#ifdef __USE_NUMA
    #include <numa.h>
#endif
#include "access/double_write.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "job/job_scheduler.h"
#include "job/job_worker.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/rbcleaner.h"
#include "replication/slot.h"
#ifdef PGXC
    #include "pgxc/pgxc.h"
    #include "pgxc/poolmgr.h"
#endif
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/spin.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "alarm/alarm.h"
#include "workload/workload.h"
#include "access/multi_redo_api.h"
#include "instruments/percentile.h"
#include "instruments/snapshot.h"
#include "instruments/instr_statement.h"
#include "utils/builtins.h"
#include "instruments/ash.h"
#include "pgaudit.h"
#ifdef ENABLE_MULTIPLE_NODES
    #include "tsdb/compaction/compaction_worker_entry.h"
#endif   /* ENABLE_MULTIPLE_NODES */

#define MAX_NUMA_NODE 16

extern THR_LOCAL uint32 *g_workingVersionNum;

static void RemoveProcFromArray(int code, Datum arg);
static void ProcKill(int code, Datum arg);
static void AuxiliaryProcKill(int code, Datum arg);
static bool CheckStatementTimeout(void);
static void CheckSessionTimeout(void);

static bool CheckStandbyTimeout(void);
static void FiniNuma(int code, Datum arg);
static inline void ReleaseChildSlot(void);

/*
 * Report shared-memory space needed by InitProcGlobal.
 */
Size ProcGlobalShmemSize(void)
{
    Size size = 0;

    /* ProcGlobal */
    size = add_size(size, sizeof(PROC_HDR));
#ifdef __aarch64__
    size = add_size(size, PG_CACHE_LINE_SIZE);
#endif
    /* MyProcs, including autovacuum workers and launcher will be alloced individually */
    size = (Size)(add_size(size, sizeof(slock_t)));

    size = (Size)(add_size(size, mul_size(g_instance.shmem_cxt.MaxBackends, sizeof(PGXACT))));
#ifdef __aarch64__
    size = add_size(size, PG_CACHE_LINE_SIZE);
#endif
    size = (Size)(add_size(size, mul_size(NUM_AUXILIARY_PROCS, sizeof(PGXACT))));
    size = (Size)(add_size(size, mul_size(NUM_TWOPHASE_PARTITIONS *
        g_instance.attr.attr_storage.max_prepared_xacts, sizeof(PGXACT))));

    return size;
}

/*
 * Report number of semaphores needed by InitProcGlobal.
 */
int ProcGlobalSemas(void)
{
    /*
     * We need a sema per backend (including autovacuum), plus one for each
     * auxiliary process.
     */
    return g_instance.shmem_cxt.MaxBackends + NUM_CMAGENT_PROCS + NUM_AUXILIARY_PROCS + NUM_DCF_CALLBACK_PROCS;
}

/*
 * Initialize NUMA related parameter.
 *
 * Compatibility with thread pool:
 * 1) allbind/cpubind: If valid in threadpool, utilize and bind cpu list in thread pool.
 * 2) nodebind: not supported for NUMA now.
 * 3) nobind: Inconsistent with NUMA. To be compatible with before, invoke numa_run_on_node
 *    in InitProcess().
 * 4) close threadpool: Invoke numa_run_on_node in InitProcess().
 *
 * Limits:
 * 1) Currently only support all of the NUMA nodes. If part of the nodes are filtered by thread pool,
 *    numa distribute will fail.
 * 2) The group num should be consistent with NUMA nodes.
 *
 */
void InitNuma(void)
{
    g_instance.numa_cxt.inheritThreadPool = false;
#ifdef __USE_NUMA
    if (strcmp(g_instance.attr.attr_common.numa_distribute_mode, "all") == 0) {
        if (numa_available() < 0) {
            ereport(FATAL, (errmsg("InitNuma NUMA is not available")));
        }

        int numaNodeNum = numa_max_node() + 1;
        if (numaNodeNum <= 1) {
            ereport(WARNING,
                    (errmsg("No multiple NUMA nodes available: %d.", numaNodeNum)));
        } else if (g_threadPoolControler) {
            if (g_threadPoolControler->CheckNumaDistribute(numaNodeNum)) {
                g_instance.shmem_cxt.numaNodeNum = numaNodeNum;
                g_instance.numa_cxt.inheritThreadPool = true;
            } else if (g_threadPoolControler->GetCpuBindType() == NO_CPU_BIND) {
                g_instance.shmem_cxt.numaNodeNum = numaNodeNum;
            } else {
                g_instance.shmem_cxt.numaNodeNum = 1;
                ereport(WARNING,
                        (errmsg("Fail to check NUMA distribute support in thread pool.")));
            }
        } else {
            g_instance.shmem_cxt.numaNodeNum = numaNodeNum;
        }
    }
#endif

    /*
     * Arrange to clean up at shmem_exit.
     */
    on_shmem_exit(FiniNuma, 0);

    ereport(LOG,
            (errmsg("InitNuma numaNodeNum: %d numa_distribute_mode: %s inheritThreadPool: %d.",
                    g_instance.shmem_cxt.numaNodeNum, g_instance.attr.attr_common.numa_distribute_mode,
                    g_instance.numa_cxt.inheritThreadPool)));
}

/*
 * Release NUMA related resources.
 */
static void FiniNuma(int code, Datum arg)
{
    ereport(LOG, (errmsg("FiniNuma allocIndex: %d.", (int)g_instance.numa_cxt.allocIndex)));

#ifdef __USE_NUMA
    for (size_t i = 0; i < g_instance.numa_cxt.allocIndex; ++i) {
        NumaMemAllocInfo& allocInfo = g_instance.numa_cxt.numaAllocInfos[i];
        numa_free(allocInfo.numaAddr, allocInfo.length);
        allocInfo.numaAddr = NULL;
        allocInfo.length = 0;
    }
    g_instance.numa_cxt.allocIndex = 0;
#endif
}

int GetThreadPoolStreamProcNum()
{
    int thread_pool_stream_thread_num = g_threadPoolControler->GetStreamThreadNum();
    float thread_pool_stream_proc_ratio = g_threadPoolControler->GetStreamProcRatio();
    int thread_pool_stream_proc_num = thread_pool_stream_thread_num * thread_pool_stream_proc_ratio;
    if (thread_pool_stream_proc_num == 0) {
        int thread_pool_thread_num = g_threadPoolControler->GetThreadNum();
        thread_pool_stream_proc_num = DEFAULT_THREAD_POOL_STREAM_PROC_RATIO * thread_pool_thread_num;
    }
    return thread_pool_stream_proc_num;
}

/*
 * InitProcGlobal -
 *	  Initialize the global process table during postmaster or standalone
 *	  backend startup.
 *
 *	  We also create all the per-process semaphores we will need to support
 *	  the requested number of backends.  We used to allocate semaphores
 *	  only when backends were actually started up, but that is bad because
 *	  it lets openGauss fail under load --- a lot of Unix systems are
 *	  (mis)configured with small limits on the number of semaphores, and
 *	  running out when trying to start another backend is a common failure.
 *	  So, now we grab enough semaphores to support the desired max number
 *	  of backends immediately at initialization --- if the sysadmin has set
 *	  MaxConnections or autovacuum_max_workers higher than his kernel will
 *	  support, he'll find out sooner rather than later.
 *
 *	  Another reason for creating semaphores here is that the semaphore
 *	  implementation typically requires us to create semaphores in the
 *	  postmaster, not in backends.
 *
 * Note: this is NOT called by individual backends under a postmaster,
 * not even in the EXEC_BACKEND case.  The ProcGlobal and &g_instance.proc_aux_base
 * pointers must be propagated specially for EXEC_BACKEND operation.
 */
void InitProcGlobal(void)
{
    PGPROC **procs = NULL;
    int i, j;
    uint32 TotalProcs = (uint32)(GLOBAL_ALL_PROCS);
    bool needPalloc = false;

    MemoryContext oldContext = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
    if (g_instance.proc_base == NULL) {
        /* Create the g_instance.proc_base shared structure */
        g_instance.proc_base = (PROC_HDR *)CACHELINEALIGN(palloc(sizeof(PROC_HDR) + PG_CACHE_LINE_SIZE));
        needPalloc = true;
    } else {
        Assert(g_instance.proc_base != NULL);
        Assert(g_instance.proc_base->allProcs != NULL);
        Assert(g_instance.proc_base->allPgXact != NULL);
        Assert(g_instance.proc_base->allProcs[0] != NULL);
    }

    /*
     * Initialize the data structures.
     */
#ifndef ENABLE_THREAD_CHECK
    g_instance.proc_base->spins_per_delay = DEFAULT_SPINS_PER_DELAY;
#endif
    g_instance.proc_base->freeProcs = NULL;
    g_instance.proc_base->externalFreeProcs = NULL;
    g_instance.proc_base->autovacFreeProcs = NULL;
    g_instance.proc_base->pgjobfreeProcs = NULL;
    g_instance.proc_base->cmAgentFreeProcs = NULL;
    g_instance.proc_base->startupProc = NULL;
    g_instance.proc_base->startupProcPid = 0;
    g_instance.proc_base->startupBufferPinWaitBufId = -1;
    g_instance.proc_base->walwriterLatch = NULL;
    g_instance.proc_base->walwriterauxiliaryLatch = NULL;
    g_instance.proc_base->checkpointerLatch = NULL;
    g_instance.proc_base->pgwrMainThreadLatch = NULL;
    g_instance.proc_base->bgworkerFreeProcs  = NULL;	
    g_instance.proc_base->cbmwriterLatch = NULL;
    g_instance.proc_base->ShareStoragexlogCopyerLatch = NULL;
    pg_atomic_init_u32(&g_instance.proc_base->procArrayGroupFirst, INVALID_PGPROCNO);
    pg_atomic_init_u32(&g_instance.proc_base->clogGroupFirst, INVALID_PGPROCNO);

    /*
     * Create and initialize all the PGPROC structures we'll need.  There are
     * four separate consumers: (1) normal backends, (2) autovacuum workers
     * and the autovacuum launcher, (3) auxiliary processes, and (4) prepared
     * transactions.  Each PGPROC structure is dedicated to exactly one of
     * these purposes, and they do not move between groups.
     */
    PGPROC *initProcs[MAX_NUMA_NODE] = {0};

    int nNumaNodes = g_instance.shmem_cxt.numaNodeNum;
    /* since myProcLocks is a various array, need palloc actrual size */
    Size actrualPgProcSize = MAXALIGN(offsetof(PGPROC, myProcLocks) + NUM_LOCK_PARTITIONS * sizeof(SHM_QUEUE)) +
       MAXALIGN(FP_LOCKBIT_NUM * sizeof(uint64)) + MAXALIGN(FP_LOCK_SLOTS_PER_BACKEND * sizeof(FastPathTag));
    Size fpLockBitsOffset = MAXALIGN(offsetof(PGPROC, myProcLocks) + NUM_LOCK_PARTITIONS * sizeof(SHM_QUEUE));
    Size fpRelIdOffset = fpLockBitsOffset + MAXALIGN(FP_LOCKBIT_NUM * sizeof(uint64));
#ifdef __USE_NUMA
    if (nNumaNodes > 1) {
        ereport(INFO, (errmsg("InitProcGlobal nNumaNodes: %d, inheritThreadPool: %d, groupNum: %d",
                              nNumaNodes, g_instance.numa_cxt.inheritThreadPool,
                              (g_threadPoolControler ? g_threadPoolControler->GetGroupNum() : 0))));

        int groupProcCount = (TotalProcs + nNumaNodes - 1) / nNumaNodes;
        size_t allocSize = groupProcCount * actrualPgProcSize;
        for (int nodeNo = 0; nodeNo < nNumaNodes; nodeNo++) {
            initProcs[nodeNo] = (PGPROC *)numa_alloc_onnode(allocSize, nodeNo);
            if (!initProcs[nodeNo]) {
                ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                                errmsg("InitProcGlobal NUMA memory allocation in node %d failed.", nodeNo)));
            }
            add_numa_alloc_info(initProcs[nodeNo], allocSize);
            int ret = memset_s(initProcs[nodeNo], allocSize, 0, allocSize);
            securec_check_c(ret, "\0", "\0");
        }
    } else {
#endif
        if (needPalloc) {
            initProcs[0] = (PGPROC *)CACHELINEALIGN(palloc0(TotalProcs * actrualPgProcSize + PG_CACHE_LINE_SIZE));
        } else {
            initProcs[0] = g_instance.proc_base->allProcs[0];
            errno_t rc = memset_s(initProcs[0], TotalProcs * actrualPgProcSize, 0, TotalProcs * actrualPgProcSize);
            securec_check(rc, "", "");
        }
        if (!initProcs[0]) {
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory")));
        }
#ifdef __USE_NUMA
    }
#endif
    if (needPalloc) {
        procs = (PGPROC **)CACHELINEALIGN(palloc0(TotalProcs * sizeof(PGPROC *) + PG_CACHE_LINE_SIZE));
        g_instance.proc_base->allProcs = procs;
    } else {
        errno_t rc = memset_s(g_instance.proc_base->allProcs, TotalProcs * sizeof(PGPROC *), 0,
            TotalProcs * sizeof(PGPROC *));
        securec_check(rc, "", "");
        procs = g_instance.proc_base->allProcs;
    }

    g_instance.proc_base->allProcCount = TotalProcs;
    g_instance.proc_base->allNonPreparedProcCount = g_instance.shmem_cxt.MaxBackends +
                                                    NUM_CMAGENT_PROCS + NUM_AUXILIARY_PROCS + 
                                                    NUM_DCF_CALLBACK_PROCS;
    if (procs == NULL)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of shared memory")));

    for (i = 0; (unsigned int)(i) < TotalProcs; i++) { /* set proc pointer to actural position */
        procs[i] = (PGPROC *)((char*)(initProcs[i % nNumaNodes]) + (i / nNumaNodes) * actrualPgProcSize);
    }

    if (needPalloc) {
        /*
        * Also allocate a separate array of PGXACT structures.  This is separate
        * from the main PGPROC array so that the most heavily accessed data is
        * stored contiguously in memory in as few cache lines as possible. This
        * provides significant performance benefits, especially on a
        * multiprocessor system.  There is one PGXACT structure for every PGPROC
        * structure.
        */
        g_instance.proc_base->allPgXact =
            (PGXACT *)CACHELINEALIGN(palloc0(TotalProcs * sizeof(PGXACT) + PG_CACHE_LINE_SIZE));
    } else {
        errno_t rc = memset_s(g_instance.proc_base->allPgXact, TotalProcs * sizeof(PGXACT), 0,
            TotalProcs * sizeof(PGXACT));
        securec_check(rc, "", "");
    }

    int thread_pool_stream_proc_num = 0;
    if (g_threadPoolControler != NULL) {
        thread_pool_stream_proc_num = GetThreadPoolStreamProcNum();
        ereport(LOG, (errmsg("Get stream thread proc num [%d].", thread_pool_stream_proc_num)));
    }

    for (i = 0; (unsigned int)(i) < TotalProcs; i++) {
        /* Common initialization for all PGPROCs, regardless of type.
         *
         * Set up per-PGPROC semaphore, latch, and backendLock. Prepared xact
         * dummy PGPROCs don't need these though - they're never associated
         * with a real process
         */
        if (i < g_instance.shmem_cxt.MaxBackends + NUM_CMAGENT_PROCS +
            NUM_AUXILIARY_PROCS + NUM_DCF_CALLBACK_PROCS) {
            PGSemaphoreCreate(&(procs[i]->sem));
            InitSharedLatch(&(procs[i]->procLatch));
            procs[i]->backendLock = LWLockAssign(LWTRANCHE_PROC);
        }
        /*
         * Starting from MaxBackends + NUM_AUXILIARY_PROCS, subxidsLock could still be used by prepared xacts.
         */
        procs[i]->subxidsLock = LWLockAssign(LWTRANCHE_PROC);

        (void)syscalllockInit(&procs[i]->deleMemContextMutex);
        procs[i]->pgprocno = i;
        procs[i]->nodeno = i % nNumaNodes;

        /*
         * Newly created PGPROCs for normal backends or for autovacuum must be
         * queued up on the appropriate free list.	Because there can only
         * ever be a small, fixed number of auxiliary processes, no free list
         * is used in that case; InitAuxiliaryProcess() instead uses a linear
         * search.	PGPROCs for prepared transactions are added to a free list
         * by TwoPhaseShmemInit().
         */
        if (i < g_instance.shmem_cxt.MaxConnections + thread_pool_stream_proc_num + AUXILIARY_BACKENDS) {
            /* PGPROC for normal backend and auxiliary backend, add to freeProcs list */
            procs[i]->links.next = (SHM_QUEUE *)g_instance.proc_base->freeProcs;
            g_instance.proc_base->freeProcs = procs[i];
        } else if (i < g_instance.shmem_cxt.MaxConnections + thread_pool_stream_proc_num + AUXILIARY_BACKENDS +
                   g_instance.attr.attr_sql.job_queue_processes + 1) {
            /* PGPROC for pg_job backend, add to pgjobfreeProcs list,  1 for Job Schedule Lancher */
            procs[i]->links.next = (SHM_QUEUE *)g_instance.proc_base->pgjobfreeProcs;
            g_instance.proc_base->pgjobfreeProcs = procs[i];
        } else if (i < g_instance.shmem_cxt.MaxConnections + thread_pool_stream_proc_num + AUXILIARY_BACKENDS +
                   g_instance.attr.attr_sql.job_queue_processes + 1 + NUM_DCF_CALLBACK_PROCS) {
            /* PGPROC for external thread, add to externalFreeProcs list */
            procs[i]->links.next = (SHM_QUEUE *)g_instance.proc_base->externalFreeProcs;
            g_instance.proc_base->externalFreeProcs = procs[i];
        } else if (i < g_instance.shmem_cxt.MaxConnections + thread_pool_stream_proc_num + AUXILIARY_BACKENDS +
                   g_instance.attr.attr_sql.job_queue_processes + 1 + NUM_DCF_CALLBACK_PROCS + NUM_CMAGENT_PROCS) {
            /*
             * This pointer indicates the first position of cm anget's procs.
             * In the first time, cmAgentFreeProcs is NULL, so procs[LAST]->links.next is NULL.
             * After NUM_CMAGENT_PROCS times, cmAgentFreeProcs is the first cm anget's procs.
             */
            procs[i]->links.next = (SHM_QUEUE*)g_instance.proc_base->cmAgentFreeProcs;
            g_instance.proc_base->cmAgentFreeProcs = procs[i];
        } else if (i < g_instance.shmem_cxt.MaxConnections + thread_pool_stream_proc_num + AUXILIARY_BACKENDS +
                   g_instance.attr.attr_sql.job_queue_processes + 1 +
                   NUM_CMAGENT_PROCS + g_max_worker_processes + NUM_DCF_CALLBACK_PROCS) {
            procs[i]->links.next = (SHM_QUEUE*)g_instance.proc_base->bgworkerFreeProcs;
            g_instance.proc_base->bgworkerFreeProcs = procs[i];
        } else if (i < g_instance.shmem_cxt.MaxBackends + NUM_CMAGENT_PROCS + NUM_DCF_CALLBACK_PROCS) {
            /*
             * PGPROC for AV launcher/worker, add to autovacFreeProcs list
             * list size is autovacuum_max_workers + AUTOVACUUM_LAUNCHERS
             */
            procs[i]->links.next = (SHM_QUEUE *)g_instance.proc_base->autovacFreeProcs;
            g_instance.proc_base->autovacFreeProcs = procs[i];
        }

        /* Initialize myProcLocks[] shared memory queues. */
        for (j = 0; j < NUM_LOCK_PARTITIONS; j++)
            SHMQueueInit(&(procs[i]->myProcLocks[j]));

        procs[i]->logictid = i;
        /* Initialize fast path slots memory */
        procs[i]->fpLockBits = (uint64*)((char*)procs[i] + fpLockBitsOffset);
        procs[i]->fpRelId = (FastPathTag*)((char*)procs[i] + fpRelIdOffset);

        /* Initialize lockGroupMembers list. */
        dlist_init(&procs[i]->lockGroupMembers);
    }

    /*
     * Save pointers to the blocks of PGPROC structures reserved for auxiliary
     * processes and prepared transactions.
     */
    g_instance.proc_aux_base = &procs[g_instance.shmem_cxt.MaxBackends +
                                      NUM_CMAGENT_PROCS + NUM_DCF_CALLBACK_PROCS];
    g_instance.proc_preparexact_base = &procs[g_instance.shmem_cxt.MaxBackends +
                                              NUM_CMAGENT_PROCS + NUM_AUXILIARY_PROCS + NUM_DCF_CALLBACK_PROCS];

    /* Create &g_instance.proc_base_lock mutexlock, too */
    pthread_mutex_init(&g_instance.proc_base_lock, NULL);

    MemoryContextSwitchTo(oldContext);
}

/*
 * GetFreeProc -- try to find the free PGPROC under the specified NUMA node
 */
PGPROC *GetFreeProc()
{
    if (!g_instance.proc_base->freeProcs) {
        return NULL;
    }

    PGPROC *current = g_instance.proc_base->freeProcs;
    if (t_thrd.threadpool_cxt.worker && g_instance.numa_cxt.inheritThreadPool) {
        int numaNodeNo = t_thrd.threadpool_cxt.worker->GetGroup()->GetNumaId();
        PGPROC *prev = NULL;
        while (current) {
            if (current->nodeno == numaNodeNo) {
                if (prev) {
                    prev->links.next = current->links.next;
                }
                break;
            }
            prev = current;
            current = (PGPROC *)current->links.next;
        }
        if (!current) {
            current = g_instance.proc_base->freeProcs;
        }
    }
    if (current && current == g_instance.proc_base->freeProcs) {
        g_instance.proc_base->freeProcs = (PGPROC *)current->links.next;
    }

    return current;
}

/*
 * If no free proc is available for cm_agent, print all thread status information.
 * Information includes application name, pid, sessionid, wait status, etc.
 */
void PgStatCMAThreadStatus()
{
    const char* appName = "cm_agent";
    MemoryContext oldContext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    /* get all threads from global status entries which name is 'cm_agent' */
    PgBackendStatusNode* result = pgstat_get_backend_status_by_appname(appName, NULL);

    if (result == NULL) {
        (void)MemoryContextSwitchTo(oldContext);
        return;
    }

    PgBackendStatusNode* tempNode = result;
    tempNode = tempNode->next;

    while (tempNode != NULL) {
        PgBackendStatus* beentry = tempNode->data;
        tempNode = tempNode->next;
        if (beentry == NULL) {
            continue;
        }

        char* wait_status = getThreadWaitStatusDesc(beentry);
        ereport(LOG, (errmsg("Print cm_agent thread information when proc is going to be not available, node_name<%s>,"
            " datid<%u>, app_name<%s>, query_id<%lu>, tid<%lu>, lwtid<%d>, parent_sessionid<%lu>, "
            "thread_level<%d>, wait_status<%s>",
            g_instance.attr.attr_common.PGXCNodeName,
            beentry->st_databaseid,
            beentry->st_appname ? beentry->st_appname : "unnamed thread",
            beentry->st_queryid,
            beentry->st_procpid,
            beentry->st_tid,
            beentry->st_parent_sessionid,
            beentry->st_thread_level,
            wait_status)));

        pfree_ext(wait_status);
    }

    /* Free node list memory */
    FreeBackendStatusNodeMemory(result);
    (void)MemoryContextSwitchTo(oldContext);
}

/*
 * GetFreeCMAgentProc -- try to find the first free CM Agent's PGPROC
 */
PGPROC* GetFreeCMAgentProc()
{
    /* sleep 100ms every time while proc has not init */
    const int sleepTime = 100000;
    const int maxRepeatTimes = 10;
    /* Set threshold value. If proc consumed number is more than threshold, send single to print thread stack */
    const float procThreshold = 0.8;
    uint32 procWarningCount = NUM_CMAGENT_PROCS * procThreshold;
    int times = 0;
    while ((g_instance.conn_cxt.CurCMAProcCount == 0) && (g_instance.proc_base->cmAgentFreeProcs == NULL)) {
        times++;
        pg_usleep(sleepTime);
        /* Check interrupt in order to reveive cancel signal and break loop */
        CHECK_FOR_INTERRUPTS();
        ereport(WARNING, (errmsg("CMA-proc list has not init completely, current %d times, total %d us period",
            times, times * sleepTime)));
        if (times >= maxRepeatTimes) {
            break;
        }
    }

    PGPROC* cmaProc = g_instance.proc_base->cmAgentFreeProcs;

    if (cmaProc != NULL) {
        (void)pg_atomic_add_fetch_u32(&g_instance.conn_cxt.CurCMAProcCount, 1);
        SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);
        ereport(DEBUG5, (errmsg("Get free proc from CMA-proc list, proc location is %p. Current proc count %d",
            cmaProc, g_instance.conn_cxt.CurCMAProcCount)));
    }

    /*
     * If proc consumed number for cm_agent is more than threshold, print all threads wait status which appname
     * equals to 'cm_agent'
     */
    if (g_instance.conn_cxt.CurCMAProcCount >= procWarningCount) {
        ereport(WARNING, (errmsg("Get free proc from CMA-proc list, proc location is %p."
            " Current proc count %d is more than threshold %d. Ready to print thread wait status",
            cmaProc, g_instance.conn_cxt.CurCMAProcCount, procWarningCount)));
        PgStatCMAThreadStatus();
    }

    return cmaProc;
}

/* Relase child slot in some cases, other role will release slot in CleanupBackend */
static inline void ReleaseChildSlot(void)
{
    if (IsUnderPostmaster && ((t_thrd.role == WLM_WORKER || t_thrd.role == WLM_MONITOR || t_thrd.role == WLM_ARBITER ||
        t_thrd.role == WLM_CPMONITOR) ||
        IsJobAspProcess() || t_thrd.role == STREAMING_BACKEND || IsStatementFlushProcess() || IsJobSnapshotProcess() ||
        t_thrd.postmaster_cxt.IsRPCWorkerThread || IsJobPercentileProcess() || t_thrd.role == ARCH ||
        IsTxnSnapCapturerProcess() || IsRbCleanerProcess() || t_thrd.role == GLOBALSTATS_THREAD ||
        t_thrd.role == BARRIER_ARCH || t_thrd.role == BARRIER_CREATOR || t_thrd.role == APPLY_LAUNCHER)) {
        (void)ReleasePostmasterChildSlot(t_thrd.proc_cxt.MyPMChildSlot);
    }
}

static void GetProcFromFreeList()
{
    if (IsAnyAutoVacuumProcess()) {
        t_thrd.proc = g_instance.proc_base->autovacFreeProcs;
    } else if (IsJobSchedulerProcess() || IsJobWorkerProcess()) {
        t_thrd.proc = g_instance.proc_base->pgjobfreeProcs;
    } else if (IsBgWorkerProcess()) {
        t_thrd.proc = g_instance.proc_base->bgworkerFreeProcs;
    } else if (t_thrd.dcf_cxt.is_dcf_thread) {
        t_thrd.proc = g_instance.proc_base->externalFreeProcs;
    } else if (u_sess->libpq_cxt.IsConnFromCmAgent) {
        t_thrd.proc = GetFreeCMAgentProc();
    } else {
#ifndef __USE_NUMA
        t_thrd.proc = g_instance.proc_base->freeProcs;
#else
        t_thrd.proc = GetFreeProc();
#endif
    }
}

/*
 * InitProcess -- initialize a per-process data structure for this backend
 */
void InitProcess(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile PROC_HDR *procglobal = g_instance.proc_base;

    /*
     * ProcGlobal should be set up already (if we are a backend, we inherit
     * this by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    if (procglobal == NULL)
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proc header uninitialized")));

    if (t_thrd.proc != NULL)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("you already exist")));

    /*
     * Initialize process-local latch support.  This could fail if the kernel
     * is low on resources, and if so we want to exit cleanly before acquiring
     * any shared-memory resources.
     */
    InitializeLatchSupport();

    /*
     * Try to get a proc struct from the free list.  If this fails, we must be
     * out of PGPROC structures (not to mention semaphores).
     *
     * While we are holding the &g_instance.proc_base_lock, also copy the current shared
     * estimate of spins_per_delay to local storage.
     */
    pthread_mutex_lock(&g_instance.proc_base_lock);
#ifndef ENABLE_THREAD_CHECK
    set_spins_per_delay(g_instance.proc_base->spins_per_delay);
#endif

    GetProcFromFreeList();

    if (t_thrd.proc != NULL) {
        t_thrd.myLogicTid = t_thrd.proc->logictid;

        if (IsAnyAutoVacuumProcess()) {
            g_instance.proc_base->autovacFreeProcs = (PGPROC*)t_thrd.proc->links.next;
        } else if (IsJobSchedulerProcess() || IsJobWorkerProcess()) {
            g_instance.proc_base->pgjobfreeProcs = (PGPROC*)t_thrd.proc->links.next;
        } else if (IsBgWorkerProcess()) {
            g_instance.proc_base->bgworkerFreeProcs = (PGPROC*)t_thrd.proc->links.next;
        } else if (t_thrd.dcf_cxt.is_dcf_thread) {
            g_instance.proc_base->externalFreeProcs = (PGPROC*)t_thrd.proc->links.next;
        } else if (u_sess->libpq_cxt.IsConnFromCmAgent) {
            g_instance.proc_base->cmAgentFreeProcs = (PGPROC *)t_thrd.proc->links.next;
        } else {
#ifndef __USE_NUMA
            g_instance.proc_base->freeProcs = (PGPROC*)t_thrd.proc->links.next;
#endif
        }

        pthread_mutex_unlock(&g_instance.proc_base_lock);
    } else {
        /*
         * If we reach here, all the PGPROCs are in use.  This is one of the
         * possible places to detect "too many backends", so give the standard
         * error message.  XXX do we need to give a different failure message
         * in the autovacuum case?
         */
        pthread_mutex_unlock(&g_instance.proc_base_lock);

        if (IsUnderPostmaster && StreamThreadAmI())
            MarkPostmasterChildUnuseForStreamWorker();

        /*
         * We have to release child slot while wlm worker process exiting, otherwise if
         * postmaster start up a new wlm worker process, old slot is still assigned to
         * old wlm worker process which has exited, if no new slot can be used while
         * postmaster starting thread, it will be throw a panic error.
         */
        ReleaseChildSlot();

        char cmaConnNumInfo[CONNINFOLEN];
        if (u_sess->libpq_cxt.IsConnFromCmAgent) {
            int rc = sprintf_s(cmaConnNumInfo, CONNINFOLEN, "All CMA proc [%d], uses[%d];",
                NUM_CMAGENT_PROCS, g_instance.conn_cxt.CurCMAProcCount);
            securec_check_ss(rc, "\0", "\0");
        }

        ereport(FATAL,
                (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                 errmsg("No free proc is available to create a new connection for %s. Please check whether the IP "
                        "address and port are available, and the CN/DN process can be connected. %s",
                        u_sess->proc_cxt.applicationName,
                        (u_sess->libpq_cxt.IsConnFromCmAgent) ? cmaConnNumInfo : "")));
    }

#ifdef __USE_NUMA
    if (g_instance.shmem_cxt.numaNodeNum > 1) {
        if (!g_instance.numa_cxt.inheritThreadPool) {
            if (-1 == numa_run_on_node(t_thrd.proc->nodeno)) {
                ereport(PANIC, (errmsg("InitProcess numa_run_on_node_mask failed. errno:%d ", errno)));
            }
        }
        numa_set_localalloc();
    }
#endif

    t_thrd.pgxact = &g_instance.proc_base_all_xacts[t_thrd.proc->pgprocno];

    if (syscalllockAcquire(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to acquire mutex lock for deleMemContextMutex.")));
    t_thrd.proc->topmcxt = t_thrd.top_mem_cxt;
    if (syscalllockRelease(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to release mutex lock for deleMemContextMutex.")));

    /*
     * Now that we have a PGPROC, mark ourselves as an active postmaster
     * child; this is so that the postmaster can detect it if we exit without
     * cleaning up.  (XXX autovac launcher currently doesn't participate in
     * this; it probably should.)
     */
    if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess() && !IsJobSchedulerProcess() &&
        !IsJobWorkerProcess() && !t_thrd.dcf_cxt.is_dcf_thread && !IsBgWorkerProcess() &&
        !IsFencedProcessingMode())
        MarkPostmasterChildActive();

    /*
     * Initialize all fields of t_thrd.proc, except for those previously
     * initialized by InitProcGlobal.
     */
    SHMQueueElemInit(&(t_thrd.proc->links));
    t_thrd.proc->waitStatus = STATUS_OK;
    t_thrd.proc->lxid = InvalidLocalTransactionId;
    t_thrd.proc->fpVXIDLock = false;
    t_thrd.proc->fpLocalTransactionId = InvalidLocalTransactionId;
    FAST_PATH_SET_LOCKBITS_ZERO(t_thrd.proc);
    t_thrd.proc->commitCSN = 0;
    t_thrd.pgxact->handle = InvalidTransactionHandle;
    t_thrd.pgxact->xid = InvalidTransactionId;
    t_thrd.pgxact->next_xid = InvalidTransactionId;
    t_thrd.pgxact->xmin = InvalidTransactionId;
    t_thrd.pgxact->csn_min = InvalidCommitSeqNo;
    t_thrd.pgxact->csn_dr = InvalidCommitSeqNo;
    t_thrd.pgxact->prepare_xid = InvalidTransactionId;
    t_thrd.proc->pid = t_thrd.proc_cxt.MyProcPid;
    /* if enable thread pool, session id will be overwritten at coupling session */
    t_thrd.proc->sessionid = (ENABLE_THREAD_POOL ? t_thrd.fake_session->session_id : t_thrd.proc_cxt.MyProcPid);
    t_thrd.proc->globalSessionId = t_thrd.fake_session->globalSessionId;
    /* backendId, databaseId and roleId will be filled in later */
    t_thrd.proc->backendId = InvalidBackendId;
    t_thrd.proc->databaseId = InvalidOid;
    t_thrd.proc->roleId = InvalidOid;
    t_thrd.proc->gtt_session_frozenxid = InvalidTransactionId; /* init session level gtt frozenxid */
    /* For backends, upgrade status is either passed down from remote backends or inherit from PM */
    t_thrd.proc->workingVersionNum = (u_sess->proc_cxt.MyProcPort ? u_sess->proc_cxt.MyProcPort->SessionVersionNum :
                                    pg_atomic_read_u32(&WorkingGrandVersionNum));
    t_thrd.pgxact->delayChkpt = false;
    t_thrd.pgxact->vacuumFlags = 0;
    t_thrd.pgxact->needToSyncXid = 0;

    /* NB -- autovac launcher intentionally does not set IS_AUTOVACUUM */
    if (IsAutoVacuumWorkerProcess())
        t_thrd.pgxact->vacuumFlags |= PROC_IS_AUTOVACUUM;
    t_thrd.proc->lwWaiting = false;
    t_thrd.proc->lwWaitMode = 0;
    t_thrd.proc->lwIsVictim = false;
    t_thrd.proc->waitLock = NULL;
    t_thrd.proc->waitProcLock = NULL;
    t_thrd.proc->blockProcLock = NULL;
    t_thrd.proc->waitLockThrd = &t_thrd;
    init_proc_dw_buf();
#ifdef USE_ASSERT_CHECKING
    if (assert_enabled) {
        int i;

        /* Last process should have released all locks. */
        for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
            Assert(SHMQueueEmpty(&(t_thrd.proc->myProcLocks[i])));
    }
#endif
    t_thrd.proc->recoveryConflictPending = false;

    /* Initialize fields for sync rep */
    t_thrd.proc->waitLSN = 0;
    t_thrd.proc->syncRepState = SYNC_REP_NOT_WAITING;
    t_thrd.proc->syncRepInCompleteQueue = false;
    SHMQueueElemInit(&(t_thrd.proc->syncRepLinks));

    /* Initialize fields for data sync rep */
    t_thrd.proc->waitDataSyncPoint.queueid = 0;
    t_thrd.proc->waitDataSyncPoint.queueoff = 0;
    t_thrd.proc->dataSyncRepState = SYNC_REP_NOT_WAITING;
    SHMQueueElemInit(&(t_thrd.proc->dataSyncRepLinks));

    /* Initialize fields for sync paxos */
    t_thrd.proc->waitPaxosLSN = 0;
    t_thrd.proc->syncPaxosState = SYNC_REP_NOT_WAITING;
    SHMQueueElemInit(&(t_thrd.proc->syncPaxosLinks));

    /* Initialize fields for group XID clearing. */
    t_thrd.proc->procArrayGroupMember = false;
    t_thrd.proc->procArrayGroupMemberXid = InvalidTransactionId;
    pg_atomic_init_u32(&t_thrd.proc->procArrayGroupNext, INVALID_PGPROCNO);

    /* Initialize fields for group transaction status update. */
    t_thrd.proc->clogGroupMember = false;
    t_thrd.proc->clogGroupMemberXid = InvalidTransactionId;
    t_thrd.proc->clogGroupMemberXidStatus = CLOG_XID_STATUS_IN_PROGRESS;
    t_thrd.proc->clogGroupMemberPage = -1;
    t_thrd.proc->clogGroupMemberLsn = InvalidXLogRecPtr;
    pg_atomic_init_u32(&t_thrd.proc->clogGroupNext, INVALID_PGPROCNO);

    /* Initialize fields for GTM */
    pg_atomic_init_u32((volatile uint32*)&t_thrd.proc->my_gtmhost, GTM_HOST_INVAILD);
    pg_atomic_init_u32(&t_thrd.proc->signal_cancel_gtm_conn_flag, 0);
    t_thrd.proc->suggested_gtmhost = GTM_HOST_INVAILD;

#ifdef __aarch64__
    /* Initialize fields for group xlog insert. */
    t_thrd.proc->xlogGroupMember = false;
    t_thrd.proc->xlogGrouprdata = NULL;
    t_thrd.proc->xlogGroupfpw_lsn = InvalidXLogRecPtr;
    t_thrd.proc->xlogGroupProcLastRecPtr = NULL;
    t_thrd.proc->xlogGroupXactLastRecEnd = NULL;
    t_thrd.proc->xlogGroupCurrentTransactionState = NULL;
    t_thrd.proc->xlogGroupRedoRecPtr = NULL;
    t_thrd.proc->xlogGroupReturntRecPtr = 0;
    t_thrd.proc->xlogGroupTimeLineID = 0;
    t_thrd.proc->xlogGroupDoPageWrites = NULL;
    t_thrd.proc->xlogGroupIsFPW = false;
    pg_atomic_init_u32(&t_thrd.proc->xlogGroupNext, INVALID_PGPROCNO);
    t_thrd.proc->snap_refcnt_bitmap = 0;
#endif

    /* Check that group locking fields are in a proper initial state. */
    Assert(t_thrd.proc->lockGroupLeader == NULL);
    Assert(dlist_is_empty(&t_thrd.proc->lockGroupMembers));

    /*
     * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch.
     * Note that there's no particular need to do ResetLatch here.
     */
    OwnLatch(&t_thrd.proc->procLatch);

    /*
     * We might be reusing a semaphore that belonged to a failed process. So
     * be careful and reinitialize its value here.	(This is not strictly
     * necessary anymore, but seems like a good idea for cleanliness.)
     */
    PGSemaphoreReset(&t_thrd.proc->sem);

    /*
     * Arrange to clean up at backend exit.
     */
    on_shmem_exit(ProcKill, 0);

    /*
     * Now that we have a PGPROC, we could try to acquire locks, so initialize
     * the deadlock checker.
     */
    InitDeadLockChecking();
}

/*
 * InitProcessPhase2 -- make t_thrd.proc visible in the shared ProcArray.
 *
 * This is separate from InitProcess because we can't acquire LWLocks until
 * we've created a PGPROC, but in the EXEC_BACKEND case ProcArrayAdd won't
 * work until after we've done CreateSharedMemoryAndSemaphores.
 */
void InitProcessPhase2(void)
{
    Assert(t_thrd.proc != NULL);

    /*
     * Add our PGPROC to the PGPROC array in shared memory.
     */
    ProcArrayAdd(t_thrd.proc);

    /*
     * Arrange to clean that up at backend exit.
     */
    on_shmem_exit(RemoveProcFromArray, 0);
}

/*
 * InitAuxiliaryProcess -- create a per-auxiliary-process data structure
 *
 * This is called by bgwriter and similar processes so that they will have a
 * t_thrd.proc value that's real enough to let them wait for LWLocks.  The PGPROC
 * and sema that are assigned are one of the extra ones created during
 * InitProcGlobal.
 *
 * Auxiliary processes are presently not expected to wait for real (lockmgr)
 * locks, so we need not set up the deadlock checker.  They are never added
 * to the ProcArray or the sinval messaging mechanism, either.	They also
 * don't get a VXID assigned, since this is only useful when we actually
 * hold lockmgr locks.
 *
 * Startup process however uses locks but never waits for them in the
 * normal backend sense. Startup process also takes part in sinval messaging
 * as a sendOnly process, so never reads messages from sinval queue. So
 * Startup process does have a VXID and does show up in pg_locks.
 */
void InitAuxiliaryProcess(void)
{
    PGPROC* auxproc = NULL;
    int proctype;

    /*
     * ProcGlobal should be set up already (if we are a backend, we inherit
     * this by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    if (g_instance.proc_base == NULL || g_instance.proc_aux_base == NULL)
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("proc header uninitialized")));

    if (t_thrd.proc != NULL)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("you already exist")));

    /*
     * Initialize process-local latch support.  This could fail if the kernel
     * is low on resources, and if so we want to exit cleanly before acquiring
     * any shared-memory resources.
     */
    InitializeLatchSupport();

    /*
     * We use the &g_instance.proc_base_lock to protect assignment and releasing of
     * &g_instance.proc_aux_base entries.
     *
     * While we are holding the &g_instance.proc_base_lock, also copy the current shared
     * estimate of spins_per_delay to local storage.
     */
    pthread_mutex_lock(&g_instance.proc_base_lock);
#ifndef ENABLE_THREAD_CHECK
    set_spins_per_delay(g_instance.proc_base->spins_per_delay);
#endif

    /*
     * Find a free auxproc ... *big* trouble if there isn't one ...
     */
    for (proctype = 0; proctype < NUM_AUXILIARY_PROCS; proctype++) {
        auxproc = g_instance.proc_aux_base[proctype];
        if (auxproc->pid == 0)
            break;
    }
    if (proctype >= NUM_AUXILIARY_PROCS) {
        pthread_mutex_unlock(&g_instance.proc_base_lock);
        ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("all &g_instance.proc_aux_base are in use")));
    }

    /* Mark auxiliary proc as in use by me */
    /* use volatile pointer to prevent code rearrangement */
    ((volatile PGPROC*)auxproc)->pid = t_thrd.proc_cxt.MyProcPid;

    t_thrd.proc = auxproc;
    t_thrd.pgxact = &g_instance.proc_base_all_xacts[auxproc->pgprocno];

    pthread_mutex_unlock(&g_instance.proc_base_lock);

    /*
     * Initialize all fields of t_thrd.proc, except for those previously
     * initialized by InitProcGlobal.
     *
     * Description:
     * At present, we initialize proc version to be WorkingGrandVersionNum
     * for auxiliary threads. This should be OK for inplace upgrade since there
     * is no need for smooth switch to newer proc version. However, when it
     * comes to online upgrade, more care should be taken.
     */
    SHMQueueElemInit(&(t_thrd.proc->links));
    t_thrd.proc->waitStatus = STATUS_OK;
    t_thrd.proc->lxid = InvalidLocalTransactionId;
    t_thrd.proc->fpVXIDLock = false;
    t_thrd.proc->fpLocalTransactionId = InvalidLocalTransactionId;
    t_thrd.pgxact->handle = InvalidTransactionHandle;
    t_thrd.pgxact->xid = InvalidTransactionId;
    t_thrd.pgxact->next_xid = InvalidTransactionId;
    t_thrd.pgxact->xmin = InvalidTransactionId;
    t_thrd.pgxact->csn_min = InvalidCommitSeqNo;
    t_thrd.pgxact->csn_dr = InvalidCommitSeqNo;
    t_thrd.proc->backendId = InvalidBackendId;
    t_thrd.proc->databaseId = InvalidOid;
    t_thrd.proc->roleId = InvalidOid;
    t_thrd.proc->gtt_session_frozenxid = InvalidTransactionId; /* init session level gtt frozenxid */
    t_thrd.pgxact->delayChkpt = false;
    t_thrd.pgxact->vacuumFlags = 0;
    t_thrd.proc->lwWaiting = false;
    t_thrd.proc->lwWaitMode = 0;
    t_thrd.proc->lwIsVictim = false;
    t_thrd.proc->waitLock = NULL;
    t_thrd.proc->waitProcLock = NULL;
    t_thrd.proc->blockProcLock = NULL;
    t_thrd.proc->waitLockThrd = &t_thrd;
    t_thrd.proc->workingVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);
    t_thrd.myLogicTid = t_thrd.proc->logictid;
    init_proc_dw_buf();

    if (syscalllockAcquire(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to acquire mutex lock for deleMemContextMutex.")));
    t_thrd.proc->topmcxt = t_thrd.top_mem_cxt;
    if (syscalllockRelease(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to release mutex lock for deleMemContextMutex.")));

    g_workingVersionNum = &t_thrd.proc->workingVersionNum;

#ifdef USE_ASSERT_CHECKING
    if (assert_enabled) {
        int i;

        /* Last process should have released all locks. */
        for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
            Assert(SHMQueueEmpty(&(t_thrd.proc->myProcLocks[i])));
    }
#endif

    /*
     * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch.
     * Note that there's no particular need to do ResetLatch here.
     */
    OwnLatch(&t_thrd.proc->procLatch);

    /* Check that group locking fields are in a proper initial state. */
    Assert(t_thrd.proc->lockGroupLeader == NULL);
    Assert(dlist_is_empty(&t_thrd.proc->lockGroupMembers));

    /*
     * We might be reusing a semaphore that belonged to a failed process. So
     * be careful and reinitialize its value here.	(This is not strictly
     * necessary anymore, but seems like a good idea for cleanliness.)
     */
    PGSemaphoreReset(&t_thrd.proc->sem);

    /*
     * Arrange to clean up at process exit.
     */
    on_shmem_exit(AuxiliaryProcKill, Int32GetDatum(proctype));
}

/* get entry index of all various stat arrays for auxiliary threads */
int GetAuxProcEntryIndex(int baseIdx)
{
    int auxType = t_thrd.bootstrap_cxt.MyAuxProcType;
    int index = 0;
    Assert(auxType > NotAnAuxProcess && auxType < NUM_AUXPROCTYPES);

    if (auxType < NUM_SINGLE_AUX_PROC) {
        index = baseIdx + auxType;
    } else {
        index = baseIdx + NUM_SINGLE_AUX_PROC;
        if (t_thrd.bootstrap_cxt.MyAuxProcType == PageWriterProcess) {
            index += get_pagewriter_thread_id();
        } else if (t_thrd.bootstrap_cxt.MyAuxProcType == PageRedoProcess) {
            index += MultiRedoGetWorkerId() + MAX_PAGE_WRITER_THREAD_NUM;
        } else if (t_thrd.bootstrap_cxt.MyAuxProcType == TpoolListenerProcess) {
            /* thread pool listerner slots follow page redo threads */
            index += t_thrd.threadpool_cxt.listener->GetGroup()->GetGroupId() +
                     MAX_PAGE_WRITER_THREAD_NUM +
                     MAX_RECOVERY_THREAD_NUM;
        }
#ifdef ENABLE_MULTIPLE_NODES
        else if (t_thrd.bootstrap_cxt.MyAuxProcType == TsCompactionConsumerProcess) {
            index += CompactionWorkerProcess::GetMyCompactionConsumerOrignId() +
                     MAX_PAGE_WRITER_THREAD_NUM +
                     MAX_RECOVERY_THREAD_NUM +
                     g_instance.shmem_cxt.ThreadPoolGroupNum;
        }
#endif /* ENABLE_MULTIPLE_NODES */
    }

    return index;
}

/*
 * Record the PID and PGPROC structures for the Startup process, for use in
 * ProcSendSignal().  See comments there for further explanation.
 */
void PublishStartupProcessInformation(void)
{
    pthread_mutex_lock(&g_instance.proc_base_lock);

    g_instance.proc_base->startupProc = t_thrd.proc;
    g_instance.proc_base->startupProcPid = t_thrd.proc_cxt.MyProcPid;

    pthread_mutex_unlock(&g_instance.proc_base_lock);
}


/*
 * Check whether there are at least N free PGPROC objects.
 *
 * Note: this is designed on the assumption that N will generally be small.
 */
bool HaveNFreeProcs(int n)
{
    PGPROC* proc = NULL;

    pthread_mutex_lock(&g_instance.proc_base_lock);

    if (u_sess->libpq_cxt.IsConnFromCmAgent) {
        proc = g_instance.proc_base->cmAgentFreeProcs;
    } else {
        proc = g_instance.proc_base->freeProcs;
    }

    while (n > 0 && proc != NULL) {
        proc = (PGPROC*)proc->links.next;
        n--;
    }

    pthread_mutex_unlock(&g_instance.proc_base_lock);

    return (n <= 0);
}

/*
 * Check if the current process is awaiting a lock.
 */
bool IsWaitingForLock(void)
{
    if (t_thrd.storage_cxt.lockAwaited == NULL)
        return false;

    return true;
}

/*
 * Cancel any pending wait for lock, when aborting a transaction, and revert
 * any strong lock count acquisition for a lock being acquired.
 *
 * (Normally, this would only happen if we accept a cancel/die
 * interrupt while waiting; but an ereport(ERROR) before or during the lock
 * wait is within the realm of possibility, too.)
 */
void LockErrorCleanup(void)
{
    LWLock* partitionLock = NULL;

    AbortStrongLockAcquire();

    /* Nothing to do if we weren't waiting for a lock */
    if (t_thrd.storage_cxt.lockAwaited == NULL)
        return;

    /* Turn off the deadlock timer, if it's still running (see ProcSleep) */
    (void)disable_sig_alarm(false);

    /* Unlink myself from the wait queue, if on it (might not be anymore!) */
    partitionLock = LockHashPartitionLock(t_thrd.storage_cxt.lockAwaited->hashcode);
    (void)LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    if (t_thrd.proc->links.next != NULL) {
        /* We could not have been granted the lock yet */
        RemoveFromWaitQueue(t_thrd.proc, t_thrd.storage_cxt.lockAwaited->hashcode);
    } else {
        /*
         * Somebody kicked us off the lock queue already.  Perhaps they
         * granted us the lock, or perhaps they detected a deadlock. If they
         * did grant us the lock, we'd better remember it in our local lock
         * table.
         */
        if (t_thrd.proc->waitStatus == STATUS_OK)
            GrantAwaitedLock();
    }

    t_thrd.storage_cxt.lockAwaited = NULL;

    LWLockRelease(partitionLock);

    /*
     * We used to do PGSemaphoreReset() here to ensure that our proc's wait
     * semaphore is reset to zero.	This prevented a leftover wakeup signal
     * from remaining in the semaphore if someone else had granted us the lock
     * we wanted before we were able to remove ourselves from the wait-list.
     * However, now that ProcSleep loops until waitStatus changes, a leftover
     * wakeup signal isn't harmful, and it seems not worth expending cycles to
     * get rid of a signal that most likely isn't there.
     */
}

/*
 * ProcReleaseLocks() -- release locks associated with current transaction
 *			at main transaction commit or abort
 *
 * At main transaction commit, we release standard locks except session locks.
 * At main transaction abort, we release all locks including session locks.
 *
 * Advisory locks are released only if they are transaction-level;
 * session-level holds remain, whether this is a commit or not.
 *
 * At subtransaction commit, we don't release any locks (so this func is not
 * needed at all); we will defer the releasing to the parent transaction.
 * At subtransaction abort, we release all locks held by the subtransaction;
 * this is implemented by retail releasing of the locks under control of
 * the ResourceOwner mechanism.
 */
void ProcReleaseLocks(bool isCommit)
{
    if (!t_thrd.proc)
        return;
    /* If waiting, get off wait queue (should only be needed after error) */
    LockErrorCleanup();
    /* Release standard locks, including session-level if aborting */
    LockReleaseAll(DEFAULT_LOCKMETHOD, !isCommit);
    /* check fastpaht bit num after release all locks */
    Check_FastpathBit();
    /* Release transaction-level advisory locks */
    LockReleaseAll(USER_LOCKMETHOD, false);
}

/*
 * RemoveProcFromArray() -- Remove this process from the shared ProcArray.
 */
static void RemoveProcFromArray(int code, Datum arg)
{
    Assert(t_thrd.proc != NULL);
    ProcArrayRemove(t_thrd.proc, InvalidTransactionId);
}

static void ProcPutBackToFreeList()
{
    /* Return PGPROC structure (and semaphore) to appropriate freelist */
    if (IsAnyAutoVacuumProcess()) {
        t_thrd.proc->links.next = (SHM_QUEUE*)g_instance.proc_base->autovacFreeProcs;
        g_instance.proc_base->autovacFreeProcs = t_thrd.proc;
    } else if (IsJobSchedulerProcess() || IsJobWorkerProcess()) {
        t_thrd.proc->links.next = (SHM_QUEUE*)g_instance.proc_base->pgjobfreeProcs;
        g_instance.proc_base->pgjobfreeProcs = t_thrd.proc;
    } else if (t_thrd.dcf_cxt.is_dcf_thread) {
        t_thrd.proc->links.next = (SHM_QUEUE *)g_instance.proc_base->externalFreeProcs;
        g_instance.proc_base->externalFreeProcs = t_thrd.proc;
    } else if (u_sess->libpq_cxt.IsConnFromCmAgent) {
        t_thrd.proc->links.next = (SHM_QUEUE*)g_instance.proc_base->cmAgentFreeProcs;
        g_instance.proc_base->cmAgentFreeProcs = t_thrd.proc;
        (void)pg_atomic_sub_fetch_u32(&g_instance.conn_cxt.CurCMAProcCount, 1);
        ereport(DEBUG5, (errmsg("Proc exit, put cm_agent to free list, current cm_agent proc count is %d",
            g_instance.conn_cxt.CurCMAProcCount)));
        if (u_sess->proc_cxt.PassConnLimit) {
            SpinLockAcquire(&g_instance.conn_cxt.ConnCountLock);
            g_instance.conn_cxt.CurCMAConnCount--;
            SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);
        }
    } else if (IsBgWorkerProcess()) {
        t_thrd.proc->links.next = (SHM_QUEUE*)g_instance.proc_base->bgworkerFreeProcs;
        g_instance.proc_base->bgworkerFreeProcs = t_thrd.proc;		
    } else {
        t_thrd.proc->links.next = (SHM_QUEUE*)g_instance.proc_base->freeProcs;
        g_instance.proc_base->freeProcs = t_thrd.proc;
        if (t_thrd.role == WORKER && u_sess->proc_cxt.PassConnLimit) {
            SpinLockAcquire(&g_instance.conn_cxt.ConnCountLock);
            g_instance.conn_cxt.CurConnCount--;
            Assert(g_instance.conn_cxt.CurConnCount >= 0);
            SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);
        }
    }
}

/*
 * ProcKill() -- Destroy the per-proc data structure for
 *		this process. Release any of its held LW locks.
 */
static void ProcKill(int code, Datum arg)
{
    Assert(t_thrd.proc != NULL);

    /* Make sure we're out of the sync rep lists */
    SyncRepCleanupAtProcExit();

    if (IsUnderPostmaster && StreamThreadAmI())
        MarkPostmasterChildUnuseForStreamWorker();

#ifdef USE_ASSERT_CHECKING
    if (assert_enabled) {
        int i;

        /* Last process should have released all locks. If not, serious problems! */
        for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
            if (!SHMQueueEmpty(&(t_thrd.proc->myProcLocks[i])))
                ereport(PANIC,
                        (errcode(ERRCODE_DATA_CORRUPTED), errmsg("there remain unreleased locks when process exists.")));
    }
#endif
    if (g_threadPoolControler) {
        g_threadPoolControler->GetSessionCtrl()->releaseLockIfNecessary();
    }
    /*
     * Release any LW locks I am holding.  There really shouldn't be any, but
     * it's cheap to check again before we cut the knees off the LWLock
     * facility by releasing our PGPROC ...
     */
    LWLockReleaseAll();

    /* Make sure active replication slots are released */
    if (t_thrd.slot_cxt.MyReplicationSlot != NULL)
        ReplicationSlotRelease();
    /*
     * Detach from any lock group of which we are a member.  If the leader
     * exist before all other group members, it's PGPROC will remain allocated
     * until the last group process exits; that process must return the
     * leader's PGPROC to the appropriate list.
     */
    if (t_thrd.proc->lockGroupLeader != NULL) {
        PGPROC *leader = t_thrd.proc->lockGroupLeader;
        LWLock *leaderLwlock = LockHashPartitionLockByProc(leader);

        LWLockAcquire(leaderLwlock, LW_EXCLUSIVE);
        Assert(!dlist_is_empty(&leader->lockGroupMembers));
        dlist_delete(&t_thrd.proc->lockGroupLink);
        if (dlist_is_empty(&leader->lockGroupMembers)) {
            leader->lockGroupLeader = NULL;
            if (leader != t_thrd.proc) {
                /* The leader must be the last one. */
                ereport(PANIC, (errmsg("The bgworker exits last, it can't happen")));
            }
        } else if (leader != t_thrd.proc) {
            t_thrd.proc->lockGroupLeader = NULL;
        } else if (t_thrd.role == WORKER && u_sess->proc_cxt.PassConnLimit) {
            SpinLockAcquire(&g_instance.conn_cxt.ConnCountLock);
            g_instance.conn_cxt.CurConnCount--;
            Assert(g_instance.conn_cxt.CurConnCount >= 0);
            SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);
        }
        LWLockRelease(leaderLwlock);
    }

    /* Release ownership of the process's latch, too */
    DisownLatch(&t_thrd.proc->procLatch);


    /* clean this proc structure */
    if (syscalllockAcquire(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to acquire mutex lock for deleMemContextMutex.")));
    t_thrd.proc->topmcxt = NULL;
    if (syscalllockRelease(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to release mutex lock for deleMemContextMutex.")));

    clean_proc_dw_buf();
    /* Clean subxid cache if needed. */
    ProcSubXidCacheClean();
    pthread_mutex_lock(&g_instance.proc_base_lock);

    /*
     * If we're still a member of a locking group, that means we're a leader
     * which has somehow exited before its children.  The last remaining child
     * will release our PGPROC.  Otherwise, release it now.
     */
    if (t_thrd.proc->lockGroupLeader == NULL) {
        /* Since lockGroupLeader is NULL, lockGroupMembers should be empty. */
        Assert(dlist_is_empty(&t_thrd.proc->lockGroupMembers));
        ProcPutBackToFreeList();
    }

    if (u_sess->libpq_cxt.IsConnFromCmAgent) {
        ereport(DEBUG5, (errmsg("Return PGPROC structure (and semaphore) to CMA-proc list.")));
    }

    errno_t rc = memset_s(t_thrd.proc->myProgName, sizeof(t_thrd.proc->myProgName), 0, sizeof(t_thrd.proc->myProgName));
    securec_check(rc, "", "");

    /* PGPROC struct isn't mine anymore */
    t_thrd.proc = NULL;

#ifndef ENABLE_THREAD_CHECK
    /* Update shared estimate of spins_per_delay */
    g_instance.proc_base->spins_per_delay = update_spins_per_delay(g_instance.proc_base->spins_per_delay);
#endif

    pthread_mutex_unlock(&g_instance.proc_base_lock);

    /*
     * This process is no longer present in shared memory in any meaningful
     * way, so tell the postmaster we've cleaned up acceptably well. (XXX
     * autovac launcher should be included here someday)
     */
    if (IsUnderPostmaster && !IsAutoVacuumLauncherProcess() && !StreamThreadAmI() && !IsJobSchedulerProcess() &&
        !IsJobWorkerProcess() && !IsBgWorkerProcess())
        MarkPostmasterChildInactive();

    /*
     * We have to release child slot while wlm worker process exiting, otherwise if
     * postmaster start up a new wlm worker process, old slot is still assigned to
     * old wlm worker process which has exited, if no new slot can be used while
     * postmaster starting thread, it will be throw a panic error.
     */
    ReleaseChildSlot();

    /* wake autovac launcher if needed -- see comments in FreeWorkerInfo */
    if (t_thrd.autovacuum_cxt.AutovacuumLauncherPid != 0)
        gs_signal_send(t_thrd.autovacuum_cxt.AutovacuumLauncherPid, SIGUSR2);

    /*
     * instrument snapshot clean
     */
    instrSnapshotClean();
}

/*
 * AuxiliaryProcKill() -- Cut-down version of ProcKill for auxiliary
 *		processes (bgwriter, etc).	The PGPROC and sema are not released, only
 *		marked as not-in-use.
 */
static void AuxiliaryProcKill(int code, Datum arg)
{
    int proctype = DatumGetInt32(arg);
    PGPROC PG_USED_FOR_ASSERTS_ONLY *auxproc = NULL;

    Assert(proctype >= 0 && proctype < NUM_AUXILIARY_PROCS);

    auxproc = g_instance.proc_aux_base[proctype];

    Assert(t_thrd.proc == auxproc);

    /* Release any LW locks I am holding (see notes above) */
    LWLockReleaseAll();

    /* Release ownership of the process's latch, too */
    DisownLatch(&t_thrd.proc->procLatch);

    if (syscalllockAcquire(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to acquire mutex lock for deleMemContextMutex.")));
    t_thrd.proc->topmcxt = NULL;
    if (syscalllockRelease(&t_thrd.proc->deleMemContextMutex) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to release mutex lock for deleMemContextMutex.")));

    clean_proc_dw_buf();

    pthread_mutex_lock(&g_instance.proc_base_lock);

    /* Mark auxiliary proc no longer in use */
    t_thrd.proc->pid = 0;

    errno_t rc = memset_s(t_thrd.proc->myProgName, sizeof(t_thrd.proc->myProgName), 0, sizeof(t_thrd.proc->myProgName));
    securec_check(rc, "", "");

    /* PGPROC struct isn't mine anymore */
    t_thrd.proc = NULL;

#ifndef ENABLE_THREAD_CHECK
    /* Update shared estimate of spins_per_delay */
    g_instance.proc_base->spins_per_delay = update_spins_per_delay(g_instance.proc_base->spins_per_delay);
#endif

    pthread_mutex_unlock(&g_instance.proc_base_lock);
}

/*
 * ProcQueue package: routines for putting processes to sleep
 *		and  waking them up
 *
 * ProcQueueAlloc -- alloc/attach to a shared memory process queue
 *
 * Returns: a pointer to the queue
 * Side Effects: Initializes the queue if it wasn't there before
 */
#ifdef NOT_USED
PROC_QUEUE* ProcQueueAlloc(const char* name)
{
    PROC_QUEUE* queue = NULL;
    bool found = false;

    queue = (PROC_QUEUE*)ShmemInitStruct(name, sizeof(PROC_QUEUE), &found);

    if (!found)
        ProcQueueInit(queue);

    return queue;
}
#endif

/*
 * ProcQueueInit -- initialize a shared memory process queue
 */
void ProcQueueInit(PROC_QUEUE* queue)
{
    SHMQueueInit(&(queue->links));
    queue->size = 0;
}

/*
 * @Description: cancel all autovacuum workers which block this relation
 * @Param[IN] lock: lock info
 * @Param[IN] lockmode: lock mode
 * @Param[OUT]is_vac_wraparound: if the PROC_VACUUM_FOR_WRAPAROUND flag has
 *			  been set, return true, otherwise, return false.
 * @See also:
 */
static void CancelAllBlockedAutovacWorkers(LOCK* lock, LOCKMODE lockmode)
{
    PGPROC* autovac = NULL;
    PGXACT* autovac_pgxact = NULL;

    /* get the first blocker */
    autovac = GetBlockingAutoVacuumPgproc();

    /* Compared with PG code, here we expand the time of ProcArrayLock locking */
    (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    while (autovac != NULL) {
        autovac_pgxact = &g_instance.proc_base_all_xacts[autovac->pgprocno];
        if ((autovac_pgxact->vacuumFlags & PROC_IS_AUTOVACUUM)) {
            ThreadId pid = autovac->pid;
            StringInfoData locktagbuf;
            StringInfoData logbuf; /* errdetail for server log */

            initStringInfo(&locktagbuf);
            initStringInfo(&logbuf);
            DescribeLockTag(&locktagbuf, &lock->tag);
            appendStringInfo(&logbuf, _("Process %lu waits for %s on %s."),
                             t_thrd.proc_cxt.MyProcPid,
                             GetLockmodeName(lock->tag.locktag_lockmethodid, lockmode),
                             locktagbuf.data);

            ereport(LOG,
                    (errmsg("sending cancel to blocking autovacuum PID %lu", pid), errdetail_log("%s", logbuf.data)));

            pfree(logbuf.data);
            logbuf.data = NULL;
            pfree(locktagbuf.data);
            locktagbuf.data = NULL;

            /* send the autovacuum worker Back to Old Kent Road */
            if (gs_signal_send(pid, SIGINT) < 0) {
                /* Just a warning to allow multiple callers */
                ereport(WARNING, (errmsg("could not send signal to process %lu: %m", pid)));
            }
        }

        /* get the next blocker */
        autovac = GetBlockingAutoVacuumPgproc();
    }

    LWLockRelease(ProcArrayLock);

    return;
}

/*
 * @Description: cancel data redistribution worker which block this relation
 * @Param[IN] lock: lock info
 * @Param[IN] lockmode: lock mode
 * @See also:
 */
void CancelBlockedRedistWorker(LOCK* lock, LOCKMODE lockmode)
{
    PGPROC* redist = NULL;
    PGXACT* redist_pgxact = NULL;

    redist = GetBlockingRedistributionPgproc();

    /* Compared with PG code, here we expand the time of ProcArrayLock locking */
    (void)LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    if (redist != NULL) {
        redist_pgxact = &g_instance.proc_base_all_xacts[redist->pgprocno];
        if (redist_pgxact->vacuumFlags & PROC_IS_REDIST) {
            ThreadId pid = redist->pid;
            StringInfoData locktagbuf;
            StringInfoData logbuf; /* errdetail for server log */

            initStringInfo(&locktagbuf);
            initStringInfo(&logbuf);
            DescribeLockTag(&locktagbuf, &lock->tag);
            appendStringInfo(&logbuf, _("Process %lu waits for %s on %s."),
                             t_thrd.proc_cxt.MyProcPid,
                             GetLockmodeName(lock->tag.locktag_lockmethodid, lockmode),
                             locktagbuf.data);

            ereport(LOG, (errmsg("sending cancel to blocking redistribution PID %lu", pid),
                          errdetail_log("%s", logbuf.data)));

            pfree(logbuf.data);
            logbuf.data = NULL;
            pfree(locktagbuf.data);
            locktagbuf.data = NULL;

            /* send the redistribution worker Back to Old Kent Road */
            if (gs_signal_send(pid, SIGINT) < 0) {
                /* Just a warning to allow multiple callers */
                ereport(WARNING, (errmsg("could not send signal to process %lu: %m", pid)));
            }
        }
    }

    LWLockRelease(ProcArrayLock);
}

/*
 * ProcSleep -- put a process to sleep on the specified lock
 *
 * Caller must have set t_thrd.proc->heldLocks to reflect locks already held
 * on the lockable object by this process (under all XIDs).
 *
 * The lock table's partition lock must be held at entry, and will be held
 * at exit.
 *
 * Result: STATUS_OK if we acquired the lock, STATUS_ERROR if not (deadlock).
 *
 * ASSUME: that no one will fiddle with the queue until after
 *		we release the partition lock.
 *
 * NOTES: The process queue is now a priority queue for locking.
 *
 * P() on the semaphore should put us to sleep.  The process
 * semaphore is normally zero, so when we try to acquire it, we sleep.
 */
int ProcSleep(LOCALLOCK* locallock, LockMethod lockMethodTable, bool allow_con_update, int waitSec)
{
    LOCKMODE lockmode = locallock->tag.mode;
    LOCK* lock = locallock->lock;
    PROCLOCK* proclock = locallock->proclock;
    uint32 hashcode = locallock->hashcode;
    LWLock* partitionLock = LockHashPartitionLock(hashcode);
    PROC_QUEUE* waitQueue = &(lock->waitProcs);
    LOCKMASK myHeldLocks = t_thrd.proc->heldLocks;
    bool early_deadlock = false;
    bool allow_autovacuum_cancel = true;
    int myWaitStatus;
    PGPROC *proc = NULL;
    PGPROC *leader = t_thrd.proc->lockGroupLeader;
    int i;

    /*
     * If group locking is in use, locks held by members of my locking group
     * need to be included in myHeldLocks.
     */
    if (leader != NULL) {
        SHM_QUEUE  *procLocks = &(lock->procLocks);
        PROCLOCK   *otherproclock;

        otherproclock = (PROCLOCK *)
        SHMQueueNext(procLocks, procLocks, offsetof(PROCLOCK, lockLink));
        while (otherproclock != NULL) {
            if (otherproclock->groupLeader == leader) {
                myHeldLocks |= otherproclock->holdMask;
            }
            otherproclock = (PROCLOCK *)SHMQueueNext(procLocks, &otherproclock->lockLink, offsetof(PROCLOCK, lockLink));
        }
    }

    /*
     * Determine where to add myself in the wait queue.
     *
     * Normally I should go at the end of the queue.  However, if I already
     * hold locks that conflict with the request of any previous waiter, put
     * myself in the queue just in front of the first such waiter. This is not
     * a necessary step, since deadlock detection would move me to before that
     * waiter anyway; but it's relatively cheap to detect such a conflict
     * immediately, and avoid delaying till deadlock timeout.
     *
     * Special case: if I find I should go in front of some waiter, check to
     * see if I conflict with already-held locks or the requests before that
     * waiter.	If not, then just grant myself the requested lock immediately.
     * This is the same as the test for immediate grant in LockAcquire, except
     * we are only considering the part of the wait queue before my insertion
     * point.
     */
    if (myHeldLocks != 0) {
        LOCKMASK aheadRequests = 0;

        proc = (PGPROC*)waitQueue->links.next;
        for (i = 0; i < waitQueue->size; i++) {
            /*
             * If we're part of the same locking group as this waiter, its
             * locks neither conflict with ours nor contribute to
             * aheadRequests.
             */
            if (leader != NULL && leader == proc->lockGroupLeader) {
                proc = (PGPROC *) proc->links.next;
                continue;
            }
            /* Must he wait for me? */
            if (lockMethodTable->conflictTab[proc->waitLockMode] & myHeldLocks) {
                /* Must I wait for him ? */
                if (lockMethodTable->conflictTab[lockmode] & proc->heldLocks) {
                    /*
                     * Yes, so we have a deadlock.	Easiest way to clean up
                     * correctly is to call RemoveFromWaitQueue(), but we
                     * can't do that until we are *on* the wait queue. So, set
                     * a flag to check below, and break out of loop.  Also,
                     * record deadlock info for later message.
                     */
                    RememberSimpleDeadLock(t_thrd.proc, lockmode, lock, proc);
                    early_deadlock = true;
                    break;
                }
                /* I must go before this waiter.  Check special case. */
                if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
                    LockCheckConflicts(lockMethodTable, lockmode, lock, proclock, t_thrd.proc) == STATUS_OK) {
                    /* Skip the wait and just grant myself the lock. */
                    GrantLock(lock, proclock, lockmode);
                    GrantAwaitedLock();
                    return STATUS_OK;
                }
                /* Break out of loop to put myself before him */
                break;
            }
            /* Nope, so advance to next waiter */
            aheadRequests |= LOCKBIT_ON((unsigned int)proc->waitLockMode);
            proc = (PGPROC*)proc->links.next;
        }

        /*
         * If we fall out of loop normally, proc points to waitQueue head, so
         * we will insert at tail of queue as desired.
         */
    } else {
        /* I hold no locks, so I can't push in front of anyone. */
        proc = (PGPROC*)&(waitQueue->links);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if ((StreamTopConsumerAmI() || StreamThreadAmI()) &&
        LockCheckConflicts(lockMethodTable, lockmode, lock, proclock, t_thrd.proc) == STATUS_OK) {
        /* Skip the wait and just grant myself the lock. */
        GrantLock(lock, proclock, lockmode);
        GrantAwaitedLock();
        return STATUS_OK;
    }
#endif

    if (t_thrd.proc->waitStatus == STATUS_WAITING) {
        Assert(false);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("could not insert into waitQueue if exists.")));
    }

    /*
     * Insert self into queue, ahead of the given proc (or at tail of queue).
     */
    SHMQueueInsertBefore(&(proc->links), &(t_thrd.proc->links));
    waitQueue->size++;

    lock->waitMask |= LOCKBIT_ON((unsigned int)lockmode);

    /* Set up wait information in PGPROC object, too */
    t_thrd.proc->waitLock = lock;
    t_thrd.proc->waitProcLock = proclock;
    t_thrd.proc->waitLockMode = lockmode;

    t_thrd.proc->waitStatus = STATUS_WAITING;

    /*
     * If we detected deadlock, give up without waiting.  This must agree with
     * CheckDeadLock's recovery code, except that we shouldn't release the
     * semaphore since we haven't tried to lock it yet.
     */
    if (early_deadlock) {
        RemoveFromWaitQueue(t_thrd.proc, hashcode);
        return STATUS_ERROR;
    }

    /* mark that we are waiting for a lock */
    t_thrd.storage_cxt.lockAwaited = locallock;

    /*
     * Release the lock table's partition lock.
     *
     * NOTE: this may also cause us to exit critical-section state, possibly
     * allowing a cancel/die interrupt to be accepted. This is OK because we
     * have recorded the fact that we are waiting for a lock, and so
     * LockErrorCleanup will clean up if cancel/die happens.
     */
    LWLockRelease(partitionLock);

    /*
     * Also, now that we will successfully clean up after an ereport, it's
     * safe to check to see if there's a buffer pin deadlock against the
     * Startup process.  Of course, that's only necessary if we're doing Hot
     * Standby and are not the Startup process ourselves.
     */
    if (RecoveryInProgress() && !t_thrd.xlog_cxt.InRecovery)
        CheckRecoveryConflictDeadlock();

    /* Reset deadlock_state before enabling the signal handler */
    t_thrd.storage_cxt.deadlock_state = DS_NOT_YET_CHECKED;

    /* enlarge the deadlock-check timeout if needed. */
    int deadLockTimeout = !t_thrd.storage_cxt.EnlargeDeadlockTimeout ? u_sess->attr.attr_storage.DeadlockTimeout
                        : u_sess->attr.attr_storage.DeadlockTimeout * 3;

    /*
     * Set timer so we can wake up after awhile and check for a deadlock. If a
     * deadlock is detected, the handler releases the process's semaphore and
     * sets t_thrd.proc->waitStatus = STATUS_ERROR, allowing us to know that we
     * must report failure rather than success.
     *
     * By delaying the check until we've waited for a bit, we can avoid
     * running the rather expensive deadlock-check code in most cases.
     */
    if (!enable_sig_alarm(deadLockTimeout, false))
        ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("could not set timer for process wakeup")));

    /*
     * If someone wakes us between LWLockRelease and PGSemaphoreLock,
     * PGSemaphoreLock will not block.	The wakeup is "saved" by the semaphore
     * implementation.	While this is normally good, there are cases where a
     * saved wakeup might be leftover from a previous operation (for example,
     * we aborted ProcWaitForSignal just before someone did ProcSendSignal).
     * So, loop to wait again if the waitStatus shows we haven't been granted
     * nor denied the lock yet.
     *
     * We pass interruptOK = true, which eliminates a window in which
     * cancel/die interrupts would be held off undesirably.  This is a promise
     * that we don't mind losing control to a cancel/die interrupt here.  We
     * don't, because we have no shared-state-change work to do after being
     * granted the lock (the grantor did it all).  We do have to worry about
     * updating the locallock table, but if we lose control to an error,
     * LockErrorCleanup will fix that up.
     */
    do {
        PGSemaphoreLock(&t_thrd.proc->sem, true);

        /*
         * waitStatus could change from STATUS_WAITING to something else
         * asynchronously.	Read it just once per loop to prevent surprising
         * behavior (such as missing log messages).
         */
        myWaitStatus = t_thrd.proc->waitStatus;

        /*
         * If we are not deadlocked, but are waiting on an autovacuum-induced
         * task, send a signal to interrupt it. If I am an autovacuum worker, don't
         * send a signal to inerrutpt.
         *
         * If we are not deadlocked, but are waiting on a data redistribution
         * -induced task, send a signal to interrupt it. No extra enable_cluster_resize
         * check is needed, since DS_BLOCKED_BY_REDISTRIBUTION can only be set
         * by blocking proc with enable_cluster_resize on.
         */
        if (t_thrd.storage_cxt.deadlock_state == DS_BLOCKED_BY_AUTOVACUUM && allow_autovacuum_cancel &&
            !IsAutoVacuumWorkerProcess()) {
            /* cancle all existing blockers */
            CancelAllBlockedAutovacWorkers(lock, lockmode);

            /* prevent signal from being resent more than once */
            allow_autovacuum_cancel = false;
        } else if (t_thrd.storage_cxt.deadlock_state == DS_BLOCKED_BY_REDISTRIBUTION) {
            /*
             * We only cancel blocked redistribution thread when redistribution_cancelable
             * flag is on, otherwise we reset the thread local variable
             * blocking_redistribution_proc to prevent the following scenario from
             * happening.
             *
             * thread 1:
             * set enable_cluster_resize=on;
             * START TRANSACTION;
             * select pg_enable_redis_proc_cancelable();
             * LOCK TABLE t1 IN ACCESS SHARE MODE;
             *
             * thread 2:
             * truncate t1; <-- waiting due to LOCK TABLE t1
             * ctrl+c to cancel it
             * truncate t1; <-- causing thread 1 exits even when t1 is not in append_mode
             *
             * The first truncate detects redistribution thread blocks t1 and set
             * blocking_redistribution_proc and return DS_BLOCKED_BY_REDISTRIBUTION, however
             * the blocking_redistribution_proc should be reset if object is not in append_mode
             * so the global variable of blocking_redistribution_proc will not affect the very
             * next truncate/drop statement.
             */
            if (u_sess->catalog_cxt.redistribution_cancelable) {
                CancelBlockedRedistWorker(lock, lockmode);

                u_sess->catalog_cxt.redistribution_cancelable = false;
            } else {
                t_thrd.storage_cxt.blocking_redistribution_proc = NULL;
            }
        }

        /*
         * If awoken after the deadlock check interrupt has run, and
         * log_lock_waits is on, then report about the wait.
         */
        if ((u_sess->attr.attr_storage.log_lock_waits && t_thrd.storage_cxt.deadlock_state != DS_NOT_YET_CHECKED) ||
            (t_thrd.storage_cxt.deadlock_state == DS_LOCK_TIMEOUT)) {
            StringInfoData buf;
            const char* modename = NULL;
            long secs;
            int usecs;
            long msecs;

            initStringInfo(&buf);
            DescribeLockTag(&buf, &locallock->tag.lock);
            modename = GetLockmodeName(locallock->tag.lock.locktag_lockmethodid, lockmode);
            TimestampDifference(t_thrd.storage_cxt.timeout_start_time, GetCurrentTimestamp(), &secs, &usecs);
            msecs = secs * 1000 + usecs / 1000;
            usecs = usecs % 1000;

            if (t_thrd.storage_cxt.deadlock_state == DS_SOFT_DEADLOCK) {
                ereport(LOG,
                        (errmsg("thread %lu avoided deadlock for %s on %s by rearranging queue order after %ld.%03d ms",
                                t_thrd.proc_cxt.MyProcPid,
                                modename,
                                buf.data,
                                msecs,
                                usecs)));
            } else if (t_thrd.storage_cxt.deadlock_state == DS_HARD_DEADLOCK) {
                /*
                 * This message is a bit redundant with the error that will be
                 * reported subsequently, but in some cases the error report
                 * might not make it to the log (eg, if it's caught by an
                 * exception handler), and we want to ensure all long-wait
                 * events get logged.
                 */
                ereport(LOG,
                        (errmsg("thread %lu detected deadlock while waiting for %s on %s after %ld.%03d ms",
                                t_thrd.proc_cxt.MyProcPid,
                                modename,
                                buf.data,
                                msecs,
                                usecs)));
            }

            if (myWaitStatus == STATUS_WAITING) {
                ereport(LOG,
                        (errmsg("thread %lu still waiting for %s on %s after %ld.%03d ms",
                                t_thrd.proc_cxt.MyProcPid,
                                modename,
                                buf.data,
                                msecs,
                                usecs)));
            } else if (myWaitStatus == STATUS_OK) {
                ereport(LOG,
                        (errmsg("thread %lu acquired %s on %s after %ld.%03d ms",
                                t_thrd.proc_cxt.MyProcPid,
                                modename,
                                buf.data,
                                msecs,
                                usecs)));
            } else {
                Assert(myWaitStatus == STATUS_ERROR);

                /*
                 * Currently, the deadlock checker always kicks its own
                 * process, which means that we'll only see STATUS_ERROR when
                 * deadlock_state == DS_HARD_DEADLOCK, and there's no need to
                 * print redundant messages.  But for completeness and
                 * future-proofing, print a message if it looks like someone
                 * else kicked us off the lock.
                 */
                if (t_thrd.storage_cxt.deadlock_state != DS_HARD_DEADLOCK) {
                    ereport(LOG, (errmsg("thread %lu failed to acquire %s on %s after %ld.%03d ms",
                                         t_thrd.proc_cxt.MyProcPid, modename, buf.data, msecs, usecs)));
                }
            }

            /* ereport when we reach lock wait timeout to avoid distributed deadlock. */
            if (t_thrd.storage_cxt.deadlock_state == DS_LOCK_TIMEOUT) {
                if (waitSec > 0) {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("could not obtain lock on row in relation,waitSec = %d", waitSec)));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_WAIT_TIMEOUT),
                                (errmsg("Lock wait timeout: thread %lu on node %s waiting for %s on %s after %ld.%03d ms",
                                        t_thrd.proc_cxt.MyProcPid, g_instance.attr.attr_common.PGXCNodeName, modename, buf.data, msecs,
                                        usecs),
                                 errdetail("blocked by%sthread %lu, statement <%s>,%slockmode %s.",
                                            t_thrd.storage_cxt.conflicting_lock_by_holdlock ? " hold lock " : " lock requested waiter ",
                                            t_thrd.storage_cxt.conflicting_lock_thread_id,
                                            t_thrd.storage_cxt.conflicting_lock_thread_id == (ThreadId)0 ?
                                            "pending twophase transaction" :
                                            pgstat_get_backend_current_activity(t_thrd.storage_cxt.conflicting_lock_thread_id, false),
                                            t_thrd.storage_cxt.conflicting_lock_by_holdlock ? " hold " : " requested ",
                                            t_thrd.storage_cxt.conflicting_lock_mode_name))));
                }
            }

            /*
             * At this point we might still need to wait for the lock. Reset
             * state so we don't print the above messages again.
             */
            t_thrd.storage_cxt.deadlock_state = DS_NO_DEADLOCK;

            pfree(buf.data);
            buf.data = NULL;
        }

        /*
         * Set timer so we can wake up after awhile and check for a lock acquire
         * time out. If time out, ereport and abort current transaction.
         */
        int needWaitTime = Max(1000, (allow_con_update ? u_sess->attr.attr_storage.LockWaitUpdateTimeout :
                               u_sess->attr.attr_storage.LockWaitTimeout) - u_sess->attr.attr_storage.DeadlockTimeout);
        if (waitSec > 0) {
            needWaitTime =Max(1, (waitSec * 1000) - u_sess->attr.attr_storage.DeadlockTimeout);
        }

        if (myWaitStatus == STATUS_WAITING && u_sess->attr.attr_storage.LockWaitTimeout > 0 && 
            t_thrd.storage_cxt.deadlock_timeout_active == false) {
            if (!enable_lockwait_sig_alarm(needWaitTime)) {
                ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not set timer for process wakeup")));
            }
        }
    } while (myWaitStatus == STATUS_WAITING);

    /*
     * Disable the timer, if it's still running
     */
    if (!disable_sig_alarm(false))
        ereport(FATAL, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("could not disable timer for process wakeup")));

    /*
     * Re-acquire the lock table's partition lock.  We have to do this to hold
     * off cancel/die interrupts before we can mess with lockAwaited (else we
     * might have a missed or duplicated locallock update).
     */
    (void)LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * We no longer want LockErrorCleanup to do anything.
     */
    t_thrd.storage_cxt.lockAwaited = NULL;

    /*
     * If we got the lock, be sure to remember it in the locallock table.
     */
    if (t_thrd.proc->waitStatus == STATUS_OK)
        GrantAwaitedLock();

    /*
     * We don't have to do anything else, because the awaker did all the
     * necessary update of the lock table and t_thrd.proc.
     */
    return t_thrd.proc->waitStatus;
}

/*
 * ProcWakeup -- wake up a process by releasing its private semaphore.
 *
 *	 Also remove the process from the wait queue and set its links invalid.
 *	 RETURN: the next process in the wait queue.
 *
 * The appropriate lock partition lock must be held by caller.
 *
 * XXX: presently, this code is only used for the "success" case, and only
 * works correctly for that case.  To clean up in failure case, would need
 * to twiddle the lock's request counts too --- see RemoveFromWaitQueue.
 * Hence, in practice the waitStatus parameter must be STATUS_OK.
 */
PGPROC* ProcWakeup(PGPROC* proc, int waitStatus)
{
    PGPROC* retProc = NULL;

    /* Proc should be sleeping ... */
    if (proc->links.prev == NULL || proc->links.next == NULL)
        return NULL;
    Assert(proc->waitStatus == STATUS_WAITING);

    /* Save next process before we zap the list link */
    retProc = (PGPROC*)proc->links.next;

    /* Remove process from wait queue */
    SHMQueueDelete(&(proc->links));
    (proc->waitLock->waitProcs.size)--;

    /* Clean up process' state and pass it the ok/fail signal */
    proc->waitLock = NULL;
    proc->waitProcLock = NULL;
    proc->waitStatus = waitStatus;

    /* And awaken it */
    PGSemaphoreUnlock(&proc->sem);

    return retProc;
}

/* 
 * ProcBlockerUpdate - udpate lock waiter's block information
 *
 * waiterProc:      the lock appliant's PGPROC
 * blockerProcLock: the blocker's PROCLOCK
 * lockMode:        lockmode of the lock held by blocker
 * isLockHolder:    blocker is holder or waiter.
 */
void ProcBlockerUpdate(PGPROC *waiterProc,
    PROCLOCK *blockerProcLock, const char* lockMode, bool isLockHolder)
{
    knl_thrd_context *pThrd = (knl_thrd_context*)waiterProc->waitLockThrd;

    Assert(pThrd != NULL);
    pThrd->storage_cxt.conflicting_lock_by_holdlock = isLockHolder;
    pThrd->storage_cxt.conflicting_lock_mode_name = lockMode;
    pThrd->storage_cxt.conflicting_lock_thread_id = blockerProcLock->tag.myProc->pid;

    waiterProc->blockProcLock = blockerProcLock;
}

/*
 * ProcLockWakeup -- routine for waking up processes when a lock is
 *		released (or a prior waiter is aborted).  Scan all waiters
 *		for lock, waken any that are no longer blocked.
 *
 * The appropriate lock partition lock must be held by caller.
 */
void ProcLockWakeup(LockMethod lockMethodTable, LOCK* lock, const PROCLOCK* proclock)
{
    PROC_QUEUE* waitQueue = &(lock->waitProcs);
    int queue_size = waitQueue->size;
    PGPROC* proc = NULL;
    LOCKMASK aheadRequests = 0;

    Assert(queue_size >= 0);

    if (queue_size == 0) {
        return;
    }

    proc = (PGPROC*)waitQueue->links.next;

    while (queue_size-- > 0) {
        int status = STATUS_OK;
        LOCKMODE lockmode = proc->waitLockMode;
        PROCLOCK* oldBlockProcLock = proc->blockProcLock;

        /*
         * Waken if (a) doesn't conflict with requests of earlier waiters, and
         * (b) doesn't conflict with already-held locks.
         */
        if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
            (status = LockCheckConflicts(lockMethodTable, lockmode, lock, proc->waitProcLock, proc)) == STATUS_OK) {
            /* OK to waken */
            GrantLock(lock, proc->waitProcLock, lockmode);
            proc = ProcWakeup(proc, STATUS_OK);

            /*
             * ProcWakeup removes proc from the lock's waiting process queue
             * and returns the next proc in chain; don't use proc's next-link,
             * because it's been cleared.
             */
        } else {
            /*
             * When status is STATUS_OK, it means there are some conflict between this lock waiter
             * with ahead ones. If it was related to the released PROCLOCK, or the whole wait queue
             * was rearranged by dead lock checker, try to find out this waiter's new blocker.
             */
            if (status == STATUS_OK && (proclock == NULL || (proclock == proc->blockProcLock &&
                !(proclock->holdMask & lockMethodTable->conflictTab[lockmode])))) {
                PGPROC* curProc = (PGPROC*)waitQueue->links.next;

                /* traverse the whole queue to find the first conflicter who blocks this proc. */
                for (int j = 0; j < waitQueue->size; j++) {
                    if (lockMethodTable->conflictTab[lockmode] & LOCKBIT_ON((unsigned int)curProc->waitLockMode)) {
                        break;
                    }
                    curProc = (PGPROC*)curProc->links.next;
                }

                /* above must be a normal break. */
                Assert(curProc != NULL);

                /* refresh waiter's with new blocker information. */
                ProcBlockerUpdate(proc, curProc->waitProcLock,
                    lockMethodTable->lockModeNames[curProc->waitLockMode], false);
                status = STATUS_FOUND;
            }

            if (status == STATUS_FOUND && oldBlockProcLock != proc->blockProcLock) {
                /* report new blocking session id into PgBackendStatus. */
                pgstat_report_blocksid(proc->waitLockThrd,
                    proc->blockProcLock->tag.myProc->sessionid);
            }

            /*
             * Cannot wake this guy. Remember his request for later checks.
             */
            aheadRequests |= LOCKBIT_ON((unsigned int)lockmode);
            proc = (PGPROC*)proc->links.next;
        }
    }

    Assert(waitQueue->size >= 0);
}

/*
 * CheckDeadLock
 *
 * We only get to this routine if we got SIGALRM after DeadlockTimeout
 * while waiting for a lock to be released by some other process.  Look
 * to see if there's a deadlock; if not, just return and continue waiting.
 * (But signal ProcSleep to log a message, if log_lock_waits is true.)
 * If we have a real deadlock, remove ourselves from the lock's wait queue
 * and signal an error to ProcSleep.
 *
 * NB: this is run inside a signal handler, so be very wary about what is done
 * here or in called routines.
 */
static void CheckDeadLock(void)
{
    int i;

    /*
     * Acquire exclusive lock on the entire shared lock data structures. Must
     * grab LWLocks in partition-number order to avoid LWLock deadlock.
     *
     * Note that the deadlock check interrupt had better not be enabled
     * anywhere that this process itself holds lock partition locks, else this
     * will wait forever.  Also note that LWLockAcquire creates a critical
     * section, so that this routine cannot be interrupted by cancel/die
     * interrupts.
     */
    for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
        (void)LWLockAcquire(GetMainLWLockByIndex(FirstLockMgrLock + i), LW_EXCLUSIVE);

    /*
     * Check to see if we've been awoken by anyone in the interim.
     *
     * If we have, we can return and resume our transaction -- happy day.
     * Before we are awoken the process releasing the lock grants it to us so
     * we know that we don't have to wait anymore.
     *
     * We check by looking to see if we've been unlinked from the wait queue.
     * This is quicker than checking our semaphore's state, since no kernel
     * call is needed, and it is safe because we hold the lock partition lock.
     */
    if (t_thrd.proc->links.prev == NULL || t_thrd.proc->links.next == NULL)
        goto check_done;

#ifdef LOCK_DEBUG
    if (u_sess->attr.attr_storage.Debug_deadlocks)
        DumpAllLocks();
#endif

    /* Run the deadlock check, and set deadlock_state for use by ProcSleep */
    t_thrd.storage_cxt.deadlock_state = DeadLockCheck(t_thrd.proc);

    if (t_thrd.storage_cxt.deadlock_state == DS_HARD_DEADLOCK) {
        /*
         * Oops.  We have a deadlock.
         *
         * Get this process out of wait state. (Note: we could do this more
         * efficiently by relying on lockAwaited, but use this coding to
         * preserve the flexibility to kill some other transaction than the
         * one detecting the deadlock.)
         *
         * RemoveFromWaitQueue sets t_thrd.proc->waitStatus to STATUS_ERROR, so
         * ProcSleep will report an error after we return from the signal
         * handler.
         */
        Assert(t_thrd.proc->waitLock != NULL);
        RemoveFromWaitQueue(t_thrd.proc, LockTagHashCode(&(t_thrd.proc->waitLock->tag)));

        /*
         * Unlock my semaphore so that the interrupted ProcSleep() call can
         * finish.
         */
        PGSemaphoreUnlock(&t_thrd.proc->sem);

        /*
         * We're done here.  Transaction abort caused by the error that
         * ProcSleep will raise will cause any other locks we hold to be
         * released, thus allowing other processes to wake up; we don't need
         * to do that here.  NOTE: an exception is that releasing locks we
         * hold doesn't consider the possibility of waiters that were blocked
         * behind us on the lock we just failed to get, and might now be
         * wakable because we're not in front of them anymore.  However,
         * RemoveFromWaitQueue took care of waking up any such processes.
         */
    } else if (u_sess->attr.attr_storage.log_lock_waits ||
               t_thrd.storage_cxt.deadlock_state == DS_BLOCKED_BY_AUTOVACUUM ||
               t_thrd.storage_cxt.deadlock_state == DS_BLOCKED_BY_REDISTRIBUTION) {
        /*
         * Unlock my semaphore so that the interrupted ProcSleep() call can
         * print the log message (we daren't do it here because we are inside
         * a signal handler).  It will then sleep again until someone releases
         * the lock.
         *
         * If blocked by autovacuum or data redistribution, this wakeup will
         * enable ProcSleep to send the canceling signal to the autovacuum worker
         * or the redistribution proc.
         *
         */
        PGSemaphoreUnlock(&t_thrd.proc->sem);
    } else if (u_sess->attr.attr_storage.LockWaitTimeout > 0) {
        /*
         * Unlock my semaphore so that interrupt the ProcSleep() call and
         * Enable the SIGALRM interrupt for lock wait timeout.
         */
        PGSemaphoreUnlock(&t_thrd.proc->sem);
    }

    /*
     * And release locks.  We do this in reverse order for two reasons: (1)
     * Anyone else who needs more than one of the locks will be trying to lock
     * them in increasing order; we don't want to release the other process
     * until it can get all the locks it needs. (2) This avoids O(N^2)
     * behavior inside LWLockRelease.
     */
check_done:
    for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
        LWLockRelease(GetMainLWLockByIndex(FirstLockMgrLock + i));
}

/*
 * ProcWaitForSignal - wait for a signal from another backend.
 *
 * This can share the semaphore normally used for waiting for locks,
 * since a backend could never be waiting for a lock and a signal at
 * the same time.  As with locks, it's OK if the signal arrives just
 * before we actually reach the waiting state.	Also as with locks,
 * it's necessary that the caller be robust against bogus wakeups:
 * always check that the desired state has occurred, and wait again
 * if not.	This copes with possible "leftover" wakeups.
 */
void ProcWaitForSignal(void)
{
    PGSemaphoreLock(&t_thrd.proc->sem, true);
}

/*
 * ProcSendSignal - send a signal to a backend identified by PID
 */
void ProcSendSignal(ThreadId pid)
{
    PGPROC* proc = NULL;

    if (RecoveryInProgress()) {
        pthread_mutex_lock(&g_instance.proc_base_lock);

        /*
         * Check to see whether it is the Startup process we wish to signal.
         * This call is made by the buffer manager when it wishes to wake up a
         * process that has been waiting for a pin in so it can obtain a
         * cleanup lock using LockBufferForCleanup(). Startup is not a normal
         * backend, so BackendPidGetProc() will not return any pid at all. So
         * we remember the information for this special case.
         */
        proc = MultiRedoThreadPidGetProc(pid);

        pthread_mutex_unlock(&g_instance.proc_base_lock);
    }

    if (proc == NULL)
        proc = BackendPidGetProc(pid);

    if (proc != NULL)
        PGSemaphoreUnlock(&proc->sem);
}

/*****************************************************************************
 * SIGALRM interrupt support
 *
 * Maybe these should be in pqsignal.c?
 *****************************************************************************/
/*
 * Enable the SIGALRM interrupt to fire after the specified delay
 *
 * Delay is given in milliseconds.	Caller should be sure a SIGALRM
 * signal handler is installed before this is called.
 *
 * This code properly handles nesting of deadlock timeout alarms within
 * statement timeout alarms.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool enable_sig_alarm(int delayms, bool is_statement_timeout)
{
    TimestampTz fin_time;
    struct itimerval timeval;
    errno_t ret = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
    securec_check(ret, "\0", "\0");

    if (is_statement_timeout) {
        /*
         * Begin statement-level timeout
         *
         * Note that we compute statement_fin_time with reference to the
         * statement_timestamp, but apply the specified delay without any
         * correction; that is, we ignore whatever time has elapsed since
         * statement_timestamp was set.  In the normal case only a small
         * interval will have elapsed and so this doesn't matter, but there
         * are corner cases (involving multi-statement query strings with
         * embedded COMMIT or ROLLBACK) where we might re-initialize the
         * statement timeout long after initial receipt of the message. In
         * such cases the enforcement of the statement timeout will be a bit
         * inconsistent.  This annoyance is judged not worth the cost of
         * performing an additional gettimeofday() here.
         */
        Assert(!t_thrd.storage_cxt.deadlock_timeout_active);
        fin_time = GetCurrentStatementLocalStartTimestamp();
        fin_time = TimestampTzPlusMilliseconds(fin_time, delayms);
        t_thrd.storage_cxt.statement_fin_time = fin_time;
        t_thrd.storage_cxt.cancel_from_timeout = false;
        t_thrd.storage_cxt.statement_timeout_active = true;
    } else if (t_thrd.storage_cxt.statement_timeout_active) {
        /*
         * Begin deadlock timeout with statement-level timeout active
         *
         * Here, we want to interrupt at the closer of the two timeout times.
         * If fin_time >= statement_fin_time then we need not touch the
         * existing timer setting; else set up to interrupt at the deadlock
         * timeout time.
         *
         * NOTE: in this case it is possible that this routine will be
         * interrupted by the previously-set timer alarm.  This is okay
         * because the signal handler will do only what it should do according
         * to the state variables.	The deadlock checker may get run earlier
         * than normal, but that does no harm.
         */
        t_thrd.storage_cxt.timeout_start_time = GetCurrentTimestamp();
        fin_time = TimestampTzPlusMilliseconds(t_thrd.storage_cxt.timeout_start_time, delayms);
        if (fin_time >= t_thrd.storage_cxt.statement_fin_time)
            return true;
        t_thrd.storage_cxt.deadlock_timeout_active = true;
    } else {
        /* Begin deadlock timeout with no statement-level timeout */
        t_thrd.storage_cxt.deadlock_timeout_active = true;
        /* GetCurrentTimestamp can be expensive, so only do it if we must */
        if (u_sess->attr.attr_storage.log_lock_waits || u_sess->attr.attr_storage.LockWaitTimeout > 0)
            t_thrd.storage_cxt.timeout_start_time = GetCurrentTimestamp();
    }

    /* If we reach here, okay to set the timer interrupt */
    timeval.it_value.tv_sec = delayms / 1000;
    timeval.it_value.tv_usec = (delayms % 1000) * 1000;
    if (gs_signal_settimer(&timeval))
        return false;
    return true;
}

bool enable_lockwait_sig_alarm(int delayms)
{
    TimestampTz fin_time;
    struct itimerval timeval;
    errno_t ret = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
    securec_check(ret, "\0", "\0");

    /* It can reach here if ProcSleep interrupted */
    if (t_thrd.storage_cxt.lockwait_timeout_active)
        return true;

    if (t_thrd.storage_cxt.statement_timeout_active) {
        /*
         * Begin lockwait timeout with statement-level timeout active
         *
         * Here, we want to interrupt at the closer of the two timeout times.
         * If fin_time >= statement_fin_time then we need not touch the
         * existing timer setting; else set up to interrupt at the deadlock
         * timeout time.
         */
        t_thrd.storage_cxt.timeout_start_time = GetCurrentTimestamp();
        fin_time = TimestampTzPlusMilliseconds(t_thrd.storage_cxt.timeout_start_time, delayms);
        if (fin_time >= t_thrd.storage_cxt.statement_fin_time)
            return true;
        t_thrd.storage_cxt.lockwait_timeout_active = true;
    } else {
        /* Begin lock wait timeout with no statement-level timeout */
        t_thrd.storage_cxt.lockwait_timeout_active = true;
        /* GetCurrentTimestamp can be expensive, so only do it if we must */
        if (u_sess->attr.attr_storage.log_lock_waits)
            t_thrd.storage_cxt.timeout_start_time = GetCurrentTimestamp();
    }

    /* If we reach here, okay to set the timer interrupt */
    timeval.it_value.tv_sec = delayms / 1000;
    timeval.it_value.tv_usec = (delayms % 1000) * 1000;
    if (gs_signal_settimer(&timeval))
        return false;
    return true;
}

/* Enable the session timeout timer. */
bool enable_session_sig_alarm(int delayms)
{
    TimestampTz fin_time;
    struct itimerval timeval;
    errno_t rc = EOK;

    /* Session timer only work when connect form app, not work for inner conncection. */
    if (!IsConnFromApp() || IS_THREAD_POOL_STREAM) {
        return true;
    }
    /* disable session timeout */
    if (u_sess->attr.attr_common.SessionTimeout == 0) {
        return true;
    }
    int delay_time_ms = delayms;
    /* Set session_timeout flag true. */
    u_sess->storage_cxt.session_timeout_active = true;

    /* Calculate session timeout finish time. */
    fin_time = GetCurrentTimestamp();
    fin_time = TimestampTzPlusMilliseconds(fin_time, delay_time_ms);
    u_sess->storage_cxt.session_fin_time = fin_time;

    /*
     * threadpool session status must be UNINIT,ATTACH,DEATCH...
     * KNL_SESS_FAKE only use in thread preinit
     * if a new session is UNINIT
     * 		if attached, t_thrd is valid.
     * 		if not attach, t_thrd is invalid
     * so we user USESS_STATUS to check again
     * */
    if (IS_THREAD_POOL_WORKER || u_sess->status != KNL_SESS_FAKE) {
        return true;
    }
    /* Now we set the timer to interrupt. */
    rc = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
    securec_check(rc, "\0", "\0");
    timeval.it_value.tv_sec = delay_time_ms / 1000;
    timeval.it_value.tv_usec = (delay_time_ms % 1000) * 1000;
    if (gs_signal_settimer(&timeval))
        return false;

    return true;
}

/* Disable the session timeout timer. */
bool disable_session_sig_alarm(void)
{
    if (IS_THREAD_POOL_WORKER) {
        u_sess->storage_cxt.session_timeout_active = false;
        return true;
    }
    /* Session timer only work when connect from app, not work for inner conncection. */
    if (!IsConnFromApp()) {
        return true;
    }

    /* already disable */
    if (!u_sess->attr.attr_common.SessionTimeout && \
        (!u_sess->storage_cxt.session_timeout_active)) {
        return true;
    }
    /*
     * Always disable the timer if it is active; this is for guarantee the
     * atomicity of session timeout timer.
     */
    if (u_sess->storage_cxt.session_timeout_active && !gs_signal_canceltimer()) {
        u_sess->storage_cxt.session_timeout_active = false;
        return true;
    } else {
        return false;
    }
}

/*
 * Cancel the SIGALRM timer, either for a deadlock timeout or a statement
 * timeout.  If a deadlock timeout is canceled, any active statement timeout
 * remains in force.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool disable_sig_alarm(bool is_statement_timeout)
{
    /*
     * Always disable the interrupt if it is active; this avoids being
     * interrupted by the signal handler and thereby possibly getting
     * confused.
     *
     * We will re-enable the interrupt if necessary in CheckStatementTimeout.
     */
    if (t_thrd.storage_cxt.statement_timeout_active || t_thrd.storage_cxt.deadlock_timeout_active ||
        t_thrd.storage_cxt.lockwait_timeout_active || t_thrd.wlm_cxt.wlmalarm_timeout_active) {
        if (gs_signal_canceltimer()) {
            t_thrd.storage_cxt.statement_timeout_active = false;
            t_thrd.storage_cxt.cancel_from_timeout = false;
            t_thrd.storage_cxt.deadlock_timeout_active = false;
            t_thrd.storage_cxt.lockwait_timeout_active = false;
            t_thrd.wlm_cxt.wlmalarm_timeout_active = false;
            return false;
        }
    }

    /* Always cancel deadlock timeout, in case this is error cleanup */
    t_thrd.storage_cxt.deadlock_timeout_active = false;
    t_thrd.storage_cxt.lockwait_timeout_active = false;
    t_thrd.wlm_cxt.wlmalarm_timeout_active = false;

    /* Cancel or reschedule statement timeout */
    if (is_statement_timeout) {
        t_thrd.storage_cxt.statement_timeout_active = false;
        t_thrd.storage_cxt.cancel_from_timeout = false;
    } else if (t_thrd.storage_cxt.statement_timeout_active) {
        if (!CheckStatementTimeout()) {
            return false;
        }
    }
    return true;
}

/*
 * Pause the SIGALRM timer.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool pause_sig_alarm(bool is_statement_timeout)
{
    /* t_thrd.storage_cxt.timeIsPausing is true means time counter has already been paused */
    if (t_thrd.storage_cxt.timeIsPausing == true)
        return true;

    struct itimerspec restime;
    /* Save rest time for future resume */
    if (timer_gettime(t_thrd.utils_cxt.sigTimerId, /* the created timer */
        &restime)) {                                /* rest time will save into */
            return false;
    }

    /* Convert itimerval into TimestampTz */
    t_thrd.storage_cxt.restimems = restime.it_value.tv_sec * 1000 + restime.it_value.tv_nsec / 1000000ULL;

    t_thrd.storage_cxt.timeIsPausing = true;
    /* Stop time counter */
    return disable_sig_alarm(is_statement_timeout);
}

/*
 * Resume the SIGALRM timer.
 *
 * Returns TRUE if okay, FALSE on failure.
 */
bool resume_sig_alarm(bool is_statement_timeout)
{
    /*
     * The time counter was not paused before if t_thrd.utils_cxt.timeIsPausing is false.
     * You should not invoke resume_sig_alarm here.
     */
    Assert(t_thrd.storage_cxt.timeIsPausing == true);

    if (enable_sig_alarm(t_thrd.storage_cxt.restimems, is_statement_timeout)) {
        t_thrd.storage_cxt.timeIsPausing = false;
        return true;
    }

    return false;
}

static bool RescheduleInterrupt(const TimestampTz& now)
{
    /* Not time yet, so (re)schedule the interrupt */
    long secs = 0L;
    int usecs = 0;
    struct itimerval timeval;
    errno_t ret = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
    securec_check(ret, "\0", "\0");

    TimestampDifference(now, t_thrd.storage_cxt.statement_fin_time, &secs, &usecs);

    /*
     * It's possible that the difference is less than a microsecond;
     * ensure we don't cancel, rather than set, the interrupt.
     */
    if (secs == 0 && usecs == 0) {
        usecs = 1;
    }
    timeval.it_value.tv_sec = secs;
    timeval.it_value.tv_usec = usecs;
    if (gs_signal_settimer(&timeval)) {
        return false;
    }
    return true;
}

/*
 * Check for statement timeout.  If the timeout time has come,
 * trigger a query-cancel interrupt; if not, reschedule the SIGALRM
 * interrupt to occur at the right time.
 *
 * Returns true if okay, false if failed to set the interrupt.
 */
static bool CheckStatementTimeout(void)
{
    TimestampTz now;

    if (!t_thrd.storage_cxt.statement_timeout_active) {
        return true; /* do nothing if not active */
    }

    now = GetCurrentTimestamp();
    if (now >= t_thrd.storage_cxt.statement_fin_time) {
        /* Time to die */
        t_thrd.storage_cxt.statement_timeout_active = false;
        t_thrd.storage_cxt.cancel_from_timeout = true;
        (void)gs_signal_send(t_thrd.proc_cxt.MyProcPid, SIGINT);
    } else {
        return RescheduleInterrupt(now);
    }

    return true;
}

/*
 * Check for session timeout.  If the timeout time has come,
 * trigger a session-cancel interrupt;
 */
static void CheckSessionTimeout(void)
{
    if (IS_THREAD_POOL_WORKER) {
        return;
    }
    TimestampTz now = GetCurrentTimestamp();
    if (now >= u_sess->storage_cxt.session_fin_time) {
        u_sess->storage_cxt.session_timeout_active = false;
        if (u_sess->attr.attr_common.SessionTimeout) {
            ereport(WARNING, (errmsg("Session unused timeout.")));
            (void)gs_signal_canceltimer();
            (void)gs_signal_send(t_thrd.proc_cxt.MyProcPid, SIGTERM);
        }
    }
}

/*
 * Check for wlm alarm timeout.  If the timeout time has come,
 * trigger a wlm exception handler interrupt;
 */
static void CheckWLMAlarmTimeout(void)
{
    TimestampTz now = GetCurrentTimestamp();
    if (t_thrd.wlm_cxt.wlmalarm_timeout_active && now >= t_thrd.wlm_cxt.wlmalarm_fin_time) {
        t_thrd.wlm_cxt.wlmalarm_timeout_active = false;

        if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_PENDING) {
            t_thrd.wlm_cxt.collect_info->attribute = WLM_ATTR_INDB;
            t_thrd.wlm_cxt.collect_info->sdetail.msg = WLMGetExceptWarningMsg(WLM_EXCEPT_QUEUE_TIMEOUT);

            ereport(WARNING, (errmsg("%s", t_thrd.wlm_cxt.collect_info->sdetail.msg)));

            /* If the query is waiting in the queue, it will be aborted while this timer triggered. */
            t_thrd.storage_cxt.cancel_from_timeout = true;
            t_thrd.wlm_cxt.wlmalarm_pending = false;

            (void)gs_signal_send(t_thrd.proc_cxt.MyProcPid, SIGINT);
        } else {
            InterruptPending = true;
            t_thrd.wlm_cxt.wlmalarm_pending = true;
        }
    }
}

/*
 * Signal handler for SIGALRM for normal user backends
 *
 * Process deadlock check and/or statement timeout check, as needed.
 * To avoid various edge cases, we must be careful to do nothing
 * when there is nothing to be done.  We also need to be able to
 * reschedule the timer interrupt if called before end of statement.
 */
void handle_sig_alarm(SIGNAL_ARGS)
{
    int save_errno = errno;

    /* SIGALRM is cause for waking anything waiting on the process latch */
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    if (u_sess->storage_cxt.session_timeout_active) {
        CheckSessionTimeout();
    }

    if (ENABLE_WORKLOAD_CONTROL) {
        CheckWLMAlarmTimeout();
    }

    if (t_thrd.storage_cxt.deadlock_timeout_active) {
        t_thrd.storage_cxt.deadlock_timeout_active = false;
        CheckDeadLock();
    }

    if (t_thrd.storage_cxt.statement_timeout_active)
        (void)CheckStatementTimeout();

    if (t_thrd.storage_cxt.lockwait_timeout_active) {
        t_thrd.storage_cxt.deadlock_state = DS_LOCK_TIMEOUT;
        if (t_thrd.proc != NULL) {
            PGSemaphoreUnlock(&t_thrd.proc->sem);
        }
    }

    errno = save_errno;
}

/*
 * Signal handler for SIGALRM in Startup process
 *
 * To avoid various edge cases, we must be careful to do nothing
 * when there is nothing to be done.  We also need to be able to
 * reschedule the timer interrupt if called before end of statement.
 *
 * We set either deadlock_timeout_active or statement_timeout_active
 * or both. Interrupts are enabled if standby_timeout_active.
 */
bool enable_standby_sig_alarm(TimestampTz now, TimestampTz fin_time, bool deadlock_only)
{
    TimestampTz deadlock_time = TimestampTzPlusMilliseconds(now, u_sess->attr.attr_storage.DeadlockTimeout);

    if (deadlock_only) {
        /*
         * Wake up at deadlock_time only, then wait forever
         */
        t_thrd.storage_cxt.statement_fin_time = deadlock_time;
        t_thrd.storage_cxt.deadlock_timeout_active = true;
        t_thrd.storage_cxt.statement_timeout_active = false;
    } else if (fin_time > deadlock_time) {
        /*
         * Wake up at deadlock_time, then again at fin_time
         */
        t_thrd.storage_cxt.statement_fin_time = deadlock_time;
        t_thrd.storage_cxt.statement_fin_time2 = fin_time;
        t_thrd.storage_cxt.deadlock_timeout_active = true;
        t_thrd.storage_cxt.statement_timeout_active = true;
    } else {
        /*
         * Wake only at fin_time because its fairly soon
         */
        t_thrd.storage_cxt.statement_fin_time = fin_time;
        t_thrd.storage_cxt.deadlock_timeout_active = false;
        t_thrd.storage_cxt.statement_timeout_active = true;
    }

    if (t_thrd.storage_cxt.deadlock_timeout_active || t_thrd.storage_cxt.statement_timeout_active) {
        if (!RescheduleInterrupt(now)) {
            return false;
        }
        t_thrd.storage_cxt.standby_timeout_active = true;
    }

    return true;
}

bool disable_standby_sig_alarm(void)
{
    /*
     * Always disable the interrupt if it is active; this avoids being
     * interrupted by the signal handler and thereby possibly getting
     * confused.
     *
     * We will re-enable the interrupt if necessary in CheckStandbyTimeout.
     */
    if (t_thrd.storage_cxt.standby_timeout_active) {
        struct itimerval timeval;

        errno_t ret = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
        securec_check(ret, "\0", "\0");
        if (gs_signal_settimer(&timeval)) {
            t_thrd.storage_cxt.standby_timeout_active = false;
            return false;
        }
    }

    t_thrd.storage_cxt.standby_timeout_active = false;

    return true;
}

/*
 * CheckStandbyTimeout() runs unconditionally in the Startup process
 * SIGALRM handler. Timers will only be set when InHotStandby.
 * We simply ignore any signals unless the timer has been set.
 */
static bool CheckStandbyTimeout(void)
{
    TimestampTz now;
    bool reschedule = false;

    t_thrd.storage_cxt.standby_timeout_active = false;

    now = GetCurrentTimestamp();
    /*
     * Reschedule the timer if its not time to wake yet, or if we have both
     * timers set and the first one has just been reached.
     */
    if (now >= t_thrd.storage_cxt.statement_fin_time) {
        if (t_thrd.storage_cxt.deadlock_timeout_active) {
            /*
             * We're still waiting when we reach deadlock timeout, so send out
             * a request to have other backends check themselves for deadlock.
             * Then continue waiting until statement_fin_time, if that's set.
             */
            SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);
            t_thrd.storage_cxt.deadlock_timeout_active = false;

            /*
             * Begin second waiting period if required.
             */
            if (t_thrd.storage_cxt.statement_timeout_active) {
                reschedule = true;
                t_thrd.storage_cxt.statement_fin_time = t_thrd.storage_cxt.statement_fin_time2;
            }
        } else {
            /*
             * We've now reached statement_fin_time, so ask all conflicts to
             * leave, so we can press ahead with applying changes in recovery.
             */
            SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);
        }
    } else
        reschedule = true;

    if (reschedule) {
        long secs;
        int usecs;
        struct itimerval timeval;

        TimestampDifference(now, t_thrd.storage_cxt.statement_fin_time, &secs, &usecs);
        if (secs == 0 && usecs == 0) {
            usecs = 1;
        }
        errno_t ret = memset_s(&timeval, sizeof(struct itimerval), 0, sizeof(struct itimerval));
        securec_check(ret, "\0", "\0");
        timeval.it_value.tv_sec = secs;
        timeval.it_value.tv_usec = usecs;
        if (gs_signal_settimer(&timeval))
            return false;
        t_thrd.storage_cxt.standby_timeout_active = true;
    }

    return true;
}

void handle_standby_sig_alarm(SIGNAL_ARGS)
{
    int save_errno = errno;

    if (t_thrd.storage_cxt.standby_timeout_active)
        (void)CheckStandbyTimeout();

    errno = save_errno;
}

ThreadId getThreadIdFromLogicThreadId(int logictid)
{
    ThreadId tid = 0;

    if ((logictid >= 0) && ((unsigned int)logictid < g_instance.proc_base->allProcCount)) {
        tid = g_instance.proc_base_all_procs[logictid]->pid;
    }

    /* If logic thread id is invalid, return 0 */
    return tid;
}

ThreadId getThreadIdForLibcomm(int logictid)
{
    return getThreadIdFromLogicThreadId(logictid);
}

/* get logic thread id from thread id */
int getLogicThreadIdFromThreadId(ThreadId tid)
{
    int i = 0;

    for (i = 1; (uint32)i < (g_instance.proc_base->allProcCount); i++) {
        if (tid == (g_instance.proc_base_all_procs[i]->pid)) {
            return i;
        }
    }
    return 0;
}

TimestampTz GetStatementFinTime()
{
    return t_thrd.storage_cxt.statement_fin_time;
}

AlarmCheckResult ConnectionOverloadChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam)
{
#ifdef PGXC
    if (!IS_PGXC_COORDINATOR) {
        return ALM_ACR_UnKnown;
    }
#endif

    int connectionLimit =
        int(u_sess->attr.attr_common.ConnectionAlarmRate * g_instance.shmem_cxt.MaxConnections);
    SpinLockAcquire(&g_instance.conn_cxt.ConnCountLock);
    int currentConnections = g_instance.conn_cxt.CurConnCount;
    SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);

    if (currentConnections > connectionLimit) {
        /* fill the alarm message */
        WriteAlarmAdditionalInfo(additionalParam,
                                 g_instance.attr.attr_common.PGXCNodeName,
                                 "",
                                 "",
                                 alarm,
                                 ALM_AT_Fault,
                                 g_instance.attr.attr_common.PGXCNodeName,
                                 connectionLimit);
        return ALM_ACR_Abnormal;
    } else {
        /* fill the alarm message */
        WriteAlarmAdditionalInfo(
            additionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarm, ALM_AT_Resume);
        return ALM_ACR_Normal;
    }
}

/*
 * BecomeLockGroupLeader - designate process as lock group leader
 *
 * Once this function has returned, other processes can join the lock group
 * by calling BecomeLockGroupMember.
 */
void BecomeLockGroupLeader(void)
{
    LWLock *leaderLwlock;

    /* If we already did it, we don't need to do it again. */
    if (t_thrd.proc->lockGroupLeader == t_thrd.proc)
        return;

    /* We had better not be a follower. */
    Assert(t_thrd.proc->lockGroupLeader == NULL);

    /* Create single-member group, containing only ourselves. */
    leaderLwlock = LockHashPartitionLockByProc(t_thrd.proc);
    LWLockAcquire(leaderLwlock, LW_EXCLUSIVE);
    t_thrd.proc->lockGroupLeader = t_thrd.proc;
    dlist_push_head(&t_thrd.proc->lockGroupMembers, &t_thrd.proc->lockGroupLink);
    LWLockRelease(leaderLwlock);
}

/*
 * BecomeLockGroupMember - designate process as lock group member
 *
 * This is pretty straightforward except for the possibility that the leader
 * whose group we're trying to join might exit before we manage to do so;
 * and the PGPROC might get recycled for an unrelated process.  To avoid
 * that, we require the caller to pass the PID of the intended PGPROC as
 * an interlock.  Returns true if we successfully join the intended lock
 * group, and false if not.
 */
void BecomeLockGroupMember(PGPROC *leader)
{
    LWLock *leaderLwlock;

    /* Group leader can't become member of group */
    Assert(t_thrd.proc != leader);

    /* Can't already be a member of a group */
    Assert(t_thrd.proc->lockGroupLeader == NULL);

    /*
     * Get lock protecting the group fields.  Note LockHashPartitionLockByProc
     * accesses leader->pgprocno in a PGPROC that might be free.  This is safe
     * because all PGPROCs' pgprocno fields are set during shared memory
     * initialization and never change thereafter; so we will acquire the
     * correct lock even if the leader PGPROC is in process of being recycled.
     */
    leaderLwlock = LockHashPartitionLockByProc(leader);
    LWLockAcquire(leaderLwlock, LW_EXCLUSIVE);

    /* Is this the leader we're looking for? */
    Assert(leader->lockGroupLeader == leader);
    /* OK, join the group */
    t_thrd.proc->lockGroupLeader = leader;
    dlist_push_tail(&leader->lockGroupMembers, &t_thrd.proc->lockGroupLink);
    LWLockRelease(leaderLwlock);
}
