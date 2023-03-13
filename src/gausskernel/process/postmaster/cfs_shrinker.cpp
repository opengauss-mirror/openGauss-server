/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * snapcapturer.cpp
 *
 * openGauss snapshot capturer thread Implementation
 *
 * IDENTIFICATION
 * src/gausskernel/process/postmaster/cfs_shrinker.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_database.h"
#include "fmgr.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_thread.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"

#include "postmaster/cfs_shrinker.h"

/* the unit is seconds, interval of alarm check loop. */
static const int CFS_SHRINKER_INTERVAL = 180;

/*
 * CfsShrinkerShmemSize
 *		Size of CfsShrinker related shared memory
 */
Size CfsShrinkerShmemSize()
{
    return sizeof(CfsShrinkerShmemStruct);
}

/*
 * CfsShrinkerShmemInit
 *		Allocate and initialize CfsShrinker -related shared memory
 */
void CfsShrinkerShmemInit(void)
{
    bool found = false;

    t_thrd.cfs_shrinker_cxt.cfsShrinkerShmem =
        (CfsShrinkerShmemStruct *)ShmemInitStruct("CsfShrinker Data", sizeof(CfsShrinkerShmemStruct), &found);

    if (!found) {
        /*
         * First time through, so initialize.  Note that we zero the whole
         * requests array; this is so that CompactCheckpointerRequestQueue
         * can assume that any pad bytes in the request structs are zeroes.
         */
        errno_t ret = memset_s(t_thrd.cfs_shrinker_cxt.cfsShrinkerShmem, sizeof(CfsShrinkerShmemStruct),
            0, sizeof(CfsShrinkerShmemStruct));
        securec_check(ret, "\0", "\0");
    }

    return;
}

void CfsShrinkerShmemListPush(const RelFileNode &rnode, ForkNumber forknum, char parttype)
{
    CfsShrinkerShmemStruct *ctx = t_thrd.cfs_shrinker_cxt.cfsShrinkerShmem;

    while (1) {
        SpinLockAcquire(&ctx->spinlck);
        if (!CfsShrinkListIsFull(ctx)) {  // break only if the list is not full.
            break;
        }

        SpinLockRelease(&ctx->spinlck);
        pg_usleep(1000L);  // sleep 1ms
    }

    // push item from the tail location, FIFO.
    CfsShrinkItem *node = &ctx->nodes[ctx->tail];
    node->node = rnode;
    node->forknum = forknum;
    node->parttype = parttype;

    ctx->tail = (ctx->tail + 1) % CFS_SHRINKER_ITEM_MAX_COUNT;
    SpinLockRelease(&ctx->spinlck);

    /*
     * Wake cfs-shrinker only when we have add item into the queue.
     */
    SetLatch(&t_thrd.proc->procLatch);
}

CfsShrinkItem* CfsShrinkerShmemListPop()
{
    CfsShrinkerShmemStruct *ctx = t_thrd.cfs_shrinker_cxt.cfsShrinkerShmem;
    uint8 i = 0;

    while (1) {
        SpinLockAcquire(&ctx->spinlck);
        if (!CfsShrinkListIsEmpty(ctx)) {  // break only if the list is not empty.
            break;
        }

        SpinLockRelease(&ctx->spinlck);
        pg_usleep(1000L);  // sleep 1ms

        i++;
        if (i >= CFS_POP_WAIT_TIMES) {
            return NULL;
        }
    }

    // pop item from the head location, FIFO.
    CfsShrinkItem *node = &ctx->nodes[ctx->head];
    ctx->head = (ctx->head + 1) % CFS_SHRINKER_ITEM_MAX_COUNT;
    
    SpinLockRelease(&ctx->spinlck);
    return node;
}

/*
 * signal handle functions
 */
 
/* SIGHUP: set flag to re-read config file at next convenient time */
static void CfsShrinkerSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.cfs_shrinker_cxt.got_SIGHUP = (int)true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void CfsShrinkerSigtermHander(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.cfs_shrinker_cxt.got_SIGTERM = (int)true;

    SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;

    elog(LOG, "CfsShrinker signaled: SIGTERM");
}

ThreadId StartCfsShrinkerCapturer(void)
{
    if (ENABLE_DMS) {
        return 0;
    }

    if (!IsPostmasterEnvironment) {
        return 0;
    }

    if (canAcceptConnections(false) == CAC_OK) {
        return initialize_util_thread(CFS_SHRINKER);
    }

    ereport(LOG,
        (errmsg("not ready to start snapshot capturer.")));
    return 0;
}

static void CfsShrinkerQuitAndClean(int code, Datum arg)
{
    (void)code;
    (void)arg;
    g_instance.pid_cxt.CfsShrinkerPID = 0;
}

static void CfsShrinkerProcInterrupts(int rc)
{
    /* Process sinval catchup interrupts that happened while sleeping */
    ProcessCatchupInterrupt();

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        proc_exit(1);
    }

    /* the normal shutdown case */
    if (t_thrd.cfs_shrinker_cxt.got_SIGTERM) {
        elog(LOG, "cfs shrinker is shutting down.");
        proc_exit(0);
    }

    /*
     * reload the postgresql.conf
     */
    if (t_thrd.cfs_shrinker_cxt.got_SIGHUP) {
        t_thrd.cfs_shrinker_cxt.got_SIGHUP = (int)false;
        ProcessConfigFile(PGC_SIGHUP);
    }
}

bool IsCfsShrinkerProcess(void)
{
    return t_thrd.role == CFS_SHRINKER;
}

void CfsShrinkImpl()
{
    // firstly, we should check whether there is any items in list.
    while (!CfsShrinkListIsEmpty(t_thrd.cfs_shrinker_cxt.cfsShrinkerShmem)) {
        CfsShrinkItem *item = CfsShrinkerShmemListPop();
        if (item == NULL) {
            break;
        }

        SmgrChunkFragmentsRestore(item->node, item->forknum, item->parttype, false);
    }

    // lazy check and shrink by my self
}

NON_EXEC_STATIC void CfsShrinkerMain()
{
    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = CFS_SHRINKER;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "CfsShrinker";

    /* Identify myself via ps */
    init_ps_display("CfsShrinker process", "", "", "");

    ereport(LOG, (errmsg("CfsShrinker started")));

    if (u_sess->attr.attr_security.PostAuthDelay)
        pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);

    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, CfsShrinkerSigtermHander);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP,  CfsShrinkerSighupHandler);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    on_proc_exit(CfsShrinkerQuitAndClean, 0);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, NULL);
    t_thrd.proc_cxt.PostInit->InitCfsShrinker();

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("CfsShrinker");
    pgstat_report_activity(STATE_IDLE, NULL);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    MemoryContext workMxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt,
        "CfsShrinker",
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(workMxt);

    int curTryCounter = 0;
    int* oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false;

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * Abort the current transaction in order to recover.
         */
        AbortCurrentTransaction();

        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(workMxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(workMxt);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.cfs_shrinker_cxt.got_SIGTERM) {
            goto shutdown;
        }

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* loop until shutdown request */
    while (!t_thrd.cfs_shrinker_cxt.got_SIGTERM) {
        int rc;

        CHECK_FOR_INTERRUPTS();

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        rc = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            CFS_SHRINKER_INTERVAL * 1000L);

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /* check the latch rc */
        CfsShrinkerProcInterrupts(rc);

        /* Do the real hard work. */
        CfsShrinkImpl();

        MemoryContextResetAndDeleteChildren(workMxt);
    }

shutdown:
    elog(LOG, "CfsShrinker shutting down");
    g_instance.pid_cxt.CfsShrinkerPID = 0;
    proc_exit(0);
}

