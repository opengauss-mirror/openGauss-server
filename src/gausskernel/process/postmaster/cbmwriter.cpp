/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cbmwriter.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/cbmwriter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>

#include "access/cbmparsexlog.h"
#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/smgr/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "gssignal/gs_signal.h"

/* Signal handlers */
static void CBM_quickdie(SIGNAL_ARGS);
static void CBMSigHupHandler(SIGNAL_ARGS);
static void CBMShutdownHandler(SIGNAL_ARGS);
static void CBMReaderShutdownHandler(SIGNAL_ARGS);
static void CBMwriter_sigusr1_handler(SIGNAL_ARGS);

/*
 * Main entry point for cbmwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void CBMWriterMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    ResourceOwner cbmwriter_resourceOwner;
    t_thrd.role = CBMWRITER;
    ereport(LOG, (errmsg("cbm writer started")));
    u_sess->attr.attr_storage.CheckPointTimeout = ENABLE_INCRE_CKPT
                                                      ? u_sess->attr.attr_storage.incrCheckPointTimeout
                                                      : u_sess->attr.attr_storage.fullCheckPointTimeout;

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     */
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, CBMSigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, CBMShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, CBMShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, CBM_quickdie);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, CBMwriter_sigusr1_handler); /* 11-1 muiyulinzhong warning 存疑 */
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    cbmwriter_resourceOwner = ResourceOwnerCreate(NULL, "CBM Writer",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.utils_cxt.CurrentResourceOwner = cbmwriter_resourceOwner;
    t_thrd.cbm_cxt.XlogCbmSys->actualWorkerNum = u_sess->attr.attr_storage.cbm_threads_num;
    t_thrd.cbm_cxt.XlogCbmSys->xlogFilesEpoch = u_sess->attr.attr_storage.cbm_xlog_files_epoch;
    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    if (g_instance.comm_cxt.cbm_cxt.cbmTopContext == NULL) {
        g_instance.comm_cxt.cbm_cxt.cbmTopContext =  AllocSetContextCreate(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                  "CBM Writer", ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    } else {
        MemoryContextResetAndDeleteChildren(g_instance.comm_cxt.cbm_cxt.cbmTopContext);
    }
    MemoryContextSwitchTo(g_instance.comm_cxt.cbm_cxt.cbmTopContext);
    pg_atomic_write_u32(&g_instance.comm_cxt.cbm_cxt.skipIncomingRequest, CBM_ACCEPT_TASK);
    
    /*
     * If an exception is encountered, processing resumes here.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);
        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().	We don't have very many resources to worry
         * about in cbmwriter, but we do have LWLocks, and perhaps others?
         */
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        ResourceOwnerRelease(cbmwriter_resourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        t_thrd.utils_cxt.CurrentResourceOwner = cbmwriter_resourceOwner;

        FreeAllAllocatedDescs();
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(g_instance.comm_cxt.cbm_cxt.cbmTopContext);
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);

        t_thrd.cbm_cxt.XlogCbmSys->needReset = true;
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    /*
     * Advertise our latch that backends can use to wake us up while we're
     * sleeping.
     */
    g_instance.proc_base->cbmwriterLatch = &t_thrd.proc->procLatch;

    pgstat_report_appname("CBM Writer");
    pgstat_report_activity(STATE_IDLE, NULL);

    if (!CreateCBMReaderWorkers()) {
        ereport(ERROR, (errmsg("Can not start CBM Writer when all reader worker init failed.")));
    }
    
    /* We should wait for all CBM reader create success. */
    while (!t_thrd.cbm_cxt.shutdown_requested && !CheckCBMReaderWorkersStatus()) {
        pg_usleep(10000);
    }
    ereport(LOG, (errmsg("All CBM Reader worker init done, Begin to work")));
    /*
     * Loop forever
     */
    for (;;) {
        int rc;
        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        pgstat_report_activity(STATE_RUNNING, NULL);

        /* 
         * Process any requests or signals received recently.
         */
        if (t_thrd.cbm_cxt.got_SIGHUP) {
            t_thrd.cbm_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            u_sess->attr.attr_storage.CheckPointTimeout = ENABLE_INCRE_CKPT
                                                              ? u_sess->attr.attr_storage.incrCheckPointTimeout
                                                              : u_sess->attr.attr_storage.fullCheckPointTimeout;
            /* Modify the parms by signal */
            ModifyCBMReaderByConfig();
            t_thrd.cbm_cxt.XlogCbmSys->xlogFilesEpoch = u_sess->attr.attr_storage.cbm_xlog_files_epoch;
        }

        if (t_thrd.cbm_cxt.shutdown_requested) {
            bool isAllQuit = false;
            /* When CBM Writer receive the SIGTERM, should make sure that CBM Reader quit at first. */
            g_instance.proc_base->cbmwriterLatch = NULL;
            /* Send Sigterm to CBM Reader. */
            pg_atomic_write_u32(&g_instance.comm_cxt.cbm_cxt.skipIncomingRequest, CBM_REJECT_TASK);
            for (int i = 0; i < t_thrd.cbm_cxt.XlogCbmSys->actualWorkerNum; i++) {
                SignalCBMReaderWorker(SIGTERM);
            }
            WakeUpCBMWorkers();
            /* Wait for all CBM Reader quit, CBM Writer will handle the recourse clean up. */
            while (!isAllQuit) {
                isAllQuit = true;
                for (int i = 0; i < t_thrd.cbm_cxt.XlogCbmSys->actualWorkerNum; i++) {
                    if (pg_atomic_read_u32(&g_instance.comm_cxt.cbm_cxt.CBMThreadStatusList[i].workState) != CBM_THREAD_INVALID) {
                        isAllQuit = false;
                    }
                }
            }
            /* clean cbm track resources */
            ResetXlogCbmSys();
            /* Normal exit from the walwriter is here */
            proc_exit(0); /* done */
        }
        
        WaitAndCheckCBMReaderWorkReboot(CBM_THREAD_NORMAL, true, t_thrd.cbm_cxt.shutdown_requested);

        /* Main work function under CBM Writer */
        CBMWriteAndFollowXlog();
        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            (long)u_sess->attr.attr_storage.CheckPointTimeout * 1000);

        /* Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (rc & WL_POSTMASTER_DEATH) { 
            g_instance.proc_base->cbmwriterLatch = NULL;
            gs_thread_exit(1);
        }
    }
}

/*
 * Main entry point for CBM Reader process
 *
 * This is invoked from CBM Writer, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void CBMReaderMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    ResourceOwner cbmReaderResourceOwner;
    t_thrd.role = CBMREADER;
    ereport(LOG, (errmsg("cbm reader started")));

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     */
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, CBMSigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, CBMReaderShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, CBMReaderShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, CBM_quickdie);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, CBMwriter_sigusr1_handler); /* 11-1 muiyulinzhong warning 存疑 */
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* 12-5 used for notify reader skip work */

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    cbmReaderResourceOwner = ResourceOwnerCreate(NULL, "CBM Reader",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.utils_cxt.CurrentResourceOwner = cbmReaderResourceOwner;

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    Assert(g_instance.comm_cxt.cbm_cxt.cbmTopContext != NULL);
    /* Duo to the context create in g_instance, so it should be done before CBM Reader. */
    Assert(t_thrd.cbm_cxt.CBMReaderStatus->cbmReaderFreeContext != NULL);
    Assert(t_thrd.cbm_cxt.CBMReaderStatus->cbmReaderNormalContext != NULL);
    MemoryContextSwitchTo(t_thrd.cbm_cxt.CBMReaderStatus->cbmReaderNormalContext);

    /*
     * If an exception is encountered, processing resumes here.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);
        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().	We don't have very many resources to worry
         * about in cbmwriter, but we do have LWLocks, and perhaps others?
         */
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        ResourceOwnerRelease(cbmReaderResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        t_thrd.utils_cxt.CurrentResourceOwner = cbmReaderResourceOwner;

        FreeAllAllocatedDescs();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(t_thrd.cbm_cxt.CBMReaderStatus->cbmReaderNormalContext);
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    /* 
     * Advertise our latch that backends can use to wake us up while we're
     * sleeping.
     */
    
    /* CBM Reader set NORMAL STATE, begin to work. */
    pg_atomic_write_u32(&t_thrd.cbm_cxt.CBMReaderStatus->workState, CBM_THREAD_NORMAL);
    /*
     * Loop forever
     */
    for (;;) {
        if (t_thrd.cbm_cxt.shutdown_requested) {
            pg_atomic_write_u32(&t_thrd.cbm_cxt.CBMReaderStatus->workState, CBM_THREAD_INVALID);
            proc_exit(0); /* done */
        }
        /* Main work loop */
        CBMReadAndParseXLog();
    }
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
/*
 * cbm_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void CBM_quickdie(SIGNAL_ARGS)
{
    g_instance.proc_base->cbmwriterLatch = NULL;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void CBMSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.cbm_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void CBMShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.cbm_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

static void CBMReaderShutdownHandler(SIGNAL_ARGS)
{
    /* Just writer flag, make CBMWriter can detected. */
    pg_atomic_write_u32(&t_thrd.cbm_cxt.CBMReaderStatus->workState, CBM_THREAD_INVALID);
    t_thrd.cbm_cxt.shutdown_requested = true;
    ereport(LOG, (errmsg("Thread CBMReader %d have decided to exit.", t_thrd.cbm_cxt.CBMReaderIndex)));
}

/* SIGUSR1: used for latch wakeups */
static void CBMwriter_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}