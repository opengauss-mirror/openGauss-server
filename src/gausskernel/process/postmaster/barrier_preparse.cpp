/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * barrier_preparse.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/process/postmaster/barrier_preparse.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gs_thread.h"
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "replication/walreceiver.h"
#include "pgxc/barrier.h"
#include "postmaster/barrier_preparse.h"

typedef struct XLogPageReadPrivate {
    const char *datadir;
    TimeLineID tli;
} XLogPageReadPrivate;

#define NEED_INSERT_INTO_HASH \
    ((record->xl_rmid == RM_BARRIER_ID) && ((info == XLOG_BARRIER_SWITCHOVER) || \
        (IS_PGXC_COORDINATOR && info == XLOG_BARRIER_COMMIT) || (IS_PGXC_DATANODE && info == XLOG_BARRIER_CREATE)))

static void InitBarrierHash()
{
    if (g_instance.csn_barrier_cxt.barrier_context == NULL) {
        g_instance.csn_barrier_cxt.barrier_context = AllocSetContextCreate(g_instance.instance_context,
            "CsnBarrierContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }

    HASHCTL ctl;
    errno_t rc = 0;

    /* Init hash table */
    rc = memset_s(&ctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(rc, "", "");
    ctl.keysize = MAX_BARRIER_ID_LENGTH * sizeof(char);
    ctl.entrysize = MAX_BARRIER_ID_LENGTH * sizeof(char);
    ctl.hash = string_hash;
    ctl.hcxt = g_instance.csn_barrier_cxt.barrier_context;
    g_instance.csn_barrier_cxt.barrier_hash_table = hash_create("Barrier Id Storage Table", INIBARRIERCACHESIZE,
                                                                &ctl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
    g_instance.csn_barrier_cxt.barrier_hashtbl_lock = LWLockAssign(LWTRANCHE_BARRIER_TBL);
}

static void SetBarrieID(const char *barrierId, XLogRecPtr lsn)
{
    errno_t rc = EOK;
    const uint32 shiftSize = 32;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    rc = strncpy_s((char *)walrcv->lastReceivedBarrierId, MAX_BARRIER_ID_LENGTH, barrierId, MAX_BARRIER_ID_LENGTH - 1);
    securec_check(rc, "\0", "\0");

    walrcv->lastReceivedBarrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
    walrcv->lastReceivedBarrierLSN = lsn;
    SpinLockRelease(&walrcv->mutex);

    ereport(LOG, (errmsg("SetBarrieID set the barrier ID is %s, the barrier LSN is %08X/%08X", barrierId,
        (uint32)(lsn >> shiftSize), (uint32)lsn)));
}

static void BarrierPreParseSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.barrier_preparse_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

static void BarrierPreParseShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.barrier_preparse_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

static void BarrierPreParseQuickDie(SIGNAL_ARGS)
{
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

static void BarrierPreParseSigUsr1Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    latch_sigusr1_handler();

    errno = saveErrno;
}

/*
 * Called when the BarrierPreParseMain is ending.
 */
static void ShutdownBarrierPreParse(int code, Datum arg)
{
    g_instance.proc_base->BarrierPreParseLatch = NULL;
}

void SetBarrierPreParseLsn(XLogRecPtr startptr)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->lastReceivedBarrierLSN = startptr;
    SpinLockRelease(&walrcv->mutex);
}

void BarrierPreParseMain(void)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    MemoryContext preParseContext;
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = NULL;
    char *errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr startLSN = InvalidXLogRecPtr;
    XLogRecPtr preStartLSN = InvalidXLogRecPtr;
    XLogRecPtr lastReadLSN = InvalidXLogRecPtr;
    bool found = false;
    XLogRecPtr barrierLSN = InvalidXLogRecPtr;
    char *xLogBarrierId = NULL;
    char barrierId[MAX_BARRIER_ID_LENGTH] = {0};
    const uint32 shiftSize = 32;
    int rc;

    ereport(LOG, (errmsg("[BarrierPreParse] barrier preparse thread started")));

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, BarrierPreParseSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, BarrierPreParseShutdownHandler);
    (void)gspqsignal(SIGQUIT, BarrierPreParseQuickDie); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, BarrierPreParseSigUsr1Handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    on_shmem_exit(ShutdownBarrierPreParse, 0);

    preParseContext = AllocSetContextCreate(t_thrd.top_mem_cxt, "Barrier PreParse", ALLOCSET_DEFAULT_MINSIZE,
                                            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(preParseContext);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    g_instance.proc_base->BarrierPreParseLatch = &t_thrd.proc->procLatch;

    startLSN = walrcv->lastReceivedBarrierLSN;
    ereport(LOG, (errmsg("[BarrierPreParse] preparse thread start at %08X/%08X", (uint32)(startLSN >> shiftSize),
                         (uint32)startLSN)));

    if (g_instance.csn_barrier_cxt.barrier_hash_table == NULL) {
        InitBarrierHash();
    }

    readprivate.datadir = t_thrd.proc_cxt.DataDir;
    readprivate.tli = GetRecoveryTargetTLI();

    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("memory is temporarily unavailable while allocate xlog reader")));

    /*
     * Loop forever
     */
    for (;;) {
        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        if (t_thrd.barrier_preparse_cxt.got_SIGHUP) {
            t_thrd.barrier_preparse_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (t_thrd.barrier_preparse_cxt.shutdown_requested) {
            ereport(LOG, (errmsg("[BarrierPreParse] preparse thread shut down")));
            XLogReaderFree(xlogreader);
            proc_exit(0); /* done */
        }

        found = false;
        preStartLSN = startLSN;
        ereport(DEBUG1, (errmsg("[BarrierPreParse] start to preparse at: %08X/%08X",
            (uint32)(startLSN >> shiftSize), (uint32)startLSN)));
        startLSN = XLogFindNextRecord(xlogreader, startLSN);
        if (XLogRecPtrIsInvalid(startLSN)) {
            startLSN = preStartLSN;
            if (!XLByteEQ(walrcv->receiver_flush_location, startLSN) &&
                !XLByteEQ(walrcv->lastRecoveredBarrierLSN, startLSN)) {
                /* reset startLSN */
                startLSN = walrcv->lastRecoveredBarrierLSN;
                ereport(LOG, (errmsg("[BarrierPreParse] reset startLSN with lastRecoveredBarrierLSN: %08X/%08X",
                    (uint32)(startLSN >> shiftSize), (uint32)startLSN)));
            }
            continue;
        }

        do {
            record = XLogReadRecord(xlogreader, startLSN, &errormsg);
            if (record == NULL) {
                break;
            }
            lastReadLSN = xlogreader->EndRecPtr;
            uint8 info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
            if (NEED_INSERT_INTO_HASH) {
                xLogBarrierId = XLogRecGetData(xlogreader);
                if (!IS_CSN_BARRIER(xLogBarrierId)) {
                    ereport(WARNING, (errmsg("[BarrierPreParse] %s is not for standby cluster", xLogBarrierId)));
                } else {
                    // insert into hash table
                    found = true;
                    barrierLSN = xlogreader->EndRecPtr;
                    rc = strncpy_s((char *)barrierId, MAX_BARRIER_ID_LENGTH, xLogBarrierId, MAX_BARRIER_ID_LENGTH - 1);
                    securec_check(rc, "\0", "\0");
                    barrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
                    LWLockAcquire(g_instance.csn_barrier_cxt.barrier_hashtbl_lock, LW_EXCLUSIVE);
                    BarrierCacheInsertBarrierId(barrierId);
                    LWLockRelease(g_instance.csn_barrier_cxt.barrier_hashtbl_lock);
                    ereport(LOG, (errmsg("[BarrierPreParse] insert barrierID %s to the hash table, rmid: %d, crc: %d.",
                    barrierId, record->xl_rmid, record->xl_crc)));
                }
            }
            startLSN = InvalidXLogRecPtr;
        } while (!t_thrd.barrier_preparse_cxt.shutdown_requested);

        /* close xlogreadfd after circulation */
        CloseXlogFile();

        if (found) {
            SetBarrieID(barrierId, barrierLSN);
        }

        startLSN = XLogRecPtrIsInvalid(lastReadLSN) ? preStartLSN : lastReadLSN;

        if (XLogRecPtrIsInvalid(xlogreader->ReadRecPtr) && errormsg) {
            ereport(LOG, (errmsg("[BarrierPreParse] preparse thread get an error info %s", errormsg)));
        }
        const long sleepTime = 1000;
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, sleepTime);
        if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
            XLogReaderFree(xlogreader);
            ereport(LOG, (errmsg("[BarrierPreParse] preparse thread shut down with code 1")));
            gs_thread_exit(1);
        }
    }
}

void WakeUpBarrierPreParseBackend()
{
    if (g_instance.pid_cxt.BarrierPreParsePID != 0) {
        if (g_instance.proc_base->BarrierPreParseLatch != NULL) {
            SetLatch(g_instance.proc_base->BarrierPreParseLatch);
        }
    }
}
