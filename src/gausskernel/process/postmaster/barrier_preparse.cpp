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
    (IS_MULTI_DISASTER_RECOVER_MODE && (record->xl_rmid == RM_BARRIER_ID) && ((info == XLOG_BARRIER_SWITCHOVER) || \
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

    if (strcmp(barrierId, (char *)walrcv->lastReceivedBarrierId) > 0) {
        SpinLockAcquire(&walrcv->mutex);
        rc = strncpy_s((char *)walrcv->lastReceivedBarrierId, MAX_BARRIER_ID_LENGTH, barrierId,
                       MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");

        walrcv->lastReceivedBarrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
        walrcv->lastReceivedBarrierLSN = lsn;
        SpinLockRelease(&walrcv->mutex);

        ereport(LOG, (errmsg("[BarrierPreParse] SetBarrieID set the barrier ID is %s, the barrier LSN is %08X/%08X",
            barrierId, (uint32)(lsn >> shiftSize), (uint32)lsn)));
    }
}

static void request_wal_stream_for_preparse(XLogRecPtr recptr)
{
    if (t_thrd.barrier_preparse_cxt.shutdown_requested) {
        return;
    }
    if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0) {
        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
        SpinLockAcquire(&walrcv->mutex);
        walrcv->receivedUpto = 0;
        SpinLockRelease(&walrcv->mutex);
        if (t_thrd.xlog_cxt.readFile >= 0) {
            (void)close(t_thrd.xlog_cxt.readFile);
            t_thrd.xlog_cxt.readFile = -1;
        }

        ereport(LOG, (errmsg("[BarrierPreParse] preparse thread request xlog streaming")));
        if (t_thrd.xlog_cxt.is_cascade_standby) {
            RequestXLogStreaming(&recptr, NULL, REPCONNTARGET_STANDBY, NULL, true);
        } else {
            RequestXLogStreaming(&recptr, NULL, REPCONNTARGET_PRIMARY, NULL, true);
        }
    }
}

static XLogRecPtr get_preparse_start_lsn()
{
    XLogRecPtr start_lsn = InvalidXLogRecPtr;
    const uint32 shift_size = 32;

    while (XLogRecPtrIsInvalid(start_lsn)) {
        GetXLogReplayRecPtr(NULL, &start_lsn);
        if (t_thrd.barrier_preparse_cxt.shutdown_requested) {
            return start_lsn;
        }
    }
    ereport(LOG, (errmsg("[BarrierPreParse] preparse thread start at %08X/%08X",
        (uint32)(start_lsn >> shift_size), (uint32)start_lsn)));
    return start_lsn;
}

static bool check_preparse_run_time(TimestampTz *start_time)
{
    long secs;
    int usecs;

    if (!IS_MULTI_DISASTER_RECOVER_MODE && g_instance.csn_barrier_cxt.max_run_time > 0) {
        if (*start_time < 0) {
            *start_time = GetCurrentTimestamp();
        }
        TimestampDifference(*start_time, GetCurrentTimestamp(), &secs, &usecs);
        if (secs >= g_instance.csn_barrier_cxt.max_run_time) {
            g_instance.csn_barrier_cxt.max_run_time = 0;
            return true;
        }
    }

    return false;
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

bool check_preparse_result(XLogRecPtr *recptr)
{
    if (t_thrd.barrier_preparse_cxt.shutdown_requested) {
        return false;
    }
    if (XLogRecPtrIsInvalid(g_instance.csn_barrier_cxt.latest_valid_record)) {
        XLogRecPtr lastReplayRecPtr = InvalidXLogRecPtr;
        (void)GetXLogReplayRecPtr(NULL, &lastReplayRecPtr);
        if (XLogRecPtrIsInvalid(lastReplayRecPtr)) {
            *recptr = lastReplayRecPtr;
        }
        return false;
    }
    if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0) {
        return true;
    }
    return true;
}

void RequestXLogStreamForBarrier()
{
    XLogRecPtr replayEndPtr = GetXLogReplayRecPtr(NULL);
    if (t_thrd.xlog_cxt.is_cascade_standby && (CheckForSwitchoverTrigger() || CheckForFailoverTrigger())) {
        HandleCascadeStandbyPromote(&replayEndPtr);
        return;
    }
    if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0) {
        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
        SpinLockAcquire(&walrcv->mutex);
        walrcv->receivedUpto = 0;
        SpinLockRelease(&walrcv->mutex);
        if (t_thrd.xlog_cxt.readFile >= 0) {
            (void)close(t_thrd.xlog_cxt.readFile);
            t_thrd.xlog_cxt.readFile = -1;
        }

        RequestXLogStreaming(&replayEndPtr, t_thrd.xlog_cxt.PrimaryConnInfo, REPCONNTARGET_PRIMARY,
                             u_sess->attr.attr_storage.PrimarySlotName);
    }
}

void check_exit_preparse_conditions(XLogReaderState *xlogreader, TimestampTz *start_time,
    XLogRecPtr *start_lsn, int try_time)
{
    int rc;
    const int max_try_times = 20;

    if (check_preparse_result(start_lsn)) {
        request_wal_stream_for_preparse(*start_lsn);
    }

    if (!IS_MULTI_DISASTER_RECOVER_MODE && g_instance.csn_barrier_cxt.max_run_time == 0 && try_time > max_try_times) {
        ereport(LOG, (errmsg("[BarrierPreParse] preparse thread shut down")));
        XLogReaderFree(xlogreader);
        proc_exit(0);
    }

    if (check_preparse_run_time(start_time)) {
        ereport(LOG, (errmsg("[BarrierPreParse] preparse thread shut down")));
        XLogReaderFree(xlogreader);
        proc_exit(0);
    }

    const long sleepTime = 1000;
    rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, sleepTime);
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        XLogReaderFree(xlogreader);
        ereport(LOG, (errmsg("[BarrierPreParse] preparse thread shut down with code 1")));
        gs_thread_exit(1);
    }
}

void BarrierPreParseMain(void)
{
    MemoryContext preParseContext;
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = NULL;
    char *errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr startLSN = InvalidXLogRecPtr;
    XLogRecPtr preStartLSN = InvalidXLogRecPtr;
    XLogRecPtr lastReadLSN = InvalidXLogRecPtr;
    XLogRecPtr barrierLSN = InvalidXLogRecPtr;
    char *xLogBarrierId = NULL;
    char barrierId[MAX_BARRIER_ID_LENGTH] = {0};
    TimestampTz start_time = -1;
    int try_time = 0;
    XLogRecPtr latest_valid_record;
    pg_crc32 latest_record_crc;
    uint32 latest_record_len;
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
    (void)gspqsignal(SIGURG, print_stack);

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

    startLSN = get_preparse_start_lsn();
    if (IS_MULTI_DISASTER_RECOVER_MODE && g_instance.csn_barrier_cxt.barrier_hash_table == NULL) {
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

        preStartLSN = startLSN;
        startLSN = XLogFindNextRecord(xlogreader, startLSN);
        if (XLogRecPtrIsInvalid(startLSN)) {
            startLSN = preStartLSN;
        }

        /*
         * If at page start, we must skip over the page header using xrecoff check.
         */
        if (0 == startLSN % XLogSegSize) {
            XLByteAdvance(startLSN, SizeOfXLogLongPHD);
        } else if (0 == startLSN % XLOG_BLCKSZ) {
            XLByteAdvance(startLSN, SizeOfXLogShortPHD);
        }

        do {
            record = XLogReadRecord(xlogreader, startLSN, &errormsg);
            if (record == NULL) {
                break;
            }
            lastReadLSN = xlogreader->ReadRecPtr;
            latest_valid_record = lastReadLSN;
            latest_record_crc = record->xl_crc;
            latest_record_len = record->xl_tot_len;
            uint8 info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
            if (NEED_INSERT_INTO_HASH) {
                xLogBarrierId = XLogRecGetData(xlogreader);
                if (!IS_CSN_BARRIER(xLogBarrierId)) {
                    ereport(WARNING, (errmsg("[BarrierPreParse] %s is not for standby cluster", xLogBarrierId)));
                } else {
                    // insert into hash table
                    barrierLSN = xlogreader->EndRecPtr;
                    rc = strncpy_s((char *)barrierId, MAX_BARRIER_ID_LENGTH, xLogBarrierId, MAX_BARRIER_ID_LENGTH - 1);
                    securec_check(rc, "\0", "\0");
                    barrierId[MAX_BARRIER_ID_LENGTH - 1] = '\0';
                    LWLockAcquire(g_instance.csn_barrier_cxt.barrier_hashtbl_lock, LW_EXCLUSIVE);
                    BarrierCacheInsertBarrierId(barrierId);
                    LWLockRelease(g_instance.csn_barrier_cxt.barrier_hashtbl_lock);
                    SetBarrieID(barrierId, barrierLSN);
                }
            }
            startLSN = InvalidXLogRecPtr;
        } while (!t_thrd.barrier_preparse_cxt.shutdown_requested);

        /* close xlogreadfd after circulation */
        CloseXlogFile();
        XLogReaderInvalReadState(xlogreader);

        g_instance.csn_barrier_cxt.latest_valid_record = latest_valid_record;
        g_instance.csn_barrier_cxt.latest_record_crc = latest_record_crc;
        g_instance.csn_barrier_cxt.latest_record_len = latest_record_len;

        startLSN = XLogRecPtrIsInvalid(lastReadLSN) ? preStartLSN : lastReadLSN;

        if (XLogRecPtrIsInvalid(xlogreader->ReadRecPtr) && errormsg) {
            ereport(LOG, (errmsg("[BarrierPreParse] preparse thread get an error info %s", errormsg)));
        }

        try_time++;
        check_exit_preparse_conditions(xlogreader, &start_time, &startLSN, try_time);
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
