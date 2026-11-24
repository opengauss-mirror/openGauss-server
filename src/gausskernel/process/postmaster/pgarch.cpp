/*
 *
 * pgarch.cpp
 *
 *	openGauss WAL archiver
 *
 *	All functions relating to archiver are included here
 *
 *	- All functions executed by archiver process
 *
 *	- archiver is forked from postmaster, and the two
 *	processes then communicate using signals. All functions
 *	executed by postmaster are included in this file.
 *
 *	Initial author: Simon Riggs		simon@2ndquadrant.com
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/pgarch.cpp
 *
 * -------------------------------------------------------------------------
 */
#define __STDC_FORMAT_MACROS
#include "postgres.h"
#include "knl/knl_variable.h"

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/prctl.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <inttypes.h>
#include <poll.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/fd.h"
#include "storage/copydir.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/file/fio_device.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "pgstat.h"

#include "gssignal/gs_signal.h"
#include "alarm/alarm.h"
#include "replication/syncrep.h"
#include "replication/slot.h"
#include "replication/walsender_private.h"
#include "replication/walsender.h"
#include "pgxc/pgxc.h"
#include "replication/archive_walreceiver.h"
#include "replication/walreceiver.h"
#include "access/obs/obs_am.h"
#include "access/archive/archive_am.h"
#include "replication/dcf_replication.h"

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGARCH_AUTOWAKE_INTERVAL           \
    (60 * 1000) /* How often to force a poll of the \
        * archive status directory; in     \
        * millseconds. */
#define PGARCH_RESTART_INTERVAL             \
    10 /* How often to attempt to restart a \
        * failed archiver; in seconds. */
#define TIME_GET_MILLISEC(t) (((long)(t).tv_sec * 1000) + ((long)(t).tv_usec) / 1000)

/* ----------
 * Archiver control info.
 *
 * We expect that archivable files within pg_xlog will have names between
 * MIN_XFN_CHARS and MAX_XFN_CHARS in length, consisting only of characters
 * appearing in VALID_XFN_CHARS.  The status files in archive_status have
 * corresponding names with ".ready" or ".done" appended.
 * ----------
 */
#define MIN_XFN_CHARS 16
#define MAX_XFN_CHARS 40
#define VALID_XFN_CHARS "0123456789ABCDEF.history.backup"
#define ARCH_TIME_FOLDER "arch_time"

#define NUM_ARCHIVE_RETRIES 3

#define ARCHIVE_BUF_SIZE (1024 * 1024)

#define ARCHIVE_RESULT_TIMEOUT 120000

#ifndef ENABLE_LITE_MODE
/*
 * Messages exchanged with the archive command agent over the execute/result
 * pipes. Both directions carry a request id, so a result that the agent writes
 * for an earlier command abandoned by the archiver (e.g. after a timeout) is
 * recognized by its id and discarded instead of being mistaken for the result
 * of a later command.
 */
typedef struct ArchiveCmdMsg {
    uint64 reqId;           /* request id assigned by the archiver */
    char cmd[MAXPGPATH];    /* NUL-terminated archive command */
} ArchiveCmdMsg;

typedef struct ArchiveCmdResultMsg {
    uint64 reqId;           /* id of the request this result belongs to */
    int exitCode;           /* command exit code; -1 means timed out or unknown */
} ArchiveCmdResultMsg;

/* read exactly len bytes; returns len, 0 on EOF (peer closed), or -1 on error */
static ssize_t read_full(int fd, void* buf, size_t len)
{
    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, (char*)buf + got, len - got);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            /* the peer closed its end of the pipe */
            return 0;
        }
        got += (size_t)n;
    }
    return (ssize_t)got;
}

/* write exactly len bytes; returns false on error */
static bool write_full(int fd, const void* buf, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, (const char*)buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += (size_t)n;
    }
    return true;
}

/* waitpid() that survives EINTR; returns waitpid()'s result */
static pid_t waitpid_retry(pid_t pid, int* status, int options)
{
    pid_t ret = -1;
    do {
        ret = waitpid(pid, status, options);
    } while (ret == -1 && errno == EINTR);
    return ret;
}

/* current time in milliseconds on a monotonic clock */
static int64 now_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64)ts.tv_sec * 1000 + (int64)ts.tv_nsec / 1000000;
}

/*
 * poll() on a single fd with EINTR retry that never waits past deadline_ms,
 * an absolute deadline in now_ms() time. Returns poll()'s result.
 */
static int poll_deadline(struct pollfd* fds, int64 deadline_ms)
{
    for (;;) {
        int timeout = (int)(deadline_ms - now_ms());
        if (timeout < 0) {
            timeout = 0;
        }
        int ret = poll(fds, 1, timeout);
        if (ret < 0 && errno == EINTR) {
            continue;
        }
        return ret;
    }
}

/*
 * Request ids must stay unique across archiver restarts: the agent and its
 * pipes survive an archiver restart and may still hold results of requests
 * sent by the previous archiver, so ids never start over at a small number.
 */
static uint64 next_archive_req_id(void)
{
    static uint64 reqId = 0;
    if (reqId == 0) {
        reqId = ((uint64)time(NULL) << 31) + (uint32)(getpid() & 0x7fffffff);
    }
    return ++reqId;
}

/* common tail of the failure paths in pgarch_archiveXlog */
static bool pgarch_reportArchiveFailure(char* activitymsg, Size activitymsg_len, const char* xlog)
{
    errno_t rc = EOK;
    g_instance.WalSegmentArchSucceed = false;
    rc = snprintf_s(activitymsg, activitymsg_len, activitymsg_len - 1, "failed on %s", xlog);
    securec_check_ss(rc, "\0", "\0");
    set_ps_display(activitymsg, false);
    return false;
}
#endif /* ENABLE_LITE_MODE */

NON_EXEC_STATIC void PgArchiverMain(knl_thread_arg* arg);
static void pgarch_exit(SIGNAL_ARGS);
static void ArchSigHupHandler(SIGNAL_ARGS);
static void ArchSigTermHandler(SIGNAL_ARGS);
static void pgarch_waken(SIGNAL_ARGS);
static void pgarch_waken_stop(SIGNAL_ARGS);
static void pgarch_MainLoop(void);
static void pgarch_ArchiverCopyLoop(void);
static bool pgarch_archiveXlog(char* xlog);
static bool pgarch_readyXlog(char* xlog, int xlog_length);
static void pgarch_archiveDone(const char* xlog);
static void archKill(int code, Datum arg);
#ifndef ENABLE_LITE_MODE
static bool CheckArchiveCmdExecuterExists(int *pid, bool clean);
static void pgarch_archiveRoachForPitrStandby();
static bool pgarch_archiveRoachForPitrMaster(XLogRecPtr targetLsn);
static bool pgarch_archiveRoachForCoordinator(XLogRecPtr targetLsn);
static WalSnd* pgarch_chooseWalsnd(XLogRecPtr targetLsn);
typedef bool(*doArchive)(XLogRecPtr);
static void pgarch_ArchiverObsCopyLoop(XLogRecPtr flushPtr, doArchive fun);
static void InitArchiverLastTaskLsn(ArchiveSlotConfig* obs_archive_slot);
#endif

AlarmCheckResult DataInstArchChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam)
{
    if (true == g_instance.WalSegmentArchSucceed) {
        // fill the resume message
        WriteAlarmAdditionalInfo(
            additionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarm, ALM_AT_Resume);
        return ALM_ACR_Normal;
    } else {
        // fill the alarm message
        WriteAlarmAdditionalInfo(additionalParam,
            g_instance.attr.attr_common.PGXCNodeName,
            "",
            "",
            alarm,
            ALM_AT_Fault,
            g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Abnormal;
    }
}

void setObsArchLatch(const Latch* latch)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->obsArchLatch = (Latch *)latch;
    SpinLockRelease(&walrcv->mutex);
}
/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */

/*
 * pgarch_start
 *
 *	Called from postmaster at startup or after an existing archiver
 *	died.  Attempt to fire up a fresh archiver process.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
ThreadId pgarch_start()
{
    time_t curtime;

    /*
     * Do nothing if no archiver needed
     */
    if (!XLogArchivingActive())
        return 0;
    /*
     * Do nothing if too soon since last archiver start.  This is a safety
     * valve to protect against continuous respawn attempts if the archiver is
     * dying immediately at launch. Note that since we will be re-called from
     * the postmaster main loop, we will get another chance later.
     */
    curtime = time(NULL);
    if ((unsigned int)(curtime - t_thrd.arch.last_pgarch_start_time) < (unsigned int)PGARCH_RESTART_INTERVAL)
        return 0;
    t_thrd.arch.last_pgarch_start_time = curtime;

    return initialize_util_thread(ARCH);
}

/* ------------------------------------------------------------
 * Local functions called by archiver follow
 * ------------------------------------------------------------
 */
/*
 * PgArchiverMain
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.  However,
 *	since we don't use 'em, it hardly matters...
 */
NON_EXEC_STATIC void PgArchiverMain(knl_thread_arg* arg)
{
    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    t_thrd.proc_cxt.MyStartTime = time(NULL); /* record Start Time for logging */

    t_thrd.proc_cxt.MyProgName = "PgArchiver";

    t_thrd.myLogicTid = noProcLogicTid + PGARCH_LID;

    t_thrd.arch.slot_name = pstrdup((char *)arg->payload);

    ereport(LOG, (errmsg("PgArchiver started")));

    InitLatch(&t_thrd.arch.mainloop_latch); /* initialize latch used in main loop */
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except for SIGHUP, SIGTERM, SIGUSR1, SIGUSR2, and SIGQUIT.
     */

    (void)gspqsignal(SIGHUP, ArchSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, ArchSigTermHandler);
    (void)gspqsignal(SIGQUIT, pgarch_exit);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, pgarch_waken);
    (void)gspqsignal(SIGUSR2, pgarch_waken_stop);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
    (void)gspqsignal(SIGURG, print_stack);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    on_shmem_exit(archKill, 0);

    /*
     * Identify myself via ps
     */
    init_ps_display("archiver process", "", "", "");
    setObsArchLatch(&t_thrd.arch.mainloop_latch);

#ifndef ENABLE_LITE_MODE
    InitArchiverLastTaskLsn(NULL);
#endif
    pgarch_MainLoop();
    ArchiveTaskStatus *archiveTaskStatus = NULL;
    archiveTaskStatus = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archiveTaskStatus != NULL) {
        archiveTaskStatus->archiver_latch = NULL;
    }

    gs_thread_exit(0);
}

/* SIGQUIT signal handler for archiver process */
static void pgarch_exit(SIGNAL_ARGS)
{
    /* SIGQUIT means curl up and die ... */
    exit(1);
}

/* SIGHUP signal handler for archiver process */
static void ArchSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    /* set flag to re-read config file at next convenient time */
    t_thrd.worker_sig_flags.got_SIGHUP = true;
    SetLatch(&t_thrd.arch.mainloop_latch);

    errno = save_errno;
}

/* SIGTERM signal handler for archiver process */
static void ArchSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    /*
     * The postmaster never sends us SIGTERM, so we assume that this means
     * that init is trying to shut down the whole system.  If we hang around
     * too long we'll get SIGKILL'd.  Set flag to prevent starting any more
     * archive commands.
     */
    t_thrd.worker_sig_flags.got_SIGTERM = true;
    SetLatch(&t_thrd.arch.mainloop_latch);

    errno = save_errno;
}

/* SIGUSR1 signal handler for archiver process */
static void pgarch_waken(SIGNAL_ARGS)
{
    int save_errno = errno;

    /* set flag that there is work to be done */
    t_thrd.arch.wakened = true;
    SetLatch(&t_thrd.arch.mainloop_latch);
    errno = save_errno;
}

/* SIGUSR2 signal handler for archiver process */
static void pgarch_waken_stop(SIGNAL_ARGS)
{
    int save_errno = errno;

    /* set flag to do a final cycle and shut down afterwards */
    t_thrd.arch.ready_to_stop = true;
    SetLatch(&t_thrd.arch.mainloop_latch);

    errno = save_errno;
}

static void VerifyDestDirIsEmptyOrCreate(char* dirname)
{
    switch (pg_check_dir(dirname)) {
        case 0:
            /* Does not exist, so create */
            if (pg_mkdir_p(dirname, S_IRWXU) == -1) {
                ereport(FATAL, (errmsg_internal("could not create directory \"%s\": %s\n", dirname, strerror(errno))));
            }
            break;
        case -1:
            /* Access problem */
            ereport(FATAL, (errmsg_internal("could not access directory \"%s\": %s\n", dirname, strerror(errno))));
        default: /* Nothing */
            break;
    }
    
    return;
}

/*
 * pgarch_MainLoop
 *
 * Main loop for archiver
 */
static void pgarch_MainLoop(void)
{
    struct timeval last_copy_time;
    gettimeofday(&last_copy_time, NULL);
    bool time_to_stop = false;
#ifndef ENABLE_LITE_MODE
    doArchive fun = NULL;
    const int millitosec = 1000;
#endif

    /*
     * We run the copy loop immediately upon entry, in case there are
     * unarchived files left over from a previous database run (or maybe the
     * archiver died unexpectedly).  After that we wait for a signal or
     * timeout before doing more.
     */
    t_thrd.arch.wakened = true;
    ArchiveSlotConfig *obs_archive_slot = NULL;

    if (XLogArchiveDestSet()) {
        VerifyDestDirIsEmptyOrCreate(u_sess->attr.attr_storage.XLogArchiveDest);
    }

    /*
     * There shouldn't be anything for the archiver to do except to wait for a
     * signal ... however, the archiver exists to protect our data, so she
     * wakes up occasionally to allow herself to be proactive.
     */
    do {
        ResetLatch(&t_thrd.arch.mainloop_latch);
        struct timeval curtime;

        /* When we get SIGUSR2, we do one more archive cycle, then exit */
        time_to_stop = t_thrd.arch.ready_to_stop;

        /* Check for config update */
        if (t_thrd.worker_sig_flags.got_SIGHUP) {
            t_thrd.worker_sig_flags.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            if (!XLogArchivingActive() && getArchiveReplicationSlot() == NULL) {
                ereport(LOG, (errmsg("PgArchiver exit")));
                return;
            }
        }

        /*
         * If we've gotten SIGTERM, we normally just sit and do nothing until
         * SIGUSR2 arrives.  However, that means a random SIGTERM would
         * disable archiving indefinitely, which doesn't seem like a good
         * idea.  If more than 60 seconds pass since SIGTERM, exit anyway, so
         * that the postmaster can start a new archiver if needed.
         */
        if (t_thrd.worker_sig_flags.got_SIGTERM) {
            time_t icurtime = time(NULL);

            if (t_thrd.arch.last_sigterm_time == 0)
                t_thrd.arch.last_sigterm_time = icurtime;
            else if ((unsigned int)(icurtime - t_thrd.arch.last_sigterm_time) >= (unsigned int)60)
                break;
        }
#ifndef ENABLE_LITE_MODE
        load_server_mode();
        if (IsServerModeStandby()) {
            
            ArchiveTaskStatus *archive_task_status = NULL;
            archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
            if (archive_task_status == NULL) {
                ereport(ERROR,
                    (errmsg("pgarch_Archive main loop failed because could not get an archive task status.")));
            }
            /* if we should do pitr archive, for standby */
            volatile unsigned int *pitr_task_status = &archive_task_status->pitr_task_status;
            if (unlikely(pg_atomic_read_u32(pitr_task_status) == PITR_TASK_GET)) {
                pgarch_archiveRoachForPitrStandby();
                pg_atomic_write_u32(pitr_task_status, PITR_TASK_DONE);
                update_archive_start_end_location_file(archive_task_status->archive_task.targetLsn,
                    TIME_GET_MILLISEC(last_copy_time));
            }
        }
#endif

        /* Do what we're here for */
        if (t_thrd.arch.wakened || time_to_stop) {
            t_thrd.arch.wakened = false;
#ifndef ENABLE_LITE_MODE
            obs_archive_slot = getArchiveReplicationSlot();
            if (obs_archive_slot != NULL && !IsServerModeStandby()) {
                gettimeofday(&curtime, NULL);
                const long time_diff = TIME_GET_MILLISEC(curtime) -  t_thrd.arch.last_arch_time;
                XLogRecPtr receivePtr;
                XLogRecPtr writePtr;
                XLogRecPtr flushPtr;
                XLogRecPtr replayPtr;
                bool got_recptr = false;
                bool amSync = false;
                int retryTimes = 3;
                /* FlushPtr <= ConsensusPtr on DCF mode */
                if (IS_PGXC_COORDINATOR || g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
                    flushPtr = GetFlushRecPtr();
                    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf &&
                        XLogRecPtrIsInvalid(t_thrd.arch.pitr_task_last_lsn) &&
                        XLByteLE(OBS_XLOG_SLICE_BLOCK_SIZE, flushPtr))
                        /* restart thread to init pitr_task_last_lsn from obs after DCF election */
                        ereport(ERROR,
                            (errmsg("Invalid pitr task last lsn on DCF mode.")));
                } else {
                    if (t_thrd.arch.pitr_task_last_lsn == InvalidXLogRecPtr) {
                        InitArchiverLastTaskLsn(obs_archive_slot);
                        ereport(LOG, (errmsg("update arch thread last lsn because current node is not standby, "
                            "and init last lsn is %08X%08X", (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32),
                            (uint32)t_thrd.arch.pitr_task_last_lsn)));
                    }
                    while (retryTimes--) {
                        got_recptr =
                            SyncRepGetSyncRecPtr(&receivePtr, &writePtr, &flushPtr, &replayPtr, &amSync, false);
                        if (got_recptr == true) {
                            break;
                        } else {
                            pg_usleep(1000000L);
                        }
                    }
                    if (got_recptr == false) {
                        ereport(ERROR,
                            (errmsg("pgarch_ArchiverObsCopyLoop failed when call SyncRepGetSyncRecPtr")));
                    }
                }

                if (time_diff >= (u_sess->attr.attr_storage.archive_interval * millitosec)
                    || XLByteDifference(flushPtr, t_thrd.arch.pitr_task_last_lsn) >= OBS_XLOG_SLICE_BLOCK_SIZE) {
                    if (IS_PGXC_COORDINATOR) {
                        fun = &pgarch_archiveRoachForCoordinator;
                    } else {
                        fun = &pgarch_archiveRoachForPitrMaster;
#ifndef ENABLE_MULTIPLE_NODES
                        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf)
                            fun = &DcfArchiveRoachForPitrMaster;
#endif
                    }
                    pgarch_ArchiverObsCopyLoop(flushPtr, fun);
                }
            } else {
#endif
                pgarch_ArchiverCopyLoop();
                gettimeofday(&last_copy_time, NULL);
#ifndef ENABLE_LITE_MODE
            }
#endif
        }

        pgstat_send_threadio_stats();

        /*
         * Sleep until a signal is received, or until a poll is forced by
         * PGARCH_AUTOWAKE_INTERVAL having passed since last_copy_time, or
         * until postmaster dies.
         */
        if (!time_to_stop) {
            long wait_interval;
            long last_time;
            /* Don't wait during last iteration */
            if (obs_archive_slot != NULL) {
                wait_interval = t_thrd.arch.task_wait_interval;
                last_time = t_thrd.arch.last_arch_time;
            } else {
                wait_interval = PGARCH_AUTOWAKE_INTERVAL;
                last_time = TIME_GET_MILLISEC(last_copy_time);
            }
            gettimeofday(&curtime, NULL);
            long time_diff = (long)TIME_GET_MILLISEC(curtime) - last_time;
            if (time_diff < 0) {
                time_diff = 0;
            }
            long timeout = wait_interval - time_diff;
            if (timeout < 0 && IsServerModeStandby()) {
                /* sleep 100ms for check next task */
                timeout = 100;
            }
            if (timeout > 0) {
                int rc;
                rc = WaitLatch(
                    &t_thrd.arch.mainloop_latch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, timeout);
                if (rc & WL_TIMEOUT) {
                    t_thrd.arch.wakened = true;
                    ereport(DEBUG1, (errmsg("mainloop_latch end for timeout")));
                }
                if (rc & WL_LATCH_SET) {
                    ereport(DEBUG1, (errmsg("mainloop_latch end for set")));
                }
            } else {
                t_thrd.arch.wakened = true;
            }
            ereport(DEBUG1,
                (errmsg("pgarch_MainLoop cur time: %ld, last_time:%ld, time_diff:%ld, timeout:%ld",
                    (long)TIME_GET_MILLISEC(curtime), (long)t_thrd.arch.last_arch_time, time_diff, timeout)));
        }

        /*
         * The archiver quits either when the postmaster dies (not expected)
         * or after completing one more archiving cycle after receiving
         * SIGUSR2.
         */
    } while (PostmasterIsAlive() && !time_to_stop && (XLogArchivingActive() || getArchiveReplicationSlot() != NULL));
}

/*
 * pgarch_ArchiverCopyLoop
 *
 * Archives all outstanding xlogs then returns
 */
static void pgarch_ArchiverCopyLoop(void)
{
    char xlog[MAX_XFN_CHARS + 1];

    /*
     * loop through all xlogs with archive_status of .ready and archive
     * them...mostly we expect this to be a single file, though it is possible
     * some backend will add files onto the list of those that need archiving
     * while we are still copying earlier archives
     */
    while (pgarch_readyXlog(xlog, MAX_XFN_CHARS + 1)) {
        int failures = 0;

        for (;;) {
            /*
             * Do not initiate any more archive commands after receiving
             * SIGTERM, nor after the postmaster has died unexpectedly. The
             * first condition is to try to keep from having init SIGKILL the
             * command, and the second is to avoid conflicts with another
             * archiver spawned by a newer postmaster.
             */
            if (t_thrd.worker_sig_flags.got_SIGTERM || !PostmasterIsAlive()) {
                return;
            }

            /*
             * Check for config update.  This is so that we'll adopt a new
             * setting for archive_command as soon as possible, even if there
             * is a backlog of files to be archived.
             */
            if (t_thrd.worker_sig_flags.got_SIGHUP) {
                ProcessConfigFile(PGC_SIGHUP);
                if (!XLogArchivingActive()) {
                    return;
                }
                t_thrd.worker_sig_flags.got_SIGHUP = false;
            }

            /* can't do anything if no command ... */
            if (!XLogArchiveCommandSet() && !XLogArchiveDestSet()) {
                ereport(WARNING, (errmsg("archive_mode enabled, yet archive_command or archive_dest is not set")));
                return;
            }

            if (pgarch_archiveXlog(xlog)) {
                /* successful */
                pgarch_archiveDone(xlog);
                break; /* out of inner retry loop */
            } else {
                if (++failures >= NUM_ARCHIVE_RETRIES) {
                    ereport(WARNING,
                        (errmsg("xlog file \"%s\" could not be archived: too many failures", xlog)));
                    return; /* give up archiving for now */
                }
                pg_usleep(1000000L); /* wait a bit before retrying */
            }
        }
    }
}

/*
 * PgarchArchiveXlogToDest
 *
 * Invokes read/write to copy one archive file to wherever it should go
 *
 * Returns true if successful
 */
static bool PgarchArchiveXlogToDest(const char* xlog)
{
    int fdSrc = -1;
    int fdDest = -1;
    char srcPath[PATH_MAX + 1] = {0};
    char destPath[PATH_MAX + 1] = {0};
    char archPath[PATH_MAX + 1] = {0};
    char activitymsg[MAXFNAMELEN + 16];
    long int fileBytes = 0;
    int rc = 0;
    char* retVal = NULL;

    if (xlog == NULL) {
        return false;
    }
    
    rc = snprintf_s(srcPath, PATH_MAX, PATH_MAX - 1, "%s/%s", SS_XLOGDIR, xlog);
    securec_check_ss(rc, "\0", "\0");

    retVal = realpath(u_sess->attr.attr_storage.XLogArchiveDest, destPath);
    if (retVal == NULL) {
        ereport(FATAL, (errmsg_internal("realpath dest %s failed:%m\n", u_sess->attr.attr_storage.XLogArchiveDest)));
    }
    rc = snprintf_s(archPath, PATH_MAX, PATH_MAX - 1, "%s/%s", destPath, xlog);
    securec_check_ss(rc, "\0", "\0");

    if ((fdSrc = open(srcPath, O_RDONLY, S_IRUSR | S_IWUSR)) >= 0) {
        if ((fdDest = open(archPath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR)) >= 0) {
            char pbuff[ARCHIVE_BUF_SIZE] = {0};

            while (true) {
                ThreadIoTimer read_timer;
                fileBytes = read(fdSrc, pbuff, sizeof(pbuff));
                if (fileBytes <= 0) {
                    break;
                }
                if (PgstatThreadIoEnabled()) {
                    pgstat_track_thread_io(IO_OBJECT_WAL, IO_CONTEXT_NORMAL,
                        (uint64)fileBytes, read_timer.elapsed_us(), false);
                }

                {
                    ThreadIoTimer write_timer;
                    if (write(fdDest, pbuff, fileBytes) != fileBytes) {
                        close(fdSrc);
                        ereport(FATAL, (errmsg_internal("could not write file\"%s\":%m\n", archPath)));
                    }
                    if (PgstatThreadIoEnabled()) {
                        pgstat_track_thread_io(IO_OBJECT_ARCHIVE, IO_CONTEXT_NORMAL,
                            (uint64)fileBytes, write_timer.elapsed_us(), true);
                    }
                }
                (void)memset_s(pbuff, sizeof(pbuff), 0, sizeof(pbuff));
            }

            close(fdSrc);
            close(fdDest);

            if (fileBytes < 0) {
                ereport(FATAL, (errmsg_internal("could not read file\"%s\":%m\n", xlog)));
            }

            g_instance.WalSegmentArchSucceed = true;
            ereport(DEBUG1, (errmsg("archived transaction log file \"%s\"", xlog)));
            
            rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "last was %s", xlog);
            securec_check_ss(rc, "\0", "\0");
            set_ps_display(activitymsg, false);
            
            return true;
        } else {
            close(fdSrc);
            ereport(FATAL, (errmsg_internal("could not open archive dest file \"%s\":%m\n", archPath)));
        }
    } else {
        ereport(FATAL, (errmsg_internal("could not open archive src file \"%s\":%m\n", srcPath)));
    }

    return false;
}

/*
 * Update archived xlog LSN for barrier.
 *
 */
static inline void UpdateArchivedLsn(XLogRecPtr targetLsn)
{
    ArchiveTaskStatus *archive_task_status = nullptr;
    archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archive_task_status == nullptr) {
        return;
    }
    archive_task_status->archived_lsn = targetLsn;
}

#ifndef ENABLE_LITE_MODE
/*
 * pgarch_ArchiverObsCopyLoop
 *
 * Archives all outstanding xlogs then returns
 */
static void pgarch_ArchiverObsCopyLoop(XLogRecPtr flushPtr, doArchive fun)
{
    ereport(LOG,
        (errmsg("pgarch_ArchiverObsCopyLoop")));
    struct timeval tv;
    long currTimestamp = 0;
    const int millitosec = 1000;
    bool time_to_stop = false;

    /*
     * loop through all xlogs with archive_status of .ready and archive
     * them...mostly we expect this to be a single file, though it is possible
     * some backend will add files onto the list of those that need archiving
     * while we are still copying earlier archives
     */
    do {
        XLogRecPtr targetLsn;
        time_to_stop = t_thrd.arch.ready_to_stop;

        /*
         * Do not initiate any more archive commands after receiving
         * SIGTERM, nor after the postmaster has died unexpectedly. The
         * first condition is to try to keep from having init SIGKILL the
         * command, and the second is to avoid conflicts with another
         * archiver spawned by a newer postmaster.
         */
        if (t_thrd.worker_sig_flags.got_SIGTERM || !PostmasterIsAlive() || time_to_stop) {
            return;
        }

        /*
         * Check for config update.  This is so that we'll adopt a new
         * setting for archive_command as soon as possible, even if there
         * is a backlog of files to be archived.
         */
        if (t_thrd.worker_sig_flags.got_SIGHUP) {
            ProcessConfigFile(PGC_SIGHUP);
            t_thrd.worker_sig_flags.got_SIGHUP = false;
        }
        if (flushPtr == InvalidXLogRecPtr) {
            targetLsn = t_thrd.arch.pitr_task_last_lsn + OBS_XLOG_SLICE_BLOCK_SIZE -
                        (t_thrd.arch.pitr_task_last_lsn % OBS_XLOG_SLICE_BLOCK_SIZE) - 1;
        } else {
            targetLsn = Min(t_thrd.arch.pitr_task_last_lsn + OBS_XLOG_SLICE_BLOCK_SIZE -
                        (t_thrd.arch.pitr_task_last_lsn % OBS_XLOG_SLICE_BLOCK_SIZE) - 1,
                        flushPtr);
        }
        /* The previous slice has been archived, switch to the next. */
        if (t_thrd.arch.pitr_task_last_lsn == targetLsn) {
            targetLsn = Min(targetLsn + OBS_XLOG_SLICE_BLOCK_SIZE, flushPtr);
        }

        if (fun(targetLsn) == false) {
            ereport(WARNING,
                (errmsg("xlog file \"%X/%X\" could not be archived: try again", 
                    (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
            pg_usleep(1000000L); /* wait a bit before retrying */
        } else {
            if (g_instance.roach_cxt.isXLogForceRecycled && !g_instance.roach_cxt.forceAdvanceSlotTigger) {
                g_instance.roach_cxt.isXLogForceRecycled = false;
                ereport(LOG, (errmsg("PgArch force advance slot success")));
            }
            gettimeofday(&tv, NULL);
            currTimestamp = TIME_GET_MILLISEC(tv);
            t_thrd.arch.pitr_task_last_lsn = targetLsn;
            if (currTimestamp - t_thrd.arch.last_slot_advance_time  >
                (u_sess->attr.attr_storage.archive_interval * millitosec)) {
                AdvanceArchiveSlot(targetLsn);
                t_thrd.arch.last_slot_advance_time = currTimestamp;
            }
            t_thrd.arch.last_arch_time = currTimestamp;
            UpdateArchivedLsn(targetLsn);
            ResetLatch(&t_thrd.arch.mainloop_latch);
            ereport(LOG,
                (errmsg("pgarch_ArchiverObsCopyLoop time change to %ld", t_thrd.arch.last_arch_time)));
        }
    } while (XLByteLT(t_thrd.arch.pitr_task_last_lsn, flushPtr));
}
#endif

/*
 * pgarch_archiveXlog
 *
 * Invokes system(3) to copy one archive file to wherever it should go
 *
 * Returns true if successful
 */
static bool pgarch_archiveXlog(char* xlog)
{
    char xlogarchcmd[MAXPGPATH];
    char pathname[MAXPGPATH];
    char activitymsg[MAXFNAMELEN + 16];
    char* dp = NULL;
    char* endp = NULL;
    const char* sp = NULL;
    int rc = 0;

    rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", SS_XLOGDIR, xlog);
    securec_check_ss(rc, "\0", "\0");

    /* archive_dest is preferred over archive_command */
    if (XLogArchiveDestSet()) {
        return PgarchArchiveXlogToDest(xlog);
    }

#ifndef ENABLE_LITE_MODE
    rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", g_instance.attr.attr_common.data_directory,
        SS_XLOGDIR, xlog);
    securec_check_ss(rc, "\0", "\0");
#endif

    /*
     * construct the command to be executed
     */
    dp = xlogarchcmd;
    endp = xlogarchcmd + MAXPGPATH - 1;
    *endp = '\0';

    for (sp = u_sess->attr.attr_storage.XLogArchiveCommand; *sp; sp++) {
        if (*sp == '%') {
            switch (sp[1]) {
                case 'p':
                    /* %p: relative path of source file */
                    sp++;
                    strlcpy(dp, pathname, endp - dp);
                    make_native_path(dp);
                    dp += strlen(dp);
                    break;
                case 'f':
                    /* %f: filename of source file */
                    sp++;
                    strlcpy(dp, xlog, endp - dp);
                    dp += strlen(dp);
                    break;
                case '%':
                    /* convert %% to a single % */
                    sp++;
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
                default:
                    /* otherwise treat the % as not special */
                    if (dp < endp) {
                        *dp++ = *sp;
                    }
                    break;
            }
        } else {
            if (dp < endp) {
                *dp++ = *sp;
            }
        }
    }
#ifndef ENABLE_LITE_MODE
    *dp++ = '\n';
#endif
    *dp = '\0';

    ereport(DEBUG3, (errmsg_internal("executing archive command \"%s\"", xlogarchcmd)));

    /* Report archive activity in PS display */
    rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "archiving %s", xlog);
    securec_check_ss(rc, "\0", "\0");

    set_ps_display(activitymsg, false);

#ifndef ENABLE_LITE_MODE
    int pid;
    if (!CheckArchiveCmdExecuterExists(&pid, false)) {
        ereport(WARNING, (errmsg("Skip as archive command executer process doesn't exist.")));
        SendPostmasterSignal(PMSIGNAL_START_ARCHIVE_CMD_AGENT);
        g_instance.WalSegmentArchSucceed = false;
        rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "failed on %s", xlog);
        securec_check_ss(rc, "\0", "\0");
        set_ps_display(activitymsg, false);
        return false;
    }

    if (g_instance.attr.attr_common.archiveCmdExecutePipe[1] < 0 ||
        g_instance.attr.attr_common.archiveCmdResultPipe[0] < 0) {
        ereport(WARNING, (errmsg("archive command pipe not initialized")));
        g_instance.WalSegmentArchSucceed = false;
        rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "failed on %s", xlog);
        securec_check_ss(rc, "\0", "\0");
        set_ps_display(activitymsg, false);
        return false;
    }

    /*
     * Send the command to the agent, tagged with a request id. The agent
     * echoes the id back with the exit code, so a result written for an
     * earlier, abandoned command can never be mistaken for the result of
     * this one.
     */
    ArchiveCmdMsg cmdMsg;
    rc = memset_s(&cmdMsg, sizeof(cmdMsg), 0, sizeof(cmdMsg));
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(cmdMsg.cmd, MAXPGPATH, xlogarchcmd);
    securec_check(rc, "\0", "\0");
    cmdMsg.reqId = next_archive_req_id();
    if (!write_full(g_instance.attr.attr_common.archiveCmdExecutePipe[1], &cmdMsg, sizeof(cmdMsg))) {
        ereport(WARNING,
            (errmsg("failed to write archive command to pipe: %m"),
                errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
        return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
    }

    /*
     * Wait for the result of this request. A command abandoned after a
     * timeout is still finished by the agent, and its result stays in the
     * pipe; such stale results are recognized by the request id, discarded,
     * and every command keeps being judged by its own exit code.
     */
    ArchiveCmdResultMsg result;
    rc = memset_s(&result, sizeof(result), 0, sizeof(result));
    securec_check(rc, "\0", "\0");
    bool gotResult = false;
    int64 resultDeadline = now_ms() + ARCHIVE_RESULT_TIMEOUT;
    while (!gotResult) {
        struct pollfd result_fd;
        result_fd.fd = g_instance.attr.attr_common.archiveCmdResultPipe[0];
        result_fd.events = POLLIN;
        int poll_ret = poll_deadline(&result_fd, resultDeadline);
        if (poll_ret < 0) {
            ereport(WARNING,
                (errmsg("poll failed while waiting for archive result: %m"),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
            return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
        } else if (poll_ret == 0) {
            ereport(WARNING,
                (errmsg("archive command timeout after %d seconds", ARCHIVE_RESULT_TIMEOUT / 1000),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
            return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
        }

        ssize_t read_ret = read_full(result_fd.fd, &result, sizeof(result));
        if (read_ret == 0) {
            ereport(WARNING,
                (errmsg("archive command agent exited while running the command"),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
            return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
        } else if (read_ret != (ssize_t)sizeof(result)) {
            ereport(WARNING,
                (errmsg("failed to read archive result from result pipe: %m, read_ret=%zd", read_ret),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
            return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
        }

        if (result.reqId == cmdMsg.reqId) {
            gotResult = true;
        } else if (result.reqId > cmdMsg.reqId) {
            /* cannot happen unless the agent is broken */
            ereport(WARNING,
                (errmsg("unexpected archive result request id %lu", (unsigned long)result.reqId),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
            return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
        } else {
            /*
             * Stale result of a command the archiver abandoned earlier: it
             * has already been consumed (and thereby discarded) by the read
             * above. Do not return here: loop back to the poll at the top
             * and keep waiting for the result of the request just sent.
             */
            ereport(WARNING,
                (errmsg("discarding stale archive result of request %lu", (unsigned long)result.reqId),
                    errdetail("The archive command was: \"%s\" ", xlogarchcmd)));
        }
    }

    if (result.exitCode != 0) {
        /*
         * -1 means the command timed out or its exit status is unknown; that
         * is a normal archive failure (LOG), not a reason to kill the
         * archiver with FATAL and make it restart to retry the same file.
         * Exit codes above 128 keep the shell convention for commands that
         * died on a signal.
         */
        int lev = (result.exitCode > 128) ? FATAL : LOG;
        if (result.exitCode == -1) {
            ereport(lev,
                (errmsg("archive command failed with unknown error or timed out"),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
        } else {
            ereport(lev,
                (errmsg("archive command failed with exit code %d", result.exitCode),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
        }
        return pgarch_reportArchiveFailure(activitymsg, sizeof(activitymsg), xlog);
    }
#else
     rc = gs_popen_security(xlogarchcmd);
     if (rc != 0) {
        g_instance.WalSegmentArchSucceed = false;
        int lev = (WIFSIGNALED(rc) || WEXITSTATUS(rc) > 128) ? FATAL : LOG;
        if (WIFEXITED(rc)) {
            ereport(lev,
                (errmsg("archive command failed with exit code %d", WEXITSTATUS(rc)),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
        } else if (WIFSIGNALED(rc)) {
#if defined(WIN32)
            ereport(lev,
                (errmsg("archive command was terminated by exception 0x%X", WTERMSIG(rc)),
                    errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value."),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
            ereport(lev,
                (errmsg("archive command was terminated by signal %d: %s",
                     WTERMSIG(rc),
                     WTERMSIG(rc) < NSIG ? sys_siglist[WTERMSIG(rc)] : "(unknown)"),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
#else
            ereport(lev,
                (errmsg("archive command was terminated by signal %d", WTERMSIG(rc)),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
#endif
        } else {
            ereport(lev,
                (errmsg("archive command exited with unrecognized status %d", rc),
                    errdetail("The failed archive command was: \"%s\" ", xlogarchcmd)));
        }
        rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "failed on %s", xlog);
        securec_check_ss(rc, "\0", "\0");
        set_ps_display(activitymsg, false);
        return false;
     }
#endif
    g_instance.WalSegmentArchSucceed = true;
    ereport(DEBUG1, (errmsg("archived transaction log file \"%s\"", xlog)));

    rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "last was %s", xlog);
    securec_check_ss(rc, "\0", "\0");

    set_ps_display(activitymsg, false);

    return true;
}

/*
 * pgarch_readyXlog
 *
 * Return name of the oldest xlog file that has not yet been archived.
 * No notification is set that file archiving is now in progress, so
 * this would need to be extended if multiple concurrent archival
 * tasks were created. If a failure occurs, we will completely
 * re-copy the file at the next available opportunity.
 *
 * It is important that we return the oldest, so that we archive xlogs
 * in order that they were written, for two reasons:
 * 1) to maintain the sequential chain of xlogs required for recovery
 * 2) because the oldest ones will sooner become candidates for
 * recycling at time of checkpoint
 *
 * NOTE: the "oldest" comparison will presently consider all segments of
 * a timeline with a smaller ID to be older than all segments of a timeline
 * with a larger ID; the net result being that past timelines are given
 * higher priority for archiving.  This seems okay, or at least not
 * obviously worth changing.
 */
static bool pgarch_readyXlog(char* xlog, int xlog_length)
{
    /*
     * open xlog status directory and read through list of xlogs that have the
     * .ready suffix, looking for earliest file. It is possible to optimise
     * this code, though only a single file is expected on the vast majority
     * of calls, so....
     */
    char XLogArchiveStatusDir[MAXPGPATH];
    char newxlog[MAX_XFN_CHARS + 6 + 1];
    DIR* rldir = NULL;
    struct dirent* rlde = NULL;
    bool found = false;
    int rc = 0;

    rc = snprintf_s(XLogArchiveStatusDir, MAXPGPATH, MAXPGPATH - 1, "%s/archive_status", SS_XLOGDIR);
    securec_check_ss(rc, "", "");

    rldir = AllocateDir(XLogArchiveStatusDir);
    if (rldir == NULL)
        ereport(ERROR,
            (errcode_for_file_access(),
                errmsg("could not open archive status directory \"%s\": %m", XLogArchiveStatusDir)));

    while ((rlde = ReadDir(rldir, XLogArchiveStatusDir)) != NULL) {
        int basenamelen = (int)strlen(rlde->d_name) - 6;

        if (basenamelen >= MIN_XFN_CHARS && basenamelen <= MAX_XFN_CHARS &&
            strspn(rlde->d_name, VALID_XFN_CHARS) >= (size_t)basenamelen &&
            strcmp(rlde->d_name + basenamelen, ".ready") == 0) {
            errno_t rc = EOK;

            if (!found) {
                rc = strcpy_s(newxlog, MAX_XFN_CHARS + 6 + 1, rlde->d_name);
                securec_check(rc, "\0", "\0");

                found = true;
            } else {
                if (strcmp(rlde->d_name, newxlog) < 0) {
                    rc = strcpy_s(newxlog, MAX_XFN_CHARS + 6 + 1, rlde->d_name);
                    securec_check(rc, "\0", "\0");
                }
            }
        }
    }
    FreeDir(rldir);

    if (found) {
        errno_t rc = EOK;

        /* truncate off the .ready */
        newxlog[strlen(newxlog) - 6] = '\0';
        rc = strcpy_s(xlog, (size_t)xlog_length, newxlog);
        securec_check(rc, "\0", "\0");
    }
    return found;
}

/*
 * pgarch_archiveDone
 *
 * Emit notification that an xlog file has been successfully archived.
 * We do this by renaming the status file from NNN.ready to NNN.done.
 * Eventually, a checkpoint process will notice this and delete both the
 * NNN.done file and the xlog file itself.
 */
static void pgarch_archiveDone(const char* xlog)
{
    char rlogready[MAXPGPATH];
    char rlogdone[MAXPGPATH];

    StatusFilePath(rlogready, MAXPGPATH, xlog, ".ready");
    StatusFilePath(rlogdone, MAXPGPATH, xlog, ".done");
    (void)durable_rename(rlogready, rlogdone, WARNING);
}

static void archKill(int code, Datum arg)
{
    setObsArchLatch(NULL);
    volatile WalSnd *walsnd = NULL;
    for (int i = 0; i< g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volitile pointer to prevent code rearrangement */
        walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        SpinLockAcquire(&walsnd->mutex_archive_task_list);
        walsnd->archive_task_count=0;
        SpinLockRelease(&walsnd->mutex_archive_task_list);
    }
    ereport(LOG, (errmsg("arch thread shut down, slotName: %s", t_thrd.arch.slot_name)));
    pfree_ext(t_thrd.arch.slot_name);
    if (t_thrd.arch.archive_config != NULL && t_thrd.arch.archive_config->archive_config.conn_config != NULL) {
        pfree_ext(t_thrd.arch.archive_config->archive_config.conn_config->obs_address);
        pfree_ext(t_thrd.arch.archive_config->archive_config.conn_config->obs_bucket);
        pfree_ext(t_thrd.arch.archive_config->archive_config.conn_config->obs_ak);
        pfree_ext(t_thrd.arch.archive_config->archive_config.conn_config->obs_sk);
    }
}

#ifndef ENABLE_LITE_MODE
/*
 * pgarch_archiveRoachForPitrStandby
 * get signal from walreceiver, fork a roach process to archive xlog
*/
static void pgarch_archiveRoachForPitrStandby()
{
    ArchiveTaskStatus *archive_task_status = NULL;
    archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archive_task_status == NULL) {
        ereport(ERROR,
            (errmsg("pgarch_archiveRoachForPitrStandby failed because could not get an archive task status.")));
    }
    ereport(LOG,
        (errmsg("pgarch_archiveRoachForPitrStandby %s : %X/%X, term:%d, subterm:%d", 
            archive_task_status->slotname,
            (uint32)(archive_task_status->archive_task.targetLsn >> 32), 
            (uint32)(archive_task_status->archive_task.targetLsn), 
            archive_task_status->archive_task.term, 
            archive_task_status->archive_task.sub_term)));
    if (archive_task_status->archive_task.targetLsn == InvalidXLogSegPtr) {
        volatile unsigned int *pitr_task_status = &archive_task_status->pitr_task_status;
        pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);
        ereport(LOG, (errmsg("PgArch standby receive invalid lsn for slot force advance")));
    }
    if (ArchiveReplicationAchiver(&archive_task_status->archive_task) == 0) {
        SpinLockAcquire(&archive_task_status->mutex);
        archive_task_status->pitr_finish_result = true;
        SpinLockRelease(&archive_task_status->mutex);
    } else {
        ereport(WARNING,
            (errmsg("error when pgarch_archiveRoachForPitrStandby %s : %X/%X, term:%d, subterm:%d", 
                archive_task_status->slotname,
                (uint32)(archive_task_status->archive_task.targetLsn >> 32), 
                (uint32)(archive_task_status->archive_task.targetLsn), 
                archive_task_status->archive_task.term, 
                archive_task_status->archive_task.sub_term)));
        SpinLockAcquire(&archive_task_status->mutex);
        archive_task_status->pitr_finish_result = false;
        SpinLockRelease(&archive_task_status->mutex);
    }
}

/*
 * pgarch_archiveRoachForPitrMaster
 * choose a walsender to send archive command
*/
static bool pgarch_archiveRoachForPitrMaster(XLogRecPtr targetLsn)
{
    ArchiveTaskStatus *archive_task_status = NULL;
    archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archive_task_status == NULL) {
        return false;
    }
    SpinLockAcquire(&archive_task_status->mutex);
    archive_task_status->pitr_finish_result = false;
    archive_task_status->archive_task.targetLsn = targetLsn;
    archive_task_status->archive_task.tli = get_controlfile_timeline();
    archive_task_status->archive_task.term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file, 
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    SpinLockRelease(&archive_task_status->mutex);
    if (g_instance.roach_cxt.forceAdvanceSlotTigger) {
        SpinLockAcquire(&archive_task_status->mutex);
        archive_task_status->pitr_finish_result = false;
        archive_task_status->archive_task.targetLsn = InvalidXLogRecPtr;
        SpinLockRelease(&archive_task_status->mutex);
        g_instance.roach_cxt.forceAdvanceSlotTigger = false;
        ereport(LOG, (errmsg("PgArch need force advance this time in primary")));
    }
    /* subterm update when walsender changed */
    int rc = strcpy_s(archive_task_status->archive_task.slot_name, NAMEDATALEN, t_thrd.arch.slot_name);
    securec_check(rc, "\0", "\0");
    ereport(LOG,
        (errmsg("%s : pgarch_archiveRoachForPitrMaster %X/%X", 
            t_thrd.arch.slot_name, (uint32)(targetLsn >> 32), 
            (uint32)(targetLsn))));
    WalSnd* walsnd = pgarch_chooseWalsnd(targetLsn);
    if (walsnd == NULL) {
        ereport(WARNING,
            (errmsg("pgarch_archiveRoachForPitrMaster failed for no health standby %X/%X", 
                (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
        return false;
    }
    archive_task_status->archiver_latch = &t_thrd.arch.mainloop_latch;
    add_archive_task_to_list(t_thrd.arch.archive_task_idx, walsnd);
    SetLatch(&walsnd->latch);
    ResetLatch(&t_thrd.arch.mainloop_latch);
    rc = WaitLatch(&t_thrd.arch.mainloop_latch,
        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 3000L);

    if (rc & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }
    if (rc & WL_TIMEOUT) {
        return false;
    }
    /*
     * check targetLsn and g_instance.archive_obs_cxt.archive_task.targetLsn for deal message with wrong order
     */
    SpinLockAcquire(&archive_task_status->mutex);
    if (archive_task_status->pitr_finish_result == true 
        && XLByteEQ(archive_task_status->archive_task.targetLsn, targetLsn)) {
        archive_task_status->pitr_finish_result = false;
        SpinLockRelease(&archive_task_status->mutex);
        return true;
    } else {
        SpinLockRelease(&archive_task_status->mutex);
        return false;
    }
}

/*
 * pgarch_archiveRoachForPitrMaster
 * choose a walsender to send archive command
*/
static bool pgarch_archiveRoachForCoordinator(XLogRecPtr targetLsn)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    ArchiveTaskStatus *archive_task_status = NULL;
    archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archive_task_status == NULL) {
        ereport(ERROR,
            (errmsg("pgarch_archiveRoachForCoordinator failed because could not get an archive task status.")));
    }
    ArchiveXlogMessage archive_xlog_info;
    archive_xlog_info.targetLsn = targetLsn;
    archive_xlog_info.term = 0;
    archive_xlog_info.sub_term = 0;
    archive_xlog_info.tli = 0;
    if (ArchiveReplicationAchiver(&archive_xlog_info) != 0) {
        archive_task_status->pitr_finish_result = false;
        ereport(WARNING,
            (errmsg("error when pgarch_archiveRoachForCoordinator %s : %X/%X, term:%d, subterm:%d", 
                archive_task_status->slotname,
                (uint32)(archive_task_status->archive_task.targetLsn >> 32), 
                (uint32)(archive_task_status->archive_task.targetLsn), 
                archive_task_status->archive_task.term, 
                archive_task_status->archive_task.sub_term)));
        ResetLatch(&t_thrd.arch.mainloop_latch);
        int rc;
        /* wait and try again */
        rc = WaitLatch(&t_thrd.arch.mainloop_latch,
            WL_TIMEOUT | WL_POSTMASTER_DEATH, 500);
        return false;

    } else {
        archive_task_status->pitr_finish_result = true;
        archive_task_status->archive_task.targetLsn = targetLsn;
        update_archive_start_end_location_file(archive_task_status->archive_task.targetLsn,
                    TIME_GET_MILLISEC(tv));
    }
    ereport(LOG,
        (errmsg("pgarch_archiveRoachForCoordinator %X/%X", (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
    
    return archive_task_status->pitr_finish_result;
}

/* check if there is any wal sender alive. */
static WalSnd* pgarch_chooseWalsnd(XLogRecPtr targetLsn)
{
    int i;
    volatile WalSnd* walsnd = NULL;
    ereport(DEBUG1, (errmsg("pgarch current walsender index: %d and last walsender index: %d",
        t_thrd.arch.sync_walsender_idx, g_instance.archive_obs_cxt.chosen_walsender_index)));
    t_thrd.arch.sync_walsender_idx = g_instance.archive_obs_cxt.chosen_walsender_index;
    if (t_thrd.arch.sync_walsender_idx >= 0) {
        walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[t_thrd.arch.sync_walsender_idx];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole) 
            && !XLogRecPtrIsInvalid(walsnd->flush) && XLByteLE(targetLsn, walsnd->flush) &&
            walsnd->is_cross_cluster == false && !strstr((char*)walsnd->remote_application_name, "gs_probackup")) {
            SpinLockRelease(&walsnd->mutex);
            if (g_instance.roach_cxt.isXLogForceRecycled) {
                ereport(LOG, (errmsg("pgarch choose walsender index:%d and pid is %ld, when force advance slot",
                    t_thrd.arch.sync_walsender_idx, walsnd->pid)));
            }
            return (WalSnd*)walsnd;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];

        SpinLockAcquire(&walsnd->mutex);

        if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole) &&
            walsnd->is_cross_cluster == false && !strstr((char*)walsnd->remote_application_name, "gs_probackup")) {
            if (XLByteLE(targetLsn, walsnd->flush)) {
                SpinLockRelease(&walsnd->mutex);
                ArchiveTaskStatus *archive_status = NULL;
                archive_status = find_archive_task_status(t_thrd.arch.slot_name);
                if (archive_status == NULL) {
                    ereport(ERROR, (errmsg("pgarch_chooseWalsnd has change from %d to %d, but not find slot",
                        t_thrd.arch.sync_walsender_idx, i)));
                }
                archive_status->sync_walsender_term++;
                ereport(LOG, (errmsg("pgarch_chooseWalsnd has change from %d to %d , sub_term:%d",
                    t_thrd.arch.sync_walsender_idx, i, archive_status->sync_walsender_term)));
                t_thrd.arch.sync_walsender_idx = i;
                g_instance.archive_obs_cxt.chosen_walsender_index = i;
                return (WalSnd*)walsnd;
            }
        }
        SpinLockRelease(&walsnd->mutex);
    }
    return NULL;
}

static XLogRecPtr GetLastTaskLsnFromServer(ArchiveSlotConfig* obs_archive_slot)
{
    ArchiveXlogMessage obs_archive_info;
    XLogRecPtr pitr_task_last_lsn;

    if (archive_replication_get_last_xlog(&obs_archive_info, &obs_archive_slot->archive_config) == 0) {
        pitr_task_last_lsn =  obs_archive_info.targetLsn;
        ereport(LOG,
            (errmsg("initLastTaskLsn update lsn to  %X/%X from server", (uint32)(pitr_task_last_lsn >> 32),
            (uint32)(pitr_task_last_lsn))));
    } else {
        XLogRecPtr targetLsn = GetFlushRecPtr();
        pitr_task_last_lsn = targetLsn - (targetLsn % XLogSegSize);
        ereport(LOG,
            (errmsg("initLastTaskLsn update lsn to  %X/%X from local", (uint32)(pitr_task_last_lsn >> 32),
            (uint32)(pitr_task_last_lsn))));
    }
    return pitr_task_last_lsn;
}

static void InitArchiverLastTaskLsn(ArchiveSlotConfig* obs_archive_slot)
{
    struct timeval tv;
    load_server_mode();
    gettimeofday(&tv,NULL);

    if (obs_archive_slot == NULL) {
        t_thrd.arch.last_arch_time = TIME_GET_MILLISEC(tv);
        t_thrd.arch.last_slot_advance_time = t_thrd.arch.last_arch_time;
        obs_archive_slot = getArchiveReplicationSlot();
    }
    volatile int *slot_idx = &t_thrd.arch.slot_idx;
    if (obs_archive_slot != NULL && !IsServerModeStandby() && !RecoveryInProgress()) {
        if (likely(*slot_idx != -1) && *slot_idx < g_instance.attr.attr_storage.max_replication_slots) {
            ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[*slot_idx];
            SpinLockAcquire(&slot->mutex);
            if (slot->in_use == true && slot->archive_config != NULL) {
                /*
                 * In old version(<92599), the last task lsn is initialized from archive server or current flush
                 * position, but in new version is initialized from local slot.
                 * During the upgrade, the local restart lsn may be 0, so initialize it with old version way.
                 */
                if (slot->data.restart_lsn == InvalidXLogRecPtr &&
                    t_thrd.proc->workingVersionNum < PITR_INIT_VERSION_NUM) {
                    SpinLockRelease(&slot->mutex);
                    t_thrd.arch.pitr_task_last_lsn = GetLastTaskLsnFromServer(obs_archive_slot);
                } else {
                    t_thrd.arch.pitr_task_last_lsn = slot->data.restart_lsn;
                    SpinLockRelease(&slot->mutex);
                }
            } else {
                SpinLockRelease(&slot->mutex);
                ereport(ERROR, (errcode_for_file_access(), errmsg("slot idx not valid, obs slot %X/%X not advance ",
                    (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32), (uint32)(t_thrd.arch.pitr_task_last_lsn))));
            }
            ereport(LOG, (errmsg("successful init archive slot %X/%X ",
                (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32), (uint32)(t_thrd.arch.pitr_task_last_lsn))));
        } else {
            ereport(WARNING, (errmsg("could not init archive slot cause slot index is invalid: %d", *slot_idx)));
        }
    } else {
        ereport(WARNING, (errmsg("could not init archive slot cause current server mode is %d",
            t_thrd.xlog_cxt.server_mode)));
    }
}

#define ARCHIVE_COMMAND_PID_FILE "archive_cmd.pid"
static bool CheckArchiveCmdExecuterExists(int *pid, bool clean)
{
    char pathname[MAXPGPATH];
    int rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", g_instance.attr.attr_common.data_directory,
        ARCHIVE_COMMAND_PID_FILE);
    securec_check_ss(rc, "\0", "\0");
    FILE* f = fopen(pathname, "r");
    if (!f) {
        return false;
    }

    bool ret = false;
    if (fscanf_s(f, "%d", pid) == 1) {
        ret = kill((*pid), 0);
    }
    fclose(f);
    if (clean) {
        unlink(pathname);
    }
    return !ret;
}

void CleanArchiveCmdExecuter()
{
    int pid;
    if (CheckArchiveCmdExecuterExists(&pid, true)) {
        kill(pid, SIGTERM);
    }
    if (g_instance.attr.attr_common.archiveCmdExecutePipe[1] >= 0) {
        close(g_instance.attr.attr_common.archiveCmdExecutePipe[1]);
        g_instance.attr.attr_common.archiveCmdExecutePipe[1] = -1;
    }
    if (g_instance.attr.attr_common.archiveCmdResultPipe[0] >= 0) {
        close(g_instance.attr.attr_common.archiveCmdResultPipe[0]);
        g_instance.attr.attr_common.archiveCmdResultPipe[0] = -1;
    }
}

void SaveArchiveCmdExecuterPid(int pid)
{
    char pathname[MAXPGPATH];
    int rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", g_instance.attr.attr_common.data_directory,
        ARCHIVE_COMMAND_PID_FILE);
    securec_check_ss(rc, "\0", "\0");
    FILE* f = fopen(pathname, "w");
    if (f) {
        fprintf(f, "%d\n", pid);
        fclose(f);
    }
}

void CleanupArchiveCmdExecuter(int sig)
{
    _exit(0);
}

void InitArchiveCmdExecuter()
{
    g_instance.attr.attr_common.archiveCmdExecutePipe[0] = -1;
    g_instance.attr.attr_common.archiveCmdExecutePipe[1] = -1;
    g_instance.attr.attr_common.archiveCmdResultPipe[0] = -1;
    g_instance.attr.attr_common.archiveCmdResultPipe[1] = -1;
    CleanArchiveCmdExecuter();
    if (pipe(g_instance.attr.attr_common.archiveCmdExecutePipe) < 0) {
        ereport(WARNING, (errcode_for_socket_access(), (errmsg("could not create pipe for archive command: %m"))));
        return;
    }

    if (pipe(g_instance.attr.attr_common.archiveCmdResultPipe) < 0) {
        ereport(WARNING, (errcode_for_socket_access(),
            (errmsg("could not create result pipe for archive command: %m"))));
        close(g_instance.attr.attr_common.archiveCmdExecutePipe[0]);
        close(g_instance.attr.attr_common.archiveCmdExecutePipe[1]);
        g_instance.attr.attr_common.archiveCmdExecutePipe[0] = -1;
        g_instance.attr.attr_common.archiveCmdExecutePipe[1] = -1;
        return;
    }

    int cmdAgentPid = fork();
    if (cmdAgentPid == 0) {
        IsUnderPostmaster = true;
        set_ps_display("archiver", true);
        signal(SIGTERM, CleanupArchiveCmdExecuter);
        prctl(PR_SET_PDEATHSIG, SIGTERM);
        prctl(PR_SET_NAME, "archiver");
        if (getppid() == 1) {
            _exit(0);
        }
        close(g_instance.attr.attr_common.archiveCmdExecutePipe[1]);
        close(g_instance.attr.attr_common.archiveCmdResultPipe[0]);
        for (;;) {
            ArchiveCmdMsg cmdMsg;
            ssize_t n = read_full(g_instance.attr.attr_common.archiveCmdExecutePipe[0], &cmdMsg, sizeof(cmdMsg));
            if (n == 0) {
                /* the archiver closed the pipe: normal shutdown */
                break;
            }
            if (n < 0) {
                ereport(WARNING, (errmsg("failed to read archive command from pipe: %m")));
                break;
            }

            char* cmd = cmdMsg.cmd;
            size_t cmd_len = strlen(cmd);
            if (cmd_len > 0 && cmd[cmd_len - 1] == '\n') {
                cmd[cmd_len - 1] = '\0';
            }

            int output_pipe[2];
            int exit_code = -1;
            if (pipe(output_pipe) < 0) {
                ereport(WARNING, (errmsg("failed to create output pipe for command: %s, error: %m", cmd)));
            } else {
                int pid = fork();
                if (pid == 0) {
                    close(output_pipe[0]);
                    close(g_instance.attr.attr_common.archiveCmdExecutePipe[0]);
                    close(g_instance.attr.attr_common.archiveCmdResultPipe[1]);
                    dup2(output_pipe[1], fileno(stdout));
                    dup2(output_pipe[1], fileno(stderr));
                    close(output_pipe[1]);
                    execl("/bin/sh", "sh", "-c", cmd, (char*)NULL);
                    _exit(127);
                } else if (pid > 0) {
                    close(output_pipe[1]);
                    char output_buf[MAXPGPATH] = {0};
                    ssize_t total = 0;
                    ssize_t bytes_read = 0;
                    int status = 0;
                    bool status_valid = false; /* status was filled by a successful waitpid */
                    bool timed_out = false;
                    bool wait_failed = false;

                    /*
                     * Total budget for one command, well below the archiver's
                     * ARCHIVE_RESULT_TIMEOUT, so the result normally reaches
                     * the archiver before the archiver abandons the request.
                     */
                    const int archive_cmd_timeout_ms = 60 * 1000;
                    int64 deadline = now_ms() + archive_cmd_timeout_ms;

                    /* read command output until EOF, buffer full, or deadline */
                    while (total < (ssize_t)sizeof(output_buf) - 1) {
                        struct pollfd output_fd;
                        output_fd.fd = output_pipe[0];
                        output_fd.events = POLLIN | POLLHUP;
                        int poll_ret = poll_deadline(&output_fd, deadline);
                        if (poll_ret < 0) {
                            ereport(WARNING, (errmsg("poll failed for command output: %m")));
                            break;
                        }
                        if (poll_ret == 0) {
                            /* out of time; the wait loop below handles it */
                            break;
                        }
                        if ((output_fd.revents & (POLLIN | POLLHUP | POLLERR)) == 0) {
                            continue;
                        }
                        bytes_read = read(output_pipe[0], output_buf + total,
                            sizeof(output_buf) - 1 - total);
                        if (bytes_read < 0) {
                            if (errno == EINTR) {
                                continue;
                            }
                            ereport(WARNING, (errmsg("failed to read command output: %m")));
                            break;
                        }
                        if (bytes_read == 0) {
                            /* EOF: all writers closed the output */
                            break;
                        }
                        total += bytes_read;
                    }
                    output_buf[total] = '\0';

                    /* wait for the command to exit, bounded by the same deadline */
                    for (;;) {
                        pid_t wait_ret = waitpid_retry(pid, &status, WNOHANG);
                        if (wait_ret < 0) {
                            wait_failed = true;
                            break;
                        }
                        if (wait_ret > 0) {
                            status_valid = true;
                            break;
                        }
                        if (now_ms() >= deadline) {
                            timed_out = true;
                            break;
                        }
                        /*
                         * Sleep briefly and recheck. A plain sleep is used on
                         * purpose: polling for POLLHUP here could spin at full
                         * speed, because POLLHUP stays reported while the
                         * command keeps its output closed but keeps running.
                         */
                        pg_usleep(200 * 1000);
                    }

                    if (wait_failed) {
                        ereport(WARNING, (errmsg("waitpid failed for command: %s: %m", cmd)));
                        exit_code = -1;
                    } else if (timed_out) {
                        ereport(WARNING,
                            (errmsg("archive command timeout after %d seconds for: %s",
                                archive_cmd_timeout_ms / 1000, cmd)));
                        (void)kill(pid, SIGKILL);
                        /* reap the command; the status only reflects our own kill */
                        (void)waitpid_retry(pid, &status, 0);
                        exit_code = -1;
                    } else if (WIFEXITED(status)) {
                        exit_code = WEXITSTATUS(status);
                    } else if (WIFSIGNALED(status)) {
                        /* report signal deaths the way a shell does, so the
                         * archiver reacts as it did to system() results */
                        exit_code = 128 + WTERMSIG(status);
                    } else {
                        exit_code = -1;
                    }

                    close(output_pipe[0]);
                    if (exit_code != 0) {
                        ereport(WARNING,
                            (errmsg("archive command output: %s",
                                total > 0 ? output_buf : "no output from command")));
                        if (status_valid && WIFEXITED(status)) {
                            int lev = WEXITSTATUS(status) > 128 ? FATAL : LOG;
                            ereport(lev,
                                (errmsg("archive command failed with exit code %d", WEXITSTATUS(status)),
                                    errdetail("The failed archive command was: \"%s\" ", cmd)));
                        } else if (status_valid && WIFSIGNALED(status)) {
                            ereport(FATAL,
                                (errmsg("archive command was terminated by signal %d", WTERMSIG(status)),
                                    errdetail("The failed archive command was: \"%s\" ", cmd)));
                        } else if (status_valid) {
                            ereport(LOG,
                                (errmsg("archive command exited with unrecognized status %d", status),
                                    errdetail("The failed archive command was: \"%s\" ", cmd)));
                        }
                        /*
                         * Otherwise status was never filled by a successful
                         * waitpid: the waitpid-failure and timeout paths have
                         * already logged their own specific message, and
                         * interpreting the initial status 0 would wrongly
                         * report "failed with exit code 0".
                         */
                    }
                } else {
                    close(output_pipe[0]);
                    close(output_pipe[1]);
                    exit_code = -1;
                }
            }

            ArchiveCmdResultMsg resultMsg;
            errno_t rc = EOK;
            rc = memset_s(&resultMsg, sizeof(resultMsg), 0, sizeof(resultMsg));
            securec_check(rc, "\0", "\0");
            resultMsg.reqId = cmdMsg.reqId;
            resultMsg.exitCode = exit_code;
            if (!write_full(g_instance.attr.attr_common.archiveCmdResultPipe[1], &resultMsg, sizeof(resultMsg))) {
                ereport(WARNING, (errmsg("failed to write exit code to result pipe: %m")));
            }
        }
        close(g_instance.attr.attr_common.archiveCmdExecutePipe[0]);
        close(g_instance.attr.attr_common.archiveCmdResultPipe[1]);
        _exit(0);
    }

    SaveArchiveCmdExecuterPid(cmdAgentPid);
    close(g_instance.attr.attr_common.archiveCmdExecutePipe[0]);
    close(g_instance.attr.attr_common.archiveCmdResultPipe[1]);
    return;
}
#endif
