/*
 *
 * pgarch.cpp
 *
 *	PostgreSQL WAL archiver
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
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/copydir.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/ps_status.h"

#include "gssignal/gs_signal.h"
#include "alarm/alarm.h"
#include "replication/syncrep.h"
#include "replication/slot.h"
#include "replication/walsender_private.h"
#include "replication/walsender.h"
#include "pgxc/pgxc.h"
#include "replication/obswalreceiver.h"
#include "replication/walreceiver.h"
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

#define NUM_ARCHIVE_RETRIES 3

#define ARCHIVE_BUF_SIZE (1024 * 1024)

/* 
 * Timeout interval for sending the status of the archive thread on the standby node
 */
#define UPDATE_STATUS_WAIT 3000

/* the status of archiver on standby */
#define ARCHIVE_ON true
#define ARCHIVE_OFF false

#define STANDBY_ARCHIVE_XLOG_UPGRADE_VERSION 92299

/* the interval of sending archive status is 1s. */
#define SEND_ARCHIVE_STATUS_INTERVAL 1000000L

NON_EXEC_STATIC void PgArchiverMain();
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
static void pgarch_archiveRoachForPitrStandby();
static void CreateArchiveReadyFile();
static bool pgarch_archiveRoachForPitrMaster(XLogRecPtr targetLsn);
static bool pgarch_archiveRoachForCoordinator(XLogRecPtr targetLsn);
static WalSnd* pgarch_chooseWalsnd(XLogRecPtr targetLsn);
typedef bool(*doArchive)(XLogRecPtr);
static void pgarch_ArchiverObsCopyLoop(XLogRecPtr flushPtr, doArchive fun);
static void archKill(int code, Datum arg);
static void initLastTaskLsn();
static bool SendArchiveThreadStatusInternal(bool is_archive_activited);
static void SendArchiveThreadStatus(bool status);
static bool NotifyPrimaryArchiverActived(ThreadId* last_walrcv_pid, bool* sent_archive_status);
static void NotifyPrimaryArchiverShutdown(bool sent_archive_status);
static void ChangeArchiveTaskStatus2Done();

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
ThreadId pgarch_start(void)
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
NON_EXEC_STATIC void PgArchiverMain()
{
    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    t_thrd.proc_cxt.MyStartTime = time(NULL); /* record Start Time for logging */

    t_thrd.proc_cxt.MyProgName = "PgArchiver";

    t_thrd.myLogicTid = noProcLogicTid + PGARCH_LID;

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

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    on_shmem_exit(archKill, 0);

    /*
     * Identify myself via ps
     */
    init_ps_display("archiver process", "", "", "");
    setObsArchLatch(&t_thrd.arch.mainloop_latch);
    SetStandbyArchLatch(&t_thrd.arch.mainloop_latch);
    initLastTaskLsn();
    pgarch_MainLoop();

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
    t_thrd.arch.got_SIGHUP = true;
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
    t_thrd.arch.got_SIGTERM = true;
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
    ereport(DEBUG1, (errmsg("pgarch_waken")));
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
    doArchive fun = NULL;
    bool sent_archive_status = false;
    ThreadId last_walrcv_pid = 0;

    /*
     * We run the copy loop immediately upon entry, in case there are
     * unarchived files left over from a previous database run (or maybe the
     * archiver died unexpectedly).  After that we wait for a signal or
     * timeout before doing more.
     */
    t_thrd.arch.wakened = true;
    ReplicationSlot *obs_archive_slot = NULL;

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
        if (t_thrd.arch.got_SIGHUP) {
            t_thrd.arch.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            if (!XLogArchivingActive()) {
                NotifyPrimaryArchiverShutdown(sent_archive_status);
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
        if (t_thrd.arch.got_SIGTERM) {
            time_t icurtime = time(NULL);

            if (t_thrd.arch.last_sigterm_time == 0)
                t_thrd.arch.last_sigterm_time = icurtime;
            else if ((unsigned int)(icurtime - t_thrd.arch.last_sigterm_time) >= (unsigned int)60)
                break;
        }
        load_server_mode();
        if (IsServerModeStandby()) {
            /*
             * this step was used by standby to notify primary the archive thread is actived
             */
            if (obs_archive_slot == NULL && t_thrd.proc->workingVersionNum >= STANDBY_ARCHIVE_XLOG_UPGRADE_VERSION &&
                !NotifyPrimaryArchiverActived(&last_walrcv_pid, &sent_archive_status)) {
                pg_usleep(SEND_ARCHIVE_STATUS_INTERVAL);
                continue;
            }
            /* if we should do pitr archive, for standby */
            volatile unsigned int *pitr_task_status = &g_instance.archive_obs_cxt.pitr_task_status;
            if (unlikely(pg_atomic_read_u32(pitr_task_status) == PITR_TASK_GET)) {
                pgarch_archiveRoachForPitrStandby();
                pg_atomic_write_u32(pitr_task_status, PITR_TASK_DONE);
            }

            /* if we should do archive by standby, we should create a .ready file*/
            volatile unsigned int* arch_task_status = &g_instance.archive_standby_cxt.arch_task_status;
            if (unlikely(pg_atomic_read_u32(arch_task_status) == ARCH_TASK_GET)) {
                CreateArchiveReadyFile();
            }
        }

        /* Do what we're here for */
        if (t_thrd.arch.wakened || time_to_stop) {
            t_thrd.arch.wakened = false;
            obs_archive_slot = getObsReplicationSlot();
            if (obs_archive_slot != NULL && !IsServerModeStandby()) {
                gettimeofday(&curtime, NULL);
                const long time_diff = TIME_GET_MILLISEC(curtime) -  t_thrd.arch.last_arch_time;
                XLogRecPtr receivePtr;
                XLogRecPtr writePtr;
                XLogRecPtr flushPtr;
                XLogRecPtr replayPtr;
                bool got_recptr = false;
                bool amSync = false;
                if (IS_PGXC_COORDINATOR) {
                    flushPtr = GetFlushRecPtr();
                } else {
                    got_recptr = SyncRepGetSyncRecPtr(&receivePtr, &writePtr, &flushPtr, &replayPtr, &amSync, false);
                    if (got_recptr != true) {
                        ereport(ERROR,
                            (errmsg("pgarch_ArchiverObsCopyLoop failed when call SyncRepGetSyncRecPtr")));
                    }
                }
                if (time_diff >= t_thrd.arch.task_wait_interval 
                    || XLByteDifference(flushPtr, t_thrd.arch.pitr_task_last_lsn) >= OBS_XLOG_SLICE_BLOCK_SIZE) {
                    if (IS_PGXC_COORDINATOR) {
                        fun = &pgarch_archiveRoachForCoordinator;
                    } else {
                        fun = &pgarch_archiveRoachForPitrMaster;
                    }
                    pgarch_ArchiverObsCopyLoop(flushPtr, fun);
                    advanceObsSlot(flushPtr);
                }
            } else {
                pgarch_ArchiverCopyLoop();
                gettimeofday(&last_copy_time, NULL);
            }
        }

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
                wait_interval = IsServerModeStandby() ? t_thrd.arch.task_wait_interval : PGARCH_AUTOWAKE_INTERVAL;
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
    } while (PostmasterIsAlive() && XLogArchivingActive() && !time_to_stop);

    /* 
     * This step don't need to check the version number, because we have checked the version in the loop.
     * 
     * If the version number is bigger than STANDBY_ARCHIVE_XLOG_UPGRADE_VERSION and we notify the primary successfully
     * in the loop, the sent_archive_status will be set to true. We don't need to send the archive status when
     * the sent_archive_status is false, so we don't need to check the version number in this step.
     */
    NotifyPrimaryArchiverShutdown(sent_archive_status);
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
            if (t_thrd.arch.got_SIGTERM || !PostmasterIsAlive())
                return;

            /*
             * Check for config update.  This is so that we'll adopt a new
             * setting for archive_command as soon as possible, even if there
             * is a backlog of files to be archived.
             */
            if (t_thrd.arch.got_SIGHUP) {
                ProcessConfigFile(PGC_SIGHUP);
                if (!XLogArchivingActive()) {
                    return;
                }
                t_thrd.arch.got_SIGHUP = false;
            }

            /* can't do anything if no command ... */
            if (!XLogArchiveCommandSet() && !XLogArchiveDestSet()) {
                ereport(WARNING, (errmsg("archive_mode enabled, yet archive_command or archive_dest is not set")));
                return;
            }

            if (pgarch_archiveXlog(xlog)) {
                /* successful */
                pgarch_archiveDone(xlog);

                /* archive xlog on standby success, we need to change the status of archive task. */
                ChangeArchiveTaskStatus2Done();
                break; /* out of inner retry loop */
            } else {
                if (++failures >= NUM_ARCHIVE_RETRIES) {
                    ereport(WARNING,
                        (errmsg("transaction log file \"%s\" could not be archived: too many failures", xlog)));
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
    char activitymsg[MAXFNAMELEN + 16];
    long int fileBytes = 0;
    int rc = 0;
    char tempPath[PATH_MAX] = {0};
    char* retVal = NULL;

    if (xlog == NULL) {
        return false;
    }
    
    rc = snprintf_s(tempPath, PATH_MAX, PATH_MAX - 1, XLOGDIR "/%s", xlog);
    securec_check_ss(rc, "\0", "\0");
    retVal = realpath(tempPath, srcPath);
    if (retVal == NULL) {
        ereport(FATAL, (errmsg_internal("realpath src %s failed:%m\n", tempPath)));
    }
    retVal = realpath(u_sess->attr.attr_storage.XLogArchiveDest, destPath);
    if (retVal == NULL) {
        ereport(FATAL, (errmsg_internal("realpath dest %s failed:%m\n", u_sess->attr.attr_storage.XLogArchiveDest)));
    }
    rc = snprintf_s(destPath, PATH_MAX, PATH_MAX - 1, "%s/%s", destPath, xlog);
    securec_check_ss(rc, "\0", "\0");

    if ((fdSrc = open(srcPath, O_RDONLY)) >= 0) {                
        if ((fdDest = open(destPath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR)) >= 0) {
            char pbuff[ARCHIVE_BUF_SIZE] = {0};

            while ((fileBytes = read(fdSrc, pbuff, sizeof(pbuff))) > 0) {
                if (write(fdDest, pbuff, fileBytes) != fileBytes) {
                    close(fdSrc);
                    ereport(FATAL, (errmsg_internal("could not write file\"%s\":%m\n", srcPath)));
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
            ereport(FATAL, (errmsg_internal("could not open archive dest file \"%s\":%m\n", destPath)));
        }
    } else {
        ereport(FATAL, (errmsg_internal("could not open archive src file \"%s\":%m\n", srcPath)));
    }

    return false;
}


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
        if (t_thrd.arch.got_SIGTERM || !PostmasterIsAlive() || time_to_stop) {
            return;
        }

        /*
         * Check for config update.  This is so that we'll adopt a new
         * setting for archive_command as soon as possible, even if there
         * is a backlog of files to be archived.
         */
        if (t_thrd.arch.got_SIGHUP) {
            ProcessConfigFile(PGC_SIGHUP);
            if (!XLogArchivingActive()) {
                return;
            }
            t_thrd.arch.got_SIGHUP = false;
        }
        targetLsn = Min(t_thrd.arch.pitr_task_last_lsn + OBS_XLOG_SLICE_BLOCK_SIZE -
                        (t_thrd.arch.pitr_task_last_lsn % OBS_XLOG_SLICE_BLOCK_SIZE) - 1,
                        flushPtr);

        /* The previous slice has been archived, switch to the next. */
        if (t_thrd.arch.pitr_task_last_lsn == targetLsn) {
            targetLsn = Min(targetLsn + OBS_XLOG_SLICE_BLOCK_SIZE, flushPtr);
        }

        if (fun(targetLsn) == false) {
            ereport(WARNING,
                (errmsg("transaction log file \"%X/%X\" could not be archived: try again", 
                    (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
            pg_usleep(1000000L); /* wait a bit before retrying */
        } else {
            gettimeofday(&tv, NULL);
            t_thrd.arch.last_arch_time = TIME_GET_MILLISEC(tv);
            t_thrd.arch.pitr_task_last_lsn = targetLsn;
            ResetLatch(&t_thrd.arch.mainloop_latch);
            ereport(LOG,
                (errmsg("pgarch_ArchiverObsCopyLoop time change to %ld", t_thrd.arch.last_arch_time)));
        }
        /* reset result flag */
        g_instance.archive_obs_cxt.pitr_finish_result = false;
    } while (XLByteLT(t_thrd.arch.pitr_task_last_lsn, flushPtr));
}

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

    rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlog);
    securec_check_ss(rc, "\0", "\0");

    /* archive_dest is preferred over archive_command */
    if (XLogArchiveDestSet()) {
        return PgarchArchiveXlogToDest(xlog);
    }

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
    *dp = '\0';

    ereport(DEBUG3, (errmsg_internal("executing archive command \"%s\"", xlogarchcmd)));

    /* Report archive activity in PS display */
    rc = snprintf_s(activitymsg, sizeof(activitymsg), sizeof(activitymsg) - 1, "archiving %s", xlog);
    securec_check_ss(rc, "\0", "\0");

    set_ps_display(activitymsg, false);

    rc = gs_popen_security(xlogarchcmd);
    if (rc != 0) {
        /* If execute archive command failed, we report an alarm */
        g_instance.WalSegmentArchSucceed = false;

        /*
         * If either the shell itself, or a called command, died on a signal,
         * abort the archiver.	We do this because system() ignores SIGINT and
         * SIGQUIT while waiting; so a signal is very likely something that
         * should have interrupted us too.	If we overreact it's no big deal,
         * the postmaster will just start the archiver again.
         *
         * Per the Single Unix Spec, shells report exit status > 128 when a
         * called command died on a signal.
         */
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

    rc = snprintf_s(XLogArchiveStatusDir, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status");
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

    StatusFilePath(rlogready, xlog, ".ready");
    StatusFilePath(rlogdone, xlog, ".done");
    (void)durable_rename(rlogready, rlogdone, WARNING);
}

/*
 * pgarch_archiveRoachForPitrStandby
 * get signal from walreceiver, fork a roach process to archive xlog
*/
static void pgarch_archiveRoachForPitrStandby()
{
    ereport(LOG,
        (errmsg("pgarch_archiveRoachForPitrStandby %X/%X, term:%d, subterm:%d", 
            (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn >> 32), (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn), 
            g_instance.archive_obs_cxt.archive_task.term, g_instance.archive_obs_cxt.archive_task.sub_term)));
    if (obs_replication_archive(&g_instance.archive_obs_cxt.archive_task) == 0) {
        g_instance.archive_obs_cxt.pitr_finish_result = true;
    } else {
        ereport(WARNING,
            (errmsg("error when pgarch_archiveRoachForPitrStandby %X/%X, term:%d, subterm:%d", 
                (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn >> 32), (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn), 
                g_instance.archive_obs_cxt.archive_task.term, g_instance.archive_obs_cxt.archive_task.sub_term)));
        g_instance.archive_obs_cxt.pitr_finish_result = false;
    }
}

static void CreateArchiveReadyFile()
{
    char xlogfname[MAXFNAMELEN];
    ereport(LOG,
        (errmsg("CreateArchiveReadyFile %X/%X", (uint32)(g_instance.archive_standby_cxt.archive_task.targetLsn >> 32),
                (uint32)(g_instance.archive_standby_cxt.archive_task.targetLsn))));
    
    XLogSegNo xlogSegno = 0;
    XLByteToSeg(g_instance.archive_standby_cxt.archive_task.targetLsn, xlogSegno);
    if (xlogSegno == InvalidXLogSegPtr) {
        g_instance.archive_standby_cxt.arch_finish_result = false;
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid Lsn: %lu", g_instance.archive_standby_cxt.archive_task.targetLsn)));
    }
    XLogFileName(xlogfname, DEFAULT_TIMELINE_ID, xlogSegno);

    char tempPath[PATH_MAX] = {0};
    char srcPath[PATH_MAX + 1] = {0};
    int rc = snprintf_s(tempPath, PATH_MAX, PATH_MAX - 1, XLOGDIR "/%s", xlogfname);
    securec_check_ss(rc, "\0", "\0");
    char* retVal = realpath(tempPath, srcPath);
    if (retVal == NULL) {
        ereport(WARNING, (errmsg_internal("realpath src %s failed:%m\n", tempPath)));
    } else {
        XLogArchiveNotify(xlogfname);
    }
}

/*
 * pgarch_archiveRoachForPitrMaster
 * choose a walsender to send archive command
*/
static bool pgarch_archiveRoachForPitrMaster(XLogRecPtr targetLsn)
{
    ResetLatch(&t_thrd.arch.mainloop_latch);
    ereport(LOG,
        (errmsg("pgarch_archiveRoachForPitrMaster %X/%X", (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
    int rc;
    WalSnd* walsnd = pgarch_chooseWalsnd(targetLsn);
    if (walsnd == NULL) {
        ereport(WARNING,
            (errmsg("pgarch_archiveRoachForPitrMaster failed for no health standby %X/%X", 
                (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
        return false;
    }
    walsnd->arch_latch = &t_thrd.arch.mainloop_latch;
    pg_atomic_write_u32(&walsnd->archive_flag, 1);
    SetLatch(&walsnd->latch);
    rc = WaitLatch(&t_thrd.arch.mainloop_latch,
        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
        (long)t_thrd.arch.task_wait_interval);

    if (rc & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }
    if (rc & WL_TIMEOUT) {
        return false;
    }
    /*
    * check targetLsn and g_instance.archive_obs_cxt.archive_task.targetLsn for deal message with wrong order
    */
    if (g_instance.archive_obs_cxt.pitr_finish_result == true 
        && XLByteEQ(g_instance.archive_obs_cxt.archive_task.targetLsn, targetLsn)) {
        return true;
    } else {
        return false;
    }
}

/*
 * pgarch_archiveRoachForPitrMaster
 * choose a walsender to send archive command
*/
static bool pgarch_archiveRoachForCoordinator(XLogRecPtr targetLsn)
{
    ArchiveXlogMessage archive_xlog_info;
    archive_xlog_info.targetLsn = targetLsn;
    archive_xlog_info.term = 0;
    archive_xlog_info.sub_term = 0;
    archive_xlog_info.tli = 0;
    if (obs_replication_archive(&archive_xlog_info) != 0) {
        g_instance.archive_obs_cxt.pitr_finish_result = false;
        ereport(WARNING,
            (errmsg("error when pgarch_archiveRoachForCoordinator %X/%X, term:%d, subterm:%d", 
                (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn >> 32), (uint32)(g_instance.archive_obs_cxt.archive_task.targetLsn), 
                g_instance.archive_obs_cxt.archive_task.term, g_instance.archive_obs_cxt.archive_task.sub_term)));
        ResetLatch(&t_thrd.arch.mainloop_latch);
        int rc;
        /* wait and try again */
        rc = WaitLatch(&t_thrd.arch.mainloop_latch,
            WL_TIMEOUT | WL_POSTMASTER_DEATH, 500);
        return false;

    } else {
        g_instance.archive_obs_cxt.pitr_finish_result = true;
        g_instance.archive_obs_cxt.archive_task.targetLsn = targetLsn;
    }
    ereport(LOG,
        (errmsg("pgarch_archiveRoachForPitrMaster %X/%X", (uint32)(targetLsn >> 32), (uint32)(targetLsn))));
    
    return g_instance.archive_obs_cxt.pitr_finish_result;
}

/* check if there is any wal sender alive. */
static WalSnd* pgarch_chooseWalsnd(XLogRecPtr targetLsn)
{
    int i;
    volatile WalSnd* walsnd = NULL;
    if (t_thrd.arch.sync_walsender_idx >= 0) {
        walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[t_thrd.arch.sync_walsender_idx];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole) 
            && !XLogRecPtrIsInvalid(walsnd->flush) && XLByteLE(targetLsn, walsnd->flush)) {
            walsnd->arch_task_lsn = targetLsn;
            walsnd->archive_obs_subterm = g_instance.archive_obs_cxt.sync_walsender_term;
            SpinLockRelease(&walsnd->mutex);
            return (WalSnd*)walsnd;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];

        SpinLockAcquire(&walsnd->mutex);

        if (walsnd->pid != 0 && ((walsnd->sendRole & SNDROLE_PRIMARY_STANDBY) == walsnd->sendRole)) {
            if (XLByteLE(targetLsn, walsnd->flush)) {
                SpinLockRelease(&walsnd->mutex);
                walsnd->arch_task_lsn = targetLsn;
                t_thrd.arch.sync_walsender_idx = i;
                g_instance.archive_obs_cxt.sync_walsender_term++;
                ereport(LOG,
                    (errmsg("pgarch_chooseWalsnd has change from %d to %d , sub_term:%d", 
                        t_thrd.arch.sync_walsender_idx, i, g_instance.archive_obs_cxt.sync_walsender_term)));
                walsnd->archive_obs_subterm = g_instance.archive_obs_cxt.sync_walsender_term;
                return (WalSnd*)walsnd;
            }
        }
        SpinLockRelease(&walsnd->mutex);
    }
    return NULL;
}


static void archKill(int code, Datum arg)
{
    setObsArchLatch(NULL);
    ereport(LOG, (errmsg("arch thread shut down")));
}

static void initLastTaskLsn()
{
    struct timeval tv;
    load_server_mode();
    gettimeofday(&tv,NULL);
    XLogRecPtr targetLsn;
    t_thrd.arch.last_arch_time = TIME_GET_MILLISEC(tv);
    ReplicationSlot* obs_archive_slot = getObsReplicationSlot();
    if (obs_archive_slot != NULL && !IsServerModeStandby()) {
        ArchiveXlogMessage obs_archive_info;
        if (obs_replication_get_last_xlog(&obs_archive_info) == 0) {
            t_thrd.arch.pitr_task_last_lsn = obs_archive_info.targetLsn;
            advanceObsSlot(t_thrd.arch.pitr_task_last_lsn);
            ereport(LOG,
                (errmsg("initLastTaskLsn update lsn to  %X/%X from obs", (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32), 
                    (uint32)(t_thrd.arch.pitr_task_last_lsn))));
        } else {
            targetLsn = GetFlushRecPtr();
            t_thrd.arch.pitr_task_last_lsn = targetLsn - (targetLsn % XLogSegSize);
            advanceObsSlot(t_thrd.arch.pitr_task_last_lsn);
            ereport(LOG,
                (errmsg("initLastTaskLsn update lsn to  %X/%X from local", (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32), 
                    (uint32)(t_thrd.arch.pitr_task_last_lsn))));
        }
    } else {
        targetLsn = GetFlushRecPtr();
        t_thrd.arch.pitr_task_last_lsn = targetLsn - (targetLsn % XLogSegSize);
        advanceObsSlot(t_thrd.arch.pitr_task_last_lsn);
        ereport(LOG,
            (errmsg("initLastTaskLsn update lsn to  %X/%X from local with not obs slot", (uint32)(t_thrd.arch.pitr_task_last_lsn >> 32), 
                (uint32)(t_thrd.arch.pitr_task_last_lsn))));
    }
}

/*
 * This function is used to send the status of archiver
 */
static void SendArchiveThreadStatus(bool status)
{
    if (!WalRcvIsOnline()) {
        return;
    }
    while(!SendArchiveThreadStatusInternal(status)) {
        ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                    errmsg("Notifing primary to update archive status is failed.")));
        pg_usleep(SEND_ARCHIVE_STATUS_INTERVAL);
    }
    ereport(LOG, (errmsg("Notifing primary to update archive status is success.")));
}

static bool SendArchiveThreadStatusInternal(bool is_archive_activited)
{
    ResetLatch(&t_thrd.arch.mainloop_latch);
    g_instance.archive_standby_cxt.archive_enabled = is_archive_activited;
    (void)gs_signal_send(g_instance.pid_cxt.WalReceiverPID, SIGUSR2);
    g_instance.archive_standby_cxt.arch_latch = &t_thrd.arch.mainloop_latch;
    int rc = WaitLatch(&t_thrd.arch.mainloop_latch,
                        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                        (long)(UPDATE_STATUS_WAIT));
    if (rc & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }
    if (rc & WL_TIMEOUT) {
        return false;
    }
    return true;
}

static bool NotifyPrimaryArchiverActived(ThreadId* last_walrcv_pid, bool* sent_archive_status)
{
    if (WalRcvIsOnline()) {
        /* last_walrcv_pid is used to check whether the primary node is switched over. */
        if (!(*sent_archive_status) || *last_walrcv_pid != g_instance.pid_cxt.WalReceiverPID) {
            SendArchiveThreadStatus(ARCHIVE_ON);
            *sent_archive_status = true;
            *last_walrcv_pid = g_instance.pid_cxt.WalReceiverPID;
            return true;
        }
        if (*sent_archive_status) {
            return true;
        }
    }
    *sent_archive_status = false;
    return false;
}

static void NotifyPrimaryArchiverShutdown(bool sent_archive_status)
{
    if (sent_archive_status) {
        SendArchiveThreadStatus(ARCHIVE_OFF);
    }
}

static void ChangeArchiveTaskStatus2Done()
{
    if (IsServerModeStandby()) {
        XLogRecPtr* standby_archive_start_point = &g_instance.archive_standby_cxt.standby_archive_start_point;
        volatile unsigned int *arch_task_status = &g_instance.archive_standby_cxt.arch_task_status;
        if (unlikely(pg_atomic_read_u32(arch_task_status) == ARCH_TASK_GET)) {
            *standby_archive_start_point = g_instance.archive_standby_cxt.archive_task.targetLsn;
            pg_memory_barrier();
            g_instance.archive_standby_cxt.arch_finish_result = true;
            pg_atomic_write_u32(arch_task_status, ARCH_TASK_DONE);
        }
    }
}