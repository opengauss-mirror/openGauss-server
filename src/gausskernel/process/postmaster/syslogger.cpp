/* -------------------------------------------------------------------------
 *
 * syslogger.cpp
 *
 * The system logger (syslogger) appeared in Postgres 8.0. It catches all
 * stderr output from the postmaster, backends, and other subprocesses
 * by redirecting to a pipe, and writes it to a set of logfiles.
 * It's possible to have size and age limits for the logfile configured
 * in postgresql.conf. If these limits are reached or passed, the
 * current logfile is closed and a new one is created (rotated).
 * The logfiles are stored in a subdirectory (configurable in
 * postgresql.conf), using a user-selectable naming scheme.
 *
 * Author: Andreas Pflug <pgadmin@pse-consulting.de>
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2004-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/syslogger.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgtime.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "gssignal/gs_signal.h"

#define PROFILE_LOG_ROTATE_SIZE ((long)20 * 1024 * 1024L)

/*
 * We really want line-buffered mode for logfile output, but Windows does
 * not have it, and interprets _IOLBF as _IOFBF (bozos).  So use _IONBF
 * instead on Windows.
 */
#ifdef WIN32
#define LBF_MODE _IONBF
#else
#define LBF_MODE _IOLBF
#endif

/*
 * We read() into a temp buffer twice as big as a chunk, so that any fragment
 * left after processing can be moved down to the front and we'll still have
 * room to read a full chunk.
 */
#define READ_BUF_SIZE (2 * LOGPIPE_CHUNK_SIZE)

#define ERROR_BUF_SIZE 1024

static char logCtlTimeZone[TZ_STRLEN_MAX + 1] = {0};
static char logCtlHostName[LOG_MAX_NODENAME_LEN] = {0};
static char logCtlNodeName[LOG_MAX_NODENAME_LEN] = {0};

/* put all LogControlData of all log types into this array */
static LogControlData* allLogCtl[LOG_TYPE_MAXVALID + 1] = {
    NULL, /* LOG_TYPE_ELOG */
    NULL, /* LOG_TYPE_PLOG */
    NULL, /* LOG_TYPE_PLAN_LOG */
    NULL, /* LOG_TYPE_ASP_LOG */
    NULL  /* LOG_TYPE_MAXVALID */
};

/* exclude error log in this FOR loop */
#define foreach_logctl(_logctl) for (int i = LOG_TYPE_ELOG + 1; ((_logctl) = allLogCtl[i]) != NULL; ++i)

/*
 * Buffers for saving partial messages from different backends.
 *
 * Keep NBUFFER_LISTS lists of these, with the entry for a given source pid
 * being in the list numbered (pid % NBUFFER_LISTS), so as to cut down on
 * the number of entries we have to examine for any one incoming message.
 * There must never be more than one entry for the same source pid.
 *
 * An inactive buffer is not removed from its list, just held for re-use.
 * An inactive buffer has pid == 0 and undefined contents of data.
 */
typedef struct {
    ThreadId pid;        /* PID of source process */
    StringInfoData data; /* accumulated data, as a StringInfo */
} save_buffer;

/* These must be exported for EXEC_BACKEND case ... annoying */
#ifndef WIN32

#else
HANDLE syslogPipe[2] = {0, 0};
#endif

#ifdef WIN32
static HANDLE threadHandle = 0;
static CRITICAL_SECTION sysloggerSection;
#endif

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
/* Local subroutines */
static void syslogger_setfd(int fd);

static void process_pipe_input(char* logbuffer, int* bytes_in_logbuffer);
static void flush_pipe_input(char* logbuffer, int* bytes_in_logbuffer);
static void open_csvlogfile(void);
#ifdef ENABLE_UT
#define static
#endif

static FILE* logfile_open(const char* filename, const char* mode, bool allow_errors);

#if defined(ENABLE_UT) && defined(static)
#undef static
#endif

#ifdef WIN32
static unsigned int __stdcall pipeThread(void* arg);
#endif
static void logfile_rotate(bool time_based_rotation, int size_rotation_for);
static char* logfile_getname(pg_time_t timestamp, const char* suffix, const char* logdir, const char* filename);
static void set_next_rotation_time(void);
static void sigHupHandler(SIGNAL_ARGS);
static void sigUsr1Handler(SIGNAL_ARGS);

static void LogCtlSetGlobalNames(void);
static void LogCtlSetTimeZone(void);
static char* LogCtlGetLogDirectory(const char* logid, bool include_nodename);
static void LogCtlCreateLogParentDirectory(void);
static void LogCtlWriteFileHeader(LogControlData* logctl);
static void LogCtlRotateLogFileIfNeeded(LogControlData*, bool);
static void LogCtlFlushBuf(LogControlData* logctl);
static char* LogCtlGetFilenamePattern(const char* post_suffix);
static void LogCtlProcessInput(LogControlData* logctl, const char* msg, int len);
static void PLogCtlInit(void);
static void slow_query_logfile_rotate(bool time_based_rotation, int size_rotation_for);
static void asp_logfile_rotate(bool time_based_rotation, int size_rotation_for);

/*
 * Main entry point for syslogger process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void SysLoggerMain(int fd)
{
#ifndef WIN32
    char logbuffer[READ_BUF_SIZE];
    int bytes_in_logbuffer = 0;
#endif
    LogControlData* logctl = NULL;
    char* currentLogDir = NULL;
    char* currentLogFilename = NULL;
    int currentLogRotationAge;
    pg_time_t now;

    DISABLE_MEMORY_PROTECT();

    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    t_thrd.proc_cxt.MyStartTime = time(NULL); /* set our start time in case we call elog */
    now = t_thrd.proc_cxt.MyStartTime;

    t_thrd.proc_cxt.MyProgName = "syslogger";

    t_thrd.myLogicTid = noProcLogicTid + SYSLOGGER_LID;

    syslogger_setfd(fd);

    t_thrd.role = SYSLOGGER;

    init_ps_display("logger process", "", "", "");

    /*
     * Syslogger's own stderr can't be the syslogPipe, so set it back to text
     * mode if we didn't just close it. (It was set to binary in
     * SubPostmasterMain).
     */
#ifdef WIN32
    else
        _setmode(_fileno(stderr), _O_TEXT);
#endif

        /*
         * Also close our copy of the write end of the pipe.  This is needed to
         * ensure we can detect pipe EOF correctly.  (But note that in the restart
         * case, the postmaster already did this.)
         */
#ifndef WIN32
    t_thrd.postmaster_cxt.syslogPipe[1] = -1;
#else
    syslogPipe[1] = 0;
#endif

    InitializeLatchSupport(); /* needed for latch waits */

    /* Initialize private latch for use by signal handlers */
    InitLatch(&t_thrd.logger.sysLoggerLatch);

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we ignore all termination signals, and instead exit only when all
     * upstream processes are gone, to ensure we don't miss any dying gasps of
     * broken backends...
     */
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, sigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, sigUsr1Handler); /* request log rotation */
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

#ifdef WIN32
    /* Fire up separate data transfer thread */
    InitializeCriticalSection(&sysloggerSection);
    EnterCriticalSection(&sysloggerSection);

    threadHandle = (HANDLE)_beginthreadex(NULL, 0, pipeThread, NULL, 0, NULL);
    if (threadHandle == 0)
        ereport(FATAL, (errmsg("could not create syslogger data transfer thread: %m")));
#endif /* WIN32 */

    PLogCtlInit();
    /* create slow query directory */
    if (g_instance.attr.attr_common.query_log_directory == NULL) {
        init_instr_log_directory(true, SLOWQUERY_LOG_TAG);
    } else {
        (void)pg_mkdir_p(g_instance.attr.attr_common.query_log_directory, S_IRWXU);
    }

    /* create asp directory */
    if (g_instance.attr.attr_common.asp_log_directory == NULL) {
        init_instr_log_directory(true, ASP_LOG_TAG);
    } else {
        (void)pg_mkdir_p(g_instance.attr.attr_common.asp_log_directory, S_IRWXU);
    }

    /* init the other logs */
    /*
     * Remember active logfile's name.  We recompute this from the reference
     * time because passing down just the pg_time_t is a lot cheaper than
     * passing a whole file path in the EXEC_BACKEND case.
     */
    t_thrd.logger.last_file_name = logfile_getname(t_thrd.logger.first_syslogger_file_time,
        NULL,
        u_sess->attr.attr_common.Log_directory,
        u_sess->attr.attr_common.Log_filename);
    t_thrd.logger.syslogFile = logfile_open(t_thrd.logger.last_file_name, "a", false);

    foreach_logctl(logctl) {
        /*
         * fd leaking maybe happens when syslogger thread is
         * restarted repeated by postmaster thread if exception occurs.
         * maybe include switch over case.
         * if the thread restarted, the original memory context must be deleted and need not free the memory 
         */
        logctl->now_file_name =
            logfile_getname(t_thrd.logger.first_syslogger_file_time, NULL, logctl->log_dir, logctl->filename_pattern);

        if (logctl->now_file_fd != NULL) {
            (void)fclose(logctl->now_file_fd);
            logctl->now_file_fd = NULL;
        }
        logctl->now_file_fd = logfile_open(logctl->now_file_name, "a", false);
        LogCtlWriteFileHeader(logctl);
    }

    /* remember active logfile parameters */
    currentLogDir = pstrdup(u_sess->attr.attr_common.Log_directory);
    currentLogFilename = pstrdup(u_sess->attr.attr_common.Log_filename);
    currentLogRotationAge = u_sess->attr.attr_common.Log_RotationAge;
    /* set next planned rotation time */
    set_next_rotation_time();

    /* main worker loop */
    for (;;) {
        bool time_based_rotation = false;
        int size_rotation_for = 0;
        long cur_timeout;
        int cur_flags;

#ifndef WIN32
        int rc;
#endif

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.logger.sysLoggerLatch);

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.logger.got_SIGHUP) {
            t_thrd.logger.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);

            /*
             * Check if the log directory or filename pattern changed in
             * postgresql.conf. If so, force rotation to make sure we're
             * writing the logfiles in the right place.
             */
            if (strcmp(u_sess->attr.attr_common.Log_directory, currentLogDir) != 0) {
                pfree(currentLogDir);
                currentLogDir = pstrdup(u_sess->attr.attr_common.Log_directory);
                t_thrd.logger.rotation_requested = true;
                /* not affect pLogCtl's Log_directory */
                /*
                 * Also, create new directory if not present; ignore errors
                 */
                mkdir(u_sess->attr.attr_common.Log_directory, S_IRWXU);
            }
            if (strcmp(u_sess->attr.attr_common.Log_filename, currentLogFilename) != 0) {
                pfree(currentLogFilename);
                currentLogFilename = pstrdup(u_sess->attr.attr_common.Log_filename);
                t_thrd.logger.rotation_requested = true;
                foreach_logctl(logctl) {
                    if (logctl->filename_pattern != NULL) {
                        pfree_ext(logctl->filename_pattern);
                    }
                    /* recompute log file pattern */
                    logctl->filename_pattern = LogCtlGetFilenamePattern(logctl->file_suffix);
                    logctl->rotation_requested = true;
                }
            }

            /*
             * If rotation time parameter changed, reset next rotation time,
             * but don't immediately force a rotation.
             */
            if (currentLogRotationAge != u_sess->attr.attr_common.Log_RotationAge) {
                currentLogRotationAge = u_sess->attr.attr_common.Log_RotationAge;
                set_next_rotation_time();
            }

            /*
             * If we had a rotation-disabling failure, re-enable rotation
             * attempts after SIGHUP, and force one immediately.
             */
            if (t_thrd.logger.rotation_disabled) {
                t_thrd.logger.rotation_disabled = false;
                t_thrd.logger.rotation_requested = true;
                foreach_logctl(logctl) {
                    /* keep the same step with error log, and rotate this log file */
                    logctl->rotation_requested = true;
                }
            }
        }

        if (u_sess->attr.attr_common.Log_RotationAge > 0 && !t_thrd.logger.rotation_disabled) {
            /* Do a logfile rotation if it's time */
            now = (pg_time_t)time(NULL);
            if (now >= t_thrd.logger.next_rotation_time) {
                t_thrd.logger.rotation_requested = time_based_rotation = true;
                foreach_logctl(logctl) {
                    /* share the same rotation age */
                    logctl->rotation_requested = true;
                }
            }
        }

        if (!t_thrd.logger.rotation_requested && u_sess->attr.attr_common.Log_RotationSize > 0 &&
            !t_thrd.logger.rotation_disabled) {
            
            /* Do a rotation if file is too big */
            if (ftell(t_thrd.logger.syslogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) {
                t_thrd.logger.rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_STDERR;
            }
            if (t_thrd.logger.csvlogFile != NULL &&
                ftell(t_thrd.logger.csvlogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) {
                t_thrd.logger.rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_CSVLOG;
            }
            if (t_thrd.logger.querylogFile != NULL &&
                ftell(t_thrd.logger.querylogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) {
                t_thrd.logger.rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_QUERYLOG;
            }
            if (t_thrd.logger.asplogFile != NULL &&
                ftell(t_thrd.logger.asplogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) {
                t_thrd.logger.rotation_requested = true;
                size_rotation_for |= LOG_DESTINATION_ASPLOG;
            }
        }

        /*
         * check the other log types' file rotation before error log type.
         * logfile_rotate() will change t_thrd.logger.next_rotation_time value by calling
         * set_next_rotation_time().
         */
        foreach_logctl(logctl) {
            LogCtlRotateLogFileIfNeeded(logctl, time_based_rotation);
        }
        if (t_thrd.logger.rotation_requested) {
            /*
             * Force rotation when both values are zero. It means the request
             * was sent by pg_rotate_logfile.
             */
            if (!time_based_rotation && size_rotation_for == 0)
                size_rotation_for = LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG |
                    LOG_DESTINATION_QUERYLOG | LOG_DESTINATION_ASPLOG;
            asp_logfile_rotate(time_based_rotation, size_rotation_for);
            slow_query_logfile_rotate(time_based_rotation, size_rotation_for);
            
            /* only last one can recalculate next_rotation_time */
            logfile_rotate(time_based_rotation, size_rotation_for);
        }

        /*
         * Calculate time till next time-based rotation, so that we don't
         * sleep longer than that.	We assume the value of "now" obtained
         * above is still close enough.  Note we can't make this calculation
         * until after calling logfile_rotate(), since it will advance
         * t_thrd.logger.next_rotation_time.
         *
         * Also note that we need to beware of overflow in calculation of the
         * timeout: with large settings of Log_RotationAge, t_thrd.logger.next_rotation_time
         * could be more than INT_MAX msec in the future.  In that case we'll
         * wait no more than INT_MAX msec, and try again.
         */
        if (u_sess->attr.attr_common.Log_RotationAge > 0 && !t_thrd.logger.rotation_disabled) {
            pg_time_t delay;

            delay = t_thrd.logger.next_rotation_time - now;
            if (delay > 0) {
                if (delay > INT_MAX / 1000)
                    delay = INT_MAX / 1000;
                cur_timeout = delay * 1000L; /* msec */
            } else
                cur_timeout = 0;
            cur_flags = WL_TIMEOUT;
        } else {
            cur_timeout = -1L;
            cur_flags = 0;
        }

        /*
         * Sleep until there's something to do
         */
#ifndef WIN32
        rc = WaitLatchOrSocket(&t_thrd.logger.sysLoggerLatch,
            WL_LATCH_SET | WL_SOCKET_READABLE | cur_flags,
            t_thrd.postmaster_cxt.syslogPipe[0],
            cur_timeout);

        if (rc & WL_SOCKET_READABLE) {
            int bytesRead;

            bytesRead = read(t_thrd.postmaster_cxt.syslogPipe[0],
                logbuffer + bytes_in_logbuffer,
                sizeof(logbuffer) - bytes_in_logbuffer);
            if (bytesRead < 0) {
                if (errno != EINTR)
                    ereport(LOG, (errcode_for_socket_access(), errmsg("could not read from logger pipe: %m")));
            } else if (bytesRead > 0) {
                bytes_in_logbuffer += bytesRead;
                process_pipe_input(logbuffer, &bytes_in_logbuffer);
                continue;
            } else {
                /*
                 * ELSE branch is never executed forever in multi-thread mode 
                 *
                 * Zero bytes read when select() is saying read-ready means
                 * EOF on the pipe: that is, there are no longer any processes
                 * with the pipe write end open.  Therefore, the postmaster
                 * and all backends are shut down, and we are done.
                 */
                t_thrd.logger.pipe_eof_seen = true;

                /* if there's any data left then force it out now */
                flush_pipe_input(logbuffer, &bytes_in_logbuffer);
            }
        }
#else  /* WIN32 */

        /*
         * On Windows we leave it to a separate thread to transfer data and
         * detect pipe EOF.  The main thread just wakes up to handle SIGHUP
         * and rotation conditions.
         *
         * Server code isn't generally thread-safe, so we ensure that only one
         * of the threads is active at a time by entering the critical section
         * whenever we're not sleeping.
         */
        LeaveCriticalSection(&sysloggerSection);

        (void)WaitLatch(&t_thrd.logger.sysLoggerLatch, WL_LATCH_SET | cur_flags, cur_timeout);

        EnterCriticalSection(&sysloggerSection);
#endif /* WIN32 */

        if (t_thrd.logger.pipe_eof_seen) {
            /*
             * seeing this message on the real stderr is annoying - so we make
             * it DEBUG1 to suppress in normal use.
             */
            ereport(DEBUG1, (errmsg("logger shutting down")));

            /*
             * Normal exit from the syslogger is here.	Note that we
             * deliberately do not close t_thrd.logger.syslogFile before exiting; this is to
             * allow for the possibility of elog messages being generated
             * inside proc_exit.  Regular exit() will take care of flushing
             * and closing stdio channels.
             */
            proc_exit(0);
        }
    }
}

/*
 * Postmaster subroutine to start a syslogger subprocess.
 */
ThreadId SysLogger_Start(void)
{
    ThreadId sysloggerPid;
    char* filename = NULL;

    if (!g_instance.attr.attr_common.Logging_collector)
        return 0;

        /*
         * If first time through, create the pipe which will receive stderr
         * output.
         *
         * If the syslogger crashes and needs to be restarted, we continue to use
         * the same pipe (indeed must do so, since extant backends will be writing
         * into that pipe).
         *
         * This means the postmaster must continue to hold the read end of the
         * pipe open, so we can pass it down to the reincarnated syslogger. This
         * is a bit klugy but we have little choice.
         */
#ifndef WIN32
    if (t_thrd.postmaster_cxt.syslogPipe[0] < 0) {
        if (pipe(t_thrd.postmaster_cxt.syslogPipe) < 0)
            ereport(FATAL, (errcode_for_socket_access(), (errmsg("could not create pipe for syslog: %m"))));
    }
#else
    if (!t_thrd.postmaster_cxt.syslogPipe[0]) {
        SECURITY_ATTRIBUTES sa;

        errno_t rc = memset_s(&sa, sizeof(SECURITY_ATTRIBUTES), 0, sizeof(SECURITY_ATTRIBUTES));
        securec_check_c(rc, "\0", "\0");

        sa.nLength = sizeof(SECURITY_ATTRIBUTES);
        sa.bInheritHandle = TRUE;

        if (!CreatePipe(&t_thrd.postmaster_cxt.syslogPipe[0], &t_thrd.postmaster_cxt.syslogPipe[1], &sa, 32768))
            ereport(FATAL, (errcode_for_file_access(), (errmsg("could not create pipe for syslog: %m"))));
    }
#endif

    /*
     * Create log directory if not present; ignore errors
     */
    (void)pg_mkdir_p(u_sess->attr.attr_common.Log_directory, S_IRWXU);
    /* create log directory */
    LogCtlCreateLogParentDirectory();
    /* set global names from postmaster */
    LogCtlSetGlobalNames();
    /* set time zone from postmaster */
    LogCtlSetTimeZone();

    /*
     * The initial logfile is created right in the postmaster, to verify that
     * the Log_directory is writable.  We save the reference time so that
     * the syslogger child process can recompute this file name.
     *
     * It might look a bit strange to re-do this during a syslogger restart,
     * but we must do so since the postmaster closed t_thrd.logger.syslogFile after the
     * previous fork (and remembering that old file wouldn't be right anyway).
     * Note we always append here, we won't overwrite any existing file.  This
     * is consistent with the normal rules, because by definition this is not
     * a time-based rotation.
     */
    t_thrd.logger.first_syslogger_file_time = time(NULL);
    filename = logfile_getname(t_thrd.logger.first_syslogger_file_time,
        NULL,
        u_sess->attr.attr_common.Log_directory,
        u_sess->attr.attr_common.Log_filename);

    pfree(filename);

    sysloggerPid = initialize_util_thread(SYSLOGGER);

    /* success, in postmaster */
    if (sysloggerPid != 0) {
        /* now we redirect stderr, if not done already */
        if (!t_thrd.postmaster_cxt.redirection_done) {
#ifndef WIN32
            fflush(stdout);
            if (dup2(t_thrd.postmaster_cxt.syslogPipe[1], fileno(stdout)) < 0)
                ereport(FATAL, (errcode_for_file_access(), errmsg("could not redirect stdout: %m")));
            fflush(stderr);
            if (dup2(t_thrd.postmaster_cxt.syslogPipe[1], fileno(stderr)) < 0)
                ereport(FATAL, (errcode_for_file_access(), errmsg("could not redirect stderr: %m")));
            /* Now we are done with the write end of the pipe. */
            close(t_thrd.postmaster_cxt.syslogPipe[1]);
            t_thrd.postmaster_cxt.syslogPipe[1] = -1;
#else
            int fd;

            /*
             * open the pipe in binary mode and make sure stderr is binary
             * after it's been dup'ed into, to avoid disturbing the pipe
             * chunking protocol.
             */
            fflush(stderr);
            fd = _open_osfhandle((intptr_t)t_thrd.postmaster_cxt.syslogPipe[1], _O_APPEND | _O_BINARY);
            if (dup2(fd, _fileno(stderr)) < 0)
                ereport(FATAL, (errcode_for_file_access(), errmsg("could not redirect stderr: %m")));
            close(fd);
            _setmode(_fileno(stderr), _O_BINARY);

            /*
             * Now we are done with the write end of the pipe.
             * CloseHandle() must not be called because the preceding
             * close() closes the underlying handle.
             */
            t_thrd.postmaster_cxt.syslogPipe[1] = 0;
#endif
            t_thrd.postmaster_cxt.redirection_done = true;
        }

        t_thrd.logger.syslogFile = NULL;
        return sysloggerPid;
    }

    /* we should never reach here */
    return 0;
}

/* close the t_thrd.logger.syslogFile */
void SysLoggerClose(void)
{
    if (t_thrd.logger.syslogFile) {
        fclose(t_thrd.logger.syslogFile);
        t_thrd.logger.syslogFile = NULL;
    }
}

#ifdef EXEC_BACKEND

/*
 * syslogger_ereprint() -
 *
 * print error in syslogger thread
 */
static void syslogger_erewrite(FILE* file, const char* buffer)
{
    int tryTimes = 0;
    for (;;) {
        /* clear errno before calling IO write */
        errno = 0;
        int buffer_len = (int)strlen(buffer);
        int rc = fwrite(buffer, 1, buffer_len, file);
        if ((errno != 0) || (rc != buffer_len)) {
            tryTimes++;
            if (tryTimes >= 3)
                break;

            /*
             * if no disk space, we will retry,
             * and we can not report a log, because there is not space to write.
             */
            if (errno == ENOSPC) {
                break;
            }
        }
        break;
    }
}

/*
 * syslogger_setfd() -
 *
 * Extract data from the arglist for exec'ed syslogger process
 */
static void syslogger_setfd(int fd)
{
    if (fd != -1) {
        t_thrd.logger.syslogFile = fdopen(fd, "a");
        if (t_thrd.logger.syslogFile == NULL) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("syslogger could not open file %d: %m,exit\n", fd)));
            proc_exit(1);
        }

        setvbuf(t_thrd.logger.syslogFile, NULL, LBF_MODE, 0);
    }
}
#endif /* EXEC_BACKEND */

/*
 * CheckPipeProtoHeader() -
 *
 * check whether p has the features of a LogPipeProtoHeader
 */
static bool CheckPipeProtoHeader(const LogPipeProtoHeader p)
{
    if (p.nuls[0] == '\0' && p.nuls[1] == '\0' && p.len > 0 &&
         p.len <= LOGPIPE_MAX_PAYLOAD && p.pid != 0 &&
        (p.is_last == 't' || p.is_last == 'f' || p.is_last == 'T' || p.is_last == 'F') &&
         p.logtype >= LOG_TYPE_ELOG && p.logtype < LOG_TYPE_MAXVALID &&
        p.magic == PROTO_HEADER_MAGICNUM)
        return true;
    return false;
}
/* --------------------------------
 *		pipe protocol handling
 * --------------------------------
 */
/*
 * Process data received through the syslogger pipe.
 *
 * This routine interprets the log pipe protocol which sends log messages as
 * (hopefully atomic) chunks - such chunks are detected and reassembled here.
 *
 * The protocol has a header that starts with two nul bytes, then has a 16 bit
 * length, the pid of the sending process, and a flag to indicate if it is
 * the last chunk in a message. Incomplete chunks are saved until we read some
 * more, and non-final chunks are accumulated until we get the final chunk.
 *
 * All of this is to avoid 2 problems:
 * . partial messages being written to logfiles (messes rotation), and
 * . messages from different backends being interleaved (messages garbled).
 *
 * Any non-protocol messages are written out directly. These should only come
 * from non-PostgreSQL sources, however (e.g. third party libraries writing to
 * stderr).
 *
 * logbuffer is the data input buffer, and *bytes_in_logbuffer is the number
 * of bytes present.  On exit, any not-yet-eaten data is left-justified in
 * logbuffer, and *bytes_in_logbuffer is updated.
 */
static void process_pipe_input(char* logbuffer, int* bytes_in_logbuffer)
{
    char* cursor = logbuffer;
    int count = *bytes_in_logbuffer;
    int dest = LOG_DESTINATION_STDERR;

    /* While we have enough for a header, process data... */
    while (count >= (int)sizeof(LogPipeProtoHeader)) {
        LogPipeProtoHeader p;
        int chunklen;

        /* Do we have a valid header? */
        errno_t rcs = memcpy_s(&p, sizeof(LogPipeProtoHeader), cursor, sizeof(LogPipeProtoHeader));
        securec_check(rcs, "\0", "\0");

        if (CheckPipeProtoHeader(p) == true) {
            List* buffer_list = NULL;
            ListCell* cell = NULL;
            save_buffer* existing_slot = NULL;
            save_buffer* free_slot = NULL;
            StringInfo str;

            chunklen = LOGPIPE_HEADER_SIZE + p.len;

            /* Fall out of loop if we don't have the whole chunk yet */
            if (count < chunklen)
                break;

            if (p.logtype == LOG_TYPE_ELOG || p.logtype == LOG_TYPE_PLAN_LOG || p.logtype == LOG_TYPE_ASP_LOG) {
                if (p.logtype == LOG_TYPE_PLAN_LOG)
                    dest = LOG_DESTINATION_QUERYLOG;
                else if (p.logtype == LOG_TYPE_ASP_LOG)
                    dest = LOG_DESTINATION_ASPLOG;
                else
                    dest = (p.is_last == 'T' || p.is_last == 'F') ? LOG_DESTINATION_CSVLOG : LOG_DESTINATION_STDERR;

                /* Locate any existing buffer for this source pid */
                buffer_list = t_thrd.logger.buffer_lists[p.pid % NBUFFER_LISTS];
                foreach (cell, buffer_list) {
                    save_buffer* buf = (save_buffer*)lfirst(cell);

                    if (buf->pid == p.pid) {
                        existing_slot = buf;
                        break;
                    }
                    if (buf->pid == 0 && free_slot == NULL)
                        free_slot = buf;
                }

                if (p.is_last == 'f' || p.is_last == 'F') {
                    /*
                     * Save a complete non-final chunk in a per-pid buffer
                     */
                    if (existing_slot != NULL) {
                        /* Add chunk to data from preceding chunks */
                        str = &(existing_slot->data);
                        appendBinaryStringInfo(str, cursor + LOGPIPE_HEADER_SIZE, p.len);
                    } else {
                        /* First chunk of message, save in a new buffer */
                        if (free_slot == NULL) {
                            /*
                             * Need a free slot, but there isn't one in the list,
                             * so create a new one and extend the list with it.
                             */
                            free_slot = (save_buffer*)palloc(sizeof(save_buffer));
                            buffer_list = lappend(buffer_list, free_slot);
                            t_thrd.logger.buffer_lists[p.pid % NBUFFER_LISTS] = buffer_list;
                        }
                        free_slot->pid = p.pid;
                        str = &(free_slot->data);
                        initStringInfo(str);
                        appendBinaryStringInfo(str, cursor + LOGPIPE_HEADER_SIZE, p.len);
                    }
                } else {
                    /*
                     * Final chunk --- add it to anything saved for that pid, and
                     * either way write the whole thing out.
                     */
                    if (existing_slot != NULL) {
                        str = &(existing_slot->data);
                        appendBinaryStringInfo(str, cursor + LOGPIPE_HEADER_SIZE, p.len);
                        write_syslogger_file(str->data, str->len, dest);
                        /* Mark the buffer unused, and reclaim string storage */
                        existing_slot->pid = 0;
                        pfree(str->data);
                    } else {
                        /* The whole message was one chunk, evidently. */
                        write_syslogger_file(cursor + LOGPIPE_HEADER_SIZE, p.len, dest);
                    }
                }
            } else if (p.logtype < LOG_TYPE_MAXVALID) {
                Assert(LOG_TYPE_MAXVALID <= LOG_TYPE_UPLIMIT);
                LogCtlProcessInput(allLogCtl[(int)p.logtype], cursor + LOGPIPE_HEADER_SIZE, p.len);
            } else {
                Assert(0);
            }

            /* Finished processing this chunk */
            cursor += chunklen;
            count -= chunklen;
        } else {
            /* Process non-protocol data */
            /*
             * Look for the start of a protocol header.  If found, dump data
             * up to there and repeat the loop.  Otherwise, dump it all and
             * fall out of the loop.  (Note: we want to dump it all if at all
             * possible, so as to avoid dividing non-protocol messages across
             * logfiles.  We expect that in many scenarios, a non-protocol
             * message will arrive all in one read(), and we want to respect
             * the read() boundary if possible.)
             */
            for (chunklen = 1; chunklen < count; chunklen++) {
                if (cursor[chunklen] == '\0')
                    break;
            }
            /* fall back on the stderr log as the destination */
            write_syslogger_file(cursor, chunklen, LOG_DESTINATION_STDERR);
            cursor += chunklen;
            count -= chunklen;
        }
    }

    /* We don't have a full chunk, so left-align what remains in the buffer */
    if (count > 0 && cursor != logbuffer) {
        errno_t rc = memmove_s(logbuffer, count, cursor, count);
        securec_check_c(rc, "\0", "\0");
    }
    *bytes_in_logbuffer = count;
}

/*
 * Force out any buffered data
 *
 * This is currently used only at syslogger shutdown, but could perhaps be
 * useful at other times, so it is careful to leave things in a clean state.
 */
static void flush_pipe_input(char* logbuffer, int* bytes_in_logbuffer)
{
    int i;

    /* Dump any incomplete protocol messages */
    for (i = 0; i < NBUFFER_LISTS; i++) {
        List* list = t_thrd.logger.buffer_lists[i];
        ListCell* cell = NULL;

        foreach (cell, list) {
            save_buffer* buf = (save_buffer*)lfirst(cell);

            if (buf->pid != 0) {
                StringInfo str = &(buf->data);

                write_syslogger_file(str->data, str->len, LOG_DESTINATION_STDERR);
                /* Mark the buffer unused, and reclaim string storage */
                buf->pid = 0;
                pfree(str->data);
            }
        }
    }

    /*
     * Force out any remaining pipe data as-is; we don't bother trying to
     * remove any protocol headers that may exist in it.
     */
    if (*bytes_in_logbuffer > 0)
        write_syslogger_file(logbuffer, *bytes_in_logbuffer, LOG_DESTINATION_STDERR);
    *bytes_in_logbuffer = 0;
}

/* --------------------------------
 *		logfile routines
 * --------------------------------
 */
/*
 * Write text to the currently open logfile
 *
 * This is exported so that elog.c can call it when am_syslogger is true.
 * This allows the syslogger process to record elog messages of its own,
 * even though its stderr does not point at the syslog pipe.
 */
void write_syslogger_file(char* buffer, int count, int destination)
{
    int rc;
    FILE* logfile = NULL;
    bool            doOpen = false;

    if (destination == LOG_DESTINATION_CSVLOG && t_thrd.logger.csvlogFile == NULL)
        open_csvlogfile();

    if (destination == LOG_DESTINATION_QUERYLOG)
        logfile = (FILE *)SQMOpenLogFile(&doOpen);
    else if (destination == LOG_DESTINATION_ASPLOG)
        logfile = (FILE *)ASPOpenLogFile(&doOpen);
    else
        logfile = (destination == LOG_DESTINATION_CSVLOG) ? t_thrd.logger.csvlogFile : t_thrd.logger.syslogFile;

    errno = 0;

    rc = fwrite(buffer, 1, count, logfile);

    /* can't use ereport here because of possible recursion */
    if (rc != count) {
        /*
         * if no disk space, we will retry,
         * and we can not report a log, because there is not space to write.
         */
        if (errno == ENOSPC) {
            return;
        }
        char errorbuf[ERROR_BUF_SIZE] = {'\0'};
        rc = sprintf_s(errorbuf, ERROR_BUF_SIZE, "ERROR: could not write to log file: %s\n", gs_strerror(errno));
        securec_check_ss_c(rc, "\0", "\0");
        syslogger_erewrite(logfile, errorbuf);
    }
}

#ifdef WIN32

/*
 * Worker thread to transfer data from the pipe to the current logfile.
 *
 * We need this because on Windows, WaitforMultipleObjects does not work on
 * unnamed pipes: it always reports "signaled", so the blocking ReadFile won't
 * allow for SIGHUP; and select is for sockets only.
 */
static unsigned int __stdcall pipeThread(void* arg)
{
    char logbuffer[READ_BUF_SIZE];
    int bytes_in_logbuffer = 0;

    for (;;) {
        DWORD bytesRead;
        BOOL result = false;

        result = ReadFile(t_thrd.postmaster_cxt.syslogPipe[0],
            logbuffer + bytes_in_logbuffer,
            sizeof(logbuffer) - bytes_in_logbuffer,
            &bytesRead,
            0);

        /*
         * Enter critical section before doing anything that might touch
         * global state shared by the main thread. Anything that uses
         * palloc()/pfree() in particular are not safe outside the critical
         * section.
         */
        EnterCriticalSection(&sysloggerSection);
        if (!result) {
            DWORD error = GetLastError();

            if (error == ERROR_HANDLE_EOF || error == ERROR_BROKEN_PIPE)
                break;
            _dosmaperr(error);
            ereport(LOG, (errcode_for_file_access(), errmsg("could not read from logger pipe: %m")));
        } else if (bytesRead > 0) {
            bytes_in_logbuffer += bytesRead;
            process_pipe_input(logbuffer, &bytes_in_logbuffer);
        }

        /*
         * If we've filled the current logfile, nudge the main thread to do a
         * log rotation.
         */
        if (u_sess->attr.attr_common.Log_RotationSize > 0) {
            if (ftell(t_thrd.logger.syslogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L ||
                (t_thrd.logger.querylogFile != NULL &&
                    ftell(t_thrd.logger.querylogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) ||
                (t_thrd.logger.asplogFile != NULL &&
                    ftell(t_thrd.logger.asplogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L) ||
                (t_thrd.logger.csvlogFile != NULL &&
                    ftell(t_thrd.logger.csvlogFile) >= u_sess->attr.attr_common.Log_RotationSize * 1024L))
                SetLatch(&t_thrd.logger.sysLoggerLatch);
        }
        LeaveCriticalSection(&sysloggerSection);
    }

    /* We exit the above loop only upon detecting pipe EOF */
    t_thrd.logger.pipe_eof_seen = true;

    /* if there's any data left then force it out now */
    flush_pipe_input(logbuffer, &bytes_in_logbuffer);

    /* set the latch to waken the main thread, which will quit */
    SetLatch(&t_thrd.logger.sysLoggerLatch);

    LeaveCriticalSection(&sysloggerSection);
    _endthread();
    return 0;
}
#endif /* WIN32 */

/*
 * Open the csv log file - we do this opportunistically, because
 * we don't know if CSV logging will be wanted.
 *
 * This is only used the first time we open the csv log in a given syslogger
 * process, not during rotations.  As with opening the main log file, we
 * always append in this situation.
 */
static void open_csvlogfile(void)
{
    char* filename = NULL;

    filename = logfile_getname(
        time(NULL), ".csv", u_sess->attr.attr_common.Log_directory, u_sess->attr.attr_common.Log_filename);

    t_thrd.logger.csvlogFile = logfile_open(filename, "a", false);

    if (t_thrd.logger.last_csv_file_name != NULL) /* probably shouldn't happen */
        pfree(t_thrd.logger.last_csv_file_name);

    t_thrd.logger.last_csv_file_name = filename;
}

#ifdef ENABLE_UT
#define static
#endif
/*
 * Open a new logfile with proper permissions and buffering options.
 *
 * If allow_errors is true, we just log any open failure and return NULL
 * (with errno still correct for the fopen failure).
 * Otherwise, errors are treated as fatal.
 */
static FILE* logfile_open(const char* filename, const char* mode, bool allow_errors)
{
    FILE* fh = NULL;
    struct stat checkdir;
    bool dirIsExist = false;
    /*
     * Note we do not let Log_file_mode disable IWUSR, since we certainly want
     * to be able to write the files ourselves.
     */
    if (filename == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("group_name can not be NULL ")));
    }

    if (stat(filename, &checkdir) == 0) {
        dirIsExist = true;
    }

    fh = fopen(filename, mode);

    if (fh != NULL) {
        setvbuf(fh, NULL, LBF_MODE, 0);

#ifdef WIN32
        /* use CRLF line endings on Windows */
        _setmode(_fileno(fh), _O_TEXT);
#endif
    } else {
        int save_errno = errno;

        if (allow_errors) {
            ereport(LOG, (errcode_for_file_access(), errmsg("could not open log file \"%s\": %m", filename)));
            errno = save_errno;
        } else {
            /*
             * If open file failed, we can not write any log to file, make the
             * system crash is safe.
             */
			ereport(WARNING, (errcode_for_file_access(), errmsg("failed to open log file \"%s\": %m", filename)));

            t_thrd.int_cxt.ImmediateInterruptOK = false;
            fflush(stdout);
            fflush(stderr);
            abort();
        }
    }

    /*
     * Note we do not let Log_file_mode disable IWUSR, since we certainly want
     * to be able to write the files ourselves.
     */
    if (!dirIsExist) {
        if (chmod(filename,
            (((mode_t)u_sess->attr.attr_common.Log_file_mode | S_IWUSR) & (~S_IXUSR) & (~S_IXOTH) & (~S_IXGRP))) < 0) {
            int save_errno = errno;

            ereport(allow_errors ? LOG : FATAL,
                (errcode_for_file_access(), errmsg("could not chmod log file \"%s\": %m", filename)));
            errno = save_errno;
        }
    }

    return fh;
}
#if defined(ENABLE_UT) && defined(static)
#undef static
#endif

/*
 * perform logfile rotation
 */
static void logfile_rotate(bool time_based_rotation, int size_rotation_for)
{
    char* filename = NULL;
    char* csvfilename = NULL;
    pg_time_t fntime;
    FILE* fh = NULL;

    t_thrd.logger.rotation_requested = false;

    /*
     * When doing a time-based rotation, invent the new logfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        fntime = t_thrd.logger.next_rotation_time;
    else
        fntime = time(NULL);
    filename =
        logfile_getname(fntime, NULL, u_sess->attr.attr_common.Log_directory, u_sess->attr.attr_common.Log_filename);
    if (t_thrd.logger.csvlogFile != NULL)
        csvfilename = logfile_getname(
            fntime, ".csv", u_sess->attr.attr_common.Log_directory, u_sess->attr.attr_common.Log_filename);

    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: t_thrd.logger.last_file_name should never be NULL here, but if it is, append.
     */
    if (time_based_rotation || (size_rotation_for & LOG_DESTINATION_STDERR)) {
        if (u_sess->attr.attr_common.Log_truncate_on_rotation && time_based_rotation &&
            t_thrd.logger.last_file_name != NULL && strcmp(filename, t_thrd.logger.last_file_name) != 0)
            fh = logfile_open(filename, "w", true);
        else
            fh = logfile_open(filename, "a", true);

        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Log_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                t_thrd.logger.rotation_disabled = true;
            }

            if (filename != NULL)
                pfree(filename);
            if (csvfilename != NULL)
                pfree(csvfilename);
            return;
        }

        fclose(t_thrd.logger.syslogFile);
        t_thrd.logger.syslogFile = fh;

        /* instead of pfree'ing filename, remember it for next time */
        if (t_thrd.logger.last_file_name != NULL)
            pfree(t_thrd.logger.last_file_name);
        t_thrd.logger.last_file_name = filename;
        filename = NULL;
    }

    /* Same as above, but for csv file. */
    if (t_thrd.logger.csvlogFile != NULL && (time_based_rotation || (size_rotation_for & LOG_DESTINATION_CSVLOG)) &&
        ((unsigned int)t_thrd.log_cxt.Log_destination & LOG_DESTINATION_CSVLOG)) {
        if (u_sess->attr.attr_common.Log_truncate_on_rotation && time_based_rotation &&
            t_thrd.logger.last_csv_file_name != NULL && strcmp(csvfilename, t_thrd.logger.last_csv_file_name) != 0)
            fh = logfile_open(csvfilename, "w", true);
        else
            fh = logfile_open(csvfilename, "a", true);

        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Log_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                t_thrd.logger.rotation_disabled = true;
            }

            if (filename != NULL)
                pfree(filename);
            if (csvfilename != NULL)
                pfree(csvfilename);
            return;
        }

        fclose(t_thrd.logger.csvlogFile);
        t_thrd.logger.csvlogFile = fh;

        /* instead of pfree'ing filename, remember it for next time */
        if (t_thrd.logger.last_csv_file_name != NULL)
            pfree(t_thrd.logger.last_csv_file_name);
        t_thrd.logger.last_csv_file_name = csvfilename;
        csvfilename = NULL;
    }

    if (filename != NULL)
        pfree(filename);
    if (csvfilename != NULL)
        pfree(csvfilename);

    set_next_rotation_time();
}

/*
 * construct logfile name using timestamp information
 *
 * If suffix isn't NULL, append it to the name, replacing any ".log"
 * that may be in the pattern.
 *
 * Result is palloc'd.
 */
static char* logfile_getname(pg_time_t timestamp, const char* suffix, const char* logdir, const char* filename_pattern)
{
    char* filename = NULL;
    int len = 0;
    int ret = 0;

    filename = (char*)palloc(MAXPGPATH);

    ret = snprintf_s(filename, MAXPGPATH, MAXPGPATH - 1, "%s/", logdir);
    securec_check_ss(ret, "", "");

    len = strlen(filename);

    /* treat Log_filename as a strftime pattern */
    pg_strftime(filename + len, MAXPGPATH - len, filename_pattern, pg_localtime(&timestamp, log_timezone));

    if (suffix != NULL) {
        len = strlen(filename);
        if (len > 4 && (strcmp(filename + (len - 4), ".log") == 0))
            len -= 4;
        strlcpy(filename + len, suffix, MAXPGPATH - len);
    }

    return filename;
}

/*
 * Determine the next planned rotation time, and store in t_thrd.logger.next_rotation_time.
 */
static void set_next_rotation_time(void)
{
    pg_time_t now;
    struct pg_tm* tm_t = NULL;
    int rotinterval;

    /* nothing to do if time-based rotation is disabled */
    if (u_sess->attr.attr_common.Log_RotationAge <= 0)
        return;

    /*
     * The requirements here are to choose the next time > now that is a
     * "multiple" of the log rotation interval.  "Multiple" can be interpreted
     * fairly loosely.	In this version we align to log_timezone rather than
     * GMT.
     */
    rotinterval = u_sess->attr.attr_common.Log_RotationAge * SECS_PER_MINUTE; /* convert to seconds */
    now = (pg_time_t)time(NULL);
    tm_t = pg_localtime(&now, log_timezone);
    if (NULL == tm_t) {
        return;
    }
    now += tm_t->tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= tm_t->tm_gmtoff;
    t_thrd.logger.next_rotation_time = now;
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */
/* SIGHUP: set flag to reload config file */
static void sigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.logger.got_SIGHUP = true;
    SetLatch(&t_thrd.logger.sysLoggerLatch);

    errno = save_errno;
}

/* SIGUSR1: set flag to rotate logfile */
static void sigUsr1Handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    LogControlData* logctl = NULL;

    if (g_instance.flush_buf_requested) {
        foreach_logctl(logctl) {
            /* request to flush buffer data */
            logctl->flush_requested = true;
        }
        /* reset this request */
        g_instance.flush_buf_requested = false;
    } else {
        t_thrd.logger.rotation_requested = true;
        /* all log should be rotation requested */
        foreach_logctl(logctl) {
            logctl->rotation_requested = true;
        }
    }
    SetLatch(&t_thrd.logger.sysLoggerLatch);
    errno = save_errno;
}

void set_flag_to_flush_buffer(void)
{
    g_instance.flush_buf_requested = true;
}

/*
 * @Description: get log directory for different log type.
 * @Param[IN] logid: log type tag
 * @Param[IN] include_nodename: nodename appears in log directory info
 * @Return: log directory
 * @See also:
 */
static char* LogCtlGetLogDirectory(const char* logid, bool include_nodename)
{
    char path[MAXPGPATH] = {0};
    char* rootdir = gs_getenv_r("GAUSSLOG");
    char log_rootdir[PATH_MAX + 1] = {'\0'};
    if (rootdir == NULL || realpath(rootdir, log_rootdir) == NULL) {
        ereport(WARNING,
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
            errmsg("Failed to obtain environment value $GAUSSLOG!"),
            errdetail("N/A"),
            errcause("Incorrect environment value."),
            erraction("Please refer to backend log for more details.")));
    }
    rootdir = NULL;
    int rc = 0;

    /*
     * $GAUSSLOG env must not be an empty string.
     * if so, log directory will be under root dir '/' and permition denied.
     */
    if (*log_rootdir != '\0') {
        check_backend_env(log_rootdir);
        rc = strcat_s(path, MAXPGPATH, log_rootdir);
        securec_check_c(rc, "\0", "\0");

        rc = strcat_s(path, MAXPGPATH, "/");
        securec_check_c(rc, "\0", "\0");
    }
    /* if GAUSSLOG not set, create directory under node rootdir */
    rc = strcat_s(path, MAXPGPATH, logid);
    securec_check_c(rc, "\0", "\0");

    rc = strcat_s(path, MAXPGPATH, "/");
    securec_check_c(rc, "\0", "\0");

    if (include_nodename) {
        rc = strcat_s(path, MAXPGPATH, g_instance.attr.attr_common.PGXCNodeName);
        securec_check_c(rc, "\0", "\0");
    }
    return pstrdup(path);
}

/* copy name of src to dst whose max capacity is LOG_MAX_NODENAME_LEN */
static void copy_name(const char* src, char* dst)
{
    size_t len = strlen(src);
    if (len >= LOG_MAX_NODENAME_LEN) {
        /* truncate this name */
        len = LOG_MAX_NODENAME_LEN - 1;
    }
    int rc = memcpy_s(dst, LOG_MAX_NODENAME_LEN, src, len + 1);
    securec_check(rc, "\0", "\0");
    dst[len] = '\0';
}

/*
 * logCtlNodeName && logCtlHostName will be inited by postmaster only once.
 * it's a pity that PGXCNodeName is null in syslog thread,
 * so we have to set this global information from postmaster thread.
 */
static void LogCtlSetGlobalNames(void)
{
    if (0 == logCtlNodeName[0]) {
        copy_name(g_instance.attr.attr_common.PGXCNodeName, logCtlNodeName);
    }
    if (0 == logCtlHostName[0]) {
        const char* hostname = gs_getenv_r("HOSTNAME");
        if (NULL == hostname) {
            /* just set a default hostname */
            hostname = "UnknownHostname";
        }
        check_backend_env(hostname);
        copy_name(hostname, logCtlHostName);
    }
}

/*
 * logCtlTimeZone will be inited by postmaster only once.`
 * it's a pity that log_timezone is null in syslog thread,
 * so we have to set this global information from postmaster thread.
 */
static void LogCtlSetTimeZone(void)
{
    if (log_timezone) {
        int rc = memcpy_s(logCtlTimeZone, TZ_STRLEN_MAX + 1, pg_get_timezone_name(log_timezone), TZ_STRLEN_MAX + 1);
        securec_check(rc, "\0", "\0");
    }
}

/*
 * @Description: write head data for each new log file
 * @Param[IN] logctl: log manager for different log types
 * @Return: void
 * @See also:
 */
static void LogCtlWriteFileHeader(LogControlData* logctl)
{
    size_t hostname_len = strlen(logCtlHostName) + 1; /* including tail 0 */
    size_t nodename_len = strlen(logCtlNodeName) + 1; /* including tail 0 */
    size_t timezone_len = strlen(logCtlTimeZone) + 1; /* including tail 0 */
    size_t total_len = sizeof(LogFileHeader) + hostname_len + nodename_len + timezone_len;
    int rc = 0;

    total_len = MAXALIGN(total_len);
    Assert(total_len <= BLCKSZ);

    char* buf = (char*)palloc0(total_len);
    int off = 0;

    /* the first magic data */
    *(unsigned long*)(buf + off) = LOG_MAGICNUM;
    off += sizeof(unsigned long);

    /* version */
    *(uint16*)(buf + off) = logctl->ver;
    off += sizeof(uint16);

    /* host name */
    *(uint8*)(buf + off) = (uint8)hostname_len;
    off += sizeof(uint8);

    /* node name */
    *(uint8*)(buf + off) = (uint8)nodename_len;
    off += sizeof(uint8);

    /* time zone */
    *(uint16*)(buf + off) = (uint16)timezone_len;
    off += sizeof(uint16);

    /* host name string */
    rc = memcpy_s(buf + off, total_len - off, logCtlHostName, hostname_len);
    securec_check(rc, "\0", "\0");
    off += hostname_len;

    /* node name string */
    rc = memcpy_s(buf + off, total_len - off, logCtlNodeName, nodename_len);
    securec_check(rc, "\0", "\0");
    off += nodename_len;

    /* time zone string */
    rc = memcpy_s(buf + off, total_len - off, logCtlTimeZone, timezone_len);
    securec_check(rc, "\0", "\0");
    off += timezone_len;

    /* the last magic data */
    Assert(total_len - off >= sizeof(unsigned long));
    *(unsigned long*)(buf + total_len - sizeof(unsigned long)) = LOG_MAGICNUM;

    Assert(logctl->now_file_fd);
    errno = 0; /* clear errno before disk write */
    size_t ret_len = fwrite(buf, 1, total_len, logctl->now_file_fd);

    if ((errno != 0) || (ret_len != total_len)) {
        char errorbuf[ERROR_BUF_SIZE] = {'\0'};
        rc = sprintf_s(
            errorbuf, ERROR_BUF_SIZE, "ERROR: could not write file head for binary log: %s\n", gs_strerror(errno));
        securec_check_ss_c(rc, "\0", "\0");
        syslogger_erewrite(logctl->now_file_fd, errorbuf);
    }
    pfree(buf);
}

/*
 * @Description: rotate and create a new log file
 * @Param[IN] logctl: log manager for different log types
 * @Param[IN] time_based_rotation: time based rotation
 * @Return: void
 * @See also:
 */
static void LogCtlRotateFile(LogControlData* logctl, bool time_based_rotation)
{
    logctl->rotation_requested = false;

    pg_time_t fntime = time_based_rotation ? t_thrd.logger.next_rotation_time : time(NULL);
    char* filename = logfile_getname(fntime, NULL, logctl->log_dir, logctl->filename_pattern);

    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: logctl->now_file_name should never be NULL here, but if it is, append.
     */
    char* write_mode = NULL;
    if (u_sess->attr.attr_common.Log_truncate_on_rotation && time_based_rotation && logctl->now_file_name &&
        strcmp(filename, logctl->now_file_name) != 0) {
        write_mode = "w";
    } else {
        write_mode = "a";
    }
    FILE* fh = logfile_open(filename, write_mode, true);

    if (fh == NULL) {
        if (errno != ENFILE && errno != EMFILE) {
            ereport(LOG, (errmsg("disable automatic PLOG rotation (use SIGHUP to re-enable)")));
            /* disable file rotation of all log types if IO error happens */
            t_thrd.logger.rotation_disabled = true;
        }

        pfree(filename);
        return;
    }

    fclose(logctl->now_file_fd);
    logctl->now_file_fd = fh;

    /* instead of pfree'ing filename, remember it for next time */
    if (logctl->now_file_name != NULL) {
        pfree(logctl->now_file_name);
    }
    logctl->now_file_name = filename;

    /* write file header */
    LogCtlWriteFileHeader(logctl);
}

/*
 * @Description: rotate log file if needed. we will flush
 *   the buffered data into previous log file.
 * @Param[IN] logctl: log manager for different log types
 * @Param[IN] time_based_rotation: time based rotation
 * @Return: void
 * @See also:
 */
static void LogCtlRotateLogFileIfNeeded(LogControlData* logctl, bool time_based_rotation)
{
    /* just flush buffered log into file, not rotate file */
    if (logctl->flush_requested) {
        logctl->flush_requested = false;
        LogCtlFlushBuf(logctl);
        /* rotation will be handled by the following */
    }

    /*
     * case 1: rotation requested, including rotation age request.
     * case 2: single file size reaches PROFILE_LOG_ROTATE_SIZE amount.
     */
    if (logctl->rotation_requested ||
        (!t_thrd.logger.rotation_disabled &&
            (ftell(logctl->now_file_fd) >= (PROFILE_LOG_ROTATE_SIZE - logctl->cur_len)))) {
        LogCtlFlushBuf(logctl);
        LogCtlRotateFile(logctl, time_based_rotation);
    }
}

/*
 * @Description: flush log buffer data into disk.
 *    if disk is full, process will enter waiting forever until this problem
 *    is solved.
 *    if disk writing error happens, stderr will be written. (may be some problems)
 * @Return: void
 * @See also:
 */
static void LogCtlFlushBuf(LogControlData* logctl)
{
    /* flush log into disk file */
    for (;;) {
        /* clear errno before calling IO write */
        errno = 0;

        size_t rc = fwrite(logctl->log_buf, 1, logctl->cur_len, logctl->now_file_fd);
        if ((errno != 0) || ((int)rc != logctl->cur_len)) {
            /*
             * if no disk space, we will retry,
             * and we can not report a log, because there is not space to write.
             */
            if (errno == ENOSPC) {
                break;
            }

            /* disk IO error, print message and discard this logs */
            char errorbuf[ERROR_BUF_SIZE] = {'\0'};
            int rc_error = sprintf_s(errorbuf,
                ERROR_BUF_SIZE,
                "ERROR: could not write profile log: %s\n"
                "WARNING: discard profile log because of disk IO error\n",
                gs_strerror(errno));
            securec_check_ss_c(rc_error, "\0", "\0");
            syslogger_erewrite(logctl->now_file_fd, errorbuf);
        }
        break;
    }

    /* buffer is empty */
    logctl->cur_len = 0;
}

/*
 * @Description: append an input log message into log buffer.
 *    if buffer will be full, flush buffer first.
 * @Param[IN] logctl: log manager for different log types
 * @Param[IN] msg: input log message
 * @Param[IN] len: length of input log message
 * @Return: void
 * @See also:
 */
static void LogCtlProcessInput(LogControlData* logctl, const char* msg, int len)
{
    if (logctl->cur_len + len > logctl->max_len) {
        LogCtlFlushBuf(logctl);
    }

    int rc = memcpy_s(logctl->log_buf + logctl->cur_len, logctl->max_len, msg, len);
    securec_check(rc, "\0", "\0");
    logctl->cur_len += len;
}

/*
 * @Description: get its log file name pattern for different log type.
 * @Param[IN] post_suffix: log file suffix
 * @Return: log file name pattern
 * @See also:
 */
static char* LogCtlGetFilenamePattern(const char* post_suffix)
{
    char* pattern = NULL;
    const size_t len = strlen(u_sess->attr.attr_common.Log_filename);
    int maxlen = 0;
    int rc = 0;

    /*
     * if there is a suffix within file pattern,
     * replace it with my own suffix. otherwise,
     * append with my own suffix.
     */
    char* p = (char*)memrchr(u_sess->attr.attr_common.Log_filename, '.', len);
    size_t filename_len = p ? (p - u_sess->attr.attr_common.Log_filename) : len;

    /* 5 = strlen(PROFILE_LOG_SUFFIX) + sizeof('\0') */
    maxlen = filename_len + 5;
    pattern = (char*)palloc(maxlen);
    rc = memcpy_s(pattern, maxlen, u_sess->attr.attr_common.Log_filename, filename_len);
    securec_check(rc, "\0", "\0");
    /* append filename post suffix */
    rc = memcpy_s(pattern + filename_len, maxlen - filename_len, PROFILE_LOG_SUFFIX, 4);
    securec_check(rc, "\0", "\0");
    pattern[maxlen - 1] = '\0';

    return pattern;
}

/*
 * @Description: Init profile log control data.
 *   notice, this function is reentrant.
 * @Return: void
 * @See also:
 */
static void PLogCtlInit(void)
{
    t_thrd.log_cxt.pLogCtl = (LogControlData*)palloc0(sizeof(LogControlData));
    t_thrd.log_cxt.pLogCtl->ver = PROFILE_LOG_VERSION;
    t_thrd.log_cxt.pLogCtl->rotation_requested = false;
    t_thrd.log_cxt.pLogCtl->flush_requested = false;

    if (NULL == t_thrd.log_cxt.pLogCtl->log_dir) {
        t_thrd.log_cxt.pLogCtl->log_dir = LogCtlGetLogDirectory(PROFILE_LOG_TAG, true);
        if (0 == mkdir(t_thrd.log_cxt.pLogCtl->log_dir, S_IRWXU) || (EEXIST == errno)) {
            /* make sure dir permition is 700 */
            (void)chmod(t_thrd.log_cxt.pLogCtl->log_dir, S_IRWXU);
        } else {
            /* this directory may be created already, don't care this case */
            if (EEXIST != errno) {
                ereport(FATAL,
                    (errmsg(
                        "ERROR: could not create directory \"%s\": %s\n", PROFILE_LOG_TAG, gs_strerror(errno))));
            }
        }
    }

    if (NULL == t_thrd.log_cxt.pLogCtl->filename_pattern) {
        t_thrd.log_cxt.pLogCtl->file_suffix = PROFILE_LOG_SUFFIX;
        /* plog file pattern should be the same with error log, but post suffix */
        t_thrd.log_cxt.pLogCtl->filename_pattern = LogCtlGetFilenamePattern(PROFILE_LOG_SUFFIX);
        t_thrd.log_cxt.pLogCtl->now_file_name = NULL;
        t_thrd.log_cxt.pLogCtl->now_file_fd = NULL;
    }

    if (NULL == t_thrd.log_cxt.pLogCtl->log_buf) {
        t_thrd.log_cxt.pLogCtl->log_buf = (char*)palloc(READ_BUF_SIZE);
        t_thrd.log_cxt.pLogCtl->max_len = READ_BUF_SIZE;
        t_thrd.log_cxt.pLogCtl->cur_len = 0;
    }

    /* do the last two steps */
    t_thrd.log_cxt.pLogCtl->inited = true;
    allLogCtl[LOG_TYPE_PLOG] = t_thrd.log_cxt.pLogCtl;
}

/*
 * @Description: create log directory for the other types if not present,
 *     ignore errors.
 * @Return: void
 * @See also:
 */
static void LogCtlCreateLogParentDirectory(void)
{
    char* logdir = NULL;

    /* create directory for profile log.
     * if EEXIST == errno, this directory may be created already, don't care this case.
     */
    logdir = LogCtlGetLogDirectory(PROFILE_LOG_TAG, false);
    if (0 == mkdir(logdir, S_IRWXU) || (EEXIST == errno)) {
        /*
         * make sure dir permition is 700.
         * parent directory may be created by OM tool. if not so
         * chmod() may be called by many process, and it maybe failed.
         * ignore its returned value of this case.
         */
        (void)chmod(logdir, S_IRWXU);
    } else if (EEXIST != errno) {
        ereport(FATAL, (errmsg("could not create log directory \"%s\": %s\n", logdir, gs_strerror(errno))));
    }
    pfree(logdir);

    /* for the other log types */
}

/*
 * @Description: Postmaster must do the last flush for all the logs.
 *    Under thread mode, postmaster doesn't wait the syslogger exit
 *    before the whole process exit, and it finishes first. so syslogger
 *    thread may be died directly and log data maybe loss.
 *    error log doesn't have this problem, because it's written directly
 *    into disk and flushed by OS during process exits.
 * @Return: void
 * @See also:
 */
void LogCtlLastFlushBeforePMExit(void)
{
    /* return in fast path if log is off */
    if (!g_instance.attr.attr_common.Logging_collector) {
        return;
    }

    LogControlData* logctl = NULL;

    /*
     * needn't lock, because no profile data is generated,
     * and syslogger never call flushing buffer.
     */
    foreach_logctl(logctl) {
        LogCtlFlushBuf(logctl);

        /*
         * memory and fd will be released because
         * the whole process is exiting.
         */
    }
}

void* ASPOpenLogFile(bool *doOpen)
{
    if (doOpen != NULL) {
        *doOpen = false;
    }

    if (t_thrd.logger.asplogFile == NULL) {
        char *filename = logfile_getname(time(NULL), ".log",
            g_instance.attr.attr_common.asp_log_directory, u_sess->attr.attr_common.asp_log_filename);
        t_thrd.logger.asplogFile = logfile_open(filename, "a", false);

        if (t_thrd.logger.last_asp_file_name != NULL) /* probably shouldn't happen */
            pfree(t_thrd.logger.last_asp_file_name);
        t_thrd.logger.last_asp_file_name = filename;
        if (doOpen != NULL) {
            *doOpen = true;
        }
    }
    return (void *)t_thrd.logger.asplogFile;
}

void ASPCloseLogFile()
{
    if (t_thrd.logger.asplogFile != NULL) {
        fclose(t_thrd.logger.asplogFile);
        t_thrd.logger.asplogFile = NULL;
    }
}

/*
 *  * perform logfile rotation
 *   */
static void asp_logfile_rotate(bool time_based_rotation, int size_rotation_for)
{
    char* aspFilename = NULL;
    pg_time_t fntime;
    FILE* fh = NULL;

    t_thrd.logger.rotation_requested = false;

    /*
     * When doing a time-based rotation, invent the new logfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        fntime = t_thrd.logger.next_rotation_time;
    else
        fntime = time(NULL);
    aspFilename =
        logfile_getname(time(NULL), ".log",
            g_instance.attr.attr_common.asp_log_directory,
            u_sess->attr.attr_common.asp_log_filename);
    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: t_thrd.logger.last_file_name should never be NULL here, but if it is, append.
     */
    if ((time_based_rotation || (size_rotation_for & LOG_DESTINATION_ASPLOG)) && pmState == PM_RUN) {
        if (u_sess->attr.attr_common.Log_truncate_on_rotation && time_based_rotation &&
            t_thrd.logger.asplogFile != NULL && strcmp(aspFilename, t_thrd.logger.last_asp_file_name) != 0) {
            fh = logfile_open(aspFilename, "w", true);
        } else {
            fh = logfile_open(aspFilename, "a", true);
        }
        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Log_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                t_thrd.logger.rotation_disabled = true;
            }

            if (aspFilename != NULL)
                pfree(aspFilename);
            return;
        }

        if (t_thrd.logger.asplogFile != NULL) {
            fclose(t_thrd.logger.asplogFile);
        }

        t_thrd.logger.asplogFile = fh;

        /* instead of pfree'ing filename, remember it for next time */
        if (t_thrd.logger.last_asp_file_name != NULL)
            pfree(t_thrd.logger.last_asp_file_name);
        t_thrd.logger.last_asp_file_name = aspFilename;
        aspFilename = NULL;
    }

    if (aspFilename != NULL) {
        pfree(aspFilename);
    }
}

void* SQMOpenLogFile(bool *doOpen)
{
    if (doOpen != NULL)
        *doOpen = false;

    if (t_thrd.logger.querylogFile == NULL) {
        char *filename = logfile_getname(time(NULL), ".log",
                                         g_instance.attr.attr_common.query_log_directory,
                                         u_sess->attr.attr_common.query_log_file);
        t_thrd.logger.querylogFile = logfile_open(filename, "a", false);
        pfree(filename);
        if (doOpen != NULL) {
            *doOpen = true;
        }
    }
    return (void *)t_thrd.logger.querylogFile;
}

void SQMCloseLogFile()
{
    if (t_thrd.logger.querylogFile != NULL) {
        fclose(t_thrd.logger.querylogFile);
        t_thrd.logger.querylogFile = NULL;
    }
}

static void slow_query_logfile_rotate(bool time_based_rotation, int size_rotation_for)
{
    char* queryFilename = NULL;
    pg_time_t fntime;
    FILE* fh = NULL;

    t_thrd.logger.rotation_requested = false;

    /*
     * When doing a time-based rotation, invent the new logfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        fntime = t_thrd.logger.next_rotation_time;
    else
        fntime = time(NULL);
    queryFilename =
        logfile_getname(time(NULL), ".log", g_instance.attr.attr_common.query_log_directory, u_sess->attr.attr_common.query_log_file);
    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: t_thrd.logger.last_file_name should never be NULL here, but if it is, append.
     */
    if ((time_based_rotation || (size_rotation_for & LOG_DESTINATION_QUERYLOG)) && pmState == PM_RUN) {
        if (u_sess->attr.attr_common.Log_truncate_on_rotation && time_based_rotation &&
            t_thrd.logger.querylogFile != NULL && strcmp(queryFilename, t_thrd.logger.last_query_log_file_name) != 0) {
            fh = logfile_open(queryFilename, "w", true);
        } else {
            fh = logfile_open(queryFilename, "a", true);
        }

        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Log_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                t_thrd.logger.rotation_disabled = true;
            }

            if (queryFilename != NULL)
                pfree(queryFilename);
            return;
        }

        if (t_thrd.logger.querylogFile != NULL) {
            fclose(t_thrd.logger.querylogFile);
        }

        t_thrd.logger.querylogFile = fh;

        /* instead of pfree'ing filename, remember it for next time */
        if (t_thrd.logger.last_query_log_file_name != NULL)
            pfree(t_thrd.logger.last_query_log_file_name);
        t_thrd.logger.last_query_log_file_name = queryFilename;
        queryFilename = NULL;
    }

    if (queryFilename != NULL)
        pfree(queryFilename);
}

void init_instr_log_directory(bool include_nodename, const char* logid)
{
    /* create directory for aspy & slow query log */
    char* logdir = NULL;
    logdir = LogCtlGetLogDirectory(logid, include_nodename);

    if (pg_mkdir_p(logdir, S_IRWXU) == 0 || (errno == EEXIST)) {
        /*
         * make sure dir permition is 700.
         * parent directory may be created by OM tool. if not so
         * chmod() may be called by many process, and it maybe failed.
         * ignore its returned value of this case.
         */
        (void)chmod(logdir, S_IRWXU);
    } else {
        /* this directory may be created already, don't care this case */
        if (errno != EEXIST) {
            pfree(logdir);
            ereport(FATAL,
                (errmsg(
                    "ERROR: could not create instr log directory \"%s\": %s\n", logid, gs_strerror(errno))));
        }
    }

    if (include_nodename) {
        if (strcmp(logid, ASP_LOG_TAG) == 0) {
            g_instance.attr.attr_common.asp_log_directory = logdir;
        } else if (strcmp(logid, SLOWQUERY_LOG_TAG) == 0) {
            g_instance.attr.attr_common.query_log_directory = logdir;
        } else if (strcmp(logid, PERF_JOB_TAG) == 0) {
            g_instance.attr.attr_common.Perf_directory = logdir;
        }
    } else {
        pfree(logdir);
    }
}
