/*-------------------------------------------------------------------------
 *
 * parallel.cpp
 *
 *	Parallel support for pg_dump and pg_restore
 *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/bin/pg_dump/parallel.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Parallel operation works like this:
 *
 * The original, leader process calls ParallelBackupStart(), which forks off
 * the desired number of worker processes, which each enter WaitForCommands().
 *
 * The leader process dispatches an individual work item to one of the worker
 * processes in DispatchJobForTocEntry().  We send a command string such as
 * "DUMP 1234" or "RESTORE 1234", where 1234 is the TocEntry ID.
 * The worker process receives and decodes the command and passes it to the
 * routine pointed to by AH->WorkerJobDumpPtr or AH->WorkerJobRestorePtr,
 * which are routines of the current archive format.  That routine performs
 * the required action (dump or restore) and returns an integer status code.
 * This is passed back to the leader where we pass it to the
 * ParallelCompletionPtr callback function that was passed to
 * DispatchJobForTocEntry().  The callback function does state updating
 * for the leader control logic in pg_backup_archiver.c.
 *
 * In principle additional archive-format-specific information might be needed
 * in commands or worker status responses, but so far that hasn't proved
 * necessary, since workers have full copies of the ArchiveHandle/TocEntry
 * data structures.  Remember that we have forked off the workers only after
 * we have read in the catalog.  That's why our worker processes can also
 * access the catalog information.
 *
 * In the leader process, the workerStatus field for each worker has one of
 * the following values:
 *		WRKR_NOT_STARTED: we've not yet forked this worker
 *		WRKR_IDLE: it's waiting for a command
 *		WRKR_WORKING: it's working on a command
 *		WRKR_TERMINATED: process ended
 * The pstate->te[] entry for each worker is valid when it's in WRKR_WORKING
 * state, and must be NULL in other states.
 */
#include "postgres_fe.h"
#include "dumpmem.h"
#include "parallel.h"
#include <signal.h>
#include <sys/select.h>
#include <sys/wait.h>
#include "libpq/pqsignal.h"
#include "bin/elog.h"

/* Mnemonic macros for indexing the fd array returned by pipe(2) */
#define PIPE_READ 0
#define PIPE_WRITE 1

#define NO_SLOT (-1) /* Failure result for GetIdleWorker() */

/* Worker process statuses */
typedef enum { WRKR_NOT_STARTED = 0, WRKR_IDLE, WRKR_WORKING, WRKR_TERMINATED } T_WorkerStatus;

#define WORKER_IS_RUNNING(workerStatus) ((workerStatus) == WRKR_IDLE || (workerStatus) == WRKR_WORKING)

#define messageStartsWith(msg, prefix) (strncmp(msg, prefix, strlen(prefix)) == 0)

/*
 * Private per-parallel-worker state (typedef for this is in parallel.h).
 *
 * Much of this is valid only in the leader process But the AH field shoule
 * be touched only by workers. The pipe descriptors are valid everywhere.
 */
struct ParallelSlotN {
    T_WorkerStatus workerStatus; /* see enum above */
    int pipeRead;                /* leader's end of the pipes */
    int pipeWrite;
    int pipeRevRead; /* child's end of the pipes */
    int pipeRevWrite;

    ArchiveHandle* AH; /* Archive data worker is using */

    /* Child process/thread identity info: */
    pid_t pid;
};

/*
 * State info for archive_close_connection() shutdown callback.
 */
typedef struct ShutdownInformationN {
    ParallelStateN* pstate;
    Archive* AHX;
} ShutdownInformationN;

static ShutdownInformationN shutdown_info;

/*
 * State info for signal handling.
 * We assume signal_info initializes to zeroes.
 *
 * On Unix, myAH is the leader DB connection in the leader process, and the
 * worker's own connection in worker processes.
 */
typedef struct DumpSignalInformation {
    ArchiveHandle* myAH;    /* database connection to issue cancel for */
    ParallelStateN* pstate; /* parallel state, if any */
    bool handler_set;       /* signal handler set up in this process? */
#ifndef WIN32
    bool am_worker; /* am I a worker process? */
#endif
} DumpSignalInformation;

static volatile DumpSignalInformation signal_info;

static void WaitForCommands(ArchiveHandle* AH, int pipefd[2]);
static void sendMessageToPipe(int fd, const char* str);
static char* readMessageFromPipe(int fd);
static char* getMessageFromLeader(int pipefd[2]);
static void RunWorker(ArchiveHandle* AH, ParallelSlotN* slot);
static void buildWorkerCommand(TocEntry* te, T_Action act, char* buf, int buflen);
static void parseWorkerCommand(ArchiveHandle* AH, TocEntry** te, T_Action* act, const char* msg);
static int GetIdleWorker(ParallelStateN* pstate);
static bool ListenToWorkers(ParallelStateN* pstate, bool do_wait);
static char* getMessageFromWorker(ParallelStateN* pstate, bool do_wait, int* worker);
static int select_loop(int maxFd, fd_set* workerset);
static bool HasEveryWorkerTerminated(ParallelStateN* pstate);
static void lockTableForWorker(ArchiveHandle* AH, TocEntry* te);
static ParallelSlotN* GetMyPSlot(ParallelStateN* pstate);
static void ShutdownWorkersHard(ParallelStateN* pstate);
static void WaitForTerminatingWorkers(ParallelStateN* pstate);
static void sigTermHandler(SIGNAL_ARGS);
static void setup_cancel_handler(void);
static const char* fmtQualifiedId(const char* schema, const char* id);
static void archive_close_connection(int code, void* arg);

/*
 * fmtQualifiedId - construct a schema-qualified name, with quoting as needed.
 *
 * Like fmtId, use the result before calling again.
 *
 * Since we call fmtId and it also uses getLocalPQExpBuffer() we cannot
 * use that buffer until we're finished with calling fmtId().
 */
static const char* fmtQualifiedId(const char* schema, const char* id)
{
    static PQExpBuffer id_return = NULL;

    if (id_return != NULL) /* first time through? */
        resetPQExpBuffer(id_return);
    else
        id_return = createPQExpBuffer();

    /* Suppress schema name if fetching from pre-7.3 DB */
    if (schema != NULL && *schema) {
        appendPQExpBuffer(id_return, "%s.", fmtId(schema));
    }
    appendPQExpBuffer(id_return, "%s", fmtId(id));

    return id_return->data;
}
/*
 * Find the ParallelSlotN for the current worker process or thread.
 *
 * Returns NULL if no matching slot is found (this implies we're the leader).
 */
static ParallelSlotN* GetMyPSlot(ParallelStateN* pstate)
{
    int i;

    for (i = 0; i < pstate->workerNum; i++) {
        if (pstate->parallelSlot[i].pid == getpid())
            return &(pstate->parallelSlot[i]);
    }

    return NULL;
}

/*
 * pg_dump and pg_restore call this to register the cleanup handler
 * as soon as they've created the ArchiveHandle.
 */
void on_exit_close_parallel_archive(Archive* AHX)
{
    shutdown_info.AHX = AHX;
    on_exit_nicely(archive_close_connection, &shutdown_info);
}

/*
 * on_exit_nicely handler for shutting down database connections and
 * worker processes cleanly.
 */
static void archive_close_connection(int code, void* arg)
{
    ShutdownInformationN* si = (ShutdownInformationN*)arg;

    if (si->pstate) {
        /* In parallel mode, must figure out who we are */
        ParallelSlotN* slot = GetMyPSlot(si->pstate);

        if (!slot) {
            /*
             * We're the leader.  Forcibly shut down workers, then close our
             * own database connection, if any.
             */
            ShutdownWorkersHard(si->pstate);

            if (si->AHX)
                DisconnectDatabase(si->AHX);
        } else {
            /*
             * We're a worker.  Shut down our own DB connection if any.
             * (Without this, if this is a premature exit, the leader would
             * fail to detect it because there would be no EOF condition on
             * the other end of the pipe.)
             */
            if (slot->AH)
                DisconnectDatabase(&(slot->AH->publicArc));
        }
    } else {
        /* Non-parallel operation: just kill the leader DB connection */
        if (si->AHX)
            DisconnectDatabase(si->AHX);
    }
}

/*
 * Forcibly shut down any remaining workers, waiting for them to finish.
 *
 * Note that we don't expect to come here during normal exit (the workers
 * should be long gone, and the ParallelStateN too).  We're only here in a
 * fatal() situation, so intervening to cancel active commands is
 * appropriate.
 */
static void ShutdownWorkersHard(ParallelStateN* pstate)
{
    int i;

    /*
     * Close our write end of the sockets so that any workers waiting for
     * commands know they can exit.  (Note: some of the pipeWrite fields might
     * still be zero, if we failed to initialize all the workers.  Hence, just
     * ignore errors here.)
     */
    for (i = 0; i < pstate->workerNum; i++)
        closesocket(pstate->parallelSlot[i].pipeWrite);

    /*
     * Force early termination of any commands currently in progress.
     */
    /* On non-Windows, send SIGTERM to each worker process. */
    for (i = 0; i < pstate->workerNum; i++) {
        pid_t pid = pstate->parallelSlot[i].pid;

        if (pid != 0)
            kill(pid, SIGTERM);
    }

    /* Now wait for them to terminate. */
    WaitForTerminatingWorkers(pstate);
}
/*
 * Wait for all workers to terminate.
 */
static void WaitForTerminatingWorkers(ParallelStateN* pstate)
{
    while (!HasEveryWorkerTerminated(pstate)) {
        ParallelSlotN* slot = NULL;
        int j;

        /* On non-Windows, use wait() to wait for next worker to end */
        int status;
        pid_t pid = wait(&status);

        /* Find dead worker's slot, and clear the PID field */
        for (j = 0; j < pstate->workerNum; j++) {
            slot = &(pstate->parallelSlot[j]);
            if (slot->pid == pid) {
                slot->pid = 0;
                break;
            }
        }

        /* On all platforms, update workerStatus and te[] as well */
        Assert(j < pstate->workerNum);
        slot->workerStatus = WRKR_TERMINATED;
        pstate->te[j] = NULL;
    }
}

/*
 * Code for responding to cancel interrupts (SIGINT, control-C, etc)
 *
 * This doesn't quite belong in this module, but it needs access to the
 * ParallelStateN data, so there's not really a better place either.
 *
 * When we get a cancel interrupt, we could just die, but in pg_restore that
 * could leave a SQL command (e.g., CREATE INDEX on a large table) running
 * for a long time.  Instead, we try to send a cancel request and then die.
 * pg_dump probably doesn't really need this, but we might as well use it
 * there too.  Note that sending the cancel directly from the signal handler
 * is safe because PQcancel() is written to make it so.
 *
 * In parallel operation on Unix, each process is responsible for canceling
 * its own connection (this must be so because nobody else has access to it).
 * Furthermore, the leader process should attempt to forward its signal to
 * each child.  In simple manual use of pg_dump/pg_restore, forwarding isn't
 * needed because typing control-C at the console would deliver SIGINT to
 * every member of the terminal process group --- but in other scenarios it
 * might be that only the leader gets signaled.
 */

/*
 * Signal handler (Unix only)
 */
static void sigTermHandler(SIGNAL_ARGS)
{
    int i;
    char errbuf[1];

    /*
     * Some platforms allow delivery of new signals to interrupt an active
     * signal handler.  That could muck up our attempt to send PQcancel, so
     * disable the signals that setup_cancel_handler enabled.
     */
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SIG_IGN);
    pqsignal(SIGQUIT, SIG_IGN);

    /*
     * If we're in the leader, forward signal to all workers.  (It seems best
     * to do this before PQcancel; killing the leader transaction will result
     * in invalid-snapshot errors from active workers, which maybe we can
     * quiet by killing workers first.)  Ignore any errors.
     */
    if (signal_info.pstate != NULL) {
        for (i = 0; i < signal_info.pstate->workerNum; i++) {
            pid_t pid = signal_info.pstate->parallelSlot[i].pid;

            if (pid != 0)
                kill(pid, SIGTERM);
        }
    }

    /*
     * Send QueryCancel if we have a connection to send to.  Ignore errors,
     * there's not much we can do about them anyway.
     */
    if (signal_info.myAH != NULL && signal_info.myAH->connCancel != NULL)
        (void)PQcancel(signal_info.myAH->connCancel, errbuf, sizeof(errbuf));

    /*
     * Report we're quitting, using nothing more complicated than write(2).
     * When in parallel operation, only the leader process should do this.
     */
    if (!signal_info.am_worker) {
        if (progname) {
            write_stderr(progname);
            write_stderr(": ");
        }
        write_stderr("terminated by user\n");
    }

    /*
     * And die, using _exit() not exit() because the later will invoke atexit
     * handlers that can fail if we interrupted related code.
     */
    _exit(1);
}

/*
 * Enable cancel interrupt handler, if not already done.
 */
static void setup_cancel_handler(void)
{
    /*
     * When forking, signal_info.handler_set will propagate into the new
     * process, but that's fine because the signal handler state does too.
     */
    if (!signal_info.handler_set) {
        signal_info.handler_set = true;

        pqsignal(SIGINT, sigTermHandler);
        pqsignal(SIGTERM, sigTermHandler);
        pqsignal(SIGQUIT, sigTermHandler);
    }
}

/*
 * set_archive_cancel_info
 *
 * Fill AH->connCancel with cancellation info for the specified database
 * connection; or clear it if conn is NULL.
 */
void set_archive_cancel_info(ArchiveHandle* AH, PGconn* conn)
{
    PGcancel* oldConnCancel;

    /*
     * Activate the interrupt handler if we didn't yet in this process.
     */
    setup_cancel_handler();

    /*
     * On Unix, we assume that storing a pointer value is atomic with respect
     * to any possible signal interrupt.
     */

    /* Free the old one if we have one */
    oldConnCancel = AH->connCancel;
    /* be sure interrupt handler doesn't use pointer while freeing */
    AH->connCancel = NULL;

    if (oldConnCancel != NULL)
        PQfreeCancel(oldConnCancel);

    /* Set the new one if specified */
    if (conn)
        AH->connCancel = PQgetCancel(conn);

    /*
     * On Unix, there's only ever one active ArchiveHandle per process, so we
     * can just set signal_info.myAH unconditionally.
     */
    signal_info.myAH = AH;
}

ParallelStateN* ParallelBackupStart(ArchiveHandle* AH)
{
    ParallelStateN* pstate;

    Assert(AH->publicArc.workerNum > 1);

    pstate = (ParallelStateN*)pg_malloc(sizeof(ParallelStateN));

    pstate->workerNum = AH->publicArc.workerNum;

    /* Create status arrays, being sure to initialize all fields to 0 */
    pstate->te = (TocEntry**)pg_malloc_zero(pstate->workerNum * sizeof(TocEntry*));
    pstate->parallelSlot = (ParallelSlotN*)pg_malloc_zero(pstate->workerNum * sizeof(ParallelSlotN));

    /*
     * Set the pstate in shutdown_info, to tell the exit handler that it must
     * clean up workers as well as the main database connection.  But we don't
     * set this in signal_info yet, because we don't want child processes to
     * inherit non-NULL signal_info.pstate.
     */
    shutdown_info.pstate = pstate;

    /*
     * Temporarily disable query cancellation on the leader connection.  This
     * ensures that child processes won't inherit valid AH->connCancel
     * settings and thus won't try to issue cancels against the leader's
     * connection.  No harm is done if we fail while it's disabled, because
     * the leader connection is idle at this point anyway.
     */
    set_archive_cancel_info(AH, NULL);

    /* Ensure stdio state is quiesced before forking */
    fflush(NULL);

    for (int i = 0; i < AH->publicArc.workerNum; i++) {
        pid_t pid;

        /* instruct signal handler that we're in a worker now */
        signal_info.am_worker = true;
        ParallelSlotN* slot = &(pstate->parallelSlot[i]);
        int pipeMW[2],  // master -> worker
            pipeWM[2];

        /* Create communication pipes for this worker */
        if (pipe(pipeMW) < 0 || pipe(pipeWM) < 0)
            exit_horribly(NULL, "could not create communication channels %m\n");

        /* leader's ends of the pipes */
        slot->pipeRead = pipeWM[PIPE_READ];
        slot->pipeWrite = pipeMW[PIPE_WRITE];
        /* child's ends of the pipes */
        slot->pipeRevRead = pipeMW[PIPE_READ];
        slot->pipeRevWrite = pipeWM[PIPE_WRITE];

        pid = fork();
        if (pid == 0) {
            /* we are the worker */
            /* close read end of Worker -> Leader */
            closesocket(pipeWM[PIPE_READ]);
            /* close write end of Leader -> Worker */
            closesocket(pipeMW[PIPE_WRITE]);

            /*
             * Close all inherited fds for communication of the leader with
             * previously-forked workers.
             */
            for (int j = 0; j < i; j++) {
                closesocket(pstate->parallelSlot[j].pipeRead);
                closesocket(pstate->parallelSlot[j].pipeWrite);
            }

            /* Run the worker ... */
            RunWorker(AH, slot);

            /* We can just exit(0) when done */
            exit(0);
        } else if (pid < 0) {
            /* fork failed */
            exit_horribly(NULL, "could not create worker process: \"%m\"");
        }

        slot->pid = pid;
        slot->workerStatus = WRKR_IDLE;

        /* close read end of Leader -> Worker */
        closesocket(pipeMW[PIPE_READ]);
        /* close write end of Worker -> Leader */
        closesocket(pipeWM[PIPE_WRITE]);
    }

    pqsignal(SIGPIPE, SIG_IGN);

    /*
     * Re-establish query cancellation on the leader connection.
     */
    set_archive_cancel_info(AH, AH->connection);

    /*
     * Tell the cancel signal handler to forward signals to worker processes,
     * too.  (As with query cancel, we did not need this earlier because the
     * workers have not yet been given anything to do; if we die before this
     * point, any already-started workers will see EOF and quit promptly.)
     */
    shutdown_info.pstate = pstate;

    return pstate;
}

/*
 * Close down a parallel dump or restore.
 */
void ParallelBackupEnd(ArchiveHandle* AH, ParallelStateN* pstate)
{
    int i;

    /* No work if non-parallel */
    if (pstate->workerNum == 1)
        return;

    /* There should not be any unfinished jobs */
    Assert(IsEveryWorkerIdle(pstate));

    /* Close the sockets so that the workers know they can exit */
    for (i = 0; i < pstate->workerNum; i++) {
        closesocket(pstate->parallelSlot[i].pipeRead);
        closesocket(pstate->parallelSlot[i].pipeWrite);
    }

    /* Wait for them to exit */
    WaitForTerminatingWorkers(pstate);

    /*
     * Unlink pstate from shutdown_info, so the exit handler will not try to
     * use it; and likewise unlink from signal_info.
     */
    shutdown_info.pstate = NULL;

    /* Release state (mere neatnik-ism, since we're about to terminate) */
    free(pstate->te);
    free(pstate->parallelSlot);
    free(pstate);
}

/*
 * This function is called by Unix to set up and run a worker
 * process.  Caller should exit the process (or thread) upon return.
 */
static void RunWorker(ArchiveHandle* AH, ParallelSlotN* slot)
{
    int pipefd[2];

    /* fetch child ends of pipes */
    pipefd[PIPE_READ] = slot->pipeRevRead;
    pipefd[PIPE_WRITE] = slot->pipeRevWrite;

    /*
     * Clone the archive so that we have our own state to work with, and in
     * particular our own database connection.
     *
     * we don't need to because fork() gives us a copy in our own address space
     * already.  But CloneArchive resets the state information and also clones
     * the database connection which both seem kinda helpful.
     */
    AH = CloneArchive(AH);

    /* Remember cloned archive where signal handler can find it */
    slot->AH = AH;

    /*
     * Call the setup worker function that's defined in the ArchiveHandle.
     */
    (AH->SetupWorkerptr)((Archive*)AH);

    /*
     * Execute commands until done.
     */
    WaitForCommands(AH, pipefd);

    /*
     * Disconnect from database and clean up.
     */
    slot->AH = NULL;
    DisconnectDatabase(&AH->publicArc);
    DeCloneArchive(AH);
}

/*
 * These next four functions handle construction and parsing of the command
 * strings and response strings for parallel workers.
 *
 * Currently, these can be the same regardless of which archive format we are
 * processing.  In future, we might want to let format modules override these
 * functions to add format-specific data to a command or response.
 */

/*
 * buildWorkerCommand: format a command string to send to a worker.
 *
 * The string is built in the caller-supplied buffer of size buflen.
 */
static void buildWorkerCommand(TocEntry* te, T_Action act, char* buf, int buflen)
{
    if (act == ACT_DUMP) {
        int nRet = snprintf_s(buf, buflen, buflen - 1, "DUMP %d", te->dumpId);
        securec_check_ss_c(nRet, buf, "\0");
    } else
        Assert(false);
}

/*
 * parseWorkerCommand: interpret a command string in a worker.
 */
static void parseWorkerCommand(ArchiveHandle* AH, TocEntry** te, T_Action* act, const char* msg)
{
    DumpId dumpId;
    int nBytes;

    if (messageStartsWith(msg, "DUMP ")) {
        *act = ACT_DUMP;
        int ret = sscanf_s(msg, "DUMP %d%n", &dumpId, &nBytes);
        securec_check_for_sscanf_s(ret, 1, "\0", "\0");
        Assert(nBytes == strlen(msg));
        *te = getTocEntryByDumpId(AH, dumpId);
        Assert(*te != NULL);
    } else {
        exit_horribly(NULL, "unrecognized command received from leader: \"%s\"", msg);
    }
}

static void buildWorkerResponse(ArchiveHandle* AH, TocEntry* te, char* buf, int buflen)
{
    int nRet = snprintf_s(buf, buflen, buflen - 1, "OK %d %d", te->dumpId, AH->publicArc.n_errors);
    securec_check_ss_c(nRet, buf, "\0");
}

static int parseWorkerResponse(TocEntry* te, const char* msg)
{
    DumpId dumpId;
    int nBytes, n_errors;

    if (messageStartsWith(msg, "OK ")) {
        int ret = sscanf_s(msg, "OK %d %d%n", &dumpId, &n_errors, &nBytes);
        securec_check_for_sscanf_s(ret, 1, "\0", "\0");
        Assert(dumpId == te->dumpId);
        Assert(nBytes == strlen(msg));
    } else
        exit_horribly(NULL, "invalid message received from worker: \"%s\"", msg);

    return n_errors;
}
/*
 * Acquire lock on a table to be dumped by a worker process.
 *
 * The leader process is already holding an ACCESS SHARE lock.  Ordinarily
 * it's no problem for a worker to get one too, but if anything else besides
 * pg_dump is running, there's a possible deadlock:
 *
 * 1) Leader dumps the schema and locks all tables in ACCESS SHARE mode.
 * 2) Another process requests an ACCESS EXCLUSIVE lock (which is not granted
 *	  because the leader holds a conflicting ACCESS SHARE lock).
 * 3) A worker process also requests an ACCESS SHARE lock to read the table.
 *	  The worker is enqueued behind the ACCESS EXCLUSIVE lock request.
 * 4) Now we have a deadlock, since the leader is effectively waiting for
 *	  the worker.  The server cannot detect that, however.
 *
 * To prevent an infinite wait, prior to touching a table in a worker, request
 * a lock in ACCESS SHARE mode but with NOWAIT.  If we don't get the lock,
 * then we know that somebody else has requested an ACCESS EXCLUSIVE lock and
 * so we have a deadlock.  We must fail the backup in that case.
 */
static void lockTableForWorker(ArchiveHandle* AH, TocEntry* te)
{
    const char* qualId;
    PQExpBuffer query;
    PGresult* res;

    /* Nothing to do for BLOBS */
    if (strcmp(te->desc, "BLOBS") == 0)
        return;

    query = createPQExpBuffer();

    qualId = fmtQualifiedId(te->nmspace, te->tag);
    appendPQExpBuffer(query, "LOCK TABLE %s IN ACCESS SHARE MODE NOWAIT", qualId);

    res = PQexec(AH->connection, query->data);

    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
        exit_horribly(NULL, "could not obtain lock on relation \"%s\"\n"
              "This usually means that someone requested an ACCESS EXCLUSIVE lock "
              "on the table after the pg_dump parent process had gotten the "
              "initial ACCESS SHARE lock on the table.",
              qualId);

    PQclear(res);
    destroyPQExpBuffer(query);
}

/*
 * WaitForCommands: main routine for a worker process.
 *
 * Read and execute commands from the leader until we see EOF on the pipe.
 */
static void WaitForCommands(ArchiveHandle* AH, int pipefd[2])
{
    char* command;
    TocEntry* te;
    T_Action act;
    char buf[256];
    int count = 0;

    for (;;) {
        if (!(command = getMessageFromLeader(pipefd))) {
            /* EOF, so done */
            return;
        }

        /* Decode the command */
        parseWorkerCommand(AH, &te, &act, command);

        if (act == ACT_DUMP) {
            /* Acquire lock on this table within the worker's session */
            lockTableForWorker(AH, te);

            /* Perform the dump command */
            WriteDataChunksForTocEntry(AH, te);
        } else
            Assert(false);

        /* Return status to leader */
        buildWorkerResponse(AH, te, buf, sizeof(buf));
        sendMessageToPipe(pipefd[PIPE_WRITE], buf);

        /* command was pg_malloc'd and we are responsible for free()ing it. */
        free(command);
    }
}

/*
 * Dispatch a job to some free worker.
 *
 * te is the TocEntry to be processed, act is the action to be taken on it.
 * callback is the function to call on completion of the job.
 *
 * If no worker is currently available, this will block, and previously
 * registered callback functions may be called.
 */
void DispatchJobForTocEntry(ParallelStateN* pstate, TocEntry* te, T_Action act)
{
    int worker;
    char buf[256];

    /* Get a worker, waiting if none are idle */
    while ((worker = GetIdleWorker(pstate)) == NO_SLOT)
        WaitForWorkers(pstate, WFW_ONE_IDLE);

    /* Construct and send command string */
    buildWorkerCommand(te, act, buf, sizeof(buf));
    sendMessageToPipe(pstate->parallelSlot[worker].pipeWrite, buf);
    pstate->parallelSlot[worker].workerStatus = WRKR_WORKING;
    pstate->te[worker] = te;
}

/*
 * Find an idle worker and return its slot number.
 * Return NO_SLOT if none are idle.
 */
static int GetIdleWorker(ParallelStateN* pstate)
{
    for (int i = 0; i < pstate->workerNum; i++) {
        if (pstate->parallelSlot[i].workerStatus == WRKR_IDLE)
            return i;
    }
    return NO_SLOT;
}

/*
 * Send a command message to the specified fd.
 */
static void sendMessageToPipe(int fd, const char* str)
{
    int len = strlen(str) + 1;
    if (write(fd, str, len) != len) {
        exit_horribly(NULL, "could not write to the communication channel: %m\n");
    }
}

/*
 * Read one command message from the leader, blocking if necessary
 * until one is available, and return it as a malloc'd string.
 * On EOF, return NULL.
 *
 * This function is executed in worker processes.
 */
static char* getMessageFromLeader(int pipefd[2])
{
    return readMessageFromPipe(pipefd[PIPE_READ]);
}

/*
 * Read one message from the specified pipe (fd), blocking if necessary
 * until one is available, and return it as a malloc'd string.
 * On EOF, return NULL.
 *
 * A "message" on the channel is just a null-terminated string.
 */
static char* readMessageFromPipe(int fd)
{
    char* msg;
    int msgsize, bufsize;
    int ret;

    /*
     * In theory, if we let piperead() read multiple bytes, it might give us
     * back fragments of multiple messages.  (That can't actually occur, since
     * neither leader nor workers send more than one message without waiting
     * for a reply, but we don't wish to assume that here.)  For simplicity,
     * read a byte at a time until we get the terminating '\0'.  This method
     * is a bit inefficient, but since this is only used for relatively short
     * command and status strings, it shouldn't matter.
     */
    bufsize = 64; /* could be any number */
    msg = (char*)pg_malloc(bufsize);
    msgsize = 0;
    for (;;) {
        Assert(msgsize < bufsize);
        ret = read(fd, msg + msgsize, 1);
        if (ret <= 0)
            break; /* error or connection closure */

        Assert(ret == 1);

        if (msg[msgsize] == '\0')
            return msg; /* collected whole message */

        msgsize++;
        if (msgsize == bufsize) { /* enlarge buffer if needed */
            bufsize += 16;        /* could be any number */
            msg = (char*)pg_realloc(msg, bufsize);
        }
    }

    /* Other end has closed the connection */
    free(msg);
    return NULL;
}

/*
 * Wait until some descriptor in "workerset" becomes readable.
 * Returns -1 on error, else the number of readable descriptors.
 */
static int select_loop(int maxFd, fd_set* workerset)
{
    int i;
    fd_set saveSet = *workerset;
    for (;;) {
        *workerset = saveSet;
        i = select(maxFd + 1, workerset, NULL, NULL, NULL);
        if (i < 0 && errno == EINTR)
            continue;
        break;
    }

    return i;
}

/*
 * Check for messages from worker processes.
 *
 * If a message is available, return it as a malloc'd string, and put the
 * index of the sending worker in *worker.
 *
 * If nothing is available, wait if "do_wait" is true, else return NULL.
 *
 * If we detect EOF on any socket, we'll return NULL.  It's not great that
 * that's hard to distinguish from the no-data-available case, but for now
 * our one caller is okay with that.
 *
 * This function is executed in the leader process.
 */
static char* getMessageFromWorker(ParallelStateN* pstate, bool do_wait, int* worker)
{
    int i;
    fd_set workerset;
    int maxFd = -1;
    struct timeval nowait = {0, 0};

    /* construct bitmap of socket descriptors for select() */
    FD_ZERO(&workerset);
    for (i = 0; i < pstate->workerNum; i++) {
        if (!WORKER_IS_RUNNING(pstate->parallelSlot[i].workerStatus))
            continue;
        FD_SET(pstate->parallelSlot[i].pipeRead, &workerset);
        if (pstate->parallelSlot[i].pipeRead > maxFd)
            maxFd = pstate->parallelSlot[i].pipeRead;
    }

    if (do_wait) {
        i = select_loop(maxFd, &workerset);
        Assert(i != 0);
    } else {
        if ((i = select(maxFd + 1, &workerset, NULL, NULL, &nowait)) == 0)
            return NULL;
    }

    if (i < 0)
        exit_horribly(NULL, "%s() failed: %m", "select");

    for (i = 0; i < pstate->workerNum; i++) {
        char* msg;

        if (!WORKER_IS_RUNNING(pstate->parallelSlot[i].workerStatus))
            continue;
        if (!FD_ISSET(pstate->parallelSlot[i].pipeRead, &workerset))
            continue;

        /*
         * Read the message if any.  If the socket is ready because of EOF,
         * we'll return NULL instead (and the socket will stay ready, so the
         * condition will persist).
         *
         * Note: because this is a blocking read, we'll wait if only part of
         * the message is available.  Waiting a long time would be bad, but
         * since worker status messages are short and are always sent in one
         * operation, it shouldn't be a problem in practice.
         */
        msg = readMessageFromPipe(pstate->parallelSlot[i].pipeRead);
        *worker = i;
        return msg;
    }
    Assert(false);
    return NULL;
}

/*
 * Check for status messages from workers.
 *
 * If do_wait is true, wait to get a status message; otherwise, just return
 * immediately if there is none available.
 *
 * When we get a status message, we pass the status code to the callback
 * function that was specified to DispatchJobForTocEntry, then reset the
 * worker status to IDLE.
 *
 * Returns true if we collected a status message, else false.
 *
 * XXX is it worth checking for more than one status message per call?
 * It seems somewhat unlikely that multiple workers would finish at exactly
 * the same time.
 */
static bool ListenToWorkers(ParallelStateN* pstate, bool do_wait)
{
    int worker;
    char* msg;

    /* Try to collect a status message */
    msg = getMessageFromWorker(pstate, do_wait, &worker);
    if (!msg) {
        /* If do_wait is true, we must have detected EOF on some socket */
        if (do_wait)
            exit_horribly(NULL, "a worker process died unexpectedly");
        return false;
    }

    /* Process it and update our idea of the worker's status */
    if (messageStartsWith(msg, "OK ")) {
        ParallelSlotN* slot = &pstate->parallelSlot[worker];
        TocEntry* te = pstate->te[worker];
        int n_errors;

        n_errors = parseWorkerResponse(te, msg);
        slot->workerStatus = WRKR_IDLE;
        pstate->te[worker] = NULL;
    } else
        exit_horribly(NULL, "invalid message received from worker: \"%s\"", msg);

    /* Free the string returned from getMessageFromWorker */
    free(msg);

    return true;
}

/*
 * Return true iff no worker is running.
 */
static bool HasEveryWorkerTerminated(ParallelStateN* pstate)
{
    int i;

    for (i = 0; i < pstate->workerNum; i++) {
        if (WORKER_IS_RUNNING(pstate->parallelSlot[i].workerStatus))
            return false;
    }
    return true;
}

/*
 * Return true if every worker is in the WRKR_IDLE state.
 */
bool IsEveryWorkerIdle(ParallelStateN* pstate)
{
    int i;

    for (i = 0; i < pstate->workerNum; i++) {
        if (pstate->parallelSlot[i].workerStatus != WRKR_IDLE)
            return false;
    }
    return true;
}

/*
 * Check for status results from workers, waiting if necessary.
 *
 * Available wait modes are:
 * WFW_NO_WAIT: reap any available status, but don't block
 * WFW_GOT_STATUS: wait for at least one more worker to finish
 * WFW_ONE_IDLE: wait for at least one worker to be idle
 * WFW_ALL_IDLE: wait for all workers to be idle
 *
 * Any received results are passed to the callback specified to
 * DispatchJobForTocEntry.
 *
 * This function is executed in the leader process.
 */
void WaitForWorkers(ParallelStateN* pstate, WFW_WaitOption mode)
{
    bool do_wait = false;
    /*
     * In GOT_STATUS mode, always block waiting for a message, since we can't
     * return till we get something.  In other modes, we don't block the first
     * time through the loop.
     */
    if (mode == WFW_GOT_STATUS) {
        /* Assert that caller knows what it's doing */
        Assert(!IsEveryWorkerIdle(pstate));
        do_wait = true;
    }

    for (;;) {
        /*
         * Check for status messages, even if we don't need to block.  We do
         * not try very hard to reap all available messages, though, since
         * there's unlikely to be more than one.
         */
        if (ListenToWorkers(pstate, do_wait)) {
            /*
             * If we got a message, we are done by definition for GOT_STATUS
             * mode, and we can also be certain that there's at least one idle
             * worker.  So we're done in all but ALL_IDLE mode.
             */
            if (mode != WFW_ALL_IDLE)
                return;
        }

        /* Check whether we must wait for new status messages */
        switch (mode) {
            case WFW_NO_WAIT:
                return; /* never wait */
            case WFW_GOT_STATUS:
                Assert(false); /* can't get here, because we waited */
                break;
            case WFW_ONE_IDLE:
                if (GetIdleWorker(pstate) != NO_SLOT)
                    return;
                break;
            case WFW_ALL_IDLE:
                if (IsEveryWorkerIdle(pstate))
                    return;
                break;
        }

        /* Loop back, and this time wait for something to happen */
        do_wait = true;
    }
}