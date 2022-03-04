/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * src/gausskernel/process/postmaster/snapcapturer.cpp
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
#include "catalog/pg_snapshot.h"
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
#include "storage/tcap.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

#include "postmaster/snapcapturer.h"

/* seconds, interval of alarm check loop. */
static const int SNAP_CAPTURE_INTERVAL = 3;

/*
 * TxnSnapCapShmemSize
 *		Size of TxnSnapCapturer related shared memory
 */
Size TxnSnapCapShmemSize()
{
    return sizeof(TxnSnapCapShmemStruct);
}

/*
 * TxnSnapCapShmemInit
 *		Allocate and initialize TxnSnapCapturer -related shared memory
 */
void TxnSnapCapShmemInit(void)
{
    bool found = false;

    t_thrd.snapcapturer_cxt.snapCapShmem =
        (TxnSnapCapShmemStruct *)ShmemInitStruct("TxnSnapCapturer Data", sizeof(TxnSnapCapShmemStruct), &found);

    if (!found) {
        /*
         * First time through, so initialize.  Note that we zero the whole
         * requests array; this is so that CompactCheckpointerRequestQueue
         * can assume that any pad bytes in the request structs are zeroes.
         */
        errno_t ret = memset_s(t_thrd.snapcapturer_cxt.snapCapShmem, sizeof(TxnSnapCapShmemStruct),
            0, sizeof(TxnSnapCapShmemStruct));
        securec_check(ret, "\0", "\0");
    }

    return;
}

/*
 * signal handle functions
 */
 
/* SIGHUP: set flag to re-read config file at next convenient time */
static void TxnSnapSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.snapcapturer_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

ThreadId StartTxnSnapCapturer(void)
{
    if (!IsPostmasterEnvironment) {
        return 0;
    }

    if (canAcceptConnections(false) == CAC_OK) {
        return initialize_util_thread(TXNSNAP_CAPTURER);
    }

    ereport(LOG,
        (errmsg("not ready to start snapshot capturer.")));
    return 0;
}

bool IsTxnSnapCapturerProcess(void)
{
    return t_thrd.role == TXNSNAP_CAPTURER;
}

bool IsTxnSnapWorkerProcess(void)
{
    return t_thrd.role == TXNSNAP_WORKER;
}

static GTM_Timeline TxnSnapParseTimelineFromText(const char* prefix, char** s)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    GTM_Timeline val;

    if (strncmp(ptr, prefix, prefixlen) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    ptr += prefixlen;
    if (sscanf_s(ptr, "%u", &val) != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    ptr = strchr(ptr, '\n');
    if (ptr == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    *s = ptr + 1;
    return val;
}

static TransactionId TxnSnapParseXidFromText(const char* prefix, char** s)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    TransactionId val;

    if (strncmp(ptr, prefix, prefixlen) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    ptr += prefixlen;
    if (sscanf_s(ptr, XID_FMT, &val) != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    ptr = strchr(ptr, '\n');
    if (ptr == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    }

    *s = ptr + 1;
    return val;
}

static int TxnSnapParseIntFromText(const char* prefix, char** s)
{
    char* ptr = *s;
    int prefixlen = strlen(prefix);
    int val;

    if (strncmp(ptr, prefix, prefixlen) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    ptr += prefixlen;
    if (sscanf_s(ptr, "%d", &val) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    ptr = strchr(ptr, '\n');
    if (ptr == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), 
                errmsg("invalid snapshot data in gs_txn_snapshot")));
    *s = ptr + 1;
    return val;
}

static void TxnSnapSerialize(Snapshot snap, StringInfo buf)
{
    appendStringInfo(buf, "xmin:" XID_FMT "\n", snap->xmin);
    appendStringInfo(buf, "xmax:" XID_FMT "\n", snap->xmax);

    appendStringInfo(buf, "snapshotcsn:" XID_FMT "\n", snap->snapshotcsn);
    appendStringInfo(buf, "timeline:%u\n", snap->timeline);
    appendStringInfo(buf, "rec:%u\n", snap->takenDuringRecovery);
    
    buf->data[buf->len] = '\0';
}

void TxnSnapDeserialize(char *buf, Snapshot snap)
{
    char *tmpBuf = buf;
    snap->xmin = TxnSnapParseXidFromText("xmin:", &tmpBuf);
    snap->xmax = TxnSnapParseXidFromText("xmax:", &tmpBuf);

    snap->snapshotcsn = TxnSnapParseXidFromText("snapshotcsn:", &tmpBuf);
    snap->timeline = TxnSnapParseTimelineFromText("timeline:", &tmpBuf);
    snap->takenDuringRecovery = TxnSnapParseIntFromText("rec:", &tmpBuf);
}

/*
 * Get the current snapshot and record to pg_snapshot.
 */
static void TxnSnapInsert(void)
{
    Relation rel;
    HeapTuple tup;
    bool nulls[Natts_pg_snapshot] = { false };
    Datum values[Natts_pg_snapshot] = { 0 };
    Snapshot snap = (Snapshot)palloc0(sizeof(SnapshotData));
    StringInfoData buf;

    (void)GetSnapshotData(snap, false);

    initStringInfo(&buf);
    TxnSnapSerialize(snap, &buf);

    values[Anum_pg_snapshot_snptime - 1] = TimestampTzGetDatum(GetCurrentTimestamp());
    values[Anum_pg_snapshot_snpxmin - 1] = Int64GetDatum(snap->xmin);
    values[Anum_pg_snapshot_snpcsn - 1] = Int64GetDatum(snap->snapshotcsn);
    values[Anum_pg_snapshot_snpsnapshot - 1] = CStringGetTextDatum(buf.data);

    rel = heap_open(SnapshotRelationId, RowExclusiveLock);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    (void)simple_heap_insert(rel, tup);
    CatalogUpdateIndexes(rel, tup);

    heap_close(rel, RowExclusiveLock);
    FreeSnapshotDeepForce(snap);
    pfree(buf.data);
    heap_freetuple_ext(tup);

    return;
}

/*
 * Delete the out-date snapshots, that is,
 *     DELETE FROM pg_snapshot WHERE snptime <= now() - '3 days';
 */
static void TxnSnapDelete(void)
{
#define TXNSNAP_EXTRA_RETRNTION_TIME 900
    Relation rel;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple tup;

    /* Retent snapshots for up to undo_retention_time + 15min. */
    const int64 snapRetentionMs = 1000L * (u_sess->attr.attr_storage.undo_retention_time +
        TXNSNAP_EXTRA_RETRNTION_TIME);
    TimestampTz ft = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), snapRetentionMs * -1);

    rel = heap_open(SnapshotRelationId, RowExclusiveLock);
    ScanKeyInit(&skey[0], Anum_pg_snapshot_snptime, BTLessEqualStrategyNumber, 
        F_TIMESTAMP_LE, TimestampTzGetDatum(ft));

    sd = systable_beginscan(rel, SnapshotTimeCsnIndexId, true, NULL, 1, skey);
    while ((tup = systable_getnext(sd)) != NULL) {
        simple_heap_delete(rel, &tup->t_self);
    }

    systable_endscan(sd);
    heap_close(rel, RowExclusiveLock);
}

/*
 * TxnSnapWorkerImpl
 *
 * maintains the sys table pg_snapshot.
 */
static void TxnSnapWorkerImpl(void)
{
    int retentionTime = u_sess->attr.attr_storage.undo_retention_time;
    TimestampTz result;

    StartTransactionCommand();

    TxnSnapInsert();

    TxnSnapDelete();

#ifdef HAVE_INT64_TIMESTAMP
    result = GetCurrentTimestamp() - retentionTime * (INT64CONST(1000000));
#else
    result = GetCurrentTimestamp() - retentionTime;
#endif
    g_instance.flashback_cxt.oldestXminInFlashback = TvFetchSnpxminRecycle(result);

    CommitTransactionCommand();
}

static void TxnSnapWorkerQuitAndClean(int code, Datum arg)
{
    TxnSnapWorkerInfo *workInfo = &t_thrd.snapcapturer_cxt.snapCapShmem->workerInfo;
    pg_atomic_write_u64(&workInfo->snapworkerPid, 0);
    *(volatile TxnWorkerStatus *)&workInfo->status = TXNWORKER_DONE;
}

NON_EXEC_STATIC void TxnSnapWorkerMain()
{
    sigjmp_buf localSigjmpBuf;
    TxnSnapWorkerInfo *workInfo = &t_thrd.snapcapturer_cxt.snapCapShmem->workerInfo;
    pg_atomic_write_u64(&workInfo->snapworkerPid, t_thrd.proc_cxt.MyProcPid);

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = TXNSNAP_WORKER;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "TxnSnapWorker";

    /* Identify myself via ps */
    init_ps_display("txnsnapworker process", "", "", "");

    elog(DEBUG1, "txnsnapworker started(%s)", NameStr(workInfo->dbName));
    if (u_sess->attr.attr_security.PostAuthDelay)
        pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);

    SetProcessingMode(InitProcessing);

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we deliberately ignore SIGTERM, because during a standard Unix
     * system shutdown cycle, init will SIGTERM all processes at once.	We
     * want to wait for the backends to exit, whereupon the postmaster will
     * tell us it's okay to shut down (via SIGUSR2).
     */
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, TxnSnapSighupHandler);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    on_proc_exit(TxnSnapWorkerQuitAndClean, 0);

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(NameStr(workInfo->dbName)), InvalidOid, NULL); 
    t_thrd.proc_cxt.PostInit->InitTxnSnapWorker();

    SetProcessingMode(NormalProcessing);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    MemoryContext workMxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt,
        "TxnSnapWorker",
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(workMxt);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
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

        /* Notify txnsnapworker done in time */
        SetLatch(&t_thrd.snapcapturer_cxt.snapCapShmem->workerInfo.latch);

        /*
         * Abort the current transaction in order to recover.
         */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /* Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
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

        /* just go away */
        goto shutdown;
    }

    *(volatile TxnWorkerStatus *)&workInfo->status = TXNWORKER_STARTED;

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Set lockwait_timeout/update_lockwait_timeout to 30s avoid unexpected suspend. */
    SetConfigOption("lockwait_timeout", "30s", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("update_lockwait_timeout", "30s", PGC_SUSET, PGC_S_OVERRIDE);

    /* Do the hard work */
    if (TcapFeatureAvail()) {
        TxnSnapWorkerImpl();
    }

    MemoryContextResetAndDeleteChildren(workMxt);

shutdown:
    elog(DEBUG1, "txnsnapworker shutting down(%s)", NameStr(workInfo->dbName));
    proc_exit(0);
}

static inline TxnSnapWorkerInfo *TxnSnapWorkerInfoInit(char *dbName)
{
    TxnSnapWorkerInfo *workerInfo = &t_thrd.snapcapturer_cxt.snapCapShmem->workerInfo;
    errno_t rc;

    workerInfo->status = TXNWORKER_DEFAULT;

    rc = strcpy_s(NameStr(workerInfo->dbName), NAMEDATALEN, dbName);
    securec_check_c(rc, "\0", "\0");

    InitLatch(&workerInfo->latch);

    return workerInfo;
}

static void TxnSnapCapProcInterrupts(int rc)
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
    if (t_thrd.snapcapturer_cxt.got_SIGTERM) {
        elog(LOG, "txnsnapcapturer is shutting down.");
        proc_exit(0);
    }

    /*
     * reload the postgresql.conf
     */
    if (t_thrd.snapcapturer_cxt.got_SIGHUP) {
        t_thrd.snapcapturer_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }
}

/*
 * TxnGetDatabaseList
 *		Return a list of all databases found in pg_database.
 */
static List* TxnGetDatabaseList(void)
{
    List* dblist = NIL;
    Relation rel;
    SysScanDesc sd;
    HeapTuple tup;

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    sd = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_database pgdatabase = (Form_pg_database)GETSTRUCT(tup);
        if (strcmp(NameStr(pgdatabase->datname), "template0") == 0 || 
            strcmp(NameStr(pgdatabase->datname), "template1") == 0) {
            continue;
        }
        dblist = lappend(dblist, pstrdup(NameStr(pgdatabase->datname)));
    }

    systable_endscan(sd);
    heap_close(rel, AccessShareLock);

    return dblist;
}

static bool TxnSnapWorkerIsAlive()
{
    TxnSnapWorkerInfo *workInfo = &t_thrd.snapcapturer_cxt.snapCapShmem->workerInfo;
    int ntries;
    const int maxtries = 500;

    /* Wait 5s until txnsnapworker stopped. */
    for (ntries = 0; ; ntries++) {
        if (pg_atomic_read_u64(&workInfo->snapworkerPid) == 0) {
            return false;
        }
        if (ntries >= maxtries) {
            return true;
        }
        /* wait 0.01 sec, then retry */
        pg_usleep(10000L);
    }

    return true;
}

static void TxnSnapCapImpl(void)
{
    List *dblist;
    ListCell* cell = NULL;
    const long int snapcapIntervalMs = 10L;
    const long int snapcapStartThreshMs = 30000L;

    StartTransactionCommand();
    dblist = TxnGetDatabaseList();

    foreach (cell, dblist) {
        TimestampTz launchTime;
        TxnSnapWorkerInfo *workInfo;

        if (TxnSnapWorkerIsAlive()) {
            elog(ERROR, "start txnsnapworker failed: txnsnapworker already exists!");
        }

        workInfo = TxnSnapWorkerInfoInit((char *)lfirst(cell));

        SendPostmasterSignal(PMSIGNAL_START_TXNSNAPWORKER);

        launchTime = GetCurrentTimestamp();
        while (true) {
            /* Clear any already-pending wakeups */
            ResetLatch(&workInfo->latch);

            /* Wait latch for a maximum of 10ms for a snapworker done. */
            int rc = WaitLatch(&workInfo->latch,
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 
                snapcapIntervalMs);

            TxnSnapCapProcInterrupts(rc);

            /*
             * We raise an ERROR if txnsnapworker not started as soon during to
             * unexcepted error or hang in initialization. 
             */
            if (*(volatile TxnWorkerStatus *)&workInfo->status < TXNWORKER_STARTED && 
                TimestampDifferenceExceeds(launchTime, GetCurrentTimestamp(), snapcapStartThreshMs)) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
                    errmsg("txnsnapworker took too long to start over 30 seconds.")));
                break;
            }

            if (*(volatile TxnWorkerStatus *)&workInfo->status == TXNWORKER_DONE) {
                break;
            }
        }
    }

    list_free_deep(dblist);
    CommitTransactionCommand();
}

NON_EXEC_STATIC void TxnSnapCapturerMain()
{
    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = TXNSNAP_CAPTURER;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "TxnSnapCapturer";

    /* Identify myself via ps */
    init_ps_display("txnsnapcapturer process", "", "", "");

    ereport(LOG, (errmsg("txnsnapcapturer started")));

    if (u_sess->attr.attr_security.PostAuthDelay)
        pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);

    SetProcessingMode(InitProcessing);

    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, TxnSnapSighupHandler);
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

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, NULL); 
    t_thrd.proc_cxt.PostInit->InitTxnSnapCapturer();

    SetProcessingMode(NormalProcessing);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.
     */
    MemoryContext workMxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt,
        "TxnSnapCapturer",
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

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

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
        if (t_thrd.snapcapturer_cxt.got_SIGTERM) {
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
    while (!t_thrd.snapcapturer_cxt.got_SIGTERM && ENABLE_TCAP_VERSION) {
        int rc;

        CHECK_FOR_INTERRUPTS();

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /*
         * Wait until naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         */
        rc = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            SNAP_CAPTURE_INTERVAL * 1000L);

        TxnSnapCapProcInterrupts(rc);

        /* Do the hard work. */
        if (TcapFeatureAvail()) {
            TxnSnapCapImpl();
        }

        MemoryContextResetAndDeleteChildren(workMxt);
    }

shutdown:
    elog(LOG, "txnsnapcapturer shutting down");
    proc_exit(0);
}

