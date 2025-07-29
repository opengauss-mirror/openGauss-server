
/* ---------------------------------------------------------------------------------------
 *
 * imcstore_vacuum.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imcstore_vacuum.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "component/thread/mpmcqueue.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/smgr/smgr.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "access/tableam.h"
#include "ddes/dms/ss_transaction.h"
#include "access/htap/imcstore_insert.h"
#include "access/htap/imcucache_mgr.h"
#include "access/htap/ss_imcucache_mgr.h"
#include "access/htap/imcs_hash_table.h"
#include "access/htap/imcustorage.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/imcstore_delta.h"

#ifdef ENABLE_HTAP
#define TRY_ENQUEUE_TIMES (16)

constexpr int VACUUM_PUSH_WAIT_TIME = 100;
constexpr int VACUMM_WAIT_TIME = 60 * 1000;
constexpr int VACUUMQUEUE_SIZE = (1 << 12);

void ClearImcstoreCacheIfNeed(Oid droppingDBOid)
{
    if (!HAVE_HTAP_TABLES || ENABLE_DSS || g_instance.pid_cxt.IMCStoreVacuumPID == 0) {
        return;
    }
    bool needClear = false;
    pthread_rwlock_rdlock(&g_instance.imcstore_cxt.context_mutex);
    needClear = (droppingDBOid == g_instance.imcstore_cxt.dboid);
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
    if (needClear) {
        ereport(WARNING, (errmsg("Drop db with imcstore tables, all imcstore data will be cleared.")));
        gs_signal_send(g_instance.pid_cxt.IMCStoreVacuumPID, SIGUSR2);
    }
}

void IMCStoreVacuumQueueCleanup(int code, Datum arg)
{
    if (!arg) {
        return;
    }
    MpmcBoundedQueue<IMCStoreVacuumTarget>* queue = (MpmcBoundedQueue<IMCStoreVacuumTarget>*)arg;
    delete queue;
}

void InitIMCStoreVacuumQueue(knl_g_imcstore_context* context)
{
    context->vacuum_queue = new MpmcBoundedQueue<IMCStoreVacuumTarget>(VACUUMQUEUE_SIZE);
    on_proc_exit(&IMCStoreVacuumQueueCleanup, (Datum)context->vacuum_queue);
}

bool IMCStoreVacuumPushWork(Oid relid, uint32 cuId, TransactionId xid)
{
    pthread_rwlock_rdlock(&g_instance.imcstore_cxt.context_mutex);
    bool needClean = g_instance.imcstore_cxt.should_clean;
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
    if (needClean) {
        return false;
    }

    IMCStoreVacuumTarget target;
    target.isLocalType = true;
    target.relOid = relid;
    target.rowGroupId = cuId;
    target.xid = xid;
    for (int i = 0; i < TRY_ENQUEUE_TIMES; ++i) {
        if (g_instance.imcstore_cxt.vacuum_queue->Enqueue(target)) {
            break;
        }
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        pg_usleep(VACUUM_PUSH_WAIT_TIME);
    }
    SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
    return true;
}

void IMCStoreSyncVacuumPushWork(Oid relid, uint32 cuId, TransactionId xid, uint64 cuSize, CUDesc** CUDesc, CU** CUs)
{
    IMCStoreVacuumTarget target;
    target.isLocalType = false;
    target.relOid = relid;
    target.rowGroupId = cuId;
    target.CUDescs = CUDesc;
    target.CUs = CUs;
    target.newCuSize = cuSize;
    target.xid = xid;
    for (int i = 0; i < TRY_ENQUEUE_TIMES; ++i) {
        if (g_instance.imcstore_cxt.vacuum_queue->Enqueue(target)) {
            break;
        }
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        pg_usleep(VACUUM_PUSH_WAIT_TIME);
    }
    SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void IMCStoreVacuumSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.imcstore_vacuum_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);

    errno = save_errno;
}

static void IMCStoreVacuumSigUsr2Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.imcstore_vacuum_cxt.got_SIGUSR2 = true;
    pg_atomic_write_u32(&g_instance.imcstore_cxt.is_imcstore_cache_down, IMCSTORE_CACHE_DOWN);

    if (t_thrd.proc)
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);

    errno = save_errno;
}

static void CleanAllCacheCU()
{
    IMCUDataCacheMgr::ResetInstance();
    if (g_instance.imcstore_cxt.vacuum_queue) {
        delete g_instance.imcstore_cxt.vacuum_queue;
        g_instance.imcstore_cxt.vacuum_queue = new MpmcBoundedQueue<IMCStoreVacuumTarget>(VACUUMQUEUE_SIZE);
    }
    g_instance.imcstore_cxt.should_clean = false;
    g_instance.imcstore_cxt.dbname = nullptr;
    g_instance.imcstore_cxt.dboid = InvalidOid;
    pg_atomic_write_u32(&(g_instance.imcstore_cxt.dbname_reference_count), 0);
    pg_atomic_write_u32(&g_instance.imcstore_cxt.is_imcstore_cache_down, IMCSTORE_CACHE_UP);
}

/* SIGTERM: time to die */
static void IMCStoreVacuumSigtermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.imcstore_vacuum_cxt.got_SIGTERM = true;
    if (t_thrd.proc)
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);

    errno = save_errno;
}

void ItempointerGetTuple(Relation rel, ItemPointerData item, Datum *val, bool *null, IMCStoreInsert &imcstoreInsert,
                         TransactionId frozen)
{
    char tuplebuf[BLCKSZ];
    Buffer buf = InvalidBuffer;
    TupleDesc relTupleDesc = rel->rd_att;

    /*
     * here is an explain why we use SnapshotAny here, there is two situation:
     *   1. this line have new update in delta table, we will get data from heap store, 100% correct
     *   2. this line don't have any update in feature, SnapshotAny will give us latest data in disk (ustore)
     */
    if (RelationIsUstoreFormat(rel)) {
        UHeapTuple tuple = (UHeapTuple)(&tuplebuf);
        tuple->disk_tuple = (UHeapDiskTuple)(tuplebuf + UHeapTupleDataSize);
        tuple->tupTableType = UHEAP_TUPLE;
        if (!UHeapFetch(rel, SnapshotAny, &item, tuple, &buf, false, false)) {
            return;
        }
        TransactionId xid;
        UHTSVResult result = UHeapTupleSatisfiesOldestXmin(tuple, frozen, buf, true, NULL, &xid, NULL, rel);
        if (result != UHEAPTUPLE_LIVE && result != UHEAPTUPLE_RECENTLY_DEAD) {
            ReleaseBuffer(buf);
            return;
        }
        tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
        imcstoreInsert.AppendOneTuple(val, null);
        ReleaseBuffer(buf);
    } else {
        HeapTuple tuple = (HeapTuple)(&tuplebuf);
        tuple->tupTableType = HEAP_TUPLE;
        tuple->t_self = item;
        tuple->t_data = (HeapTupleHeader)(tuplebuf + HEAPTUPLESIZE);
        if (!heap_fetch(rel, SnapshotAny, tuple, &buf, false, NULL)) {
            return;
        }
        HTSV_Result result = HeapTupleSatisfiesVacuum(tuple, frozen, buf);
        if (result != HEAPTUPLE_LIVE && result != HEAPTUPLE_RECENTLY_DEAD) {
            ReleaseBuffer(buf);
            return;
        }
        tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
        imcstoreInsert.AppendOneTuple(val, null);
        ReleaseBuffer(buf);
    }
}

void IMCStoreVacuumLocalNode(Relation rel, IMCSDesc *imcsDesc, uint32 cuid, TransactionId frozen)
{
    TupleDesc relTupleDesc = rel->rd_att;
    Datum *val = (Datum *)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool *null = (bool *)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    /* form TupleDesc for imcs */
    TupleDesc imcsTupleDesc = FormImcsTupleDesc(relTupleDesc, imcsDesc->imcsAttsNum, imcsDesc->imcsNatts);

    /* init imcstoreInsert */
    IMCStoreInsert imcstoreInsert(rel, imcsTupleDesc, imcsDesc->imcsAttsNum);

    BlockNumber begin = cuid * MAX_IMCS_PAGES_ONE_CU;
    BlockNumber end = Min((cuid + 1) * MAX_IMCS_PAGES_ONE_CU, RelationGetNumberOfBlocks(rel));
    for (BlockNumber curr = begin; curr < end; ++curr) {
        CHECK_FOR_INTERRUPTS();
        Buffer buffer = ReadBuffer(rel, curr);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buffer);
        OffsetNumber lines;
        if (RelationIsUstoreFormat(rel)) {
            lines = UHeapPageGetMaxOffsetNumber(page);
        } else {
            lines = PageGetMaxOffsetNumber(page);
        }
        for (OffsetNumber lineoff = FirstOffsetNumber; lineoff <= lines; ++lineoff) {
            ImcstoreCtid imcsCtid;
            ItemPointerSetBlockNumber(&imcsCtid.ctid, curr);
            ItemPointerSetOffsetNumber(&imcsCtid.ctid, lineoff);
            imcsCtid.reservedSpace = 0;
            errno_t rc = memcpy_s(&val[relTupleDesc->natts], sizeof(Datum), &imcsCtid, sizeof(ImcstoreCtid));
            securec_check_c(rc, "\0", "\0");
            null[relTupleDesc->natts] = false;
            ItempointerGetTuple(rel, imcsCtid.ctid, val, null, imcstoreInsert, frozen);
        }
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
    }

    imcstoreInsert.BatchReInsertCommon(imcsDesc, cuid, frozen);
    imcstoreInsert.ResetBatchRows(true);

    pfree(val);
    pfree(null);
    imcstoreInsert.Destroy();
}

void IMCStoreVacuum(Relation rel, IMCSDesc *imcsDesc, uint32 cuid, TransactionId xid)
{
    TransactionId frozen = TransactionIdIsValid(xid) ? xid : GetOldestXmin(rel);
    if (SS_PRIMARY_MODE && !imcsDesc->populateInShareMem && !SS_IMCU_CACHE->CheckRGOwnedByCurNode(cuid)) {
        elog(DEBUG1, "SS Send vacuum request to standy: cuid(%u), rel(%s), xid(%u).",
            cuid, RelationGetRelationName(rel), xid);
        SSBroadcastIMCStoreVacuumLocalMemory(RelationGetRelid(rel), cuid, frozen);
        return;
    }

    elog(DEBUG1, "vacuum on local node: cuid(%u), rel(%s), xid(%u).", cuid, RelationGetRelationName(rel), xid);
    IMCStoreVacuumLocalNode(rel, imcsDesc, cuid, frozen);
}

void IMCStoreVacuumWorkerMain(void)
{
    int retry = 0;
    constexpr int MAX_RETRY_TIMES = 10;
    char username[NAMEDATALEN] = {'\0'};
    sigjmp_buf local_sigjmp_buf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = IMCSTORE_VACUUM;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "IMCStoreVacuumWorker";
    u_sess->attr.attr_common.application_name = pstrdup("IMCStoreVacuumWorker");

    /* Identify myself via ps */
    init_ps_display("imcstore vacuum worker process", "", "", "");
    ereport(LOG, (errmsg("imcstore vacuum worker started")));

    SetProcessingMode(InitProcessing);

    InitLatch(&g_instance.imcstore_cxt.vacuum_latch);

    /*
     * Set up signal handlers. We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, IMCStoreVacuumSigHupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, IMCStoreVacuumSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, IMCStoreVacuumSigUsr2Handler);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);
    gspqsignal(SIGURG, print_stack);
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
    MemoryContext thread_context = AllocSetContextCreate(t_thrd.top_mem_cxt, "imcstore vacuum thread",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    (void)MemoryContextSwitchTo(thread_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);
        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        (void)MemoryContextSwitchTo(thread_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(thread_context);

        RESUME_INTERRUPTS();

        /*
         * We can now go away. Note that because we called InitProcess, a
         * callback was registered to do ProcKill, which will clean up
         * necessary state.
         */
        proc_exit(0);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    on_shmem_exit(PGXCNodeCleanAndRelease, 0);

    pgstat_report_appname("IMCStore Vacuum");
    pgstat_report_activity(STATE_RUNNING, NULL);

    if (CHECK_IMCSTORE_CACHE_DOWN) {
        CleanAllCacheCU();
    }

    // get database name
    while (!t_thrd.imcstore_vacuum_cxt.got_SIGTERM && !t_thrd.imcstore_vacuum_cxt.got_SIGUSR2 &&
           (pmState == PM_RUN || pmState == PM_HOT_STANDBY)) {
        /* Clear any already-pending wakeups */
        ResetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        bool dbnameInited;
        pthread_rwlock_rdlock(&g_instance.imcstore_cxt.context_mutex);
        dbnameInited = g_instance.imcstore_cxt.dbname != NULL;
        if (dbnameInited) {
            ereport(LOG, (errmsg("IMCStoreVacuum: Init DB name, current DB name: %s.",
                g_instance.imcstore_cxt.dbname)));
            t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(g_instance.imcstore_cxt.dbname, InvalidOid, username);
            PG_TRY();
            {
                t_thrd.proc_cxt.PostInit->InitHTAPImcsVacuum();
            }
            PG_CATCH();
            {
                pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
                PG_RE_THROW();
            }
            PG_END_TRY();
            pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
            break;
        }
        pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
        // wait weakup, or check per second
        int rc = WaitLatch(&g_instance.imcstore_cxt.vacuum_latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)VACUMM_WAIT_TIME);
        if (rc & WL_POSTMASTER_DEATH) {
            proc_exit(0);
        }
    }

    SetProcessingMode(NormalProcessing);

    IMCStoreVacuumTarget target;
    while (!t_thrd.imcstore_vacuum_cxt.got_SIGTERM && !t_thrd.imcstore_vacuum_cxt.got_SIGUSR2 &&
           (pmState == PM_RUN || pmState == PM_HOT_STANDBY)) {
        int rc = 0;
        /* Clear any already-pending wakeups */
        ResetLatch(&g_instance.imcstore_cxt.vacuum_latch);

        if (g_instance.imcstore_cxt.should_clean) {
            pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
            ereport(LOG, (errmsg("IMCStoreVacuum: reset vacuum thread.")));
            if (g_instance.imcstore_cxt.vacuum_queue) {
                delete g_instance.imcstore_cxt.vacuum_queue;
                g_instance.imcstore_cxt.vacuum_queue = new MpmcBoundedQueue<IMCStoreVacuumTarget>(VACUUMQUEUE_SIZE);
            }
            g_instance.imcstore_cxt.should_clean = false;
            pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
            break;
        }

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.imcstore_vacuum_cxt.got_SIGHUP) {
            t_thrd.imcstore_vacuum_cxt.got_SIGHUP = false;
            MemoryContext backcontext = MemoryContextSwitchTo(thread_context);
            ProcessConfigFile(PGC_SIGHUP);
            MemoryContextSwitchTo(backcontext);
        }
        CHECK_FOR_INTERRUPTS();

        if (!g_instance.imcstore_cxt.vacuum_queue->Dequeue(target)) {
            ++retry;
            if (retry < MAX_RETRY_TIMES) {
                continue;
            }

            retry = 0;
            rc = WaitLatch(&g_instance.imcstore_cxt.vacuum_latch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                (long)VACUMM_WAIT_TIME);
            if (rc & WL_POSTMASTER_DEATH) {
                break;
            }
            continue;
        }
        retry = 0;

        start_xact_command();
        PushActiveSnapshot(GetTransactionSnapshot(false));
        Relation rel = heap_open(target.relOid, NoLock);
        if (!RelationIsValid(rel)) {
            PopActiveSnapshot();
            finish_xact_command();
            ereport(WARNING, (errmsg("imcstore vacuum: open relation failed, :%d", target.relOid)));
            continue;
        }

        IMCSDesc *imcsDesc = (IMCSDesc *)IMCS_HASH_TABLE->GetImcsDesc(target.relOid);
        if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) {
            continue;
        }

        MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
        PG_TRY();
        {
            if (g_instance.attr.attr_memory.enable_borrow_memory && imcsDesc->borrowMemPool != NULL) {
                u_sess->imcstore_ctx.pinnedBorrowMemPool = imcsDesc->borrowMemPool;
            }
            if (target.isLocalType) {
                IMCStoreVacuum(rel, imcsDesc, target.rowGroupId, target.xid);
            } else {
                RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(target.rowGroupId);
                rowgroup->VacuumFromRemote(rel, imcsDesc, target.CUDescs, target.CUs, target.xid, target.newCuSize);
                imcsDesc->UnReferenceRowGroup();
            }
        }
        PG_CATCH();
        {
            heap_close(rel, NoLock);
            PopActiveSnapshot();
            finish_xact_command();
            MemoryContextSwitchTo(oldcontext);
            PG_RE_THROW();
        }
        PG_END_TRY();

        heap_close(rel, NoLock);
        PopActiveSnapshot();
        finish_xact_command();
        MemoryContextSwitchTo(oldcontext);
    }

    if (t_thrd.imcstore_vacuum_cxt.got_SIGUSR2) {
        CleanAllCacheCU();
    }
    MemoryContextDelete(thread_context);

    elog(LOG, "imcstore vacuum thread is shutting down.");
}
#endif
