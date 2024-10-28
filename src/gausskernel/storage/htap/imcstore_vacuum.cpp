
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
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "catalog/pg_database.h"
#include "gssignal/gs_signal.h"
#include "access/tableam.h"
#include "access/htap/imcstore_insert.h"
#include "access/htap/imcucache_mgr.h"
#include "access/htap/imcustorage.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/imcstore_delta.h"

#ifdef ENABLE_HTAP
#define TRY_ENQUEUE_TIMES 3

constexpr int VACUMM_WAIT_TIME = 1000;
constexpr int VACUUMQUEUE_SIZE = (1 << 10);

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

void IMCStoreVacuumPushWork(Oid relid, uint32 cuId)
{
    IMCStoreVacuumTarget target = {
        .relOid = relid,
        .rowGroupId = cuId,
    };
    for (int i = 0; i < TRY_ENQUEUE_TIMES; ++i) {
        if (g_instance.imcstore_cxt.vacuum_queue->Enqueue(target)) {
            break;
        }
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        pg_usleep(VACUMM_WAIT_TIME);
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

/* SIGTERM: time to die */
static void IMCStoreVacuumSigtermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.imcstore_vacuum_cxt.got_SIGTERM = true;
    if (t_thrd.proc)
        SetLatch(&g_instance.imcstore_cxt.vacuum_latch);

    errno = save_errno;
}

bool CheckHeapFrozen(HeapTuple tuple, TransactionId frozen)
{
    TransactionId xmin = HeapTupleGetRawXmin(tuple);
    TransactionId xmax = HeapTupleGetRawXmax(tuple);

    /* any frozen tuple should be scaned */
    if (HeapTupleHeaderXminFrozen(tuple->t_data)) {
        return true;
    }

    /*
     * if tuple not delete yet, should be scaned
     * we accept not commited tuple
     * ecause in this case we will have a record inside delta, and will check visiablity while scan
     */
    if (TransactionIdPrecedes(xmin, frozen) && (xmax == InvalidTransactionId || TransactionIdPrecedes(frozen, xmax))) {
        return true;
    }
    return false;
}

bool CheckUHeapFrozen(UHeapTuple tuple, TransactionId frozen)
{
    int tdSlot = UHeapTupleHeaderGetTDSlot(tuple->disk_tuple);
    if (tdSlot == UHEAPTUP_SLOT_FROZEN) {
        return true;
    }

    if (TransactionIdPrecedes(tuple->xmin, frozen) &&
        (tuple->xmax == InvalidTransactionId || TransactionIdPrecedes(frozen, tuple->xmax))) {
        return true;
    }
    return false;
}

void ItempointerGetTuple(Relation rel, ItemPointerData item, Datum *val, bool *null, IMCStoreInsert &imcstoreInsert)
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
        if (UHeapFetch(rel, SnapshotAny, &item, tuple, &buf, false, false)) {
            tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
            imcstoreInsert.AppendOneTuple(val, null);
            ReleaseBuffer(buf);
        }
    } else {
        HeapTuple tuple = (HeapTuple)(&tuplebuf);
        tuple->tupTableType = HEAP_TUPLE;
        tuple->t_self = item;
        tuple->t_data = (HeapTupleHeader)(tuplebuf + HEAPTUPLESIZE);
        if (heap_fetch(rel, SnapshotAny, tuple, &buf, false, NULL)) {
            tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
            imcstoreInsert.AppendOneTuple(val, null);
            ReleaseBuffer(buf);
        }
    }
}

void BuildCtidScanBitmap(
    unsigned char* deltaMask, RowGroup* rowgroup, int ctidCol, RelFileNodeOld* rdNode, uint32 cuid, uint32 &maskMax)
{
    CUDesc* cuDescPtr = rowgroup->m_cuDescs[ctidCol];
    if (!cuDescPtr) return;

    bool hasFound = false;
    DataSlotTag slotTag = IMCU_CACHE->InitCUSlotTag(rdNode, ctidCol, cuDescPtr->cu_id, cuDescPtr->cu_pointer);
    CacheSlotId_t slotId = IMCU_CACHE->ReserveDataBlock(&slotTag, cuDescPtr->cu_size, hasFound);
    if (!hasFound) return;
    CU* ctidCU = IMCU_CACHE->GetCUBuf(slotId);

    /* build current ctid map */
    for (int i = 0; i < cuDescPtr->row_count; ++i) {
        ScalarValue val  = ctidCU->GetValue<sizeof(ImcstoreCtid), false>(i);
        ItemPointer curr = &(((ImcstoreCtid*)(&val))->ctid);
        BlockNumber blockoffset = ItemPointerGetBlockNumber(curr) - cuid * MAX_IMCS_PAGES_ONE_CU;
        OffsetNumber offset = ItemPointerGetOffsetNumber(curr);
        uint64 idx = blockoffset * MAX_POSSIBLE_ROW_PER_PAGE + offset;
        deltaMask[idx >> 3] |= (1 << (idx % 8));
        maskMax = Max(idx, maskMax);
    }
    IMCU_CACHE->UnPinDataBlock(slotId);
}

void UpdateBitmapByDelta(
    unsigned char* deltaMask, RowGroup* rowgroup, TransactionId frozen, uint32 cuid, uint32 &maskMax)
{
    DeltaTableIterator deltaIter = rowgroup->m_delta->ScanInit();
    ItemPointer item = NULL;
    DeltaOperationType ctidtype;
    TransactionId xid;
    int total = rowgroup->m_delta->rowNumber;
    ItemPointerData *itemList = (ItemPointerData*)palloc(sizeof(ItemPointerData) * total);
    int deleteEnd = 0;
    int insertBegin = total;

    while ((item = deltaIter.GetNext(&ctidtype, &xid)) != NULL) {
        if (TransactionIdFollows(xid, frozen)) {
            continue;
        }

        if (ctidtype == DeltaOperationType::IMCSTORE_DELETE) {
            itemList[deleteEnd] = *item;
            ++deleteEnd;
        } else {
            --insertBegin;
            itemList[insertBegin] = *item;
        }
    }

    for (int i = 0; i < deleteEnd; ++i) {
        ItemPointer curr = &itemList[i];
        BlockNumber blockoffset = ItemPointerGetBlockNumber(curr) - cuid * MAX_IMCS_PAGES_ONE_CU;
        OffsetNumber offset = ItemPointerGetOffsetNumber(curr);
        uint64 idx = blockoffset * MAX_POSSIBLE_ROW_PER_PAGE + offset;
        if ((idx >> 3) > MAX_IMCSTORE_DEL_BITMAP_SIZE) {
            break;
        }
        deltaMask[idx >> 3] &= ~(1 << (idx % 8));
        maskMax = Max(idx, maskMax);
    }

    for (int i = insertBegin; i < total; ++i) {
        ItemPointer curr = &itemList[i];
        BlockNumber blockoffset = ItemPointerGetBlockNumber(curr) - cuid * MAX_IMCS_PAGES_ONE_CU;
        OffsetNumber offset = ItemPointerGetOffsetNumber(curr);
        uint64 idx = blockoffset * MAX_POSSIBLE_ROW_PER_PAGE + offset;
        if ((idx >> 3) > MAX_IMCSTORE_DEL_BITMAP_SIZE) {
            break;
        }
        deltaMask[idx >> 3] |= (1 << (idx % 8));
        maskMax = Max(idx, maskMax);
    }
    pfree(itemList);
}

void IMCStoreVacuum(Relation rel, IMCSDesc *imcsDesc, uint32 cuid)
{
    TupleDesc relTupleDesc = rel->rd_att;
    unsigned char deltaMask[MAX_IMCSTORE_DEL_BITMAP_SIZE] = {0};
    uint64 frozen = pg_atomic_read_u64(&g_instance.undo_cxt.globalFrozenXid);

    RowGroup* rowgroup = imcsDesc->GetNewRGForCUInsert(cuid);
    pthread_rwlock_rdlock(&rowgroup->m_mutex);

    uint32 maskMax = 0;
    BuildCtidScanBitmap(deltaMask, rowgroup, imcsDesc->imcsNatts, (RelFileNodeOld*)&rel->rd_node, cuid, maskMax);
    UpdateBitmapByDelta(deltaMask, rowgroup, frozen, cuid, maskMax);
    pthread_rwlock_unlock(&rowgroup->m_mutex);

    Datum *val = (Datum *)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool *null = (bool *)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    /* form TupleDesc for imcs */
    TupleDesc imcsTupleDesc = FormImcsTupleDesc(relTupleDesc, imcsDesc->imcsAttsNum, imcsDesc->imcsNatts);

    /* init imcstoreInsert */
    IMCStoreInsert imcstoreInsert(rel, imcsTupleDesc, imcsDesc->imcsAttsNum);

    ItemPointerData ctid;
    uint32 curr = 0;
    while (curr <= maskMax) {
        if (deltaMask[curr >> 3] == 0) {
            /* skip this byte */
            curr = ((curr >> 3) + 1) * 8;
            continue;
        }
        if ((deltaMask[curr >> 3] & (1 << (curr % 8))) == 0) {
            ++curr;
            continue;
        }
        BlockNumber blk = curr / MAX_POSSIBLE_ROW_PER_PAGE;
        OffsetNumber offset = curr - blk * MAX_POSSIBLE_ROW_PER_PAGE;
        blk += cuid * MAX_IMCS_PAGES_ONE_CU;
        ItemPointerSetBlockNumber(&ctid, blk);
        ItemPointerSetOffsetNumber(&ctid, offset);
        ImcstoreCtid imcsCtid;
        imcsCtid.ctid = ctid;
        imcsCtid.reservedSpace = 0;
        errno_t rc = memcpy_s(&val[relTupleDesc->natts], sizeof(Datum), &imcsCtid, sizeof(ImcstoreCtid));
        securec_check_c(rc, "\0", "\0");
        null[relTupleDesc->natts] = false;

        ItempointerGetTuple(rel, ctid, val, null, imcstoreInsert);
        ++curr;
    }
    imcsDesc->UnReferenceRowGroup();

    imcstoreInsert.BatchReInsertCommon(imcsDesc, cuid, frozen);
    imcstoreInsert.ResetBatchRows(true);

    pfree(val);
    pfree(null);
    imcstoreInsert.Destroy();
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
    gspqsignal(SIGUSR2, SIG_IGN);
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

    // get database name
    while (!t_thrd.imcstore_vacuum_cxt.got_SIGTERM) {
        bool dbnameInited;
        pthread_rwlock_rdlock(&g_instance.imcstore_cxt.context_mutex);
        dbnameInited = g_instance.imcstore_cxt.dbname != NULL;
        pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
        if (dbnameInited) {
            t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(g_instance.imcstore_cxt.dbname, InvalidOid, username);
            t_thrd.proc_cxt.PostInit->InitHTAPImcsVacuum();
            break;
        }
        // wait weakup, or check per second
        int rc = WaitLatch(&g_instance.imcstore_cxt.vacuum_latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, (long)VACUMM_WAIT_TIME);
        if (rc & WL_POSTMASTER_DEATH) {
            proc_exit(0);
        }
    }

    SetProcessingMode(NormalProcessing);

    IMCStoreVacuumTarget target;
    while (!t_thrd.imcstore_vacuum_cxt.got_SIGTERM) {
        int rc = 0;

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.imcstore_vacuum_cxt.got_SIGHUP) {
            t_thrd.imcstore_vacuum_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
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

        /* Clear any already-pending wakeups */
        ResetLatch(&g_instance.imcstore_cxt.vacuum_latch);
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

        IMCSDesc *imcsDesc = (IMCSDesc *)IMCU_CACHE->GetImcsDesc(target.relOid);
        if (imcsDesc == NULL || imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) {
            continue;
        }

        MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
        PG_TRY();
        {
            IMCStoreVacuum(rel, imcsDesc, target.rowGroupId);
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
    elog(LOG, "imcstore vacuum thread is shutting down.");
}
#endif
