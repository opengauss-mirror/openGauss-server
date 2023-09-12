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
 * standby_read_proc.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/standby_read/standby_read_proc.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"
#include "access/multi_redo_api.h"
#include "access/extreme_rto/dispatcher.h"
#include "storage/procarray.h"
#include "replication/walreceiver.h"

inline void invalid_msg_leak_warning(XLogRecPtr trxn_lsn)
{
    if (t_thrd.page_redo_cxt.invalid_msg.valid) {
        ereport(WARNING,
            (errmsg(EXRTOFORMAT("[exrto_generate_snapshot] not send invalid msg: %08X/%08X"),
                (uint32)(trxn_lsn >> UINT64_HALF),
                (uint32)trxn_lsn)));
    }
}

void exrto_generate_snapshot(XLogRecPtr trxn_lsn)
{
    if (!g_instance.attr.attr_storage.EnableHotStandby) {
        return;
    }

    ExrtoSnapshot exrto_snapshot = g_instance.comm_cxt.predo_cxt.exrto_snapshot;
    /*
     * do not generate the same snapshot repeatedly.
     */
    if (XLByteLE(trxn_lsn, exrto_snapshot->read_lsn)) {
        invalid_msg_leak_warning(trxn_lsn);
        return;
    }

    TransactionId xmin;
    TransactionId xmax;
    CommitSeqNo snapshot_csn;

    exrto_get_snapshot_data(xmin, xmax, snapshot_csn);
    (void)LWLockAcquire(ExrtoSnapshotLock, LW_EXCLUSIVE);
    exrto_snapshot->snapshot_csn = snapshot_csn;
    exrto_snapshot->xmin = xmin;
    exrto_snapshot->xmax = xmax;
    exrto_snapshot->read_lsn = trxn_lsn;
    send_delay_invalid_message();
    LWLockRelease(ExrtoSnapshotLock);
}

void exrto_read_snapshot(Snapshot snapshot)
{
    if ((!is_exrto_standby_read_worker()) || u_sess->proc_cxt.clientIsCMAgent || dummyStandbyMode) {
        return;
    }

    ExrtoSnapshot exrto_snapshot = g_instance.comm_cxt.predo_cxt.exrto_snapshot;
    bool retry_get = false;
    const static uint64 WAIT_COUNT = 0x7FFFF;
    uint64 retry_count = 0;
    t_thrd.pgxact->xmin = InvalidTransactionId;
    t_thrd.proc->exrto_min = InvalidXLogRecPtr;
RETRY_GET:
    if (retry_get) {
        CHECK_FOR_INTERRUPTS();
        pg_usleep(100L);
    }
    retry_count++;
    if ((retry_count & WAIT_COUNT) == WAIT_COUNT) {
        ereport(LOG,
                (errmsg("retry to get exrto-standby-read snapshot, standby_redo_cleanup_xmin = %lu, "
                        "standby_redo_cleanup_xmin_lsn = %08X/%08X, "
                        "exrto_snapshot->xmin = %lu, read_lsn = %08X/%08X",
                        t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin,
                        (uint32)(t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn >> UINT64_HALF),
                        (uint32)t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn, exrto_snapshot->xmin,
                        (uint32)(exrto_snapshot->read_lsn >> UINT64_HALF), (uint32)exrto_snapshot->read_lsn)));
    }
    (void)LWLockAcquire(ExrtoSnapshotLock, LW_SHARED);
    if (XLByteEQ(exrto_snapshot->read_lsn, 0)) {
        LWLockRelease(ExrtoSnapshotLock);
        ereport(ERROR, (errmsg("could not get a valid snapshot with extreme rto")));
    }

    /* In exrto_standby_read_opt mode, getting a snapshot needs to wait for the cleanup-info xlog to be processed. */
    if (g_instance.attr.attr_storage.enable_exrto_standby_read_opt) {
        LWLockAcquire(ProcArrayLock, LW_SHARED);
        bool condition =
            (exrto_snapshot->xmin <=
             t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXmin) &&
            (t_thrd.xact_cxt.ShmemVariableCache->standbyRedoCleanupXminLsn > exrto_snapshot->read_lsn);
        LWLockRelease(ProcArrayLock);
        if (condition) {
            retry_get = true;
            LWLockRelease(ExrtoSnapshotLock);
            goto RETRY_GET;
        }
    }

    snapshot->snapshotcsn = exrto_snapshot->snapshot_csn;
    snapshot->xmin = exrto_snapshot->xmin;
    snapshot->xmax = exrto_snapshot->xmax;
    snapshot->read_lsn = exrto_snapshot->read_lsn;

    t_thrd.pgxact->xmin = snapshot->xmin;
    u_sess->utils_cxt.TransactionXmin = snapshot->xmin;

    t_thrd.proc->exrto_read_lsn = snapshot->read_lsn;
    t_thrd.proc->exrto_min = snapshot->read_lsn;
    LWLockRelease(ExrtoSnapshotLock);

    if (t_thrd.proc->exrto_gen_snap_time == 0) {
        t_thrd.proc->exrto_gen_snap_time = GetCurrentTimestamp();
    }
    Assert(XLogRecPtrIsValid(t_thrd.proc->exrto_read_lsn));
}

static inline uint64 get_force_recycle_pos(uint64 recycle_pos, uint64 insert_pos)
{
    const double force_recyle_ratio = 0.3; /* to be adjusted */
    Assert(recycle_pos <= insert_pos);
    return recycle_pos + (uint64)((insert_pos - recycle_pos) * force_recyle_ratio);
}

XLogRecPtr calculate_force_recycle_lsn_per_worker(StandbyReadMetaInfo *meta_info)
{
    uint64 base_page_recycle_pos;
    uint64 lsn_info_recycle_pos;
    XLogRecPtr base_page_recycle_lsn = InvalidXLogRecPtr;
    XLogRecPtr lsn_info_recycle_lsn = InvalidXLogRecPtr;
    Buffer buffer;
    Page page;

    /* for base page */
    if (meta_info->base_page_recyle_position < meta_info->base_page_next_position) {
        base_page_recycle_pos =
            get_force_recycle_pos(meta_info->base_page_recyle_position, meta_info->base_page_next_position);
        buffer = extreme_rto_standby_read::buffer_read_base_page(
            meta_info->batch_id, meta_info->redo_id, base_page_recycle_pos, RBM_NORMAL);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        base_page_recycle_lsn = PageGetLSN(BufferGetPage(buffer));
        UnlockReleaseBuffer(buffer);
    }

    /* for lsn info */
    if (meta_info->lsn_table_recyle_position < meta_info->lsn_table_next_position) {
        lsn_info_recycle_pos =
            get_force_recycle_pos(meta_info->lsn_table_recyle_position, meta_info->lsn_table_next_position);
        page = extreme_rto_standby_read::get_lsn_info_page(
            meta_info->batch_id, meta_info->redo_id, lsn_info_recycle_pos, RBM_NORMAL, &buffer);
        if (unlikely(page == NULL || buffer == InvalidBuffer)) {
            ereport(PANIC,
                (errmsg(EXRTOFORMAT("get_lsn_info_page failed, batch_id: %u, redo_id: %u, pos: %lu"),
                    meta_info->batch_id,
                    meta_info->redo_id,
                    lsn_info_recycle_pos)));
        }
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        extreme_rto_standby_read::LsnInfo lsn_info =
            (extreme_rto_standby_read::LsnInfo)(page + extreme_rto_standby_read::LSN_INFO_HEAD_SIZE);
        lsn_info_recycle_lsn = lsn_info->lsn[0];
        UnlockReleaseBuffer(buffer);
    }

    return rtl::max(base_page_recycle_lsn, lsn_info_recycle_lsn);
}

void calculate_force_recycle_lsn(XLogRecPtr &recycle_lsn)
{
    XLogRecPtr recycle_lsn_per_worker;
    uint32 worker_nums = extreme_rto::g_dispatcher->allWorkersCnt;
    extreme_rto::PageRedoWorker **workers = extreme_rto::g_dispatcher->allWorkers;

    for (uint32 i = 0; i < worker_nums; ++i) {
        extreme_rto::PageRedoWorker *page_redo_worker = workers[i];
        if (page_redo_worker->role != extreme_rto::REDO_PAGE_WORKER || (page_redo_worker->isUndoSpaceWorker)) {
            continue;
        }
        recycle_lsn_per_worker = calculate_force_recycle_lsn_per_worker(&page_redo_worker->standby_read_meta_info);
        if (XLByteLT(recycle_lsn, recycle_lsn_per_worker)) {
            recycle_lsn = recycle_lsn_per_worker;
        }
    }
    ereport(LOG,
        (errmsg(EXRTOFORMAT("[exrto_recycle] try force recycle, recycle lsn: %08X/%08X"),
            (uint32)(recycle_lsn >> UINT64_HALF),
            (uint32)recycle_lsn)));
}

static inline bool exceed_standby_max_query_time(TimestampTz start_time)
{
    if (start_time == 0) {
        return false;
    }
    return TimestampDifferenceExceeds(
        start_time, GetCurrentTimestamp(), g_instance.attr.attr_storage.standby_max_query_time * MSECS_PER_SEC);
}

/* 1. resolve recycle conflict with backends
 * 2. get oldest xmin and oldest readlsn of backends. */
void proc_array_get_oldeset_readlsn(
    XLogRecPtr recycle_lsn, XLogRecPtr &oldest_lsn, TransactionId &oldest_xmin, bool &conflict)
{
    ProcArrayStruct *proc_array = g_instance.proc_array_idx;
    conflict = false;

    (void)LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (int index = 0; index < proc_array->numProcs; index++) {
        int pg_proc_no = proc_array->pgprocnos[index];
        PGPROC *pg_proc = g_instance.proc_base_all_procs[pg_proc_no];
        PGXACT *pg_xact = &g_instance.proc_base_all_xacts[pg_proc_no];
        TransactionId pxmin = pg_xact->xmin;
        XLogRecPtr read_lsn = pg_proc->exrto_min;
        ereport(DEBUG1,
            (errmsg(EXRTOFORMAT("proc_array_get_oldeset_readlsn info, read_lsn: %08X/%08X ,xmin: %lu ,vacuum_flags: "
                                "%hhu ,pid: %lu"),
                (uint32)(read_lsn >> UINT64_HALF),
                (uint32)read_lsn,
                pxmin,
                pg_xact->vacuumFlags,
                pg_proc->pid)));

        if (pg_proc->pid == 0 || XLogRecPtrIsInvalid(read_lsn)) {
            continue;
        }

        Assert(!(pg_xact->vacuumFlags & PROC_IN_VACUUM));
        /*
         * Backend is doing logical decoding which manages xmin
         * separately, check below.
         */
        if (pg_xact->vacuumFlags & PROC_IN_LOGICAL_DECODING) {
            continue;
        }

        /* cancel query when its read_lsn < recycle_lsn or its runtime > standby_max_query_time */
        if (XLByteLT(read_lsn, recycle_lsn) || exceed_standby_max_query_time(pg_proc->exrto_gen_snap_time)) {
            pg_proc->recoveryConflictPending = true;
            conflict = true;
            if (pg_proc->pid != 0) {
                /*
                 * Kill the pid if it's still here. If not, that's what we
                 * wanted so ignore any errors.
                 */
                (void)SendProcSignal(pg_proc->pid, PROCSIG_RECOVERY_CONFLICT_SNAPSHOT, pg_proc->backendId);
                ereport(LOG,
                    (errmsg(
                        EXRTOFORMAT("read_lsn is less than recycle_lsn or query time exceed max_query_time while "
                                    "get_oldeset_readlsn, read_lsn %lu, "
                                    "recycle_lsn: %lu, exrto_gen_snap_time: %ld, current_time: %ld, thread id = %lu\n"),
                        read_lsn,
                        recycle_lsn,
                        pg_proc->exrto_gen_snap_time,
                        GetCurrentTimestamp(),
                        pg_proc->pid)));
                /*
                 * Wait a little bit for it to die so that we avoid flooding
                 * an unresponsive backend when system is heavily loaded.
                 */
                pg_usleep(5000L);
            }
            continue;
        }

        if (XLogRecPtrIsInvalid(oldest_lsn) || (XLogRecPtrIsValid(read_lsn) && XLByteLT(read_lsn, oldest_lsn))) {
            oldest_lsn = read_lsn;
        }

        if (!TransactionIdIsValid(oldest_xmin) ||
            (TransactionIdIsValid(pxmin) && TransactionIdFollows(oldest_xmin, pxmin))) {
            oldest_xmin = pxmin;
        }
    }
    LWLockRelease(ProcArrayLock);
}

void proc_array_get_oldeset_xmin_for_undo(TransactionId &oldest_xmin)
{
    ProcArrayStruct *proc_array = g_instance.proc_array_idx;

    (void)LWLockAcquire(ProcArrayLock, LW_SHARED);
    for (int index = 0; index < proc_array->numProcs; index++) {
        int pg_proc_no = proc_array->pgprocnos[index];
        PGPROC *pg_proc = g_instance.proc_base_all_procs[pg_proc_no];
        PGXACT *pg_xact = &g_instance.proc_base_all_xacts[pg_proc_no];
        TransactionId pxmin = pg_xact->xmin;

        if (pg_proc->pid == 0 || !TransactionIdIsValid(pxmin)) {
            continue;
        }

        Assert(!(pg_xact->vacuumFlags & PROC_IN_VACUUM));
        /*
         * Backend is doing logical decoding which manages xmin
         * separately, check below.
         */
        if (pg_xact->vacuumFlags & PROC_IN_LOGICAL_DECODING) {
            continue;
        }
        if (!TransactionIdIsValid(oldest_xmin) ||
            (TransactionIdIsValid(pxmin) && TransactionIdFollows(oldest_xmin, pxmin))) {
            oldest_xmin = pxmin;
        }
    }
    LWLockRelease(ProcArrayLock);
}

XLogRecPtr exrto_calculate_recycle_position(bool force_recyle)
{
    Assert(t_thrd.role != PAGEREDO);
    Assert(IS_EXRTO_READ);

    XLogRecPtr recycle_lsn = pg_atomic_read_u64(&g_instance.comm_cxt.predo_cxt.global_recycle_lsn);
    XLogRecPtr oldest_lsn = InvalidXLogRecPtr;
    TransactionId oldest_xmin = InvalidTransactionId;
    bool conflict = false;
    const int max_check_times = 1000;
    int check_times = 0;

    if (force_recyle) {
        calculate_force_recycle_lsn(recycle_lsn);
    }
    ereport(DEBUG1,
        (errmsg(EXRTOFORMAT("time information of calculate recycle position, current_time: %ld, snapshot "
                            "read_lsn: %08X/%08X, gen_snaptime:%ld"),
            GetCurrentTimestamp(),
            (uint32)(g_instance.comm_cxt.predo_cxt.exrto_snapshot->read_lsn >> UINT64_HALF),
            (uint32)g_instance.comm_cxt.predo_cxt.exrto_snapshot->read_lsn,
            g_instance.comm_cxt.predo_cxt.exrto_snapshot->gen_snap_time)));

    /*
     * If there is no backend read threads, set read oldest lsn to snapshot lsn.
     */
    ExrtoSnapshot exrto_snapshot = NULL;
    exrto_snapshot = g_instance.comm_cxt.predo_cxt.exrto_snapshot;
    (void)LWLockAcquire(ExrtoSnapshotLock, LW_SHARED);
    if (XLByteEQ(exrto_snapshot->read_lsn, 0)) {
        ereport(WARNING, (errmsg("could not get a valid snapshot with extreme rto")));
    } else {
        oldest_lsn = exrto_snapshot->read_lsn;
        oldest_xmin = exrto_snapshot->xmin;
    }
    LWLockRelease(ExrtoSnapshotLock);
    /* Loop checks to avoid conflicting queries that were not successfully canceled. */
    do {
        RedoInterruptCallBack();
        proc_array_get_oldeset_readlsn(recycle_lsn, oldest_lsn, oldest_xmin, conflict);
        check_times++;
    } while (conflict && check_times < max_check_times);

    recycle_lsn = rtl::max(recycle_lsn, oldest_lsn);

    ereport(LOG,
        (errmsg(
            EXRTOFORMAT(
                "[exrto_recycle] calculate recycle position, oldestlsn: %08X/%08X, snapshot read_lsn: %08X/%08X, try "
                "recycle lsn: %08X/%08X, xmin: %lu"),
            (uint32)(oldest_lsn >> UINT64_HALF),
            (uint32)oldest_lsn,
            (uint32)(g_instance.comm_cxt.predo_cxt.exrto_snapshot->read_lsn >> UINT64_HALF),
            (uint32)g_instance.comm_cxt.predo_cxt.exrto_snapshot->read_lsn,
            (uint32)(recycle_lsn >> UINT64_HALF),
            (uint32)recycle_lsn, oldest_xmin)));
    pg_atomic_write_u64(&g_instance.comm_cxt.predo_cxt.exrto_recyle_xmin, oldest_xmin);
    return recycle_lsn;
}

TransactionId exrto_calculate_recycle_xmin_for_undo()
{
    Assert(t_thrd.role != PAGEREDO);
    Assert(IS_EXRTO_READ);
    TransactionId oldest_xmin = InvalidTransactionId;
    TransactionId snapshot_xmin = InvalidTransactionId;
    proc_array_get_oldeset_xmin_for_undo(oldest_xmin);

    /*
     * If there is no backend read threads, set read oldest lsn to snapshot lsn.
     */
    if ((oldest_xmin == InvalidTransactionId) && (extreme_rto::g_dispatcher != NULL)) {
        ExrtoSnapshot exrto_snapshot = NULL;
        exrto_snapshot = g_instance.comm_cxt.predo_cxt.exrto_snapshot;
        (void)LWLockAcquire(ExrtoSnapshotLock, LW_SHARED);
        if (XLByteEQ(exrto_snapshot->xmin, InvalidTransactionId)) {
            ereport(WARNING,
                (errmsg("exrto_calculate_recycle_xmin_for_undo: could not get a valid snapshot in exrto_snapshot")));
        } else {
            snapshot_xmin = exrto_snapshot->xmin;
        }

        LWLockRelease(ExrtoSnapshotLock);
    }
    ereport(DEBUG1,
        (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("exrto_calculate_recycle_xmin_for_undo: oldest_xmin: %lu, snapshot_xmin: %lu."),
                oldest_xmin,
                snapshot_xmin)));

    if (oldest_xmin == InvalidTransactionId) {
        return snapshot_xmin;
    }
    return oldest_xmin;
}
