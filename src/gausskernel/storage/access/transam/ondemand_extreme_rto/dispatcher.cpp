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
 * dispatcher.cpp
 *      Parallel recovery has a centralized log dispatcher which runs inside
 *      the StartupProcess.  The dispatcher is responsible for managing the
 *      life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 *      records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/ondemand_extreme_rto/dispatcher.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/startup.h"
#include "access/clog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/hash_xlog.h"
#include "access/xlogreader.h"
#include "access/gist_private.h"
#include "access/multixact.h"
#include "access/spgist_private.h"
#include "access/gin_private.h"
#include "access/xlogutils.h"
#include "access/gin.h"

#include "catalog/storage_xlog.h"
#include "storage/buf/buf_internals.h"
#include "storage/ipc.h"
#include "storage/standby.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/guc.h"
#include "utils/relmapper.h"

#include "portability/instr_time.h"

#include "access/ondemand_extreme_rto/dispatcher.h"
#include "access/ondemand_extreme_rto/page_redo.h"
#include "access/multi_redo_api.h"

#include "access/ondemand_extreme_rto/txn_redo.h"
#include "access/ondemand_extreme_rto/spsc_blocking_queue.h"
#include "access/ondemand_extreme_rto/redo_item.h"
#include "access/ondemand_extreme_rto/batch_redo.h"
#include "access/ondemand_extreme_rto/xlog_read.h"

#include "catalog/storage.h"
#include <sched.h>
#include "utils/memutils.h"

#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "commands/sequence.h"

#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/ddlmessage.h"
#include "gssignal/gs_signal.h"
#include "utils/atomic.h"
#include "pgstat.h"
#include "ddes/dms/ss_reform_common.h"
#include "ddes/dms/ss_transaction.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#ifdef ENABLE_UT
#include "utils/utesteventutil.h"
#define STATIC
#else
#define STATIC static
#endif

extern THR_LOCAL bool redo_oldversion_xlog;

namespace ondemand_extreme_rto {
LogDispatcher *g_dispatcher = NULL;

static const int XLOG_INFO_SHIFT_SIZE = 4; /* xlog info flag shift size */

static const int32 MAX_PENDING = 1;
static const int32 MAX_PENDING_STANDBY = 1;
static const int32 ITEM_QUQUE_SIZE_RATIO = 16;

static const uint32 EXIT_WAIT_DELAY = 100; /* 100 us */
uint32 g_readManagerTriggerFlag = TRIGGER_NORMAL;
static const int invalid_worker_id = -1;

static const int UNDO_START_BLK = 1;
static const int UHEAP_UPDATE_UNDO_START_BLK = 2; 

struct ControlFileData restoreControlFile;

typedef void *(*GetStateFunc)(PageRedoWorker *worker);

static void AddSlotToPLSet(uint32);
static void **CollectStatesFromWorkers(GetStateFunc);
static void GetSlotIds(XLogReaderState *record);
static void GetUndoSlotIds(XLogReaderState *record);
STATIC LogDispatcher *CreateDispatcher();
static void SSDestroyRecoveryWorkers();

static void DispatchRecordWithPages(XLogReaderState *, List *);
static void DispatchRecordWithoutPage(XLogReaderState *, List *);
static void DispatchTxnRecord(XLogReaderState *, List *);
static void StartPageRedoWorkers(uint32 totalThrdNum, bool inRealtimeBuild);
static void StopRecoveryWorkers(int, Datum);
static bool StandbyWillChangeStandbyState(const XLogReaderState *);
static void DispatchToSpecPageWorker(XLogReaderState *record, List *expectedTLIs);

static bool DispatchXLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchCLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHashRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchCompresseShrinkRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchDataBaseRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchTableSpaceRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchMultiXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRelMapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchStandbyRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSeqRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchGinRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchGistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSpgistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRepSlotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchHeap3Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchDefaultRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchBarrierRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
#ifdef ENABLE_MOT
static bool DispatchMotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
#endif
static bool DispatchBtreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchSegpageSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchRepOriginRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);

static bool DispatchUBTreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static bool DispatchUBTree2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool RmgrRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);
static bool RmgrGistRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo);

/* Ustore table */
static bool DispatchUHeapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchUHeap2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchUHeapUndoRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchUndoActionRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchRollbackFinishRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime);
static bool DispatchLogicalDDLMsgRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime);
static inline uint32 GetUndoSpaceWorkerId(int zid);

static XLogReaderState *GetXlogReader(XLogReaderState *readerState);
void CopyDataFromOldReader(XLogReaderState *newReaderState, const XLogReaderState *oldReaderState);
void SendSingalToPageWorker(int signal);

static void RestoreControlFileForRealtimeBuild();

/* dispatchTable must consistent with RmgrTable */
static const RmgrDispatchData g_dispatchTable[RM_MAX_ID + 1] = {
    { DispatchXLogRecord, RmgrRecordInfoValid, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, XLOG_DELAY_XLOG_RECYCLE },
    { DispatchXactRecord, RmgrRecordInfoValid, RM_XACT_ID, XLOG_XACT_COMMIT, XLOG_XACT_ABORT_WITH_XID },
    { DispatchSmgrRecord, RmgrRecordInfoValid, RM_SMGR_ID, XLOG_SMGR_CREATE, XLOG_SMGR_TRUNCATE },
    { DispatchCLogRecord, RmgrRecordInfoValid, RM_CLOG_ID, CLOG_ZEROPAGE, CLOG_TRUNCATE },
    { DispatchDataBaseRecord, RmgrRecordInfoValid, RM_DBASE_ID, XLOG_DBASE_CREATE, XLOG_DBASE_DROP },
    { DispatchTableSpaceRecord, RmgrRecordInfoValid, RM_TBLSPC_ID, XLOG_TBLSPC_CREATE, XLOG_TBLSPC_RELATIVE_CREATE },
    { DispatchMultiXactRecord,
      RmgrRecordInfoValid,
      RM_MULTIXACT_ID,
      XLOG_MULTIXACT_ZERO_OFF_PAGE,
      XLOG_MULTIXACT_CREATE_ID },
    { DispatchRelMapRecord, RmgrRecordInfoValid, RM_RELMAP_ID, XLOG_RELMAP_UPDATE, XLOG_RELMAP_UPDATE },
    { DispatchStandbyRecord, RmgrRecordInfoValid, RM_STANDBY_ID, XLOG_STANDBY_LOCK, XLOG_STANDBY_CSN_ABORTED },

    { DispatchHeap2Record, RmgrRecordInfoValid, RM_HEAP2_ID, XLOG_HEAP2_FREEZE, XLOG_HEAP2_LOGICAL_NEWPAGE },
    { DispatchHeapRecord, RmgrRecordInfoValid, RM_HEAP_ID, XLOG_HEAP_INSERT, XLOG_HEAP_INPLACE },
    { DispatchBtreeRecord, RmgrRecordInfoValid, RM_BTREE_ID, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_DEDUP },
    { DispatchHashRecord, RmgrRecordInfoValid, RM_HASH_ID, XLOG_HASH_INIT_META_PAGE, XLOG_HASH_VACUUM_ONE_PAGE },
    { DispatchGinRecord, RmgrRecordInfoValid, RM_GIN_ID, XLOG_GIN_CREATE_INDEX, XLOG_GIN_VACUUM_DATA_LEAF_PAGE },
    /* XLOG_GIST_PAGE_DELETE is not used and info isn't continus  */
    { DispatchGistRecord, RmgrGistRecordInfoValid, RM_GIST_ID, 0, 0 },
    { DispatchSeqRecord, RmgrRecordInfoValid, RM_SEQ_ID, XLOG_SEQ_LOG, XLOG_SEQ_LOG },
    { DispatchSpgistRecord, RmgrRecordInfoValid, RM_SPGIST_ID, XLOG_SPGIST_CREATE_INDEX, XLOG_SPGIST_VACUUM_REDIRECT },
    { DispatchRepSlotRecord, RmgrRecordInfoValid, RM_SLOT_ID, XLOG_SLOT_CREATE, XLOG_TERM_LOG },
    { DispatchHeap3Record, RmgrRecordInfoValid, RM_HEAP3_ID, XLOG_HEAP3_NEW_CID, XLOG_HEAP3_INVALID },
    { DispatchBarrierRecord, RmgrRecordInfoValid, RM_BARRIER_ID, XLOG_BARRIER_CREATE, XLOG_BARRIER_SWITCHOVER },
#ifdef ENABLE_MOT
    {DispatchMotRecord, NULL, RM_MOT_ID, 0, 0},
#endif
    { DispatchUHeapRecord, RmgrRecordInfoValid, RM_UHEAP_ID, XLOG_UHEAP_INSERT, XLOG_UHEAP_MULTI_INSERT },
    { DispatchUHeap2Record, RmgrRecordInfoValid, RM_UHEAP2_ID, XLOG_UHEAP2_BASE_SHIFT, XLOG_UHEAP2_EXTEND_TD_SLOTS },
    { DispatchUHeapUndoRecord, RmgrRecordInfoValid, RM_UNDOLOG_ID, XLOG_UNDO_EXTEND, XLOG_UNDO_DISCARD },
    { DispatchUndoActionRecord, RmgrRecordInfoValid, RM_UHEAPUNDO_ID, 
        XLOG_UHEAPUNDO_PAGE, XLOG_UHEAPUNDO_ABORT_SPECINSERT },
    { DispatchRollbackFinishRecord, RmgrRecordInfoValid, RM_UNDOACTION_ID, XLOG_ROLLBACK_FINISH, XLOG_ROLLBACK_FINISH },
    { DispatchUBTreeRecord, RmgrRecordInfoValid, RM_UBTREE_ID, XLOG_UBTREE_INSERT_LEAF, XLOG_UBTREE_PRUNE_PAGE },
    { DispatchUBTree2Record, RmgrRecordInfoValid, RM_UBTREE2_ID, XLOG_UBTREE2_SHIFT_BASE,
        XLOG_UBTREE2_FREEZE },
    { DispatchSegpageSmgrRecord, RmgrRecordInfoValid, RM_SEGPAGE_ID, XLOG_SEG_ATOMIC_OPERATION,
        XLOG_SEG_NEW_PAGE},
    { DispatchRepOriginRecord, RmgrRecordInfoValid, RM_REPLORIGIN_ID, XLOG_REPLORIGIN_SET, XLOG_REPLORIGIN_DROP },
    { DispatchCompresseShrinkRecord, RmgrRecordInfoValid, RM_COMPRESSION_REL_ID, XLOG_CFS_SHRINK_OPERATION,
        XLOG_CFS_SHRINK_OPERATION },
    { DispatchLogicalDDLMsgRecord, RmgrRecordInfoValid, RM_LOGICALDDLMSG_ID, XLOG_LOGICAL_DDL_MESSAGE,
        XLOG_LOGICAL_DDL_MESSAGE },
};

const int REDO_WAIT_SLEEP_TIME = 5000; /* 5ms */
const int MAX_REDO_WAIT_LOOP = 24000;  /* 5ms*24000 = 2min */

uint32 GetReadyWorker()
{
    uint32 readyWorkerCnt = 0;

    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; i++) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state >= PAGE_REDO_WORKER_READY) {
            ++readyWorkerCnt;
        }
    }
    return readyWorkerCnt;
}

void WaitWorkerReady()
{
    uint32 waitLoop = 0;
    uint32 readyWorkerCnt = 0;
    /* MAX wait 2min */
    for (waitLoop = 0; waitLoop < MAX_REDO_WAIT_LOOP; ++waitLoop) {
        readyWorkerCnt = GetReadyWorker();
        if (readyWorkerCnt == g_instance.comm_cxt.predo_cxt.totalNum) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%u",
                                 g_dispatcher->allWorkersCnt, readyWorkerCnt)));
            break;
        }
        pg_usleep(REDO_WAIT_SLEEP_TIME);
    }
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    g_instance.comm_cxt.predo_cxt.state = REDO_STARTING_END;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    readyWorkerCnt = GetReadyWorker();
    if (waitLoop == MAX_REDO_WAIT_LOOP && readyWorkerCnt == 0) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("WaitWorkerReady failed, no worker is ready for work. totalWorkerCount :%u",
                               g_dispatcher->allWorkersCnt)));
    }

    /* RTO_DEMO */
    if (readyWorkerCnt != g_dispatcher->allWorkersCnt) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("WaitWorkerReady total thread count:%u, readyWorkerCnt:%u, not all thread ready",
                               g_dispatcher->allWorkersCnt, readyWorkerCnt)));
    }
}

void CheckAlivePageWorkers()
{
    for (uint32 i = 0; i < MAX_RECOVERY_THREAD_NUM; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState != PAGE_REDO_WORKER_INVALID) {
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                            errmsg("CheckAlivePageWorkers: thread %lu is still alive",
                                   g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId)));
        }
        g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId = 0;
    }
    g_instance.comm_cxt.predo_cxt.totalNum = 0;
}

#ifdef USE_ASSERT_CHECKING
void InitLsnCheckCtl(XLogRecPtr readRecPtr)
{
    g_dispatcher->originLsnCheckAddr = (void *)palloc0(sizeof(LsnCheckCtl) + ONDEMAND_EXTREME_RTO_ALIGN_LEN);
    g_dispatcher->lsnCheckCtl = (LsnCheckCtl *)TYPEALIGN(ONDEMAND_EXTREME_RTO_ALIGN_LEN, g_dispatcher->originLsnCheckAddr);
    g_dispatcher->lsnCheckCtl->curLsn = readRecPtr;
    g_dispatcher->lsnCheckCtl->curPosition = 0;
    SpinLockInit(&g_dispatcher->updateLck);
#if (!defined __x86_64__) && (!defined __aarch64__)
    SpinLockInit(&g_dispatcher->lsnCheckCtl->ptrLck);
#endif
}
#endif

void AllocRecordReadBuffer(XLogReaderState *xlogreader, uint32 privateLen)
{
    XLogReaderState *initreader;
    errno_t errorno = EOK;

    initreader = GetXlogReader(xlogreader);
    initreader->isPRProcess = true;
    g_dispatcher->rtoXlogBufState.readWorkerState = WORKER_STATE_STOP;
    g_dispatcher->rtoXlogBufState.readPageWorkerState = WORKER_STATE_STOP;
    g_dispatcher->rtoXlogBufState.readSource = 0;
    g_dispatcher->rtoXlogBufState.failSource = 0;
    g_dispatcher->rtoXlogBufState.xlogReadManagerState = READ_MANAGER_RUN;
    g_dispatcher->rtoXlogBufState.targetRecPtr = InvalidXLogRecPtr;
    g_dispatcher->rtoXlogBufState.expectLsn = InvalidXLogRecPtr;
    g_dispatcher->rtoXlogBufState.waitRedoDone = 0;
    g_dispatcher->rtoXlogBufState.readsegbuf = (char *)palloc0(XLogSegSize * MAX_ALLOC_SEGNUM);
    g_dispatcher->rtoXlogBufState.readBuf = (char *)palloc0(XLOG_BLCKSZ);
    g_dispatcher->rtoXlogBufState.readprivate = (void *)palloc0(MAXALIGN(privateLen));
    errorno = memset_s(g_dispatcher->rtoXlogBufState.readprivate, MAXALIGN(privateLen), 0, MAXALIGN(privateLen));
    securec_check(errorno, "", "");

    g_dispatcher->rtoXlogBufState.errormsg_buf = (char *)palloc0(MAX_ERRORMSG_LEN + 1);
    g_dispatcher->rtoXlogBufState.errormsg_buf[0] = '\0';

    char *readsegbuf = g_dispatcher->rtoXlogBufState.readsegbuf;
    for (uint32 i = 0; i < MAX_ALLOC_SEGNUM; i++) {
        g_dispatcher->rtoXlogBufState.xlogsegarray[i].readsegbuf = readsegbuf;
        readsegbuf += XLogSegSize;
        g_dispatcher->rtoXlogBufState.xlogsegarray[i].bufState = NONE;
    }

    g_dispatcher->rtoXlogBufState.applyindex = 0;

    g_dispatcher->rtoXlogBufState.readindex = 0;

    g_dispatcher->rtoXlogBufState.xlogsegarray[0].segno = xlogreader->readSegNo;
    g_dispatcher->rtoXlogBufState.xlogsegarray[0].segoffset = xlogreader->readOff;
    g_dispatcher->rtoXlogBufState.xlogsegarray[0].readlen = xlogreader->readOff + xlogreader->readLen;

    initreader->readBuf = g_dispatcher->rtoXlogBufState.xlogsegarray[0].readsegbuf +
                          g_dispatcher->rtoXlogBufState.xlogsegarray[0].segoffset;

    errorno = memcpy_s(initreader->readBuf, XLOG_BLCKSZ, xlogreader->readBuf, xlogreader->readLen);
    securec_check(errorno, "", "");
    initreader->errormsg_buf = g_dispatcher->rtoXlogBufState.errormsg_buf;
    initreader->private_data = g_dispatcher->rtoXlogBufState.readprivate;
    CopyDataFromOldReader(initreader, xlogreader);
    g_dispatcher->rtoXlogBufState.initreader = initreader;

    g_recordbuffer = &g_dispatcher->rtoXlogBufState;
    g_startupTriggerState = TRIGGER_NORMAL;
    g_readManagerTriggerFlag = TRIGGER_NORMAL;
#ifdef USE_ASSERT_CHECKING
    InitLsnCheckCtl(xlogreader->ReadRecPtr);
#endif
}

void SSAllocRecordReadBuffer(XLogReaderState *xlogreader, uint32 privateLen)
{
    XLogReaderState *initreader;
    errno_t errorno = EOK;

    initreader = GetXlogReader(xlogreader);
    initreader->isPRProcess = true;
    g_dispatcher->rtoXlogBufState.readWorkerState = WORKER_STATE_STOP;
    g_dispatcher->rtoXlogBufState.readPageWorkerState = WORKER_STATE_STOP;
    g_dispatcher->rtoXlogBufState.readSource = 0;
    g_dispatcher->rtoXlogBufState.failSource = 0;
    g_dispatcher->rtoXlogBufState.xlogReadManagerState = READ_MANAGER_RUN;
    g_dispatcher->rtoXlogBufState.targetRecPtr = InvalidXLogRecPtr;
    g_dispatcher->rtoXlogBufState.expectLsn = InvalidXLogRecPtr;
    g_dispatcher->rtoXlogBufState.waitRedoDone = 0;
    g_dispatcher->rtoXlogBufState.readBuf = (char *)palloc0(XLOG_BLCKSZ);
    g_dispatcher->rtoXlogBufState.readprivate = (void *)palloc0(MAXALIGN(privateLen));
    errorno = memset_s(g_dispatcher->rtoXlogBufState.readprivate, MAXALIGN(privateLen), 0, MAXALIGN(privateLen));
    securec_check(errorno, "", "");

    g_dispatcher->rtoXlogBufState.errormsg_buf = (char *)palloc0(MAX_ERRORMSG_LEN + 1);
    g_dispatcher->rtoXlogBufState.errormsg_buf[0] = '\0';

    initreader->readBuf = g_dispatcher->rtoXlogBufState.readBuf;
    errorno = memcpy_s(initreader->readBuf, XLOG_BLCKSZ, xlogreader->readBuf, xlogreader->readLen);
    securec_check(errorno, "", "");
    initreader->errormsg_buf = g_dispatcher->rtoXlogBufState.errormsg_buf;
    initreader->private_data = g_dispatcher->rtoXlogBufState.readprivate;
    CopyDataFromOldReader(initreader, xlogreader);
    g_dispatcher->rtoXlogBufState.initreader = initreader;

    g_recordbuffer = &g_dispatcher->rtoXlogBufState;
    g_startupTriggerState = TRIGGER_NORMAL;
    g_readManagerTriggerFlag = TRIGGER_NORMAL;
#ifdef USE_ASSERT_CHECKING
    InitLsnCheckCtl(xlogreader->ReadRecPtr);
#endif
}

void HandleStartupInterruptsForExtremeRto()
{
    Assert(AmStartupProcess());

    uint32 newtriggered = (uint32)CheckForSatartupStatus();
    if (newtriggered != TRIGGER_NORMAL) {
        uint32 triggeredstate = pg_atomic_read_u32(&(g_startupTriggerState));
        if (triggeredstate != newtriggered) {
            ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                          errmsg("HandleStartupInterruptsForExtremeRto:g_startupTriggerState set from %u to %u",
                                 triggeredstate, newtriggered)));
            pg_atomic_write_u32(&(g_startupTriggerState), newtriggered);
        }
    }

    if (t_thrd.startup_cxt.got_SIGHUP) {
        t_thrd.startup_cxt.got_SIGHUP = false;
        SendSingalToPageWorker(SIGHUP);
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.startup_cxt.shutdown_requested) {
        if (g_instance.status != SmartShutdown) {
            if (ENABLE_ONDEMAND_REALTIME_BUILD &&
                (SS_PERFORMING_SWITCHOVER || (SS_STANDBY_MODE && DMS_REFORM_TYPE_FOR_FAILOVER_OPENGAUSS))) {
                Assert(!SS_ONDEMAND_REALTIME_BUILD_DISABLED);
                proc_exit(0);
            } else {
                proc_exit(1);
            }
        } else {
            g_dispatcher->smartShutdown = true;
        }
    }
    if (t_thrd.startup_cxt.check_repair) {
        t_thrd.startup_cxt.check_repair = false;
    }

    if (SS_STANDBY_FAILOVER && pmState == PM_STARTUP && SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        OndemandRealtimeBuildHandleFailover();
    }
}

static void SetOndemandXLogParseFlagValue(uint32 maxParseBufNum)
{
    g_ondemandXLogParseMemFullValue = maxParseBufNum * ONDEMAND_FORCE_PRUNE_RATIO;
    g_ondemandXLogParseMemApproachFullVaule = maxParseBufNum * ONDEMAND_DISTRIBUTE_RATIO;

    g_ondemandRealtimeBuildQueueFullValue = REALTIME_BUILD_RECORD_QUEUE_SIZE * ONDEMAND_FORCE_PRUNE_RATIO;
}

/* Run from the dispatcher thread. */
void StartRecoveryWorkers(XLogReaderState *xlogreader, uint32 privateLen)
{
    if (get_real_recovery_parallelism() > 1) {
        if (t_thrd.xlog_cxt.StandbyModeRequested) {
            ReLeaseRecoveryLatch();
        }

        CheckAlivePageWorkers();
        g_dispatcher = CreateDispatcher();
        g_dispatcher->oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
        g_instance.comm_cxt.redoItemCtx = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
            "redoItemSharedMemory",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
        g_instance.comm_cxt.predo_cxt.redoItemHashCtrl = PRInitRedoItemHashForAllPipeline(g_instance.comm_cxt.redoItemCtx);
        if (ENABLE_ONDEMAND_REALTIME_BUILD && !SS_ONDEMAND_REALTIME_BUILD_DISABLED) {
            errno_t rc = EOK;
            g_dispatcher->restoreControlFile = (ControlFileData *)palloc(sizeof(ControlFileData));
            rc = memcpy_s(g_dispatcher->restoreControlFile, (size_t)sizeof(ControlFileData), &restoreControlFile, (size_t)sizeof(ControlFileData));
            securec_check(rc, "", "");
        }
        g_dispatcher->maxItemNum = (get_batch_redo_num() + 4) * PAGE_WORK_QUEUE_SIZE *
                                   ITEM_QUQUE_SIZE_RATIO;  // 4: a startup, readmanager, txnmanager, txnworker
        uint32 maxParseBufNum = (uint32)((uint64)g_instance.attr.attr_storage.dms_attr.ondemand_recovery_mem_size *
            1024 / (sizeof(XLogRecParseState) + sizeof(ParseBufferDesc) + sizeof(RedoMemSlot)));
        XLogParseBufferInitFunc(&(g_dispatcher->parseManager), maxParseBufNum, &recordRefOperate, RedoInterruptCallBack);
        SetOndemandXLogParseFlagValue(maxParseBufNum);
        /* alloc for record readbuf */
        SSAllocRecordReadBuffer(xlogreader, privateLen);
        StartPageRedoWorkers(get_real_recovery_parallelism(), SS_ONDEMAND_REALTIME_BUILD_NORMAL);

        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("[PR]: max=%d, thrd=%d", g_instance.attr.attr_storage.max_recovery_parallelism,
                             get_real_recovery_parallelism())));
        WaitWorkerReady();
        SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
        g_instance.comm_cxt.predo_cxt.state = REDO_IN_PROGRESS;
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
        on_shmem_exit(StopRecoveryWorkers, 0);

        g_dispatcher->oldStartupIntrruptFunc = RegisterRedoInterruptCallBack(HandleStartupInterruptsForExtremeRto);

        close_readFile_if_open();
    }
}

void DumpDispatcher()
{
    knl_parallel_redo_state state;
    PageRedoPipeline *pl = NULL;
    state = g_instance.comm_cxt.predo_cxt.state;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("[REDO_LOG_TRACE]dispatcher : totalWorkerCount %d, state %u, curItemNum %u, maxItemNum %u",
                             get_real_recovery_parallelism(), (uint32)state, g_dispatcher->curItemNum,
                             g_dispatcher->maxItemNum)));

        for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
            pl = &(g_dispatcher->pageLines[i]);
            DumpPageRedoWorker(pl->batchThd);
            DumpPageRedoWorker(pl->managerThd);
            DumpPageRedoWorker(pl->htabThd);
            for (uint32 j = 0; j < pl->redoThdNum; j++) {
                DumpPageRedoWorker(pl->redoThd[j]);
            }
        }
        DumpPageRedoWorker(g_dispatcher->trxnLine.managerThd);
        DumpPageRedoWorker(g_dispatcher->trxnLine.redoThd);
        DumpPageRedoWorker(g_dispatcher->auxiliaryLine.segRedoThd);
        DumpPageRedoWorker(g_dispatcher->auxiliaryLine.ctrlThd);
        DumpXlogCtl();
    }
}

/* Run from the dispatcher thread. */
STATIC LogDispatcher *CreateDispatcher()
{
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "ParallelRecoveryDispatcher",
                                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);

    LogDispatcher *newDispatcher = (LogDispatcher *)MemoryContextAllocZero(ctx, sizeof(LogDispatcher));

    g_instance.comm_cxt.predo_cxt.parallelRedoCtx = ctx;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    g_instance.comm_cxt.predo_cxt.state = REDO_STARTING_BEGIN;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));
    newDispatcher->totalCostTime = 0;
    newDispatcher->txnCostTime = 0;
    newDispatcher->pprCostTime = 0;
    newDispatcher->syncEnterCount = 0;
    newDispatcher->syncExitCount = 0;
    newDispatcher->batchThrdEnterNum = 0;
    newDispatcher->batchThrdExitNum = 0;
    newDispatcher->segpageXactDoneFlag = 0;
    newDispatcher->restoreControlFile = NULL; 

    pg_atomic_init_u32(&(newDispatcher->standbyState), STANDBY_INITIALIZED);
    newDispatcher->needImmediateCheckpoint = false;
    newDispatcher->needFullSyncCheckpoint = false;
    newDispatcher->smartShutdown = false;
    newDispatcher->startupTimeCost = t_thrd.xlog_cxt.timeCost;
    newDispatcher->trxnQueue = SPSCBlockingQueueCreate(REALTIME_BUILD_RECORD_QUEUE_SIZE, RedoWorkerQueueCallBack);
    newDispatcher->segQueue = SPSCBlockingQueueCreate(REALTIME_BUILD_RECORD_QUEUE_SIZE, RedoWorkerQueueCallBack);
    return newDispatcher;
}

void RedoRoleInit(PageRedoWorker **dstWk, PageRedoWorker *srcWk, RedoRole role, 
    uint32 slotId, bool isUndoSpaceWorker)
{
    *dstWk = srcWk;
    (*dstWk)->role = role;
    (*dstWk)->slotId = slotId;
    (*dstWk)->isUndoSpaceWorker = isUndoSpaceWorker;
}

/* Run from the dispatcher thread. */
static void StartPageRedoWorkers(uint32 totalThrdNum, bool inRealtimeBuild)
{
    uint32 batchNum = get_batch_redo_num();
    uint32 batchWorkerPerMng = get_page_redo_worker_num_per_manager();
    uint32 undoSpaceWorkersNum = get_recovery_undozidworkers_num();
    uint32 workerCnt = 0;
    PageRedoWorker **tmpWorkers;
    uint32 started;
    ereport(LOG, (errmsg("StartPageRedoWorkers, totalThrdNum:%u, "
                         "batchNum:%u, batchWorkerPerMng is %u",
                         totalThrdNum, batchNum, batchWorkerPerMng)));

    g_dispatcher->allWorkers = (PageRedoWorker **)palloc0(sizeof(PageRedoWorker *) * totalThrdNum);
    g_dispatcher->allWorkersCnt = totalThrdNum;
    g_dispatcher->pageLines = (PageRedoPipeline *)palloc(sizeof(PageRedoPipeline) * batchNum);

    for (started = 0; started < totalThrdNum; started++) {
        g_dispatcher->allWorkers[started] = CreateWorker(started, inRealtimeBuild);
        if (g_dispatcher->allWorkers[started] == NULL) {
            ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                            errmsg("[REDO_LOG_TRACE]StartPageRedoWorkers CreateWorker failed, started:%u", started)));
        }
    }
    tmpWorkers = g_dispatcher->allWorkers;
    for (uint32 i = 0; i < batchNum; i++) {
        bool isUndoSpaceWorker = false;
        if (i >= (batchNum - undoSpaceWorkersNum)) {
            isUndoSpaceWorker = true;
        }
        RedoRoleInit(&(g_dispatcher->pageLines[i].batchThd), tmpWorkers[workerCnt++], REDO_BATCH, i, isUndoSpaceWorker);
        RedoRoleInit(&(g_dispatcher->pageLines[i].managerThd), tmpWorkers[workerCnt++], REDO_PAGE_MNG, i,
            isUndoSpaceWorker);
        g_dispatcher->pageLines[i].managerThd->redoItemHashCtrl = g_instance.comm_cxt.predo_cxt.redoItemHashCtrl[i];
        RedoRoleInit(&(g_dispatcher->pageLines[i].htabThd), tmpWorkers[workerCnt++], REDO_HTAB_MNG, i,
            isUndoSpaceWorker);
        g_dispatcher->pageLines[i].redoThd = (PageRedoWorker **)palloc(sizeof(PageRedoWorker *) * batchWorkerPerMng);
        g_dispatcher->pageLines[i].chosedRTIds = (uint32 *)palloc(sizeof(uint32) * batchWorkerPerMng);
        g_dispatcher->pageLines[i].chosedRTCnt = 0;
        for (uint32 j = 0; j < batchWorkerPerMng; j++) {
            RedoRoleInit(&(g_dispatcher->pageLines[i].redoThd[j]), tmpWorkers[workerCnt++], REDO_PAGE_WORKER, j,
                isUndoSpaceWorker);
        }
        g_dispatcher->pageLines[i].redoThdNum = batchWorkerPerMng;
    }

    RedoRoleInit(&(g_dispatcher->trxnLine.managerThd), tmpWorkers[workerCnt++], REDO_TRXN_MNG, 0, false);
    RedoRoleInit(&(g_dispatcher->trxnLine.redoThd), tmpWorkers[workerCnt++], REDO_TRXN_WORKER, 0, false);

    RedoRoleInit(&(g_dispatcher->readLine.managerThd), tmpWorkers[workerCnt++], REDO_READ_MNG, 0, false);
    RedoRoleInit(&(g_dispatcher->readLine.readPageThd), tmpWorkers[workerCnt++], REDO_READ_PAGE_WORKER, 0, false);
    RedoRoleInit(&(g_dispatcher->readLine.readThd), tmpWorkers[workerCnt++], REDO_READ_WORKER, 0, false);
    RedoRoleInit(&(g_dispatcher->auxiliaryLine.segRedoThd), tmpWorkers[workerCnt++], REDO_SEG_WORKER, 0, false);
    RedoRoleInit(&(g_dispatcher->auxiliaryLine.ctrlThd), tmpWorkers[workerCnt++], REDO_CTRL_WORKER, 0, false);

    for (started = 0; started < totalThrdNum; started++) {
        if (StartPageRedoWorker(g_dispatcher->allWorkers[started]) == NULL) {
            ereport(PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]StartPageRedoWorkers StartPageRedoWorker failed, started:%u", started)));
        }
    }

    Assert(totalThrdNum == workerCnt);
    g_dispatcher->pageLineNum = batchNum;
    g_instance.comm_cxt.predo_cxt.totalNum = workerCnt;
    g_dispatcher->chosedPageLineIds = (uint32 *)palloc(sizeof(uint32) * batchNum);
    g_dispatcher->chosedPLCnt = 0;
}

static void ResetChosedPageLineList()
{
    g_dispatcher->chosedPLCnt = 0;

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        g_dispatcher->chosedPageLineIds[i] = 0;
    }
}

bool DispathCouldExit()
{
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            return false;
        }
    }

    return true;
}

void SetPageWorkStateByThreadId(uint32 threadState)
{
    gs_thread_t curThread = gs_thread_get_cur_thread();
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        if (g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId == curThread.thid) {
            pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState), threadState);
            break;
        }
    }
}

void SendSingalToPageWorker(int signal)
{
    for (uint32 i = 0; i < g_instance.comm_cxt.predo_cxt.totalNum; ++i) {
        uint32 state = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadState));
        if (state == PAGE_REDO_WORKER_READY) {
            int err = gs_signal_send(g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId, signal);
            if (0 != err) {
                ereport(WARNING, (errmsg("Dispatch kill(pid %lu, signal %d) failed: \"%s\",",
                                         g_instance.comm_cxt.predo_cxt.pageRedoThreadStatusList[i].threadId, signal,
                                         gs_strerror(err))));
            }
        }
    }
}

/* Run from the dispatcher thread. */
static void StopRecoveryWorkers(int code, Datum arg)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("parallel redo workers are going to stop, code:%d, arg:%lu",
                         code, DatumGetUInt64(arg))));
    SendSingalToPageWorker(SIGTERM);
    if (ENABLE_ONDEMAND_REALTIME_BUILD && SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status = BUILD_TO_DISABLED;
        ereport(LOG, (errmsg("[On-demand] start to shutdown realtime build, set status to BUILD_TO_DISABLED.")));
    }

    uint64 count = 0;
    while (!DispathCouldExit()) {
        ++count;
        if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(WARNING,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait page work exit")));
            if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                DumpDispatcher();
                ereport(PANIC,
                        (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("StopRecoveryWorkers wait too long!!!")));
            }
            pg_usleep(EXIT_WAIT_DELAY);
        }
    }

    pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.readWorkerState, WORKER_STATE_EXIT);
    ShutdownWalRcv();
    CloseAllXlogFileInFdCache();
    FreeAllocatedRedoItem();
    if (ENABLE_ONDEMAND_REALTIME_BUILD && SS_ONDEMAND_REALTIME_BUILD_SHUTDOWN) {
        Assert(g_dispatcher->restoreControlFile != NULL);
        RestoreControlFileForRealtimeBuild();
        g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status = DISABLED;
        ereport(LOG, (errmsg("[On-demand] realtime build shutdown, set status to DISABLED.")));
    }
    SSDestroyRecoveryWorkers();
    g_startupTriggerState = TRIGGER_NORMAL;
    g_readManagerTriggerFlag = TRIGGER_NORMAL;
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("parallel redo(startup) thread exit")));
}

/* Run from the dispatcher thread. */
static void SSDestroyRecoveryWorkers()
{
    if (g_dispatcher != NULL) {
        SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            DestroyPageRedoWorker(g_dispatcher->pageLines[i].batchThd);
            DestroyPageRedoWorker(g_dispatcher->pageLines[i].managerThd);
            DestroyPageRedoWorker(g_dispatcher->pageLines[i].htabThd);
            for (uint32 j = 0; j < g_dispatcher->pageLines[i].redoThdNum; j++) {
                DestroyPageRedoWorker(g_dispatcher->pageLines[i].redoThd[j]);
            }
            if (g_dispatcher->pageLines[i].chosedRTIds != NULL) {
                pfree(g_dispatcher->pageLines[i].chosedRTIds);
            }
        }
        DestroyPageRedoWorker(g_dispatcher->trxnLine.managerThd);
        DestroyPageRedoWorker(g_dispatcher->trxnLine.redoThd);

        DestroyPageRedoWorker(g_dispatcher->readLine.managerThd);
        DestroyPageRedoWorker(g_dispatcher->readLine.readThd);
        DestroyPageRedoWorker(g_dispatcher->auxiliaryLine.segRedoThd);
        DestroyPageRedoWorker(g_dispatcher->auxiliaryLine.ctrlThd);
        pfree(g_dispatcher->rtoXlogBufState.readBuf);
        pfree(g_dispatcher->rtoXlogBufState.errormsg_buf);
        pfree(g_dispatcher->rtoXlogBufState.readprivate);
        SPSCBlockingQueueDestroy(g_dispatcher->trxnQueue);
        SPSCBlockingQueueDestroy(g_dispatcher->segQueue);

        if (ENABLE_ONDEMAND_REALTIME_BUILD && g_dispatcher->restoreControlFile != NULL) {
            pfree(g_dispatcher->restoreControlFile);
        }
#ifdef USE_ASSERT_CHECKING
        if (g_dispatcher->originLsnCheckAddr != NULL) {
            pfree(g_dispatcher->originLsnCheckAddr);
            g_dispatcher->originLsnCheckAddr = NULL;
            g_dispatcher->lsnCheckCtl = NULL;
        }
#endif
        if (get_real_recovery_parallelism() > 1) {
            (void)MemoryContextSwitchTo(g_dispatcher->oldCtx);
            MemoryContextDelete(g_instance.comm_cxt.predo_cxt.parallelRedoCtx);
            g_instance.comm_cxt.predo_cxt.parallelRedoCtx = NULL;
        }
        
        if (g_instance.comm_cxt.predo_cxt.redoItemHashCtrl != NULL) {
            (void)MemoryContextSwitchTo(g_dispatcher->oldCtx);
            MemoryContextDelete(g_instance.comm_cxt.redoItemCtx);
            g_instance.comm_cxt.predo_cxt.redoItemHashCtrl = NULL;
        }

        g_dispatcher = NULL;
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    }
}

static bool RmgrRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    switch (XLogRecGetRmid(record)) {
        case RM_HEAP2_ID:
        case RM_HEAP_ID: {
            info = (info & XLOG_HEAP_OPMASK); 
            break;
        }
        case RM_MULTIXACT_ID: {
            info = (info & XLOG_MULTIXACT_MASK);
            break;
        }
        case RM_UHEAP_ID:
        case RM_UNDOLOG_ID:
        case RM_UHEAPUNDO_ID:
        case RM_UNDOACTION_ID: {
            info = (info & XLOG_UHEAP_OPMASK);
            break;
        }
        default:
            break;
    }

    info = (info >> XLOG_INFO_SHIFT_SIZE);
    minInfo = (minInfo >> XLOG_INFO_SHIFT_SIZE);
    maxInfo = (maxInfo >> XLOG_INFO_SHIFT_SIZE);

    if ((info >= minInfo) && (info <= maxInfo)) {
        return true;
    }
    return false;
}

static bool RmgrGistRecordInfoValid(XLogReaderState *record, uint8 minInfo, uint8 maxInfo)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if ((info == XLOG_GIST_PAGE_UPDATE) || (info == XLOG_GIST_PAGE_SPLIT) || (info == XLOG_GIST_CREATE_INDEX)) {
        return true;
    }

    return false;
}

/* Run from the dispatcher thread. */
void DispatchRedoRecordToFile(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool fatalerror = false;
    uint32 indexid = RM_NEXT_ID;

    Assert(record != NULL);

    uint32 rmid = XLogRecGetRmid(record);
    uint32 term = XLogRecGetTerm(record);
    if (term > g_instance.comm_cxt.localinfo_cxt.term_from_xlog) {
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog = term;
    }
    t_thrd.xlog_cxt.redoItemIdx = 0;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        if (rmid <= RM_MAX_ID) {
            indexid = g_dispatchTable[rmid].rm_id;
            if ((indexid != rmid) ||
                ((g_dispatchTable[rmid].rm_loginfovalid != NULL) &&
                 (g_dispatchTable[rmid].rm_loginfovalid(record, g_dispatchTable[rmid].rm_mininfo,
                                                        g_dispatchTable[rmid].rm_maxinfo) == false))) {
                /* it's invalid info */
                fatalerror = true;
            }
        } else {
            fatalerror = true;
        }
        ResetChosedPageLineList();
        if (fatalerror != true) {
#ifdef ENABLE_UT
            TestXLogReaderProbe(UTEST_EVENT_RTO_DISPATCH_REDO_RECORD_TO_FILE, __FUNCTION__, record);
#endif
            g_dispatchTable[rmid].rm_dispatch(record, expectedTLIs, recordXTime);
        } else {
            DispatchDefaultRecord(record, expectedTLIs, recordXTime);
            DumpDispatcher();
            DumpItem(GetRedoItemPtr(record), "DispatchRedoRecordToFile");
            ereport(PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]DispatchRedoRecord encounter fatal error:rmgrID:%u, info:%u, indexid:%u",
                            rmid, (uint32)XLogRecGetInfo(record), indexid)));
        }
    } else {
        ereport(PANIC,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_LOG_TRACE]DispatchRedoRecord could not be here config recovery num %d, work num %u",
                        get_real_recovery_parallelism(), GetBatchCount())));
    }
}

void UpdateCheckpointRedoPtrForPrune(XLogRecPtr prunePtr)
{
    if (SS_ONDEMAND_REALTIME_BUILD_DISABLED) {
        return;
    }

    XLogRecPtr ckptRedoPtr;
    do {
        ckptRedoPtr = pg_atomic_read_u64(&g_dispatcher->ckptRedoPtr);
    } while (XLByteLT(ckptRedoPtr, prunePtr) &&
        !pg_atomic_compare_exchange_u64(&g_dispatcher->ckptRedoPtr, &ckptRedoPtr, prunePtr));
}

/**
 * process record need sync with page worker and trxn thread
 * trxnthreadexe is true when the record need execute on trxn thread
 * pagethredexe is true when the record need execute on pageworker thread
 */
static void DispatchSyncTxnRecord(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);

    if ((g_dispatcher->chosedPLCnt != 1) && (XLogRecGetRmid(&item->record) != RM_XACT_ID)) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_LOG_TRACE]DispatchSyncTxnRecord maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                        XLogRecGetRmid(&item->record), XLogRecGetInfo(&item->record), g_dispatcher->chosedPLCnt)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }

    /* ensure eyery pageworker is receive recored to update pageworker Lsn
     * trxn record's recordtime must set , see SetLatestXTime
     */
    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, item);
    return;
}

static void DispatchToOnePageWorker(XLogReaderState *record, const RelFileNode rnode, List *expectedTLIs)
{
    /* for bcm different attr need to dispath to the same page redo thread */
    uint32 slotId = GetSlotId(rnode, 0, 0, GetBatchCount());
    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    AddPageRedoItem(g_dispatcher->pageLines[slotId].batchThd, item);
}

static void DispatchToSpecificOnePageWorker(XLogReaderState *record, uint32 slotId, List *expectedTLIs)
{
    Assert(slotId <= GetBatchCount());
    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    AddPageRedoItem(g_dispatcher->pageLines[slotId].batchThd, item);
}

/**
* The transaction worker waits until every page worker has replayed
* all records before this.  We dispatch a LSN marker to every page
* worker so they can update their progress.
*
* We need to dispatch to page workers first, because the transaction
* worker runs in the dispatcher thread and may block wait on page
* workers.
* ensure eyery pageworker is receive recored to update pageworker Lsn
* trxn record's recordtime must set , see SetLatestXTime

*/
static void DispatchTxnRecord(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *trxnItem = GetRedoItemPtr(record);
    ReferenceRedoItem(trxnItem);
    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, trxnItem);
}

/* Run  from the dispatcher thread. */
static bool DispatchBarrierRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    RedoItem *item = GetRedoItemPtr(record);
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    ReferenceRedoItem(item);
    if (info != XLOG_BARRIER_COMMIT) {
        item->record.isFullSync = true;
    }
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        ReferenceRedoItem(item);
        AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
    }

    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, item);
    return false;
}

#ifdef ENABLE_MOT
static bool DispatchMotRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return false;
}
#endif

/* Run  from the dispatcher thread. */
static bool DispatchRepSlotRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchHeap3Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_HEAP3_INVALID) {
        DispatchRecordWithPages(record, expectedTLIs);
    } else {
        DispatchTxnRecord(record, expectedTLIs);
    }
    return false;
}

/* record of rmid or info error, we inter this function to make every worker run to this position */
static bool DispatchDefaultRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return true;
}

/* Run from the dispatcher thread. */
static bool DispatchXLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (IsCheckPoint(record)) {
        RedoItem *item = GetRedoItemPtr(record);
        XLogRecPtr ckptRecordRedoPtr = GetRedoLocInCheckpointRecord(record);
        FreeRedoItem(item);
        UpdateCheckpointRedoPtrForPrune(ckptRecordRedoPtr);
        AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, &g_hashmapPruneMark);
    } else if ((info == XLOG_FPI) || (info == XLOG_FPI_FOR_HINT)) {
        DispatchRecordWithPages(record, expectedTLIs);
    } else {
        /* process in trxn thread and need to sync to other pagerredo thread */
        DispatchTxnRecord(record, expectedTLIs);
    }

    return isNeedFullSync;
}

/* Run  from the dispatcher thread. */
static bool DispatchRelMapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* page redo worker directly use relnode, will not use relmapfile */
    DispatchTxnRecord(record, expectedTLIs);
    return false;
}

/* Run  from the dispatcher thread. */
static bool DispatchXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (XactWillRemoveRelFiles(record)) {
        bool hasSegpageRelFile = XactHasSegpageRelFiles(record);
        uint32 doneFlag = 0;
        
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            AddSlotToPLSet(i);
        }
        
        if (hasSegpageRelFile) {
            doneFlag = 0;
            pg_atomic_compare_exchange_u32((volatile uint32 *)&g_dispatcher->segpageXactDoneFlag, &doneFlag, 1);
        }
        
        /* sync with trxn thread */
        /* trx execute drop action, pageworker forger invalid page,
         * pageworker first exe and update lastcomplateLSN
         * then trx thread exe
         * first pageworker execute and update lsn, then trxn thread */
        DispatchSyncTxnRecord(record, expectedTLIs);

        if (hasSegpageRelFile) {
            doneFlag = pg_atomic_read_u32(&g_dispatcher->segpageXactDoneFlag);
            while (doneFlag != 0) {
                RedoInterruptCallBack();
                doneFlag = pg_atomic_read_u32(&g_dispatcher->segpageXactDoneFlag);
            }
        }
    } else {
        /* process in trxn thread and need to sync to other pagerredo thread */
        DispatchTxnRecord(record, expectedTLIs);
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchStandbyRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* change standbystate, must be full sync, see UpdateStandbyState */
    bool isNeedFullSync = StandbyWillChangeStandbyState(record);

    DispatchTxnRecord(record, expectedTLIs);

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchMultiXactRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /* page worker will not use multixact */
    DispatchTxnRecord(record, expectedTLIs);

    return false;
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithoutPage(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        ReferenceRedoItem(item);
        AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
    }
    DereferenceRedoItem(item);
}

/* Run from the dispatcher thread. */
static void DispatchRecordWithPages(XLogReaderState *record, List *expectedTLIs)
{
    GetSlotIds(record);

    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }
    DereferenceRedoItem(item);
}

static bool DispatchHeapRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    if (record->max_block_id >= 0)
        DispatchRecordWithPages(record, expectedTLIs);
    else
        DispatchRecordWithoutPage(record, expectedTLIs);

    return false;
}

static bool DispatchSeqRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs);

    return false;
}

static bool DispatchDataBaseRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    if (IsDataBaseDrop(record)) {
        isNeedFullSync = true;
        RedoItem *item = GetRedoItemPtr(record);

        ReferenceRedoItem(item);
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
        DereferenceRedoItem(item);
    } else {
        /* database dir may impact many rel so need to sync to all pageworks */
        DispatchRecordWithoutPage(record, expectedTLIs);
        g_dispatcher->needFullSyncCheckpoint = true;
    }

    g_dispatcher->needImmediateCheckpoint = true;
    return isNeedFullSync;
}

static bool DispatchTableSpaceRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    RedoItem *item = GetRedoItemPtr(record);
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_TBLSPC_CREATE || info == XLOG_TBLSPC_RELATIVE_CREATE) {
        item->record.isFullSync = true;
    }
    ReferenceRedoItem(item);    
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; ++i) {
        ReferenceRedoItem(item);
        AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
    }
    AddTxnRedoItem(g_dispatcher->trxnLine.managerThd, item);

    g_dispatcher->needImmediateCheckpoint = true;
    return false;
}

static bool DispatchSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_SMGR_CREATE) {
        /* only need to dispatch to one page worker */
        xl_smgr_create *xlrec = (xl_smgr_create *)XLogRecGetData(record);
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
        rnode.opt = GetCreateXlogFileNodeOpt(record);
        DispatchToOnePageWorker(record, rnode, expectedTLIs);
    } else if (IsSmgrTruncate(record)) {
        xl_smgr_truncate *xlrec = (xl_smgr_truncate *)XLogRecGetData(record);
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
        rnode.opt = GetTruncateXlogFileNodeOpt(record);
        uint32 id = GetSlotId(rnode, 0, 0, GetBatchCount());
        AddSlotToPLSet(id);

        DispatchToSpecPageWorker(record, expectedTLIs);
    }

    return isNeedFullSync;
}

static void DispatchRecordBySegHeadBuffer(XLogReaderState* record, List* expectedTLIs, uint32 segHeadBlockId)
{
    RelFileNode rnode;
    BlockNumber blknum;
    XLogRecGetBlockTag(record, segHeadBlockId, &rnode, NULL, &blknum);
    rnode.relNode = blknum;

    DispatchToOnePageWorker(record, rnode, expectedTLIs);
}

static bool DispatchSegpageSmgrRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    switch (info) {
        case XLOG_SEG_ATOMIC_OPERATION:
        case XLOG_SEG_SEGMENT_EXTEND:
        case XLOG_SEG_CREATE_EXTENT_GROUP:
        case XLOG_SEG_INIT_MAPPAGE:
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
        case XLOG_SEG_ADD_NEW_GROUP:
        case XLOG_SEG_SPACE_SHRINK:
        case XLOG_SEG_SPACE_DROP:
        case XLOG_SEG_NEW_PAGE:
            DispatchToSpecificOnePageWorker(record, 0, expectedTLIs);
            break;
        case XLOG_SEG_TRUNCATE:
            DispatchRecordBySegHeadBuffer(record, expectedTLIs, 0);
            break;
        default:
            ereport(PANIC,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                errmsg("[SS][REDO_LOG_TRACE] xlog info %u doesn't belong to segpage.", info)));
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static bool DispatchRepOriginRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchCLogRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchHashRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsHashVacuumPages(record) && g_supportHotStandby) {
        GetSlotIds(record);
        /* sync with trxn thread */
        /* only need to process in pageworker  thread, wait trxn sync */
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs);
    }

    return isNeedFullSync;
}

/* for cfs row-compression. */
static bool DispatchCompresseShrinkRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return true;
}

static bool DispatchLogicalDDLMsgRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchTxnRecord(record, expectedTLIs);
    return true;
}

static bool DispatchBtreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_BTREE_REUSE_PAGE) {
        DispatchTxnRecord(record, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs);
    }

    return false;
}

static bool DispatchUBTreeRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_UBTREE_REUSE_PAGE) {
        DispatchTxnRecord(record, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs);
    }

    return false;
}

static bool DispatchUBTree2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchGinRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_GIN_DELETE_LISTPAGE) {
        ginxlogDeleteListPages *data = (ginxlogDeleteListPages *)XLogRecGetData(record);
        /* output warning */
        if (data->ndeleted != record->max_block_id) {
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_LOG_TRACE]DispatchGinRecord warnninginfo:ndeleted:%d, max_block_id:%d",
                                     data->ndeleted, record->max_block_id)));
        }
    }

    /* index not support mvcc, so we need to sync with trx thread when the record is vacuum */
    if (IsGinVacuumPages(record) && g_supportHotStandby) {
        GetSlotIds(record);
        /* sync with trxn thread */
        /* only need to process in pageworker  thread, wait trxn sync */
        /* pageworker exe, trxn don't need exe */
        DispatchToSpecPageWorker(record, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs);
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchGistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (info == XLOG_GIST_PAGE_SPLIT) {
        gistxlogPageSplit *xldata = (gistxlogPageSplit *)XLogRecGetData(record);
        /* output warning */
        if (xldata->npage != record->max_block_id) {
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                              errmsg("[REDO_LOG_TRACE]DispatchGistRecord warnninginfo:npage:%u, max_block_id:%d",
                                     xldata->npage, record->max_block_id)));
        }
    }

    DispatchRecordWithPages(record, expectedTLIs);
    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchSpgistRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    DispatchRecordWithPages(record, expectedTLIs);
    return false;
}

/**
 *  dispatch record to a specified thread
 */
static void DispatchToSpecPageWorker(XLogReaderState *record, List *expectedTLIs)
{
    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);

    if (g_dispatcher->chosedPLCnt != 1) {
        ereport(WARNING,
                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                 errmsg("[REDO_LOG_TRACE]DispatchToSpecPageWorker maybe some error:rmgrID:%u, info:%u, workerCount:%u",
                        XLogRecGetRmid(&item->record), XLogRecGetInfo(&item->record), g_dispatcher->chosedPLCnt)));
    }

    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
        }
    }

    DereferenceRedoItem(item);
}

static bool DispatchHeap2VacuumRecord(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    /*
     * don't support consistency view
     */
    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if (info == XLOG_HEAP2_CLEANUP_INFO) {
        DispatchTxnRecord(record, expectedTLIs);
    } else {
        DispatchRecordWithPages(record, expectedTLIs);
    }

    return false;
}

/* Run from the dispatcher thread. */
static bool DispatchHeap2Record(XLogReaderState *record, List *expectedTLIs, TimestampTz recordXTime)
{
    bool isNeedFullSync = false;

    uint8 info = ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) & XLOG_HEAP_OPMASK);

    if (info == XLOG_HEAP2_MULTI_INSERT) {
        DispatchRecordWithPages(record, expectedTLIs);
    } else if (info == XLOG_HEAP2_BCM) {
        /* we use renode as dispatch key, so the same relation will dispath to the same page redo thread
         * although they have different fork num
         */
        /* for parallel redo performance */
        xl_heap_bcm *xlrec = (xl_heap_bcm *)XLogRecGetData(record);
        RelFileNode tmp_node;
        RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
        DispatchToOnePageWorker(record, tmp_node, expectedTLIs);

    } else if (info == XLOG_HEAP2_LOGICAL_NEWPAGE) {
        if (IS_DN_MULTI_STANDYS_MODE()) {
            xl_heap_logical_newpage *xlrec = (xl_heap_logical_newpage *)XLogRecGetData(record);

            if (xlrec->type == COLUMN_STORE && xlrec->hasdata) {
                RelFileNode tmp_node;
                RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
                DispatchToOnePageWorker(record, tmp_node, expectedTLIs);
            } else {
                RedoItem *item = GetRedoItemPtr(record);
#ifdef USE_ASSERT_CHECKING
                ereport(LOG, (errmsg("LOGICAL NEWPAGE %X/%X type:%u, hasdata:%u no need replay",
                                     (uint32)(record->EndRecPtr >> 32), (uint32)(record->EndRecPtr),
                                     (uint32)xlrec->type, (uint32)xlrec->hasdata)));
                for (int i = 0; i <= item->record.max_block_id; ++i) {
                    if (item->record.blocks[i].in_use) {
                        item->record.blocks[i].replayed = 1;
                    }
                }
#endif
                FreeRedoItem(item);
            }
        } else {
            if (!g_instance.attr.attr_storage.enable_mix_replication) {
                isNeedFullSync = true;
                DispatchTxnRecord(record, expectedTLIs);
            } else {
                RedoItem *item = GetRedoItemPtr(record);
#ifdef USE_ASSERT_CHECKING
                ereport(LOG, (errmsg("LOGICAL NEWPAGE %X/%X not multistandby,no need replay",
                                     (uint32)(record->EndRecPtr >> 32), (uint32)(record->EndRecPtr))));
                for (int i = 0; i <= item->record.max_block_id; ++i) {
                    if (item->record.blocks[i].in_use) {
                        item->record.blocks[i].replayed = 1;
                    }
                }
#endif
                FreeRedoItem(item);
            }
        }
    } else {
        isNeedFullSync = DispatchHeap2VacuumRecord(record, expectedTLIs, recordXTime);
    }

    return isNeedFullSync;
}

/* Run from the dispatcher thread. */
static void GetSlotIds(XLogReaderState *record)
{
    for (int i = 0; i <= record->max_block_id; i++) {
        DecodedBkpBlock *block = &record->blocks[i];

        if (block->in_use) {
            uint32 id = GetSlotId(block->rnode, 0, 0, GetBatchCount());
            AddSlotToPLSet(id);
        }
    }
}

/**
 * count slot id  by hash
 */
uint32 GetSlotId(const RelFileNode node, BlockNumber block, ForkNumber forkNum, uint32 workerCount)
{
    uint32 undoSpaceWorkersNum = get_recovery_undozidworkers_num();
    workerCount = workerCount - undoSpaceWorkersNum;

    if (workerCount == 0)
        return ANY_WORKER;

    return tag_hash((const void*)&node.relNode, sizeof(node.relNode)) % workerCount;
}

/* Run from the dispatcher thread. */
static void GetUndoSlotIds(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;
    int size = 0;

    switch (op) {
        case XLOG_UHEAP_INSERT: {
            size = SizeOfUHeapInsert;
            break;
        }
        case XLOG_UHEAP_DELETE: {
            size = SizeOfUHeapDelete;
            break;
        }
        case XLOG_UHEAP_UPDATE: {
            size = SizeOfUHeapUpdate;
            break;
        }
        case XLOG_UHEAP_MULTI_INSERT: {
            size = 0;
            break;
        }
        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN: {
            /* No undo actions to redo */
            return;
        }
        default:
            ereport(ERROR, (errmsg("Invalid op in DispatchUHeapRecord")));
    }

    /* Get slot id for undo zone */
    char *xlrec = XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)(xlrec + size);
    int zoneid = UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr);
    uint32 undoSlotId = GetUndoSpaceWorkerId(zoneid);

    AddSlotToPLSet(undoSlotId);
}

static void AddSlotToPLSet(uint32 id)
{
    if (id >= g_dispatcher->pageLineNum) {
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                        errmsg("[REDO_LOG_TRACE]AddWorkerToSet:input work id error, id:%u, batch work num %u", id,
                               g_dispatcher->pageLineNum)));
        return;
    }

    if (g_dispatcher->chosedPageLineIds[id] == 0) {
        g_dispatcher->chosedPLCnt += 1;
    }
    ++(g_dispatcher->chosedPageLineIds[id]);
}

/* Run from the dispatcher thread. */
static bool StandbyWillChangeStandbyState(const XLogReaderState *record)
{
    /*
     * If standbyState has reached SNAPSHOT_READY, it will not change
     * anymore.  Otherwise, it will change if the log record's redo
     * function calls ProcArrayApplyRecoveryInfo().
     */
    if ((t_thrd.xlog_cxt.standbyState < STANDBY_SNAPSHOT_READY) && (XLogRecGetRmid(record) == RM_STANDBY_ID) &&
        ((XLogRecGetInfo(record) & (~XLR_INFO_MASK)) == XLOG_RUNNING_XACTS)) {
        /* change standbystate, must be full sync, see UpdateStandbyState */
        return true;
    }

    return false;
}

#ifdef USE_ASSERT_CHECKING
void ItemBlocksOfItemIsReplayed(RedoItem *item)
{
    for (uint32 i = 0; i <= XLR_MAX_BLOCK_ID; ++i) {
        if (item->record.blocks[i].in_use) {
            if (item->record.blocks[i].forknum == MAIN_FORKNUM) {
                Assert((item->record.blocks[i].replayed == 1));
            }
        } else {
            Assert((item->record.blocks[i].replayed == 0));
        }
    }
}

void GetLsnCheckInfo(uint64 *curPosition, XLogRecPtr *curLsn)
{
    volatile LsnCheckCtl *checkCtl = g_dispatcher->lsnCheckCtl;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u current = atomic_compare_and_swap_u128((uint128_u *)&checkCtl->curPosition);
    Assert(sizeof(checkCtl->curPosition) == sizeof(uint64));
    Assert(sizeof(checkCtl->curLsn) == sizeof(XLogRecPtr));

    *curPosition = current.u64[0];
    *curLsn = current.u64[1];
#else
    SpinLockAcquire(&checkCtl->ptrLck);
    *curPosition = checkCtl->curPosition;
    *curLsn = checkCtl->curLsn;
    SpinLockRelease(&checkCtl->ptrLck);
#endif
}

void SetLsnCheckInfo(uint64 curPosition, XLogRecPtr curLsn)
{
    volatile LsnCheckCtl *checkCtl = g_dispatcher->lsnCheckCtl;
#if defined(__x86_64__) || defined(__aarch64__)
    uint128_u exchange;

    uint128_u compare = atomic_compare_and_swap_u128((uint128_u *)&checkCtl->curPosition);
    Assert(sizeof(checkCtl->curPosition) == sizeof(uint64));
    Assert(sizeof(checkCtl->curLsn) == sizeof(XLogRecPtr));

    exchange.u64[0] = curPosition;
    exchange.u64[1] = curLsn;

    uint128_u current = atomic_compare_and_swap_u128((uint128_u *)&checkCtl->curPosition, compare, exchange);
    Assert(compare.u128 == current.u128);
#else
    SpinLockAcquire(&checkCtl->ptrLck);
    checkCtl->curPosition = curPosition;
    checkCtl->curLsn = curLsn;
    SpinLockRelease(&checkCtl->ptrLck);
#endif /* __x86_64__ */
}

bool PushCheckLsn()
{
    uint64 curPosition;
    XLogRecPtr curLsn;
    GetLsnCheckInfo(&curPosition, &curLsn);
    uint32 len = pg_atomic_read_u32(&g_dispatcher->lsnCheckCtl->lsnCheckBuf[curPosition]);

    if (len == 0) {
        return false;
    }

    // someone else changed it, no need to do it
    if (!pg_atomic_compare_exchange_u32(&g_dispatcher->lsnCheckCtl->lsnCheckBuf[curPosition], &len, 0)) {
        return false;
    }

    SetLsnCheckInfo((curPosition + len) % LSN_CHECK_BUF_SIZE, curLsn + len);
    return true;
}

void ItemLsnCheck(RedoItem *item)
{
    uint64 curPosition;
    XLogRecPtr curLsn;
    GetLsnCheckInfo(&curPosition, &curLsn);
    XLogRecPtr endPtr = item->record.EndRecPtr;
    if (endPtr % XLogSegSize == 0) {
        XLByteAdvance(endPtr, SizeOfXLogLongPHD);
    } else if (endPtr % XLOG_BLCKSZ == 0) {
        XLByteAdvance(endPtr, SizeOfXLogShortPHD);
    }
    uint32 len = (uint32)(endPtr - item->record.ReadRecPtr);

    uint64 nextPosition = (curPosition + (item->record.ReadRecPtr - curLsn)) % LSN_CHECK_BUF_SIZE;
    pg_atomic_write_u32(&g_dispatcher->lsnCheckCtl->lsnCheckBuf[nextPosition], len);

    SpinLockAcquire(&g_dispatcher->updateLck);
    while (PushCheckLsn()) {
    }
    SpinLockRelease(&g_dispatcher->updateLck);
}

void AllItemCheck()
{
    RedoItem *nextItem = g_dispatcher->allocatedRedoItem;
    while (nextItem != NULL) {
        Assert((nextItem->record.refcount == 0));
        nextItem = nextItem->allocatedNext;
    }
}

#endif

void ClearRecordInfo(XLogReaderState *xlogState)
{
    xlogState->decoded_record = NULL;
    xlogState->main_data = NULL;
    xlogState->main_data_len = 0;
    
    for (int i = 0; i <= xlogState->max_block_id; ++i) {
        xlogState->blocks[i].data = NULL;
        xlogState->blocks[i].data_len = 0;
        xlogState->blocks[i].in_use = false;
        xlogState->blocks[i].has_image = false;
        xlogState->blocks[i].has_data = false;
        xlogState->blocks[i].tdeinfo = NULL;
#ifdef USE_ASSERT_CHECKING
        xlogState->blocks[i].replayed = 0;
#endif
    }
    xlogState->max_block_id = -1;
    if (xlogState->readRecordBufSize > BIG_RECORD_LENGTH) {
        pfree(xlogState->readRecordBuf);
        xlogState->readRecordBuf = NULL;
        xlogState->readRecordBufSize = 0;
    }

    xlogState->isDecode = false;
    xlogState->isFullSync = false;
    xlogState->refcount = 0;
}

/* Run from each page worker thread. */
void FreeRedoItem(RedoItem *item)
{
    if (item->record.isDecode) {
#ifdef USE_ASSERT_CHECKING
        ItemBlocksOfItemIsReplayed(item);
        ItemLsnCheck(item);
#endif
        CountXLogNumbers(&item->record);
    }
    ClearRecordInfo(&item->record);
    pg_write_barrier();
    RedoItem *oldHead = (RedoItem *)pg_atomic_read_uintptr((uintptr_t *)&g_dispatcher->freeHead);
    do {
        pg_atomic_write_uintptr((uintptr_t *)&item->freeNext, (uintptr_t)oldHead);
    } while (!pg_atomic_compare_exchange_uintptr((uintptr_t *)&g_dispatcher->freeHead, (uintptr_t *)&oldHead,
                                                 (uintptr_t)item));
}

void InitReaderStateByOld(XLogReaderState *newState, XLogReaderState *oldState, bool isNew)
{
    if (isNew) {
        newState->read_page = oldState->read_page;
        newState->system_identifier = oldState->system_identifier;
        newState->private_data = oldState->private_data;
        newState->errormsg_buf = oldState->errormsg_buf;
        newState->isPRProcess = oldState->isPRProcess;
    }

    newState->ReadRecPtr = oldState->ReadRecPtr;
    newState->EndRecPtr = oldState->EndRecPtr;
    newState->readSegNo = oldState->readSegNo;
    newState->readOff = oldState->readOff;
    newState->readPageTLI = oldState->readPageTLI;
    newState->curReadSegNo = oldState->curReadSegNo;
    newState->curReadOff = oldState->curReadOff;
    newState->latestPagePtr = oldState->latestPagePtr;
    newState->latestPageTLI = oldState->latestPageTLI;
    newState->currRecPtr = oldState->currRecPtr;
    newState->readBuf = oldState->readBuf;
    newState->readLen = oldState->readLen;
    newState->preReadStartPtr = oldState->preReadStartPtr;
    newState->preReadBuf = oldState->preReadBuf;

    newState->decoded_record = NULL;
    newState->main_data = NULL;
    newState->main_data_len = 0;

    newState->max_block_id = -1;
    newState->readblocks = 0;
    /* move block clear to FreeRedoItem because we used MCXT_ALLOC_ZERO to alloc buf, if the variable is not init to 0,
        you should put it here. */

}

static XLogReaderState *GetXlogReader(XLogReaderState *readerState)
{
    RedoItem *newItem = NULL;
    bool isNew = false;
    uint64 count = 0;
    do {
        if (g_dispatcher->freeStateHead != NULL) {
            newItem = g_dispatcher->freeStateHead;
            g_dispatcher->freeStateHead = newItem->freeNext;
            break;
        } else {
            RedoItem *head = (RedoItem *)pg_atomic_exchange_uintptr((uintptr_t *)&g_dispatcher->freeHead,
                (uintptr_t)NULL);
            if (head != NULL) {
                pg_read_barrier();
                newItem = head;
                g_dispatcher->freeStateHead = newItem->freeNext;
                break;
            } else if (g_dispatcher->maxItemNum > g_dispatcher->curItemNum) {
                newItem = (RedoItem *)palloc_extended(MAXALIGN(sizeof(RedoItem)), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
                if (newItem != NULL) {
                    newItem->allocatedNext = g_dispatcher->allocatedRedoItem;
                    g_dispatcher->allocatedRedoItem = newItem;
                    isNew = true;
                    ++(g_dispatcher->curItemNum);
                    break;
                }
            }

            ++count;
            if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
                ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                                  errmsg("GetXlogReader Allocated record buffer failed!, cur item:%u, max item:%u",
                                         g_dispatcher->curItemNum, g_dispatcher->maxItemNum)));
                if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                    DumpDispatcher();
                }
            }
            if (newItem == NULL) {
                RedoInterruptCallBack();
            }
        }
    } while (newItem == NULL);

    InitReaderStateByOld(&newItem->record, readerState, isNew);
    newItem->freeNext = NULL;

    return &newItem->record;
}


void CopyDataFromOldReader(XLogReaderState *newReaderState, const XLogReaderState *oldReaderState)
{
    errno_t rc = EOK;
    if ((newReaderState->readRecordBuf == NULL) ||
        (oldReaderState->readRecordBufSize > newReaderState->readRecordBufSize)) {
        if (!allocate_recordbuf(newReaderState, oldReaderState->readRecordBufSize)) {
            ereport(PANIC,
                (errmodule(MOD_REDO),
                    errcode(ERRCODE_LOG),
                    errmsg("Allocated record buffer failed!, cur item:%u, max item:%u",
                        g_dispatcher->curItemNum,
                        g_dispatcher->maxItemNum)));
        }
    }

    rc = memcpy_s(newReaderState->readRecordBuf,
        newReaderState->readRecordBufSize,
        oldReaderState->readRecordBuf,
        oldReaderState->readRecordBufSize);
    securec_check(rc, "\0", "\0");
    newReaderState->decoded_record = (XLogRecord *)newReaderState->readRecordBuf;
    newReaderState->max_block_id = oldReaderState->max_block_id;
    rc = memcpy_s(newReaderState->blocks, sizeof(DecodedBkpBlock) * (XLR_MAX_BLOCK_ID + 1), oldReaderState->blocks,
        sizeof(DecodedBkpBlock) * (XLR_MAX_BLOCK_ID + 1));
    securec_check(rc, "\0", "\0");
    for (int i = 0; i <= oldReaderState->max_block_id; i++) {
        if (oldReaderState->blocks[i].has_image)
            newReaderState->blocks[i].bkp_image =
                (char *)((uintptr_t)newReaderState->decoded_record +
                         ((uintptr_t)oldReaderState->blocks[i].bkp_image - (uintptr_t)oldReaderState->decoded_record));
        if (oldReaderState->blocks[i].has_data) {
            newReaderState->blocks[i].data = (char *)((uintptr_t)newReaderState->decoded_record +
  		    ((uintptr_t)oldReaderState->blocks[i].data - (uintptr_t)oldReaderState->decoded_record));
            newReaderState->blocks[i].data_len = oldReaderState->blocks[i].data_len;
        }
    }
    if (oldReaderState->main_data_len > 0) {
        newReaderState->main_data =
            (char*)((uintptr_t)newReaderState->decoded_record +
                    ((uintptr_t)oldReaderState->main_data - (uintptr_t)oldReaderState->decoded_record));
        newReaderState->main_data_len = oldReaderState->main_data_len;
    }
}

XLogReaderState *NewReaderState(XLogReaderState *readerState)
{
    Assert(readerState != NULL);
    if (!readerState->isPRProcess)
        return readerState;
    if (DispatchPtrIsNull())
        ereport(PANIC, (errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg("NewReaderState Dispatch is null")));

    XLogReaderState *retReaderState = GetXlogReader(readerState);
    return retReaderState;
}

void FreeAllocatedRedoItem()
{
    while ((g_dispatcher != NULL) && (g_dispatcher->allocatedRedoItem != NULL)) {
        RedoItem *pItem = g_dispatcher->allocatedRedoItem;
        g_dispatcher->allocatedRedoItem = pItem->allocatedNext;
        XLogReaderState *tmpRec = &(pItem->record);

        if (tmpRec->readRecordBuf) {
            pfree(tmpRec->readRecordBuf);
            tmpRec->readRecordBuf = NULL;
        }

        pfree(pItem);
    }
}

void SendRecoveryEndMarkToWorkersAndWaitForReach(int code)
{
    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("[SS][REDO_LOG_TRACE] On-demand recovery dispatch finish, send RecoveryEndMark to workers, code: %d", 
            code)));
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        WaitPageRedoWorkerReachLastMark(g_dispatcher->readLine.readPageThd);
        PageRedoPipeline *pl = g_dispatcher->pageLines;

        /* Read finish, need to check if can go to Phase two */
        XLogRecPtr lastReadEndPtr = g_dispatcher->readLine.readPageThd->lastReplayedEndRecPtr;

        /* Wait for trxn finished replay and redo hash table complete */
        while (true) {
            XLogRecPtr trxnCompletePtr = GetCompletedRecPtr(g_dispatcher->trxnLine.redoThd);
            XLogRecPtr pageMngrCompletePtr = InvalidXLogRecPtr;
            for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
                if (g_dispatcher->allWorkers[i]->role == REDO_PAGE_MNG) {
                    XLogRecPtr tmpStart = MAX_XLOG_REC_PTR;
                    XLogRecPtr tmpEnd = MAX_XLOG_REC_PTR;
                    GetCompletedReadEndPtr(g_dispatcher->allWorkers[i], &tmpStart, &tmpEnd);
                    if (XLByteLT(tmpEnd, pageMngrCompletePtr) || pageMngrCompletePtr == InvalidXLogRecPtr) {
                        pageMngrCompletePtr = tmpEnd;
                    }
                }
            }
            ereport(DEBUG1, (errmsg("[SS][REDO_LOG_TRACE] lastReadXact: %lu, trxnComplete: %lu, pageMgrComplele: %lu",
                        lastReadEndPtr, trxnCompletePtr, pageMngrCompletePtr)));
            if (XLByteEQ(trxnCompletePtr, lastReadEndPtr) && XLByteEQ(pageMngrCompletePtr, lastReadEndPtr)) {
                break;
            }

            long sleeptime = 5 * 1000;
            pg_usleep(sleeptime);
        }
        /* we only send end mark but don't wait */
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            SendPageRedoEndMark(pl[i].batchThd);
        }
        SendPageRedoEndMark(g_dispatcher->trxnLine.managerThd);
        SendPageRedoEndMark(g_dispatcher->auxiliaryLine.ctrlThd);

        /* Stop Read Thrd only */
        pg_atomic_write_u32(&(g_dispatcher->rtoXlogBufState.xlogReadManagerState), READ_MANAGER_STOP);
        WaitPageRedoWorkerReachLastMark(g_dispatcher->readLine.managerThd);
        WaitPageRedoWorkerReachLastMark(g_dispatcher->readLine.readThd);
        WaitPageRedoWorkerReachLastMark(g_dispatcher->auxiliaryLine.ctrlThd);
        LsnUpdate();
        XLogRecPtr lastReplayed = GetXLogReplayRecPtr(NULL);
        ereport(LOG, (errmsg("[SS][REDO_LOG_TRACE] Current LastReplayed: %lu", lastReplayed)));
        (void)RegisterRedoInterruptCallBack(g_dispatcher->oldStartupIntrruptFunc);
    }
}

void WaitRedoFinish()
{
    /* make pmstate as run so db can accept service from now */
    g_instance.fatal_error = false;
    g_instance.demotion = NoDemote;
    pmState = PM_RUN;
    write_stderr_with_prefix("[On-demand] LOG: database system is ready to accept connections");

    g_instance.dms_cxt.SSRecoveryInfo.cluster_ondemand_status = CLUSTER_IN_ONDEMAND_REDO;
    /* for other nodes in cluster */
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    g_instance.dms_cxt.SSReformerControl.clusterStatus = CLUSTER_IN_ONDEMAND_REDO;
    SSUpdateReformerCtrl();
    LWLockRelease(ControlFileLock);
    SSRequestAllStandbyReloadReformCtrlPage();

    SpinLockAcquire(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
    t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandBuildDone = true;
    SpinLockRelease(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);

#ifdef USE_ASSERT_CHECKING
    XLogRecPtr minStart = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;
    GetReplayedRecPtr(&minStart, &minEnd);
    ereport(LOG, (errmsg("[SS][REDO_LOG_TRACE] Current LastReplayed: %lu", minEnd)));
#endif

    PageRedoPipeline *pl = g_dispatcher->pageLines;
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        WaitPageRedoWorkerReachLastMark(pl[i].batchThd);
    }
    WaitPageRedoWorkerReachLastMark(g_dispatcher->trxnLine.managerThd);
    XLogParseBufferDestoryFunc(&(g_dispatcher->parseManager));
    LsnUpdate();
#ifdef USE_ASSERT_CHECKING
    AllItemCheck();
#endif
    SpinLockAcquire(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
    t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandRedoDone = true;
    SpinLockRelease(&t_thrd.shemem_ptr_cxt.XLogCtl->info_lck);
}

void WaitRealtimeBuildShutdown()
{
    Assert(g_instance.pid_cxt.StartupPID != 0);
    SendPostmasterSignal(PMSIGNAL_DMS_TERM_STARTUP);
    while (true) {
        if (g_instance.pid_cxt.StartupPID == 0) {
            break;
        }
        pg_usleep(100L);
    }
}

/* Run from each page worker and the txn worker thread. */
int GetDispatcherExitCode()
{
    return (int)pg_atomic_read_u32((uint32 *)&g_dispatcher->exitCode);
}

/* Run from the dispatcher thread. */
uint32 GetAllWorkerCount()
{
    return g_dispatcher == NULL ? 0 : g_dispatcher->allWorkersCnt;
}

/* Run from the dispatcher thread. */
uint32 GetBatchCount()
{
    return g_dispatcher == NULL ? 0 : g_dispatcher->pageLineNum;
}

bool DispatchPtrIsNull()
{
    return (g_dispatcher == NULL);
}

/* Run from each page worker thread. */
PGPROC *StartupPidGetProc(ThreadId pid)
{
    if (pid == g_instance.proc_base->startupProcPid)
        return g_instance.proc_base->startupProc;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
            PGPROC *proc = GetPageRedoWorkerProc(g_dispatcher->allWorkers[i]);
            if (pid == proc->pid)
                return proc;
        }
    }
    return NULL;
}

/* Run from the dispatcher and txn worker thread. */
void UpdateStandbyState(HotStandbyState newState)
{
    PageRedoPipeline *pl = NULL;
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
            pl = &(g_dispatcher->pageLines[i]);
            UpdatePageRedoWorkerStandbyState(pl->batchThd, newState);
            UpdatePageRedoWorkerStandbyState(pl->managerThd, newState);
            UpdatePageRedoWorkerStandbyState(pl->htabThd, newState);
            for (uint32 j = 0; j < pl->redoThdNum; j++) {
                UpdatePageRedoWorkerStandbyState(pl->redoThd[j], newState);
            }
        }
        UpdatePageRedoWorkerStandbyState(g_dispatcher->trxnLine.managerThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->trxnLine.redoThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->readLine.managerThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->readLine.readPageThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->readLine.readThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->auxiliaryLine.segRedoThd, newState);
        UpdatePageRedoWorkerStandbyState(g_dispatcher->auxiliaryLine.ctrlThd, newState);
        pg_atomic_write_u32(&(g_dispatcher->standbyState), newState);
    }
}

void UpdateMinRecoveryForTrxnRedoThd(XLogRecPtr newMinRecoveryPoint)
{
    if ((get_real_recovery_parallelism() > 1) && (GetBatchCount() > 0)) {
        g_dispatcher->trxnLine.redoThd->minRecoveryPoint = newMinRecoveryPoint;
    }
}

/* Run from the dispatcher thread. */
void **GetXLogInvalidPagesFromWorkers()
{
    return CollectStatesFromWorkers(GetXLogInvalidPages);
}

/* Run from the dispatcher thread. */
static void **CollectStatesFromWorkers(GetStateFunc getStateFunc)
{
    if (g_dispatcher->allWorkersCnt > 0) {
        void **stateArray = (void **)palloc(sizeof(void *) * g_dispatcher->allWorkersCnt);
        for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++)
            stateArray[i] = getStateFunc(g_dispatcher->allWorkers[i]);
        return stateArray;
    } else
        return NULL;
}

XLogRecPtr GetSafeMinCheckPoint()
{
    XLogRecPtr minSafeCheckPoint = MAX_XLOG_REC_PTR;
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if (g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER) {
            if (XLByteLT(g_dispatcher->allWorkers[i]->lastCheckedRestartPoint, minSafeCheckPoint)) {
                minSafeCheckPoint = g_dispatcher->allWorkers[i]->lastCheckedRestartPoint;
            }
        }
    }

    return minSafeCheckPoint;
}

void GetReplayedRecPtr(XLogRecPtr *startPtr, XLogRecPtr *endPtr)
{
    XLogRecPtr minStart = MAX_XLOG_REC_PTR;
    XLogRecPtr minEnd = MAX_XLOG_REC_PTR;
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        if ((g_dispatcher->allWorkers[i]->role == REDO_PAGE_WORKER) ||
            (g_dispatcher->allWorkers[i]->role == REDO_TRXN_WORKER)) {
            XLogRecPtr tmpStart = MAX_XLOG_REC_PTR;
            XLogRecPtr tmpEnd = MAX_XLOG_REC_PTR;
            GetCompletedReadEndPtr(g_dispatcher->allWorkers[i], &tmpStart, &tmpEnd);
            if (XLByteLT(tmpEnd, minEnd)) {
                minStart = tmpStart;
                minEnd = tmpEnd;
            }
        }
    }
    *startPtr = minStart;
    *endPtr = minEnd;
}

RedoWaitInfo redo_get_io_event(int32 event_id)
{
    WaitStatisticsInfo *tmpStatics = NULL;
    RedoWaitInfo resultInfo;
    resultInfo.counter = 0;
    resultInfo.total_duration = 0;
    PgBackendStatus *beentry = NULL;
    int index = MAX_BACKEND_SLOT + StartupProcess;

    if (IS_PGSTATE_TRACK_UNDEFINE || t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        return resultInfo;
    }

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + index;
    tmpStatics = &(beentry->waitInfo.event_info.io_info[event_id - WAIT_EVENT_BUFFILE_READ]);
    resultInfo.total_duration = tmpStatics->total_duration;
    resultInfo.counter = tmpStatics->counter;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL || g_dispatcher->allWorkers == NULL || 
        g_instance.comm_cxt.predo_cxt.state != REDO_IN_PROGRESS) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        return resultInfo;
    }

    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; i++) {
        if (g_dispatcher->allWorkers[i] == NULL) {
            break;
        }
        index = g_dispatcher->allWorkers[i]->index;
        beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + index;
        tmpStatics = &(beentry->waitInfo.event_info.io_info[event_id - WAIT_EVENT_BUFFILE_READ]);
        resultInfo.total_duration += tmpStatics->total_duration;
        resultInfo.counter += tmpStatics->counter;
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));

    return resultInfo;
}

void redo_get_worker_statistic(uint32 *realNum, RedoWorkerStatsData *worker, uint32 workerLen)
{
    PageRedoWorker *redoWorker = NULL;
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        *realNum = 0;
        return;
    }
    *realNum = g_dispatcher->pageLineNum;
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        redoWorker = (g_dispatcher->pageLines[i].batchThd);
        worker[i].id = redoWorker->id;
        worker[i].queue_usage = SPSCGetQueueCount(redoWorker->queue);
        worker[i].queue_max_usage = (uint32)(pg_atomic_read_u32(&((redoWorker->queue)->maxUsage)));
        worker[i].redo_rec_count = (uint32)(pg_atomic_read_u64(&((redoWorker->queue)->totalCnt)));
    }
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
}

void make_worker_static_info(RedoWorkerTimeCountsInfo *workerCountInfo, PageRedoWorker *redoWorker,
    int piplineid, int id)
{
    const uint32 pipelineNumSize = 2;
    const uint32 redoWorkerNumSize = 2;
    const char *role_name = RedoWokerRole2Str(redoWorker->role);
    uint32 allocSize = strlen(role_name) + pipelineNumSize + 1 + redoWorkerNumSize + 1;
    workerCountInfo->worker_name = (char*)palloc0(allocSize);
    if (id != invalid_worker_id) {
        errno_t rc = sprintf_s(workerCountInfo->worker_name, allocSize, "%s%02d%02d", role_name, piplineid, id);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = sprintf_s(workerCountInfo->worker_name, allocSize, "%s%02d", role_name, piplineid);
        securec_check_ss(rc, "\0", "\0");
    }
    workerCountInfo->time_cost = redoWorker->timeCostList;
}

void redo_get_worker_time_count(RedoWorkerTimeCountsInfo **workerCountInfoList, uint32 *realNum)
{
    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.rwlock));
    knl_parallel_redo_state state = g_instance.comm_cxt.predo_cxt.state;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.rwlock));

    if (state != REDO_IN_PROGRESS) {
        *realNum = 0;
        return;
    }

    PageRedoWorker *redoWorker = NULL;

    SpinLockAcquire(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    if (g_dispatcher == NULL) {
        SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
        *realNum = 0;
        return;
    }
    *realNum = g_dispatcher->allWorkersCnt + 1;
    RedoWorkerTimeCountsInfo *workerList =
        (RedoWorkerTimeCountsInfo *)palloc0((*realNum) * sizeof(RedoWorkerTimeCountsInfo));
    errno_t rc;
    uint32 cur_pos = 0;
    uint32 allocSize;
    for (int i = 0; i < (int)g_dispatcher->pageLineNum; ++i) {
        redoWorker = (g_dispatcher->pageLines[i].batchThd);
        make_worker_static_info(&workerList[cur_pos++], redoWorker, i, invalid_worker_id);

        redoWorker = (g_dispatcher->pageLines[i].managerThd);
        make_worker_static_info(&workerList[cur_pos++], redoWorker, i, invalid_worker_id);

        redoWorker = (g_dispatcher->pageLines[i].htabThd);
        make_worker_static_info(&workerList[cur_pos++], redoWorker, i, invalid_worker_id);

        for (int j = 0; j < (int)g_dispatcher->pageLines[i].redoThdNum; ++j) {
            redoWorker = (g_dispatcher->pageLines[i].redoThd[j]);
            make_worker_static_info(&workerList[cur_pos++], redoWorker, i, j);
        }
    }

    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->trxnLine.managerThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->trxnLine.redoThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->readLine.readPageThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->readLine.readThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->readLine.managerThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->auxiliaryLine.segRedoThd, 0, invalid_worker_id);
    make_worker_static_info(&workerList[cur_pos++], g_dispatcher->auxiliaryLine.ctrlThd, 0, invalid_worker_id);

    const char *startupName = "startup";
    allocSize = strlen(startupName) + 1;
    workerList[cur_pos].worker_name = (char*)palloc0(allocSize);
    rc = sprintf_s(workerList[cur_pos].worker_name, allocSize, "%s", startupName);
    securec_check_ss(rc, "\0", "\0");
    workerList[cur_pos++].time_cost = g_dispatcher->startupTimeCost;
    SpinLockRelease(&(g_instance.comm_cxt.predo_cxt.destroy_lock));
    *workerCountInfoList = workerList;
    Assert(cur_pos == *realNum);
}

void CheckCommittingCsnList()
{
#ifndef ENABLE_MULTIPLE_NODES
    for (uint32 i = 0; i < g_dispatcher->allWorkersCnt; ++i) {
        CleanUpMakeCommitAbort(reinterpret_cast<List *>(g_dispatcher->allWorkers[i]->committingCsnList));
        g_dispatcher->allWorkers[i]->committingCsnList = NULL;
    }
#else
    TransactionId clean_xid = InvalidTransactionId;
    if (!IS_PGXC_COORDINATOR && t_thrd.proc->workingVersionNum >= DISASTER_READ_VERSION_NUM) {
        if (log_min_messages <= DEBUG4) {
            ereport(LOG, (errmsg("CheckCommittingCsnList: insert clean xlog")));
        }
        XLogBeginInsert();
        XLogRegisterData((char*)(&clean_xid), sizeof(TransactionId));
        XLogInsert(RM_STANDBY_ID, XLOG_STANDBY_CSN_ABORTED);
    }
#endif
}

/* uheap dispatch functions */
static bool DispatchUHeapRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    GetSlotIds(record);
    GetUndoSlotIds(record);

    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
    DereferenceRedoItem(item);

    return false;
}

static bool DispatchUHeap2Record(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    GetSlotIds(record);

    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
    DereferenceRedoItem(item);

    return false;
}

static bool DispatchUHeapUndoRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;
    char *opName = NULL;
    int zoneId = 0;

    switch (op) {
        case XLOG_UNDO_DISCARD: {
            undo::XlogUndoDiscard *xlrec = (undo::XlogUndoDiscard *)XLogRecGetData(record);
            zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->startSlot);
            opName = "UNDO_DISCARD";
            break;
        }
        case XLOG_UNDO_UNLINK: 
        case XLOG_SLOT_UNLINK: {
            undo::XlogUndoUnlink *xlrec = (undo::XlogUndoUnlink *)XLogRecGetData(record);
            zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->head);
            opName = "UNDO_UNLINK";
            break;
        }
        case XLOG_UNDO_EXTEND: 
        case XLOG_SLOT_EXTEND: {
            undo::XlogUndoExtend *xlrec = (undo::XlogUndoExtend *) XLogRecGetData(record);
            zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->tail);
            opName = "UNDO_ALLOCATE";
            break;
        }
        default: {
            elog(ERROR, "Invalid op in DispatchUHeapUndoRecord: %u", (uint8) op);
        }
    }

    uint32 undoWorkerId = GetUndoSpaceWorkerId(zoneId);
    AddSlotToPLSet(undoWorkerId);
    elog(DEBUG1, "Dispatch %s xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
        opName, XLogRecGetXid(record), record->EndRecPtr, zoneId, undoWorkerId);

    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
    DereferenceRedoItem(item);

    return false;
}

static bool DispatchUndoActionRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;

    switch (op) {
        case XLOG_UHEAPUNDO_RESET_SLOT: {
            elog(DEBUG1, "Dispatch UHEAPUNDO_RESET_SLOT xid(%lu) lsn(%016lx)",
                XLogRecGetXid(record), record->EndRecPtr);          
            break;
        }
        case XLOG_UHEAPUNDO_PAGE: {
            elog(DEBUG1, "Dispatch XLOG_UHEAPUNDO_PAGE xid(%lu) lsn(%016lx)",
                XLogRecGetXid(record), record->EndRecPtr);
            break;
        }
        case XLOG_UHEAPUNDO_ABORT_SPECINSERT: {
            elog(DEBUG1, "Dispatch XLOG_UHEAPUNDO_ABORT_SPECINSERT xid(%lu) lsn(%016lx)",
                XLogRecGetXid(record), record->EndRecPtr);
            break;
        }
        default: {
            elog(ERROR, "Invalid op in DispatchUndoActionRecord: %u", (uint8) op);
        }
    }

    GetSlotIds(record);

    RedoItem *item = GetRedoItemPtr(record);
    ReferenceRedoItem(item);
    for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
        if (g_dispatcher->chosedPageLineIds[i] > 0) {
            ReferenceRedoItem(item);
            AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
            elog(DEBUG1, "Dispatch page worker %d", i);
        }
    }
    DereferenceRedoItem(item);

    return false;
}

static bool DispatchRollbackFinishRecord(XLogReaderState* record, List* expectedTLIs, TimestampTz recordXTime)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    uint8 op = info & XLOG_UHEAP_OPMASK;

    switch (op) {
        case XLOG_ROLLBACK_FINISH: {
            undo::XlogRollbackFinish *xlrec = (undo::XlogRollbackFinish *)XLogRecGetData(record);
            uint32 undoWorkerId = 0;
            
            undoWorkerId = GetUndoSpaceWorkerId((int)UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr));
            AddSlotToPLSet(undoWorkerId);
            elog(DEBUG1, "Dispatch ROLLBACK_FINISH xid(%lu) lsn(%016lx) undo worker zid %d, undoWorkerId %d",
                XLogRecGetXid(record), record->EndRecPtr, (int)UNDO_PTR_GET_ZONE_ID(xlrec->slotPtr), undoWorkerId);

            RedoItem *item = GetRedoItemPtr(record);
            ReferenceRedoItem(item);
            for (uint32 i = 0; i < g_dispatcher->pageLineNum; i++) {
                if (g_dispatcher->chosedPageLineIds[i] > 0) {
                    ReferenceRedoItem(item);
                    AddPageRedoItem(g_dispatcher->pageLines[i].batchThd, item);
                    elog(DEBUG1, "Dispatch page worker %d", i);
                }
            }
            DereferenceRedoItem(item);
            break;
        }
        default: {
            elog(ERROR, "Invalid op in DispatchRollbackFinishRecord: %u", (uint8) op);
        }
    }

    return false;
}

static inline uint32 GetUndoSpaceWorkerId(int zid)
{
    uint32 workerCount = GetBatchCount();
    uint32 undoSpaceWorkersNum = get_recovery_undozidworkers_num();
    int firstUndoLogWorker = (workerCount - undoSpaceWorkersNum);

    if (workerCount == 0)
        return ANY_WORKER;

    Assert(undoSpaceWorkersNum != 0);

    return (tag_hash(&zid, sizeof(zid)) % undoSpaceWorkersNum + firstUndoLogWorker);
}

void BackupControlFileForRealtimeBuild(ControlFileData* controlFile) {
    Assert(controlFile != NULL);
    int len = sizeof(ControlFileData);
    errno_t rc = EOK;
    rc = memcpy_s(&restoreControlFile, (size_t)len, controlFile, (size_t)len);
    securec_check(rc, "", "");
}

/*
 * standby node need to read control file when realtime build start,
 * so restore control file when ondemand realtime build shutdown.
 */
static void RestoreControlFileForRealtimeBuild() {
    Assert(g_dispatcher->restoreControlFile != NULL);
    errno_t rc = EOK;
    int len = sizeof(ControlFileData);
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    rc = memcpy_s(t_thrd.shemem_ptr_cxt.ControlFile, (size_t)len, g_dispatcher->restoreControlFile, (size_t)len);
    securec_check(rc, "", "");
    LWLockRelease(ControlFileLock);
}

}  // namespace ondemand_extreme_rto
