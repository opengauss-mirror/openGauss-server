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
 * smb_analyzer.cpp
 * Parallel recovery has a centralized log dispatcher which runs inside
 * the StartupProcess.  The dispatcher is responsible for managing the
 * life cycle of PageRedoWorkers and the TxnRedoWorker, analyzing log
 * records and dispatching them to workers for processing.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/transam/smb_recovery/smb_analyzer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/standby.h"
#include "access/smb.h"
#include "access/multi_redo_api.h"
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
#include "replication/ss_disaster_cluster.h"
#include "replication/walreceiver.h"
#include "ddes/dms/ss_reform_common.h"

#include "catalog/storage_xlog.h"
#include "catalog/storage.h"

namespace smb_recovery {

const long THOUSAND_MICROSECOND = 1000;

typedef struct XLogPageReadPrivate {
    int emode;
    bool ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

typedef struct {
    void (*proc)(XLogReaderState *record);
    RmgrId id;
} RmgrAnalyzeData;

static SMBAnalyzer *CreateAnalyzer()
{
    SMBAnalyzer *tmp = (SMBAnalyzer *)palloc0(sizeof(SMBAnalyzer));
    tmp->initialServerMode = (ServerMode)t_thrd.xlog_cxt.server_mode;
    tmp->initialTimeLineID = t_thrd.xlog_cxt.ThisTimeLineID;
    tmp->expectedTLIs = t_thrd.xlog_cxt.expectedTLIs;
    tmp->recoveryTargetTLI = t_thrd.xlog_cxt.recoveryTargetTLI;
    tmp->recoveryRestoreCommand = t_thrd.xlog_cxt.recoveryRestoreCommand;
    tmp->ArchiveRecoveryRequested = t_thrd.xlog_cxt.ArchiveRecoveryRequested;
    tmp->StandbyModeRequested = t_thrd.xlog_cxt.StandbyModeRequested;
    tmp->InArchiveRecovery = t_thrd.xlog_cxt.InArchiveRecovery;
    tmp->InRecovery = t_thrd.xlog_cxt.InRecovery;
    tmp->ArchiveRestoreRequested = t_thrd.xlog_cxt.ArchiveRestoreRequested;
    tmp->minRecoveryPoint = t_thrd.xlog_cxt.minRecoveryPoint;
    tmp->standbyState = (HotStandbyState)t_thrd.xlog_cxt.standbyState;
    tmp->StandbyMode = t_thrd.xlog_cxt.StandbyMode;
    tmp->latestObservedXid = t_thrd.storage_cxt.latestObservedXid;
    tmp->DataDir = t_thrd.proc_cxt.DataDir;
    tmp->RecentXmin = u_sess->utils_cxt.RecentXmin;
    return tmp;
}

void InitSMBAly()
{
    if (t_thrd.xlog_cxt.server_mode == STANDBY_MODE || t_thrd.xlog_cxt.server_mode == PRIMARY_MODE ||
        t_thrd.xlog_cxt.server_mode == NORMAL_MODE) {
        /* start aly thread on standby or crash recovery */
        g_instance.smb_cxt.cur_lsn = t_thrd.xlog_cxt.ReadRecPtr;
        g_instance.smb_cxt.SMBAlyPID = initialize_util_thread(SMB_ALY_THREAD, CreateAnalyzer());
        g_instance.smb_cxt.SMBAlyAuxPID = initialize_util_thread(SMB_ALY_AUXILIARY_THREAD);
    }
}

static void SetUnsafe(XLogRecPtr curPtr)
{
    g_instance.smb_cxt.smb_unsafe_max_lsn = curPtr;

    /* wait redo worker */
    XLogRecPtr LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
    while (XLByteLT(LatestReplayedRecPtr, curPtr)) {
        pg_usleep(THOUSAND_MICROSECOND);
        LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
    }
}

static void SMBSetPageBackend(BufferTag tag, XLogPhyBlock pblk, XLogRecPtr endRecPtr)
{
    SMBAnalysePage item;
    uint64 tail;
    uint64 actualLoc;

    SMBAnalysePageQueue *queue = g_instance.smb_cxt.SMBAlyPageQueue;
    tail = pg_atomic_fetch_add_u64(&queue->tail, 1);
    actualLoc = tail % queue->size;

    item.tag = tag;
    item.pblk = pblk;
    item.lsn = endRecPtr;

    queue->pages[actualLoc] = item;
}

static void AddToHash(XLogReaderState *record)
{
    for (int i = 0; i <= record->max_block_id; i++) {
        BufferTag tag;
        XLogPhyBlock pblk;
        if (!XLogRecGetBlockTag(record, i, &tag.rnode, &tag.forkNum, &tag.blockNum, &pblk)) {
            continue;
        }
        SMBSetPageBackend(tag, pblk, record->EndRecPtr);
    }
}

static void ApplyRecord(XLogReaderState *record)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.smb_cxt.old_ctx);
    ApplyRedoRecord(record);
    (void)MemoryContextSwitchTo(oldCtx);
}

static void AnalyzeXLogRecord(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    if (IsCheckPoint(record)) {
        return;
    } else if ((info == XLOG_FPI) || (info == XLOG_FPI_FOR_HINT)) {
        AddToHash(record);
    } else {
        ApplyRecord(record);
    }
}

static void AnalyzeXactRecord(XLogReaderState *record)
{
    if (XactWillRemoveRelFiles(record)) {
        SetUnsafe(record->EndRecPtr);
    } else {
        ApplyRecord(record);
    }
}

static void AnalyzeSmgrRecord(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_SMGR_CREATE) {
        ApplyRecord(record);
    } else if (IsSmgrTruncate(record)) {
        SetUnsafe(record->EndRecPtr);
    }
}

static void AnalyzeCLogRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeDataBaseRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeTableSpaceRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeMultiXactRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeRelMapRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeStandbyRecord(XLogReaderState *record)
{
    // not support hot-standby
    ApplyRecord(record);
}

static void AnalyzeHeap2Record(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_FREEZE:
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_VISIBLE:
        case XLOG_HEAP2_MULTI_INSERT:
            AddToHash(record);
            break;
        case XLOG_HEAP2_CLEANUP_INFO:
            ApplyRecord(record);
            break;
        case XLOG_HEAP2_BCM:
            SetUnsafe(record->EndRecPtr);
            break;
        case XLOG_HEAP2_LOGICAL_NEWPAGE: {
            if (IS_DN_MULTI_STANDYS_MODE()) {
                ApplyRecord(record);
            } else {
                if (!g_instance.attr.attr_storage.enable_mix_replication) {
                    SetUnsafe(record->EndRecPtr);
                } else {
                    ApplyRecord(record);
                }
            }
            break;
        }
        default:
            ereport(PANIC, (errmsg("heap2_redo: unknown op code %hhu", info)));
    }
}

static void AnalyzeHeapRecord(XLogReaderState *record)
{
    if (record->max_block_id >= 0) {
        AddToHash(record);
    } else {
        ApplyRecord(record);
    }
}

static void AnalyzeBtreeRecord(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_BTREE_REUSE_PAGE) {
        ApplyRecord(record);
    } else {
        AddToHash(record);
    }
}

static void AnalyzeHashRecord(XLogReaderState *record)
{
    if (IsHashVacuumPages(record) && g_supportHotStandby) {
        SetUnsafe(record->EndRecPtr);
    } else {
        AddToHash(record);
    }
}

static void AnalyzeGinRecord(XLogReaderState *record)
{
    if (IsGinVacuumPages(record) && g_supportHotStandby) {
        SetUnsafe(record->EndRecPtr);
    } else {
        AddToHash(record);
    }
}

static void AnalyzeGistRecord(XLogReaderState *record)
{
    AddToHash(record);
}

static void AnalyzeSeqRecord(XLogReaderState *record)
{
    AddToHash(record);
}

static void AnalyzeSpgistRecord(XLogReaderState *record)
{
    AddToHash(record);
}

static void AnalyzeRepSlotRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeHeap3Record(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));
    if (info == XLOG_HEAP3_INVALID) {
        AddToHash(record);
    } else {
        ApplyRecord(record);
    }
}

static void AnalyzeBarrierRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

#ifdef ENABLE_MOT
static void AnalyzeMotRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}
#endif

static void AnalyzeUHeapRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeUHeap2Record(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeUHeapUndoRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeUndoActionRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeRollbackFinishRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeUBTreeRecord(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeUBTree2Record(XLogReaderState *record)
{
    SetUnsafe(record->EndRecPtr);
}

static void AnalyzeSegpageSmgrRecord(XLogReaderState *record)
{
    uint8 info = (XLogRecGetInfo(record) & (~XLR_INFO_MASK));

    switch (info) {
        case XLOG_SEG_ATOMIC_OPERATION:
        case XLOG_SEG_SEGMENT_EXTEND:
        case XLOG_SEG_INIT_MAPPAGE:
        case XLOG_SEG_INIT_INVRSPTR_PAGE:
        case XLOG_SEG_ADD_NEW_GROUP:
            AddToHash(record);
            break;
        case XLOG_SEG_SPACE_SHRINK:
        case XLOG_SEG_SPACE_DROP:
        case XLOG_SEG_CREATE_EXTENT_GROUP:
        case XLOG_SEG_NEW_PAGE:
        case XLOG_SEG_TRUNCATE:
            SetUnsafe(record->EndRecPtr);
            break;
        default:
            ereport(PANIC,
                    (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[SS][REDO_LOG_TRACE] xlog info %u doesn't belong to segpage.", info)));
    }
}

static void AnalyzeRepOriginRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeCompresseShrinkRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static void AnalyzeLogicalDDLMsgRecord(XLogReaderState *record)
{
    ApplyRecord(record);
}

static const RmgrAnalyzeData g_analyzeTable[RM_MAX_ID + 1] = {
    { AnalyzeXLogRecord, RM_XLOG_ID},
    { AnalyzeXactRecord, RM_XACT_ID},
    { AnalyzeSmgrRecord, RM_SMGR_ID},
    { AnalyzeCLogRecord, RM_CLOG_ID},
    { AnalyzeDataBaseRecord, RM_DBASE_ID},
    { AnalyzeTableSpaceRecord, RM_TBLSPC_ID},
    { AnalyzeMultiXactRecord, RM_MULTIXACT_ID},
    { AnalyzeRelMapRecord, RM_RELMAP_ID},
    { AnalyzeStandbyRecord, RM_STANDBY_ID},

    { AnalyzeHeap2Record, RM_HEAP2_ID},
    { AnalyzeHeapRecord, RM_HEAP_ID},
    { AnalyzeBtreeRecord, RM_BTREE_ID},
    { AnalyzeHashRecord, RM_HASH_ID},
    { AnalyzeGinRecord, RM_GIN_ID},
    /* XLOG_GIST_PAGE_DELETE is not used and info isn't continus */
    { AnalyzeGistRecord, RM_GIST_ID},
    { AnalyzeSeqRecord, RM_SEQ_ID},
    { AnalyzeSpgistRecord, RM_SPGIST_ID},
    { AnalyzeRepSlotRecord, RM_SLOT_ID},
    { AnalyzeHeap3Record, RM_HEAP3_ID},
    { AnalyzeBarrierRecord, RM_BARRIER_ID},
#ifdef ENABLE_MOT
    { AnalyzeMotRecord, RM_MOT_ID},
#endif
    { AnalyzeUHeapRecord, RM_UHEAP_ID},
    { AnalyzeUHeap2Record, RM_UHEAP2_ID},
    { AnalyzeUHeapUndoRecord, RM_UNDOLOG_ID},
    { AnalyzeUndoActionRecord, RM_UHEAPUNDO_ID},
    { AnalyzeRollbackFinishRecord, RM_UNDOACTION_ID},
    { AnalyzeUBTreeRecord, RM_UBTREE_ID},
    { AnalyzeUBTree2Record, RM_UBTREE2_ID},
    { AnalyzeSegpageSmgrRecord, RM_SEGPAGE_ID},
    { AnalyzeRepOriginRecord, RM_REPLORIGIN_ID},
    { AnalyzeCompresseShrinkRecord, RM_COMPRESSION_REL_ID},
    { AnalyzeLogicalDDLMsgRecord, RM_LOGICALDDLMSG_ID},
};

static void SMBShutdownHandler(SIGNAL_ARGS)
{
    g_instance.smb_cxt.shutdownSMBAly = true;
}

void SetSMBAnalyzerInfo(SMBAnalyzer *g_analyzer)
{
    t_thrd.xlog_cxt.server_mode = g_analyzer->initialServerMode;
    t_thrd.xlog_cxt.ThisTimeLineID = g_analyzer->initialTimeLineID;
    t_thrd.xlog_cxt.expectedTLIs = g_analyzer->expectedTLIs;
    t_thrd.xlog_cxt.standbyState = g_analyzer->standbyState;
    t_thrd.xlog_cxt.StandbyMode = g_analyzer->StandbyMode;
    t_thrd.xlog_cxt.InRecovery = true;
    t_thrd.xlog_cxt.startup_processing = true;
    t_thrd.proc_cxt.DataDir = g_analyzer->DataDir;
    u_sess->utils_cxt.RecentXmin = g_analyzer->RecentXmin;
    t_thrd.storage_cxt.latestObservedXid = g_analyzer->latestObservedXid;
    t_thrd.xlog_cxt.recoveryTargetTLI= g_analyzer->recoveryTargetTLI;
    t_thrd.xlog_cxt.recoveryRestoreCommand = g_analyzer->recoveryRestoreCommand;
    t_thrd.xlog_cxt.ArchiveRecoveryRequested = g_analyzer->ArchiveRecoveryRequested;
    t_thrd.xlog_cxt.StandbyModeRequested = g_analyzer->StandbyModeRequested;
    t_thrd.xlog_cxt.InArchiveRecovery = g_analyzer->InArchiveRecovery;
    t_thrd.xlog_cxt.InRecovery = g_analyzer->InRecovery;
    t_thrd.xlog_cxt.ArchiveRestoreRequested = g_analyzer->ArchiveRestoreRequested;
    t_thrd.xlog_cxt.minRecoveryPoint = g_analyzer->minRecoveryPoint;
    t_thrd.xlog_cxt.curFileTLI = t_thrd.xlog_cxt.ThisTimeLineID;
}

static void SetupSignalHandlers()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SMBShutdownHandler);
    (void)gspqsignal(SIGTERM, SMBShutdownHandler);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

bool IsSMBSafe(XLogRecPtr curPtr)
{
    if (!g_instance.smb_cxt.analyze_end_flag || !g_instance.smb_cxt.analyze_aux_end_flag) {
        return false;
    }
    if (g_instance.smb_cxt.smb_end_lsn == 0 || !g_instance.smb_cxt.mount_end_flag) {
        return false;
    }
    if (XLByteLT(curPtr, g_instance.smb_cxt.smb_unsafe_max_lsn)) {
        return false;
    }
    if (XLByteLT(g_instance.smb_cxt.smb_start_lsn, curPtr) && XLByteLT(curPtr, g_instance.smb_cxt.smb_end_lsn)) {
        return true;
    }
    return false;
}

static int XLogFileRead(XLogSegNo segno, int emode, TimeLineID tli, int source, bool notfoundOk)
{
    char xlogfname[MAXFNAMELEN];
    char activitymsg[MAXFNAMELEN + 16];
    char path[MAXPGPATH];
    int fd;
    errno_t errorno = EOK;

    errorno = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", tli,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    switch (source) {
        case XLOG_FROM_PG_XLOG:
        case XLOG_FROM_STREAM:
            errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", SS_XLOGDIR, tli,
                                 (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");
            t_thrd.xlog_cxt.restoredFromArchive = false;
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("invalid XLogFileRead source %d", source)));
            break;
    }

    fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
    if (fd >= 0) {
        /* Success! */
        t_thrd.xlog_cxt.curFileTLI = tli;

        /* Track source of data in assorted state variables */
        t_thrd.xlog_cxt.readSource = source;
        t_thrd.xlog_cxt.XLogReceiptSource = source;
        /* In FROM_STREAM case, caller tracks receipt time, not me */
        if (source != XLOG_FROM_STREAM) {
            t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
        }

        return fd;
    }

    if (!FILE_POSSIBLY_DELETED(errno) || !notfoundOk) { /* unexpected failure? */
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %s", path,
            XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno), TRANSLATE_ERRNO)));
    }
    return -1;
}

static XLogRecord *SMBReadRecord(XLogReaderState *xlogreader, XLogRecPtr RecPtr, int emode, bool ckpt)
{
    XLogRecord *record = NULL;
    uint32 streamFailCount = 0;
    XLogPageReadPrivate *readprivate = (XLogPageReadPrivate *)xlogreader->private_data;

    /* Pass through parameters to XLogPageRead */
    readprivate->ckpt = ckpt;
    readprivate->emode = emode;
    readprivate->randAccess = !XLByteEQ(RecPtr, InvalidXLogRecPtr);

    t_thrd.xlog_cxt.failedSources = 0;

    for (;;) {
        char *errormsg = NULL;
        if (SS_DORADO_CLUSTER) {
            /* not support yet */
            return NULL;
        } else {
            record = XLogReadRecord(xlogreader, RecPtr, &errormsg);
        }

        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;

        if (record == NULL) {
            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            if (errormsg != NULL) {
                ereport(emode_for_corrupt_record(emode, XLByteEQ(RecPtr, InvalidXLogRecPtr) ? t_thrd.xlog_cxt.EndRecPtr
                                                                                            : RecPtr),
                        (errmsg_internal("%s", errormsg) /* already translated */));
            }
        } else if ((!timeLineInHistory(xlogreader->latestPageTLI, t_thrd.xlog_cxt.expectedTLIs)) &&
                   (!(g_instance.attr.attr_storage.IsRoachStandbyCluster && dummyStandbyMode))) {
            char fname[MAXFNAMELEN];
            XLogSegNo targetSegNo;
            uint32 offset;
            errno_t errorno = EOK;

            XLByteToSeg(xlogreader->latestPagePtr, targetSegNo);
            offset = xlogreader->latestPagePtr % XLogSegSize;

            errorno = snprintf_s(fname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", xlogreader->readPageTLI,
                                 (uint32)((targetSegNo) / XLogSegmentsPerXLogId),
                                 (uint32)((targetSegNo) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");

            ereport(emode_for_corrupt_record(emode,
                                             XLByteEQ(RecPtr, InvalidXLogRecPtr) ? t_thrd.xlog_cxt.EndRecPtr : RecPtr),
                    (errmsg("unexpected timeline ID %u in log segment %s, offset %u", xlogreader->latestPageTLI, fname,
                            offset)));
            record = NULL;
        }

        if (record != NULL) {
            return record;
        } else {
            /* No valid record available from this source */
            if (streamFailCount < XLOG_STREAM_READREC_MAXTRY) {
                streamFailCount++;
                pg_usleep(XLOG_STREAM_READREC_INTERVAL);
            } else {
                t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;
            }

            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            if (xlogreader->preReadBuf != NULL) {
                xlogreader->preReadStartPtr = InvalidXlogPreReadStartPtr;
            }

            /* In standby mode, loop back to retry. Otherwise, give up. */
            if ((t_thrd.xlog_cxt.StandbyMode && !dummyStandbyMode && !g_instance.smb_cxt.trigger) ||
                SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
                continue;
            } else {
                return NULL;
            }
        }
    }
}

static int SMBXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                           char *readBuf, TimeLineID *readTLI, char* xlogPath)
{
    /* Load reader private data */
    XLogPageReadPrivate *readprivate = (XLogPageReadPrivate *)xlogreader->private_data;
    int emode = readprivate->emode;
    bool randAccess = readprivate->randAccess;
    bool ckpt = readprivate->ckpt;
    uint32 targetPageOff;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr RecPtr = targetPagePtr;
    XLogSegNo replayedSegNo;
    uint32 ret;
#ifdef USE_ASSERT_CHECKING
    XLogSegNo targetSegNo;

    XLByteToSeg(targetPagePtr, targetSegNo);
#endif
    targetPageOff = targetPagePtr % XLogSegSize;

    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if (t_thrd.xlog_cxt.readFile >= 0 && !XLByteInSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo)) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        t_thrd.xlog_cxt.readSource = 0;
    }

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

retry:
    /* See if we need to retrieve more data */
    if (t_thrd.xlog_cxt.readFile < 0 ||
        (t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM && XLByteLT(t_thrd.xlog_cxt.receivedUpto, RecPtr))) {
        if (t_thrd.xlog_cxt.StandbyMode && t_thrd.xlog_cxt.startup_processing && !dummyStandbyMode) {
            /*
             * In standby mode, wait for the requested record to become
             * available, either via restore_command succeeding to restore the
             * segment, or via walreceiver having streamed the record.
             */
            for (;;) {
                if (g_instance.smb_cxt.shutdownSMBAly) {
                    goto triggered;
                }
                if (WalRcvInProgress()) {
                    XLogRecPtr expectedRecPtr = RecPtr;
                    bool havedata = false;

                    if (t_thrd.xlog_cxt.failedSources & XLOG_FROM_STREAM) {
                        ShutdownWalRcv();
                        continue;
                    }

                    if (RecPtr % XLogSegSize == 0) {
                        XLByteAdvance(expectedRecPtr, SizeOfXLogLongPHD);
                    } else if (RecPtr % XLOG_BLCKSZ == 0) {
                        XLByteAdvance(expectedRecPtr, SizeOfXLogShortPHD);
                    }

                    if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                        havedata = true;
                    } else {
                        XLogRecPtr latestChunkStart;

                        t_thrd.xlog_cxt.receivedUpto = GetWalRcvWriteRecPtr(&latestChunkStart);
                        if (XLByteLT(expectedRecPtr, t_thrd.xlog_cxt.receivedUpto)) {
                            havedata = true;
                            if (!XLByteLT(RecPtr, latestChunkStart)) {
                                t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
                            }
                        } else {
                            havedata = false;
                        }
                    }

                    if (havedata) {
                        if (t_thrd.xlog_cxt.readFile < 0) {
                            t_thrd.xlog_cxt.readFile = XLogFileRead(t_thrd.xlog_cxt.readSegNo, PANIC,
                                                                    t_thrd.xlog_cxt.recoveryTargetTLI, XLOG_FROM_STREAM,
                                                                    false);
                            Assert(t_thrd.xlog_cxt.readFile >= 0);
                        } else {
                            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
                            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
                        }

                        break;
                    }

                    pg_memory_barrier();

                    if (WalRcvIsDone() && g_instance.smb_cxt.trigger) {
                        goto retry;
                    }

                    /*
                     * Wait for more WAL to arrive, or timeout to be reached
                     */
                    WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->SMBWakeupLatch, WL_LATCH_SET | WL_TIMEOUT, 1000L);
                    ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->SMBWakeupLatch);
                } else {
                    if (t_thrd.xlog_cxt.readFile >= 0) {
                        close(t_thrd.xlog_cxt.readFile);
                        t_thrd.xlog_cxt.readFile = -1;
                    }
                    /* Reset curFileTLI if random fetch. */
                    if (randAccess) {
                        t_thrd.xlog_cxt.curFileTLI = 0;
                    }

                    uint32 sources = XLOG_FROM_PG_XLOG;

                    t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, DEBUG2, sources);
                    if (t_thrd.xlog_cxt.readFile >= 0) {
                        break;
                    }

                    if (!RecoveryInProgress()) {
                        g_instance.smb_cxt.shutdownSMBAly = true;
                        goto triggered;
                    }

                    if (g_instance.smb_cxt.trigger) {
                        XLogRecPtr receivedUpto = GetWalRcvWriteRecPtr(NULL);
                        XLogRecPtr EndRecPtrTemp = t_thrd.xlog_cxt.EndRecPtr;
                        XLByteAdvance(EndRecPtrTemp, SizeOfXLogRecord);
                        if (XLByteLT(EndRecPtrTemp, receivedUpto)) {
                            return -1;
                        }
                        goto triggered;
                    }
                    pg_usleep(150000L);
                }
            }
        } else {
            /* In archive or crash recovery. */
            if (t_thrd.xlog_cxt.readFile < 0) {
                uint32 sources;

                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                sources = XLOG_FROM_PG_XLOG;

                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, emode, sources);

                if (t_thrd.xlog_cxt.readFile < 0) {
                    return -1;
                }
            }
        }
    }

    /*
     * At this point, we have the right segment open and if we're streaming we
     * know the requested record is in it.
     */
    Assert(t_thrd.xlog_cxt.readFile != -1);

    /*
     * If the current segment is being streamed from master, calculate how
     * much of the current page we have received already. We know the
     * requested record has been received, but this is for the benefit of
     * future calls, to allow quick exit at the top of this function.
     */
    if (t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM) {
        if ((targetPagePtr / XLOG_BLCKSZ) != (t_thrd.xlog_cxt.receivedUpto / XLOG_BLCKSZ)) {
            t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
        } else {
            t_thrd.xlog_cxt.readLen = t_thrd.xlog_cxt.receivedUpto % XLogSegSize - targetPageOff;
        }
    } else {
        t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
    }

    /* Read the requested page */
    t_thrd.xlog_cxt.readOff = targetPageOff;

try_again:
    if (lseek(t_thrd.xlog_cxt.readFile, (off_t)t_thrd.xlog_cxt.readOff, SEEK_SET) < 0) {
        ereport(emode_for_corrupt_record(emode, RecPtr),
                (errcode_for_file_access(),
                    errmsg("could not seek in log file %s to offset %u: %s",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff, TRANSLATE_ERRNO)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    pgstat_report_waitevent(WAIT_EVENT_WAL_READ);
    ret = read(t_thrd.xlog_cxt.readFile, readBuf, XLOG_BLCKSZ);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (ret != XLOG_BLCKSZ) {
        ereport(emode_for_corrupt_record(emode, RecPtr),
                (errcode_for_file_access(),
                    errmsg("could not read from log file %s to offset %u: %s",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff, TRANSLATE_ERRNO)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    Assert(targetSegNo == t_thrd.xlog_cxt.readSegNo);
    Assert(targetPageOff == t_thrd.xlog_cxt.readOff);
    Assert((uint32)reqLen <= t_thrd.xlog_cxt.readLen);

    *readTLI = t_thrd.xlog_cxt.curFileTLI;

    return t_thrd.xlog_cxt.readLen;

next_record_is_invalid:
    t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;

    /* In standby-mode, keep trying */
    if (t_thrd.xlog_cxt.StandbyMode) {
        goto retry;
    } else {
        return -1;
    }

triggered:
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;
    t_thrd.xlog_cxt.recoveryTriggered = true;

    return -1;
}

void SMBAnalysisMain(void)
{
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = NULL;
    XLogPageReadPrivate readprivate;

    ereport(LOG, (errmsg("SMB ALY Start.")));
    pgstat_report_appname("SMB ALY");
    pgstat_report_activity(STATE_IDLE, NULL);
    SetupSignalHandlers();
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "SMBAnalyzerThread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.role = SMB_ALY_THREAD;

    OwnLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->SMBWakeupLatch);
    ResourceManagerStartup();
    InitRecoveryLockHash();
    EnableSyncRequestForwarding();

    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "SMBAnalysis",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    g_instance.smb_cxt.ctx = ctx;
    g_instance.smb_cxt.old_ctx = MemoryContextSwitchTo(g_instance.smb_cxt.ctx);
    SMBAlyMemInit();
    g_instance.smb_cxt.aly_mem_init_flag = true;

    if (SS_PRIMARY_MODE) {
        xlogreader = SSXLogReaderAllocate(&SSXLogPageRead, &readprivate, ALIGNOF_BUFFER);
    } else {
        xlogreader = XLogReaderAllocate(&SMBXLogPageRead, &readprivate);
    }
    if (xlogreader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor")));
    }
    xlogreader->system_identifier = t_thrd.shemem_ptr_cxt.ControlFile->system_identifier;

    record = SMBReadRecord(xlogreader, g_instance.smb_cxt.cur_lsn, LOG, false);
    while (record != NULL) {
        if (g_instance.smb_cxt.shutdownSMBAly) {
            break;
        }

        g_analyzeTable[XLogRecGetRmid(xlogreader)].proc(xlogreader);
        record = SMBReadRecord(xlogreader, InvalidXLogRecPtr, LOG, false);
    }
    XLogReaderFree(xlogreader);
    xlogreader = NULL;

    DisownLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->SMBWakeupLatch);
    (void)MemoryContextSwitchTo(g_instance.smb_cxt.old_ctx);
    StandbyReleaseAllLocks();
    ResourceManagerStop();

    XLogRecPtr LatestReplayedRecPtr = GetXLogReplayRecPtr(NULL);
    ereport(LOG, (errmsg("SMB analyze done, start lsn: %lu, last lsn: %lu, cur_redo_lsn: %lu, max_unsafe_lsn: %lu.",
        g_instance.smb_cxt.cur_lsn, t_thrd.xlog_cxt.EndRecPtr,
        LatestReplayedRecPtr, g_instance.smb_cxt.smb_unsafe_max_lsn)));
    // tell SMBWriter to wake up
    if (!g_instance.smb_cxt.shutdownSMBAly) {
        g_instance.smb_cxt.analyze_end_flag = true;
        while (!g_instance.smb_cxt.analyze_aux_end_flag) {
            pg_usleep(10000L);
        }
    }
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    g_instance.smb_cxt.SMBAlyPID = 0;
    ereport(LOG, (errmsg("SMB ALY End.")));
    proc_exit(0);
}

} // namespace smb_recovery