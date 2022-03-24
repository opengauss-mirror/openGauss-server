/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * knl_thread.cpp
 *    Initial functions for thread level global variables.
 *
 *  The thread level variables will be inited at the start up time of one
 *  thread, to be specific, at the beginning of SubPostmasterMain(). When
 *  anyone try to add variable in thread level context, remember to add
 *  initialization at this file.
 *
 *  And be very careful to alloc memory here, the memory will be alloced from
 *  TopMemoryContext and will not be freed until the thread exit.
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/process/threadpool/knl_thread.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <c.h>
#include <libaio.h>

#include "access/gin_private.h"
#include "access/reloptions.h"
#include "access/slru.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "executor/instrument.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_thread.h"
#include "optimizer/cost.h"
#include "optimizer/dynsmp.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "parser/scanner.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "regex/regex.h"
#include "replication/dataprotocol.h"
#include "replication/walprotocol.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/predicate_internals.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "utils/be_module.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/plog.h"
#include "utils/plpgsql.h"
#include "utils/postinit.h"
#include "utils/relmapper.h"
#include "workload/workload.h"

THR_LOCAL knl_thrd_context t_thrd;

extern void temp_file_context_init(knl_t_libpq_context* libpq_cxt);

static void knl_t_aes_init(knl_t_aes_context* aes_cxt)
{
    size_t arrlen = sizeof(GS_UCHAR) * RANDOM_LEN;
    errno_t rc;

    aes_cxt->encryption_function_call = false;
    aes_cxt->decryption_function_call = false;
    aes_cxt->decryption_count = 0;
    aes_cxt->insert_position = NUMBER_OF_SAVED_DERIVEKEYS / 2;
    aes_cxt->random_salt_tag = false;
    aes_cxt->random_salt_count = 0;

    rc = memset_s(aes_cxt->derive_vector_saved, arrlen, 0, arrlen);
    securec_check(rc, "\0", "\0");
    rc = memset_s(aes_cxt->mac_vector_saved, arrlen, 0, arrlen);
    securec_check(rc, "\0", "\0");
    rc = memset_s(aes_cxt->input_saved, arrlen, 0, arrlen);
    securec_check(rc, "\0", "\0");
    rc = memset_s(aes_cxt->random_salt_saved, arrlen, 0, arrlen);
    securec_check(rc, "\0", "\0");
    rc = memset_s(aes_cxt->gs_random_salt_saved, arrlen, 0, arrlen);
    securec_check(rc, "\0", "\0");
    rc = memset_s(aes_cxt->usage_frequency,
        sizeof(GS_UINT32) * NUMBER_OF_SAVED_DERIVEKEYS,
        0,
        sizeof(GS_UINT32) * NUMBER_OF_SAVED_DERIVEKEYS);
    securec_check(rc, "\0", "\0");

    for (int i = 0; i < NUMBER_OF_SAVED_DERIVEKEYS; i++) {
        rc = memset_s(aes_cxt->derive_vector_used[i], arrlen, 0, arrlen);
        securec_check(rc, "\0", "\0");
        rc = memset_s(aes_cxt->mac_vector_used[i], arrlen, 0, arrlen);
        securec_check(rc, "\0", "\0");
        rc = memset_s(aes_cxt->random_salt_used[i], arrlen, 0, arrlen);
        securec_check(rc, "\0", "\0");
        rc = memset_s(aes_cxt->user_input_used[i], arrlen, 0, arrlen);
        securec_check(rc, "\0", "\0");
    }
}

static void knl_t_codegen_init(knl_t_codegen_context* codegen_cxt)
{
    codegen_cxt->thr_codegen_obj = NULL;
    codegen_cxt->g_runningInFmgr = false;
    codegen_cxt->codegen_IRload_thr_count = 0;
}

static void knl_t_format_init(knl_t_format_context* format_cxt)
{
    format_cxt->all_digits = false;
    format_cxt->DCH_cache = (DCHCacheEntry*)palloc0(sizeof(DCHCacheEntry) * (DCH_CACHE_FIELDS + 1));
    format_cxt->n_DCH_cache = 0;
    format_cxt->DCH_counter = 0;
    format_cxt->NUM_cache = (NUMCacheEntry*)palloc0(sizeof(NUMCacheEntry) * (NUM_CACHE_FIELDS + 1));
    format_cxt->n_NUM_cache = 0;
    format_cxt->NUM_counter = 0;
    format_cxt->last_NUM_cache_entry = NULL;
}

static void knl_t_log_init(knl_t_log_context* log_cxt)
{
    log_cxt->plog_msg_switch_tm = {1, 0};
    log_cxt->plog_md_read_entry = NULL;
    log_cxt->plog_md_write_entry = NULL;
    log_cxt->plog_obs_list_entry = NULL;
    log_cxt->plog_obs_read_entry = NULL;
    log_cxt->plog_obs_write_entry = NULL;
    log_cxt->plog_hdp_read_entry = NULL;
    log_cxt->plog_hdp_write_entry = NULL;
    log_cxt->plog_hdp_open_entry = NULL;
    log_cxt->plog_remote_read_entry = NULL;
    log_cxt->g_plog_msgmem_array = (char***)palloc0(sizeof(char**) * DS_VALID_NUM);
    for (int i = 0; i < DSRQ_VALID_NUM; i++) {
        log_cxt->g_plog_msgmem_array[i] = (char**)palloc0(sizeof(char*) * DSRQ_VALID_NUM);
    }

    log_cxt->error_context_stack = NULL;
    log_cxt->call_stack = NULL;
    log_cxt->PG_exception_stack = NULL;
    log_cxt->thd_bt_symbol = NULL;
    log_cxt->flush_message_immediately = false;
    log_cxt->Log_destination = LOG_DESTINATION_STDERR;
    log_cxt->disable_log_output = false;
    log_cxt->on_mask_password = false;
    log_cxt->openlog_done = false;
    log_cxt->error_with_nodename = false;
    log_cxt->errordata = (ErrorData*)palloc0(sizeof(ErrorData) * ERRORDATA_STACK_SIZE);
    log_cxt->pLogCtl = NULL;
    log_cxt->errordata_stack_depth = -1;
    log_cxt->recursion_depth = 0;
    log_cxt->syslog_seq = 0;
    log_cxt->log_line_number = 0;
    log_cxt->log_my_pid = 0;
    log_cxt->csv_log_line_number = 0;
    log_cxt->csv_log_my_pid = 0;
    log_cxt->msgbuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
    log_cxt->module_logging_configure = (unsigned char*)palloc0(sizeof(char) * BEMD_BITMAP_SIZE);
}

static void knl_t_relopt_init(knl_t_relopt_context* relopt_cxt)
{
    relopt_cxt->relOpts = NULL;
    relopt_cxt->last_assigned_kind = RELOPT_KIND_LAST_DEFAULT;
    relopt_cxt->num_custom_options = 0;
    relopt_cxt->custom_options = NULL;
    relopt_cxt->need_initialization = true;
    relopt_cxt->max_custom_options = 0;
}

static void knl_t_cstore_init(knl_t_cstore_context* cstore_cxt)
{
    cstore_cxt->gCStoreAlterReg = NULL;
    cstore_cxt->bulkload_memsize_used = 0;
    cstore_cxt->cstore_prefetch_count = 0;
    cstore_cxt->InProgressAioCUDispatch = NULL;
    cstore_cxt->InProgressAioCUDispatchCount = 0;
}

static void knl_t_dfs_init(knl_t_dfs_context* dfs_cxt)
{
    dfs_cxt->pending_free_reader_list = NIL;
    dfs_cxt->pending_free_writer_list = NIL;
}

static void knl_t_obs_init(knl_t_obs_context* obs_cxt)
{
    obs_cxt->ObsMemoryContext = NULL;
    obs_cxt->pCAInfo = NULL;
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif
    obs_cxt->retrySleepInterval = 1 * SLEEP_UNITS_PER_SECOND;
}

static void knl_t_cbm_init(knl_t_cbm_context* cbm_cxt)
{
    cbm_cxt->XlogCbmSys = NULL;

    cbm_cxt->got_SIGHUP = false;
    cbm_cxt->shutdown_requested = false;
    cbm_cxt->cbmwriter_context = NULL;
    cbm_cxt->cbmwriter_page_context = NULL;
}

static void knl_t_shemem_ptr_init(knl_t_shemem_ptr_context* shemem_ptr_cxt)
{
    shemem_ptr_cxt->scan_locations = NULL;
    shemem_ptr_cxt->MultiXactOffsetCtl = (SlruCtlData*)palloc0(sizeof(SlruCtlData));
    shemem_ptr_cxt->MultiXactMemberCtl = (SlruCtlData*)palloc0(sizeof(SlruCtlData));
    shemem_ptr_cxt->MultiXactState = NULL;
    shemem_ptr_cxt->OldestMemberMXactId = NULL;
    shemem_ptr_cxt->OldestVisibleMXactId = NULL;
    shemem_ptr_cxt->ClogCtl = NULL;
    shemem_ptr_cxt->CsnlogCtlPtr = NULL;
    shemem_ptr_cxt->XLogCtl = NULL;
    shemem_ptr_cxt->GlobalWALInsertLocks = NULL;
    shemem_ptr_cxt->LocalGroupWALInsertLocks = NULL;
    shemem_ptr_cxt->ControlFile = NULL;
    shemem_ptr_cxt->g_LsnXlogFlushChkFile = NULL;
    shemem_ptr_cxt->OldSerXidSlruCtl = (SlruCtlData*)palloc0(sizeof(SlruCtlData));
    shemem_ptr_cxt->oldSerXidControl = NULL;
    shemem_ptr_cxt->OldCommittedSxact = NULL;
    shemem_ptr_cxt->PredXact = NULL;
    shemem_ptr_cxt->RWConflictPool = NULL;
    shemem_ptr_cxt->SerializableXidHash = NULL;
    shemem_ptr_cxt->PredicateLockTargetHash = NULL;
    shemem_ptr_cxt->PredicateLockHash = NULL;
    shemem_ptr_cxt->FinishedSerializableTransactions = NULL;

    shemem_ptr_cxt->BackendStatusArray = NULL;
    shemem_ptr_cxt->BackendClientHostnameBuffer = NULL;
    shemem_ptr_cxt->BackendAppnameBuffer = NULL;
    shemem_ptr_cxt->BackendConninfoBuffer = NULL;
    shemem_ptr_cxt->BackendActivityBuffer = NULL;
    shemem_ptr_cxt->WaitCountBuffer = NULL;
    shemem_ptr_cxt->BackendActivityBufferSize = 0;

    shemem_ptr_cxt->MyBEEntry = NULL;

    shemem_ptr_cxt->mySessionStatEntry = NULL;
    shemem_ptr_cxt->sessionStatArray = NULL;
    shemem_ptr_cxt->mySessionMemoryEntry = NULL;
    shemem_ptr_cxt->sessionMemoryArray = NULL;

    shemem_ptr_cxt->mySessionTimeEntry = NULL;
    shemem_ptr_cxt->sessionTimeArray = NULL;

    shemem_ptr_cxt->ProcSignalSlots = NULL;
    shemem_ptr_cxt->MyProcSignalSlot = NULL;
    shemem_ptr_cxt->ShmemSegHdr = NULL;
    shemem_ptr_cxt->ShmemBase = NULL;
    shemem_ptr_cxt->ShmemEnd = NULL;
    shemem_ptr_cxt->ShmemLock = NULL;
    shemem_ptr_cxt->ShmemIndex = NULL;
    shemem_ptr_cxt->shmInvalBuffer = NULL;
    shemem_ptr_cxt->PMSignalState = NULL;
    shemem_ptr_cxt->mainLWLockArray = NULL;
}

static void knl_t_xact_init(knl_t_xact_context* xact_cxt)
{
    errno_t rc;

    /* init var in transam.cpp */
    xact_cxt->cachedFetchCSNXid = InvalidTransactionId;
    xact_cxt->cachedFetchCSN = 0;
    xact_cxt->latestFetchCSNXid = InvalidTransactionId;
    xact_cxt->latestFetchCSN = 0;
    xact_cxt->latestFetchXid = InvalidTransactionId;
    xact_cxt->latestFetchXidStatus = 0;
    xact_cxt->cachedFetchXid = InvalidTransactionId;
    xact_cxt->cachedFetchXidStatus = 0;
    xact_cxt->cachedCommitLSN = 0;

    /* init var in multixact.cpp */
    xact_cxt->MXactCache = NULL;
    xact_cxt->MXactContext = NULL;

    /* init var in twophase.cpp */
    xact_cxt->MyLockedGxact = NULL;
    xact_cxt->twophaseExitRegistered = false;
    xact_cxt->cached_xid = InvalidTransactionId;
    xact_cxt->cached_gxact = NULL;
    xact_cxt->TwoPhaseState = NULL;
    rc = memset_s(&xact_cxt->records, sizeof(xllist), 0, sizeof(xllist));
    securec_check(rc, "\0", "\0");

    /* init var in varsup.cpp */
#define FirstBootstrapObjectId 10000
    xact_cxt->cn_xid = InvalidTransactionId;
    xact_cxt->next_xid = InvalidTransactionId;
    xact_cxt->force_get_xid_from_gtm = false;
    xact_cxt->InplaceUpgradeNextOid = FirstBootstrapObjectId;
    xact_cxt->ShmemVariableCache = NULL;

    /* init var in xact.cpp */
    xact_cxt->CancelStmtForReadOnly = false;
    xact_cxt->MyXactAccessedTempRel = false;
    xact_cxt->MyXactAccessedRepRel = false;
    xact_cxt->needRemoveTwophaseState = false;
    xact_cxt->bInAbortTransaction = false;
    xact_cxt->handlesDestroyedInCancelQuery = false;
    xact_cxt->xactDelayDDL = false;
    xact_cxt->nUnreportedXids = 0;
    xact_cxt->currentSubTransactionId = TopSubTransactionId;
    xact_cxt->currentCommandId = FirstCommandId;
    xact_cxt->currentCommandIdUsed = false;
    xact_cxt->isCommandIdReceived = false;
    xact_cxt->sendCommandId = false;
    xact_cxt->receivedCommandId = FirstCommandId;
    xact_cxt->xactStartTimestamp = 0;
    xact_cxt->stmtStartTimestamp = 0;
    xact_cxt->xactStopTimestamp = 0;
    xact_cxt->GTMxactStartTimestamp = 0;
    xact_cxt->GTMdeltaTimestamp = 0;
    xact_cxt->timestamp_from_cn = false;
    xact_cxt->XactLocalNodePrepared = false;
    xact_cxt->XactReadLocalNode = false;
    xact_cxt->XactWriteLocalNode = false;
    xact_cxt->XactLocalNodeCanAbort = true;
    xact_cxt->XactPrepareSent = false;
    xact_cxt->AlterCoordinatorStmt = false;
    xact_cxt->forceSyncCommit = false;
    /* alloc in TopMemory Context, initialization is NULL when create new thread */
    xact_cxt->TransactionAbortContext = NULL;
    xact_cxt->Seq_callbacks = NULL;
    xact_cxt->lxid = InvalidTransactionId;
    xact_cxt->stablexid = InvalidTransactionId;

    /* init var in gtm.cpp */
    xact_cxt->currentGxid = InvalidTransactionId;
    xact_cxt->conn = NULL;

    /* init var in slru.cpp */
    xact_cxt->slru_errcause = SLRU_MAX_FAILED;
    xact_cxt->slru_errno = 0;

    /* init var in predicate.cpp */
    xact_cxt->ScratchTargetTagHash = 0;
    xact_cxt->ScratchPartitionLock = NULL;
    xact_cxt->LocalPredicateLockHash = NULL;
    xact_cxt->MySerializableXact = InvalidSerializableXact;
    xact_cxt->MyXactDidWrite = false;

    xact_cxt->useLocalSnapshot = false;

    xact_cxt->PGXCBucketMap = NULL;
    xact_cxt->PGXCBucketCnt = 0;
    xact_cxt->PGXCGroupOid = InvalidOid;
    xact_cxt->PGXCNodeId = -1;
    xact_cxt->inheritFileNode = false;
    xact_cxt->applying_subxact_undo = false;
    xact_cxt->XactXidStoreForCheck = InvalidTransactionId;
    xact_cxt->enable_lock_cancel = false;
    xact_cxt->ActiveLobRelid = InvalidOid;
    xact_cxt->isSelectInto = false;
}

static void knl_t_mem_init(knl_t_mem_context* mem_cxt)
{
    mem_cxt->postmaster_mem_cxt = NULL;
    mem_cxt->msg_mem_cxt = NULL;
    mem_cxt->cur_transaction_mem_cxt = NULL;
    mem_cxt->gs_signal_mem_cxt = NULL;
    mem_cxt->mask_password_mem_cxt = NULL;
    mem_cxt->row_desc_mem_cxt = NULL;
    mem_cxt->portal_mem_cxt = NULL;
    mem_cxt->mem_track_mem_cxt = NULL;
    mem_cxt->batch_encode_numeric_mem_cxt = NULL;
    mem_cxt->pgAuditLocalContext = NULL;
}

static void knl_t_xlog_init(knl_t_xlog_context* xlog_cxt)
{
    errno_t rc;

    xlog_cxt->ThisTimeLineID = 0;
    xlog_cxt->InRecovery = false;
    xlog_cxt->standbyState = STANDBY_DISABLED;
    xlog_cxt->LastRec = 0;
    xlog_cxt->latestRecordCrc = 0;
    xlog_cxt->lastFullPageWrites = false;
    xlog_cxt->LocalRecoveryInProgress = true;
    xlog_cxt->LocalHotStandbyActive = false;
    xlog_cxt->LocalXLogInsertAllowed = -1;
    xlog_cxt->ArchiveRecoveryRequested = false;
    xlog_cxt->InArchiveRecovery = false;
    xlog_cxt->ArchiveRestoreRequested = false;
    xlog_cxt->restoredFromArchive = false;
    xlog_cxt->recoveryRestoreCommand = NULL;
    xlog_cxt->recoveryEndCommand = NULL;
    xlog_cxt->archiveCleanupCommand = NULL;
    xlog_cxt->recoveryTarget = RECOVERY_TARGET_UNSET;
    xlog_cxt->recoveryTargetInclusive = true;
    xlog_cxt->recoveryPauseAtTarget = true;
    xlog_cxt->recoveryTargetXid = InvalidTransactionId;
    xlog_cxt->recoveryTargetTime = 0;
    xlog_cxt->obsRecoveryTargetTime = NULL;
    xlog_cxt->recoveryTargetBarrierId = NULL;
    xlog_cxt->recoveryTargetName = NULL;
    xlog_cxt->recoveryTargetLSN = InvalidXLogRecPtr;
    xlog_cxt->isRoachSingleReplayDone = false;
    xlog_cxt->StandbyModeRequested = false;
    xlog_cxt->PrimaryConnInfo = NULL;
    xlog_cxt->TriggerFile = NULL;
    xlog_cxt->StandbyMode = false;
    xlog_cxt->recoveryTriggered = false;
    xlog_cxt->recoveryStopXid = InvalidTransactionId;
    xlog_cxt->recoveryStopTime = 0;
    xlog_cxt->recoveryStopLSN = InvalidXLogRecPtr;
    rc = memset_s(xlog_cxt->recoveryStopName, MAXFNAMELEN * sizeof(char), 0, MAXFNAMELEN * sizeof(char));
    securec_check(rc, "\0", "\0");
    xlog_cxt->recoveryStopAfter = false;
    xlog_cxt->recoveryTargetTLI = 0;
    xlog_cxt->recoveryTargetIsLatest = false;
    xlog_cxt->expectedTLIs = NIL;
    xlog_cxt->curFileTLI = 0;
    xlog_cxt->ProcLastRecPtr = InvalidXLogRecPtr;
    xlog_cxt->XactLastRecEnd = InvalidXLogRecPtr;
    xlog_cxt->XactLastCommitEnd = InvalidXLogRecPtr;
    xlog_cxt->RedoRecPtr = InvalidXLogRecPtr;
    xlog_cxt->doPageWrites = false;
    xlog_cxt->RedoStartLSN = InvalidXLogRecPtr;
    xlog_cxt->server_mode = UNKNOWN_MODE;
    xlog_cxt->is_cascade_standby = false;
    xlog_cxt->is_hadr_main_standby = false;
    xlog_cxt->startup_processing = false;
    xlog_cxt->openLogFile = -1;
    xlog_cxt->openLogSegNo = 0;
    xlog_cxt->openLogOff = 0;
    xlog_cxt->readFile = -1;
    xlog_cxt->readSegNo = 0;
    xlog_cxt->readOff = 0;
    xlog_cxt->readLen = 0;
    xlog_cxt->readSource = 0;
    xlog_cxt->failedSources = 0;
    xlog_cxt->XLogReceiptTime = 0;
    xlog_cxt->XLogReceiptSource = 0;
    xlog_cxt->ReadRecPtr = 0;
    xlog_cxt->EndRecPtr = 0;
    xlog_cxt->minRecoveryPoint = 0;
    xlog_cxt->updateMinRecoveryPoint = true;
    xlog_cxt->reachedConsistency = false;
    xlog_cxt->InRedo = false;
    xlog_cxt->RedoDone = false;
    xlog_cxt->bgwriterLaunched = false;
    xlog_cxt->pagewriter_launched = false;
    xlog_cxt->MyLockNo = 0;
    xlog_cxt->holdingAllLocks = false;
    xlog_cxt->lockToTry = -1;
    xlog_cxt->cachedPage = 0;
    xlog_cxt->cachedPos = NULL;
#ifdef WIN32
    xlog_cxt->deletedcounter = 1;
#endif
    rc = memset_s(xlog_cxt->buf, STR_TIME_BUF_LEN, 0, STR_TIME_BUF_LEN);
    securec_check(rc, "\0", "\0");

    xlog_cxt->receivedUpto = InvalidXLogRecPtr;
    xlog_cxt->lastComplaint = InvalidXLogRecPtr;
    xlog_cxt->failover_triggered = false;
    xlog_cxt->switchover_triggered = false;
    xlog_cxt->registered_buffers = NULL;
    xlog_cxt->max_registered_buffers = 0;
    xlog_cxt->max_registered_block_id = 0;
    xlog_cxt->mainrdata_head = NULL;
    xlog_cxt->mainrdata_last = NULL;
    xlog_cxt->mainrdata_len = 0;
    xlog_cxt->ptr_hdr_rdt = (XLogRecData*)palloc0(sizeof(XLogRecData));
    xlog_cxt->hdr_scratch = NULL;
    xlog_cxt->rdatas = NULL;
    xlog_cxt->num_rdatas = 0;
    xlog_cxt->max_rdatas = 0;
    xlog_cxt->begininsert_called = false;
    xlog_cxt->include_origin = false;
    xlog_cxt->xloginsert_cxt = NULL;
    xlog_cxt->invalid_page_tab = NULL;
    xlog_cxt->remain_segs = NULL;
    xlog_cxt->sendId = 0;
    xlog_cxt->sendFile = -1;
    xlog_cxt->sendSegNo = 0;
    xlog_cxt->sendOff = 0;
    xlog_cxt->sendTLI = 0;
    xlog_cxt->incomplete_actions = NIL;
    xlog_cxt->gin_opCtx = NULL;
    xlog_cxt->gist_opCtx = NULL;
    xlog_cxt->spg_opCtx = NULL;
    xlog_cxt->CheckpointStats = (CheckpointStatsData*)palloc0(sizeof(CheckpointStatsData));
    xlog_cxt->LogwrtResult = (XLogwrtResult*)palloc0(sizeof(XLogwrtResult));
    xlog_cxt->LogwrtPaxos = (XLogwrtPaxos*)palloc0(sizeof(XLogwrtPaxos));
    xlog_cxt->needImmediateCkp = false;
    xlog_cxt->redoItemIdx = 0;
#ifndef ENABLE_MULTIPLE_NODES
    xlog_cxt->committing_csn_list = NIL;
#endif
    xlog_cxt->max_page_flush_lsn = MAX_XLOG_REC_PTR;
    xlog_cxt->redoInterruptCallBackFunc = NULL;
    xlog_cxt->redoPageRepairCallBackFunc = NULL;
    xlog_cxt->xlog_atomic_op = NULL;
    xlog_cxt->currentRetryTimes = 0;
}

static void knl_t_index_init(knl_t_index_context* index_cxt)
{
    index_cxt->ptr_data = (ginxlogInsertDataInternal*)palloc0(sizeof(ginxlogInsertDataInternal));
    index_cxt->ptr_entry = (ginxlogInsertEntry*)palloc0(sizeof(ginxlogInsertEntry));
    index_cxt->ginInsertCtx = NULL;
    index_cxt->btvacinfo = NULL;
}

static void knl_t_time_init(knl_t_time_context* time_cxt)
{
    time_cxt->is_abstimeout_in = false;
    return;
}

static void knl_t_dynahash_init(knl_t_dynahash_context* dyhash_cxt)
{
    dyhash_cxt->CurrentDynaHashCxt = NULL;
    dyhash_cxt->num_seq_scans = 0;
}

static void knl_t_interrupt_init(knl_t_interrupt_context* int_cxt)
{
    int_cxt->QueryCancelPending = false;
    int_cxt->PoolValidateCancelPending = false;
    int_cxt->ProcDiePending = false;
    int_cxt->ClientConnectionLost = false;
    int_cxt->StreamConnectionLost = false;
    int_cxt->ImmediateInterruptOK = false;
    int_cxt->InterruptHoldoffCount = 0;
    int_cxt->QueryCancelHoldoffCount = 0;
    int_cxt->CritSectionCount = 0;
    int_cxt->InterruptByCN = false;
    int_cxt->InterruptCountResetFlag = false;
    int_cxt->ignoreBackendSignal = false;
}

static void knl_t_proc_init(knl_t_proc_context* proc_cxt)
{
    proc_cxt->MyProgName = "unknown";
    proc_cxt->MyBackendId = InvalidBackendId;
    proc_cxt->MyStartTime = 0;
    proc_cxt->DataDir = NULL;
    proc_cxt->postgres_initialized = false;
    proc_cxt->PostInit = (PostgresInitializer*)New(CurrentMemoryContext) PostgresInitializer();
    proc_cxt->proc_exit_inprogress = false;
    proc_cxt->sess_exit_inprogress = false;
    proc_cxt->pooler_connection_inprogress = false;
    proc_cxt->MyPMChildSlot = 0;
}

static void knl_t_wlm_init(knl_t_wlmthrd_context* wlm_cxt)
{
    wlm_cxt->thread_node_group = NULL;
    wlm_cxt->thread_climgr = NULL;
    wlm_cxt->thread_srvmgr = NULL;
    wlm_cxt->wlm_got_sighup = 0;
    wlm_cxt->wlmalarm_pending = false;
    wlm_cxt->wlmalarm_timeout_active = false;
    wlm_cxt->wlmalarm_dump_active = false;
    wlm_cxt->wlm_xact_start = false;
    wlm_cxt->has_cursor_record = false;
    wlm_cxt->wlmalarm_fin_time = 0;
    wlm_cxt->MaskPasswordMemoryContext = NULL;
    wlm_cxt->query_resource_track_mcxt = NULL;

    wlm_cxt->except_ctl = (ExceptionManager*)palloc0(sizeof(ExceptionManager));
    wlm_cxt->collect_info = (WLMCollectInfo*)palloc0(sizeof(WLMCollectInfo));
    wlm_cxt->dn_cpu_detail = (WLMDNRealTimeInfoDetail*)palloc0(sizeof(WLMDNRealTimeInfoDetail));
}

static void knl_t_audit_init(knl_t_audit_context* audit)
{
    audit->audit_indextbl = NULL;
    audit->sysauditFile = NULL;
    audit->policyauditFile = NULL;
    audit->Audit_delete = false;
    audit->pipe_eof_seen = false;
    audit->rotation_disabled = false;
    audit->pgaudit_totalspace = 0;
    audit->user_login_time = 0;
    audit->need_exit = false;
    audit->got_SIGHUP = false;
    audit->rotation_requested = false;
    audit->space_beyond_size = (10 * 1024 * 1024);
    audit->pgaudit_filepath[0] = '\0';
    audit->cur_thread_idx = -1;
}

static void knl_t_async_init(knl_t_async_context* asy_cxt)
{
    asy_cxt->listenChannels = NIL;
    asy_cxt->pendingActions = NIL;
    asy_cxt->upperPendingActions = NIL;
    asy_cxt->pendingNotifies = NIL;
    asy_cxt->upperPendingNotifies = NIL;
    asy_cxt->unlistenExitRegistered = false;
    asy_cxt->amRegisteredListener = false;
    asy_cxt->backendHasSentNotifications = false;
}

static void knl_t_explain_init(knl_t_explain_context* explain_cxt)
{
    explain_cxt->explain_light_proxy = false;
}

static void knl_t_vacuum_init(knl_t_vacuum_context* vacuum_cxt)
{
    vacuum_cxt->VacuumPageHit = 0;
    vacuum_cxt->VacuumPageMiss = 0;
    vacuum_cxt->VacuumPageDirty = 0;
    vacuum_cxt->VacuumCostBalance = 0;
    vacuum_cxt->VacuumCostActive = false;
    vacuum_cxt->vacuum_full_compact = false;
    vacuum_cxt->vac_context = NULL;
    vacuum_cxt->in_vacuum = false;
}

static void knl_t_arch_init(knl_t_arch_context* arch)
{
    arch->got_SIGHUP = false;
    arch->got_SIGTERM = false;
    arch->wakened = false;
    arch->ready_to_stop = false;
    arch->last_sigterm_time = 0;
    arch->pitr_task_last_lsn = 0;
    arch->task_wait_interval = 1000;
    arch->last_arch_time = 0;
    arch->arch_start_timestamp = 0;
    arch->arch_start_lsn = InvalidXLogRecPtr;
    arch->sync_walsender_idx = -1;
#ifndef ENABLE_MULTIPLE_NODES
    arch->sync_follower_id = -1;
#endif
}

static void knl_t_barrier_arch_init(knl_t_barrier_arch_context* barrier_arch)
{
    barrier_arch->got_SIGHUP = false;
    barrier_arch->got_SIGTERM = false;
    barrier_arch->wakened = false;
    barrier_arch->ready_to_stop = false;
    barrier_arch->slot_name = NULL;
    barrier_arch->lastArchiveLoc = InvalidXLogRecPtr;
}


static void knl_t_logger_init(knl_t_logger_context* logger)
{
    logger->pipe_eof_seen = false;
    logger->rotation_disabled = false;
    logger->syslogFile = NULL;
    logger->csvlogFile = NULL;
    logger->querylogFile = NULL;
    logger->asplogFile = NULL;
    logger->first_syslogger_file_time = 0;
    logger->last_file_name = NULL;
    logger->last_csv_file_name = NULL;
    logger->last_asp_file_name = NULL;
    logger->got_SIGHUP = false;
    logger->rotation_requested = false;
}

static void knl_t_bulkload_init(knl_t_bulkload_context* bulk_cxt)
{
    int rc = 0;

    rc = memset_s(bulk_cxt->distExportDataDir, MAX_PATH_LEN, 0, MAX_PATH_LEN);
    securec_check(rc, "\0", "\0");
    rc = memset_s(bulk_cxt->distExportTimestampStr, TIME_STAMP_STR_LEN, 0, TIME_STAMP_STR_LEN);
    securec_check(rc, "\0", "\0");

    bulk_cxt->distExportCurrXid = 0;
    bulk_cxt->distExportNextSegNo = 0;
    bulk_cxt->illegal_character_err_cnt = 0;
    bulk_cxt->illegal_character_err_threshold_reported = false;
}

static void knl_t_job_init(knl_t_job_context* job_cxt)
{
    job_cxt->JobScheduleShmem = NULL;
    job_cxt->got_SIGHUP = false;
    job_cxt->got_SIGUSR2 = false;
    job_cxt->got_SIGTERM = false;
    job_cxt->JobScheduleMemCxt = NULL;
    job_cxt->ExpiredJobList = NULL;
    job_cxt->ExpiredJobListCtx = NULL;
    job_cxt->MyWorkerInfo = NULL;
}

static void knl_t_basebackup_init(knl_t_basebackup_context* basebackup_cxt)
{
    int rc = memset_s(basebackup_cxt->g_xlog_location, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    basebackup_cxt->buf_block = NULL;
}

static void knl_t_datarcvwriter_init(knl_t_datarcvwriter_context* datarcvwriter_cxt)
{
    datarcvwriter_cxt->data_writer_rel_tab = NULL;
    datarcvwriter_cxt->dataRcvWriterFlushPageErrorCount = 0;
    datarcvwriter_cxt->gotSIGHUP = false;
    datarcvwriter_cxt->shutdownRequested = false;
    datarcvwriter_cxt->AmDataReceiverForDummyStandby = false;
    datarcvwriter_cxt->dummy_data_writer_file_num = 0;
}

static void knl_t_libwalreceiver_init(knl_t_libwalreceiver_context* libwalreceiver_cxt)
{
    libwalreceiver_cxt->streamConn = NULL;
    libwalreceiver_cxt->recvBuf = NULL;
    libwalreceiver_cxt->shared_storage_buf = NULL;
    libwalreceiver_cxt->shared_storage_read_buf = NULL;
    libwalreceiver_cxt->decompressBuf = NULL;
    libwalreceiver_cxt->xlogreader = NULL;
}

static void knl_t_sig_init(knl_t_sig_context* sig_cxt)
{
    sig_cxt->signal_handle_cnt = 0;
    sig_cxt->gs_sigale_check_type = SIGNAL_CHECK_NONE;
    sig_cxt->session_id = 0;
    sig_cxt->cur_ctrl_index = 0;
}

static void knl_t_slot_init(knl_t_slot_context* slot_cxt)
{
    slot_cxt->ReplicationSlotCtl = NULL;
    slot_cxt->MyReplicationSlot = NULL;
}

static void knl_t_datareceiver_init(knl_t_datareceiver_context* datareceiver_cxt)
{
    datareceiver_cxt->DataReplFlag = 1;
    datareceiver_cxt->got_SIGHUP = false;
    datareceiver_cxt->got_SIGTERM = false;
    datareceiver_cxt->reply_message = (StandbyDataReplyMessage*)palloc0(sizeof(StandbyDataReplyMessage));
    datareceiver_cxt->dataStreamingConn = NULL;
    datareceiver_cxt->AmDataReceiverForDummyStandby = false;
    datareceiver_cxt->recvBuf = NULL;
    datareceiver_cxt->DataRcv = NULL;
    datareceiver_cxt->DataRcvImmediateInterruptOK = false;
}

static void knl_t_datasender_init(knl_t_datasender_context* datasender_cxt)
{
    datasender_cxt->DataSndCtl = NULL;
    datasender_cxt->MyDataSnd = NULL;
    datasender_cxt->am_datasender = false;
    datasender_cxt->reply_message = (StringInfoData*)palloc0(sizeof(StringInfoData));
    datasender_cxt->output_message = NULL;
    datasender_cxt->dummy_data_read_file_num = 1;
    datasender_cxt->dummy_data_read_file_fd = NULL;
    datasender_cxt->ping_sent = false;
    datasender_cxt->got_SIGHUP = false;
    datasender_cxt->datasender_shutdown_requested = false;
    datasender_cxt->datasender_ready_to_stop = false;
}

static void knl_t_walreceiverfuncs_init(knl_t_walreceiverfuncs_context* walreceiverfuncs_cxt)
{
    walreceiverfuncs_cxt->WalRcv = NULL;
    walreceiverfuncs_cxt->WalReplIndex = 1;
}

static void knl_t_replgram_init(knl_t_replgram_context* replgram_cxt)
{
    replgram_cxt->replication_parse_result = NULL;
}

static void knl_t_replscanner_init(knl_t_replscanner_context* replscanner_cxt)
{
    replscanner_cxt->scanbufhandle = NULL;
    replscanner_cxt->litbuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
}

static void knl_t_syncgram_init(knl_t_syncrepgram_context* syncrepgram_cxt)
{
    syncrepgram_cxt->syncrep_parse_result = NIL;
}

static void knl_t_syncrepscanner_init(knl_t_syncrepscanner_context* syncrepscanner_cxt)
{
    syncrepscanner_cxt->scanbufhandle = NULL;
    syncrepscanner_cxt->xdbuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
    syncrepscanner_cxt->result = 1;
}

static void knl_t_syncrep_init(knl_t_syncrep_context* syncrep_cxt)
{
    syncrep_cxt->SyncRepConfig = NULL;
    syncrep_cxt->SyncRepConfigGroups = 0;
    syncrep_cxt->SyncRepMaxPossib = 0;
    syncrep_cxt->announce_next_takeover = true;
}

static void knl_t_logical_init(knl_t_logical_context* logical_cxt)
{
    logical_cxt->sendFd = -1;
    logical_cxt->sendSegNo = 0;
    logical_cxt->sendOff = 0;
    logical_cxt->ExportInProgress = false;
}

#define BCMElementArrayLen 8192
#define BCMElementArrayLenHalf (BCMElementArrayLen / 2)

static void knl_t_dataqueue_init(knl_t_dataqueue_context* dataqueue_cxt)
{
    dataqueue_cxt->DataSenderQueue = NULL;
    dataqueue_cxt->DataWriterQueue = NULL;
    dataqueue_cxt->BCMElementArray = NULL;
    dataqueue_cxt->BCMElementArrayIndex1 = 0;
    dataqueue_cxt->BCMElementArrayIndex2 = BCMElementArrayLenHalf;
    dataqueue_cxt->BCMElementArrayOffset1 = (DataQueuePtr*)palloc0(sizeof(DataQueuePtr));
    dataqueue_cxt->BCMElementArrayOffset2 = (DataQueuePtr*)palloc0(sizeof(DataQueuePtr));
    dataqueue_cxt->save_send_dummy_count = 0;
    dataqueue_cxt->heap_sync_rel_tab = NULL;
}

static void knl_t_walrcvwriter_init(knl_t_walrcvwriter_context* walrcvwriter_cxt)
{
    walrcvwriter_cxt->gotSIGHUP = false;
    walrcvwriter_cxt->shutdownRequested = false;
    walrcvwriter_cxt->WAL_WRITE_UNIT_BYTES = 1024 * 1024;
    walrcvwriter_cxt->ws_dummy_data_writer_file_num = 0;
}

static void knl_t_postgres_init(knl_t_postgres_context* postgres_cxt)
{
    postgres_cxt->clear_key_memory = false;
    postgres_cxt->table_created_in_CTAS = false;
    postgres_cxt->debug_query_string = NULL;
    postgres_cxt->isInResetUserName = false;
    postgres_cxt->whereToSendOutput = DestDebug;
    postgres_cxt->local_foreign_respool = NULL;
    postgres_cxt->max_stack_depth_bytes = 100 * 1024L;
    postgres_cxt->password_changed = false;
    postgres_cxt->stack_base_ptr = NULL;
    postgres_cxt->xact_started = false;
    postgres_cxt->DoingCommandRead = false;
    postgres_cxt->userDoption = NULL;
    postgres_cxt->EchoQuery = false;
#ifndef TCOP_DONTUSENEWLINE
    postgres_cxt->UseNewLine = 1; /* Use newlines query delimiters (the default) */
#else
    postgres_cxt->UseNewLine = 0; /* Use EOF as query delimiters */
#endif /* TCOP_DONTUSENEWLINE */
    postgres_cxt->RecoveryConflictPending = false;
    postgres_cxt->RecoveryConflictRetryable = true;

    postgres_cxt->row_description_buf = (StringInfoData*)palloc0(sizeof(StringInfoData));

    postgres_cxt->clobber_qstr = NULL;
    postgres_cxt->query_result = 0;
    postgres_cxt->g_NoAnalyzeRelNameList = NULL;
    postgres_cxt->mark_explain_analyze = false;
    postgres_cxt->mark_explain_only = false;
    postgres_cxt->enable_explicit_stmt_name = false;
    postgres_cxt->val = 0;

    postgres_cxt->gpc_fisrt_send_clean = true;
}

static void knl_t_utils_init(knl_t_utils_context* utils_cxt)
{
    utils_cxt->mctx_sequent_count = 0;
    utils_cxt->ExecutorMemoryTrack = NULL;
#ifdef MEMORY_CONTEXT_CHECKING
    utils_cxt->memory_track_sequent_count = -1;
    utils_cxt->memory_track_plan_nodeid = -1;

    utils_cxt->detailTrackingBuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
#endif

    utils_cxt->track_cnt = 0;

    utils_cxt->partId = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));
    utils_cxt->gValueCompareContext = NULL;
    utils_cxt->ContextUsedCount = 0;
#define RANGE_PARTKEYMAXNUM 4
    int rc = memset_s(
        utils_cxt->valueItemArr, RANGE_PARTKEYMAXNUM * sizeof(Const*), 0, RANGE_PARTKEYMAXNUM * sizeof(Const*));
    securec_check(rc, "\0", "\0");
    utils_cxt->CurrentResourceOwner = NULL;
    utils_cxt->STPSavedResourceOwner = NULL;
    utils_cxt->CurTransactionResourceOwner = NULL;
    utils_cxt->TopTransactionResourceOwner = NULL;
    utils_cxt->SortColumnOptimize = false;
    utils_cxt->pRelatedRel = NULL;
    utils_cxt->sigTimerId = NULL;
    utils_cxt->pg_strtok_ptr = NULL;

    utils_cxt->maxChunksPerThread = 0;
    utils_cxt->beyondChunk = 0;
}

static void knl_t_pgxc_init(knl_t_pgxc_context* pgxc_cxt)
{
    errno_t rc;

    pgxc_cxt->current_installation_nodegroup = NULL;
    pgxc_cxt->current_redistribution_nodegroup = NULL;
    pgxc_cxt->globalBucketLen = 0;

    pgxc_cxt->shmemNumCoords = NULL;
    pgxc_cxt->shmemNumCoordsInCluster = NULL;
    pgxc_cxt->shmemNumDataNodes = NULL;
    pgxc_cxt->shmemNumDataStandbyNodes = NULL;
    pgxc_cxt->coDefs = NULL;
    pgxc_cxt->coDefsInCluster = NULL;
    pgxc_cxt->dnDefs = NULL;
    pgxc_cxt->dnStandbyDefs = NULL;

    pgxc_cxt->pgxc_net_ctl = (PGXCNodeNetCtlLayer*)palloc0(sizeof(PGXCNodeNetCtlLayer));
    pgxc_cxt->GlobalNetInstr = NULL;
    pgxc_cxt->compute_pool_handle = NULL;
    pgxc_cxt->compute_pool_conn = NULL;

    pgxc_cxt->temp_object_included = false;
    pgxc_cxt->dbcleanup_info = (abort_callback_type*)palloc0(sizeof(abort_callback_type));
    rc = memset_s(pgxc_cxt->socket_buffer, SOCKET_BUFFER_LEN, 0, SOCKET_BUFFER_LEN);
    securec_check(rc, "\0", "\0");
    rc = memset_s(pgxc_cxt->begin_cmd, BEGIN_CMD_BUFF_SIZE, 0, BEGIN_CMD_BUFF_SIZE);
    securec_check(rc, "\0", "\0");
}

static void knl_t_conn_init(knl_t_conn_context* conn_cxt)
{
    conn_cxt->ecCtrl = NULL;
    conn_cxt->dl_handle = NULL;
    conn_cxt->_conn = NULL;
    conn_cxt->_result = NULL;
    conn_cxt->_DatabaseEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];
    conn_cxt->_float_inf = "Inf";
}

static void knl_t_page_redo_init(knl_t_page_redo_context* page_redo_cxt)
{
    page_redo_cxt->shutdown_requested = false;
    page_redo_cxt->got_SIGHUP = false;
    page_redo_cxt->sleep_long = false;
    page_redo_cxt->check_repair = false;
}

static void knl_t_parallel_decode_init(knl_t_parallel_decode_worker_context* parallel_decode_cxt)
{
    parallel_decode_cxt->shutdown_requested = false;
    parallel_decode_cxt->got_SIGHUP = false;
    parallel_decode_cxt->sleep_long = false;
}

static void knl_t_parallel_decode_reader_init(knl_t_logical_read_worker_context* parallel_decode_reader_cxt)
{
    parallel_decode_reader_cxt->shutdown_requested = false;
    parallel_decode_reader_cxt->got_SIGHUP = false;
    parallel_decode_reader_cxt->sleep_long = false;
}

static void knl_t_startup_init(knl_t_startup_context* startup_cxt)
{
    startup_cxt->got_SIGHUP = false;
    startup_cxt->shutdown_requested = false;
    startup_cxt->failover_triggered = false;
    startup_cxt->switchover_triggered = false;
    startup_cxt->primary_triggered = false;
    startup_cxt->standby_triggered = false;
    startup_cxt->in_restore_command = false;
    startup_cxt->NotifySigState = NULL;
}

static void knl_t_alarmchecker_init(knl_t_alarmchecker_context* alarm_cxt)
{
    alarm_cxt->gotSighup = false;
    alarm_cxt->gotSigdie = false;
}

static void knl_t_lwlockmoniter_init(knl_t_lwlockmoniter_context* lwm_cxt)
{
    lwm_cxt->got_SIGHUP = false;
    lwm_cxt->shutdown_requested = false;
}

static void knl_t_walwriter_init(knl_t_walwriter_context* walwriter_cxt)
{
    walwriter_cxt->got_SIGHUP = false;
    walwriter_cxt->shutdown_requested = false;
}

static void knl_t_walwriterauxiliary_init(knl_t_walwriterauxiliary_context *const walwriterauxiliary_cxt)
{
    walwriterauxiliary_cxt->got_SIGHUP = false;
    walwriterauxiliary_cxt->shutdown_requested = false;
}

static void knl_t_poolcleaner_init(knl_t_poolcleaner_context* poolcleaner_cxt)
{
    poolcleaner_cxt->shutdown_requested = false;
    poolcleaner_cxt->got_SIGHUP = false;
}

static void knl_t_catchup_init(knl_t_catchup_context* catchup_cxt)
{
    catchup_cxt->catchup_shutdown_requested = false;
}

/* interval for calling AbsorbFsyncRequests in CheckpointWriteDelay */
#define WRITES_PER_ABSORB 1000

static void knl_t_checkpoint_init(knl_t_checkpoint_context* checkpoint_cxt)
{
    checkpoint_cxt->CheckpointerShmem = NULL;
    checkpoint_cxt->got_SIGHUP = false;
    checkpoint_cxt->checkpoint_requested = false;
    checkpoint_cxt->shutdown_requested = false;
    checkpoint_cxt->ckpt_active = false;
    checkpoint_cxt->absorbCounter = WRITES_PER_ABSORB;
    checkpoint_cxt->ckpt_done = 0;
}

static void knl_t_autovacuum_init(knl_t_autovacuum_context* autovacuum_cxt)
{
    autovacuum_cxt->got_SIGHUP = false;
    autovacuum_cxt->got_SIGUSR2 = false;
    autovacuum_cxt->got_SIGTERM = false;
    autovacuum_cxt->pgStatAutoVacInfo = NULL;
    autovacuum_cxt->autovacuum_coordinators_string = "";
    autovacuum_cxt->autovac_iops_limits = 0;
    autovacuum_cxt->DatabaseList = NULL;
    autovacuum_cxt->DatabaseListCxt = NULL;
    autovacuum_cxt->MyWorkerInfo = NULL;
    autovacuum_cxt->AutovacuumLauncherPid = 0;
    autovacuum_cxt->last_read = 0;
}

static void KnlTApplyLauncherInit(knl_t_apply_launcher_context* applyLauncherCxt)
{
    applyLauncherCxt->got_SIGHUP = false;
    applyLauncherCxt->newWorkerRequest = false;
    applyLauncherCxt->got_SIGTERM = false;
    applyLauncherCxt->onCommitLauncherWakeup = false;
    applyLauncherCxt->applyLauncherShm = NULL;
}

static void KnlTApplyWorkerInit(knl_t_apply_worker_context* applyWorkerCxt)
{
    applyWorkerCxt->got_SIGHUP = false;
    applyWorkerCxt->got_SIGTERM = false;
    applyWorkerCxt->lsnMapping.head.next = &applyWorkerCxt->lsnMapping.head;
    applyWorkerCxt->lsnMapping.head.prev = &applyWorkerCxt->lsnMapping.head;
    applyWorkerCxt->logicalRepRelMap = NULL;
    applyWorkerCxt->sendTime = 0;
    applyWorkerCxt->lastRecvpos = InvalidXLogRecPtr;
    applyWorkerCxt->lastWritepos = InvalidXLogRecPtr;
    applyWorkerCxt->lastFlushpos = InvalidXLogRecPtr;
    applyWorkerCxt->mySubscription = NULL;
    applyWorkerCxt->mySubscriptionValid = false;
    applyWorkerCxt->inRemoteTransaction = false;
    applyWorkerCxt->curWorker = NULL;
    applyWorkerCxt->messageContext = NULL;
    applyWorkerCxt->logicalRepRelMapContext = NULL;
    applyWorkerCxt->applyContext = NULL;
}

static void KnlTPublicationInit(knl_t_publication_context* publicationCxt)
{
    publicationCxt->publications_valid = false;
    publicationCxt->RelationSyncCache = NULL;
}

static void KnlTUndolauncherInit(knl_t_undolauncher_context* undolauncherCxt)
{
    undolauncherCxt->got_SIGHUP = false;
    undolauncherCxt->got_SIGUSR2 = false;
    undolauncherCxt->got_SIGTERM = false;

    undolauncherCxt->UndoLauncherPid = 0;
}

static void KnlTUndoInit (knl_t_undo_context* undoCtx)
{
    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence upersistence = static_cast<UndoPersistence>(i);
        undoCtx->zids[upersistence] = INVALID_ZONE_ID;
        undoCtx->prevXid[upersistence] = InvalidTransactionId;
        undoCtx->slots[upersistence] = NULL;
        undoCtx->slotPtr[upersistence] = INVALID_UNDO_REC_PTR;
    }
    undoCtx->transUndoSize = 0;
    undoCtx->fetchRecord = false;
}

static void KnlTUndoworkerInit(knl_t_undoworker_context* undoworkerCxt)
{
    undoworkerCxt->got_SIGHUP = false;
    undoworkerCxt->got_SIGUSR2 = false;
    undoworkerCxt->got_SIGTERM = false;
}

static void KnlTUndorecyclerInit(knl_t_undorecycler_context* undorecyclerCxt)
{
    undorecyclerCxt->got_SIGHUP = false;
    undorecyclerCxt->shutdown_requested = false;
}

static void KnlTRollbackRequestsInit(knl_t_rollback_requests_context* context)
{
    context->rollback_requests_hash = NULL;
    context->next_bucket_for_scan = 0;
}

static void KnlTGstatInit(knl_t_gstat_context* context)
{
    context->got_SIGHUP = false;
    context->got_SIGUSR2 = false;
    context->got_SIGTERM = false;
}

static void knl_t_aiocompleter_init(knl_t_aiocompleter_context* aio_cxt)
{
    aio_cxt->shutdown_requested = false;
    aio_cxt->config_requested = false;
}

static void knl_t_twophasecleaner_init(knl_t_twophasecleaner_context* tpcleaner_cxt)
{
    tpcleaner_cxt->got_SIGHUP = false;
    tpcleaner_cxt->shutdown_requested = false;
}

static void knl_t_bgwriter_init(knl_t_bgwriter_context* bgwriter_cxt)
{
    bgwriter_cxt->got_SIGHUP = false;
    bgwriter_cxt->shutdown_requested = false;
}

static void knl_t_pagewriter_init(knl_t_pagewriter_context* pagewriter_cxt)
{
    pagewriter_cxt->got_SIGHUP = false;
    pagewriter_cxt->sync_requested = false;
    pagewriter_cxt->sync_retry = false;
    pagewriter_cxt->shutdown_requested = false;
    pagewriter_cxt->page_writer_after = WRITEBACK_MAX_PENDING_FLUSHES;
    pagewriter_cxt->pagewriter_id = -1;
}

static void knl_t_barrier_creator_init(knl_t_barrier_creator_context* barrier_creator_cxt)
{
    barrier_creator_cxt->archive_slot_names = NULL;
    barrier_creator_cxt->got_SIGHUP = false;
    barrier_creator_cxt->is_first_barrier = false;
    barrier_creator_cxt->barrier_update_last_time_info = NULL;
    barrier_creator_cxt->shutdown_requested = false;
    barrier_creator_cxt->first_cn_timeline = 0;
}

static void knl_t_xlogcopybackend_init(knl_t_sharestoragexlogcopyer_context* cxt)
{
    cxt->got_SIGHUP = false;
    cxt->shutdown_requested = false;
    cxt->wakeUp = false;
    cxt->readFile = -1;
    cxt->readOff = 0;
    cxt->readSegNo = 0;
    cxt->buf = NULL;
    cxt->originBuf = NULL;
}


extern bool HeapTupleSatisfiesNow(HeapTuple htup, Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer);
extern bool HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot, Buffer buffer);

static void knl_t_snapshot_init(knl_t_snapshot_context* snapshot_cxt)
{
    snapshot_cxt->SnapshotNowData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    snapshot_cxt->SnapshotNowData->satisfies = SNAPSHOT_NOW;

    snapshot_cxt->SnapshotSelfData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    snapshot_cxt->SnapshotSelfData->satisfies = SNAPSHOT_SELF;

    snapshot_cxt->SnapshotAnyData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    snapshot_cxt->SnapshotAnyData->satisfies = SNAPSHOT_ANY;

    snapshot_cxt->SnapshotToastData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    snapshot_cxt->SnapshotToastData->satisfies = SNAPSHOT_TOAST;
}

static void knl_t_comm_init(knl_t_comm_context* comm_cxt)
{
    Assert(comm_cxt != NULL);
    comm_cxt->g_receiver_loop_poll_up = 0;
    comm_cxt->LibcommThreadType = LIBCOMM_NONE;
    comm_cxt->libcomm_semaphore = NULL;
    comm_cxt->g_libcomm_poller_list = NULL;
    comm_cxt->g_libcomm_recv_poller_hndl_list = NULL;
    comm_cxt->g_consumer_process_duration = 0;
    comm_cxt->g_producer_process_duration = 0;
    comm_cxt->debug_query_id = 0;
}

static void knl_t_libpq_init(knl_t_libpq_context* libpq_cxt)
{
    errno_t rc;

    Assert(libpq_cxt != NULL);
    libpq_cxt->listen_fd_for_recv_flow_ctrl = -1;
    rc = memset_s(libpq_cxt->sock_path, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(libpq_cxt->ha_sock_path, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    libpq_cxt->PqSendBuffer = NULL;
    libpq_cxt->PqCommBusy = false;
    libpq_cxt->DoingCopyOut = false;

    libpq_cxt->save_query_result_to_disk = false;
    temp_file_context_init(libpq_cxt);

    libpq_cxt->parsed_hba_lines = NIL;
    libpq_cxt->parsed_hba_context = NULL;
}


static void knl_t_contrib_init(knl_t_contrib_context* contrib_cxt)
{
    contrib_cxt->g_searchletId = 0;
    contrib_cxt->vec_info = NULL;
    contrib_cxt->assert_enabled = true;
    contrib_cxt->g_log_hostname = (Datum)0;
    contrib_cxt->g_log_nodename = (Datum)0;
    contrib_cxt->ShippableCacheHash = NULL;
}

static void knl_t_walreceiver_init(knl_t_walreceiver_context* walreceiver_cxt)
{
    errno_t rc;

    walreceiver_cxt->got_SIGHUP = false;
    walreceiver_cxt->got_SIGTERM = false;
    walreceiver_cxt->start_switchover = false;
    rc = memset_s(walreceiver_cxt->gucconf_file, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(walreceiver_cxt->temp_guc_conf_file, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(walreceiver_cxt->gucconf_lock_file, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    walreceiver_cxt->reserve_item = {0};
    walreceiver_cxt->check_file_timeout = 60 * 60 * 1000;
    walreceiver_cxt->walRcvCtlBlock = NULL;
    walreceiver_cxt->reply_message = (StandbyReplyMessage*)palloc0(sizeof(StandbyReplyMessage));
    walreceiver_cxt->feedback_message = (StandbyHSFeedbackMessage*)palloc0(sizeof(StandbyHSFeedbackMessage));
    ;
    walreceiver_cxt->request_message = (StandbySwitchRequestMessage*)palloc0(sizeof(StandbySwitchRequestMessage));
    ;
    walreceiver_cxt->reply_modify_message = (ConfigModifyTimeMessage*)palloc0(sizeof(ConfigModifyTimeMessage));
    ;
    walreceiver_cxt->WalRcvImmediateInterruptOK = false;
    walreceiver_cxt->AmWalReceiverForFailover = false;
    walreceiver_cxt->AmWalReceiverForStandby = false;
    walreceiver_cxt->control_file_writed = 0;
    walreceiver_cxt->checkConsistencyOK = false;
    walreceiver_cxt->hasReceiveNewData = false;
    walreceiver_cxt->termChanged = false;
}

static void knl_t_storage_init(knl_t_storage_context* storage_cxt)
{
    errno_t rc;

    storage_cxt->latestObservedXid = InvalidTransactionId;
    storage_cxt->CurrentRunningXacts = (RunningTransactionsData*)palloc0(sizeof(RunningTransactionsData));
    storage_cxt->proc_vxids = NULL;

    storage_cxt->BufferDescriptors = NULL;
    storage_cxt->BufferBlocks = NULL;
    storage_cxt->BackendWritebackContext = (WritebackContext*)palloc0(sizeof(WritebackContext));
    storage_cxt->SharedBufHash = NULL;
    storage_cxt->InProgressBuf = NULL;
    storage_cxt->IsForInput = false;
    storage_cxt->PinCountWaitBuf = NULL;
    storage_cxt->InProgressAioDispatch = NULL;
    storage_cxt->InProgressAioDispatchCount = 0;
    storage_cxt->InProgressAioBuf = NULL;
    storage_cxt->InProgressAioType = AioUnkown;
    storage_cxt->is_btree_split = false;
    storage_cxt->PrivateRefCountArray =
        (PrivateRefCountEntry*)palloc0(sizeof(PrivateRefCountEntry) * REFCOUNT_ARRAY_ENTRIES);
    storage_cxt->PrivateRefCountHash = NULL;
    storage_cxt->PrivateRefCountOverflowed = 0;
    storage_cxt->PrivateRefCountClock = 0;
    storage_cxt->saved_info_valid = false;
    storage_cxt->prev_strategy_buf_id = 0;
    storage_cxt->prev_strategy_passes = 0;
    storage_cxt->next_to_clean = 0;
    storage_cxt->next_passes = 0;
    storage_cxt->smoothed_alloc = 0;
    storage_cxt->smoothed_density = 10.0;
    storage_cxt->StrategyControl = NULL;
    storage_cxt->CacheBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;
    storage_cxt->CacheBlockInProgressUncompress = CACHE_BLOCK_INVALID_IDX;
    storage_cxt->MetaBlockInProgressIO = CACHE_BLOCK_INVALID_IDX;

#define STANDBY_INITIAL_WAIT_US 1000
    storage_cxt->RecoveryLockList = NIL;
    storage_cxt->standbyWait_us = STANDBY_INITIAL_WAIT_US;
    storage_cxt->lo_heap_r = NULL;
    storage_cxt->lo_index_r = NULL;
    rc = memset_s(&storage_cxt->dummy_spinlock, sizeof(slock_t), 0, sizeof(slock_t));
    securec_check(rc, "\0", "\0");
    storage_cxt->spins_per_delay = DEFAULT_SPINS_PER_DELAY;

    storage_cxt->visitedProcs = NULL;
    storage_cxt->nVisitedProcs = 0;
    storage_cxt->topoProcs = NULL;
    storage_cxt->beforeConstraints = NULL;
    storage_cxt->afterConstraints = NULL;
    storage_cxt->waitOrders = NULL;
    storage_cxt->nWaitOrders = 0;
    storage_cxt->waitOrderProcs = NULL;
    storage_cxt->curConstraints = NULL;
    storage_cxt->nCurConstraints = 0;
    storage_cxt->maxCurConstraints = 0;
    storage_cxt->possibleConstraints = NULL;
    storage_cxt->nPossibleConstraints = 0;
    storage_cxt->maxPossibleConstraints = 0;
    storage_cxt->deadlockDetails = NULL;
    storage_cxt->nDeadlockDetails = 0;
    storage_cxt->blocking_autovacuum_proc = NULL;
    storage_cxt->blocking_autovacuum_proc_num = 0;
    storage_cxt->deadlock_checker_start_time = 0;
    storage_cxt->conflicting_lock_mode_name = NULL;
    storage_cxt->conflicting_lock_thread_id = 0;
    storage_cxt->conflicting_lock_by_holdlock = true;
    storage_cxt->FastPathLocalUseCount = 0;
    storage_cxt->FastPathStrongRelationLocks = NULL;
    storage_cxt->LockMethodLockHash = NULL;
    storage_cxt->LockMethodProcLockHash = NULL;
    storage_cxt->LockMethodLocalHash = NULL;
    storage_cxt->StrongLockInProgress = NULL;
    storage_cxt->awaitedLock = NULL;
    storage_cxt->awaitedOwner = NULL;
    storage_cxt->blocking_redistribution_proc = NULL;
    storage_cxt->lock_vxids = NULL;

    storage_cxt->EnlargeDeadlockTimeout = false;
    storage_cxt->lockAwaited = NULL;
    storage_cxt->standby_timeout_active = false;
    storage_cxt->statement_timeout_active = false;
    storage_cxt->deadlock_timeout_active = false;
    storage_cxt->lockwait_timeout_active = false;
    storage_cxt->deadlock_state = DS_NOT_YET_CHECKED;
    storage_cxt->cancel_from_timeout = false;
    storage_cxt->timeout_start_time = 0;
    storage_cxt->statement_fin_time = 0;
    storage_cxt->statement_fin_time2 = 0;
    storage_cxt->pageCopy = NULL;

    storage_cxt->isSwitchoverLockHolder = false;
    storage_cxt->num_held_lwlocks = 0;
    storage_cxt->held_lwlocks = (LWLockHandle*)palloc0(MAX_SIMUL_LWLOCKS * sizeof(LWLockHandle));
    storage_cxt->lock_addin_request = 0;
    storage_cxt->lock_addin_request_allowed = true;
    storage_cxt->counts_for_pid = 0;
    storage_cxt->block_counts = NULL;
    storage_cxt->remote_function_context = NULL;
    storage_cxt->work_env_init = false;

    storage_cxt->shmem_startup_hook = NULL;
    storage_cxt->total_addin_request = 0;
    storage_cxt->addin_request_allowed = true;
    storage_cxt->atexit_callback_setup = false;
    rc = memset_s(storage_cxt->on_proc_exit_list, MAX_ON_EXITS * sizeof(ONEXIT), 0, MAX_ON_EXITS * sizeof(ONEXIT));
    securec_check(rc, "\0", "\0");
    rc = memset_s(storage_cxt->on_shmem_exit_list, MAX_ON_EXITS * sizeof(ONEXIT), 0, MAX_ON_EXITS * sizeof(ONEXIT));
    securec_check(rc, "\0", "\0");
    storage_cxt->on_proc_exit_index = 0;
    storage_cxt->on_shmem_exit_index = 0;

    storage_cxt->cmprMetaInfo = (CmprMetaUnion*)palloc0(sizeof(CmprMetaUnion));
    storage_cxt->DataFileIdCache = NULL;
    storage_cxt->SegSpcCache = NULL;
    storage_cxt->uidHashCache = NULL;
    storage_cxt->DisasterCache = NULL;

    storage_cxt->max_safe_fds = 32;
    storage_cxt->max_userdatafiles = 8192 - 1000;
    storage_cxt->timeoutRemoteOpera = 0;
}

static void knl_t_port_init(knl_t_port_context* port_cxt)
{
    port_cxt->thread_is_exiting = false;
    port_cxt->save_locale_r = (locale_t)0;
    port_cxt->cur_datcollate.data[0] = '\0';
    port_cxt->cur_datctype.data[0] = '\0';
    port_cxt->cur_monetary.data[0] = '\0';
    port_cxt->cur_numeric.data[0] = '\0';
}

static void knl_t_walsender_init(knl_t_walsender_context* walsender_cxt)
{
    errno_t rc;

    walsender_cxt->load_cu_buffer = NULL;
    walsender_cxt->load_cu_buffer_size = 64 * 1024 * 1024;
    walsender_cxt->WalSndCtl = NULL;
    walsender_cxt->MyWalSnd = NULL;
    walsender_cxt->logical_xlog_advanced_timeout = 10 * 1000;
    walsender_cxt->Demotion = NoDemote;
    walsender_cxt->wake_wal_senders = false;
    walsender_cxt->wal_send_completed = false;
    walsender_cxt->sendFile = -1;
    walsender_cxt->sendSegNo = 0;
    walsender_cxt->sendOff = 0;
    walsender_cxt->sentPtr = 0;
    walsender_cxt->catchup_threshold = 0;
    walsender_cxt->output_xlog_msg_prefix_len = 0;
    walsender_cxt->output_data_msg_cur_len = 0;
    walsender_cxt->output_data_msg_start_xlog = InvalidXLogRecPtr;
    walsender_cxt->output_data_msg_end_xlog = InvalidXLogRecPtr;
    walsender_cxt->ws_xlog_reader = NULL;
    walsender_cxt->waiting_for_ping_response = false;
    walsender_cxt->got_SIGHUP = false;
    walsender_cxt->walsender_shutdown_requested = false;
    walsender_cxt->walsender_ready_to_stop = false;
    walsender_cxt->response_switchover_requested = false;
    walsender_cxt->server_run_mode = NORMAL_MODE;
    rc = memset_s(walsender_cxt->gucconf_file, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(walsender_cxt->gucconf_lock_file, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
    rc = memset_s(walsender_cxt->slotname, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    walsender_cxt->ws_dummy_data_read_file_fd = NULL;
    walsender_cxt->ws_dummy_data_read_file_num = 1;
    walsender_cxt->CheckCUArray = NULL;
    walsender_cxt->logical_decoding_ctx = NULL;
    walsender_cxt->logical_startptr = InvalidXLogRecPtr;
    walsender_cxt->wsXLogJustSendRegion = (WSXLogJustSendRegion*)palloc0(sizeof(WSXLogJustSendRegion));
    walsender_cxt->wsXLogJustSendRegion->start_ptr = InvalidXLogRecPtr;
    walsender_cxt->wsXLogJustSendRegion->end_ptr = InvalidXLogRecPtr;
    walsender_cxt->reply_message = (StringInfoData*)palloc0(sizeof(StringInfoData));
    walsender_cxt->tmpbuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
    walsender_cxt->remotePort = 0;
    walsender_cxt->walSndCaughtUp = false;
    walsender_cxt->LogicalSlot = -1;
    walsender_cxt->last_check_timeout_timestamp = 0;
    walsender_cxt->isWalSndSendTimeoutMessage = false;
    walsender_cxt->advancePrimaryConn = NULL;
    walsender_cxt->xlogReadBuf = NULL;
    walsender_cxt->compressBuf = NULL;
    walsender_cxt->ep_fd = -1;
    walsender_cxt->datafd = -1;
    walsender_cxt->is_obsmode = false;
    walsender_cxt->standbyConnection = false;
    walsender_cxt->restoreLogicalLogHead = NULL;
}

static void knl_t_tsearch_init(knl_t_tsearch_context* tsearch_cxt)
{
    tsearch_cxt->nres = 0;
    tsearch_cxt->ntres = 0;
}

static void knl_t_postmaster_init(knl_t_postmaster_context* postmaster_cxt)
{
    postmaster_cxt->ProcessStartupPacketForLogicConn = false;
    postmaster_cxt->sock_for_libcomm = -1;
    postmaster_cxt->port_for_libcomm = NULL;
    postmaster_cxt->KeepSocketOpenForStream = false;

    int rc;
    rc = memset_s(postmaster_cxt->ReplConnArray,
        MAX_REPLNODE_NUM * sizeof(replconninfo*),
        0,
        MAX_REPLNODE_NUM * sizeof(replconninfo*));
    securec_check(rc, "\0", "\0");
    rc = memset_s(postmaster_cxt->ReplConnChangeType, MAX_REPLNODE_NUM * sizeof(int),
        NO_CHANGE, MAX_REPLNODE_NUM * sizeof(int));
    securec_check(rc, "\0", "\0");

    rc = memset_s(postmaster_cxt->CrossClusterReplConnArray, MAX_REPLNODE_NUM * sizeof(replconninfo*), 0,
        MAX_REPLNODE_NUM * sizeof(replconninfo*));
    securec_check(rc, "\0", "\0");
    rc = memset_s(postmaster_cxt->CrossClusterReplConnChanged, MAX_REPLNODE_NUM * sizeof(bool), 0,
                  MAX_REPLNODE_NUM * sizeof(bool));
    securec_check(rc, "\0", "\0");

    postmaster_cxt->HaShmData = NULL;
    postmaster_cxt->LocalIpNum = 0;
    postmaster_cxt->IsRPCWorkerThread = false;
    postmaster_cxt->audit_primary_start = true;
    postmaster_cxt->audit_primary_failover = false;
    postmaster_cxt->audit_standby_switchover = false;
    postmaster_cxt->senderToDummyStandby = false;
    postmaster_cxt->senderToBuildStandby = false;
    postmaster_cxt->ReachedNormalRunning = false;
    postmaster_cxt->redirection_done = false;
    postmaster_cxt->start_autovac_launcher = false;
    postmaster_cxt->avlauncher_needs_signal = false;
    postmaster_cxt->start_job_scheduler = false;
    postmaster_cxt->jobscheduler_needs_signal = false;
    postmaster_cxt->random_seed = 0;
    postmaster_cxt->forceNoSeparate = false;
    postmaster_cxt->postmaster_alive_fds[0] = -1;
    postmaster_cxt->postmaster_alive_fds[1] = -1;

    postmaster_cxt->syslogPipe[0] = -1;
    postmaster_cxt->syslogPipe[1] = -1;
}

static void knl_t_bootstrap_init(knl_t_bootstrap_context* bootstrap_cxt)
{
    bootstrap_cxt->num_columns_read = 0;
    bootstrap_cxt->yyline = 1;
    bootstrap_cxt->MyAuxProcType = NotAnAuxProcess;
    bootstrap_cxt->Typ = NULL;
    bootstrap_cxt->Ap = NULL;
    bootstrap_cxt->nogc = NULL;
    bootstrap_cxt->ILHead = NULL;
}

static void knl_t_pencentile_init(knl_t_percentile_context* percentile_cxt)
{
    percentile_cxt->need_exit = false;
    percentile_cxt->need_reset_timer = true;
    percentile_cxt->pgxc_all_handles = NULL;
    percentile_cxt->got_SIGHUP = false;
}

static void knl_t_perf_snap_init(knl_t_perf_snap_context* perf_snap_cxt)
{
    perf_snap_cxt->need_exit = false;
    perf_snap_cxt->got_SIGHUP = false;
    perf_snap_cxt->last_snapshot_start_time = 0;
    perf_snap_cxt->curr_table_size = 0;
    perf_snap_cxt->curr_snapid = 0;
    perf_snap_cxt->connect = NULL;
    perf_snap_cxt->cancel_request = 0;
    perf_snap_cxt->request_snapshot = false;
    perf_snap_cxt->res = NULL;
    perf_snap_cxt->is_mem_protect = false;
}

static void knl_t_ash_init(knl_t_ash_context* ash_cxt)
{
    ash_cxt->last_ash_start_time = 0;
    ash_cxt->need_exit = false;
    ash_cxt->got_SIGHUP = false;
    ash_cxt->slot = 0;
    ash_cxt->waitEventStr = NULL;
}

static void knl_t_statement_init(knl_t_statement_context* statement_cxt)
{
    statement_cxt->need_exit = false;
    statement_cxt->got_SIGHUP = false;
    statement_cxt->full_sql_retention_time = 0;
    statement_cxt->slow_sql_retention_time = 0;
    statement_cxt->instr_prev_post_parse_analyze_hook = NULL;
}


static void knl_t_stat_init(knl_t_stat_context* stat_cxt)
{
    stat_cxt->local_bad_block_mcxt = NULL;
    stat_cxt->local_bad_block_stat = NULL;
    stat_cxt->need_exit = false;
}

static void knl_t_heartbeat_init(knl_t_heartbeat_context* heartbeat_cxt)
{
    heartbeat_cxt->got_SIGHUP = false;
    heartbeat_cxt->shutdown_requested = false;
    heartbeat_cxt->state = NULL;
    heartbeat_cxt->total_failed_times = 0;
    heartbeat_cxt->last_failed_timestamp = 0;
}

static void knl_t_streaming_init(knl_t_streaming_context* streaming_cxt)
{
    errno_t rc = EOK;
    rc = memset_s(streaming_cxt, sizeof(knl_t_streaming_context), 0, sizeof(knl_t_streaming_context));
    securec_check(rc, "\0", "\0");
    return;
}

static void knl_t_ts_compaction_init(knl_t_ts_compaction_context* compaction_cxt)
{
    compaction_cxt->got_SIGHUP = false;
    compaction_cxt->shutdown_requested = false;
    compaction_cxt->sleep_long = true;
    compaction_cxt->compaction_mem_cxt = NULL;
    compaction_cxt->compaction_data_cxt = NULL;
}


static void knl_t_security_policy_init(knl_t_security_policy_context *const policy_cxt)
{
    policy_cxt->StringMemoryContext = NULL;
    policy_cxt->VectorMemoryContext = NULL;
    policy_cxt->MapMemoryContext = NULL;
    policy_cxt->SetMemoryContext = NULL;
    policy_cxt->OidRBTreeMemoryContext = NULL;
    policy_cxt->node_location = 0;
    policy_cxt->prepare_stmt_name = "";
}

static void knl_t_security_ledger_init(knl_t_security_ledger_context * const ledger_cxt)
{
    ledger_cxt->prev_ExecutorEnd = NULL;
}

static void knl_t_csnmin_sync_init(knl_t_csnmin_sync_context* csnminsync_cxt)
{
    csnminsync_cxt->got_SIGHUP = false;
    csnminsync_cxt->shutdown_requested = false;
}

static void knl_t_bgworker_init(knl_t_bgworker_context* bgworker_cxt)
{
    slist_init(&bgworker_cxt->bgwlist);
    bgworker_cxt->bgwcontext = NULL;
    bgworker_cxt->bgworker = NULL;
}

static void knl_index_advisor_init(knl_t_index_advisor_context* index_advisor_cxt)
{
    index_advisor_cxt->stmt_table_list = NULL;
    index_advisor_cxt->stmt_target_list = NULL;
}

#ifdef ENABLE_MOT
static void knl_t_mot_init(knl_t_mot_context* mot_cxt)
{
    mot_cxt->last_error_code = 0;
    mot_cxt->last_error_severity = 0;
    mot_cxt->error_frame_count = 0;

    mot_cxt->log_line_pos = 0;
    mot_cxt->log_line_overflow = false;
    mot_cxt->log_line_buf = NULL;
    mot_cxt->log_level = 3; // the equivalent of MOT::LogLevel::LL_INFO

    mot_cxt->currentThreadId = (uint16_t)-1;
    mot_cxt->currentNumaNodeId = (-2);

    mot_cxt->bindPolicy = 2; // MPOL_BIND
    mot_cxt->mbindFlags = 0;
}

void knl_thread_mot_init()
{
    knl_t_mot_init(&t_thrd.mot_cxt);
}
#endif

#ifdef DEBUG_UHEAP
static void
knl_t_uheap_stats_init(knl_t_uheap_stats_context* uheap_stats_cxt)
{
    /* monitoring infrastructure */
    uheap_stats_cxt->uheap_stat_shared = NULL;
    uheap_stats_cxt->uheap_stat_local = (UHeapStat_Collect *)palloc0(sizeof(UHeapStat_Collect));
}
#endif

void KnlDcfContextInit(knl_t_dcf_context* dcfContext)
{
    dcfContext->isDcfShmemInited = false;
    dcfContext->is_dcf_thread = false;
    dcfContext->dcfCtxInfo = NULL;
}

void KnlLscContextInit(knl_t_lsc_context *lsc_cxt)
{
    lsc_cxt->lsc = NULL;
    lsc_cxt->enable_lsc = false;
    lsc_cxt->FetchTupleFromCatCList = NULL;
}

void knl_thread_init(knl_thread_role role)
{
    t_thrd.role = role;
    t_thrd.subrole = NO_SUBROLE;
    t_thrd.proc = NULL;
    t_thrd.pgxact = NULL;
    t_thrd.bn = NULL;
    t_thrd.is_inited = false;
    t_thrd.myLogicTid = 10000;
    t_thrd.fake_session = NULL;
    t_thrd.threadpool_cxt.reaper_dead_session = false;
    ENABLE_MEMORY_PROTECT();
    MemoryContextUnSeal(t_thrd.top_mem_cxt);
    t_thrd.mcxt_group = New(t_thrd.top_mem_cxt) MemoryContextGroup();
    t_thrd.mcxt_group->Init(t_thrd.top_mem_cxt);
    MemoryContextSeal(t_thrd.top_mem_cxt);
    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    KnlLscContextInit(&t_thrd.lsc_cxt);
    /* CommProxy Support */
    t_thrd.comm_sock_option = g_default_invalid_sock_opt;
    t_thrd.comm_epoll_option = g_default_invalid_epoll_opt;
    t_thrd.comm_poll_option = g_default_invalid_poll_opt;

    knl_t_aes_init(&t_thrd.aes_cxt);
    knl_t_aiocompleter_init(&t_thrd.aio_cxt);
    knl_t_alarmchecker_init(&t_thrd.alarm_cxt);
    knl_t_arch_init(&t_thrd.arch);
    knl_t_xlogcopybackend_init(&t_thrd.sharestoragexlogcopyer_cxt);
    knl_t_barrier_arch_init(&t_thrd.barrier_arch);
    knl_t_async_init(&t_thrd.asy_cxt);
    knl_t_audit_init(&t_thrd.audit);
    knl_t_autovacuum_init(&t_thrd.autovacuum_cxt);
    knl_t_basebackup_init(&t_thrd.basebackup_cxt);
    knl_t_bgwriter_init(&t_thrd.bgwriter_cxt);
    knl_t_bootstrap_init(&t_thrd.bootstrap_cxt);
    knl_t_pagewriter_init(&t_thrd.pagewriter_cxt);
    knl_t_barrier_creator_init(&t_thrd.barrier_creator_cxt);
    knl_t_bulkload_init(&t_thrd.bulk_cxt);
    knl_t_cbm_init(&t_thrd.cbm_cxt);
    knl_t_checkpoint_init(&t_thrd.checkpoint_cxt);
    knl_t_codegen_init(&t_thrd.codegen_cxt);
    knl_t_comm_init(&t_thrd.comm_cxt);
    knl_t_conn_init(&t_thrd.conn_cxt);
    knl_t_contrib_init(&t_thrd.contrib_cxt);
    knl_t_cstore_init(&t_thrd.cstore_cxt);
    knl_t_csnmin_sync_init(&t_thrd.csnminsync_cxt);
    knl_t_dataqueue_init(&t_thrd.dataqueue_cxt);
    knl_t_datarcvwriter_init(&t_thrd.datarcvwriter_cxt);
    knl_t_datareceiver_init(&t_thrd.datareceiver_cxt);
    knl_t_datasender_init(&t_thrd.datasender_cxt);
    knl_t_dfs_init(&t_thrd.dfs_cxt);
    knl_t_dynahash_init(&t_thrd.dyhash_cxt);
    knl_t_explain_init(&t_thrd.explain_cxt);
    knl_t_format_init(&t_thrd.format_cxt);
    knl_t_index_init(&t_thrd.index_cxt);
    knl_t_interrupt_init(&t_thrd.int_cxt);
    knl_t_job_init(&t_thrd.job_cxt);
    knl_t_libwalreceiver_init(&t_thrd.libwalreceiver_cxt);
    knl_t_logger_init(&t_thrd.logger);
    knl_t_logical_init(&t_thrd.logical_cxt);
    knl_t_log_init(&t_thrd.log_cxt);
    knl_t_libpq_init(&t_thrd.libpq_cxt);
    knl_t_lwlockmoniter_init(&t_thrd.lwm_cxt);
    knl_t_mem_init(&t_thrd.mem_cxt);
    knl_t_obs_init(&t_thrd.obs_cxt);
    knl_t_pgxc_init(&t_thrd.pgxc_cxt);
    knl_t_port_init(&t_thrd.port_cxt);
    knl_t_postgres_init(&t_thrd.postgres_cxt);
    knl_t_postmaster_init(&t_thrd.postmaster_cxt);
    knl_t_proc_init(&t_thrd.proc_cxt);
    knl_t_relopt_init(&t_thrd.relopt_cxt);
    knl_t_replgram_init(&t_thrd.replgram_cxt);
    knl_t_replscanner_init(&t_thrd.replscanner_cxt);
    knl_t_shemem_ptr_init(&t_thrd.shemem_ptr_cxt);
    knl_t_sig_init(&t_thrd.sig_cxt);
    knl_t_slot_init(&t_thrd.slot_cxt);
    knl_t_snapshot_init(&t_thrd.snapshot_cxt);
    knl_t_startup_init(&t_thrd.startup_cxt);
    knl_t_stat_init(&t_thrd.stat_cxt);
    knl_t_storage_init(&t_thrd.storage_cxt);
    knl_t_syncgram_init(&t_thrd.syncrepgram_cxt);
    knl_t_syncrep_init(&t_thrd.syncrep_cxt);
    knl_t_syncrepscanner_init(&t_thrd.syncrepscanner_cxt);
    knl_t_time_init(&t_thrd.time_cxt);
    knl_t_tsearch_init(&t_thrd.tsearch_cxt);
    knl_t_twophasecleaner_init(&t_thrd.tpcleaner_cxt);
    knl_t_utils_init(&t_thrd.utils_cxt);
    knl_t_vacuum_init(&t_thrd.vacuum_cxt);
    knl_t_walrcvwriter_init(&t_thrd.walrcvwriter_cxt);
    knl_t_walreceiver_init(&t_thrd.walreceiver_cxt);
    knl_t_walreceiverfuncs_init(&t_thrd.walreceiverfuncs_cxt);
    knl_t_walsender_init(&t_thrd.walsender_cxt);
    knl_t_walwriter_init(&t_thrd.walwriter_cxt);
    knl_t_walwriterauxiliary_init(&t_thrd.walwriterauxiliary_cxt);
    knl_t_catchup_init(&t_thrd.catchup_cxt);
    knl_t_wlm_init(&t_thrd.wlm_cxt);
    knl_t_xact_init(&t_thrd.xact_cxt);
    knl_t_xlog_init(&t_thrd.xlog_cxt);
    knl_t_ash_init(&t_thrd.ash_cxt);
    knl_t_statement_init(&t_thrd.statement_cxt);
    knl_t_pencentile_init(&t_thrd.percentile_cxt);
    knl_t_perf_snap_init(&t_thrd.perf_snap_cxt);
    knl_t_page_redo_init(&t_thrd.page_redo_cxt);
    knl_t_parallel_decode_init(&t_thrd.parallel_decode_cxt);
    knl_t_parallel_decode_reader_init(&t_thrd.logicalreadworker_cxt);
    knl_t_heartbeat_init(&t_thrd.heartbeat_cxt);
    knl_t_streaming_init(&t_thrd.streaming_cxt);
    knl_t_poolcleaner_init(&t_thrd.poolcleaner_cxt);
    knl_t_ts_compaction_init(&t_thrd.ts_compaction_cxt);
    KnlTUndoInit(&t_thrd.undo_cxt);
    KnlTUndolauncherInit(&t_thrd.undolauncher_cxt);
    KnlTUndoworkerInit(&t_thrd.undoworker_cxt);
    KnlTUndorecyclerInit(&t_thrd.undorecycler_cxt);
    KnlTRollbackRequestsInit(&t_thrd.rollback_requests_cxt);
    knl_t_security_policy_init(&t_thrd.security_policy_cxt);
    knl_t_security_ledger_init(&t_thrd.security_ledger_cxt);
    knl_t_bgworker_init(&t_thrd.bgworker_cxt);
    knl_index_advisor_init(&t_thrd.index_advisor_cxt);
    KnlTApplyLauncherInit(&t_thrd.applylauncher_cxt);
    KnlTApplyWorkerInit(&t_thrd.applyworker_cxt);
    KnlTPublicationInit(&t_thrd.publication_cxt);
#ifdef ENABLE_MOT
    knl_t_mot_init(&t_thrd.mot_cxt);
#endif

    KnlTGstatInit(&t_thrd.gstat_cxt);

#ifdef DEBUG_UHEAP
    knl_t_uheap_stats_init(&t_thrd.uheap_stats_cxt);
#endif
    KnlDcfContextInit(&t_thrd.dcf_cxt);

}

__attribute__ ((__used__)) knl_thrd_context *GetCurrentThread()
{
    return &t_thrd;
}

RedoInterruptCallBackFunc RegisterRedoInterruptCallBack(RedoInterruptCallBackFunc func)
{
    RedoInterruptCallBackFunc oldFunc = t_thrd.xlog_cxt.redoInterruptCallBackFunc;
    t_thrd.xlog_cxt.redoInterruptCallBackFunc = func;
    return oldFunc;
}

RedoPageRepairCallBackFunc RegisterRedoPageRepairCallBack(RedoPageRepairCallBackFunc func)
{
    RedoPageRepairCallBackFunc oldFunc = t_thrd.xlog_cxt.redoPageRepairCallBackFunc;
    t_thrd.xlog_cxt.redoPageRepairCallBackFunc = func;
    return oldFunc;
}


void RedoInterruptCallBack()
{
    if (t_thrd.xlog_cxt.redoInterruptCallBackFunc != NULL) {
        t_thrd.xlog_cxt.redoInterruptCallBackFunc();
        return;
    }

    Assert(!AmStartupProcess());
    Assert(!AmPageRedoWorker());
}

void RedoPageRepairCallBack(RepairBlockKey key, XLogPhyBlock pblk)
{
    if (t_thrd.xlog_cxt.redoPageRepairCallBackFunc != NULL) {
        t_thrd.xlog_cxt.redoPageRepairCallBackFunc(key, pblk);
    }
}

