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
 * mot_internal.cpp
 *    MOT Foreign Data Wrapper internal interfaces to the MOT engine.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_internal.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <ostream>
#include <istream>
#include <iomanip>
#include <pthread.h>
#include <cstring>

#include "postgres.h"
#include "access/sysattr.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/syscache.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "commands/dbcommands.h"
#include "knl/knl_session.h"
#include "utils/date.h"

#include "mot_internal.h"
#include "mot_fdw_helpers.h"
#include "row.h"
#include "log_statistics.h"
#include "spin_lock.h"
#include "txn.h"
#include "table.h"
#include "utilities.h"
#include "mot_engine.h"
#include "sentinel.h"
#include "txn.h"
#include "txn_access.h"
#include "index_factory.h"
#include "column.h"
#include "mm_raw_chunk_store.h"
#include "ext_config_loader.h"
#include "config_manager.h"
#include "mot_error.h"
#include "utilities.h"
#include "jit_context.h"
#include "mm_cfg.h"
#include "jit_statistics.h"
#include "gaussdb_config_loader.h"

MOT::MOTEngine* MOTAdaptor::m_engine = nullptr;
static XLOGLogger g_xlogger;
static SnapshotManager g_snapshotMgr;

// enable MOT Engine logging facilities
DECLARE_LOGGER(InternalExecutor, FDW)

// in a thread-pooled environment we need to ensure thread-locals are initialized properly
static inline bool EnsureSafeThreadAccessInline(bool throwError = true)
{
    if (MOTCurrThreadId == INVALID_THREAD_ID) {
        MOT_LOG_DEBUG("Initializing safe thread access for current thread");
        if (MOT::AllocThreadId() == INVALID_THREAD_ID) {
            MOT_LOG_ERROR("Failed to allocate thread identifier");
            if (throwError) {
                ereport(ERROR, (errmodule(MOD_MOT), errmsg("Failed to allocate thread identifier")));
            }
            return false;
        }
        // register for cleanup only once - not having a current thread id is the safe indicator we never registered
        // proc-exit callback for this thread
        if (g_instance.attr.attr_common.enable_thread_pool) {
            on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
            MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
        }
    }
    if (MOTCurrentNumaNodeId == MEM_INVALID_NODE) {
        if (!MOT::InitCurrentNumaNodeId()) {
            MOT_LOG_ERROR("Failed to allocate NUMA node identifier");
            if (throwError) {
                ereport(ERROR, (errmodule(MOD_MOT), errmsg("Failed to allocate NUMA node identifier")));
            }
            return false;
        }
    }
    if (!MOT::InitMasstreeThreadinfo()) {
        MOT_LOG_ERROR("Failed to initialize thread-local masstree info");
        if (throwError) {
            ereport(ERROR, (errmodule(MOD_MOT), errmsg("Failed to initialize thread-local masstree info")));
        }
        return false;
    }
    return true;
}

extern bool EnsureSafeThreadAccess(bool throwError /* = true */)
{
    return EnsureSafeThreadAccessInline(throwError);
}

static GaussdbConfigLoader* gaussdbConfigLoader = nullptr;

bool MOTAdaptor::m_initialized = false;
bool MOTAdaptor::m_callbacks_initialized = false;

static void WakeupWalWriter()
{
    if (g_instance.proc_base->walwriterLatch != nullptr) {
        SetLatch(g_instance.proc_base->walwriterLatch);
    }
}

void MOTAdaptor::Init()
{
    if (m_initialized) {
        // This is highly unexpected, and should especially be guarded in scenario of switch-over to standby.
        elog(FATAL, "Double attempt to initialize MOT engine, it is already initialized");
    }

    m_engine = MOT::MOTEngine::CreateInstanceNoInit(g_instance.attr.attr_common.MOTConfigFileName, 0, nullptr);
    if (m_engine == nullptr) {
        elog(FATAL, "Failed to create MOT engine");
    }

    MOT::MOTConfiguration& motCfg = MOT::GetGlobalConfiguration();
    motCfg.SetTotalMemoryMb(g_instance.attr.attr_memory.max_process_memory / KILO_BYTE);

    gaussdbConfigLoader = new (std::nothrow) GaussdbConfigLoader();
    if (gaussdbConfigLoader == nullptr) {
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to allocate memory for GaussDB/MOTEngine configuration loader.");
    }
    MOT_LOG_TRACE("Adding external configuration loader for GaussDB");
    if (!m_engine->AddConfigLoader(gaussdbConfigLoader)) {
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to add GaussDB/MOTEngine configuration loader");
    }

    if (!m_engine->LoadConfig()) {
        (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to load configuration for MOT engine.");
    }

    // Check max process memory here - we do it anyway to protect ourselves from miscalculations.
    // Attention: the following values are configured during the call to MOTEngine::LoadConfig() just above
    uint64_t globalMemoryKb = MOT::g_memGlobalCfg.m_maxGlobalMemoryMb * KILO_BYTE;
    uint64_t localMemoryKb = MOT::g_memGlobalCfg.m_maxLocalMemoryMb * KILO_BYTE;
    uint64_t maxReserveMemoryKb = globalMemoryKb + localMemoryKb;

    // check whether the 2GB gap between MOT and envelope is still kept
    if ((g_instance.attr.attr_memory.max_process_memory < (int32)maxReserveMemoryKb) ||
        ((g_instance.attr.attr_memory.max_process_memory - maxReserveMemoryKb) < MIN_DYNAMIC_PROCESS_MEMORY)) {
        // we allow one extreme case: GaussDB is configured to its limit, and zero memory is left for us
        if (maxReserveMemoryKb <= MOT::MOTConfiguration::MOT_MIN_MEMORY_USAGE_MB * KILO_BYTE) {
            MOT_LOG_WARN("Allowing MOT to work in minimal memory mode");
        } else {
            (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
            delete gaussdbConfigLoader;
            gaussdbConfigLoader = nullptr;
            MOT::MOTEngine::DestroyInstance();
            elog(FATAL,
                "The value of pre-reserved memory for MOT engine is not reasonable: "
                "Request for a maximum of %" PRIu64 " KB global memory, and %" PRIu64
                " KB session memory (total of %" PRIu64 " KB) is invalid since max_process_memory is %d KB",
                globalMemoryKb,
                localMemoryKb,
                maxReserveMemoryKb,
                g_instance.attr.attr_memory.max_process_memory);
        }
    }

    if (!m_engine->Initialize()) {
        (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize MOT engine.");
    }

    if (!JitExec::JitStatisticsProvider::CreateInstance()) {
        (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize JIT statistics.");
    }

    // make sure current thread is cleaned up properly when thread pool is enabled
    // avoid throwing errors on failure
    if (!EnsureSafeThreadAccessInline(false)) {
        (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
        MOT::MOTEngine::DestroyInstance();
        elog(FATAL, "Failed to initialize thread-local data.");
    }

    if (motCfg.m_enableRedoLog && motCfg.m_loggerType == MOT::LoggerType::EXTERNAL_LOGGER) {
        m_engine->GetRedoLogHandler()->SetLogger(&g_xlogger);
        m_engine->GetRedoLogHandler()->SetWalWakeupFunc(WakeupWalWriter);
    }

    InitSessionDetailsMap();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        InitSessionCleanup();
    }
    InitKeyOperStateMachine();

    MOT_LOG_INFO("Switching to External snapshot manager");
    m_engine->SetCSNManager(&g_snapshotMgr);

    m_initialized = true;
}

void MOTAdaptor::NotifyConfigChange()
{
    if (gaussdbConfigLoader != nullptr) {
        gaussdbConfigLoader->MarkChanged();
    }
}

void MOTAdaptor::Destroy()
{
    if (!m_initialized) {
        return;
    }

    JitExec::JitStatisticsProvider::DestroyInstance();
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        DestroySessionCleanup();
    }
    DestroySessionDetailsMap();
    if (gaussdbConfigLoader != nullptr) {
        (void)m_engine->RemoveConfigLoader(gaussdbConfigLoader);
        delete gaussdbConfigLoader;
        gaussdbConfigLoader = nullptr;
    }

    // avoid throwing errors and ignore them at this phase
    (void)EnsureSafeThreadAccessInline(false);
    MOT::MOTEngine::DestroyInstance();
    m_engine = nullptr;
    knl_thread_mot_init();  // reset all thread-locals, mandatory for standby switch-over
    m_initialized = false;
}

MOT::TxnManager* MOTAdaptor::InitTxnManager(
    const char* callerSrc, MOT::ConnectionId connection_id /* = INVALID_CONNECTION_ID */)
{
    if (!u_sess->mot_cxt.txn_manager) {
        bool attachCleanFunc =
            (MOTCurrThreadId == INVALID_THREAD_ID ? true : !g_instance.attr.attr_common.enable_thread_pool);

        // First time we handle this connection
        if (m_engine == nullptr) {
            elog(ERROR, "initTxnManager: MOT engine is not initialized");
            return nullptr;
        }

        // create new session context
        MOT::SessionContext* session_ctx =
            MOT::GetSessionManager()->CreateSessionContext(IS_PGXC_COORDINATOR, 0, nullptr, connection_id);
        if (session_ctx == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Session Initialization", "Failed to create session context in %s", callerSrc);
            ereport(ERROR, (errmsg("Session startup: failed to create session context.")));
            return nullptr;
        }
        MOT_ASSERT(u_sess->mot_cxt.session_context == session_ctx);
        MOT_ASSERT(u_sess->mot_cxt.session_id == session_ctx->GetSessionId());
        MOT_ASSERT(u_sess->mot_cxt.connection_id == session_ctx->GetConnectionId());

        // make sure we cleanup leftovers from other session
        u_sess->mot_cxt.jit_context_count = 0;

        // record session details for statistics report
        RecordSessionDetails();

        if (attachCleanFunc) {
            // schedule session cleanup when thread pool is not used
            if (!g_instance.attr.attr_common.enable_thread_pool) {
                on_proc_exit(DestroyTxn, PointerGetDatum(session_ctx));
                ScheduleSessionCleanup();
            } else {
                on_proc_exit(MOTCleanupThread, PointerGetDatum(nullptr));
                MOT_LOG_DEBUG("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
            }
        }

        u_sess->mot_cxt.txn_manager = session_ctx->GetTxnManager();
        elog(DEBUG1, "Init TXN_MAN for thread %" PRIu16, MOTCurrThreadId);
    }

    return u_sess->mot_cxt.txn_manager;
}

void MOTAdaptor::DestroyTxn(int status, Datum ptr)
{
    MOT_ASSERT(!g_instance.attr.attr_common.enable_thread_pool);

    // cleanup session
    if (!g_instance.attr.attr_common.enable_thread_pool) {
        CancelSessionCleanup();
    }
    ClearCurrentSessionDetails();
    MOT_LOG_DEBUG("DestroyTxn(): Calling DestroySessionJitContexts()");
    DestroySessionJitContexts();
    MOT::SessionContext* session = (MOT::SessionContext*)DatumGetPointer(ptr);
    if (m_engine == nullptr) {
        elog(ERROR, "destroyTxn: MOT engine is not initialized");
    }

    if (session != MOT_GET_CURRENT_SESSION_CONTEXT()) {
        MOT_LOG_WARN("Ignoring request to delete session context: already deleted");
    } else if (session != nullptr) {
        elog(DEBUG1, "Destroy SessionContext, connection_id = %u", session->GetConnectionId());
        // initialize thread data, since this call may be accessed from new thread pool worker
        // avoid throwing errors and ignore them at this phase
        (void)EnsureSafeThreadAccessInline(false);
        MOT::GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
        if (gc != nullptr) {
            gc->GcEndTxn();
        }
        DestroySession(session);
    }

    // clean up thread
    MOTOnThreadShutdown();
}

MOT::RC MOTAdaptor::ValidateCommit()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->ValidateCommit();
    } else {
        // Nothing to do in coordinator
        return MOT::RC_OK;
    }
}

void MOTAdaptor::RecordCommit(uint64_t csn)
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->RecordCommit();
    } else {
        txn->LiteCommit();
    }
}

MOT::RC MOTAdaptor::Commit(uint64_t csn)
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Commit();
    } else {
        txn->LiteCommit();
        return MOT::RC_OK;
    }
}

bool MOTAdaptor::IsTxnWriteSetEmpty()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (txn->m_txnDdlAccess->Size() > 0 or txn->m_accessMgr->Size() > 0) {
        return false;
    }
    return true;
}

void MOTAdaptor::EndTransaction()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    // Nothing to do in coordinator
    if (!IS_PGXC_COORDINATOR) {
        txn->EndTransaction();
    }
}

void MOTAdaptor::Rollback()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->Rollback();
    } else {
        txn->LiteRollback();
    }
}

MOT::RC MOTAdaptor::Prepare()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        return txn->Prepare();
    } else {
        txn->LitePrepare();
        return MOT::RC_OK;
    }
}

void MOTAdaptor::CommitPrepared(uint64_t csn)
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetCommitSequenceNumber(csn);
    if (!IS_PGXC_COORDINATOR) {
        txn->CommitPrepared();
    } else {
        txn->LiteCommitPrepared();
    }
}

void MOTAdaptor::RollbackPrepared()
{
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    if (!IS_PGXC_COORDINATOR) {
        txn->RollbackPrepared();
    } else {
        txn->LiteRollbackPrepared();
    }
}

MOT::RC MOTAdaptor::InsertRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    (void)EnsureSafeThreadAccessInline();
    uint8_t* newRowData = nullptr;
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::Table* table = fdwState->m_table;
    MOT::Row* row = table->CreateNewRow();
    if (row == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to create new row for table %s", table->GetLongTableName().c_str());
        return MOT::RC_MEMORY_ALLOCATION_ERROR;
    }
    newRowData = const_cast<uint8_t*>(row->GetData());
    PackRow(slot, table, fdwState->m_attrsUsed, newRowData);

    MOT::RC res = table->InsertRow(row, fdwState->m_currTxn);
    if ((res != MOT::RC_OK) && (res != MOT::RC_UNIQUE_VIOLATION)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Insert Row", "Failed to insert new row for table %s", table->GetLongTableName().c_str());
    }
    return res;
}

MOT::RC MOTAdaptor::UpdateRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot, MOT::Row* currRow)
{
    (void)EnsureSafeThreadAccessInline();
    MOT::RC rc;
    MOT::TxnIxColUpdate colUpd(
        fdwState->m_table, fdwState->m_currTxn, fdwState->m_attrsModified, fdwState->m_hasIndexedColUpdate);

    do {
        fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
        rc = fdwState->m_currTxn->UpdateLastRowState(MOT::AccessType::WR);
        if (rc != MOT::RC::RC_OK) {
            break;
        }
        MOT::Row* currDraft = fdwState->m_currTxn->GetLastAccessedDraft();
        if (unlikely(fdwState->m_hasIndexedColUpdate != MOT::UpdateIndexColumnType::UPDATE_COLUMN_NONE)) {
            rc = colUpd.InitAndBuildOldKeys(currDraft);
            if (rc != MOT::RC::RC_OK) {
                break;
            }
        }
        uint8_t* rowData = const_cast<uint8_t*>(currDraft->GetData());
        PackUpdateRow(slot, fdwState->m_table, fdwState->m_attrsModified, rowData);
        MOT::BitmapSet modified_columns(fdwState->m_attrsModified, fdwState->m_table->GetFieldCount() - 1);
        if (unlikely(fdwState->m_hasIndexedColUpdate)) {
            rc = colUpd.FilterColumnUpdate(currDraft);
            if (rc != MOT::RC::RC_OK) {
                break;
            }
            rc = fdwState->m_currTxn->UpdateRow(modified_columns, &colUpd);
        } else {
            rc = fdwState->m_currTxn->OverwriteRow(currDraft, modified_columns);
        }
    } while (0);

    return rc;
}

MOT::RC MOTAdaptor::DeleteRow(MOTFdwStateSt* fdwState, TupleTableSlot* slot)
{
    (void)EnsureSafeThreadAccessInline();
    fdwState->m_currTxn->SetTransactionId(fdwState->m_txnId);
    MOT::RC rc = fdwState->m_currTxn->DeleteLastRow();
    return rc;
}

bool MOTAdaptor::IsColumnIndexed(int16_t colId, MOT::Table* table)
{
    MOT::Column* col = table->GetField(colId);
    if (col != nullptr && col->IsUsedByIndex()) {
        return true;
    }
    return false;
}

// NOTE: colId starts from 1
bool MOTAdaptor::SetMatchingExpr(
    MOTFdwStateSt* state, MatchIndexArr* marr, int16_t colId, KEY_OPER op, Expr* expr, Expr* parent, bool set_local)
{
    bool res = false;
    uint16_t numIx = state->m_table->GetNumIndexes();

    for (uint16_t i = 0; i < numIx; i++) {
        MOT::Index* ix = state->m_table->GetIndex(i);
        if (ix != nullptr && ix->IsFieldPresent(colId)) {
            if (marr->m_idx[i] == nullptr) {
                marr->m_idx[i] = (MatchIndex*)palloc0(sizeof(MatchIndex));
                marr->m_idx[i]->Init();
                marr->m_idx[i]->m_ix = ix;
            }

            res |= marr->m_idx[i]->SetIndexColumn(state, colId, op, expr, parent, set_local);
        }
    }

    return res;
}

MatchIndex* MOTAdaptor::GetBestMatchIndex(MOTFdwStateSt* festate, MatchIndexArr* marr, int numClauses, bool setLocal)
{
    MatchIndex* best = nullptr;
    double bestCost = INT_MAX;
    uint16_t numIx = festate->m_table->GetNumIndexes();
    uint16_t bestI = (uint16_t)-1;

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr && marr->m_idx[i]->IsUsable()) {
            double cost = marr->m_idx[i]->GetCost(numClauses);
            if (cost < bestCost) {
                if (bestI < MAX_NUM_INDEXES) {
                    if (marr->m_idx[i]->GetNumMatchedCols() < marr->m_idx[bestI]->GetNumMatchedCols()) {
                        continue;
                    }
                }
                bestCost = cost;
                bestI = i;
            }
        }
    }

    if (bestI < MAX_NUM_INDEXES) {
        best = marr->m_idx[bestI];
        for (int k = 0; k < 2; k++) {
            for (int j = 0; j < best->m_ix->GetNumFields(); j++) {
                if (best->m_colMatch[k][j]) {
                    if (best->m_opers[k][j] < KEY_OPER::READ_INVALID) {
                        best->m_params[k][j] = AddParam(&best->m_remoteConds, best->m_colMatch[k][j]);
                        if (!list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                            best->m_remoteCondsOrig = lappend(best->m_remoteCondsOrig, best->m_parentColMatch[k][j]);
                        }

                        if (j > 0 && best->m_opers[k][j - 1] != KEY_OPER::READ_KEY_EXACT &&
                            !list_member(festate->m_localConds, best->m_parentColMatch[k][j])) {
                            if (setLocal) {
                                festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                            }
                        }
                    } else if (!list_member(festate->m_localConds, best->m_parentColMatch[k][j]) &&
                               !list_member(best->m_remoteCondsOrig, best->m_parentColMatch[k][j])) {
                        if (setLocal) {
                            festate->m_localConds = lappend(festate->m_localConds, best->m_parentColMatch[k][j]);
                        }
                        best->m_colMatch[k][j] = nullptr;
                        best->m_parentColMatch[k][j] = nullptr;
                    }
                }
            }
        }
    }

    for (uint16_t i = 0; i < numIx; i++) {
        if (marr->m_idx[i] != nullptr) {
            MatchIndex* mix = marr->m_idx[i];
            if (i != bestI) {
                if (setLocal) {
                    for (int k = 0; k < 2; k++) {
                        for (int j = 0; j < mix->m_ix->GetNumFields(); j++) {
                            if (mix->m_colMatch[k][j] &&
                                !list_member(festate->m_localConds, mix->m_parentColMatch[k][j]) &&
                                !(best != nullptr &&
                                    list_member(best->m_remoteCondsOrig, mix->m_parentColMatch[k][j]))) {
                                festate->m_localConds = lappend(festate->m_localConds, mix->m_parentColMatch[k][j]);
                            }
                        }
                    }
                }
                pfree(mix);
                marr->m_idx[i] = nullptr;
            }
        }
    }
    if (best != nullptr && best->m_ix != nullptr) {
        for (uint16_t i = 0; i < numIx; i++) {
            if (best->m_ix == festate->m_table->GetIndex(i)) {
                best->m_ixPosition = i;
                break;
            }
        }
    }

    return best;
}

void MOTAdaptor::OpenCursor(Relation rel, MOTFdwStateSt* festate)
{
    bool matchKey = true;
    bool forwardDirection = true;
    bool found = false;

    (void)EnsureSafeThreadAccessInline();

    // GetTableByExternalId cannot return nullptr at this stage, because it is protected by envelope's table lock.
    festate->m_table = festate->m_currTxn->GetTableByExternalId(rel->rd_id);

    do {
        // this scan all keys case
        // we need to open both cursors on start and end to prevent
        // infinite scan in case "insert into table A ... as select * from table A ...
        if (festate->m_bestIx == nullptr || festate->m_bestIx->m_fullScan) {
            // assumption that primary index cannot be changed, can take it from
            // table and not look on ddl_access
            MOT::Index* ix = festate->m_bestIx ? festate->m_bestIx->m_ix : festate->m_table->GetPrimaryIndex();
            uint16_t keyLength = ix->GetKeyLength();

            if (festate->m_order == SortDir::SORTDIR_ASC) {
                festate->m_cursor[0] = ix->Begin(festate->m_currTxn->GetThdId());
                festate->m_forwardDirectionScan = true;
                festate->m_cursor[1] = nullptr;
            } else {
                festate->m_stateKey[0].InitKey(keyLength);
                uint8_t* buf = festate->m_stateKey[0].GetKeyBuf();
                errno_t erc = memset_s(buf, keyLength, 0xff, keyLength);
                securec_check(erc, "\0", "\0");
                festate->m_cursor[0] =
                    ix->Search(&festate->m_stateKey[0], false, false, festate->m_currTxn->GetThdId(), found);
                festate->m_forwardDirectionScan = false;
                festate->m_cursor[1] = nullptr;
            }
            break;
        }

        for (int i = 0; i < 2; i++) {
            if (i == 1 && festate->m_bestIx->m_end < 0) {
                festate->m_cursor[1] = nullptr;
                break;
            }

            KEY_OPER oper = (i == 0 ? festate->m_bestIx->m_ixOpers[0] : festate->m_bestIx->m_ixOpers[1]);

            forwardDirection = ((static_cast<uint8_t>(oper) & ~KEY_OPER_PREFIX_BITMASK) <
                                static_cast<uint8_t>(KEY_OPER::READ_KEY_OR_PREV));

            CreateKeyBuffer(rel, festate, i);

            if (i == 0) {
                festate->m_forwardDirectionScan = forwardDirection;
            }

            switch (oper) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_PREFIX_LIKE:
                case KEY_OPER::READ_PREFIX:
                case KEY_OPER::READ_PREFIX_OR_NEXT:
                    matchKey = true;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_AFTER:
                case KEY_OPER::READ_PREFIX_AFTER:
                    matchKey = false;
                    forwardDirection = true;
                    break;

                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_PREFIX_OR_PREV:
                    matchKey = true;
                    forwardDirection = false;
                    break;

                case KEY_OPER::READ_KEY_BEFORE:
                case KEY_OPER::READ_PREFIX_BEFORE:
                    matchKey = false;
                    forwardDirection = false;
                    break;

                default:
                    elog(INFO, "Invalid key operation: %" PRIu8, static_cast<uint8_t>(oper));
                    break;
            }

            festate->m_cursor[i] = festate->m_bestIx->m_ix->Search(
                &festate->m_stateKey[i], matchKey, forwardDirection, festate->m_currTxn->GetThdId(), found);

            if (!found && oper == KEY_OPER::READ_KEY_EXACT && festate->m_bestIx->m_ix->GetUnique()) {
                festate->m_cursor[i]->Invalidate();
                festate->m_cursor[i]->Destroy();
                delete festate->m_cursor[i];
                festate->m_cursor[i] = nullptr;
            }
        }
    } while (0);
    for (int i = 0; i < 2; i++) {
        if (festate->m_cursor[i] != nullptr) {
            festate->m_currTxn->m_queryState[(uint64_t)festate->m_cursor[i]] = (uint64_t)(festate->m_cursor[i]);
        }
    }
}

static void VarLenFieldType(
    Form_pg_type typeDesc, Oid typoid, int32_t colLen, int16* typeLen, bool& isBlob, MOT::RC& res)
{
    isBlob = false;
    res = MOT::RC_OK;
    if (typeDesc->typlen < 0) {
        *typeLen = colLen;
        switch (typeDesc->typstorage) {
            case 'p':
                break;
            case 'x':
            case 'm':
                if (typoid == NUMERICOID) {
                    *typeLen = DECIMAL_MAX_SIZE;
                    break;
                }
                /* fall through */
            case 'e':
#ifdef MOT_SUPPORT_TEXT_FIELD
                if (typoid == TEXTOID)
                    *typeLen = colLen = MAX_VARCHAR_LEN;
#endif
                if (colLen > MAX_VARCHAR_LEN || colLen < 0) {
                    res = MOT::RC_COL_SIZE_INVALID;
                } else {
                    isBlob = true;
                }
                break;
            default:
                break;
        }
    }
}

static MOT::RC TableFieldType(
    const ColumnDef* colDef, MOT::MOT_CATALOG_FIELD_TYPES& type, int16* typeLen, Oid& typoid, bool& isBlob)
{
    MOT::RC res = MOT::RC_OK;
    Type tup;
    Form_pg_type typeDesc;
    int32_t colLen;

    if (colDef->typname->arrayBounds != nullptr) {
        return MOT::RC_UNSUPPORTED_COL_TYPE_ARR;
    }

    tup = typenameType(nullptr, colDef->typname, &colLen);
    typeDesc = ((Form_pg_type)GETSTRUCT(tup));
    typoid = HeapTupleGetOid(tup);
    *typeLen = typeDesc->typlen;

    // Get variable-length field length.
    VarLenFieldType(typeDesc, typoid, colLen, typeLen, isBlob, res);

    switch (typoid) {
        case CHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_CHAR;
            break;
        case INT1OID:
        case BOOLOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINY;
            break;
        case INT2OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_SHORT;
            break;
        case INT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INT;
            break;
        case INT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
            break;
        case DATEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DATE;
            break;
        case TIMEOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIME;
            break;
        case TIMESTAMPOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMP;
            break;
        case TIMESTAMPTZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMESTAMPTZ;
            break;
        case INTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_INTERVAL;
            break;
        case TINTERVALOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TINTERVAL;
            break;
        case TIMETZOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_TIMETZ;
            break;
        case FLOAT4OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_FLOAT;
            break;
        case FLOAT8OID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DOUBLE;
            break;
        case NUMERICOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL;
            break;
        case VARCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case BPCHAROID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case TEXTOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        case CLOBOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB;
            break;
        case BYTEAOID:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
            break;
        default:
            type = MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
            res = MOT::RC_UNSUPPORTED_COL_TYPE;
    }

    if (tup) {
        ReleaseSysCache(tup);
    }

    return res;
}

static void ValidateCreateIndex(IndexStmt* stmt, MOT::Table* table, MOT::TxnManager* txn)
{
    if (stmt->primary) {
        if (!table->IsTableEmpty(txn->GetThdId())) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg(
                        "Table %s is not empty, create primary index is not allowed", table->GetTableName().c_str())));
        }
    } else if (table->GetNumIndexes() >= MAX_NUM_INDEXES) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                errmsg("Cannot create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
    }

    if (strcmp(stmt->accessMethod, "btree") != 0) {
        ereport(ERROR, (errmodule(MOD_MOT), errmsg("MOT supports indexes of type BTREE only (btree or btree_art)")));
    }

    if (list_length(stmt->indexParams) > (int)MAX_KEY_COLUMNS) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_FDW_TOO_MANY_INDEX_COLUMNS),
                errmsg("Can't create index"),
                errdetail(
                    "Number of columns exceeds %d max allowed %u", list_length(stmt->indexParams), MAX_KEY_COLUMNS)));
    }
}

MOT::RC MOTAdaptor::CreateIndex(IndexStmt* stmt, ::TransactionId tid)
{
    MOT::RC res;
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    MOT::Table* table = txn->GetTableByExternalId(stmt->relation->foreignOid);

    if (table == nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("Table not found for oid %u", stmt->relation->foreignOid)));
        return MOT::RC_ERROR;
    }

    table->GetOrigTable()->WrLock();

    PG_TRY();
    {
        ValidateCreateIndex(stmt, table, txn);
    }
    PG_CATCH();
    {
        table->GetOrigTable()->Unlock();
        PG_RE_THROW();
    }
    PG_END_TRY();

    elog(LOG,
        "creating %s index %s (OID: %u), for table: %s",
        (stmt->primary ? "PRIMARY" : "SECONDARY"),
        stmt->idxname,
        stmt->indexOid,
        stmt->relation->relname);
    uint32_t keyLength = 0;
    MOT::Index* index = nullptr;
    MOT::IndexOrder index_order = MOT::IndexOrder::INDEX_ORDER_SECONDARY;

    // Use the default index tree flavor from configuration file
    MOT::IndexingMethod indexing_method = MOT::IndexingMethod::INDEXING_METHOD_TREE;
    MOT::IndexTreeFlavor flavor = MOT::GetGlobalConfiguration().m_indexTreeFlavor;

    // check if we have primary and delete previous definition
    if (stmt->primary) {
        index_order = MOT::IndexOrder::INDEX_ORDER_PRIMARY;
    } else {
        if (stmt->unique) {
            index_order = MOT::IndexOrder::INDEX_ORDER_SECONDARY_UNIQUE;
        }
    }

    index = MOT::IndexFactory::CreateIndex(index_order, indexing_method, flavor);
    if (index == nullptr) {
        table->GetOrigTable()->Unlock();
        report_pg_error(MOT::RC_ABORT);
        return MOT::RC_ABORT;
    }
    index->SetExtId(stmt->indexOid);
    if (!index->SetNumTableFields(table->GetFieldCount())) {
        table->GetOrigTable()->Unlock();
        delete index;
        report_pg_error(MOT::RC_ABORT);
        return MOT::RC_ABORT;
    }

    int count = 0;

    ListCell* lc = nullptr;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);
        if (ielem->expr != nullptr) {
            table->GetOrigTable()->Unlock();
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Expressions are not supported")));
            return MOT::RC_ERROR;
        }

        uint64_t colid = table->GetFieldId((ielem->name != nullptr ? ielem->name : ielem->indexcolname));
        if (colid == (uint64_t)-1) {  // invalid column
            table->GetOrigTable()->Unlock();
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Specified column not found in table definition")));
            return MOT::RC_ERROR;
        }

        MOT::Column* col = table->GetField(colid);

        // Temp solution for NULLs, do not allow index creation on column that does not carry not null flag
        if (!MOT::GetGlobalConfiguration().m_allowIndexOnNullableColumn && !col->m_isNotNull) {
            table->GetOrigTable()->Unlock();
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_INDEX_ON_NULLABLE_COLUMN_NOT_ALLOWED),
                    errmsg("Can't create index on nullable columns"),
                    errdetail("Column %s is nullable", col->m_name)));
            return MOT::RC_ERROR;
        }

        // Temp solution, we have to support DECIMAL and NUMERIC indexes as well
        if (col->m_type == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
            table->GetOrigTable()->Unlock();
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Can't create index on field"),
                    errdetail("INDEX on NUMERIC or DECIMAL fields not supported yet")));
            return MOT::RC_ERROR;
        }
        if (col->m_keySize > MAX_KEY_SIZE) {
            table->GetOrigTable()->Unlock();
            delete index;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Can't create index on field"),
                    errdetail("Column size is greater than maximum index size")));
            return MOT::RC_ERROR;
        }
        keyLength += col->m_keySize;

        index->SetLenghtKeyFields(count, colid, col->m_keySize);
        count++;
    }

    index->SetNumIndexFields(count);

    if ((res = index->IndexInit(keyLength, stmt->unique, stmt->idxname, nullptr)) != MOT::RC_OK) {
        table->GetOrigTable()->Unlock();
        delete index;
        report_pg_error(res);
        return res;
    }

    res = txn->CreateIndex(table, index, stmt->primary);
    if (res == MOT::RC_OK) {
        MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
            index->GetExtId(), MOT::DDL_ACCESS_CREATE_INDEX, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
    }
    table->GetOrigTable()->Unlock();
    if (res != MOT::RC_OK) {
        delete index;
        if (res == MOT::RC_TABLE_EXCEEDS_MAX_INDEXES) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TOO_MANY_INDEXES),
                    errmsg("Cannot create index, max number of indexes %u reached", MAX_NUM_INDEXES)));
            return MOT::RC_TABLE_EXCEEDS_MAX_INDEXES;
        }
        report_pg_error(txn->m_err, stmt->idxname, txn->m_errMsgBuf);
        return MOT::RC_UNIQUE_VIOLATION;
    }

    return MOT::RC_OK;
}

static void CalculateDecimalColumnTypeLen(MOT::MOT_CATALOG_FIELD_TYPES colType, ColumnDef* colDef, int16& typeLen)
{
    if (colType == MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_DECIMAL) {
        if (list_length(colDef->typname->typmods) > 0) {
            bool canMakeShort = true;
            int precision = 0;
            int scale = 0;
            int count = 0;

            ListCell* c = nullptr;
            foreach (c, colDef->typname->typmods) {
                Node* d = (Node*)lfirst(c);
                if (!IsA(d, A_Const)) {
                    canMakeShort = false;
                    break;
                }
                A_Const* ac = (A_Const*)d;

                if (ac->val.type != T_Integer) {
                    canMakeShort = false;
                    break;
                }

                if (count == 0) {
                    precision = ac->val.val.ival;
                } else {
                    scale = ac->val.val.ival;
                }

                count++;
            }

            if (canMakeShort) {
                int len = 0;

                len += scale / DEC_DIGITS;
                len += (scale % DEC_DIGITS > 0 ? 1 : 0);

                precision -= scale;

                len += precision / DEC_DIGITS;
                len += (precision % DEC_DIGITS > 0 ? 1 : 0);

                typeLen = sizeof(MOT::DecimalSt) + len * sizeof(NumericDigit);
            }
        }
    }
}

void MOTAdaptor::AddTableColumns(MOT::Table* table, List* tableElts, bool& hasBlob)
{
    hasBlob = false;
    ListCell* cell = nullptr;
    foreach (cell, tableElts) {
        int16 typeLen = 0;
        bool isBlob = false;
        MOT::MOT_CATALOG_FIELD_TYPES colType;
        ColumnDef* colDef = (ColumnDef*)lfirst(cell);

        if (colDef == nullptr || colDef->typname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmsg("Column definition is not complete"),
                    errdetail("target table is a foreign table")));
            break;
        }

        Oid typoid = InvalidOid;
        MOT::RC res = TableFieldType(colDef, colType, &typeLen, typoid, isBlob);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
        hasBlob |= isBlob;

        CalculateDecimalColumnTypeLen(colType, colDef, typeLen);

        res = table->AddColumn(colDef->colname, typeLen, colType, colDef->is_not_null, typoid);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
    }
}

MOT::RC MOTAdaptor::CreateTable(CreateForeignTableStmt* stmt, ::TransactionId tid)
{
    bool hasBlob = false;
    MOT::Index* primaryIdx = nullptr;
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__, tid);
    MOT::Table* table = nullptr;
    MOT::RC res = MOT::RC_ERROR;
    std::string tname("");
    char* dbname = NULL;

    do {
        table = new (std::nothrow) MOT::Table();
        if (table == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT), errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Allocation of table metadata failed")));
            break;
        }

        uint32_t columnCount = list_length(stmt->base.tableElts);

        // once the columns have been counted, we add one more for the nullable columns
        ++columnCount;

        // prepare table name
        dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
        if (dbname == nullptr) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", u_sess->proc_cxt.MyDatabaseId)));
            break;
        }
        (void)tname.append(dbname);
        (void)tname.append("_");
        if (stmt->base.relation->schemaname != nullptr) {
            (void)tname.append(stmt->base.relation->schemaname);
        } else {
            (void)tname.append("#");
        }

        (void)tname.append("_");
        (void)tname.append(stmt->base.relation->relname);

        if (!table->Init(stmt->base.relation->relname, tname.c_str(), columnCount, stmt->base.relation->foreignOid)) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        // the null fields are copied verbatim because we have to give them back at some point
        res = table->AddColumn(
            "null_bytes", BITMAPLEN(columnCount - 1), MOT::MOT_CATALOG_FIELD_TYPES::MOT_TYPE_NULLBYTES);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        /*
         * Add all the columns.
         * NOTE: On failure, table object will be deleted and ereport will be done in AddTableColumns.
         */
        AddTableColumns(table, stmt->base.tableElts, hasBlob);

        table->SetFixedLengthRow(!hasBlob);

        uint32_t tupleSize = table->GetTupleSize();
        if (tupleSize > (unsigned int)MAX_TUPLE_SIZE) {
            delete table;
            table = nullptr;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("MOT: Table %s tuple size %u exceeds MAX_TUPLE_SIZE=%u !!!",
                        stmt->base.relation->relname,
                        tupleSize,
                        (unsigned int)MAX_TUPLE_SIZE)));
        }

        if (!table->InitRowPool()) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        if (!table->InitTombStonePool()) {
            delete table;
            table = nullptr;
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);
            break;
        }

        elog(LOG,
            "creating table %s (OID: %u), num columns: %u, tuple: %u",
            table->GetLongTableName().c_str(),
            stmt->base.relation->foreignOid,
            columnCount,
            tupleSize);

        res = txn->CreateTable(table);
        if (res != MOT::RC_OK) {
            delete table;
            table = nullptr;
            report_pg_error(res);
            break;
        }

        // add default PK index
        primaryIdx = MOT::IndexFactory::CreatePrimaryIndexEx(
            MOT::IndexingMethod::INDEXING_METHOD_TREE, DEFAULT_TREE_FLAVOR, 8, table->GetLongTableName(), res, nullptr);
        if (res != MOT::RC_OK) {
            (void)txn->DropTable(table);
            report_pg_error(res);
            break;
        }
        primaryIdx->SetExtId(stmt->base.relation->foreignOid + 1);
        if (!primaryIdx->SetNumTableFields(columnCount)) {
            res = MOT::RC_MEMORY_ALLOCATION_ERROR;
            (void)txn->DropTable(table);
            delete primaryIdx;
            report_pg_error(res);
            break;
        }

        primaryIdx->SetNumIndexFields(1);
        primaryIdx->SetLenghtKeyFields(0, -1, 8);
        primaryIdx->SetFakePrimary(true);

        // Add default primary index
        res = txn->CreateIndex(table, primaryIdx, true);
    } while (0);

    if (res != MOT::RC_OK) {
        if (table != nullptr) {
            (void)txn->DropTable(table);
        }
        if (primaryIdx != nullptr) {
            delete primaryIdx;
        }
    } else {
        MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
            table->GetTableExId(), MOT::DDL_ACCESS_CREATE_TABLE, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
    }

    return res;
}

MOT::RC MOTAdaptor::DropIndex(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping index %s, ixoid: %u, taboid: %u", stmt->name, stmt->indexoid, stmt->reloid);

    // get table
    do {
        MOT::Table* table = txn->GetTableByExternalId(stmt->reloid);
        if (table == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("Table not found for oid %u", stmt->reloid)));
            return MOT::RC_ERROR;
        }
        table->GetOrigTable()->WrLock();
        MOT::Index* index = table->GetIndexByExtId(stmt->indexoid);
        if (index == nullptr) {
            table->GetOrigTable()->Unlock();
            elog(LOG,
                "Drop index %s error, index oid %u of table oid %u not found.",
                stmt->name,
                stmt->indexoid,
                stmt->reloid);
            res = MOT::RC_INDEX_NOT_FOUND;
        } else if (index->IsPrimaryKey()) {
            table->GetOrigTable()->Unlock();
            elog(LOG, "Drop primary index is not supported, failed to drop index: %s", stmt->name);
        } else {
            res = txn->DropIndex(index);
            if (res == MOT::RC_OK) {
                MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                    table->GetTableExId(), MOT::DDL_ACCESS_DROP_INDEX, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
            }
            table->GetOrigTable()->Unlock();
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::DropTable(DropForeignStmt* stmt, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "dropping table %s, oid: %u", stmt->name, stmt->reloid);
    do {
        tab = txn->GetTableByExternalId(stmt->reloid);
        if (tab == nullptr) {
            res = MOT::RC_TABLE_NOT_FOUND;
            elog(LOG, "Drop table %s error, table oid %u not found.", stmt->name, stmt->reloid);
        } else {
            tab->GetOrigTable()->WrLock();
            res = txn->DropTable(tab);
            if (res == MOT::RC_OK) {
                MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                    tab->GetTableExId(), MOT::DDL_ACCESS_DROP_TABLE, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
            }
            tab->GetOrigTable()->Unlock();
        }
    } while (0);

    return res;
}

MOT::RC MOTAdaptor::AlterTableAddColumn(AlterForeingTableCmd* cmd, TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;
    MOT::Column* newColumn = nullptr;
    int16 typeLen = 0;
    ColumnDef* colDef = (ColumnDef*)cmd->def;
    (void)EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    do {
        bool isBlob = false;
        Oid typoid = InvalidOid;
        MOT::MOT_CATALOG_FIELD_TYPES colType;
        tab = txn->GetTableByExternalId(cmd->rel->rd_id);
        if (tab == nullptr) {
            elog(LOG,
                "Alter table add column %s error, table oid %u not found.",
                NameStr(cmd->rel->rd_rel->relname),
                cmd->rel->rd_id);
            break;
        }

        res = TableFieldType(colDef, colType, &typeLen, typoid, isBlob);
        if (res != MOT::RC_OK) {
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }

        CalculateDecimalColumnTypeLen(colType, colDef, typeLen);

        // check default value
        size_t dftSize = 0;
        uintptr_t dftSrc = 0;
        Datum dftValue = 0;
        bytea* txt = nullptr;
        bool shouldFreeTxt = false;
        bool isNull = false;
        bool hasDefault = false;
        char buf[DECIMAL_MAX_SIZE];
        MOT::DecimalSt* d = (MOT::DecimalSt*)buf;
        if (cmd->defValue != nullptr) {
            if (contain_volatile_functions((Node*)cmd->defValue)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                        errmodule(MOD_MOT),
                        errmsg("Add column does not support volatile default value")));
                break;
            }

            EState* estate = CreateExecutorState();
            ExprState* exprstate = ExecInitExpr(expression_planner(cmd->defValue), NULL);
            ExprContext* econtext = GetPerTupleExprContext(estate);

            MemoryContext newcxt = GetPerTupleMemoryContext(estate);
            MemoryContext oldcxt = MemoryContextSwitchTo(newcxt);
            dftValue = ExecEvalExpr(exprstate, econtext, &isNull, NULL);
            (void)MemoryContextSwitchTo(oldcxt);
            if (!isNull) {
                hasDefault = true;
                switch (exprstate->resultType) {
                    case BYTEAOID:
                    case TEXTOID:
                    case VARCHAROID:
                    case CLOBOID:
                    case BPCHAROID: {
                        txt = DatumGetByteaP(dftValue);
                        dftSize = VARSIZE(txt) - VARHDRSZ;  // includes header len VARHDRSZ
                        dftSrc = (uintptr_t)VARDATA(txt);
                        shouldFreeTxt = true;
                        break;
                    }
                    case NUMERICOID: {
                        Numeric n = DatumGetNumeric(dftValue);
                        if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                            ereport(ERROR,
                                (errmodule(MOD_MOT),
                                    errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                    errmsg("Value exceeds maximum precision: %d", NUMERIC_MAX_PRECISION)));
                            break;
                        }
                        PGNumericToMOT(n, *d);
                        dftSize = DECIMAL_SIZE(d);
                        dftSrc = (uintptr_t)d;
                        break;
                    }
                    default:
                        dftSize = typeLen;
                        dftSrc = (uintptr_t)dftValue;
                        break;
                }
            }
            FreeExecutorState(estate);
        }
        tab->GetOrigTable()->WrLock();
        res = tab->CreateColumn(
            newColumn, colDef->colname, typeLen, colType, colDef->is_not_null, typoid, hasDefault, dftSrc, dftSize);
        if (res != MOT::RC_OK) {
            tab->GetOrigTable()->Unlock();
            report_pg_error(res, colDef, (void*)(int64)typeLen);
            break;
        }
        if (newColumn != nullptr) {
            res = txn->AlterTableAddColumn(tab, newColumn);
            if (res == MOT::RC_OK) {
                MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                    tab->GetTableExId(), MOT::DDL_ACCESS_ADD_COLUMN, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
            }
        }
        tab->GetOrigTable()->Unlock();
        // free if allocated
        if (shouldFreeTxt && (char*)dftSrc != (char*)txt) {
            pfree(txt);
        }
    } while (false);
    if (res != MOT::RC_OK) {
        if (newColumn != nullptr) {
            delete newColumn;
        }
        report_pg_error(res, colDef, (void*)tab);
    }
    return res;
}

MOT::RC MOTAdaptor::AlterTableDropColumn(AlterForeingTableCmd* cmd, TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    (void)EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    do {
        MOT::Table* tab = txn->GetTableByExternalId(cmd->rel->rd_id);
        if (tab == nullptr) {
            elog(LOG,
                "Alter table add column %s error, table oid %u not found.",
                NameStr(cmd->rel->rd_rel->relname),
                cmd->rel->rd_id);
            break;
        }
        tab->GetOrigTable()->WrLock();
        uint64_t colId = tab->GetFieldId(cmd->name);
        if (colId == (uint64_t)-1) {
            tab->GetOrigTable()->Unlock();
            ereport(ERROR,
                (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
                    errmodule(MOD_MOT),
                    errmsg("Column %s not found", cmd->name)));
            break;
        }
        MOT::Column* col = tab->GetField(colId);
        if (col->IsUsedByIndex()) {
            tab->GetOrigTable()->Unlock();
            ereport(ERROR,
                (errcode(ERRCODE_FDW_DROP_INDEXED_COLUMN_NOT_ALLOWED),
                    errmodule(MOD_MOT),
                    errmsg("Drop column %s is not allowed, used by one or more indexes", cmd->name)));
            break;
        }
        res = txn->AlterTableDropColumn(tab, col);
        if (res == MOT::RC_OK) {
            MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                tab->GetTableExId(), MOT::DDL_ACCESS_DROP_COLUMN, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
        }
        tab->GetOrigTable()->Unlock();
    } while (false);
    if (res != MOT::RC_OK) {
        report_pg_error(res);
    }
    return res;
}

MOT::RC MOTAdaptor::AlterTableRenameColumn(RenameForeingTableCmd* cmd, TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    (void)EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);
    do {
        MOT::Table* tab = txn->GetTableByExternalId(cmd->relid);
        if (tab == nullptr) {
            elog(LOG, "Alter table rename column error, table oid %u not found.", cmd->relid);
            break;
        }
        tab->GetOrigTable()->WrLock();
        uint64_t colId = tab->GetFieldId(cmd->oldname);
        if (colId == (uint64_t)-1) {
            tab->GetOrigTable()->Unlock();
            ereport(ERROR,
                (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
                    errmodule(MOD_MOT),
                    errmsg("Column %s not found", cmd->oldname)));
            break;
        }
        uint16_t len = strlen(cmd->newname);
        if (len >= MOT::Column::MAX_COLUMN_NAME_LEN) {
            tab->GetOrigTable()->Unlock();
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                    errmodule(MOD_MOT),
                    errmsg("Column definition of %s is not supported", cmd->newname),
                    errdetail("Column name %s exceeds max name size %u",
                        cmd->newname,
                        (uint32_t)MOT::Column::MAX_COLUMN_NAME_LEN)));
            break;
        }
        MOT::Column* col = tab->GetField(colId);
        res = txn->AlterTableRenameColumn(tab, col, cmd->newname);
        if (res == MOT::RC_OK) {
            MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                tab->GetTableExId(), MOT::DDL_ACCESS_RENAME_COLUMN, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
        }
        tab->GetOrigTable()->Unlock();
    } while (false);
    if (res != MOT::RC_OK) {
        report_pg_error(res);
    }
    return res;
}

MOT::RC MOTAdaptor::TruncateTable(Relation rel, ::TransactionId tid)
{
    MOT::RC res = MOT::RC_OK;
    MOT::Table* tab = nullptr;

    (void)EnsureSafeThreadAccessInline();

    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "truncating table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = txn->GetTableByExternalId(rel->rd_id);
        if (tab == nullptr) {
            elog(LOG, "Truncate table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->GetOrigTable()->WrLock();
        res = txn->TruncateTable(tab);
        if (res == MOT::RC_OK) {
            MOT::MOTEngine::GetInstance()->NotifyDDLEvent(
                tab->GetTableExId(), MOT::DDL_ACCESS_TRUNCATE_TABLE, MOT::TxnDDLPhase::TXN_DDL_PHASE_EXEC);
        }
        tab->GetOrigTable()->Unlock();
    } while (0);

    return res;
}

void MOTAdaptor::VacuumTable(Relation rel, ::TransactionId tid)
{
    MOT::Table* tab = nullptr;
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    txn->SetTransactionId(tid);

    elog(LOG, "vacuuming table %s, oid: %u", NameStr(rel->rd_rel->relname), rel->rd_id);
    do {
        tab = MOT::GetTableManager()->GetTableSafeByExId(rel->rd_id, true);
        if (tab == nullptr) {
            elog(LOG, "Vacuum table %s error, table oid %u not found.", NameStr(rel->rd_rel->relname), rel->rd_id);
            break;
        }

        tab->Compact(txn);
        tab->Unlock();
    } while (0);
}

uint64_t MOTAdaptor::GetTableIndexSize(uint64_t tabId, uint64_t ixId)
{
    uint64_t res = 0;
    (void)EnsureSafeThreadAccessInline();
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);
    MOT::Table* tab = nullptr;
    MOT::Index* ix = nullptr;
    uint64_t netTotal = 0;

    do {
        tab = txn->GetTableByExternalId(tabId);
        if (tab == nullptr) {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                    errmsg("Get table size error, table oid %lu not found.", tabId)));
            break;
        }

        if (ixId > 0) {
            ix = tab->GetIndexByExtId(ixId);
            if (ix == nullptr) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
                        errmsg("Get index size error, index oid %lu for table oid %lu not found.", ixId, tabId)));
                break;
            }
            res = ix->GetIndexSize(netTotal);
        } else {
            res = tab->GetTableSize(netTotal);
        }
    } while (0);

    return res;
}

MotMemoryDetail* MOTAdaptor::GetMemSize(uint32_t* nodeCount, bool isGlobal)
{
    (void)EnsureSafeThreadAccessInline();
    MotMemoryDetail* result = nullptr;
    *nodeCount = 0;

    /* We allocate an array of size (m_nodeCount + 1) to accommodate one aggregated entry of all global pools. */
    uint32_t statsArraySize = MOT::g_memGlobalCfg.m_nodeCount + 1;
    MOT::MemRawChunkPoolStats* chunkPoolStatsArray =
        (MOT::MemRawChunkPoolStats*)palloc0(statsArraySize * sizeof(MOT::MemRawChunkPoolStats));
    if (chunkPoolStatsArray != nullptr) {
        uint32_t realStatsEntries;
        if (isGlobal) {
            realStatsEntries = MOT::MemRawChunkStoreGetGlobalStats(chunkPoolStatsArray, statsArraySize);
        } else {
            realStatsEntries = MOT::MemRawChunkStoreGetLocalStats(chunkPoolStatsArray, statsArraySize);
        }

        MOT_ASSERT(realStatsEntries <= statsArraySize);
        if (realStatsEntries > 0) {
            result = (MotMemoryDetail*)palloc0(realStatsEntries * sizeof(MotMemoryDetail));
            if (result != nullptr) {
                for (uint32_t node = 0; node < realStatsEntries; ++node) {
                    result[node].numaNode = chunkPoolStatsArray[node].m_node;
                    result[node].reservedMemory = chunkPoolStatsArray[node].m_reservedBytes;
                    result[node].usedMemory = chunkPoolStatsArray[node].m_usedBytes;
                }
                *nodeCount = realStatsEntries;
            }
        }
        pfree(chunkPoolStatsArray);
    }

    return result;
}

MotSessionMemoryDetail* MOTAdaptor::GetSessionMemSize(uint32_t* sessionCount)
{
    (void)EnsureSafeThreadAccessInline();
    MotSessionMemoryDetail* result = nullptr;
    *sessionCount = 0;

    uint32_t session_count = MOT::g_memGlobalCfg.m_maxThreadCount;
    MOT::MemSessionAllocatorStats* session_stats_array =
        (MOT::MemSessionAllocatorStats*)palloc0(session_count * sizeof(MOT::MemSessionAllocatorStats));
    if (session_stats_array != nullptr) {
        uint32_t real_session_count = MOT::MemSessionGetAllStats(session_stats_array, session_count);
        if (real_session_count > 0) {
            result = (MotSessionMemoryDetail*)palloc0(real_session_count * sizeof(MotSessionMemoryDetail));
            if (result != nullptr) {
                for (uint32_t session_index = 0; session_index < real_session_count; ++session_index) {
                    GetSessionDetails(session_stats_array[session_index].m_sessionId,
                        &result[session_index].threadid,
                        &result[session_index].threadStartTime);
                    result[session_index].totalSize = session_stats_array[session_index].m_reservedSize;
                    result[session_index].usedSize = session_stats_array[session_index].m_usedSize;
                    result[session_index].freeSize = result[session_index].totalSize - result[session_index].usedSize;
                }
                *sessionCount = real_session_count;
            }
        }
        pfree(session_stats_array);
    }

    return result;
}

void MOTAdaptor::CreateKeyBuffer(Relation rel, MOTFdwStateSt* festate, int start)
{
    uint8_t* buf = nullptr;
    uint8_t pattern = 0x00;
    (void)EnsureSafeThreadAccessInline();
    int16_t num = festate->m_bestIx->m_ix->GetNumFields();
    const uint16_t* fieldLengths = festate->m_bestIx->m_ix->GetLengthKeyFields();
    const int16_t* orgCols = festate->m_bestIx->m_ix->GetColumnKeyFields();
    TupleDesc desc = rel->rd_att;
    uint16_t offset = 0;
    int32_t* exprs = nullptr;
    KEY_OPER* opers = nullptr;
    uint16_t keyLength;
    KEY_OPER oper;

    if (start == 0) {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_start];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_start];
        oper = festate->m_bestIx->m_ixOpers[0];
    } else {
        exprs = festate->m_bestIx->m_params[festate->m_bestIx->m_end];
        opers = festate->m_bestIx->m_opers[festate->m_bestIx->m_end];
        // end may be equal start but the operation maybe different, look at getCost
        oper = festate->m_bestIx->m_ixOpers[1];
    }

    keyLength = festate->m_bestIx->m_ix->GetKeyLength();
    festate->m_stateKey[start].InitKey(keyLength);
    buf = festate->m_stateKey[start].GetKeyBuf();

    switch (oper) {
        case KEY_OPER::READ_KEY_EXACT:
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_BEFORE:
        case KEY_OPER::READ_KEY_LIKE:
        case KEY_OPER::READ_PREFIX:
        case KEY_OPER::READ_PREFIX_LIKE:
        case KEY_OPER::READ_PREFIX_OR_NEXT:
        case KEY_OPER::READ_PREFIX_BEFORE:
            pattern = 0x00;
            break;

        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_PREFIX_AFTER:
        case KEY_OPER::READ_PREFIX_OR_PREV:
        case KEY_OPER::READ_KEY_AFTER:
            pattern = 0xff;
            break;

        default:
            elog(LOG, "Invalid key operation: %" PRIu8, static_cast<uint8_t>(oper));
            break;
    }

    for (int i = 0; i < num; i++) {
        if (opers[i] < KEY_OPER::READ_INVALID) {
            bool is_null = false;
            ExprState* expr = (ExprState*)list_nth(festate->m_execExprs, exprs[i] - 1);
            Datum val = ExecEvalExpr((ExprState*)(expr), festate->m_econtext, &is_null);
            if (is_null) {
                MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
                errno_t erc = memset_s(buf + offset, fieldLengths[i], 0x00, fieldLengths[i]);
                securec_check(erc, "\0", "\0");
            } else {
                MOT::Column* col = festate->m_table->GetField(orgCols[i]);
                uint8_t fill = 0x00;

                // in case of like fill rest of the key with appropriate to direction values
                if (opers[i] == KEY_OPER::READ_KEY_LIKE) {
                    switch (oper) {
                        case KEY_OPER::READ_KEY_LIKE:
                        case KEY_OPER::READ_KEY_OR_NEXT:
                        case KEY_OPER::READ_KEY_AFTER:
                        case KEY_OPER::READ_PREFIX:
                        case KEY_OPER::READ_PREFIX_LIKE:
                        case KEY_OPER::READ_PREFIX_OR_NEXT:
                        case KEY_OPER::READ_PREFIX_AFTER:
                            break;

                        case KEY_OPER::READ_PREFIX_BEFORE:
                        case KEY_OPER::READ_PREFIX_OR_PREV:
                        case KEY_OPER::READ_KEY_BEFORE:
                        case KEY_OPER::READ_KEY_OR_PREV:
                            fill = 0xff;
                            break;

                        case KEY_OPER::READ_KEY_EXACT:
                        default:
                            elog(LOG, "Invalid key operation: %" PRIu8, static_cast<uint8_t>(oper));
                            break;
                    }
                }

                DatumToMOTKey(col,
                    expr->resultType,
                    val,
                    desc->attrs[orgCols[i] - 1].atttypid,
                    buf + offset,
                    fieldLengths[i],
                    opers[i],
                    fill);
            }
        } else {
            MOT_ASSERT((offset + fieldLengths[i]) <= keyLength);
            (void)festate->m_stateKey[start].FillPattern(pattern, fieldLengths[i], offset);
        }

        offset += fieldLengths[i];
    }

    festate->m_bestIx->m_ix->AdjustKey(&festate->m_stateKey[start], pattern);
}

bool MOTAdaptor::IsScanEnd(MOTFdwStateSt* festate)
{
    bool res = false;
    (void)EnsureSafeThreadAccessInline();

    // festate->cursor[1] (end iterator) might be NULL (in case it is not in use). If this is the case, return false
    // (which means we have not reached the end yet)
    if (festate->m_cursor[1] == nullptr) {
        return false;
    }

    if (!festate->m_cursor[1]->IsValid()) {
        return true;
    } else {
        const MOT::Key* startKey = nullptr;
        const MOT::Key* endKey = nullptr;
        MOT::Index* ix = (festate->m_bestIx != nullptr ? festate->m_bestIx->m_ix : festate->m_table->GetPrimaryIndex());

        startKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[0]->GetKey());
        endKey = reinterpret_cast<const MOT::Key*>(festate->m_cursor[1]->GetKey());
        if (startKey != nullptr && endKey != nullptr) {
            int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), ix->GetKeySizeNoSuffix());

            if (festate->m_forwardDirectionScan) {
                if (cmpRes > 0)
                    res = true;
            } else {
                if (cmpRes < 0)
                    res = true;
            }
        }
    }

    return res;
}

void MOTAdaptor::PackRow(TupleTableSlot* slot, MOT::Table* table, uint8_t* attrs_used, uint8_t* destRow)
{
    errno_t erc;
    (void)EnsureSafeThreadAccessInline();
    HeapTuple srcData = (HeapTuple)slot->tts_tuple;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    bool hasnulls = HeapTupleHasNulls(srcData);
    uint64_t i = 0;
    uint64_t j = 1;
    uint64_t cols = table->GetFieldCount() - 1;  // column count includes null bits field

    // the null bytes are necessary and have to give them back
    if (!hasnulls) {
        erc = memset_s(destRow + table->GetFieldOffset(i), table->GetFieldSize(i), 0xff, table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    } else {
        erc = memcpy_s(destRow + table->GetFieldOffset(i),
            table->GetFieldSize(i),
            &srcData->t_data->t_bits[0],
            table->GetFieldSize(i));
        securec_check(erc, "\0", "\0");
    }

    // we now copy the fields, for the time being the null ones will be copied as well
    for (; i < cols; i++, j++) {
        bool isnull = false;
        Datum value = heap_slot_getattr(slot, j, &isnull);

        if (!isnull) {
            DatumToMOT(table->GetField(j), value, tupdesc->attrs[i].atttypid, destRow);
        }
    }
}

void MOTAdaptor::PackUpdateRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* destRow)
{
    (void)EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint8_t* bits;
    uint64_t i = 0;
    uint64_t j = 1;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;
    bits = destRow + table->GetFieldOffset(i);

    for (; i < cols; i++, j++) {
        if (BITMAP_GET(attrs_used, i)) {
            bool isnull = false;
            Datum value = heap_slot_getattr(slot, j, &isnull);

            if (!isnull) {
                DatumToMOT(table->GetField(j), value, tupdesc->attrs[i].atttypid, destRow);
                BITMAP_SET(bits, i);
            } else {
                BITMAP_CLEAR(bits, i);
            }
        }
    }
}

void MOTAdaptor::UnpackRow(TupleTableSlot* slot, MOT::Table* table, const uint8_t* attrs_used, uint8_t* srcRow)
{
    (void)EnsureSafeThreadAccessInline();
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    uint64_t i = 0;

    // column count includes null bits field
    uint64_t cols = table->GetFieldCount() - 1;

    for (; i < cols; i++) {
        if (BITMAP_GET(attrs_used, i)) {
            MOTToDatum(table, &tupdesc->attrs[i], srcRow, &(slot->tts_values[i]), &(slot->tts_isnull[i]));
        } else {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = PointerGetDatum(nullptr);
        }
    }
}

// useful functions for data conversion: utils/fmgr/gmgr.cpp
void MOTAdaptor::MOTToDatum(MOT::Table* table, const Form_pg_attribute attr, uint8_t* data, Datum* value, bool* is_null)
{
    (void)EnsureSafeThreadAccessInline();
    if (!BITMAP_GET(data, (attr->attnum - 1))) {
        *is_null = true;
        *value = PointerGetDatum(nullptr);

        return;
    }

    size_t len = 0;
    MOT::Column* col = table->GetField(attr->attnum);

    *is_null = false;
    switch (attr->atttypid) {
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
        case CLOBOID:
        case BYTEAOID: {
            uintptr_t tmp;
            col->Unpack(data, &tmp, len);

            bytea* result = (bytea*)palloc(len + VARHDRSZ);
            if (len > 0) {
                errno_t erc = memcpy_s(VARDATA(result), len, (uint8_t*)tmp, len);
                securec_check(erc, "\0", "\0");
            }
            SET_VARSIZE(result, len + VARHDRSZ);

            *value = PointerGetDatum(result);
            break;
        }
        case NUMERICOID: {
            MOT::DecimalSt* d;
            col->Unpack(data, (uintptr_t*)&d, len);

            *value = NumericGetDatum(MOTNumericToPG(d));
            break;
        }
        default:
            col->Unpack(data, value, len);
            break;
    }
}

void MOTAdaptor::DatumToMOT(MOT::Column* col, Datum datum, Oid type, uint8_t* data)
{
    (void)EnsureSafeThreadAccessInline();
    switch (type) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID: {
            bytea* txt = DatumGetByteaP(datum);
            size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
            char* src = VARDATA(txt);
            if (!col->Pack(data, (uintptr_t)src, size - VARHDRSZ)) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for column size (%d)", (int)(col->m_size - VARHDRSZ))));
            }

            if ((char*)datum != (char*)txt) {
                pfree(txt);
            }

            break;
        }
        case NUMERICOID: {
            Numeric n = DatumGetNumeric(datum);
            char buf[DECIMAL_MAX_SIZE];
            MOT::DecimalSt* d = (MOT::DecimalSt*)buf;

            if (NUMERIC_NDIGITS(n) > DECIMAL_MAX_DIGITS) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("Value exceeds maximum precision: %d", NUMERIC_MAX_PRECISION)));
                break;
            }
            PGNumericToMOT(n, *d);
            (void)col->Pack(data, (uintptr_t)d, DECIMAL_SIZE(d));  // simple types packing cannot fail
            break;
        }
        default:
            (void)col->Pack(data, datum, col->m_size);  // no verification required
            break;
    }
}

void MOTAdaptor::VarcharToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    bool noValue = false;
    switch (datumType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            break;
        default:
            noValue = true;
            errno_t erc = memset_s(data, len, 0x00, len);
            securec_check(erc, "\0", "\0");
            break;
    }

    if (noValue) {
        return;
    }

    bytea* txt = DatumGetByteaP(datum);
    size_t size = VARSIZE(txt);  // includes header len VARHDRSZ
    char* src = VARDATA(txt);
    size -= VARHDRSZ;
    if (oper == KEY_OPER::READ_KEY_LIKE) {
        if (src[size - 1] == '%') {
            size -= 1;
        } else {
            // switch to equal
            if (colType == BPCHAROID) {
                fill = 0x20;  // space ' ' == 0x20
            } else {
                fill = 0x00;
            }
        }
    } else if (colType == BPCHAROID) {  // handle padding for blank-padded type
        fill = 0x20;
    }

    // if the length of search value is bigger than a column key size (len)
    // we make key only from a part that equals column key length
    // this will override the column delimiter exactly by one char from search value
    if (size > len) {
        size = len;
    }
    (void)col->PackKey(data, (uintptr_t)src, size, fill);
    if ((char*)datum != (char*)txt) {
        pfree(txt);
    }
}

void MOTAdaptor::FloatToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == FLOAT8OID) {
        MOT::DoubleConvT dc;
        MOT::FloatConvT fc;
        dc.m_r = (uint64_t)datum;
        fc.m_v = (float)dc.m_v;
        uint64_t u = (uint64_t)fc.m_r;
        (void)col->PackKey(data, u, col->m_size);
    } else {
        (void)col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::NumericToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    Numeric n = DatumGetNumeric(datum);
    char buf[DECIMAL_MAX_SIZE];
    MOT::DecimalSt* d = (MOT::DecimalSt*)buf;
    PGNumericToMOT(n, *d);
    (void)col->PackKey(data, (uintptr_t)d, DECIMAL_SIZE(d));
}

void MOTAdaptor::TimestampToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPTZOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(timestamptz_timestamp, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        Timestamp result = DatumGetTimestamp(DirectFunctionCall1(date_timestamp, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else {
        (void)col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::TimestampTzToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(timestamp_timestamptz, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else if (datumType == DATEOID) {
        TimestampTz result = DatumGetTimestampTz(DirectFunctionCall1(date_timestamptz, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else {
        (void)col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::DateToMOTKey(MOT::Column* col, Oid datumType, Datum datum, uint8_t* data)
{
    if (datumType == TIMESTAMPOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamp_date, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else if (datumType == TIMESTAMPTZOID) {
        DateADT result = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, datum));
        (void)col->PackKey(data, result, col->m_size);
    } else {
        (void)col->PackKey(data, datum, col->m_size);
    }
}

void MOTAdaptor::DatumToMOTKey(
    MOT::Column* col, Oid datumType, Datum datum, Oid colType, uint8_t* data, size_t len, KEY_OPER oper, uint8_t fill)
{
    (void)EnsureSafeThreadAccessInline();
    switch (colType) {
        case BYTEAOID:
        case TEXTOID:
        case VARCHAROID:
        case CLOBOID:
        case BPCHAROID:
            VarcharToMOTKey(col, datumType, datum, colType, data, len, oper, fill);
            break;
        case FLOAT4OID:
            FloatToMOTKey(col, datumType, datum, data);
            break;
        case NUMERICOID:
            NumericToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPOID:
            TimestampToMOTKey(col, datumType, datum, data);
            break;
        case TIMESTAMPTZOID:
            TimestampTzToMOTKey(col, datumType, datum, data);
            break;
        case DATEOID:
            DateToMOTKey(col, datumType, datum, data);
            break;
        default:
            (void)col->PackKey(data, datum, col->m_size);
            break;
    }
}
