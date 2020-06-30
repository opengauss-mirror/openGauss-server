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
 * recovery_manager.cpp
 *    Handles all recovery tasks, including recovery from a checkpoint, xlog and 2 pc operations.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/recovery/recovery_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <list>
#include <atomic>
#include <thread>
#include "mot_engine.h"
#include "recovery_manager.h"
#include "checkpoint_utils.h"
#include "checkpoint_manager.h"
#include "spin_lock.h"
#include "transaction_buffer_iterator.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(RecoveryManager, Recovery);

constexpr uint32_t NUM_DELETE_THRESHOLD = 5000;
constexpr uint32_t NUM_DELETE_MAX_INC = 500;

bool RecoveryManager::Initialize()
{
    // in a thread-pooled envelope the affinity could be disabled, so we use task affinity here
    Affinity& affinity = GetTaskAffinity();
    affinity.SetAffinity(m_threadId);

    if (m_enableLogStats) {
        m_logStats = new (std::nothrow) LogStats();
        if (m_logStats == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate statistics object");
            return false;
        }
    }

    if (CheckpointControlFile::GetCtrlFile() == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate ctrlfile object");
        return false;
    }

    if (m_surrogateState.IsValid() == false || m_sState.IsValid() == false) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Initialization", "Failed to allocate surrogate state object");
        return false;
    }

    m_initialized = true;
    return m_initialized;
}

void RecoveryManager::OnError(int errCode, const char* errMsg, const char* optionalMsg)
{
    m_errorLock.lock();
    m_checkpointWorkerStop = true;
    if (!m_errorSet) {
        m_errorCode = errCode;
        m_errorMessage.clear();
        m_errorMessage.append(errMsg);
        if (optionalMsg != nullptr) {
            m_errorMessage.append(" ");
            m_errorMessage.append(optionalMsg);
        }
        m_errorSet = true;
    }
    m_errorLock.unlock();
}

int RecoveryManager::FillTasksFromMapFile()
{
    if (m_checkpointId == CheckpointControlFile::invalidId) {
        return 0;  // fresh install probably. no error
    }

    std::string mapFile;
    CheckpointUtils::MakeMapFilename(mapFile, m_workingDir, m_checkpointId);
    int fd = -1;
    if (!CheckpointUtils::OpenFileRead(mapFile, fd)) {
        MOT_LOG_ERROR("RecoveryManager::fillTasksFromMapFile: failed to open map file '%s'", mapFile.c_str());
        OnError(RecoveryManager::ErrCodes::CP_SETUP,
            "RecoveryManager::fillTasksFromMapFile: failed to open map file: ",
            mapFile.c_str());
        return -1;
    }

    CheckpointUtils::MapFileHeader mapFileHeader;
    if (CheckpointUtils::ReadFile(fd, (char*)&mapFileHeader, sizeof(CheckpointUtils::MapFileHeader)) !=
        sizeof(CheckpointUtils::MapFileHeader)) {
        MOT_LOG_ERROR("RecoveryManager::fillTasksFromMapFile: failed to read map file '%s' header", mapFile.c_str());
        CheckpointUtils::CloseFile(fd);
        OnError(RecoveryManager::ErrCodes::CP_SETUP,
            "RecoveryManager::fillTasksFromMapFile: failed to read map file: ",
            mapFile.c_str());
        return -1;
    }

    if (mapFileHeader.m_magic != CP_MGR_MAGIC) {
        MOT_LOG_ERROR("RecoveryManager::fillTasksFromMapFile: failed to verify map file'%s'", mapFile.c_str());
        CheckpointUtils::CloseFile(fd);
        OnError(RecoveryManager::ErrCodes::CP_SETUP,
            "RecoveryManager::fillTasksFromMapFile: failed to verify map file: ",
            mapFile.c_str());
        return -1;
    }

    CheckpointManager::MapFileEntry entry;
    for (uint64_t i = 0; i < mapFileHeader.m_numEntries; i++) {
        if (CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointManager::MapFileEntry)) !=
            sizeof(CheckpointManager::MapFileEntry)) {
            MOT_LOG_ERROR(
                "RecoveryManager::fillTasksFromMapFile: failed to read map file '%s' entry: %lu", mapFile.c_str(), i);
            CheckpointUtils::CloseFile(fd);
            OnError(RecoveryManager::ErrCodes::CP_SETUP,
                "RecoveryManager::fillTasksFromMapFile: failed to read map file entry ",
                mapFile.c_str());
            return -1;
        }

        if (m_tableIds.find(entry.m_id) == m_tableIds.end()) {
            m_tableIds.insert(entry.m_id);
        }

        for (uint32_t i = 0; i <= entry.m_numSegs; i++) {
            RecoveryTask* recoveryTask = new (std::nothrow) RecoveryTask();
            if (recoveryTask == nullptr) {
                CheckpointUtils::CloseFile(fd);
                OnError(RecoveryManager::ErrCodes::CP_SETUP,
                    "RecoveryManager::fillTasksFromMapFile: failed to allocate task object");
                return -1;
            }
            recoveryTask->m_id = entry.m_id;
            recoveryTask->m_seg = i;
            m_tasksList.push_back(recoveryTask);
        }
    }

    CheckpointUtils::CloseFile(fd);
    MOT_LOG_DEBUG("RecoveryManager::fillTasksFromMapFile: filled %lu tasks", m_tasksList.size());
    return 1;
}

bool RecoveryManager::GetTask(uint32_t& tableId, uint32_t& seg)
{
    bool ret = false;
    RecoveryTask* task = nullptr;
    do {
        m_tasksLock.lock();
        if (m_tasksList.empty()) {
            break;
        }
        task = m_tasksList.front();
        tableId = task->m_id;
        seg = task->m_seg;
        m_tasksList.pop_front();
        delete task;
        ret = true;
    } while (0);
    m_tasksLock.unlock();
    return ret;
}

uint32_t RecoveryManager::HaveTasks()
{
    m_tasksLock.lock();
    bool noMoreTasks = m_tasksList.empty();
    m_tasksLock.unlock();
    return !noMoreTasks;
}

bool RecoveryManager::RecoverTableMetadata(uint32_t tableId)
{
    RC status = RC_OK;
    int fd = -1;
    std::string fileName;
    CheckpointUtils::MakeMdFilename(tableId, fileName, m_workingDir);
    if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableMetadata: failed to open file: %s", fileName.c_str());
        return false;
    }

    CheckpointUtils::MetaFileHeader mFileHeader;
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)&mFileHeader, sizeof(CheckpointUtils::MetaFileHeader));
    if (reader != sizeof(CheckpointUtils::MetaFileHeader)) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableMetadata: failed to read meta file header, reader %lu", reader);
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (mFileHeader.m_fileHeader.m_magic != CP_MGR_MAGIC || mFileHeader.m_fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableMetadata: file: %s is corrupted", fileName.c_str());
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    char* dataBuf = new (std::nothrow) char[mFileHeader.m_entryHeader.m_dataLen];
    if (dataBuf == nullptr) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableMetadata: failed to allocate table buffer");
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    reader = CheckpointUtils::ReadFile(fd, dataBuf, mFileHeader.m_entryHeader.m_dataLen);
    if (reader != mFileHeader.m_entryHeader.m_dataLen) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableMetadata: failed to read table entry (%u), reader %lu",
            mFileHeader.m_entryHeader.m_dataLen,
            reader);
        CheckpointUtils::CloseFile(fd);
        delete[] dataBuf;
        return false;
    }

    CheckpointUtils::CloseFile(fd);

    Table* table = nullptr;
    CreateTable(dataBuf, status, table, true);
    delete[] dataBuf;

    return (status == RC_OK);
}

bool RecoveryManager::RecoverTableRows(
    uint32_t tableId, uint32_t seg, uint32_t tid, uint64_t& maxCsn, SurrogateState& sState)
{
    RC status = RC_OK;
    int fd = -1;
    std::string fileName;
    CheckpointUtils::MakeCpFilename(tableId, fileName, m_workingDir, seg);
    if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to open file: %s", fileName.c_str());
        return false;
    }

    CheckpointUtils::FileHeader fileHeader;
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader));
    if (reader != sizeof(CheckpointUtils::FileHeader)) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to read file header, reader %lu", reader);
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (fileHeader.m_magic != CP_MGR_MAGIC || fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableRows: file: %s is corrupted", fileName.c_str());
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    CheckpointUtils::EntryHeader entry;
    char* keyData = (char*)malloc(MAX_KEY_SIZE);
    if (keyData == nullptr) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to allocate key buffer");
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    char* entryData = (char*)malloc(MAX_TUPLE_SIZE);
    if (entryData == nullptr) {
        MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to allocate row buffer");
        CheckpointUtils::CloseFile(fd);
        free(keyData);
        return false;
    }

    for (uint64_t i = 0; i < fileHeader.m_numOps; i++) {
        if (IsRecoveryMemoryLimitReached(m_numWorkers)) {
            MOT_LOG_ERROR("Memory hard limit reached. Cannot recover datanode");
            status = RC_ERROR;
            break;
        }
        reader = CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointUtils::EntryHeader));
        if (reader != sizeof(CheckpointUtils::EntryHeader)) {
            MOT_LOG_ERROR(
                "RecoveryManager::recoverTableRows: failed to read entry header (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        if (entry.m_keyLen > MAX_KEY_SIZE || entry.m_dataLen > MAX_TUPLE_SIZE) {
            MOT_LOG_ERROR("RecoveryManager::recoverTableRows: invalid entry (elem: %lu / %lu), keyLen %u, dataLen %u",
                i,
                fileHeader.m_numOps,
                entry.m_keyLen,
                entry.m_dataLen);
            status = RC_ERROR;
            break;
        }

        reader = CheckpointUtils::ReadFile(fd, keyData, entry.m_keyLen);
        if (reader != entry.m_keyLen) {
            MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to read entry key (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        reader = CheckpointUtils::ReadFile(fd, entryData, entry.m_dataLen);
        if (reader != entry.m_dataLen) {
            MOT_LOG_ERROR("RecoveryManager::recoverTableRows: failed to read entry data (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        InsertRow(tableId,
            fileHeader.m_exId,
            keyData,
            entry.m_keyLen,
            entryData,
            entry.m_dataLen,
            entry.m_csn,
            tid,
            m_sState,
            status,
            entry.m_rowId);
        if (status != RC_OK)
            break;
        if (entry.m_csn > maxCsn)
            maxCsn = entry.m_csn;
    }
    CheckpointUtils::CloseFile(fd);

    MOT_LOG_DEBUG("[%u] RecoveryManager::recoverTableRows table %u:%u, %lu rows recovered (%s)",
        tid,
        tableId,
        seg,
        fileHeader.m_numOps,
        status == RC_OK ? "OK" : "Error");
    if (keyData != nullptr) {
        free(keyData);
    }
    if (entryData != nullptr) {
        free(entryData);
    }
    return (status == RC_OK);
}

void RecoveryManager::CpWorkerFunc()
{
    // since this is a non-kernel thread we must set-up our own u_sess struct for the current thread
    MOT_DECLARE_NON_KERNEL_THREAD();

    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();
    int threadId = MOTCurrThreadId;

    // in a thread-pooled envelope the affinity could be disabled, so we use task affinity here
    if (!GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity of checkpoint recovery worker, recovery from checkpoint performance may be"
                     " affected");
    }

    SurrogateState sState;
    if (sState.IsValid() == false) {
        GetRecoveryManager()->OnError(MOT::RecoveryManager::ErrCodes::SURROGATE,
            "RecoveryManager::workerFunc failed to allocate surrogate state");
        return;
    }
    MOT_LOG_DEBUG("RecoveryManager::workerFunc start [%u] on cpu %lu", (unsigned)MOTCurrThreadId, sched_getcpu());

    uint64_t maxCsn = 0;
    while (GetRecoveryManager()->GetCheckpointWorkerStop() == false) {
        uint32_t tableId = 0;
        uint32_t seg = 0;
        if (GetTask(tableId, seg)) {
            if (!RecoverTableRows(tableId, seg, MOTCurrThreadId, maxCsn, sState)) {
                MOT_LOG_ERROR("RecoveryManager::workerFunc recovery of table %lu's data failed", tableId);
                GetRecoveryManager()->OnError(MOT::RecoveryManager::ErrCodes::CP_RECOVERY,
                    "RecoveryManager::workerFunc failed to recover table: ",
                    std::to_string(tableId).c_str());
                break;
            }
        } else {
            break;
        }
    }

    GetRecoveryManager()->SetCsnIfGreater(maxCsn);
    if (sState.IsEmpty() == false) {
        GetRecoveryManager()->AddSurrogateArrayToList(m_sState);
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    engine->OnCurrentThreadEnding();
    MOT_LOG_DEBUG("RecoveryManager::workerFunc end [%u] on cpu %lu", (unsigned)MOTCurrThreadId, sched_getcpu());
}

bool RecoveryManager::RecoverFromCheckpoint()
{
    uint64_t lastReplayLsn = 0;
    m_checkpointWorkerStop = false;
    if (!m_tasksList.empty()) {
        MOT_LOG_ERROR("RecoveryManager:: tasksQueue is not empty!");
        OnError(RecoveryManager::ErrCodes::CP_SETUP, "RecoveryManager:: tasksQueue is not empty!");
        return false;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetId() == CheckpointControlFile::invalidId) {
        m_checkpointId = CheckpointControlFile::invalidId;  // no mot control was found.
    } else {
        if (IsCheckpointValid(CheckpointControlFile::GetCtrlFile()->GetId())) {
            m_checkpointId = CheckpointControlFile::GetCtrlFile()->GetId();
            m_lsn = CheckpointControlFile::GetCtrlFile()->GetLsn();
            lastReplayLsn = CheckpointControlFile::GetCtrlFile()->GetLastReplayLsn();
        } else {
            MOT_LOG_ERROR("RecoveryManager:: no valid checkpoint exist");
            OnError(RecoveryManager::ErrCodes::CP_SETUP, "RecoveryManager:: no valid checkpoint exist");
            return false;
        }
    }

    if (!CheckpointUtils::SetWorkingDir(m_workingDir, m_checkpointId)) {
        MOT_LOG_ERROR("RecoveryManager:: failed to obtain checkpoint's working dir");
        OnError(RecoveryManager::ErrCodes::CP_SETUP, "RecoveryManager:: failed to obtain checkpoint's working dir");
        return false;
    }

    if (m_lsn >= lastReplayLsn) {
        MOT_LOG_DEBUG("Recovery LSN Check: will use lsn: %lu (last replay lsn: %lu)", m_lsn, lastReplayLsn);
    } else {
        m_lsn = lastReplayLsn;
        MOT_LOG_WARN("Recovery LSN Check: modifying lsn to %lu (last replay lsn)", m_lsn);
    }

    int taskFillStat = FillTasksFromMapFile();
    if (taskFillStat < 0) {
        MOT_LOG_INFO("RecoveryManager:: failed to read map file");
        return false;                // error was already set
    } else if (taskFillStat == 0) {  // fresh install
        return true;
    }

    if (m_tableIds.size() > 0 && GetGlobalConfiguration().m_enableIncrementalCheckpoint) {
        MOT_LOG_ERROR("RecoveryManager::recoverFromCheckpoint: recovery of MOT "
                      "tables failed. MOT does not support incremental checkpoint");
        OnError(RecoveryManager::ErrCodes::CP_SETUP,
            "RecoveryManager:: cannot recover MOT data. MOT engine does not support incremental checkpoint");
        return false;
    }

    MOT_LOG_INFO("RecoverFromCheckpoint: starting to recover %lu tables from checkpoint id: %lu",
        m_tableIds.size(),
        m_checkpointId);

    for (auto it = m_tableIds.begin(); it != m_tableIds.end(); ++it) {
        if (IsRecoveryMemoryLimitReached(NUM_REDO_RECOVERY_THREADS)) {
            MOT_LOG_ERROR("Memory hard limit reached. Cannot recover datanode");
            OnError(RecoveryManager::ErrCodes::CP_RECOVERY, "RecoveryManager:: Memory hard limit reached");
            return false;
        }
        if (!RecoverTableMetadata(*it)) {
            MOT_LOG_ERROR("RecoveryManager::recoverFromCheckpoint: recovery of table %lu's metadata failed", *it);
            OnError(RecoveryManager::ErrCodes::CP_META,
                "RecoveryManager:: metadata recovery failed for table: ",
                std::to_string(*it).c_str());
            return false;
        }
    }

    std::vector<std::thread> recoveryThreadPool;
    for (uint32_t i = 0; i < m_numWorkers; ++i) {
        recoveryThreadPool.push_back(std::thread(&RecoveryManager::CpWorkerFunc, this));
    }

    MOT_LOG_DEBUG("RecoveryManager:: waiting for all tasks to finish");
    while (HaveTasks() && m_checkpointWorkerStop == false) {
        sleep(1);
    }

    MOT_LOG_DEBUG("RecoveryManager:: tasks finished (%s)", m_errorSet ? "error" : "ok");
    for (auto& worker : recoveryThreadPool) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    if (m_errorSet) {
        MOT_LOG_ERROR("RecoveryManager:: failed to recover from checkpoint, tasks finished with error");
        return false;
    }

    if (!RecoverTpcFromCheckpoint()) {
        MOT_LOG_ERROR("RecoveryManager:: failed to recover in-process transactions from checkpoint");
        return false;
    }

    MOT_LOG_INFO("RecoverFromCheckpoint: finished recovering %lu tables from checkpoint id: %lu",
        m_tableIds.size(),
        m_checkpointId);

    m_tableIds.clear();
    MOTEngine::GetInstance()->GetCheckpointManager()->RemoveOldCheckpoints(m_checkpointId);
    return true;
}

bool RecoveryManager::RecoverDbStart()
{
    MOT_LOG_INFO("Starting MOT recovery");
    if (!RecoverFromCheckpoint()) {
        return false;
    }
    return true;
}

bool RecoveryManager::RecoverDbEnd()
{
    if (ApplyInProcessTransactions() != RC_OK) {
        MOT_LOG_ERROR("applyInProcessTransactions failed!");
        return false;
    }

    if (m_sState.IsEmpty() == false) {
        AddSurrogateArrayToList(m_sState);
    }

    // set global commit sequence number
    GetCSNManager().SetCSN(m_maxRecoveredCsn);

    // merge and apply all SurrogateState maps
    RecoveryManager::SurrogateState::Merge(m_surrogateList, m_surrogateState);
    ApplySurrogate();

    if (m_enableLogStats && m_logStats != nullptr) {
        m_logStats->Print();
    }

    if (m_numRedoOps != 0) {
        ClearTableCache();
        GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
        if (gc != nullptr) {
            gc->GcEndTxn();
        }
    }
    bool success = !m_errorSet;
    MOT_LOG_INFO("MOT recovery %s", success ? "completed" : "failed");
    return success;
}

void RecoveryManager::CleanUp()
{
    if (!m_initialized) {
        return;
    }

    if (m_logStats != nullptr) {
        delete m_logStats;
        m_logStats = nullptr;
    }

    m_initialized = false;
}

void RecoveryManager::FreeRedoSegment(LogSegment* segment)
{
    if (segment == nullptr) {
        return;
    }
    if (segment->m_data != nullptr) {
        delete[] segment->m_data;
    }
    delete segment;
}

bool RecoveryManager::ApplyLogSegmentFromData(uint64_t redoLsn, char* data, size_t len)
{
    if (redoLsn < m_lsn) {
        // ignore old redo records which are prior to our checkpoint LSN
        MOT_LOG_DEBUG(
            "ApplyLogSegmentFromData - ignoring old redo record. Checkpoint LSN: %lu, redo LSN: %lu", m_lsn, redoLsn);
        return true;
    }

    MOTEngine::GetInstance()->GetCheckpointManager()->SetLastReplayLsn(redoLsn);
    return ApplyLogSegmentFromData(data, len);
}

bool RecoveryManager::ApplyLogSegmentFromData(char* data, size_t len)
{
    bool result = false;
    char* curData = data;

    while (data + len > curData) {
        // obtain LogSegment from buffer
        RedoLogTransactionIterator iterator(curData, len);
        LogSegment* segment = iterator.AllocRedoSegment();
        if (segment == nullptr) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - failed to allocate segment");
            return false;
        }

        // check LogSegment Op validity
        OperationCode opCode = segment->m_controlBlock.m_opCode;
        if (opCode >= OperationCode::INVALID_OPERATION_CODE) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - encountered a bad op code");
            FreeRedoSegment(segment);
            return false;
        }

        // build operation params
        uint64_t inId = segment->m_controlBlock.m_internalTransactionId;
        uint64_t exId = segment->m_controlBlock.m_externalTransactionId;
        RecoveryOpState recoveryState = IsCommitOp(opCode) ? RecoveryOpState::COMMIT : RecoveryOpState::ABORT;

        // insert the segment (if not abort)
        if (!IsAbortOp(opCode) && !InsertLogSegment(segment)) {
            MOT_LOG_ERROR("ApplyLogSegmentFromData - insert log segment failed");
            FreeRedoSegment(segment);
            return false;
        }

        // operate on the transaction if:
        // 1. abort
        // 2. mot transaction (exid = 0)
        // 3. regular transaction that's committed in the clog
        if (IsAbortOp(opCode) ||
            (IsCommitOp(opCode) && (IsMotTransactionId(segment) || IsTransactionIdCommitted(exId)))) {
            result = OperateOnRecoveredTransaction(inId, exId, recoveryState);
            if (IsAbortOp(opCode)) {
                FreeRedoSegment(segment);
            }
            if (!result) {
                MOT_LOG_ERROR("ApplyLogSegmentFromData - operateOnRecoveredTransaction failed (abort)");
                return false;
            }
        }
        curData += iterator.GetRedoTransactionLength();
    }
    return true;
}

bool RecoveryManager::InsertLogSegment(LogSegment* segment)
{
    uint64_t transactionId = segment->m_controlBlock.m_internalTransactionId;
    MOT::RecoveryManager::RedoTransactionSegments* transactionLogEntries = nullptr;
    map<uint64_t, RedoTransactionSegments*>::iterator it = m_inProcessTransactionMap.find(transactionId);
    if (it == m_inProcessTransactionMap.end()) {
        // this is a new transaction. Not found in the map.
        transactionLogEntries = new (std::nothrow) MOT::RecoveryManager::RedoTransactionSegments(transactionId);
        if (transactionLogEntries == nullptr) {
            return false;
        }
        if (!transactionLogEntries->Append(segment)) {
            MOT_LOG_ERROR("InsertLogSegment: could not append log segment, error re-allocating log segments array");
            return false;
        }
        m_inProcessTransactionMap[transactionId] = transactionLogEntries;
    } else {
        transactionLogEntries = it->second;
        if (!transactionLogEntries->Append(segment)) {
            MOT_LOG_ERROR("InsertLogSegment: could not append log segment, error re-allocating log segments array");
            return false;
        }
    }

    if (segment->m_controlBlock.m_externalTransactionId != INVALID_TRANSACTIOIN_ID) {
        m_transactionIdToInternalId[segment->m_controlBlock.m_externalTransactionId] =
            segment->m_controlBlock.m_internalTransactionId;
    }
    return true;
}

bool RecoveryManager::CommitRecoveredTransaction(uint64_t externalTransactionId)
{
    map<uint64_t, uint64_t>::iterator it = m_transactionIdToInternalId.find(externalTransactionId);
    if (it != m_transactionIdToInternalId.end()) {
        uint64_t internalTransactionId = it->second;
        m_transactionIdToInternalId.erase(it);
        return OperateOnRecoveredTransaction(internalTransactionId, externalTransactionId, RecoveryOpState::COMMIT);
    }
    return true;
}

bool RecoveryManager::OperateOnRecoveredTransaction(
    uint64_t internalTransactionId, uint64_t externalTransactionId, RecoveryOpState rState)
{
    RC status = RC_OK;
    std::lock_guard<std::mutex> lock(m_inProcessTxLock);
    map<uint64_t, RedoTransactionSegments*>::iterator it = m_inProcessTransactionMap.find(internalTransactionId);
    if (it != m_inProcessTransactionMap.end()) {
        MOT_LOG_DEBUG("operateOnRecoveredTransaction: %lu", internalTransactionId);
        RedoTransactionSegments* segments = it->second;
        m_inProcessTransactionMap.erase(it);
        if (rState != RecoveryOpState::ABORT) {
            LogSegment* segment = segments->GetSegment(segments->GetCount() - 1);
            uint64_t csn = segment->m_controlBlock.m_csn;
            for (uint32_t i = 0; i < segments->GetCount(); i++) {
                segment = segments->GetSegment(i);
                status = RedoSegment(segment, csn, internalTransactionId, rState);
                if (status != RC_OK) {
                    OnError(RecoveryManager::ErrCodes::XLOG_RECOVERY,
                        "RecoveryManager::commitRecoveredTransaction: wal recovery failed");
                    return false;
                }
            }
        }
        delete segments;
    }

    if (rState == RecoveryOpState::ABORT && externalTransactionId != INVALID_TRANSACTIOIN_ID) {
        m_transactionIdToInternalId.erase(externalTransactionId);
    }

    return true;
}

RC RecoveryManager::RedoSegment(LogSegment* segment, uint64_t csn, uint64_t transactionId, RecoveryOpState rState)
{
    RC status = RC_OK;
    bool is2pcRecovery = !MOTEngine::GetInstance()->IsRecovering();
    uint8_t* endPosition = (uint8_t*)(segment->m_data + segment->m_len);
    uint8_t* operationData = (uint8_t*)(segment->m_data);

    while (operationData < endPosition) {
        // redolog recovery - single threaded
        if (IsRecoveryMemoryLimitReached(NUM_REDO_RECOVERY_THREADS)) {
            status = RC_ERROR;
            MOT_LOG_ERROR("Memory hard limit reached. Cannot recover datanode");
            break;
        }

        if (!is2pcRecovery) {
            operationData += RecoverLogOperation(operationData, csn, transactionId, MOTCurrThreadId, m_sState, status);
            GcManager* gc = MOT_GET_CURRENT_SESSION_CONTEXT()->GetTxnManager()->GetGcSession();
            if (m_numRedoOps == 0 && gc != nullptr) {
                gc->GcStartTxn();
            }
            if (++m_numRedoOps > NUM_DELETE_THRESHOLD) {
                ClearTableCache();
                if (gc != nullptr) {
                    gc->GcEndTxn();
                }
                m_numRedoOps = 0;
            }
        } else {
            operationData +=
                TwoPhaseRecoverOp(rState, operationData, csn, transactionId, MOTCurrThreadId, m_sState, status);
        }
        if (status != RC_OK) {
            break;
        }
    }

    // single threaded, no need for locking or CAS
    if (!is2pcRecovery && csn > m_maxRecoveredCsn) {
        m_maxRecoveredCsn = csn;
    }
    if (status != RC_OK) {
        MOT_LOG_ERROR("RecoveryManager::redoSegment: got error %d on tid %lu", status, transactionId);
    }
    return status;
}

bool RecoveryManager::LogStats::FindIdx(uint64_t tableId, uint64_t& id)
{
    id = m_numEntries;
    std::map<uint64_t, int>::iterator it;
    m_slock.lock();
    it = m_idToIdx.find(tableId);
    if (it == m_idToIdx.end()) {
        Entry* newEntry = new (std::nothrow) Entry(tableId);
        if (newEntry == nullptr) {
            return false;
        }
        m_tableStats.push_back(newEntry);
        m_idToIdx.insert(std::pair<int, int>(tableId, m_numEntries));
        m_numEntries++;
    } else {
        id = it->second;
    }
    m_slock.unlock();
    return true;
}

void RecoveryManager::LogStats::Print()
{
    MOT_LOG_ERROR(">> log recovery stats >>");
    for (int i = 0; i < m_numEntries; i++) {
        MOT_LOG_ERROR("TableId %lu, Inserts: %lu, Updates: %lu, Deletes: %lu",
            m_tableStats[i]->m_id,
            m_tableStats[i]->m_inserts.load(),
            m_tableStats[i]->m_updates.load(),
            m_tableStats[i]->m_deletes.load());
    }
    MOT_LOG_ERROR("Overall tcls: %lu", m_tcls.load());
}

void RecoveryManager::SetCsnIfGreater(uint64_t csn)
{
    uint64_t currentCsn = m_maxRecoveredCsn;
    while (currentCsn < csn) {
        m_maxRecoveredCsn.compare_exchange_weak(currentCsn, csn);
        currentCsn = m_maxRecoveredCsn;
    }
}

RecoveryManager::SurrogateState::SurrogateState()
{
    uint32_t maxConnections = GetGlobalConfiguration().m_maxConnections;
    m_empty = true;
    m_maxConnections = 0;
    m_insertsArray = new (std::nothrow) uint64_t[maxConnections];
    if (m_insertsArray != nullptr) {
        m_maxConnections = maxConnections;
        errno_t erc =
            memset_s(m_insertsArray, m_maxConnections * sizeof(uint64_t), 0, m_maxConnections * sizeof(uint64_t));
        securec_check(erc, "\0", "\0");
    }
}

RecoveryManager::SurrogateState::~SurrogateState()
{
    if (m_insertsArray != nullptr) {
        delete[] m_insertsArray;
    }
}

void RecoveryManager::SurrogateState::ExtractInfoFromKey(uint64_t key, uint64_t& pid, uint64_t& insertions)
{
    pid = key >> SurrogateKeyGenerator::KEY_BITS;
    insertions = key & 0x0000FFFFFFFFFFFFULL;
    insertions++;
}

void RecoveryManager::SurrogateState::UpdateMaxInsertions(uint64_t insertions, uint32_t pid)
{
    if (pid < m_maxConnections && m_insertsArray[pid] < insertions) {
        m_insertsArray[pid] = insertions;
        if (m_empty) {
            m_empty = false;
        }
    }
}

bool RecoveryManager::SurrogateState::UpdateMaxKey(uint64_t key)
{
    uint64_t pid = 0;
    uint64_t insertions = 0;

    ExtractInfoFromKey(key, pid, insertions);
    if (pid >= m_maxConnections) {
        MOT_LOG_WARN(
            "SurrogateState::UpdateMaxKey: ConnectionId %lu exceeds max_connections %u", pid, m_maxConnections);
        return false;
    }

    UpdateMaxInsertions(insertions, pid);
    return true;
}

void RecoveryManager::SurrogateState::Merge(std::list<uint64_t*>& arrays, SurrogateState& global)
{
    std::list<uint64_t*>::iterator i;
    for (i = arrays.begin(); i != arrays.end(); ++i) {
        for (uint32_t j = 0; j < global.GetMaxConnections(); ++j) {
            global.UpdateMaxInsertions((*i)[j], j);
        }
        delete[](*i);
    }
}

void RecoveryManager::AddSurrogateArrayToList(SurrogateState& surrogate)
{
    m_surrogateListLock.lock();
    if (surrogate.IsEmpty() == false) {
        uint64_t* newArray = new uint64_t[surrogate.GetMaxConnections()];
        if (newArray != nullptr) {
            errno_t erc = memcpy_s(newArray,
                surrogate.GetMaxConnections() * sizeof(uint64_t),
                surrogate.GetArray(),
                surrogate.GetMaxConnections() * sizeof(uint64_t));
            securec_check(erc, "\0", "\0");
            m_surrogateList.push_back(newArray);
        }
    }
    m_surrogateListLock.unlock();
}

void RecoveryManager::ApplySurrogate()
{
    if (m_surrogateState.IsEmpty()) {
        return;
    }

    const uint64_t* array = m_surrogateState.GetArray();
    for (int i = 0; i < m_maxConnections; i++) {
        GetSurrogateKeyManager()->SetSurrogateSlot(i, array[i]);
    }
}

// in-process (2pc) transactions recovery
RC RecoveryManager::ApplyInProcessTransactions()
{
    RC status = RC_OK;
    MOT_LOG_DEBUG("applyInProcessTransactions (size: %lu)", m_inProcessTransactionMap.size());
    map<uint64_t, RedoTransactionSegments*>::iterator it = m_inProcessTransactionMap.begin();
    while (it != m_inProcessTransactionMap.end()) {
        RedoTransactionSegments* segments = it->second;
        LogSegment* segment = segments->GetSegment(segments->GetCount() - 1);
        uint64_t csn = segment->m_controlBlock.m_csn;
        MOT_LOG_INFO("applyInProcessTransactions: tx %lu is %u",
            segment->m_controlBlock.m_externalTransactionId,
            segment->m_controlBlock.m_opCode);
        bool isTpc =
            (segment->m_controlBlock.m_opCode == PREPARE_TX || segment->m_controlBlock.m_opCode == COMMIT_PREPARED_TX);
        if (isTpc == false) {
            MOT_LOG_ERROR("applyInProcessTransactions: tx %lu is not two-phase commit. ignore",
                segment->m_controlBlock.m_externalTransactionId);
            it = m_inProcessTransactionMap.erase(it);
        } else {
            for (uint32_t i = 0; i < segments->GetCount(); i++) {
                segment = segments->GetSegment(i);
                status = ApplyInProcessSegment(segment, csn, it->first);
                if (status != RC_OK) {
                    MOT_LOG_ERROR(
                        "applyInProcessTransactions: an error occured while applying in-process transactions");
                    return status;
                }
            }
            ++it;
        }
    }
    return status;
}

RC RecoveryManager::ApplyInProcessTransaction(uint64_t internalTransactionId)
{
    RC status = RC_OK;
    MOT_LOG_DEBUG("applyInProcessTransaction (id %lu)", internalTransactionId);
    map<uint64_t, RedoTransactionSegments*>::iterator it = m_inProcessTransactionMap.find(internalTransactionId);
    if (it != m_inProcessTransactionMap.end()) {
        RedoTransactionSegments* segments = it->second;
        LogSegment* segment = segments->GetSegment(segments->GetCount() - 1);
        uint64_t csn = segment->m_controlBlock.m_csn;
        bool isTpc = segment->m_controlBlock.m_opCode == PREPARE_TX;
        if (isTpc == false) {
            MOT_LOG_ERROR("applyInProcessTransaction: tx %lu is not two-phase commit. ignore",
                segment->m_controlBlock.m_externalTransactionId);
            it = m_inProcessTransactionMap.erase(it);
        } else {
            for (uint32_t i = 0; i < segments->GetCount(); i++) {
                segment = segments->GetSegment(i);
                status = ApplyInProcessSegment(segment, csn, it->first);
                if (status != RC_OK) {
                    MOT_LOG_ERROR("applyInProcessTransaction: an error occured while applying in-process transactions");
                    return status;
                }
            }
        }
    } else {
        MOT_LOG_ERROR("applyInProcessTransaction: could not find txid: %lu", internalTransactionId);
        status = RC_ERROR;
    }

    return status;
}

RC RecoveryManager::ApplyInProcessSegment(LogSegment* segment, uint64_t csn, uint64_t transactionId)
{
    RC status = RC_OK;
    uint8_t* endPosition = (uint8_t*)(segment->m_data + segment->m_len);
    uint8_t* operationData = (uint8_t*)(segment->m_data);
    while (operationData < endPosition) {
        operationData += TwoPhaseRecoverOp(
            RecoveryOpState::TPC_APPLY, operationData, csn, transactionId, MOTCurrThreadId, m_sState, status);
        if (status != RC_OK) {
            MOT_LOG_ERROR("applyInProcessSegment: failed seg %p, txnid %lu", segment, transactionId);
            return status;
        }
    }
    return status;
}

bool RecoveryManager::IsInProcessTx(uint64_t id)
{
    map<uint64_t, uint64_t>::iterator it = m_transactionIdToInternalId.find(id);
    if (it == m_transactionIdToInternalId.end()) {
        return false;
    }
    return true;
}

uint64_t RecoveryManager::PerformInProcessTx(uint64_t id, bool isCommit)
{
    map<uint64_t, uint64_t>::iterator it = m_transactionIdToInternalId.find(id);
    if (it == m_transactionIdToInternalId.end()) {
        MOT_LOG_ERROR("performInProcessTx: could not find tx %lu", id);
        return 0;  // invalidTransactionId
    } else {
        bool status = OperateOnRecoveredTransaction(
            it->second, INVALID_TRANSACTIOIN_ID, isCommit ? RecoveryOpState::TPC_COMMIT : RecoveryOpState::TPC_ABORT);
        return status ? it->second : 0;
    }
}

bool RecoveryManager::SerializeInProcessTxns(int fd)
{
    errno_t erc;
    char* buf = nullptr;
    size_t bufSize = 0;
    CheckpointUtils::TpcEntryHeader header;
    if (fd == -1) {
        MOT_LOG_ERROR("serializeInProcessTxns: bad fd");
        return false;
    }

    // this lock is held while serializing the in process transactions call by
    // checkpoint. It prevents gs_clean removing entries from the in-process map
    // while they are serialized
    std::lock_guard<std::mutex> lock(m_inProcessTxLock);
    map<uint64_t, RedoTransactionSegments*>::iterator it = m_inProcessTransactionMap.begin();
    while (it != m_inProcessTransactionMap.end()) {
        RedoTransactionSegments* segments = it->second;
        LogSegment* segment = segments->GetSegment(segments->GetCount() - 1);
        uint64_t csn = segment->m_controlBlock.m_csn;
        for (uint32_t i = 0; i < segments->GetCount(); i++) {
            segment = segments->GetSegment(i);
            size_t sz = segment->SerializeSize();
            if (buf == nullptr) {
                buf = (char*)malloc(sz);
                MOT_LOG_DEBUG("serializeInProcessTxns: alloc %lu - %p", sz, buf);
                bufSize = sz;
            } else if (sz > bufSize) {
                char* bufTmp = (char*)malloc(sz);
                if (bufTmp == nullptr) {
                    free(buf);
                    buf = nullptr;
                } else {
                    erc = memcpy_s(bufTmp, sz, buf, bufSize);
                    securec_check(erc, "\0", "\0");
                    free(buf);
                    buf = bufTmp;
                }
                MOT_LOG_DEBUG("serializeInProcessTxns: realloc %lu - %p", sz, buf);
                bufSize = sz;
            }

            if (buf == nullptr) {
                MOT_LOG_ERROR("serializeInProcessTxns: failed to allocate buffer (%lu bytes)", sz);
                return false;
            }

            header.m_magic = CP_MGR_MAGIC;
            header.m_len = bufSize;
            segment->Serialize(buf);

            size_t wrStat = CheckpointUtils::WriteFile(fd, (char*)&header, sizeof(CheckpointUtils::TpcEntryHeader));
            if (wrStat != sizeof(CheckpointUtils::TpcEntryHeader)) {
                MOT_LOG_ERROR("serializeInProcessTxns: failed to write header (wrote %lu) [%d:%s]",
                    wrStat,
                    errno,
                    gs_strerror(errno));
                free(buf);
                return false;
            }

            wrStat = CheckpointUtils::WriteFile(fd, buf, bufSize);
            if (wrStat != bufSize) {
                MOT_LOG_ERROR("serializeInProcessTxns: failed to write %lu bytes to file (wrote %lu) [%d:%s]",
                    bufSize,
                    wrStat,
                    errno,
                    gs_strerror(errno));
                free(buf);
                return false;
            }
            MOT_LOG_DEBUG("serializeInProcessTxns: wrote seg %p %lu bytes", segment, bufSize);
        }
        ++it;
    }

    if (buf != nullptr) {
        free(buf);
    }
    return true;
}

bool RecoveryManager::RecoverTpcFromCheckpoint()
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;
    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, m_checkpointId)) {
            break;
        }

        CheckpointUtils::MakeTpcFilename(fileName, workingDir, m_checkpointId);
        if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
            MOT_LOG_ERROR("RecoveryManager::recoverTpcFromCheckpoint: failed to open file '%s'", fileName.c_str());
            OnError(RecoveryManager::ErrCodes::CP_SETUP,
                "RecoveryManager::recoverTpcFromCheckpoint: failed to open file: ",
                fileName.c_str());
            break;
        }

        CheckpointUtils::TpcFileHeader tpcFileHeader;
        if (CheckpointUtils::ReadFile(fd, (char*)&tpcFileHeader, sizeof(CheckpointUtils::TpcFileHeader)) !=
            sizeof(CheckpointUtils::TpcFileHeader)) {
            MOT_LOG_ERROR(
                "RecoveryManager::recoverTpcFromCheckpoint: failed to read file '%s' header", fileName.c_str());
            CheckpointUtils::CloseFile(fd);
            OnError(RecoveryManager::ErrCodes::CP_SETUP,
                "RecoveryManager::recoverTpcFromCheckpoint: failed to read map file: ",
                fileName.c_str());
            break;
        }

        if (tpcFileHeader.m_magic != CP_MGR_MAGIC) {
            MOT_LOG_ERROR(
                "RecoveryManager::recoverTpcFromCheckpoint: failed to validate file's header ('%s')", fileName.c_str());
            CheckpointUtils::CloseFile(fd);
            OnError(RecoveryManager::ErrCodes::CP_SETUP,
                "RecoveryManager::recoverTpcFromCheckpoint: failed to validate file's header ('%s') ",
                fileName.c_str());
            break;
        }

        if (tpcFileHeader.m_numEntries == 0) {
            MOT_LOG_INFO("RecoveryManager::recoverTpcFromCheckpoint: no tpc entries to recover");
            CheckpointUtils::CloseFile(fd);
            ret = true;
            break;
        }

        if (!DeserializeInProcessTxns(fd, tpcFileHeader.m_numEntries)) {
            MOT_LOG_ERROR("RecoveryManager::recoverTpcFromCheckpoint: failed to deserialize in-process transactions");
            CheckpointUtils::CloseFile(fd);
            OnError(RecoveryManager::ErrCodes::CP_RECOVERY,
                "RecoveryManager::recoverTpcFromCheckpoint: failed to deserialize in-process transactions");
            break;
        }

        CheckpointUtils::CloseFile(fd);
        ret = true;
    } while (0);
    return ret;
}

bool RecoveryManager::DeserializeInProcessTxns(int fd, uint64_t numEntries)
{
    errno_t erc;
    char* buf = nullptr;
    size_t bufSize = 0;
    uint32_t readEntries = 0;
    bool success = false;
    CheckpointUtils::TpcEntryHeader header;
    if (fd == -1) {
        MOT_LOG_ERROR("deserializeInProcessTxns: bad fd");
        return false;
    }
    MOT_LOG_DEBUG("deserializeInProcessTxns: n %d", numEntries);
    while (readEntries < numEntries) {
        success = false;
        size_t sz = CheckpointUtils::ReadFile(fd, (char*)&header, sizeof(CheckpointUtils::TpcEntryHeader));
        if (sz == 0) {
            MOT_LOG_DEBUG("deserializeInProcessTxns: eof, read %d entries", readEntries);
            success = true;
            break;
        } else if (sz != sizeof(CheckpointUtils::TpcEntryHeader)) {
            MOT_LOG_ERROR("deserializeInProcessTxns: failed to read segment header", sz, errno, gs_strerror(errno));
            break;
        }

        if (header.m_magic != CP_MGR_MAGIC || header.m_len > REDO_DEFAULT_BUFFER_SIZE) {
            MOT_LOG_ERROR("deserializeInProcessTxns: bad entry %lu - %lu", header.m_magic, header.m_len);
            break;
        }

        MOT_LOG_DEBUG("deserializeInProcessTxns: entry len %lu", header.m_len);
        if (buf == nullptr || header.m_len > bufSize) {
            if (buf != nullptr) {
                free(buf);
            }
            buf = (char*)malloc(header.m_len);
            MOT_LOG_DEBUG("deserializeInProcessTxns: alloc %lu - %p", header.m_len, buf);
            bufSize = header.m_len;
        }

        if (buf == nullptr) {
            MOT_LOG_ERROR("deserializeInProcessTxns: failed to allocate buffer (%lu bytes)", header.m_magic);
            break;
        }

        if (CheckpointUtils::ReadFile(fd, buf, bufSize) != bufSize) {
            MOT_LOG_ERROR("deserializeInProcessTxns: failed to read data from file (%lu bytes)", bufSize);
            break;
        }

        MOT::LogSegment* segment = new (std::nothrow) MOT::LogSegment();
        if (segment == nullptr) {
            MOT_LOG_ERROR("deserializeInProcessTxns: failed to allocate segment");
            break;
        }

        erc = memcpy_s(&segment->m_len, sizeof(size_t), buf, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        segment->m_data = new (std::nothrow) char[segment->m_len];
        if (segment->m_data == nullptr) {
            MOT_LOG_ERROR("deserializeInProcessTxns: failed to allocate memory for segment data");
            delete segment;
            break;
        }

        segment->Deserialize(buf);
        if (!InsertLogSegment(segment)) {
            MOT_LOG_ERROR("deserializeInProcessTxns: insert log segment failed");
            delete segment;
            break;
        }
        readEntries++;
        success = true;
    }
    if (buf != nullptr) {
        free(buf);
    }
    return success;
}

bool RecoveryManager::IsSupportedOp(OperationCode op)
{
    switch (op) {
        case CREATE_ROW:
        case UPDATE_ROW:
        case UPDATE_ROW_VARIABLE:
        case OVERWRITE_ROW:
        case REMOVE_ROW:
        case PREPARE_TX:
        case COMMIT_PREPARED_TX:
        case CREATE_TABLE:
        case DROP_TABLE:
        case CREATE_INDEX:
        case DROP_INDEX:
        case TRUNCATE_TABLE:
            return true;
        default:
            return false;
    }
}

bool RecoveryManager::FetchTable(uint64_t id, Table*& table)
{
    bool ret = false;
    table = GetTableManager()->GetTable(id);
    if (table == nullptr) {
        std::map<uint64_t, TableInfo*>::iterator it = m_preCommitedTables.find(id);
        if (it != m_preCommitedTables.end() && it->second != nullptr) {
            TableInfo* tableInfo = (TableInfo*)it->second;
            table = tableInfo->m_table;
            ret = true;
        }
    } else {
        ret = true;
    }
    return ret;
}

bool RecoveryManager::IsMotTransactionId(LogSegment* segment)
{
    MOT_ASSERT(segment);
    if (segment != nullptr) {
        return segment->m_controlBlock.m_externalTransactionId == INVALID_TRANSACTIOIN_ID;
    }
    return false;
}

bool RecoveryManager::IsTransactionIdCommitted(uint64_t xid)
{
    MOT_ASSERT(m_clogCallback);
    if (m_clogCallback != nullptr) {
        return (*m_clogCallback)(xid) == TXN_COMMITED;
    }
    return false;
}

bool RecoveryManager::IsCheckpointValid(uint64_t id)
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;

    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, id)) {
            break;
        }

        CheckpointUtils::MakeEndFilename(fileName, workingDir, id);
        if (!CheckpointUtils::FileExists(fileName)) {
            MOT_LOG_ERROR("IsCheckpointValid: checkpoint id %lu is invalid", id);
            break;
        }
        ret = true;
    } while (0);
    return ret;
}

bool RecoveryManager::IsRecoveryMemoryLimitReached(uint32_t numThreads)
{
    uint64_t memoryRequiredBytes = numThreads * MEM_CHUNK_SIZE_MB * MEGA_BYTE;
    if (MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes() + memoryRequiredBytes >=
        MOTEngine::GetInstance()->GetHardMemoryLimitBytes()) {
        MOT_LOG_WARN("IsRecoveryMemoryLimitReached: recovery memory limit reached "
                     "current memory: %lu, required memory: %lu, hard limit memory: %lu",
            MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes(),
            memoryRequiredBytes,
            MOTEngine::GetInstance()->GetHardMemoryLimitBytes());
        return true;
    } else {
        return false;
    }
}

void RecoveryManager::ClearTableCache()
{
    auto it = m_tableDeletesStat.begin();
    while (it != m_tableDeletesStat.end()) {
        auto table = *it;
        if (table.second > NUM_DELETE_MAX_INC) {
            MOT_LOG_INFO("RecoveryManager::ClearTableCache: Table = %s items = %lu\n",
                table.first->GetTableName().c_str(),
                table.second);
            table.first->ClearRowCache();
            it = m_tableDeletesStat.erase(it);
        } else {
            it++;
        }
    }
}
}  // namespace MOT
