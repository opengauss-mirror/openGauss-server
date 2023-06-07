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
 * checkpoint_recovery.cpp
 *    Handles recovery from checkpoint.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/checkpoint_recovery.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <thread>
#include "mot_engine.h"
#include "checkpoint_recovery.h"
#include "irecovery_manager.h"
#include "redo_log_transaction_iterator.h"

namespace MOT {
DECLARE_LOGGER(CheckpointRecovery, Recovery);

static const char* const CKPT_RECOVERY_WORKER_NAME = "CheckpointRecoveryWorker";

bool CheckpointRecovery::Recover()
{
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    if (engine == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Checkpoint Recover Initialization", "No MOT Engine object");
        return false;
    }

    m_tasksList.clear();
    if (CheckpointControlFile::GetCtrlFile() == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Checkpoint Recovery Initialization", "Failed to allocate ctrlfile object");
        return false;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetMetaVersion() > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("CheckpointRecovery: metadata version %u is greater than current %u",
            CheckpointControlFile::GetCtrlFile()->GetMetaVersion(),
            MetadataProtoVersion::METADATA_VER_CURR);
        return false;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetMetaVersion() < METADATA_VER_MVCC) {
        MOT_LOG_INFO("CheckpointRecovery: upgrading from a pre MVCC version");
        m_preMvccUpgrade = true;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetId() == CheckpointControlFile::INVALID_ID) {
        m_checkpointId = CheckpointControlFile::INVALID_ID;  // no mot control was found.
    } else {
        if (!IsCheckpointValid(CheckpointControlFile::GetCtrlFile()->GetId())) {
            MOT_LOG_ERROR("CheckpointRecovery: no valid checkpoint exist");
            return false;
        }

        m_checkpointId = CheckpointControlFile::GetCtrlFile()->GetId();
        m_lsn = CheckpointControlFile::GetCtrlFile()->GetLsn();
        m_lastReplayLsn = CheckpointControlFile::GetCtrlFile()->GetLastReplayLsn();
    }

    if (m_checkpointId != CheckpointControlFile::INVALID_ID) {
        if (m_lsn >= m_lastReplayLsn) {
            MOT_LOG_INFO("CheckpointRecovery LSN Check: will use the LSN (%lu), ignoring the lastReplayLSN (%lu)",
                m_lsn,
                m_lastReplayLsn);
        } else {
            MOT_LOG_WARN("CheckpointRecovery LSN Check: will use the lastReplayLSN (%lu), ignoring the LSN (%lu)",
                m_lastReplayLsn,
                m_lsn);
            m_lsn = m_lastReplayLsn;
        }
    }

    if (!CheckpointUtils::SetWorkingDir(m_workingDir, m_checkpointId)) {
        MOT_LOG_ERROR("CheckpointRecovery:: failed to obtain checkpoint's working dir");
        return false;
    }

    m_maxTransactionId = CheckpointControlFile::GetCtrlFile()->GetMaxTransactionId();

    int taskFillStat = FillTasksFromMapFile();
    if (taskFillStat < 0) {
        MOT_LOG_ERROR("CheckpointRecovery:: failed to read map file");
        return false;
    } else if (taskFillStat == 0) {
        // fresh install
        return true;
    }

    if (m_tasksList.size() > 0) {
        if (GetGlobalConfiguration().m_enableIncrementalCheckpoint) {
            MOT_LOG_ERROR(
                "CheckpointRecovery: recovery of MOT tables failed. MOT does not support incremental checkpoint");
            return false;
        }

        if (!PerformRecovery()) {
            MOT_LOG_ERROR("CheckpointRecovery: perform checkpoint recovery failed");
            return false;
        }

        if (m_errorSet) {
            MOT_LOG_ERROR("Checkpoint recovery failed! Error: %u (%s)", m_errorCode, RcToString(m_errorCode));
            return false;
        }
    }

    if (!RecoverInProcessData()) {
        MOT_LOG_ERROR("Failed to recover in process txn data");
        return false;
    }

    /*
     * m_lastReplayLsn might be updated during RecoverInProcessData (if any), but even in that case it should always
     * satisfy the condition (m_lsn >= m_lastReplayLsn).
     */
    MOT_ASSERT(m_lsn >= m_lastReplayLsn);

    /*
     * Set the current valid id in the checkpoint manager in case
     * we will need to retrieve it before a new checkpoint is created.
     */
    engine->GetCheckpointManager()->SetId(m_checkpointId);

    MOT_LOG_INFO("Checkpoint Recovery: Finished recovering %lu tables from checkpoint [%lu:%lu:%lu]",
        m_tableIds.size(),
        m_checkpointId,
        m_lsn,
        m_lastReplayLsn);

    m_tableIds.clear();
    MOTEngine::GetInstance()->GetCheckpointManager()->RemoveOldCheckpoints(m_checkpointId);
    return true;
}

bool CheckpointRecovery::PerformRecovery()
{
    MOT_LOG_INFO("CheckpointRecovery: Starting to recover %lu tables using %u workers from checkpoint [%lu]",
        m_tableIds.size(),
        m_numWorkers,
        m_checkpointId);

    for (auto it = m_tableIds.begin(); it != m_tableIds.end(); (void)++it) {
        if (!RecoverTableMetadata(*it)) {
            MOT_LOG_ERROR("CheckpointRecovery: Failed to recover table metadata for table %u", *it);
            return false;
        }
    }

    std::vector<std::thread> threadPool;
    for (uint32_t i = 0; i < m_numWorkers; ++i) {
        threadPool.push_back(std::thread(CheckpointRecoveryWorker, i, this));
    }

    MOT_LOG_DEBUG("CheckpointRecovery: Waiting for all tasks to finish");
    while (!AllTasksDone() && !m_stopWorkers) {
        (void)sleep(1);
    }

    MOT_LOG_DEBUG("CheckpointRecovery: Tasks finished (%s)", m_errorSet ? "error" : "ok");
    for (auto& worker : threadPool) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    return true;
}

int CheckpointRecovery::FillTasksFromMapFile()
{
    if (m_checkpointId == CheckpointControlFile::INVALID_ID) {
        return 0;  // fresh install probably. no error
    }

    std::string mapFile;
    CheckpointUtils::MakeMapFilename(mapFile, m_workingDir, m_checkpointId);
    int fd = -1;
    if (!CheckpointUtils::OpenFileRead(mapFile, fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to open map file '%s'", mapFile.c_str());
        return -1;
    }

    CheckpointUtils::MapFileHeader mapFileHeader;
    if (CheckpointUtils::ReadFile(fd, (char*)&mapFileHeader, sizeof(CheckpointUtils::MapFileHeader)) !=
        sizeof(CheckpointUtils::MapFileHeader)) {
        MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to read map file '%s' header", mapFile.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return -1;
    }

    if (mapFileHeader.m_magic != CheckpointUtils::HEADER_MAGIC) {
        MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to verify map file'%s'", mapFile.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return -1;
    }

    CheckpointManager::MapFileEntry entry;
    for (uint64_t i = 0; i < mapFileHeader.m_numEntries; i++) {
        if (CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointManager::MapFileEntry)) !=
            sizeof(CheckpointManager::MapFileEntry)) {
            MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to read map file '%s' entry: %lu",
                mapFile.c_str(),
                i);
            (void)CheckpointUtils::CloseFile(fd);
            return -1;
        }
        (void)m_tableIds.insert(entry.m_tableId);
        for (uint32_t j = 0; j <= entry.m_maxSegId; j++) {
            Task* recoveryTask = new (std::nothrow) Task(entry.m_tableId, j);
            if (recoveryTask == nullptr) {
                (void)CheckpointUtils::CloseFile(fd);
                MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to allocate task object");
                return -1;
            }
            m_tasksList.push_back(recoveryTask);
        }
    }

    if (CheckpointUtils::CloseFile(fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::FillTasksFromMapFile: Failed to close map file");
        return -1;
    }

    MOT_LOG_INFO("CheckpointRecovery::FillTasksFromMapFile: Filled %lu tasks", m_tasksList.size());
    return 1;
}

bool CheckpointRecovery::RecoverTableMetadata(uint32_t tableId)
{
    int fd = -1;
    std::string fileName;
    CheckpointUtils::MakeMdFilename(tableId, fileName, m_workingDir);
    if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to open file: %s", fileName.c_str());
        return false;
    }

    CheckpointUtils::MetaFileHeader mFileHeader = {};
    size_t readSize =
        !m_preMvccUpgrade ? sizeof(CheckpointUtils::MetaFileHeader) : sizeof(CheckpointUtils::MetaFileHeaderBase);
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)(&mFileHeader), readSize);
    if (reader != readSize) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to read meta file header, reader %lu", reader);
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (mFileHeader.m_fileHeader.m_magic != CheckpointUtils::HEADER_MAGIC ||
        mFileHeader.m_fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: file: %s is corrupted", fileName.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    size_t dataLen = mFileHeader.m_entryHeader.m_base.m_dataLen;
    char* dataBuf = new (std::nothrow) char[dataLen];
    if (dataBuf == nullptr) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to allocate table buffer");
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    reader = CheckpointUtils::ReadFile(fd, dataBuf, dataLen);
    if (reader != dataLen) {
        MOT_LOG_ERROR(
            "CheckpointRecovery::recoverTableMetadata: failed to read table entry (%u), reader %lu", dataLen, reader);
        (void)CheckpointUtils::CloseFile(fd);
        delete[] dataBuf;
        return false;
    }

    if (CheckpointUtils::CloseFile(fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to close file: %s", fileName.c_str());
        delete[] dataBuf;
        return false;
    }

    bool status = CreateTable(dataBuf);
    delete[] dataBuf;
    return status;
}

bool CheckpointRecovery::CreateTable(char* data)
{
    string name;
    string longName;
    uint32_t intId = 0;
    uint64_t extId = 0;
    uint32_t metaVersion = MetadataProtoVersion::METADATA_VER_CURR;
    Table::DeserializeNameAndIds((const char*)data, metaVersion, intId, extId, name, longName);
    MOT_LOG_INFO("CheckpointRecovery::CreateTable: got intId: %u, extId: %lu, %s/%s",
        intId,
        extId,
        name.c_str(),
        longName.c_str());
    if (GetTableManager()->VerifyTableExists(intId, extId, name, longName)) {
        MOT_LOG_ERROR("CheckpointRecovery::CreateTable: table %u already exists", intId);
        return false;
    }

    if (metaVersion > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_ERROR("CheckpointRecovery::CreateTable: metadata version %u is greater than current %u",
            metaVersion,
            MetadataProtoVersion::METADATA_VER_CURR);
        return false;
    }

    Table* table = new (std::nothrow) Table();
    if (table == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Checkpoint Recovery Create Table", "failed to allocate table object");
        return false;
    }

    table->Deserialize((const char*)data);
    do {
        if (!table->IsDeserialized()) {
            MOT_LOG_ERROR("CheckpointRecovery::CreateTable: failed to de-serialize table");
            break;
        }

        if (!GetTableManager()->AddTable(table)) {
            MOT_LOG_ERROR("CheckpointRecovery::CreateTable: failed to add table to engine");
            break;
        }

        MOT_LOG_INFO("CheckpointRecovery::CreateTable: table %s [internal id %u] created",
            table->GetLongTableName().c_str(),
            table->GetTableId());
        return true;
    } while (0);

    MOT_LOG_ERROR("CheckpointRecovery::CreateTable: failed to recover table");
    delete table;
    return false;
}

void CheckpointRecovery::OnError(RC errCode, const char* errMsg, const char* optionalMsg)
{
    std::lock_guard<spin_lock> lock(m_errorLock);
    m_stopWorkers = true;
    if (!m_errorSet) {
        m_errorCode = errCode;
        m_errorMessage.clear();
        (void)m_errorMessage.append(errMsg);
        if (optionalMsg != nullptr) {
            (void)m_errorMessage.append(" ");
            (void)m_errorMessage.append(optionalMsg);
        }
        m_errorSet = true;
    }
}

void CheckpointRecovery::CheckpointRecoveryWorker(uint32_t workerId, CheckpointRecovery* checkpointRecovery)
{
    // since this is a non-kernel thread we must set-up our own u_sess struct for the current thread
    MOT_DECLARE_NON_KERNEL_THREAD();

    char threadName[ThreadContext::THREAD_NAME_LEN];
    errno_t rc = snprintf_s(threadName,
        ThreadContext::THREAD_NAME_LEN,
        ThreadContext::THREAD_NAME_LEN - 1,
        "%s%u",
        CKPT_RECOVERY_WORKER_NAME,
        workerId);
    securec_check_ss(rc, "", "");

    (void)pthread_setname_np(pthread_self(), threadName);

    MOT_LOG_INFO("%s - Starting", threadName);

    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    uint16_t threadId = MOTCurrThreadId;
    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();
    if (sessionContext == nullptr) {
        MOT_LOG_ERROR("CheckpointRecoveryWorker: Failed to initialize Session Context");
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecoveryWorker: Failed to initialize Session Context");
        engine->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    // in a thread-pooled envelope the affinity could be disabled, so we use task affinity here
    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity of checkpoint recovery worker, recovery from checkpoint performance "
                     "may be affected");
    }

    SurrogateState sState;
    if (!sState.Init()) {
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecoveryWorker: Failed to allocate surrogate state");
        GetSessionManager()->DestroySessionContext(sessionContext);
        engine->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    char* keyData = (char*)malloc(MAX_KEY_SIZE);
    if (keyData == nullptr) {
        MOT_LOG_ERROR("CheckpointRecoveryWorker: Failed to allocate key buffer");
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecoveryWorker: Failed to allocate key data");
        GetSessionManager()->DestroySessionContext(sessionContext);
        engine->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    char* entryData = (char*)malloc(MAX_TUPLE_SIZE);
    if (entryData == nullptr) {
        MOT_LOG_ERROR("CheckpointRecoveryWorker: Failed to allocate row buffer");
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecoveryWorker: Failed to allocate row buffer");
        free(keyData);
        GetSessionManager()->DestroySessionContext(sessionContext);
        engine->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    MOT_LOG_DEBUG("%s[%u] start on cpu %d", threadName, (unsigned)threadId, sched_getcpu());

    uint64_t maxCsn = 0;
    RC status = RC_OK;
    while (!checkpointRecovery->ShouldStopWorkers()) {
        CheckpointRecovery::Task* task = checkpointRecovery->GetTask();
        if (task != nullptr) {
            bool hadError = false;
            if (!checkpointRecovery->RecoverTableRows(task, keyData, entryData, maxCsn, sState, status)) {
                MOT_LOG_ERROR("CheckpointRecoveryWorker: Failed to recover table %u", task->m_tableId);
                checkpointRecovery->OnError(status,
                    "CheckpointRecoveryWorker: Failed to recover table",
                    std::to_string(task->m_tableId).c_str());
                hadError = true;
            }
            delete task;
            if (hadError) {
                break;
            }
        } else {
            break;
        }
    }

    if (entryData != nullptr) {
        free(entryData);
    }
    if (keyData != nullptr) {
        free(keyData);
    }

    checkpointRecovery->SetMaxCsn(maxCsn);
    if (!sState.IsEmpty()) {
        GetRecoveryManager()->AddSurrogateArrayToList(sState);
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    engine->OnCurrentThreadEnding();
    MOT_LOG_DEBUG("%s[%u] end on cpu %d", threadName, (unsigned)threadId, sched_getcpu());
}

bool CheckpointRecovery::RecoverTableRows(
    Task* task, char* keyData, char* entryData, uint64_t& maxCsn, SurrogateState& sState, RC& status)
{
    if (task == nullptr) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: no task given");
        return false;
    }

    int fd = -1;
    uint32_t seg = task->m_segId;
    uint32_t tableId = task->m_tableId;

    Table* table = GetTableManager()->GetTable(tableId);
    if (table == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "CheckpointRecovery::RecoverTableRows", "Table %llu does not exist", tableId);
        return false;
    }

    std::string fileName;
    CheckpointUtils::MakeCpFilename(tableId, fileName, m_workingDir, seg);
    if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to open file: %s", fileName.c_str());
        return false;
    }

    CheckpointUtils::FileHeader fileHeader;
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader));
    if (reader != sizeof(CheckpointUtils::FileHeader)) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to read file header, reader %lu", reader);
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (fileHeader.m_magic != CheckpointUtils::HEADER_MAGIC || fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: file: %s is corrupted", fileName.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId != fileHeader.m_exId) {
        MOT_LOG_ERROR(
            "CheckpointRecovery::RecoverTableRows: exId mismatch: my %lu - pkt %lu", tableExId, fileHeader.m_exId);
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (IsMemoryLimitReached(m_numWorkers, GetGlobalConfiguration().m_checkpointSegThreshold)) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: Memory hard limit reached. Cannot recover datanode");
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    CheckpointUtils::EntryHeader entry;
    size_t entryHeaderSize =
        !m_preMvccUpgrade ? sizeof(CheckpointUtils::EntryHeader) : sizeof(CheckpointUtils::EntryHeaderBase);
    for (uint64_t i = 0; i < fileHeader.m_numOps; i++) {
        status = ReadEntry(fd, entryHeaderSize, entry, keyData, entryData);
        if (status != RC_OK) {
            MOT_LOG_ERROR("CheckpointRecovery: failed to read row (elem: %lu / %lu), error: %s (%d)",
                i,
                fileHeader.m_numOps,
                RcToString(status),
                (int)status);
            break;
        }

        InsertRow(table,
            keyData,
            entry.m_base.m_keyLen,
            entryData,
            entry.m_base.m_dataLen,
            entry.m_base.m_csn,
            MOTCurrThreadId,
            sState,
            status,
            entry.m_base.m_rowId,
            entry.m_transactionId);
        if (status != RC_OK) {
            MOT_LOG_ERROR("CheckpointRecovery: failed to insert row (elem: %lu / %lu), error: %s (%d)",
                i,
                fileHeader.m_numOps,
                RcToString(status),
                (int)status);
            break;
        }

        MOT_LOG_DEBUG("Inserted into table %u row with CSN %" PRIu64, tableId, entry.m_base.m_csn);
        if (entry.m_base.m_csn > maxCsn) {
            maxCsn = entry.m_base.m_csn;
        }
    }

    if (CheckpointUtils::CloseFile(fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to close file: %s", fileName.c_str());
        if (status == RC_OK) {
            status = RC_ERROR;
        }
    }

    if (status == RC_OK) {
        table->UpdateRowCount(fileHeader.m_numOps);
    }
    MOT_LOG_DEBUG("[%u] CheckpointRecovery::RecoverTableRows table %u:%u, %lu rows recovered (%s)",
        MOTCurrThreadId,
        tableId,
        seg,
        fileHeader.m_numOps,
        (status == RC_OK) ? "OK" : "Error");
    return (status == RC_OK);
}

RC CheckpointRecovery::ReadEntry(
    int fd, size_t entryHeaderSize, CheckpointUtils::EntryHeader& entry, char* keyData, char* entryData) const
{
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)&entry, entryHeaderSize);
    if (reader != entryHeaderSize) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to read entry header, reader %lu", reader);
        return RC_ERROR;
    }

    if (m_preMvccUpgrade) {
        // Set a default transaction id in case of upgrade from 1VCC to MVCC.
        entry.m_transactionId = INVALID_TRANSACTION_ID;

        // In case of upgrade from 1VCC to MVCC, we use INITIAL_CSN, because the CSN in the 1VCC entry header
        // is not compatible with envelope CSN.
        entry.m_base.m_csn = INITIAL_CSN;
    }

    if (entry.m_base.m_keyLen > MAX_KEY_SIZE || entry.m_base.m_dataLen > MAX_TUPLE_SIZE) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: invalid entry, keyLen %u, dataLen %u",
            entry.m_base.m_keyLen,
            entry.m_base.m_dataLen);
        return RC_ERROR;
    }

    reader = CheckpointUtils::ReadFile(fd, keyData, entry.m_base.m_keyLen);
    if (reader != entry.m_base.m_keyLen) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to read entry key, reader %lu", reader);
        return RC_ERROR;
    }

    reader = CheckpointUtils::ReadFile(fd, entryData, entry.m_base.m_dataLen);
    if (reader != entry.m_base.m_dataLen) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: failed to read entry data, reader %lu", reader);
        return RC_ERROR;
    }

    return RC_OK;
}

CheckpointRecovery::Task* CheckpointRecovery::GetTask()
{
    std::lock_guard<std::mutex> lock(m_tasksLock);
    Task* task = nullptr;
    do {
        if (m_tasksList.empty()) {
            break;
        }
        task = m_tasksList.front();
        m_tasksList.pop_front();
    } while (0);
    return task;
}

bool CheckpointRecovery::AllTasksDone()
{
    std::lock_guard<std::mutex> lock(m_tasksLock);
    return m_tasksList.empty();
}

bool CheckpointRecovery::IsMemoryLimitReached(uint32_t numThreads, uint64_t neededBytes) const
{
    uint64_t memoryRequiredBytes = (uint64_t)numThreads * neededBytes;
    if (MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes() + memoryRequiredBytes >=
        MOTEngine::GetInstance()->GetHardMemoryLimitBytes()) {
        MOT_LOG_WARN("CheckpointRecovery::IsMemoryLimitReached: memory limit reached "
                     "current memory: %lu, required memory: %lu, hard limit memory: %lu",
            MOTEngine::GetInstance()->GetCurrentMemoryConsumptionBytes(),
            memoryRequiredBytes,
            MOTEngine::GetInstance()->GetHardMemoryLimitBytes());
        return true;
    } else {
        return false;
    }
}

void CheckpointRecovery::InsertRow(Table* table, char* keyData, uint16_t keyLen, char* rowData, uint64_t rowLen,
    uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId, uint64_t version)
{
    MaxKey key;
    Row* row = table->CreateNewRow();
    if (row == nullptr) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to create row");
        return;
    }
    row->CopyData((const uint8_t*)rowData, rowLen);
    row->SetCommitSequenceNumber(csn);
    row->SetRowId(rowId);
    if (!sState.UpdateMaxKey(rowId)) {
        status = RC_ERROR;
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Recovery Manager Insert Row", "Failed to update surrogate state");
        table->DestroyRow(row);
        return;
    }

    MOT::Index* ix = table->GetPrimaryIndex();
    if (ix->IsFakePrimary()) {
        row->SetSurrogateKey();
    }
    key.CpKey((const uint8_t*)keyData, keyLen);
    status = table->InsertRowNonTransactional(row, tid, &key);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to insert row");
        table->DestroyRow(row);
    } else {
        row->GetPrimarySentinel()->SetTransactionId(version);
    }
}

bool CheckpointRecovery::RecoverInProcessData()
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;
    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, m_checkpointId)) {
            break;
        }

        if (CheckpointControlFile::GetCtrlFile()->GetMetaVersion() < METADATA_VER_LOW_RTO) {
            CheckpointUtils::MakeTpcFilename(fileName, workingDir, m_checkpointId);
            if (!CheckpointUtils::IsFileExists(fileName)) {
                // In previous versions, Tpc file won't be created if there are no in-process txn data.
                ret = true;
                break;
            }
        } else {
            CheckpointUtils::MakeIpdFilename(fileName, workingDir, m_checkpointId);
        }

        if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
            MOT_LOG_ERROR("CheckpointRecovery::RecoverInProcessData: failed to open file '%s'", fileName.c_str());
            break;
        }

        CheckpointUtils::PendingTxnDataFileHeader ptdFileHeader;
        if (CheckpointUtils::ReadFile(fd, (char*)&ptdFileHeader, sizeof(CheckpointUtils::PendingTxnDataFileHeader)) !=
            sizeof(CheckpointUtils::PendingTxnDataFileHeader)) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverInProcessData: failed to read file '%s' header", fileName.c_str());
            (void)CheckpointUtils::CloseFile(fd);
            break;
        }

        if (ptdFileHeader.m_magic != CheckpointUtils::HEADER_MAGIC) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverInProcessData: failed to validate file's header ('%s')", fileName.c_str());
            (void)CheckpointUtils::CloseFile(fd);
            break;
        }

        if (ptdFileHeader.m_numEntries == 0) {
            MOT_LOG_INFO("RecoveryManager::RecoverInProcessData: no pending data to recover");
            if (CheckpointUtils::CloseFile(fd)) {
                MOT_LOG_ERROR("CheckpointRecovery::RecoverInProcessData: failed to close file '%s'", fileName.c_str());
                break;
            }
            ret = true;
            break;
        }

        if (!GetRecoveryManager()->DeserializePendingRecoveryData(fd)) {
            MOT_LOG_ERROR("CheckpointRecovery::RecoverInProcessData: deserialization failed");
            (void)CheckpointUtils::CloseFile(fd);
            break;
        }

        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("CheckpointRecovery::RecoverInProcessData: failed to close file '%s'", fileName.c_str());
            break;
        }
        ret = true;
    } while (0);
    return ret;
}

bool CheckpointRecovery::IsCheckpointValid(uint64_t id)
{
    std::string fileName;
    std::string workingDir;
    bool ret = false;

    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, id)) {
            break;
        }

        CheckpointUtils::MakeEndFilename(fileName, workingDir, id);
        if (!CheckpointUtils::IsFileExists(fileName)) {
            MOT_LOG_ERROR("IsCheckpointValid: checkpoint id %lu is invalid", id);
            break;
        }
        ret = true;
    } while (0);
    return ret;
}
}  // namespace MOT
