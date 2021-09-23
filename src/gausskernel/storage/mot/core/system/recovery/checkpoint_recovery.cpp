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
#include "checkpoint_utils.h"
#include "irecovery_manager.h"
#include "redo_log_transaction_iterator.h"

namespace MOT {
DECLARE_LOGGER(CheckpointRecovery, Recovery);

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

    if (CheckpointControlFile::GetCtrlFile()->GetId() == CheckpointControlFile::invalidId) {
        m_checkpointId = CheckpointControlFile::invalidId;  // no mot control was found.
    } else {
        if (!IsCheckpointValid(CheckpointControlFile::GetCtrlFile()->GetId())) {
            MOT_LOG_ERROR("CheckpointRecovery: no valid checkpoint exist");
            return false;
        }

        m_checkpointId = CheckpointControlFile::GetCtrlFile()->GetId();
        m_lsn = CheckpointControlFile::GetCtrlFile()->GetLsn();
        m_lastReplayLsn = CheckpointControlFile::GetCtrlFile()->GetLastReplayLsn();
    }

    if (m_checkpointId != CheckpointControlFile::invalidId) {
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
            MOT_LOG_ERROR("Checkpoint recovery failed! error: %u:%s", m_errorCode, RcToString(m_errorCode));
            return false;
        }
    }

    if (!RecoverInProcessTxns()) {
        MOT_LOG_ERROR("Failed to recover the in-process transactions from the checkpoint");
        return false;
    }

    /*
     * Set the current valid id in the checkpoint manager in case
     * we will need to retrieve it before a new checkpoint is created.
     */
    engine->GetCheckpointManager()->SetId(m_checkpointId);

    MOT_LOG_INFO("Checkpoint Recovery: finished recovering %lu tables from checkpoint id: %lu",
        m_tableIds.size(),
        m_checkpointId);

    m_tableIds.clear();
    MOTEngine::GetInstance()->GetCheckpointManager()->RemoveOldCheckpoints(m_checkpointId);
    return true;
}

bool CheckpointRecovery::PerformRecovery()
{
    MOT_LOG_INFO("CheckpointRecovery: starting to recover %lu tables from checkpoint id: %lu",
        m_tableIds.size(),
        m_checkpointId);

    for (auto it = m_tableIds.begin(); it != m_tableIds.end(); ++it) {
        if (!RecoverTableMetadata(*it)) {
            MOT_LOG_ERROR("CheckpointRecovery: failed to recover table metadata for table %u", *it);
            return false;
        }
    }

    std::vector<std::thread> threadPool;
    for (uint32_t i = 0; i < m_numWorkers; ++i) {
        threadPool.push_back(std::thread(CheckpointRecoveryWorker, this));
    }

    MOT_LOG_DEBUG("CheckpointRecovery: waiting for all tasks to finish");
    while (HaveTasks() && m_stopWorkers == false) {
        sleep(1);
    }

    MOT_LOG_DEBUG("CheckpointRecovery: tasks finished (%s)", m_errorSet ? "error" : "ok");
    for (auto& worker : threadPool) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    return true;
}

int CheckpointRecovery::FillTasksFromMapFile()
{
    if (m_checkpointId == CheckpointControlFile::invalidId) {
        return 0;  // fresh install probably. no error
    }

    std::string mapFile;
    CheckpointUtils::MakeMapFilename(mapFile, m_workingDir, m_checkpointId);
    int fd = -1;
    if (!CheckpointUtils::OpenFileRead(mapFile, fd)) {
        MOT_LOG_ERROR("CheckpointRecovery::fillTasksFromMapFile: failed to open map file '%s'", mapFile.c_str());
        return -1;
    }

    CheckpointUtils::MapFileHeader mapFileHeader;
    if (CheckpointUtils::ReadFile(fd, (char*)&mapFileHeader, sizeof(CheckpointUtils::MapFileHeader)) !=
        sizeof(CheckpointUtils::MapFileHeader)) {
        MOT_LOG_ERROR("CheckpointRecovery::fillTasksFromMapFile: failed to read map file '%s' header", mapFile.c_str());
        CheckpointUtils::CloseFile(fd);
        return -1;
    }

    if (mapFileHeader.m_magic != CP_MGR_MAGIC) {
        MOT_LOG_ERROR("CheckpointRecovery::fillTasksFromMapFile: failed to verify map file'%s'", mapFile.c_str());
        CheckpointUtils::CloseFile(fd);
        return -1;
    }

    CheckpointManager::MapFileEntry entry;
    for (uint64_t i = 0; i < mapFileHeader.m_numEntries; i++) {
        if (CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointManager::MapFileEntry)) !=
            sizeof(CheckpointManager::MapFileEntry)) {
            MOT_LOG_ERROR("CheckpointRecovery::fillTasksFromMapFile: failed to read map file '%s' entry: %lu",
                mapFile.c_str(),
                i);
            CheckpointUtils::CloseFile(fd);
            return -1;
        }
        m_tableIds.insert(entry.m_tableId);
        for (uint32_t i = 0; i <= entry.m_maxSegId; i++) {
            Task* recoveryTask = new (std::nothrow) Task(entry.m_tableId, i);
            if (recoveryTask == nullptr) {
                CheckpointUtils::CloseFile(fd);
                MOT_LOG_ERROR("CheckpointRecovery::fillTasksFromMapFile: failed to allocate task object");
                return -1;
            }
            m_tasksList.push_back(recoveryTask);
        }
    }

    CheckpointUtils::CloseFile(fd);
    MOT_LOG_INFO("CheckpointRecovery::fillTasksFromMapFile: filled %lu tasks", m_tasksList.size());
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

    CheckpointUtils::MetaFileHeader mFileHeader;
    size_t reader = CheckpointUtils::ReadFile(fd, (char*)&mFileHeader, sizeof(CheckpointUtils::MetaFileHeader));
    if (reader != sizeof(CheckpointUtils::MetaFileHeader)) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to read meta file header, reader %lu", reader);
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (mFileHeader.m_fileHeader.m_magic != CP_MGR_MAGIC || mFileHeader.m_fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: file: %s is corrupted", fileName.c_str());
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    char* dataBuf = new (std::nothrow) char[mFileHeader.m_entryHeader.m_dataLen];
    if (dataBuf == nullptr) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to allocate table buffer");
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    reader = CheckpointUtils::ReadFile(fd, dataBuf, mFileHeader.m_entryHeader.m_dataLen);
    if (reader != mFileHeader.m_entryHeader.m_dataLen) {
        MOT_LOG_ERROR("CheckpointRecovery::recoverTableMetadata: failed to read table entry (%u), reader %lu",
            mFileHeader.m_entryHeader.m_dataLen,
            reader);
        CheckpointUtils::CloseFile(fd);
        delete[] dataBuf;
        return false;
    }

    CheckpointUtils::CloseFile(fd);
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
    m_errorLock.lock();
    m_stopWorkers = true;
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

void CheckpointRecovery::CheckpointRecoveryWorker(CheckpointRecovery* checkpointRecovery)
{
    // since this is a non-kernel thread we must set-up our own u_sess struct for the current thread
    MOT_DECLARE_NON_KERNEL_THREAD();

    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();
    int threadId = MOTCurrThreadId;

    // in a thread-pooled envelope the affinity could be disabled, so we use task affinity here
    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity of checkpoint recovery worker, recovery from checkpoint performance may be"
                     " affected");
    }

    SurrogateState sState;
    if (sState.IsValid() == false) {
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecovery::WorkerFunc failed to allocate surrogate state");
        return;
    }
    MOT_LOG_DEBUG("CheckpointRecovery::WorkerFunc start [%u] on cpu %lu", (unsigned)MOTCurrThreadId, sched_getcpu());

    uint64_t maxCsn = 0;
    char* keyData = (char*)malloc(MAX_KEY_SIZE);
    if (keyData == nullptr) {
        MOT_LOG_ERROR("RecoveryManager::WorkerFunc: failed to allocate key buffer");
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecovery::WorkerFunc failed to allocate key data");
    }

    char* entryData = (char*)malloc(MAX_TUPLE_SIZE);
    if (entryData == nullptr) {
        MOT_LOG_ERROR("CheckpointRecovery::WorkerFunc: failed to allocate row buffer");
        checkpointRecovery->OnError(
            RC_MEMORY_ALLOCATION_ERROR, "CheckpointRecovery::WorkerFunc failed to allocate row buffer");
    }

    RC status = RC_OK;
    while (checkpointRecovery->ShouldStopWorkers() == false) {
        CheckpointRecovery::Task* task = checkpointRecovery->GetTask();
        if (task != nullptr) {
            bool hadError = false;
            if (!checkpointRecovery->RecoverTableRows(task, keyData, entryData, maxCsn, sState, status)) {
                MOT_LOG_ERROR("CheckpointRecovery::WorkerFunc recovery of table %lu's data failed", task->m_tableId);
                checkpointRecovery->OnError(status,
                    "CheckpointRecovery::WorkerFunc failed to recover table: ",
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

    (GetRecoveryManager())->SetCsn(maxCsn);
    if (sState.IsEmpty() == false) {
        (GetRecoveryManager())->AddSurrogateArrayToList(sState);
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    engine->OnCurrentThreadEnding();
    MOT_LOG_DEBUG("CheckpointRecovery::WorkerFunc end [%u] on cpu %lu", (unsigned)MOTCurrThreadId, sched_getcpu());
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
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (fileHeader.m_magic != CP_MGR_MAGIC || fileHeader.m_tableId != tableId) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: file: %s is corrupted", fileName.c_str());
        CheckpointUtils::CloseFile(fd);
        return false;
    }

    uint64_t tableExId = table->GetTableExId();
    if (tableExId != fileHeader.m_exId) {
        MOT_LOG_ERROR(
            "CheckpointRecovery::RecoverTableRows: exId mismatch: my %lu - pkt %lu", tableExId, fileHeader.m_exId);
        return false;
    }

    if (IsMemoryLimitReached(m_numWorkers, GetGlobalConfiguration().m_checkpointSegThreshold)) {
        MOT_LOG_ERROR("CheckpointRecovery::RecoverTableRows: Memory hard limit reached. Cannot recover datanode");
        return false;
    }

    CheckpointUtils::EntryHeader entry;
    for (uint64_t i = 0; i < fileHeader.m_numOps; i++) {
        reader = CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointUtils::EntryHeader));
        if (reader != sizeof(CheckpointUtils::EntryHeader)) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverTableRows: failed to read entry header (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        if (entry.m_keyLen > MAX_KEY_SIZE || entry.m_dataLen > MAX_TUPLE_SIZE) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverTableRows: invalid entry (elem: %lu / %lu), keyLen %u, dataLen %u",
                i,
                fileHeader.m_numOps,
                entry.m_keyLen,
                entry.m_dataLen);
            status = RC_ERROR;
            break;
        }

        reader = CheckpointUtils::ReadFile(fd, keyData, entry.m_keyLen);
        if (reader != entry.m_keyLen) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverTableRows: failed to read entry key (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        reader = CheckpointUtils::ReadFile(fd, entryData, entry.m_dataLen);
        if (reader != entry.m_dataLen) {
            MOT_LOG_ERROR(
                "CheckpointRecovery::RecoverTableRows: failed to read entry data (elem: %lu / %lu), reader %lu",
                i,
                fileHeader.m_numOps,
                reader);
            status = RC_ERROR;
            break;
        }

        InsertRow(table,
            keyData,
            entry.m_keyLen,
            entryData,
            entry.m_dataLen,
            entry.m_csn,
            MOTCurrThreadId,
            sState,
            status,
            entry.m_rowId);

        if (status != RC_OK) {
            MOT_LOG_ERROR(
                "CheckpointRecovery: failed to insert row %s (error code: %d)", RcToString(status), (int)status);
            break;
        }
        MOT_LOG_DEBUG("Inserted into table %u row with CSN %" PRIu64, tableId, entry.m_csn);
        if (entry.m_csn > maxCsn)
            maxCsn = entry.m_csn;
    }
    CheckpointUtils::CloseFile(fd);

    MOT_LOG_DEBUG("[%u] CheckpointRecovery::RecoverTableRows table %u:%u, %lu rows recovered (%s)",
        MOTCurrThreadId,
        tableId,
        seg,
        fileHeader.m_numOps,
        (status == RC_OK) ? "OK" : "Error");
    return (status == RC_OK);
}

CheckpointRecovery::Task* CheckpointRecovery::GetTask()
{
    Task* task = nullptr;
    m_tasksLock.lock();
    do {
        if (m_tasksList.empty()) {
            break;
        }
        task = m_tasksList.front();
        m_tasksList.pop_front();
    } while (0);
    m_tasksLock.unlock();
    return task;
}

uint32_t CheckpointRecovery::HaveTasks()
{
    m_tasksLock.lock();
    bool noMoreTasks = m_tasksList.empty();
    m_tasksLock.unlock();
    return !noMoreTasks;
}

bool CheckpointRecovery::IsMemoryLimitReached(uint32_t numThreads, uint32_t neededBytes)
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
    uint64_t csn, uint32_t tid, SurrogateState& sState, RC& status, uint64_t rowId)
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

    MOT::Index* ix = table->GetPrimaryIndex();
    if (ix->IsFakePrimary()) {
        row->SetSurrogateKey(*(uint64_t*)keyData);
        sState.UpdateMaxKey(rowId);
    }
    key.CpKey((const uint8_t*)keyData, keyLen);
    status = table->InsertRowNonTransactional(row, tid, &key);
    if (status != RC_OK) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Recovery Manager Insert Row", "failed to insert row");
        table->DestroyRow(row);
    }
}

bool CheckpointRecovery::RecoverInProcessTxns()
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
        if (!CheckpointUtils::IsFileExists(fileName)) {
            MOT_LOG_INFO("RecoveryManager::RecoverInProcessTxns: tpc file does not exist");
            ret = true;
            break;
        }

        if (!CheckpointUtils::OpenFileRead(fileName, fd)) {
            MOT_LOG_ERROR("RecoveryManager::RecoverInProcessTxns: failed to open file '%s'", fileName.c_str());
            break;
        }

        CheckpointUtils::TpcFileHeader tpcFileHeader;
        if (CheckpointUtils::ReadFile(fd, (char*)&tpcFileHeader, sizeof(CheckpointUtils::TpcFileHeader)) !=
            sizeof(CheckpointUtils::TpcFileHeader)) {
            MOT_LOG_ERROR("RecoveryManager::RecoverInProcessTxns: failed to read file '%s' header", fileName.c_str());
            CheckpointUtils::CloseFile(fd);
            break;
        }

        if (tpcFileHeader.m_magic != CP_MGR_MAGIC) {
            MOT_LOG_ERROR(
                "RecoveryManager::RecoverInProcessTxns: failed to validate file's header ('%s')", fileName.c_str());
            CheckpointUtils::CloseFile(fd);
            break;
        }

        if (tpcFileHeader.m_numEntries == 0) {
            MOT_LOG_INFO("RecoveryManager::RecoverInProcessTxns: no tpc entries to recover");
            CheckpointUtils::CloseFile(fd);
            ret = true;
            break;
        }

        if (!DeserializeInProcessTxns(fd, tpcFileHeader.m_numEntries)) {
            MOT_LOG_ERROR("RecoveryManager::RecoverInProcessTxns: failed to deserialize in-process transactions");
            CheckpointUtils::CloseFile(fd);
            break;
        }

        CheckpointUtils::CloseFile(fd);
        ret = true;
    } while (0);
    return ret;
}

bool CheckpointRecovery::DeserializeInProcessTxns(int fd, uint64_t numEntries)
{
    errno_t erc;
    char* buf = nullptr;
    size_t bufSize = 0;
    uint32_t readEntries = 0;
    bool success = false;

    CheckpointUtils::TpcEntryHeader header;
    if (fd == -1) {
        MOT_LOG_ERROR("DeserializeInProcessTxns: bad fd");
        return false;
    }

    while (readEntries < numEntries) {
        success = false;
        size_t sz = CheckpointUtils::ReadFile(fd, (char*)&header, sizeof(CheckpointUtils::TpcEntryHeader));
        if (sz == 0) {
            MOT_LOG_DEBUG("DeserializeInProcessTxns: eof, read %d entries", readEntries);
            success = true;
            break;
        } else if (sz != sizeof(CheckpointUtils::TpcEntryHeader)) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: failed to read segment header", sz, errno, gs_strerror(errno));
            break;
        }

        if (header.m_magic != CP_MGR_MAGIC || header.m_len > RedoLogBuffer::REDO_DEFAULT_BUFFER_SIZE) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: bad entry %lu - %lu", header.m_magic, header.m_len);
            break;
        }

        MOT_LOG_DEBUG("DeserializeInProcessTxns: entry len %lu", header.m_len);
        if (buf == nullptr || header.m_len > bufSize) {
            if (buf != nullptr) {
                free(buf);
            }
            buf = (char*)malloc(header.m_len);
            MOT_LOG_DEBUG("DeserializeInProcessTxns: alloc %lu - %p", header.m_len, buf);
            bufSize = header.m_len;
        }

        if (buf == nullptr) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: failed to allocate buffer (%lu bytes)", header.m_magic);
            break;
        }

        if (CheckpointUtils::ReadFile(fd, buf, bufSize) != bufSize) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: failed to read data from file (%lu bytes)", bufSize);
            break;
        }

        MOT::LogSegment* segment = new (std::nothrow) MOT::LogSegment();
        if (segment == nullptr) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: failed to allocate segment");
            break;
        }

        segment->m_replayLsn = 0;
        erc = memcpy_s(&segment->m_len, sizeof(size_t), buf, sizeof(size_t));
        securec_check(erc, "\0", "\0");
        segment->m_data = new (std::nothrow) char[segment->m_len];
        if (segment->m_data == nullptr) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: failed to allocate memory for segment data");
            delete segment;
            break;
        }

        segment->Deserialize(buf);
        if (!MOTEngine::GetInstance()->GetInProcessTransactions().InsertLogSegment(segment)) {
            MOT_LOG_ERROR("DeserializeInProcessTxns: insert log segment failed");
            delete segment;
            break;
        }
        readEntries++;
        success = true;
    }

    if (buf != nullptr) {
        free(buf);
    }

    MOT_LOG_INFO("DeserializeInProcessTxns: processed %u entries", readEntries);
    return success;
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
