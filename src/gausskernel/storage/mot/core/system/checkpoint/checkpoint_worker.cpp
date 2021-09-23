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
 * checkpoint_worker.cpp
 *    Describes the interface for callback methods from a worker thread to the manager.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_worker.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <thread>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include "checkpoint_utils.h"
#include "checkpoint_worker.h"
#include "checkpoint_manager.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(CheckpointWorkerPool, Checkpoint);

struct WorkerThreads {
    std::vector<std::thread> m_vec;
};

void CheckpointWorkerPool::Start()
{
    MOT_LOG_DEBUG("CheckpointWorkerPool::start() %d workers", m_numWorkers.load());

    if (!CheckpointUtils::SetWorkingDir(m_workingDir, m_checkpointId)) {
        m_cpManager.OnError(ErrCodes::FILE_IO, "failed to setup working dir");
    }

    WorkerThreads* threads = new (std::nothrow) WorkerThreads();
    if (threads == nullptr) {
        m_cpManager.OnError(ErrCodes::MEMORY, "failed to allocate checkpoint thread pool");
        return;
    }

    for (uint32_t i = 0; i < m_numWorkers; ++i) {
        threads->m_vec.push_back(std::thread(&CheckpointWorkerPool::WorkerFunc, this));
    }

    m_workers = (void*)threads;
}

CheckpointWorkerPool::~CheckpointWorkerPool()
{

    MOT_LOG_DEBUG("~CheckpointWorkerPool: waiting for workers");
    WorkerThreads* threads = reinterpret_cast<WorkerThreads*>(m_workers);
    for (auto& worker : threads->m_vec) {
        if (worker.joinable())
            worker.join();
    }

    delete threads;

    MOT_LOG_DEBUG("~CheckpointWorkerPool: done");
}

bool CheckpointWorkerPool::Write(Buffer* buffer, Row* row, int fd)
{
    MaxKey key;
    Key* primaryKey = &key;
    Index* index = row->GetTable()->GetPrimaryIndex();
    primaryKey->InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, primaryKey);
    if (buffer->Size() + primaryKey->GetKeyLength() + row->GetTupleSize() + sizeof(CheckpointUtils::EntryHeader) >
        buffer->MaxSize()) {
        // need to flush the buffer before serializing the next row
        size_t wrSta = CheckpointUtils::WriteFile(fd, (char*)buffer->Data(), buffer->Size());
        if (wrSta != buffer->Size()) {
            MOT_LOG_ERROR("CheckpointWorkerPool::write - failed to write %u bytes to [%d] (%d:%s)",
                buffer->Size(),
                fd,
                errno,
                gs_strerror(errno));
            return false;
        }

        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::write - failed to flush [%d]", fd);
            return false;
        }
        buffer->Reset();
    }
    CheckpointUtils::EntryHeader entryHeader;
    entryHeader.m_keyLen = primaryKey->GetKeyLength();
    entryHeader.m_dataLen = row->GetTupleSize();
    entryHeader.m_csn = row->GetCommitSequenceNumber();
    entryHeader.m_rowId = row->GetRowId();
    MOT_ASSERT((entryHeader.m_csn != CSNManager::INVALID_CSN) && (entryHeader.m_rowId != Row::INVALID_ROW_ID));
    if (!buffer->Append(&entryHeader, sizeof(CheckpointUtils::EntryHeader))) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write Failed to write entry to buffer");
        return false;
    }
    if (!buffer->Append(primaryKey->GetKeyBuf(), primaryKey->GetKeyLength())) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write Failed to write entry to buffer");
        return false;
    }
    if (!buffer->Append(row->GetData(), row->GetTupleSize())) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write Failed to write entry to buffer");
        return false;
    }
    return true;
}

int CheckpointWorkerPool::Checkpoint(Buffer* buffer, Sentinel* sentinel, int fd, uint16_t threadId, bool& isDeleted)
{
    Row* mainRow = sentinel->GetData();
    Row* stableRow = nullptr;
    int wrote = 0;
    isDeleted = false;

    if (mainRow == nullptr) {
        return 0;
    }

    bool headerLocked = sentinel->TryLock(threadId);
    if (headerLocked == false) {
        if (mainRow->GetTwoPhaseMode() == true) {
            MOT_LOG_DEBUG("checkpoint: row %p is 2pc", mainRow);
            return wrote;
        }
        sentinel->Lock(threadId);
    }

    stableRow = sentinel->GetStable();
    if (mainRow->IsRowDeleted()) {
        if (stableRow) {
            // Truly deleted and was not removed by txn manager
            isDeleted = true;
        } else {
            MOT_LOG_DEBUG("Detected Deleted row without Stable Row!");
        }
    }

    bool statusBit = sentinel->GetStableStatus();
    bool deleted = !sentinel->IsCommited(); /* this currently indicates if the row is deleted or not */

    MOT_ASSERT(sentinel->GetStablePreAllocStatus() == false);

    do {
        if (statusBit == !m_na) { /* has stable version */
            if (stableRow == nullptr) {
                break;
            }

            if (!Write(buffer, stableRow, fd)) {
                wrote = -1;
            } else {
                if (isDeleted == false) {
                    CheckpointUtils::DestroyStableRow(stableRow);
                    sentinel->SetStable(nullptr);
                }
                wrote = 1;
            }
            break;
        } else { /* no stable version */
            if (stableRow == nullptr) {
                if (deleted) {
                    wrote = 0;
                    break;
                }
                sentinel->SetStableStatus(!m_na);
                if (!Write(buffer, mainRow, fd)) {
                    wrote = -1;  // we failed to write, set error
                } else {
                    wrote = 1;
                }
                break;
            }

            /* should not happen! */
            wrote = -1;
            m_cpManager.OnError(ErrCodes::CALC, "Calc logic error - stable row");
        }
    } while (0);

    if (isDeleted) {
        CheckpointUtils::DestroyStableRow(stableRow);
        sentinel->SetStable(nullptr);
    }

    sentinel->Release();
    return wrote;
}

Table* CheckpointWorkerPool::GetTask()
{
    Table* table = nullptr;
    m_tasksLock.lock();
    do {
        if (m_tasksList.empty())
            break;
        table = m_tasksList.front();
        m_tasksList.pop_front();
    } while (0);
    m_tasksLock.unlock();
    return table;
}

void CheckpointWorkerPool::ExecuteMicroGcTransaction(
    Sentinel** deletedList, GcManager* gcSession, Table* table, uint16_t& deletedCounter, uint16_t limit)
{
    if (deletedCounter == limit) {
        gcSession->GcStartTxn();
        for (uint16_t i = 0; i < limit; i++) {
            Row* out = table->RemoveKeyFromIndex(deletedList[i]->GetData(), deletedList[i], 0, gcSession);
        }
        gcSession->GcCheckPointClean();
        deletedCounter = 0;
    }
}

void CheckpointWorkerPool::WorkerFunc()
{
    MOT_DECLARE_NON_KERNEL_THREAD();
    MOT_LOG_DEBUG("CheckpointWorkerPool::WorkerFunc");

    Buffer buffer;
    if (!buffer.Initialize()) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to initialize buffer");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("thread exiting");
        return;
    }

    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();
    if (sessionContext == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to initialize Session Context");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("thread exiting");
        return;
    }

    GcManager* gcSession = sessionContext->GetTxnManager()->GetGcSession();
    gcSession->SetGcType(GcManager::GC_CHECKPOINT);
    uint16_t threadId = MOTCurrThreadId;
    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity for checkpoint worker, checkpoint performance may be affected");
    }

    Sentinel** deletedList = new (nothrow) Sentinel*[DELETE_LIST_SIZE];
    if (!deletedList) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to allocate memory for deleted sentinel list");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        GetSessionManager()->DestroySessionContext(sessionContext);
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("thread exiting");
        return;
    }

    while (true) {
        uint32_t tableId = 0;
        uint64_t exId = 0;
        uint32_t maxSegId = 0;
        bool taskSucceeded = false;

        if (m_cpManager.ShouldStop()) {
            break;
        }

        Table* table = GetTask();
        if (table != nullptr) {
            do {
                tableId = table->GetTableId();
                exId = table->GetTableExId();

                ErrCodes errCode = WriteTableMetadataFile(table);
                if (errCode != ErrCodes::SUCCESS) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::WorkerFunc: failed to write table metadata file for table %u", tableId);
                    m_cpManager.OnError(
                        errCode, "Failed to write table metadata file for table - ", std::to_string(tableId).c_str());
                    break;
                }

                struct timespec start, end;
                uint64_t numOps = 0;
                clock_gettime(CLOCK_MONOTONIC, &start);

                errCode = WriteTableDataFile(table, &buffer, deletedList, gcSession, threadId, maxSegId, numOps);
                if (errCode != ErrCodes::SUCCESS) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::WorkerFunc: failed to write table data file for table %u", tableId);
                    m_cpManager.OnError(
                        errCode, "Failed to write table data file for table - ", std::to_string(tableId).c_str());
                    break;
                }

                taskSucceeded = true;
                clock_gettime(CLOCK_MONOTONIC, &end);
                /*
                 * (*1000000) is to convert seconds to micro seconds and
                 * (/1000) is to convert nano seconds to micro seconds
                 */
                uint64_t deltaUs = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec) / 1000;
                MOT_LOG_DEBUG(
                    "CheckpointWorkerPool::WorkerFunc: checkpoint of table %u completed in %luus, (%lu elements)",
                    tableId,
                    deltaUs,
                    numOps);
            } while (0);

            m_cpManager.TaskDone(table, maxSegId, taskSucceeded);

            if (!taskSucceeded) {
                break;
            }
        } else {
            break;
        }
    }
    delete[] deletedList;
    GetSessionManager()->DestroySessionContext(sessionContext);
    MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
    MOT_LOG_DEBUG("thread exiting");
}

bool CheckpointWorkerPool::BeginFile(int& fd, uint32_t tableId, int seg, uint64_t exId)
{
    std::string fileName;
    CheckpointUtils::MakeCpFilename(tableId, fileName, m_workingDir, seg);
    if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::BeginFile: failed to create file: %s", fileName.c_str());
        return false;
    }
    MOT_LOG_DEBUG("CheckpointWorkerPool::beginFile: %s", fileName.c_str());
    CheckpointUtils::FileHeader fileHeader{CP_MGR_MAGIC, tableId, exId, 0};
    if (CheckpointUtils::WriteFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
        sizeof(CheckpointUtils::FileHeader)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::BeginFile: failed to write file header: %s", fileName.c_str());
        CheckpointUtils::CloseFile(fd);
        fd = -1;
        return false;
    }
    return true;
}

bool CheckpointWorkerPool::FinishFile(int& fd, uint32_t tableId, uint64_t numOps, uint64_t exId)
{
    bool ret = false;
    do {
        if (!CheckpointUtils::SeekFile(fd, 0)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::FinishFile: failed to seek in file (id: %u)", tableId);
            break;
        }
        CheckpointUtils::FileHeader fileHeader{CP_MGR_MAGIC, tableId, exId, numOps};
        if (CheckpointUtils::WriteFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
            sizeof(CheckpointUtils::FileHeader)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::FinishFile: failed to write to file (id: %u)", tableId);
            break;
        }
        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::FinishFile: failed to flush file (id: %u)", tableId);
            break;
        }
        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::FinishFile: failed to close file (id: %u)", tableId);
            break;
        }
        fd = -1;
        ret = true;
    } while (0);

    return ret;
}

bool CheckpointWorkerPool::SetCheckpointId()
{
    if (!CheckpointManager::CreateCheckpointId(m_checkpointId))
        return false;
    MOT_LOG_DEBUG("CheckpointId is %lu", m_checkpointId);
    return true;
}

CheckpointWorkerPool::ErrCodes CheckpointWorkerPool::WriteTableMetadataFile(Table* table)
{
    uint32_t tableId = table->GetTableId();
    uint64_t exId = table->GetTableExId();
    int fd = -1;

    size_t tableSize = table->SerializeSize();
    char* tableBuf = new (std::nothrow) char[tableSize];
    if (tableBuf == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: Failed to allocate buffer for table %u", tableId);
        return ErrCodes::MEMORY;
    }

    table->Serialize(tableBuf);

    std::string fileName;
    CheckpointUtils::MakeMdFilename(tableId, fileName, m_workingDir);

    if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to create md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    CheckpointUtils::MetaFileHeader mFileHeader;
    mFileHeader.m_fileHeader.m_magic = CP_MGR_MAGIC;
    mFileHeader.m_fileHeader.m_tableId = tableId;
    mFileHeader.m_fileHeader.m_exId = exId;
    mFileHeader.m_entryHeader.m_dataLen = tableSize;

    if (CheckpointUtils::WriteFile(fd, (char*)&mFileHeader, sizeof(CheckpointUtils::MetaFileHeader)) !=
        sizeof(CheckpointUtils::MetaFileHeader)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to write to md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    if (CheckpointUtils::WriteFile(fd, tableBuf, tableSize) != tableSize) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to write to md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    if (CheckpointUtils::FlushFile(fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to flush md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    if (CheckpointUtils::CloseFile(fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to close md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    delete[] tableBuf;
    return ErrCodes::SUCCESS;
}

bool CheckpointWorkerPool::FlushBuffer(int fd, Buffer* buffer)
{
    if (buffer->Size() > 0) {  // there is data in the buffer that needs to be written
        if (CheckpointUtils::WriteFile(fd, (char*)buffer->Data(), buffer->Size()) != buffer->Size()) {
            return false;
        }
        buffer->Reset();
    }
    return true;
}

CheckpointWorkerPool::ErrCodes CheckpointWorkerPool::WriteTableDataFile(Table* table, Buffer* buffer,
    Sentinel** deletedList, GcManager* gcSession, uint16_t threadId, uint32_t& maxSegId, uint64_t& numOps)
{
    uint32_t tableId = table->GetTableId();
    uint64_t exId = table->GetTableExId();
    int fd = -1;
    uint16_t deletedListLocation = 0;
    uint64_t currFileOps = 0;
    uint32_t curSegLen = 0;

    maxSegId = 0;
    numOps = 0;
    Index* index = table->GetPrimaryIndex();
    if (index == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: failed to get primary index for table %u", tableId);
        return ErrCodes::INDEX;
    }

    IndexIterator* it = index->Begin(threadId);
    if (it == nullptr) {
        MOT_LOG_ERROR(
            "CheckpointWorkerPool::WriteTableDataFile: failed to get iterator for primary index on table %u", tableId);
        return ErrCodes::INDEX;
    }

    if (!BeginFile(fd, tableId, maxSegId, exId)) {
        MOT_LOG_ERROR(
            "CheckpointWorkerPool::WriteTableDataFile: failed to create data file %u for table %u", maxSegId, tableId);
        delete it;
        return ErrCodes::FILE_IO;
    }

    ErrCodes errCode = ErrCodes::SUCCESS;
    bool isDeleted = false;
    while (it->IsValid()) {
        MOT::Sentinel* sentinel = it->GetPrimarySentinel();
        MOT_ASSERT(sentinel);
        if (sentinel == nullptr) {
            MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: encountered a null sentinel");
            it->Next();
            continue;
        }
        int ckptStatus = Checkpoint(buffer, sentinel, fd, threadId, isDeleted);
        if (isDeleted) {
            deletedList[deletedListLocation++] = sentinel;
            ExecuteMicroGcTransaction(deletedList, gcSession, table, deletedListLocation, DELETE_LIST_SIZE);
        }
        if (ckptStatus == 1) {
            currFileOps++;
            curSegLen += table->GetTupleSize() + sizeof(CheckpointUtils::EntryHeader);
            if (m_checkpointSegsize > 0 && curSegLen >= m_checkpointSegsize) {
                if (!FlushBuffer(fd, buffer)) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::WriteTableDataFile: failed to write remaining buffer data (%u bytes) to "
                        "data file %u for table %u",
                        buffer->Size(),
                        maxSegId,
                        tableId);
                    errCode = ErrCodes::FILE_IO;
                    break;
                }

                /* FinishFile will reset the fd to -1 on success. */
                if (!FinishFile(fd, tableId, currFileOps, exId)) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: failed to close data file %u for table %u",
                        maxSegId,
                        tableId);
                    errCode = ErrCodes::FILE_IO;
                    break;
                }

                maxSegId++;
                numOps += currFileOps;

                if (!BeginFile(fd, tableId, maxSegId, exId)) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::WriteTableDataFile: failed to create data file %u for table %u",
                        maxSegId,
                        tableId);
                    break;
                }

                currFileOps = 0;
                curSegLen = 0;
            }
        } else if (ckptStatus < 0) {
            errCode = ErrCodes::CALC;
            break;
        }
        it->Next();
    }

    delete it;
    it = nullptr;

    if (deletedListLocation > 0) {
        MOT_LOG_DEBUG("Checkpoint worker clean leftovers, Table %s deletedListLocation = %u\n",
            table->GetTableName().c_str(),
            deletedListLocation);
        ExecuteMicroGcTransaction(deletedList, gcSession, table, deletedListLocation, deletedListLocation);
    }
    table->ClearThreadMemoryCache();

    if (errCode != ErrCodes::SUCCESS) {
        if (fd != -1) {
            (void)CheckpointUtils::CloseFile(fd);
        }
        return errCode;
    }

    if (!FlushBuffer(fd, buffer)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: failed to write remaining buffer data (%u bytes) to "
                      "data file %u for table %u",
            buffer->Size(),
            maxSegId,
            tableId);
        (void)CheckpointUtils::CloseFile(fd);
        return ErrCodes::FILE_IO;
    }

    /* FinishFile will reset the fd to -1 on success. */
    if (!FinishFile(fd, tableId, currFileOps, exId)) {
        MOT_LOG_ERROR(
            "CheckpointWorkerPool::WriteTableDataFile: failed to close data file %u for table %u", maxSegId, tableId);
        (void)CheckpointUtils::CloseFile(fd);
        return ErrCodes::FILE_IO;
    }

    numOps += currFileOps;
    return ErrCodes::SUCCESS;
}
}  // namespace MOT
