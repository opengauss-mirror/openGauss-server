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

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <ctime>
#include "checkpoint_utils.h"
#include "checkpoint_worker.h"
#include "mot_engine.h"

namespace MOT {
DECLARE_LOGGER(CheckpointWorkerPool, Checkpoint);

static const char* const CKPT_WORKER_NAME = "CheckpointWorker";

static bool Wakeup(void* obj)
{
    CheckpointManagerCallbacks* cpManager = (CheckpointManagerCallbacks*)obj;
    if (cpManager != nullptr) {
        return ((cpManager->GetThreadNotifier().GetState() == ThreadNotifier::ThreadState::TERMINATE) ||
                (cpManager->GetThreadNotifier().GetState() == ThreadNotifier::ThreadState::ACTIVE &&
                    !cpManager->GetTasksList().empty()));
    }
    return true;
}

bool CheckpointWorkerPool::Start()
{
    uint32_t numWorkers = m_cpManager.GetNumWorkers();
    MOT_LOG_TRACE("CheckpointWorkerPool::Start() %u workers", numWorkers);
    for (uint32_t i = 0; i < numWorkers; ++i) {
        ThreadContext* workerContext = new (std::nothrow) ThreadContext();
        if (workerContext == nullptr) {
            MOT_LOG_ERROR("CheckpointWorkerPool::Start: Failed to allocate context for %s%u", CKPT_WORKER_NAME, i);
            return false;
        }

        m_workerContexts.push_back(workerContext);
        m_workers.push_back(std::thread(&CheckpointWorkerPool::WorkerFunc, this, i));

        if (!WaitForThreadStart(m_workerContexts.at(i))) {
            MOT_LOG_ERROR("CheckpointWorkerPool::Start: Failed to start %s%u", CKPT_WORKER_NAME, i);
            return false;
        }
    }
    return true;
}

CheckpointWorkerPool::~CheckpointWorkerPool()
{
    MOT_LOG_TRACE("~CheckpointWorkerPool: Waiting for workers");
    for (auto& worker : m_workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    m_workers.clear();
    for (auto& workerContext : m_workerContexts) {
        delete workerContext;
        workerContext = nullptr;
    }
    m_workerContexts.clear();
    MOT_LOG_DEBUG("~CheckpointWorkerPool: Done");
}

bool CheckpointWorkerPool::Write(Buffer* buffer, Row* row, int fd, uint64_t transactionId)
{
    MaxKey key;
    Key* primaryKey = &key;
    Index* index = row->GetTable()->GetPrimaryIndex();
    primaryKey->InitKey(index->GetKeyLength());
    index->BuildKey(row->GetTable(), row, primaryKey);
    if (buffer->Size() + primaryKey->GetKeyLength() + row->GetTupleSize() + sizeof(CheckpointUtils::EntryHeader) >
        buffer->MaxSize()) {
        // need to flush the buffer before serializing the next row
        size_t wrSta = CheckpointUtils::WriteFile(fd, (const char*)buffer->Data(), buffer->Size());
        if (wrSta != buffer->Size()) {
            MOT_LOG_ERROR("CheckpointWorkerPool::Write - Failed to write %u bytes to [%d] (%d:%s)",
                buffer->Size(),
                fd,
                errno,
                gs_strerror(errno));
            return false;
        }

        buffer->Reset();
    }
    CheckpointUtils::EntryHeader entryHeader;
    entryHeader.m_base.m_keyLen = primaryKey->GetKeyLength();
    entryHeader.m_base.m_dataLen = row->GetTupleSize();
    entryHeader.m_base.m_csn = row->GetCommitSequenceNumber();
    entryHeader.m_base.m_rowId = row->GetRowId();
    entryHeader.m_transactionId = transactionId;
    MOT_ASSERT((entryHeader.m_base.m_csn != INVALID_CSN) && (entryHeader.m_base.m_rowId != Row::INVALID_ROW_ID));
    if (!buffer->Append(&entryHeader, sizeof(CheckpointUtils::EntryHeader))) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write - Failed to write entry to buffer");
        return false;
    }
    if (!buffer->Append(primaryKey->GetKeyBuf(), primaryKey->GetKeyLength())) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write - Failed to write entry to buffer");
        return false;
    }
    if (!buffer->Append(row->GetData(), row->GetTupleSize())) {
        MOT_LOG_ERROR("CheckpointWorkerPool::Write - Failed to write entry to buffer");
        return false;
    }
    return true;
}

int CheckpointWorkerPool::Checkpoint(
    Buffer* buffer, PrimarySentinel* sentinel, int fd, uint16_t threadId, bool& isDeleted, Row*& deletedVersion)
{
    Row* mainRow = nullptr;
    Row* stableRow = nullptr;
    int wrote = 0;
    isDeleted = false;
    deletedVersion = nullptr;

    sentinel->Lock(threadId);

    if (sentinel->IsCommited() == false) {
        sentinel->Unlock();
        return 0;
    }

    stableRow = sentinel->GetStable();
    mainRow = sentinel->GetData();
    if (mainRow == nullptr) {
        sentinel->Unlock();
        return 0;
    }

    if (mainRow->IsRowDeleted()) {
        if (stableRow) {
            // Truly deleted and was not removed by txn manager
            isDeleted = true;
            (void)sentinel->GetGcInfo().RefCountUpdate(INC);
            deletedVersion = mainRow;
        } else {
            MOT_LOG_DEBUG("Detected Deleted row without Stable Row!");
        }
    }

    bool statusBit = sentinel->GetStableStatus();
    /* this currently indicates if the row is deleted or not */
    bool deleted = !sentinel->IsCommited() or mainRow->IsRowDeleted();

    MOT_ASSERT(sentinel->GetStablePreAllocStatus() == false);

    do {
        if (statusBit == !m_cpManager.GetNotAvailableBit()) { /* has stable version */
            if (stableRow == nullptr) {
                break;
            }

            if (!Write(buffer, stableRow, fd, stableRow->GetStableTid())) {
                wrote = -1;
            } else {
                if (!isDeleted) {
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
                sentinel->SetStableStatus(!m_cpManager.GetNotAvailableBit());
                if (!Write(buffer, mainRow, fd, mainRow->GetPrimarySentinel()->GetTransactionId())) {
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

    sentinel->Unlock();
    return wrote;
}

Table* CheckpointWorkerPool::GetTask()
{
    std::lock_guard<std::mutex> lock(m_tasksLock);
    Table* table = nullptr;
    do {
        if (m_cpManager.GetTasksList().empty()) {
            break;
        }
        table = m_cpManager.GetTasksList().front();
        m_cpManager.GetTasksList().pop_front();
    } while (0);
    return table;
}

bool CheckpointWorkerPool::ExecuteMicroGcTransaction(
    DeletePair* deletedList, GcManager* gcSession, Table* table, uint16_t& deletedCounter, uint16_t limit)
{
    if (deletedCounter == limit) {
        // Reserve GC Memory for the current batch
        if (gcSession->ReserveGCMemory(limit) == false) {
            deletedCounter = 0;
            MOT_LOG_ERROR("CheckpointWorkerPool::ExecuteMicroGcTransaction: ReserveGCMemory failed");
            return false;
        }
        for (uint16_t i = 0; i < limit; i++) {
            DeletePair& element = deletedList[i];
            PrimarySentinel* ps = element.first;
            Row* r = element.second;
            gcSession->GcRecordObject(GC_QUEUE_TYPE::DELETE_QUEUE,
                ps->GetIndex()->GetIndexId(),
                r,
                ps,
                Row::DeleteRowDtor,
                ROW_SIZE_FROM_POOL(ps->GetIndex()->GetTable()),
                r->GetCommitSequenceNumber());
        }
        gcSession->GcCheckPointClean();
        deletedCounter = 0;
        if (gcSession->GcStartTxn() != RC_OK) {
            MOT_LOG_ERROR("CheckpointWorkerPool::ExecuteMicroGcTransaction: GcStartTxn failed");
            return false;
        }
    }
    return true;
}

void CheckpointWorkerPool::WorkerFunc(uint32_t workerId)
{
    MOT_DECLARE_NON_KERNEL_THREAD();

    char threadName[ThreadContext::THREAD_NAME_LEN];
    errno_t rc = snprintf_s(threadName,
        ThreadContext::THREAD_NAME_LEN,
        ThreadContext::THREAD_NAME_LEN - 1,
        "%s%u",
        CKPT_WORKER_NAME,
        workerId);
    securec_check_ss(rc, "", "");

    (void)pthread_setname_np(pthread_self(), threadName);

    MOT_LOG_INFO("%s - Starting", threadName);

    ThreadContext* workerContext = m_workerContexts[workerId];
    MOT_ASSERT(workerContext != nullptr);

    Buffer buffer;
    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();
    if (sessionContext == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to initialize Session Context");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        workerContext->SetError();
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    GcManager* gcSession = sessionContext->GetTxnManager()->GetGcSession();
    gcSession->SetGcType(GcManager::GC_TYPE::GC_CHECKPOINT);
    bool res = gcSession->ReserveGCMemory(MAX_ITERS_COUNT * 2);
    if (!res) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to Reserve GC Memory");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        workerContext->SetError();
        GetSessionManager()->DestroySessionContext(sessionContext);
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    uint16_t threadId = MOTCurrThreadId;
    if (GetGlobalConfiguration().m_enableNuma && !GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity for checkpoint worker, checkpoint performance may be affected");
    }

    DeletePair* deletedList = new (std::nothrow) DeletePair[DELETE_LIST_SIZE];
    if (deletedList == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to allocate memory for deleted sentinel list");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        workerContext->SetError();
        GetSessionManager()->DestroySessionContext(sessionContext);
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("%s - Exiting", threadName);
        return;
    }

    workerContext->SetReady();

    bool taskSucceeded = true;
    while (taskSucceeded) {
        if (m_cpManager.GetThreadNotifier().Wait(Wakeup, &m_cpManager) == ThreadNotifier::ThreadState::TERMINATE) {
            break;
        }

        if (!buffer.IsAllocated()) {
            if (!buffer.Initialize()) {
                MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to initialize buffer");
                m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
                workerContext->SetError();
                break;
            }
        }

        Table* table = GetTask();
        while (table != nullptr) {
            uint32_t maxSegId = 0;

            do {
                // Try to clean previous garbage if any.
                gcSession->GcEndTxn();
                taskSucceeded = false;
                uint32_t tableId = table->GetTableId();
                ErrCodes errCode = WriteTableMetadataFile(table);
                if (errCode != ErrCodes::SUCCESS) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::WorkerFunc: Failed to write table metadata file for table %u", tableId);
                    m_cpManager.OnError(
                        errCode, "Failed to write table metadata file for table - ", std::to_string(tableId).c_str());
                    workerContext->SetError();
                    break;
                }

                struct timespec start, end;
                uint64_t numOps = 0;
                (void)clock_gettime(CLOCK_MONOTONIC, &start);

                errCode = WriteTableDataFile(table, &buffer, deletedList, gcSession, threadId, maxSegId, numOps);
                if (errCode != ErrCodes::SUCCESS) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::WorkerFunc: Failed to write table data file for table %u, "
                                  "error: %u",
                        tableId,
                        errCode);
                    m_cpManager.OnError(
                        errCode, "Failed to write table data file for table - ", std::to_string(tableId).c_str());
                    workerContext->SetError();
                    break;
                }

                taskSucceeded = true;
                (void)clock_gettime(CLOCK_MONOTONIC, &end);
                /*
                 * (*1000000) is to convert seconds to micro seconds and
                 * (/1000) is to convert nano seconds to micro seconds
                 */
                uint64_t deltaUs = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_nsec - start.tv_nsec) / 1000;
                MOT_LOG_DEBUG(
                    "CheckpointWorkerPool::WorkerFunc: Checkpoint of table %u completed in %luus, (%lu elements)",
                    tableId,
                    deltaUs,
                    numOps);
            } while (0);

            m_cpManager.TaskDone(table, maxSegId, taskSucceeded);

            if (!taskSucceeded) {
                break;
            }

            table = GetTask();
        }
    }

    delete[] deletedList;
    GetSessionManager()->DestroySessionContext(sessionContext);
    MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
    MOT_LOG_INFO("%s - Exiting", threadName);
}

bool CheckpointWorkerPool::BeginFile(int& fd, uint32_t tableId, int seg, uint64_t exId)
{
    std::string fileName;
    CheckpointUtils::MakeCpFilename(tableId, fileName, m_cpManager.GetWorkingDir(), seg);
    if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::BeginFile: failed to create file: %s", fileName.c_str());
        return false;
    }
    MOT_LOG_DEBUG("CheckpointWorkerPool::beginFile: %s", fileName.c_str());
    CheckpointUtils::FileHeader fileHeader{CheckpointUtils::HEADER_MAGIC, tableId, exId, 0};
    if (CheckpointUtils::WriteFile(fd, (const char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
        sizeof(CheckpointUtils::FileHeader)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::BeginFile: failed to write file header: %s", fileName.c_str());
        (void)CheckpointUtils::CloseFile(fd);
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
        CheckpointUtils::FileHeader fileHeader{CheckpointUtils::HEADER_MAGIC, tableId, exId, numOps};
        if (CheckpointUtils::WriteFile(fd, (const char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
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
    CheckpointUtils::MakeMdFilename(tableId, fileName, m_cpManager.GetWorkingDir());

    if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to create md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    CheckpointUtils::MetaFileHeader mFileHeader = {};
    mFileHeader.m_fileHeader.m_magic = CheckpointUtils::HEADER_MAGIC;
    mFileHeader.m_fileHeader.m_tableId = tableId;
    mFileHeader.m_fileHeader.m_exId = exId;
    mFileHeader.m_entryHeader.m_base.m_dataLen = tableSize;

    if (CheckpointUtils::WriteFile(fd, (const char*)&mFileHeader, sizeof(CheckpointUtils::MetaFileHeader)) !=
        sizeof(CheckpointUtils::MetaFileHeader)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableMetadata: failed to write to md file: %s", fileName.c_str());
        delete[] tableBuf;
        return ErrCodes::FILE_IO;
    }

    if (CheckpointUtils::WriteFile(fd, (const char*)tableBuf, tableSize) != tableSize) {
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
        if (CheckpointUtils::WriteFile(fd, (const char*)buffer->Data(), buffer->Size()) != buffer->Size()) {
            return false;
        }
        buffer->Reset();
    }
    return true;
}

bool CheckpointWorkerPool::KeepIterating(
    Index* index, IndexIterator*& it, uint32_t& numIterations, uint16_t threadId, GcManager* gcSession, ErrCodes& err)
{
    err = ErrCodes::SUCCESS;
    if (it->IsValid() && numIterations >= MAX_ITERS_COUNT) {
        MaxKey lastKey;
        lastKey.CpKey(*(const Key*)(it->GetKey()));
        delete it;
        gcSession->GcReinitEpoch();
        bool found = false;
        it = index->Search(&lastKey, true, true, threadId, found);
        if (it == nullptr) {
            MOT_LOG_ERROR("CheckpointWorkerPool::KeepIterating: failed to obtain iterator");
            err = ErrCodes::MEMORY;
            return false;
        }
        numIterations = 0;
    }
    return it->IsValid();
}

CheckpointWorkerPool::ErrCodes CheckpointWorkerPool::WriteTableDataFile(Table* table, Buffer* buffer,
    DeletePair* deletedList, GcManager* gcSession, uint16_t threadId, uint32_t& maxSegId, uint64_t& numOps)
{
    uint32_t tableId = table->GetTableId();
    uint64_t exId = table->GetTableExId();
    int fd = -1;
    uint16_t deletedListLocation = 0;
    uint64_t currFileOps = 0;
    uint32_t curSegLen = 0;
    uint32_t numIterations = 0;

    maxSegId = 0;
    numOps = 0;
    Index* index = table->GetPrimaryIndex();
    if (index == nullptr) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: failed to get primary index for table %u", tableId);
        return ErrCodes::INDEX;
    }

    if (gcSession->GcStartTxn() != RC_OK) {
        MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: initial GcStartTxn failed");
        return ErrCodes::MEMORY;
    }

    IndexIterator* it = index->Begin(threadId);
    if (it == nullptr) {
        MOT_LOG_ERROR(
            "CheckpointWorkerPool::WriteTableDataFile: failed to get iterator for primary index on table %u", tableId);
        gcSession->GcEndTxn();
        return ErrCodes::INDEX;
    }

    if (!BeginFile(fd, tableId, maxSegId, exId)) {
        MOT_LOG_ERROR(
            "CheckpointWorkerPool::WriteTableDataFile: failed to create data file %u for table %u", maxSegId, tableId);
        delete it;
        gcSession->GcEndTxn();
        return ErrCodes::FILE_IO;
    }
    gcSession->SetGcType(GcManager::GC_TYPE::GC_CHECKPOINT);
    ErrCodes errCode = ErrCodes::SUCCESS;
    bool executeGcTxnFailure = false;
    bool isDeleted = false;
    bool needGc = false;
    Row* deletedVersion = nullptr;
    while (KeepIterating(index, it, numIterations, threadId, gcSession, errCode)) {
        MOT::PrimarySentinel* sentinel = static_cast<PrimarySentinel*>(it->GetPrimarySentinel());
        MOT_ASSERT(sentinel);
        if (sentinel == nullptr) {
            MOT_LOG_ERROR("CheckpointWorkerPool::WriteTableDataFile: encountered a null sentinel");
            it->Next();
            continue;
        }
        int ckptStatus = Checkpoint(buffer, sentinel, fd, threadId, isDeleted, deletedVersion);
        needGc = (isDeleted and !executeGcTxnFailure);
        if (needGc) {
            if (!ExecuteMicroGcTransaction(deletedList, gcSession, table, deletedListLocation, DELETE_LIST_SIZE)) {
                executeGcTxnFailure = true;
            }
            deletedList[deletedListLocation].first = sentinel;
            deletedList[deletedListLocation].second = deletedVersion;
            deletedListLocation++;
        }
        if (ckptStatus == 1) {
            currFileOps++;
            curSegLen += table->GetTupleSize() + sizeof(CheckpointUtils::EntryHeader);
            if (curSegLen >= m_checkpointSegsize) {
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
        numIterations++;
        it->Next();
    }

    delete it;
    it = nullptr;

    // Clean leftovers (if any).
    needGc = (deletedListLocation > 0 && !executeGcTxnFailure);
    if (needGc) {
        MOT_LOG_DEBUG("Checkpoint worker clean leftovers, Table %s deletedListLocation = %u",
            table->GetTableName().c_str(),
            deletedListLocation);
        (void)ExecuteMicroGcTransaction(deletedList, gcSession, table, deletedListLocation, deletedListLocation);
    }
    gcSession->GcEndTxn();
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
