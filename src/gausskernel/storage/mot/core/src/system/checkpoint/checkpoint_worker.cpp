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
 *    src/gausskernel/storage/mot/core/src/system/checkpoint/checkpoint_worker.cpp
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

    if (!CheckpointUtils::SetWorkingDir(m_workingDir, m_checkpointId))
        m_cpManager.OnError(ErrCodes::FILE_IO, "failed to setup working dir");

    if (!CheckpointManager::CreateCheckpointDir(m_workingDir))
        m_cpManager.OnError(ErrCodes::FILE_IO, "failed to create working dir", m_workingDir.c_str());

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

int CheckpointWorkerPool::Checkpoint(Buffer* buffer, Sentinel* sentinel, int fd, int tid)
{
    Row* mainRow = sentinel->GetData();
    int wrote = 0;

    if (mainRow != nullptr) {
        bool headerLocked = sentinel->TryLock(tid);

        if (headerLocked == false) {
            if (mainRow->GetTwoPhaseMode() == true) {
                MOT_LOG_DEBUG("checkpoint: row %p is 2pc", mainRow);
                return wrote;
            }
            sentinel->Lock(tid);
        }
    }

    Row* stableRow = sentinel->GetStable();
    bool deleted = !sentinel->IsCommited(); /* this currently indicates if the row is deleted or not */
    bool statusBit = sentinel->GetStableStatus();

    do {
        if (statusBit == !m_na) { /* has stable version */
            if (!deleted && stableRow == nullptr)
                break;
            if (deleted && stableRow == nullptr)
                break;
            if (stableRow != nullptr) {
                if (!Write(buffer, stableRow, fd)) {
                    wrote = -1;
                } else {
                    CheckpointUtils::DestroyStableRow(stableRow);
                    sentinel->SetStable(nullptr);
                    wrote = 1;
                }
                break;
            }
        } else { /* no stable version */
            if (stableRow == nullptr) {
                if (deleted) {
                    wrote = 0;
                    break;
                }
                if (mainRow == nullptr) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::checkpoint - null main row!");
                    wrote = 0;
                    break;
                }
                sentinel->SetStableStatus(!m_na);
                if (!Write(buffer, mainRow, fd))
                    wrote = -1;  // we failed to write, set error
                else
                    wrote = 1;
                break;
            }
            if (stableRow != nullptr) { /* should not happen! */
                wrote = -1;
                m_cpManager.OnError(ErrCodes::CALC, "Calc logic error - stable row");
            }
        }
    } while (0);

    if (mainRow != nullptr)
        sentinel->Release();
    return wrote;
}

bool CheckpointWorkerPool::GetTask(uint32_t& task)
{
    bool ret = false;
    do {
        m_tasksLock.lock();
        if (m_tasksList.empty())
            break;
        task = m_tasksList.front();
        m_tasksList.pop_front();
        ret = true;
    } while (0);
    m_tasksLock.unlock();
    return ret;
}

void CheckpointWorkerPool::WorkerFunc()
{
    MOT_DECLARE_NON_KERNEL_THREAD();
    MOT_LOG_DEBUG("CheckpointWorkerPool::workerFunc");
    Buffer buffer;
    if (!buffer.Initialize()) {
        MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: Failed to initialize buffer");
        m_cpManager.OnError(ErrCodes::MEMORY, "Memory allocation failure");
        MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
        MOT_LOG_DEBUG("thread exiting");
        return;
    }
    SessionContext* sessionContext = GetSessionManager()->CreateSessionContext();

    int threadId = MOTCurrThreadId;
    if (!GetTaskAffinity().SetAffinity(threadId)) {
        MOT_LOG_WARN("Failed to set affinity for checkpoint worker, checkpoint performance may be affected");
    }

    while (true) {
        uint32_t tableId = 0;
        uint64_t exId = 0;
        uint32_t curSegLen = 0;
        uint32_t seg = 0;
        bool taskSucceeded = false;
        Table* table = nullptr;

        if (m_cpManager.ShouldStop())
            break;

        bool haveWork = GetTask(tableId);
        if (haveWork) {
            int fd = -1;

            do {
                uint32_t overallOps = 0;
                table = GetTableManager()->GetTableSafe(tableId);
                if (table == nullptr) {
                    MOT_LOG_INFO(
                        "CheckpointWorkerPool::workerFunc:Table %u does not exist - probably deleted already", tableId);
                    break;
                }

                exId = table->GetTableExId();
                size_t tableSize = table->SerializeSize();
                char* tableBuf = new (std::nothrow) char[tableSize];
                if (tableBuf == nullptr) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: Failed to allocate buffer for table %u", tableId);
                    m_cpManager.OnError(
                        ErrCodes::MEMORY, "Memory allocation failure for table", std::to_string(tableId).c_str());
                    break;
                }

                table->Serialize(tableBuf);

                std::string fileName;
                CheckpointUtils::MakeMdFilename(tableId, fileName, m_workingDir);

                if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: failed to create file: %s", fileName.c_str());
                    delete[] tableBuf;
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to open md file", fileName.c_str());
                    break;
                }

                CheckpointUtils::MetaFileHeader mFileHeader;
                mFileHeader.m_fileHeader.m_magic = CP_MGR_MAGIC;
                mFileHeader.m_fileHeader.m_tableId = tableId;
                mFileHeader.m_fileHeader.m_exId = exId;
                mFileHeader.m_entryHeader.m_dataLen = tableSize;
                if (CheckpointUtils::WriteFile(fd, (char*)&mFileHeader, sizeof(CheckpointUtils::MetaFileHeader)) !=
                    sizeof(CheckpointUtils::MetaFileHeader)) {
                    delete[] tableBuf;
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to write to md file", fileName.c_str());
                    break;
                }

                if (CheckpointUtils::WriteFile(fd, tableBuf, tableSize) != tableSize) {
                    delete[] tableBuf;
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to write to md file", fileName.c_str());
                    break;
                }

                if (CheckpointUtils::FlushFile(fd)) {
                    delete[] tableBuf;
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to flush md file", fileName.c_str());
                    break;
                }
                if (CheckpointUtils::CloseFile(fd)) {
                    delete[] tableBuf;
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to close md file", fileName.c_str());
                    break;
                }

                delete[] tableBuf;
                tableBuf = nullptr;
                fd = -1;

                if (!BeginFile(fd, tableId, seg, exId)) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: failed to create file: %s", fileName.c_str());
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to create data file", fileName.c_str());
                    break;
                }

                Index* index = table->GetPrimaryIndex();
                if (index == nullptr) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: failed to get index for table: %u", tableId);
                    m_cpManager.OnError(ErrCodes::INDEX,
                        "Failed to obtain primary index for table - ",
                        std::to_string(tableId).c_str());
                    break;
                }

                struct timespec start, end;
                uint64_t numOps = 0;
                clock_gettime(CLOCK_MONOTONIC, &start);
                IndexIterator* it = index->Begin(0);
                if (it == nullptr) {
                    MOT_LOG_ERROR(
                        "CheckpointWorkerPool::workerFunc: failed to get iterator for primary index on table: %u",
                        tableId);
                    m_cpManager.OnError(ErrCodes::INDEX,
                        "Failed to obtain primary index iterator for table - ",
                        std::to_string(tableId).c_str());
                    break;
                }

                bool iterationSucceeded = true;
                while (it->IsValid()) {
                    MOT::Sentinel* Sentinel = it->GetPrimarySentinel();
                    MOT_ASSERT(Sentinel);
                    if (Sentinel == nullptr) {
                        MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: encountered a null sentinel");
                        it->Next();
                        continue;
                    }

                    int ckptStatus = Checkpoint(&buffer, Sentinel, fd, threadId);
                    if (ckptStatus == 1) {
                        numOps++;
                        curSegLen += table->GetTupleSize() + sizeof(CheckpointUtils::EntryHeader);
                        if (m_checkpointSegsize > 0 && curSegLen >= m_checkpointSegsize) {
                            if (buffer.Size() > 0) {  // there is data in the buffer that needs to be written
                                if (CheckpointUtils::WriteFile(fd, (char*)buffer.Data(), buffer.Size()) !=
                                    buffer.Size()) {
                                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: failed to write to file: %s",
                                        fileName.c_str());
                                    m_cpManager.OnError(
                                        ErrCodes::FILE_IO, "Failed to write to file - ", fileName.c_str());
                                    iterationSucceeded = false;
                                    break;
                                }
                                buffer.Reset();
                            }

                            seg++;

                            /* FinishFile will reset the fd to -1 on success. */
                            if (!FinishFile(fd, tableId, numOps, exId)) {
                                MOT_LOG_ERROR(
                                    "CheckpointWorkerPool::workerFunc: failed to close file: %s", fileName.c_str());
                                m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to close file - ", fileName.c_str());
                                iterationSucceeded = false;
                                break;
                            }

                            if (!BeginFile(fd, tableId, seg, exId)) {
                                MOT_LOG_ERROR(
                                    "CheckpointWorkerPool::workerFunc: failed to create file: %s", fileName.c_str());
                                m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to create file - ", fileName.c_str());
                                iterationSucceeded = false;
                                break;
                            }
                            overallOps += numOps;
                            numOps = 0;
                            curSegLen = 0;
                        }
                    } else if (ckptStatus < 0) {
                        m_cpManager.OnError(
                            ErrCodes::CALC, "Checkpoint failed for table - ", std::to_string(tableId).c_str());
                        iterationSucceeded = false;
                        break;
                    }
                    it->Next();
                }

                if (it != nullptr) {
                    delete it;
                    it = nullptr;
                }

                if (!iterationSucceeded)
                    break;

                overallOps += numOps;
                if (buffer.Size() > 0) {  // there is data in the buffer that needs to be written
                    if (CheckpointUtils::WriteFile(fd, (char*)buffer.Data(), buffer.Size()) != buffer.Size()) {
                        m_cpManager.OnError(ErrCodes::FILE_IO,
                            "Failed to write remaining data for table - ",
                            std::to_string(tableId).c_str());
                        break;
                    }
                    buffer.Reset();
                }

                /* FinishFile will reset the fd to -1 on success. */
                if (!FinishFile(fd, tableId, numOps, exId)) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::workerFunc: failed to close file: %s", fileName.c_str());
                    m_cpManager.OnError(ErrCodes::FILE_IO, "Failed to close file - ", fileName.c_str());
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
                    "CheckpointWorkerPool::workerFunc: checkpoint of table %u completed in %luus, (%lu elements)",
                    tableId,
                    deltaUs,
                    overallOps);
            } while (0);

            if (fd != -1) {
                /* Error case, OnError is done already and taskSucceeded is false. */
                if (CheckpointUtils::CloseFile(fd)) {
                    MOT_LOG_ERROR("CheckpointWorkerPool::finishFile: failed to close file (id: %u)", tableId);
                }
            }

            if (table != nullptr) {
                table->Unlock();
                m_cpManager.TaskDone(tableId, seg, taskSucceeded);
            } else {
                /* taskSucceeded is false, so this table won't be added to the map file. */
                m_cpManager.TaskDone(tableId, seg, taskSucceeded);

                /* Table is dropped, but we need to continue processing other tables. */
                taskSucceeded = true;
            }

            if (!taskSucceeded) {
                break;
            }
        } else {
            break;
        }
    }

    GetSessionManager()->DestroySessionContext(sessionContext);
    MOT::MOTEngine::GetInstance()->OnCurrentThreadEnding();
    MOT_LOG_DEBUG("thread exiting");
}

bool CheckpointWorkerPool::BeginFile(int& fd, uint32_t tableId, int seg, uint64_t exId)
{
    std::string fileName;
    CheckpointUtils::MakeCpFilename(tableId, fileName, m_workingDir, seg);
    if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::beginFile: failed to create file: %s", fileName.c_str());
        return false;
    }
    MOT_LOG_DEBUG("CheckpointWorkerPool::beginFile: %s", fileName.c_str());
    CheckpointUtils::FileHeader fileHeader{CP_MGR_MAGIC, tableId, exId, 0};
    if (CheckpointUtils::WriteFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
        sizeof(CheckpointUtils::FileHeader)) {
        MOT_LOG_ERROR("CheckpointWorkerPool::beginFile: failed to write file header: %s", fileName.c_str());
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
            MOT_LOG_ERROR("CheckpointWorkerPool::finishFile: failed to seek in file (id: %u)", tableId);
            break;
        }
        CheckpointUtils::FileHeader fileHeader{CP_MGR_MAGIC, tableId, exId, numOps};
        if (CheckpointUtils::WriteFile(fd, (char*)&fileHeader, sizeof(CheckpointUtils::FileHeader)) !=
            sizeof(CheckpointUtils::FileHeader)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::finishFile: failed to write to file (id: %u)", tableId);
            break;
        }
        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::finishFile: failed to flush file (id: %u)", tableId);
            break;
        }
        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("CheckpointWorkerPool::finishFile: failed to close file (id: %u)", tableId);
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
}  // namespace MOT
