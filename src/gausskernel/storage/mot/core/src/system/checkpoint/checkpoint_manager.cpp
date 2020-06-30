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
 * checkpoint_manager.cpp
 *    Interface for all checkpoint related tasks.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/checkpoint/checkpoint_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "checkpoint_manager.h"
#include "utilities.h"
#include "row.h"
#include "sentinel.h"
#include "mot_engine.h"
#include "checkpoint_utils.h"
#include "table.h"
#include "index.h"
#include <list>
#include <algorithm>

namespace MOT {
DECLARE_LOGGER(CheckpointManager, Checkpoint);

CheckpointManager::CheckpointManager()
    : m_lock(),
      m_redoLogHandler(MOTEngine::GetInstance()->GetRedoLogHandler()),
      m_cntBit(0),
      m_phase(CheckpointPhase::REST),
      m_availableBit(true),
      m_numCpTasks(0),
      m_numThreads(GetGlobalConfiguration().m_checkpointWorkers),
      m_checkpointValidation(GetGlobalConfiguration().m_validateCheckpoint),
      m_cpSegThreshold(GetGlobalConfiguration().m_checkpointSegThreshold),
      m_stopFlag(false),
      m_checkpointEnded(false),
      m_checkpointError(0),
      m_errorSet(false),
      m_counters{{0}},
      m_lsn(0),
      m_id(0),
      m_lastReplayLsn(0),
      m_emptyCheckpoint(false)
{}

void CheckpointManager::ResetFlags()
{
    m_checkpointEnded = false;
    m_stopFlag = false;
    m_errorSet = false;
    m_emptyCheckpoint = false;
}

CheckpointManager::~CheckpointManager()
{
    if (m_checkpointers != nullptr) {
        delete m_checkpointers;
        m_checkpointers = nullptr;
    }
}

bool CheckpointManager::CreateSnapShot()
{
    if (!CheckpointManager::CreateCheckpointId(m_id)) {
        MOT_LOG_ERROR("Could not begin checkpoint, checkpoint id creation failed");
        OnError(CheckpointWorkerPool::ErrCodes::CALC, "Could not begin checkpoint, checkpoint id creation failed");
        return false;
    }

    MOT_LOG_INFO("Creating MOT checkpoint snapshot: id: %lu", GetId());
    if (m_phase != CheckpointPhase::REST) {
        MOT_LOG_WARN("Could not begin checkpoint, checkpoint is already running");
        OnError(CheckpointWorkerPool::ErrCodes::CALC, "Could not begin checkpoint, checkpoint is already running");
        return false;
    }

    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();

    engine->LockDDLForCheckpoint();
    ResetFlags();

    // Ensure that there are no transactions that started in Checkpoint COMPLETE
    // phase that are not yet completed
    WaitPrevPhaseCommittedTxnComplete();

    // Move to prepare
    m_lock.WrLock();
    MoveToNextPhase();
    m_lock.WrUnlock();

    while (m_phase != CheckpointPhase::CAPTURE) {
        usleep(50000L);
    }

    return !m_errorSet;
}

bool CheckpointManager::SnapshotReady(uint64_t lsn)
{
    MOT_LOG_INFO("MOT snapshot ready. id: %lu, lsn: %lu", GetId(), m_lsn);
    if (m_phase != CheckpointPhase::CAPTURE) {
        MOT_LOG_ERROR("BAD Checkpoint state. Checkpoint ID: %lu, expected: 'CAPTURE', actual: %s",
            GetId(),
            PhaseToString(m_phase));
        m_errorSet = true;
    } else {
        SetLsn(lsn);
        if (m_redoLogHandler != nullptr)
            m_redoLogHandler->WrUnlock();
        MOT_LOG_DEBUG("Checkpoint snapshot ready. Checkpoint ID: %lu, LSN: %lu", GetId(), GetLsn());
    }
    return !m_errorSet;
}

bool CheckpointManager::BeginCheckpoint()
{
    MOT_LOG_INFO("MOT begin checkpoint capture. id: %lu, lsn: %lu", GetId(), m_lsn);
    Capture();
    while (!m_checkpointEnded) {
        usleep(100000L);
    }

    // Move to complete.
    // No need to wait for transactions started in previous checkpoint phase
    // to complete since there are no transactions that start in RESOLVE
    m_lock.WrLock();
    MoveToNextPhase();
    m_lock.WrUnlock();

    if (!m_errorSet) {
        CompleteCheckpoint(GetId());
    }

    // Ensure that there are no transactions that started in Checkpoint CAPTURE
    // phase that are not yet completed before moving to REST phase
    WaitPrevPhaseCommittedTxnComplete();

    // Move to rest
    m_lock.WrLock();
    MoveToNextPhase();
    m_lock.WrUnlock();

    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    engine->UnlockDDLForCheckpoint();
    return !m_errorSet;
}

bool CheckpointManager::Abort()
{
    // Assumption is that Abort can only be called before BeginCheckpoint and after
    // Snapshot was already taken
    if (m_phase == CheckpointPhase::CAPTURE) {
        // Release the redo_lock write lock to enable other transactions to continue
        if (m_redoLogHandler != nullptr) {
            m_redoLogHandler->WrUnlock();
        }

        // Move to completed.
        // No need to ensure that checkpoint previous phase transactions are completed
        // since there are no new transactions in RESOLVE phase
        m_lock.WrLock();
        MoveToNextPhase();
        m_lock.WrUnlock();

        // Ensure that there are no transactions that started in Checkpoint CAPTURE
        // phase that are not yet completed before moving to REST phase
        WaitPrevPhaseCommittedTxnComplete();

        // Move to rest
        m_lock.WrLock();
        MoveToNextPhase();
        m_lock.WrUnlock();

        MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
        engine->UnlockDDLForCheckpoint();
    }
    return true;
}

void CheckpointManager::BeginTransaction(TxnManager* txn)
{
    // This deviates from the CALC algorithm. We don't want transactions
    // to begin in the RESOLVE phase. Transactions for which the commit
    // starts at the RESOLVE phase are not part of the checkpoint.
    // The RESOLVE phase is the "cutoff" phase. In our case, we avoid
    // transactions from starting at the RESOLVE phase because we don't
    // want them to be written to the redo-log before we take the
    // redo-log LSN (redo-point). The redo-point will be taken by the
    // envelop after we reach the CAPTURE phase.
    m_lock.RdLock();
    while (m_phase == CheckpointPhase::RESOLVE) {
        m_lock.RdUnlock();
        usleep(5000);
        m_lock.RdLock();
    }
    txn->m_checkpointPhase = m_phase;
    txn->m_checkpointNABit = !m_availableBit;
    m_counters[m_cntBit].fetch_add(1);
    m_lock.RdUnlock();
}

void CheckpointManager::AbortTransaction(TxnManager* txn)
{
    TransactionCompleted(txn);
}

void CheckpointManager::CommitTransaction(TxnManager* txn, int writeSetSize)
{
    TxnOrderedSet_t& orderedSet = txn->m_accessMgr->GetOrderedRowSet();
    if (txn->m_checkpointPhase == PREPARE) {
        // This deviates from CALC original algorithm...
        // All transactions started in PREPARE phase should be part of the
        // checkpoint regardless of their completion phase. This is needed
        // because redo point is taken later in the capture phase.
        const Access* access = nullptr;
        for (const auto& ra_pair : orderedSet) {
            access = ra_pair.second;
            if (access->m_type == RD) {
                continue;
            }
            if (access->m_params.IsPrimarySentinel()) {
                MOT_ASSERT(access->GetRowFromHeader()->GetPrimarySentinel());
                CheckpointUtils::DestroyStableRow(access->GetRowFromHeader()->GetStable());
                access->GetRowFromHeader()->GetPrimarySentinel()->SetStable(nullptr);
                writeSetSize--;
            }
            if (!writeSetSize) {
                break;
            }
        }
    }
}

void CheckpointManager::TransactionCompleted(TxnManager* txn)
{
    if (txn->m_checkpointPhase == CheckpointPhase::NONE)
        return;
    m_lock.RdLock();
    CheckpointPhase current_phase = m_phase;
    if (txn->m_checkpointPhase == m_phase) {  // current phase
        m_counters[m_cntBit].fetch_sub(1);
    } else {  // previous phase
        m_counters[!m_cntBit].fetch_sub(1);
    }
    m_lock.RdUnlock();

    if (m_counters[!m_cntBit] == 0 && IsAutoCompletePhase()) {
        m_lock.WrLock();
        // If the state was not change by another thread in the meanwhile,
        // I am the first one to change it
        if (current_phase == m_phase) {
            MoveToNextPhase();
        }
        m_lock.WrUnlock();
    }
}

void CheckpointManager::WaitPrevPhaseCommittedTxnComplete()
{
    m_lock.RdLock();
    while (m_counters[!m_cntBit] != 0) {
        m_lock.RdUnlock();
        usleep(10000);
        m_lock.RdLock();
    }
    m_lock.RdUnlock();
}

void CheckpointManager::MoveToNextPhase()
{
    uint8_t nextPhase = ((uint8_t)m_phase + 1) % ((uint8_t)COMPLETE + 1);
    nextPhase = (!nextPhase ? 1 : nextPhase);

    MOT_LOG_DEBUG("CHECKPOINT: Move from %s to %s phase. current phase count: %u, previous phase count: %u",
        CheckpointManager::PhaseToString(m_phase),
        CheckpointManager::PhaseToString(static_cast<CheckpointPhase>(nextPhase)),
        (uint32_t)m_counters[m_cntBit],
        (uint32_t)m_counters[!m_cntBit]);

    // just before changing phase, switch available & not available bits
    if (nextPhase == REST) {
        SwapAvailableAndNotAvailable();
    }

    m_phase = (CheckpointPhase)nextPhase;
    m_cntBit = !m_cntBit;

    if (m_phase == CheckpointPhase::CAPTURE && m_redoLogHandler != nullptr) {
        // hold the redo log lock to avoid inserting additional entries to the
        // log. Once snapshot is taken, this lock will be released in SnapshotReady().
        m_redoLogHandler->WrLock();
    }

    if (m_phase == PREPARE && m_checkpointValidation == true) {
        Checkbits();
    }

    // there are no open transactions from previous phase, we can move forward to next phase
    if (m_counters[!m_cntBit] == 0 && IsAutoCompletePhase()) {
        MoveToNextPhase();
    }
}

void CheckpointManager::Checkbits()
{
    GetTableManager()->AddTableIdsToList(m_tasksList);
    m_numCpTasks = m_tasksList.size();
    while (!m_tasksList.empty()) {
        uint32_t curId = m_tasksList.front();
        MOT_LOG_DEBUG("checkbits - %u", curId);
        Table* table = GetTableManager()->GetTable(curId);
        if (table == nullptr) {
            MOT_LOG_ERROR("could not find tableId %u", curId);
            continue;
        }

        m_tasksList.pop_front();
        m_numCpTasks--;
        Index* index = table->GetPrimaryIndex();
        if (index == nullptr) {
            MOT_LOG_ERROR("could not get primary index for tableId %u", curId);
            continue;
        }

        IndexIterator* it = index->Begin(0);
        while (it != nullptr && it->IsValid()) {
            MOT::Sentinel* Sentinel = it->GetPrimarySentinel();
            MOT::Row* r = Sentinel->GetData();
            if (!r->IsAbsentRow() && Sentinel->GetStableStatus() == m_availableBit)
                MOT_LOG_ERROR("CHECKPOINT, AVAILABLE BIT IS SET");
            if (Sentinel->GetStable() != nullptr)
                MOT_LOG_ERROR("CHECKPOINT, HAS STABLE DATA!!!");
            it->Next();
        }
    }
    MOT_LOG_DEBUG("checkbits - done");
}

const char* CheckpointManager::PhaseToString(CheckpointPhase phase)
{
    switch (phase) {
        case NONE:
            return "NONE";
        case REST:
            return "REST";
        case PREPARE:
            return "PREPARE";
        case RESOLVE:
            return "RESOLVE";
        case CAPTURE:
            return "CAPTURE";
        case COMPLETE:
            return "COMPLETE";
    }
    return "UNKNOWN";
}

bool CheckpointManager::ApplyWrite(TxnManager* txnMan, Row* origRow, AccessType type)
{
    CheckpointPhase startPhase = txnMan->m_checkpointPhase;
    Sentinel* s = origRow->GetPrimarySentinel();
    MOT_ASSERT(s);
    if (s == nullptr) {
        MOT_LOG_ERROR("No sentinel on row!");
        return false;
    }

    bool statusBit = s->GetStableStatus();
    switch (startPhase) {
        case REST:
            if (type == INS)
                s->SetStableStatus(!m_availableBit);
            break;
        case PREPARE:
            if (type == INS)
                s->SetStableStatus(!m_availableBit);
            else if (statusBit == !m_availableBit) {
                if (!CheckpointUtils::SetStableRow(origRow))
                    return false;
            }
            break;
        case RESOLVE:
        case CAPTURE:
            if (type == INS)
                s->SetStableStatus(m_availableBit);
            else {
                if (statusBit == !m_availableBit) {
                    if (!CheckpointUtils::SetStableRow(origRow))
                        return false;
                    s->SetStableStatus(m_availableBit);
                }
            }
            break;
        case COMPLETE:
            if (type == INS)
                s->SetStableStatus(!txnMan->m_checkpointNABit);
            break;
        default:
            MOT_LOG_ERROR("Unknown transaction start phase: %s", CheckpointManager::PhaseToString(startPhase));
    }

    return true;
}

void CheckpointManager::FillTasksQueue()
{
    if (!m_tasksList.empty()) {
        MOT_LOG_ERROR("CheckpointManager::fillTasksQueue: queue is not empty!");
        OnError(CheckpointWorkerPool::ErrCodes::CALC, "CheckpointManager::fillTasksQueue: queue is not empty!");
        return;
    }
    GetTableManager()->AddTableIdsToList(m_tasksList);
    m_numCpTasks = m_tasksList.size();
    m_mapfileInfo.clear();
    MOT_LOG_DEBUG("CheckpointManager::fillTasksQueue:: got %d tasks", m_tasksList.size());
}

void CheckpointManager::TaskDone(uint32_t tableId, uint32_t numSegs, bool success)
{
    if (success) { /* only successful tasks are added to the map file */
        MapFileEntry* entry = new (std::nothrow) MapFileEntry();
        if (entry != nullptr) {
            entry->m_id = tableId;
            entry->m_numSegs = numSegs;
            MOT_LOG_DEBUG("TaskDone %lu: %u %u segs", GetId(), tableId, numSegs);
            std::lock_guard<std::mutex> guard(m_mapfileMutex);
            m_mapfileInfo.push_back(entry);
        } else {
            OnError(CheckpointWorkerPool::ErrCodes::MEMORY, "Failed to allocate map file entry");
            return;
        }
    }

    if (--m_numCpTasks == 0) {
        m_checkpointEnded = true;
    }
}

void CheckpointManager::CompleteCheckpoint(uint64_t checkpointId)
{
    if (m_emptyCheckpoint == true && CreateEmptyCheckpoint() == false) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Failed to create empty checkpoint");
        return;
    }

    CheckpointControlFile* ctrlFile = CheckpointControlFile::GetCtrlFile();
    if (ctrlFile == nullptr) {
        OnError(CheckpointWorkerPool::ErrCodes::MEMORY, "Failed to retrieve control file object");
        return;
    }

    if (!CreateCheckpointMap(checkpointId)) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Failed to create map file");
        return;
    }

    if (!CreateTpcRecoveryFile(checkpointId)) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Failed to create 2pc recovery file");
        return;
    }

    if (!ctrlFile->IsValid()) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Invalid control file");
        return;
    }

    m_fetchLock.WrLock();
    if (!ctrlFile->Update(checkpointId, GetLsn(), GetLastReplayLsn())) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Failed to update control file");
        return;
    }

    GetRecoveryManager()->SetCheckpointId(checkpointId);

    if (!CreateEndFile(checkpointId)) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "Failed to create completion file");
        return;
    }

    m_fetchLock.WrUnlock();
    RemoveOldCheckpoints(checkpointId);
    MOT_LOG_INFO("Checkpoint [%lu] completed", checkpointId);
}

void CheckpointManager::DestroyCheckpointers()
{
    if (m_checkpointers != nullptr) {
        delete m_checkpointers;
        m_checkpointers = nullptr;
    }
}

void CheckpointManager::CreateCheckpointers()
{
    m_checkpointers = new (std::nothrow)
        CheckpointWorkerPool(m_numThreads, !m_availableBit, m_tasksList, m_cpSegThreshold, m_id, *this);
}

void CheckpointManager::Capture()
{
    MOT_LOG_DEBUG("CheckpointManager::capture");
    if (m_numCpTasks) {
        MOT_LOG_ERROR("The number of tasks is  %d, cannot start capture!", m_numCpTasks.load());
        return;
    }

    FillTasksQueue();

    if (m_numCpTasks == 0) {
        MOT_LOG_INFO("No tasks in queue - empty checkpoint");
        m_emptyCheckpoint = true;
        m_checkpointEnded = true;
    } else {
        DestroyCheckpointers();
        CreateCheckpointers();
        if (m_checkpointers == nullptr) {
            OnError(CheckpointWorkerPool::ErrCodes::MEMORY, "failed to spawn checkpoint threads");
            return;
        }
    }
}

void CheckpointManager::RemoveOldCheckpoints(uint64_t curCheckcpointId)
{
    std::string workingDir = "";
    if (CheckpointUtils::GetWorkingDir(workingDir) == false) {
        MOT_LOG_ERROR("RemoveOldCheckpoints: failed to get the working dir");
        return;
    }

    DIR* dir = opendir(workingDir.c_str());
    if (dir) {
        struct dirent* p;
        while ((p = readdir(dir))) {
            /* Skip the names "." and ".." and anything that is not chkpt_ */
            if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..") ||
                strncmp(p->d_name, CheckpointUtils::dirPrefix, strlen(CheckpointUtils::dirPrefix)) ||
                strlen(p->d_name) <= strlen(CheckpointUtils::dirPrefix)) {
                continue;
            }

            /* Skip if the entry is not a directory. Handle DT_UNKNOWN for file systems that does not support d_type. */
            if (p->d_type != DT_DIR && p->d_type != DT_UNKNOWN) {
                continue;
            }

            uint64_t chkptId = strtoll(p->d_name + strlen(CheckpointUtils::dirPrefix), NULL, 10);
            if (chkptId == curCheckcpointId) {
                MOT_LOG_DEBUG("RemoveOldCheckpoints: exclude %lu", chkptId);
                continue;
            }
            MOT_LOG_DEBUG("RemoveOldCheckpoints: removing %lu", chkptId);
            RemoveCheckpointDir(chkptId);
        }
        closedir(dir);
    } else {
        MOT_LOG_ERROR("RemoveOldCheckpoints: failed to open dir: %s, error %d - %s",
            workingDir.c_str(),
            errno,
            gs_strerror(errno));
    }
}

void CheckpointManager::RemoveCheckpointDir(uint64_t checkpointId)
{
    errno_t erc;
    std::string oldCheckpointDir;
    if (!CheckpointUtils::SetWorkingDir(oldCheckpointDir, checkpointId)) {
        MOT_LOG_ERROR("removeCheckpointDir: failed to set working directory");
        return;
    }
    char* buf = (char*)malloc(CheckpointUtils::maxPath);
    if (buf == nullptr) {
        MOT_LOG_ERROR("removeCheckpointDir: failed to allocate buffer");
        return;
    }

    DIR* dir = opendir(oldCheckpointDir.c_str());
    if (dir != nullptr) {
        struct dirent* p;
        while ((p = readdir(dir))) {
            /* Skip the names "." and ".." */
            if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, ".."))
                continue;

            struct stat statbuf = {0};
            erc = memset_s(buf, CheckpointUtils::maxPath, 0, CheckpointUtils::maxPath);
            securec_check(erc, "\0", "\0");
            erc = snprintf_s(buf,
                CheckpointUtils::maxPath,
                CheckpointUtils::maxPath - 1,
                "%s/%s",
                oldCheckpointDir.c_str(),
                p->d_name);
            securec_check_ss(erc, "\0", "\0");
            if (!stat(buf, &statbuf) && S_ISREG(statbuf.st_mode)) {
                MOT_LOG_DEBUG("removeCheckpointDir: deleting %s", buf);
                unlink(buf);
            }
        }
        closedir(dir);
        MOT_LOG_DEBUG("removeCheckpointDir: removing dir %s", oldCheckpointDir.c_str());
        rmdir(oldCheckpointDir.c_str());
    } else {
        MOT_LOG_ERROR("removeCheckpointDir: failed to open dir: %s, error %d - %s",
            oldCheckpointDir.c_str(),
            errno,
            gs_strerror(errno));
    }

    free(buf);
}

bool CheckpointManager::CreateCheckpointMap(uint64_t checkpointId)
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;

    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, checkpointId))
            break;

        CheckpointUtils::MakeMapFilename(fileName, workingDir, checkpointId);
        if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
            MOT_LOG_ERROR("createCheckpointMap: failed to create file '%s' - %d - %s",
                fileName.c_str(),
                errno,
                gs_strerror(errno));
            break;
        }

        CheckpointUtils::MapFileHeader mapFileHeader{CP_MGR_MAGIC, m_mapfileInfo.size()};
        size_t wrStat = CheckpointUtils::WriteFile(fd, (char*)&mapFileHeader, sizeof(CheckpointUtils::MapFileHeader));
        if (wrStat != sizeof(CheckpointUtils::MapFileHeader)) {
            MOT_LOG_ERROR(
                "createCheckpointMap: failed to write map file's header (%d) %d %s", wrStat, errno, gs_strerror(errno));
            break;
        }

        int i = 0;
        for (std::list<MapFileEntry*>::iterator it = m_mapfileInfo.begin(); it != m_mapfileInfo.end(); ++it) {
            MapFileEntry* entry = *it;
            if (CheckpointUtils::WriteFile(fd, (char*)entry, sizeof(MapFileEntry)) != sizeof(MapFileEntry)) {
                MOT_LOG_ERROR("createCheckpointMap: failed to write map file entry");
                break;
            }
            delete entry;
            i++;
        }

        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("createCheckpointMap: failed to flush map file");
            break;
        }

        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("createCheckpointMap: failed to close map file");
            break;
        }
        ret = true;
    } while (0);

    return ret;
}

void CheckpointManager::OnError(int errCode, const char* errMsg, const char* optionalMsg)
{
    m_stopFlag = true;
    m_errorReportLock.lock();
    if (!m_errorSet) {
        m_checkpointError = errCode;
        m_errorMessage.clear();
        m_errorMessage.append(errMsg);
        if (optionalMsg != nullptr) {
            m_errorMessage.append(" ");
            m_errorMessage.append(optionalMsg);
        }
        m_errorSet = true;
        m_checkpointEnded = true;
    }
    m_errorReportLock.unlock();
}

bool CheckpointManager::CreateEmptyCheckpoint()
{
    std::string workingDir;

    if (!CheckpointUtils::SetWorkingDir(workingDir, m_id)) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "failed to setup working dir");
        return false;
    }

    if (!CheckpointManager::CreateCheckpointDir(workingDir)) {
        OnError(CheckpointWorkerPool::ErrCodes::FILE_IO, "failed to create working dir", workingDir.c_str());
        return false;
    }

    return true;
}

bool CheckpointManager::CreateCheckpointId(uint64_t& checkpointId)
{
    if (CheckpointControlFile::GetCtrlFile() == nullptr)
        return false;

    uint64_t curId = CheckpointControlFile::GetCtrlFile()->GetId();
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    checkpointId = time(nullptr);
    while (true) {
        if (checkpointId == curId) {
            checkpointId += 10;
        } else {
            break;
        }
    }
    MOT_LOG_DEBUG("createCheckpointId: %lu ,id:%lu", checkpointId, curId);
    return true;
}

bool CheckpointManager::GetCheckpointDirName(std::string& dirName)
{
    if (!CheckpointUtils::SetDirName(dirName, m_id)) {
        MOT_LOG_ERROR("SetDirName failed");
        return false;
    }
    return true;
}

bool CheckpointManager::GetCheckpointWorkingDir(std::string& workingDir)
{
    if (!CheckpointUtils::GetWorkingDir(workingDir)) {
        MOT_LOG_ERROR("Could not obtain working directory");
        return false;
    }
    return true;
}

bool CheckpointManager::CreateCheckpointDir(std::string& dir)
{
    if (mkdir(dir.c_str(), S_IRWXU)) { /* 0700 */
        MOT_LOG_DEBUG("Failed to create dir %s (%d:%s)", dir.c_str(), errno, gs_strerror(errno));
        return false;
    }
    return true;
}

bool CheckpointManager::CreateTpcRecoveryFile(uint64_t checkpointId)
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;

    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, checkpointId))
            break;

        CheckpointUtils::MakeTpcFilename(fileName, workingDir, checkpointId);
        if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
            MOT_LOG_ERROR("create2PCRecoveryFile: failed to create file '%s' - %d - %s",
                fileName.c_str(),
                errno,
                gs_strerror(errno));
            break;
        }

        GetRecoveryManager()->LockInProcessTxns();
        CheckpointUtils::TpcFileHeader tpcFileHeader;
        tpcFileHeader.m_magic = CP_MGR_MAGIC;
        tpcFileHeader.m_numEntries = GetRecoveryManager()->GetInProcessTxnsSize();
        size_t wrStat = CheckpointUtils::WriteFile(fd, (char*)&tpcFileHeader, sizeof(CheckpointUtils::TpcFileHeader));
        if (wrStat != sizeof(CheckpointUtils::TpcFileHeader)) {
            MOT_LOG_ERROR("create2PCRecoveryFile: failed to write 2pc file's header (%d) [%d %s]",
                wrStat,
                errno,
                gs_strerror(errno));
            break;
        }

        if (tpcFileHeader.m_numEntries > 0 && GetRecoveryManager()->SerializeInProcessTxns(fd) == false) {
            MOT_LOG_ERROR("create2PCRecoveryFile: failed to serialize transactions [%d %s]", errno, gs_strerror(errno));
            break;
        }

        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("create2PCRecoveryFile: failed to flush map file");
            break;
        }

        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("create2PCRecoveryFile: failed to close map file");
            break;
        }
        ret = true;
    } while (0);
    GetRecoveryManager()->UnlockInProcessTxns();
    return ret;
}

bool CheckpointManager::CreateEndFile(uint64_t checkpointId)
{
    int fd = -1;
    std::string fileName;
    std::string workingDir;
    bool ret = false;

    do {
        if (!CheckpointUtils::SetWorkingDir(workingDir, checkpointId)) {
            break;
        }

        CheckpointUtils::MakeEndFilename(fileName, workingDir, checkpointId);
        if (!CheckpointUtils::OpenFileWrite(fileName, fd)) {
            MOT_LOG_ERROR(
                "CreateEndFile: failed to create file '%s' - %d - %s", fileName.c_str(), errno, gs_strerror(errno));
            break;
        }

        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("CreateEndFile: failed to flush map file");
            break;
        }

        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("CreateEndFile: failed to close map file");
            break;
        }
        ret = true;
    } while (0);

    return ret;
}
}  // namespace MOT
