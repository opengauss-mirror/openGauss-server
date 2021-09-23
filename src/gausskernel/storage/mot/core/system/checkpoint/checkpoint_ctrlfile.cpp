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
 * checkpoint_ctrlfile.cpp
 *    Checkpoint control file implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_ctrlfile.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <string>
#include "checkpoint_utils.h"
#include "checkpoint_ctrlfile.h"
#include "spin_lock.h"

namespace MOT {
DECLARE_LOGGER(CheckpointControlFile, Checkpoint);

CheckpointControlFile* CheckpointControlFile::ctrlfileInst = nullptr;

bool CheckpointControlFile::initialized = false;

static spin_lock g_ctrlfileLock;

CheckpointControlFile* CheckpointControlFile::GetCtrlFile()
{
    if (initialized) {
        return ctrlfileInst;
    }

    g_ctrlfileLock.lock();
    ctrlfileInst = new (std::nothrow) CheckpointControlFile();
    if (ctrlfileInst == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Checkpoint", "Failed to allocate memory for checkpoint control file object");
    } else {
        if (!ctrlfileInst->Init()) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INVALID_CFG, "Checkpoint", "Failed to initialize checkpoint control file object");
            delete ctrlfileInst;
            ctrlfileInst = nullptr;
        }
    }
    g_ctrlfileLock.unlock();
    return ctrlfileInst;
}

bool CheckpointControlFile::Init()
{
    if (initialized) {
        return true;
    }

    do {
        if (GetGlobalConfiguration().m_checkpointDir.length() >= CheckpointUtils::maxPath) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_CFG,
                "Checkpoint",
                "Invalid checkpoint_dir configuration, length exceeds max path length");
            break;
        }

        if (!CheckpointUtils::GetWorkingDir(m_fullPath)) {
            MOT_LOG_ERROR("Could not obtain working directory");
            break;
        }

        if (!CheckpointUtils::IsDirExists(m_fullPath)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INVALID_CFG, "Checkpoint", "Invalid checkpoint_dir configuration, directory doesn't exist");
            break;
        }

        // "/" is already appended in CheckpointUtils::GetWorkingDir.
        m_fullPath.append(CTRL_FILE_NAME);
        MOT_LOG_TRACE("CheckpointControlFile: Fullpath - '%s'", m_fullPath.c_str());

        // try to open an old file
        if (!CheckpointUtils::IsFileExists(m_fullPath)) {
            MOT_LOG_INFO("CheckpointControlFile: init - a previous checkpoint was not found");
            m_ctrlFileData.Init();
        } else {
            int fd = -1;
            if (!CheckpointUtils::OpenFileRead(m_fullPath, fd)) {
                MOT_LOG_ERROR("CheckpointControlFile: init - could not open control file");
                break;
            }
            if (CheckpointUtils::ReadFile(fd, (char*)&m_ctrlFileData, sizeof(CtrlFileData)) != sizeof(CtrlFileData)) {
                MOT_LOG_ERROR("CheckpointControlFile: init - failed to read data from file");
                CheckpointUtils::CloseFile(fd);
                break;
            } else {
                MOT_LOG_INFO("CheckpointControlFile: init - loaded file: checkpointId %lu",
                    m_ctrlFileData.entry[0].checkpointId);
            }
            if (CheckpointUtils::CloseFile(fd)) {
                MOT_LOG_ERROR("CheckpointControlFile: init - failed to close file");
                break;
            }
        }
        m_valid = true;
        initialized = true;
        Print();
    } while (0);

    return initialized;
}

bool CheckpointControlFile::Update(uint64_t id, uint64_t lsn, uint64_t lastReplayLsn)
{
    int fd = -1;
    bool ret = false;

    do {
        if (!CheckpointUtils::OpenFileWrite(m_fullPath, fd)) {
            MOT_LOG_ERROR("CheckpointControlFile::update - failed to open file");
            break;
        }

        m_ctrlFileData.entry[0].checkpointId = id;
        m_ctrlFileData.entry[0].lsn = lsn;
        m_ctrlFileData.entry[0].lastReplayLsn = lastReplayLsn;
        m_ctrlFileData.entry[0].metaVersion = MetadataProtoVersion::METADATA_VER_CURR;

        if (CheckpointUtils::WriteFile(fd, (char*)&m_ctrlFileData, sizeof(CtrlFileData)) != sizeof(CtrlFileData)) {
            MOT_LOG_ERROR("CheckpointControlFile::update - failed to write control file");
            CheckpointUtils::CloseFile(fd);
            break;
        }

        if (CheckpointUtils::FlushFile(fd)) {
            MOT_LOG_ERROR("CheckpointControlFile::update - failed to flush control file");
            CheckpointUtils::CloseFile(fd);
            break;
        }

        if (CheckpointUtils::CloseFile(fd)) {
            MOT_LOG_ERROR("CheckpointControlFile::update - failed to close control file");
            break;
        }
        ret = true;
        MOT_LOG_DEBUG("CheckpointControlFile::Update %lu:%lu", id, lsn);
    } while (0);

    return ret;
}

void CheckpointControlFile::Print()
{
    MOT_LOG_DEBUG("CheckpointControlFile: [%lu:%lu:%lu]", GetLsn(), GetId(), GetLastReplayLsn());
}
}  // namespace MOT
