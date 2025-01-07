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
 * checkpoint_utils.cpp
 *    Checkpoint utility functions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "checkpoint_utils.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(CheckpointUtils, Checkpoint);

namespace CheckpointUtils {

bool IsFileExists(const std::string& fileName)
{
    struct stat statBuf = {0};
    if (stat(fileName.c_str(), &statBuf) == -1 && errno == ENOENT) {
        return false;
    } else if (!S_ISREG(statBuf.st_mode)) {
        return false;
    }
    return true;
}

bool IsDirExists(const std::string& dirName)
{
    struct stat statBuf = {0};
    if (stat(dirName.c_str(), &statBuf) == -1 && errno == ENOENT) {
        return false;
    } else if (!S_ISDIR(statBuf.st_mode)) {
        return false;
    }
    return true;
}

bool OpenFileWrite(const std::string& fileName, int& fd)
{
    fd = open(fileName.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR); /* 0600 */
    if (fd == -1) {
        MOT_REPORT_SYSTEM_ERROR(open, "N/A", "Failed to open file %s for writing", fileName.c_str());
    }
    return (fd != -1);
}

bool OpenFileRead(const std::string& fileName, int& fd)
{
    fd = open(fileName.c_str(), O_RDONLY);
    if (fd == -1) {
        MOT_REPORT_SYSTEM_ERROR(open, "N/A", "Failed to open file %s for reading", fileName.c_str());
    }
    return (fd != -1);
}

int CloseFile(int fd)
{
    int rc = close(fd);
    if (rc != 0) {
        MOT_REPORT_SYSTEM_ERROR(open, "N/A", "Failed to close file descriptor %d", fd);
    }
    return rc;
}

size_t WriteFile(int fd, const char* data, size_t len)
{
    ssize_t wrote = write(fd, (const void*)data, len);
    if (wrote == -1) {
        MOT_REPORT_SYSTEM_ERROR(
            write, "N/A", "Failed to write %u bytes at %p to file descriptor %d", (unsigned)len, data, fd);
    }
    // OA: missing case - wrote less than asked but no error
    return (size_t)wrote;
}

size_t ReadFile(int fd, char* data, size_t len)
{
    ssize_t bytesRead = read(fd, (void*)data, len);
    if (bytesRead == -1) {
        MOT_REPORT_SYSTEM_ERROR(
            write, "N/A", "Failed to read %u bytes into %p from file descriptor %d", (unsigned)len, data, fd);
    }
    // OA: missing case - read less than asked but no error
    return (size_t)bytesRead;
}

int FlushFile(int fd)
{
    int rc = fdatasync(fd);
    if (rc == -1) {
        MOT_REPORT_SYSTEM_ERROR(write, "N/A", "Failed to flush file descriptor %d to disk", fd);
    }
    return rc;
}

bool SeekFile(int fd, off64_t offset)
{
    int rc = lseek64(fd, offset, SEEK_SET);
    if (rc == -1) {
        MOT_REPORT_SYSTEM_ERROR(write, "N/A", "Failed to seek file descriptor %d to offset %" PRIu64, fd);
    }
    return (rc != -1);
}

bool GetWorkingDir(std::string& dir)
{
    dir.clear();
    char cwd[CHECKPOINT_MAX_PATH] = {0};
    size_t checkpointDirLength = GetGlobalConfiguration().m_checkpointDir.length();
    if (checkpointDirLength > 0) {
        errno_t erc = strncpy_s(cwd, CHECKPOINT_MAX_PATH, GetGlobalConfiguration().m_checkpointDir.c_str(), checkpointDirLength);
        securec_check(erc, "\0", "\0");
    } else if (!getcwd(cwd, sizeof(cwd))) {
        MOT_REPORT_SYSTEM_ERROR(getcwd, "N/A", "Failed to get current working directory");
        return false;
    }
    (void)dir.append(cwd);
    (void)dir.append("/");
    return true;
}

void Hexdump(const char* msg, char* b, uint32_t buflen)
{
    unsigned char* buf = (unsigned char*)b;
    const size_t bufSize = 256;
    const size_t elemSize = 16;
    char line[bufSize];
    char elem[elemSize];
    uint32_t i, j;
    errno_t erc;

    if (b == nullptr) {
        return;
    }

    if (buflen == 0) {
        return;
    }

    if (msg != nullptr) {
        (void)fprintf(stderr, "%s (len %u)\n", msg, buflen);
    }

    for (i = 0; i < buflen; i += elemSize) {
        erc = memset_s(line, bufSize, 0, bufSize);
        securec_check(erc, "\0", "\0");
        erc = memset_s(elem, elemSize, 0, elemSize);
        securec_check(erc, "\0", "\0");
        erc = snprintf_s(elem, elemSize, (elemSize - 1), "%06x: ", i);
        securec_check_ss(erc, "\0", "\0");
        erc = strcat_s(line, bufSize, elem);
        securec_check(erc, "\0", "\0");

        for (j = 0; j < elemSize; j++) {
            if (i + j < buflen) {
                erc = memset_s(elem, elemSize, 0, elemSize);
                securec_check(erc, "\0", "\0");
                erc = snprintf_s(elem, elemSize, (elemSize - 1), "%02x: ", buf[i + j]);
                securec_check_ss(erc, "\0", "\0");
                erc = strcat_s(line, bufSize, elem);
                securec_check(erc, "\0", "\0");
            } else {
                erc = memset_s(elem, elemSize, 0, elemSize);
                securec_check(erc, "\0", "\0");
                erc = snprintf_s(elem, elemSize, (elemSize - 1), "   ");
                securec_check_ss(erc, "\0", "\0");
                erc = strcat_s(line, bufSize, elem);
                securec_check(erc, "\0", "\0");
            }
        }
        erc = memset_s(elem, elemSize, 0, elemSize);
        securec_check(erc, "\0", "\0");
        erc = snprintf_s(elem, elemSize, (elemSize - 1), " ");
        securec_check_ss(erc, "\0", "\0");
        erc = strcat_s(line, bufSize, elem);
        securec_check(erc, "\0", "\0");

        for (j = 0; j < elemSize; j++) {
            if (i + j < buflen) {
                erc = memset_s(elem, elemSize, 0, elemSize);
                securec_check(erc, "\0", "\0");
                erc = snprintf_s(elem, elemSize, (elemSize - 1), "%c", isprint(buf[i + j]) ? buf[i + j] : '.');
                securec_check_ss(erc, "\0", "\0");
                erc = strcat_s(line, bufSize, elem);
                securec_check(erc, "\0", "\0");
            }
        }
        (void)fprintf(stderr, "%s\n", line);
    }
}

bool CheckMotTable()
{
    MOT_LOG_INFO("start check if there are mot tables");

    uint64_t m_checkpointId;
    if (CheckpointControlFile::GetCtrlFile() == nullptr) {
        MOT_LOG_WARN("CheckpointUtils: no ctrl file");
        return false;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetMetaVersion() > MetadataProtoVersion::METADATA_VER_CURR) {
        MOT_LOG_WARN("CheckpointRecovery: metadata version %u is greater than current %u",
            CheckpointControlFile::GetCtrlFile()->GetMetaVersion(),
            MetadataProtoVersion::METADATA_VER_CURR);
        return false;
    }

    if (CheckpointControlFile::GetCtrlFile()->GetId() == CheckpointControlFile::INVALID_ID) {
        m_checkpointId = CheckpointControlFile::INVALID_ID;  // no mot control was found.
        MOT_LOG_WARN("CheckpointUtils: invalid ctrl file id");
        return false;
    } else {
        m_checkpointId = CheckpointControlFile::GetCtrlFile()->GetId();
    }

    std::string m_workingDir;
    if (!CheckpointUtils::SetWorkingDir(m_workingDir, m_checkpointId)) {
        MOT_LOG_WARN("CheckpointUtils: failed to obtain checkpoint's working dir");
        return false;
    }

    std::string mapFile;
    CheckpointUtils::MakeMapFilename(mapFile, m_workingDir, m_checkpointId);
    int fd = -1;
    if (!CheckpointUtils::OpenFileRead(mapFile, fd)) {
        MOT_LOG_WARN("CheckpointUtils: Failed to open map file '%s'", mapFile.c_str());
        return false;
    }

    CheckpointUtils::MapFileHeader mapFileHeader;
    if (CheckpointUtils::ReadFile(fd, (char*)&mapFileHeader, sizeof(CheckpointUtils::MapFileHeader)) !=
        sizeof(CheckpointUtils::MapFileHeader)) {
        MOT_LOG_WARN("CheckpointUtils: Failed to read map file '%s' header", mapFile.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    if (mapFileHeader.m_magic != CheckpointUtils::HEADER_MAGIC) {
        MOT_LOG_WARN("CheckpointUtils: Failed to verify map file'%s'", mapFile.c_str());
        (void)CheckpointUtils::CloseFile(fd);
        return false;
    }

    CheckpointManager::MapFileEntry entry;
    int count = 0;
    for (uint64_t i = 0; i < mapFileHeader.m_numEntries; i++) {
        if (CheckpointUtils::ReadFile(fd, (char*)&entry, sizeof(CheckpointManager::MapFileEntry)) !=
            sizeof(CheckpointManager::MapFileEntry)) {
            MOT_LOG_WARN("CheckpointUtils: Failed to read map file '%s' entry: %lu",
                mapFile.c_str(),
                i);
            (void)CheckpointUtils::CloseFile(fd);
            return false;
        }
        if (entry.m_maxSegId >= 0) {
            MOT_LOG_ERROR("CheckpointUtils: find mot tables when enable_mot_server = off");
            return true;
        }
    }

    if (CheckpointUtils::CloseFile(fd)) {
        MOT_LOG_WARN("CheckpointUtils: Failed to close map file");
        return false;
    }

    return false;
}
}  // namespace CheckpointUtils
}  // namespace MOT
