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

bool IsFileExists(std::string fileName)
{
    struct stat statBuf = {0};
    if (stat(fileName.c_str(), &statBuf) == -1 && errno == ENOENT) {
        return false;
    } else if (!S_ISREG(statBuf.st_mode)) {
        return false;
    }
    return true;
}

bool IsDirExists(std::string dirName)
{
    struct stat statBuf = {0};
    if (stat(dirName.c_str(), &statBuf) == -1 && errno == ENOENT) {
        return false;
    } else if (!S_ISDIR(statBuf.st_mode)) {
        return false;
    }
    return true;
}

bool OpenFileWrite(std::string fileName, FILE*& pFile)
{
    pFile = fopen(fileName.c_str(), "wb");
    if (pFile == nullptr) {
        MOT_REPORT_SYSTEM_ERROR(fopen, "N/A", "Failed to open file %s for writing", fileName.c_str());
        return false;
    }
    return true;
}

bool OpenFileRead(std::string fileName, FILE*& pFile)
{
    pFile = fopen(fileName.c_str(), "rb");
    if (pFile == nullptr) {
        MOT_REPORT_SYSTEM_ERROR(fopen, "N/A", "Failed to open file %s for reading", fileName.c_str());
        return false;
    }
    return true;
}

bool OpenFileWrite(std::string fileName, int& fd)
{
    fd = open(fileName.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR); /* 0600 */
    if (fd == -1) {
        MOT_REPORT_SYSTEM_ERROR(open, "N/A", "Failed to open file %s for writing", fileName.c_str());
    }
    return (fd != -1);
}

bool OpenFileRead(std::string fileName, int& fd)
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

size_t WriteFile(int fd, char* data, size_t len)
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

bool SeekFile(int fd, uint64_t offset)
{
    int rc = lseek(fd, offset, SEEK_SET);
    if (rc == -1) {
        MOT_REPORT_SYSTEM_ERROR(write, "N/A", "Failed to seek file descriptor %d to offset %" PRIu64, fd);
    }
    return (rc != -1);
}

bool GetWorkingDir(std::string& dir)
{
    dir.clear();
    char cwd[maxPath] = {0};
    size_t checkpointDirLength = GetGlobalConfiguration().m_checkpointDir.length();
    if (checkpointDirLength > 0) {
        errno_t erc = strncpy_s(cwd, maxPath, GetGlobalConfiguration().m_checkpointDir.c_str(), checkpointDirLength);
        securec_check(erc, "\0", "\0");
    } else if (!getcwd(cwd, sizeof(cwd))) {
        MOT_REPORT_SYSTEM_ERROR(getcwd, "N/A", "Failed to get current working directory");
        return false;
    }
    dir.append(cwd);
    dir.append("/");
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
        fprintf(stderr, "%s (len %u)\n", msg, buflen);
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
        fprintf(stderr, "%s\n", line);
    }
}
}  // namespace CheckpointUtils
}  // namespace MOT
