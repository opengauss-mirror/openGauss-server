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
 * checkpoint_utils.h
 *    Checkpoint utility functions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/checkpoint/checkpoint_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_UTILS_H
#define CHECKPOINT_UTILS_H

#include "row.h"
#include <thread>
#include "mot_engine.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

const uint64_t CP_MGR_MAGIC = 0xaabbccdd;

namespace MOT {
namespace CheckpointUtils {

/**
 * @brief A wrapper function that checks if a file exists
 * @param fileName The file name to check
 * @return Boolean value denoting success or failure.
 */
bool IsFileExists(std::string fileName);

/**
 * @brief A wrapper function that checks if a dir exists
 * @param fileName The directory name to check
 * @return Boolean value denoting success or failure.
 */
bool IsDirExists(std::string dirName);

/**
 * @brief A wrapper function that opens a file for writing.
 * @param fileName The file name to open
 * @param pFile The returned FILE* pointer
 * @return Boolean value denoting success or failure.
 */
bool OpenFileWrite(std::string fileName, FILE*& pFile);

/**
 * @brief A wrapper function that opens a file for reading.
 * @param fileName The file name to open
 * @param pFile The returned FILE* pointer
 * @return Boolean value denoting success or failure.
 */
bool OpenFileRead(std::string fileName, FILE*& pFile);

/**
 * @brief A wrapper function that opens a file for writing.
 * @param fileName The file name to open
 * @param fd The returned file descriptor
 * @return Boolean value denoting success or failure.
 */
bool OpenFileWrite(std::string fileName, int& fd);

/**
 * @brief A wrapper function that opens a file for reading.
 * @param fileName The file name to open
 * @param fd The returned file descriptor
 * @return Boolean value denoting success or failure.
 */
bool OpenFileRead(std::string fileName, int& fd);

/**
 * @brief a wrapper function that closes a file fd.
 * @param fd The file descriptor to close
 * @return Int value that equals to 0 on success.
 */
int CloseFile(int fd);

/**
 * @brief A wrapper function that writes to a file fd.
 * @param fd The file descriptor to write to.
 * @param data A pointer to the data buffer to write from.
 * @param len The number of bytes to write
 * @return size_t value equals to the bytes that were written.
 */
size_t WriteFile(int fd, char* data, size_t len);

/**
 * @brief A wrapper function that reads from a file fd.
 * @param fd The file descriptor to read from.
 * @param data A pointer to the data buffer to read to.
 * @param len The number of bytes to read
 * @return size_t The number of bytes that were read.
 */
size_t ReadFile(int fd, char* data, size_t len);

/**
 * @brief A wrapper function that flushes a file fd.
 * @param fd The file descriptor to flush.
 * @return Int value which is equals to -1 on failure.
 */
int FlushFile(int fd);

/**
 * @brief A wrapper function that seesk within a file fd.
 * @param fd The file descriptor to seek in.
 * @param offset The offset in the file to seek to.
 * @return Boolean value denoting success or failure.
 */
bool SeekFile(int fd, uint64_t offset);

/**
 * @brief Frees a row's stable version row.
 * @param row The row which stable version needs to be freed.
 */
inline void DestroyStableRow(Row* row)
{
    if (row != nullptr) {
        row->GetTable()->DestroyRow(row);
    }
}

/**
 * @brief Creates and assigns a stable version of the row.
 * @param origRow The original row which its stable version data needs to be created.
 * @return Boolean value denoting success or failure.
 */
inline bool CreateStableRow(Row* origRow)
{
    Row* tmpRow = nullptr;
    Sentinel* s = origRow->GetPrimarySentinel();
    MOT_ASSERT(s);
    if (s == nullptr) {
        return false;
    }

    if (!s->GetStable()) {
        tmpRow = origRow->CreateCopy();
        if (tmpRow == nullptr) {
            return false;
        }
        s->SetStable(tmpRow);
    } else {
        tmpRow = s->GetStable();
        tmpRow->DeepCopy(origRow);
    }
    return true;
}

// Checkpoint dir prefix
static const char* dirPrefix = "chkpt_";

// Checkpoint file suffix
static const char* cpFileSuffix = ".cp";

// Metadata file suffix
static const char* mdFileSuffix = ".md";

// Map file suffix
static const char* mapFileSuffix = ".map";

// TPC file suffix
static const char* tpcFileSuffix = ".tpc";

// End file suffix
static const char* validFileSuffix = ".end";

// Max path len
static const size_t maxPath = 1024;

/**
 * @brief Returns the current working directory.
 * @param dir The returned directory string.
 * @return Boolean value denoting success or failure.
 */
bool GetWorkingDir(std::string& dir);

/**
 * @brief Creates the checkpoint directory name according to
 * the checkpoint Id info.
 * @param workingDir The returned directory string.
 * @param cpId The checkpoint id.
 * @return Boolean value denoting success or failure.
 */
inline bool SetDirName(std::string& workingDir, uint64_t cpId)
{
    workingDir.append(dirPrefix);
    workingDir.append(std::to_string(cpId));
    return true;
}

/**
 * @brief Creates the fullpath checkpoint directory according to
 * the checkpoint Id info.
 * @param workingDir The returned directory string.
 * @param cpId The checkpoint id.
 * @return Boolean value denoting success or failure.
 */
inline bool SetWorkingDir(std::string& workingDir, uint64_t cpId)
{
    if (!GetWorkingDir(workingDir)) {
        return false;
    }
    return SetDirName(workingDir, cpId);
}

/**
 * @brief Initializes a canonical file name
 * @param fileName The returned prefix for the filename.
 * @param workingDir The directory in which the file is located.
 */
inline void MakeFilename(std::string& fileName, std::string& workingDir)
{
    fileName.clear();
    fileName.append(workingDir);
    fileName.append("/");
}

/**
 * @brief Creates a map filename according to the checkpoint id
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeMapFilename(std::string& fileName, std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    fileName.append(std::to_string(cpId));
    fileName.append(mapFileSuffix);
}

/**
 * @brief Creates a checkpoint seg filename
 * @param tableId The tabled id that this file contains.
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param seg The segment number.
 */
inline void MakeCpFilename(uint64_t tableId, std::string& fileName, std::string& workingDir, int seg = 0)
{
    MakeFilename(fileName, workingDir);
    fileName.append("tab_");
    fileName.append(std::to_string(tableId));
    fileName.append("_");
    fileName.append(std::to_string(seg));
    fileName.append(cpFileSuffix);
}

/**
 * @brief Creates a checkpoint table metadata filename
 * @param tableId The tabled id that this file contains.
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 */
inline void MakeMdFilename(uint64_t tableId, std::string& fileName, std::string& workingDir)
{
    MakeFilename(fileName, workingDir);
    fileName.append(std::to_string(tableId));
    fileName.append(mdFileSuffix);
}

/**
 * @brief Creates a 2 phase commit recovery filename
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeTpcFilename(std::string& fileName, std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    fileName.append(std::to_string(cpId));
    fileName.append(tpcFileSuffix);
}

/**
 * @brief Creates a checkpoint end/valid filename
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeEndFilename(std::string& fileName, std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    fileName.append(std::to_string(cpId));
    fileName.append(validFileSuffix);
}

/**
 * @brief Sets the cpu affinity for a given thread
 * @param cpu The cpu that the thread should run on.
 * @param thread The thread to set the affinity to.
 * @return Int value equals 0 on success.
 */
inline int SetAffinity(int cpu, std::thread& thread)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    return pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
}

struct FileHeader {
    uint64_t m_magic;
    uint64_t m_tableId;
    uint64_t m_exId;
    uint64_t m_numOps;
};

struct EntryHeader {
    uint64_t m_csn;
    uint64_t m_rowId;
    uint32_t m_dataLen;
    uint16_t m_keyLen;
};

struct MetaFileHeader {
    FileHeader m_fileHeader;
    EntryHeader m_entryHeader;
};

struct MapFileHeader {
    uint64_t m_magic;
    uint64_t m_numEntries;
};

struct TpcFileHeader {
    uint64_t m_magic;
    uint64_t m_numEntries;
};

struct TpcEntryHeader {
    uint64_t m_magic;
    uint64_t m_len;
};

/**
 * @brief Produces a pretty hex printout of a given buffer to stderr
 * @param msg A text the will be displayed before the hex data printout.
 * @param b The buffer to print.
 * @param bufLen the buffer length
 */
void Hexdump(const char* msg, char* b, uint32_t bufLen);
}  // namespace CheckpointUtils
}  // namespace MOT

#endif  // CHECKPOINT_UTILS_H
