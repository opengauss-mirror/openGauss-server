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

namespace MOT {
namespace CheckpointUtils {

// Checkpoint file header magic number
const uint64_t HEADER_MAGIC = 0xaabbccdd;

// Checkpoint dir prefix
const char* const CKPT_DIR_PREFIX = "chkpt_";

// Checkpoint file suffix
const char* const CKPT_FILE_SUFFIX = ".cp";

// Metadata file suffix
const char* const MD_FILE_SUFFIX = ".md";

// Map file suffix
const char* const MAP_FILE_SUFFIX = ".map";

// TPC file suffix
const char* const TPC_FILE_SUFFIX = ".tpc";

// PTD file suffix
const char* const IPD_FILE_SUFFIX = ".ptd";

// End file suffix
const char* const END_FILE_SUFFIX = ".end";

// Max path length
const size_t MAX_PATH = 1024;

/**
 * @brief A wrapper function that checks if a file exists
 * @param fileName The file name to check
 * @return Boolean value denoting success or failure.
 */
bool IsFileExists(const std::string& fileName);

/**
 * @brief A wrapper function that checks if a dir exists
 * @param fileName The directory name to check
 * @return Boolean value denoting success or failure.
 */
bool IsDirExists(const std::string& dirName);

/**
 * @brief A wrapper function that opens a file for writing.
 * @param fileName The file name to open
 * @param fd The returned file descriptor
 * @return Boolean value denoting success or failure.
 */
bool OpenFileWrite(const std::string& fileName, int& fd);

/**
 * @brief A wrapper function that opens a file for reading.
 * @param fileName The file name to open
 * @param fd The returned file descriptor
 * @return Boolean value denoting success or failure.
 */
bool OpenFileRead(const std::string& fileName, int& fd);

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
size_t WriteFile(int fd, const char* data, size_t len);

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
bool SeekFile(int fd, off64_t offset);

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
    PrimarySentinel* s = origRow->GetPrimarySentinel();
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
    s->GetStable()->SetStableTid(s->GetTransactionId());
    return true;
}

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
    (void)workingDir.append(CKPT_DIR_PREFIX);
    (void)workingDir.append(std::to_string(cpId));
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
inline void MakeFilename(std::string& fileName, const std::string& workingDir)
{
    fileName.clear();
    (void)fileName.append(workingDir);
    (void)fileName.append("/");
}

/**
 * @brief Creates a map filename according to the checkpoint id
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeMapFilename(std::string& fileName, const std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append(std::to_string(cpId));
    (void)fileName.append(MAP_FILE_SUFFIX);
}

/**
 * @brief Creates a checkpoint seg filename
 * @param tableId The tabled id that this file contains.
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param seg The segment number.
 */
inline void MakeCpFilename(uint64_t tableId, std::string& fileName, const std::string& workingDir, int seg = 0)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append("tab_");
    (void)fileName.append(std::to_string(tableId));
    (void)fileName.append("_");
    (void)fileName.append(std::to_string(seg));
    (void)fileName.append(CKPT_FILE_SUFFIX);
}

/**
 * @brief Creates a checkpoint table metadata filename
 * @param tableId The tabled id that this file contains.
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 */
inline void MakeMdFilename(uint64_t tableId, std::string& fileName, const std::string& workingDir)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append(std::to_string(tableId));
    (void)fileName.append(MD_FILE_SUFFIX);
}

/**
 * @brief Creates a 2 phase commit recovery filename
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeTpcFilename(std::string& fileName, const std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append(std::to_string(cpId));
    (void)fileName.append(TPC_FILE_SUFFIX);
}

/**
 * @brief Creates an in-process data filename.
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeIpdFilename(std::string& fileName, const std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append(std::to_string(cpId));
    (void)fileName.append(IPD_FILE_SUFFIX);
}

/**
 * @brief Creates a checkpoint end/valid filename
 * @param fileName The returned filename string.
 * @param workingDir The directory in which the file should be located.
 * @param cpId The checkpoint id.
 */
inline void MakeEndFilename(std::string& fileName, const std::string& workingDir, uint64_t cpId)
{
    MakeFilename(fileName, workingDir);
    (void)fileName.append(std::to_string(cpId));
    (void)fileName.append(END_FILE_SUFFIX);
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

struct EntryHeaderBase {
    uint64_t m_csn;
    uint64_t m_rowId;
    uint32_t m_dataLen;
    uint16_t m_keyLen;
};

struct EntryHeader {
    EntryHeaderBase m_base;
    uint64_t m_transactionId;
};

struct MetaFileHeaderBase {
    FileHeader m_fileHeader;
    EntryHeaderBase m_entryHeader;
};

struct MetaFileHeader {
    FileHeader m_fileHeader;
    EntryHeader m_entryHeader;
};

struct MapFileHeader {
    uint64_t m_magic;
    uint64_t m_numEntries;
};

struct PendingTxnDataFileHeader {
    uint64_t m_magic;
    uint64_t m_numEntries;
};

/* Deprecated, used in older versions (metaVersion < METADATA_VER_LOW_RTO). */
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
