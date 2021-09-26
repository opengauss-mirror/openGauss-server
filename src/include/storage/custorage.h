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
 * ---------------------------------------------------------------------------------------
 * 
 * custorage.h
 *         routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/custorage.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CU_STORAGE_H
#define CU_STORAGE_H

#include "storage/cu.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/fd.h"
#include "storage/cstore/cstorealloc.h"

class CUFile;

class CUStorage : public BaseObject {
public:
    CUStorage(const CFileNode& cFileNode, CStoreAllocateStrategy strategy = APPEND_ONLY);
    virtual ~CUStorage();
    virtual void Destroy();

    friend class CUFile;

    // Write CU data into storage
    //
    void SaveCU(_in_ char* write_buf, _in_ uint64 offset, _in_ int size, bool direct_flag, bool for_extension = false);

    void OverwriteCU(
        _in_ char* write_buf, _in_ uint64 offset, _in_ int size, bool direct_flag, bool for_extension = false);

    // Load CU data from storage
    //
    void LoadCU(_in_ CU* cuPtr, _in_ uint64 offset, _in_ int size, bool direct_flag, bool inCUCache);

    void RemoteLoadCU(_in_ CU* cuPtr, _in_ uint64 offset, _in_ int size, bool direct_flag, bool inCUCache);

    // Load data from file into outbuf
    //
    void Load(_in_ uint64 offset, _in_ int size, __inout char* outbuf, bool direct_flag);

    int WSLoad(_in_ uint64 offset, _in_ int size, __inout char* outbuf, bool direct_flag);

    void GetFileName(_out_ char* fileName, _in_ const size_t capacity, _in_ const int fileId) const;
    bool IsDataFileExist(int fileId) const;

    void GetBcmFileName(_out_ char* bcmfile, _in_ int fileId) const;
    bool IsBcmFileExist(_in_ int fileId) const;

    const char* GetColumnFileName() const;

    uint64 AllocSpace(_in_ int size);

    void FlushDataFile() const;

    void SetAllocateStrategy(CStoreAllocateStrategy strategy)
    {
        m_strategy = strategy;
    };

    void SetFreeSpace(CStoreFreeSpace* fspace)
    {
        Assert(fspace != NULL);
        m_freespace = fspace;
    };

    FORCE_INLINE CStoreFreeSpace* GetFreeSpace()
    {
        return m_freespace;
    };

    void CreateStorage(int fileId, bool isRedo) const;

    File GetCUFileFd(uint64 offset);
    uint64 GetCUOffsetInFile(uint64 offset) const;
    bool IsCUStoreInOneFile(uint64 offset, int size) const;
    uint64 GetAlignCUOffset(uint64 offset) const;
    int GetAlignCUSize(int size) const;
    void FastExtendFile(uint64 extend_offset, uint32 size, bool keep_size);
    void TruncateDataFile();
    void TruncateBcmFile();
    void Set2ByteAlign(bool is_2byte_align);
    bool Is2ByteAlign();

private:
    void InitFileNamePrefix(_in_ const CFileNode& cFileNode);
    File CreateFile(_in_ char* file_name, _in_ int fileId, bool isRedo) const;
    File OpenFile(_in_ char* file_name, _in_ int fileId, bool direct_flag);
    File WSOpenFile(_in_ char* file_name, _in_ int fileId, bool direct_flag);
    void InitCstoreFreeSpace(CStoreAllocateStrategy strategy);
    void CloseFile(_in_ File fd) const;

public:
    CFileNode m_cnode;

private:
    // The common prefix of column file name
    char m_fileNamePrefix[MAXPGPATH];

    // The column file name
    char m_fileName[MAXPGPATH];

    // free space alloctor.
    CStoreFreeSpace* m_freespace;

    // Currently read/write fd
    File m_fd;

    // allocate strategy: append; reuse
    CStoreAllocateStrategy m_strategy;

    bool append_only;

    bool is_2byte_align;
};

// Notice: the following functions are in physical/low level.
//	  they don't know CU's concept, and just load data from file.
//
class CUFile : public BaseObject {
public:
    CUFile(const RelFileNode& fileNode, int col);
    virtual void Destroy();
    virtual ~CUFile();
    char* Read(uint64 offset, int wantSize, int* realSize, int align_size);

private:
    CUStorage* m_colStorage;

// private buffer
//
#define CUFILE_MAX_BUF_SIZE (512 * 1024)
    char* m_buffer;
    int m_maxpos;

    // we have to remember the total size and current offset about the physical file.
    // that's because neither CU nor CU desc exists, and we cannot know the valid
    // offset and size to read.
    //
    int m_fileId;
    uint32 m_maxFileSize;
    uint32 m_curFileOffset;
};

extern uint64 GetColDataFileSize(Relation rel, int attid);

extern const uint64 MAX_FILE_SIZE;
// get file id and offset in this file
#define CU_FILE_ID(__pos) ((__pos) / MAX_FILE_SIZE)
#define CU_FILE_OFFSET(__pos) ((__pos) % MAX_FILE_SIZE)

#define relcolpath(custorage_ptr) ((custorage_ptr)->GetColumnFileName())

#endif
