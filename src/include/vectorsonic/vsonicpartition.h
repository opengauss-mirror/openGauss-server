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
 * vsonicpartition.h
 *     sonic partition class and class member declare
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicpartition.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICPARTITION_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICPARTITION_H_

#include "postgres.h"
#include "knl/knl_variable.h"
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonicarray.h"
#include "vectorsonic/vsonicfilesource.h"

/* int typeSize has 4 values:1,2,4,8. */
#define int_typeSize_1Byte 1
#define int_typeSize_2Byte 2
#define int_typeSize_4Byte 4
#define int_typeSize_8Byte 8

typedef enum {
    partitionStatusInitial = 1,
    partitionStatusMemory,
    partitionStatusFile,
    partitionStatusFinish
} PartitionStatus;

class SonicHashPartition : public SonicHashSource {

public:
    /* memory context partition holds */
    MemoryContext m_context;

    /* partition status */
    PartitionStatus m_status;

    /* number of columns */
    uint16 m_cols;

    /* number of rows partition holds */
    int64 m_rows;

    /* number of records in each files */
    uint64* m_fileRecords;

    /* total byte size of data */
    size_t m_size;

public:
    SonicHashPartition(const char* cxtname, uint16 cols, int64 workMem);
    ~SonicHashPartition(){};

    virtual void freeResources()
    {
        Assert(false);
    }

    virtual void init(uint16 colIdx, DatumDesc* desc)
    {
        Assert(false);
    }

    virtual void putHash(uint32* hashValues, uint64 nrows = 1)
    {
        Assert(false);
    }

    template <bool optspill>
    void putVal(ScalarValue* val, uint8* flag, uint16 colIdx)
    {
        Assert(false);
    }

    virtual bool isValid()
    {
        Assert(false);
        return false;
    }
};

class SonicHashMemPartition : public SonicHashPartition {

public:
    /* where to put data */
    SonicDatumArray** m_data;

    /* For complicate join key */
    SonicDatumArray* m_hash;

    uint8 m_bucketTypeSize;

    uint32 m_hashSize;

    uint32 m_mask;

    bool m_segHashTable;

    /* non-segment hashtable */
    char* m_bucket;

    char* m_next;

    /* Segment hashtable */
    SonicDatumArray* m_segBucket;

    SonicDatumArray* m_segNext;

public:
    SonicHashMemPartition(const char* cxtname, bool hasHash, TupleDesc tupleDesc, int64 workMem);
    ~SonicHashMemPartition(){};

    void init(uint16 colIdx, DatumDesc* desc);

    void freeResources();

    void putBatch(VectorBatch* batch);

    void flushPartition(int arrIdx, uint32* partIdx, SonicHashPartition** filePartitions, int nrows);

    void loadPartition(void* filePartition);

    void loadHash(void* partition);

    void putHash(uint32* hashValues, uint64 nrows);

    inline bool isValid();

private:
    /*
     * @Description: put ScalarValue column into SonicDatumArray per column
     * 	actually, in memory, putVal has not been used.
     * @in val - data to put
     * @in flag - flag to put
     * @in colIdx - column index this data belongs to
     */
    template <bool optspill>
    void putVal(ScalarValue* val, uint8* flag, uint16 colIdx)
    {
        m_data[colIdx]->putArray(val, flag, 1);
    }
};

class SonicHashFilePartition : public SonicHashPartition {

public:
    /* number of files */
    uint16 m_fileNum;

    /* pointers of files */
    SonicHashFileSource** m_files;

    /*
     * record the size of varible data which will not contained within the Atom's allocated space
     * when loading into memory, such as SONIC_NUMERIC_COMPRESS_TYPE and SONIC_VAR_TYPE,
     * whose data may stored in buffers (eg. VarBuf) other than Atom.
     * The Atom only holds the pointers of the data.
     */
    size_t m_varSize;

    /* assembled batch */
    VectorBatch* m_batch;

    int* m_filetype;

public:
    SonicHashFilePartition(const char* cxtname, bool hasHash, TupleDesc tupleDesc, int64 workMem);

    SonicHashFilePartition(const char* cxtname, VectorBatch* out_batch, int64 workMem);

    ~SonicHashFilePartition(){};

    void init(uint16 fileIdx, DatumDesc* desc);

    void prepareFileHandlerBuffer();

    void releaseFileHandlerBuffer();

    void freeResources();

    template <bool optspill>
    void putVal(ScalarValue* val, uint8* flag, uint16 colIdx)
    {
        if (optspill) {
            int fileidx = m_filetype[colIdx];
            size_t written = m_files[fileidx]->writeVal(val, *flag);
            /* fix row number */
            m_fileRecords[fileidx]++;
            /* record written size */
            m_size += written;
            pgstat_increase_session_spill_size(written);
        } else {
            size_t written = m_files[colIdx]->writeVal(val, *flag);
            /* fix row number */
            m_fileRecords[colIdx]++;
            /* record written size */
            m_size += written;
            pgstat_increase_session_spill_size(written);
        }
    }

    void putHash(uint32* hashValues, uint64 nrows = 1);

    void getHash(uint32* hashValues, uint64 nrows);

    VectorBatch* getBatch();

    void closeFiles();

    void rewindFiles();

    inline bool isValid();
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICPARTITION_H_ */
