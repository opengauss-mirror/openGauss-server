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
 * vechashtable.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vechashtable.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECHASHTABLE_H_
#define VECHASHTABLE_H_

#include "vecexecutor/vectorbatch.h"
#include "nodes/execnodes.h"
#include "access/hash.h"
#include "storage/buf/buffile.h"
#include "utils/memutils.h"
#include "utils/batchsort.h"
#include "utils/memprot.h"

#ifdef USE_ASSERT_CHECKING
#define HASH_BASED_DEBUG(A) A /* for debug only */
#else
#define HASH_BASED_DEBUG(A)
#endif

/* hash key type */
typedef uint32 HashKey;
class vechashtable;

// row base hash value
struct hashVal {
    ScalarValue val;
    uint8 flag;
};

// we do not store the column number and row number here
// as it is duplicate information, we can remember it outside the struct.
struct hashCell {
    union {
        hashCell* m_next;
        int m_rows;
    } flag;

    hashVal m_val[FLEXIBLE_ARRAY_MEMBER];  // data begins here
};

typedef struct HashSegTbl {
    int tbl_size;         // hash table size
    hashCell** tbl_data;  // hash table data.
} HashSegTbl;

#define GET_NTH_CELL(cellHead, i) (hashCell*)((char*)cellHead + i * m_cellSize)
#define CELL_NTH_VAL(cell, i) (cell)->m_val[i].val
#define CELL_NTH_FLAG(cell, i) (cell)->m_val[i].flag
#define FILL_FACTOR 1.2
#define MHASH_FUN 0   // for in memory hash function
#define DHASH_FUN 1   // decide which partition the value will store in temp file
#define INNER_SIDE 0  // for use inner hash function
#define OUTER_SIDE 1  // for use outer hash function
#define MIN_HASH_TABLE_SIZE 4096
#define MAX_BUCKET_NUM (MaxAllocSize / sizeof(hashCell*) - 1)
#define MAX_LOG_LEN 1024

#define HASH_PREFETCH_DISTANCE 10

#define HASH_VARBUFSIZE 2 * 1024 * 1024  // 2M for buf
#define SCALAR_FUN 0
#define VAR_FUN 1

#define HASH_IN_MEMORY 0
#define HASH_IN_DISK 1
#define HASH_RESPILL 2

#define HASH_EXPAND_THRESHOLD 2 /* threshold for hashtable expanding */
#define HASH_EXPAND_SIZE 2

class hashSource : public BaseObject {
public:
    virtual VectorBatch* getBatch()
    {
        Assert(false);
        return NULL;
    }

    virtual hashCell* getCell()
    {
        Assert(false);
        return NULL;
    }

    virtual void close(int idx)
    {
        Assert(false);
    }

    virtual TupleTableSlot* getTup()
    {
        Assert(false);
        return NULL;
    }

    virtual ~hashSource()
    {}
};

class hashOpSource : public hashSource {
public:
    hashOpSource(PlanState* op);
    ~hashOpSource(){};

    VectorBatch* getBatch();

    int64 getFileSize();

    TupleTableSlot* getTup();

    void close(int idx)
    {
        return;
    }

private:
    // data source operator;
    PlanState* m_op;
};

typedef ScalarValue (*stripValFun)(ScalarValue* val);

class hashMemSource : public hashSource {
public:
    hashMemSource(List* data);
    ~hashMemSource(){};

    hashCell* getCell();

private:
    List* m_list;

    ListCell* m_cell;
};

class hashFileSource : public hashSource {
public:
    hashFileSource(VectorBatch* batch, MemoryContext context, int cellSize, hashCell* cellArray, bool complicateJoin,
        int m_write_cols, int fileNum, TupleDesc tupleDescritor);

    hashFileSource(TupleTableSlot* hashslot, int fileNum);
    ~hashFileSource(){};

    TupleTableSlot* getTup();

    void writeTup(MinimalTupleData* Tup, int idx);

    void rewind(int idx);

    void close(int idx);

    void closeAll();

    void freeFileSource();

    void setCurrentIdx(int idx);

    int getCurrentIdx();

    int64 getCurrentIdxRownum(int64 rows_in_mem);

    bool next();

    void enlargeFileSource(int fileNum);

    void writeCell(hashCell* cell, HashKey key);
    void writeBatch(VectorBatch* batch, int idx, HashKey key);
    void writeBatchToFile(VectorBatch* batch, int idx, int fileIdx);

    void writeBatchWithHashval(VectorBatch* batch, int idx, HashKey key);
    void writeBatchWithHashval(VectorBatch* batch, int idx, HashKey key, int fileIdx);

    void resetFileSource();

    void initFileSource(int fileNum);

    VectorBatch* getBatch();

    int64 getFileSize();

    hashCell* getCell();

    void resetVariableMemberIfNecessary(int fileNum);

    /* release the buffer in file handler if necessary */
    void ReleaseFileHandlerBuffer(int fileIdx);
    void ReleaseAllFileHandlerBuffer();

    /* prepare the buffer in file handler if necessary */
    void PrepareFileHandlerBuffer(int fileIdx);

public:
    int m_cellSize;

    int64* m_rownum;

    int64 m_total_filesize;

    int64* m_spill_size;

    int64* m_fileSize;

    int m_cols;

    int m_write_cols;  // just for right join

    int* m_funType;

    VectorBatch* m_batch;

    hashCell* m_cellArray;  // a bunch of cell

    MemoryContext m_context;

    void** m_file;

    TupleTableSlot* m_hashTupleSlot;

    MinimalTuple m_tuple;

    uint32 m_tupleSize;

    int m_currentFileIdx;

    int m_fileNum;

    Datum* m_values;

    bool* m_isnull;

    uint32 m_varSpaceLen;

    stripValFun* m_stripFunArray;

    size_t (hashFileSource::*m_write[2])(ScalarValue val, uint8 flag, int idx);

    size_t (hashFileSource::*m_read[2])(ScalarValue* val, uint8* flag);

    size_t (hashFileSource::*m_writeTuple)(MinimalTupleData* Tup, int idx);

    TupleTableSlot* (hashFileSource::*m_getTuple)();

    hashCell* (hashFileSource::*m_getCell)();

    void (hashFileSource::*m_rewind)(int idx);
    void (hashFileSource::*m_close)(int idx);

    size_t (hashFileSource::*m_writeCell)(hashCell* cell, int idx);

    size_t (hashFileSource::*m_writeBatch)(VectorBatch* batch, int idx, int fileIdx);

    VectorBatch* (hashFileSource::*m_getBatch)();

    size_t (hashFileSource::*m_writeBatchWithHashval)(VectorBatch* batch, int idx, HashKey key, int fileIdx);

private:
    // write the value from the file.
    template <bool compress_spill>
    size_t writeScalar(ScalarValue val, uint8 flag, int fileIdx);
    template <bool compress_spill>
    size_t writeVar(ScalarValue val, uint8 flag, int fileIdx);

    // read the value from the file.
    template <bool compress_spill>
    size_t readScalar(ScalarValue* val, uint8* flag);
    template <bool compress_spill>
    size_t readVar(ScalarValue* val, uint8* flag);

    size_t writeBatchNoCompress(VectorBatch* batch, int idx, int fileIdx);

    size_t writeBatchCompress(VectorBatch* batch, int idx, int fileIdx);

    void assembleBatch(TupleTableSlot* slot, int idx);

    template <bool get_hashval>
    VectorBatch* getBatchCompress();

    template <bool get_hashval>
    VectorBatch* getBatchNoCompress();

    void rewindCompress(int idx);
    void rewindNoCompress(int idx);
    void closeCompress(int idx);
    void closeNoCompress(int idx);

    template <bool write_hashval>
    size_t writeCellNoCompress(hashCell* cell, int fileIdx);

    template <bool write_hashval>
    size_t writeCellCompress(hashCell* cell, int fileIdx);

    template <bool write_hashval>
    hashCell* getCellNoCompress();

    template <bool write_hashval>
    hashCell* getCellCompress();

    template <bool compress_spill>
    size_t writeTupCompress(MinimalTupleData* Tup, int idx);

    template <bool compress_spill>
    TupleTableSlot* getTupCompress();

    template <bool compress_spill>
    size_t writeBatchWithHashvalCompress(VectorBatch* batch, int idx, HashKey key, int fileIdx);
};

class hashSortSource : public hashSource {
public:
    hashSortSource(Batchsortstate* batchSortState, VectorBatch* sortBatch);
    ~hashSortSource(){};

    VectorBatch* getBatch();

    VectorBatch* m_SortBatch;

private:
    Batchsortstate* m_batchSortStateIn;
};

inline int getPower2LessNum(int num)
{
    int i = 1;
    int count = 0;

    for (;;) {
        num = num / 2;
        if (num == 0)
            break;
        count++;
    }
    i <<= count;

    return i;
}

inline int getPower2NextNum(int64 num)
{
    int i = 1;
    int count = 0;

    // guard too long number
    if (num > INT_MAX / 2)
        num = INT_MAX / 2;

    num = num - 1;
    for (;;) {
        num = num / 2;
        if (num == 0)
            break;
        count++;
    }

    i <<= (count + 1);
    return i;
}

/* we need split the hash table implementation and some hash-based
 * operator common structure. */
class hashBasedOperator : public BaseObject {
public:
    hashBasedOperator() : m_spillToDisk(false), m_rows(0), m_totalMem(0), m_availmems(0)
    {
        m_filesource = NULL;
        m_innerHashFuncs = NULL;
        m_key = 0;
        m_cols = 0;
        m_sysBusy = false;
        m_outerHashFuncs = NULL;
        m_overflowsource = NULL;
        m_hashContext = NULL;
        m_keyDesc = NULL;
        m_tupleCount = 0;
        m_hashTbl = NULL;
        m_colWidth = 0;
        m_cellSize = 0;
        m_tmpContext = NULL;
        m_colDesc = NULL;
        m_spreadNum = 0;
        m_okeyIdx = NULL;
        m_fill_table_rows = 0;
        m_keyIdx = NULL;
        m_keySimple = false;
        m_eqfunctions = NULL;
        m_strategy = 0;
        m_maxMem = 0;
    }

    virtual ~hashBasedOperator()
    {}

    // build a hash table
    virtual void Build() = 0;

    // probe the hash table.
    virtual VectorBatch* Probe() = 0;

    // create temprorary file.
    hashFileSource* CreateTempFile(VectorBatch* batch, int fileNum, PlanState* planstate);

    // close temprorary file.
    void closeFile();

    FORCE_INLINE
    int64 getRows()
    {
        return m_rows;
    }

    int getFileNum()
    {
        if (m_filesource != NULL)
            return m_filesource->m_fileNum;
        else
            return 0;
    }

    int calcFileNum(long numGroups);

    // replace some equal function.
    void ReplaceEqfunc();

    // judge memory over flow.
    void JudgeMemoryOverflow(char* opname, int planid, int dop, Instrumentation* instrument = NULL);

    /* judge memory allow table expnd */
    bool JudgeMemoryAllowExpand();

    // free memory context.
    void freeMemoryContext();

    // simple hash function without string type in the key
    template <bool reHash>
    void hashCellT(hashCell* cell, int keyIdx, FmgrInfo* hashFmgr, int nval, ScalarValue* hashRes);

    template <bool reHash>
    void hashColT(ScalarVector* val, FmgrInfo* hashFmgr, int nval, ScalarValue* hashRes);

    inline void hashBatch(
        VectorBatch* batch, int* keyIdx, ScalarValue* hashRes, FmgrInfo* hashFmgr, bool needSpill = false);

    inline void hashCellArray(
        hashCell* cell, int nrows, int* keyIdx, ScalarValue* hashRes, FmgrInfo* hashFmgr, bool needSpill = false);

public:
    vechashtable* m_hashTbl;

    // memory context for hash.
    MemoryContext m_hashContext;

    // memory context for temprorary calculation.
    MemoryContext m_tmpContext;

    /* memory context for hashtable and hashcell.
     * need not free and apply memory again, only do reset when rescan.
     */

    // cache location
    ScalarValue m_cacheLoc[BatchMaxSize];

    hashCell* m_cellCache[BatchMaxSize];

    // flag the key match
    bool m_keyMatch[BatchMaxSize];

    FmgrInfo* m_eqfunctions; /* equal function */

    FmgrInfo* m_outerHashFuncs; /* lookup data for hash functions */

    FmgrInfo* m_innerHashFuncs; /* lookup data for hash functions */

    // spilling file.
    hashFileSource* m_filesource;

    hashFileSource* m_overflowsource;

    // whether spill to disk.
    bool m_spillToDisk;

    // memory or disk.
    int m_strategy;

    // how many rows in hash table
    int64 m_rows;

    /*total memory, in bytes*/
    int64 m_totalMem;

    /*avail memory */
    int64 m_availmems;

    // how many cols in the hash table.
    int m_cols;

    // how many key in the hash table.
    int m_key;

    int* m_keyIdx;  /* list of varattno on hash keys. */
    int* m_okeyIdx; /* list of original value of varattno on hash keys. */

    ScalarDesc* m_keyDesc;  // indicate key desc

    ScalarDesc* m_colDesc;  // indicate col desc;

    // whether the key is simple. 1:is simple 2: not simple
    // simple means there is no variable length data type or complex expression evaluation.
    bool m_keySimple;

    int m_cellSize;

    /* row numbers in memory of first spill */
    int m_fill_table_rows;

    /* records avg width of encoded columns in memory */
    int64 m_tupleCount;
    int64 m_colWidth;

    /* record memory auto spread info */
    int64 m_maxMem;
    int m_spreadNum;

    /* If disk spill caused by system resource danger */
    bool m_sysBusy;
};

// chain
class vechashtable : public BaseObject {
public:
    vechashtable(int hashSize) : m_size(hashSize), m_data(0)
    {
        m_data = (hashCell**)palloc0(m_size * sizeof(hashCell*));
    }
    // depend the sub class to init the member variable.
    ~vechashtable()
    {
        if (m_data)
            pfree(m_data);

        m_data = NULL;
        m_size = 0;
    }

    void Reset()
    {
        Assert(m_size != 0);
        m_data = (hashCell**)palloc0(m_size * sizeof(hashCell*));
    }

    void Profile(char* stats, bool* can_wlm_warning_statistics);

public:
    int m_size;  // hash table size

    hashCell** m_data;  // hash table data.
};

#define GET_HASH_TABLE(node) (((vechashtable*)(node->hashTbl)))

// simple hash function without string type in the key
template <bool reHash>
inline void hashBasedOperator::hashCellT(hashCell* cell, int keyIdx, FmgrInfo* hashFmgr, int nval, ScalarValue* hashRes)
{
    hashVal val;
    ScalarValue hashV;
    FunctionCallInfoData fcinfo;
    Datum args[2];
    fcinfo.arg = &args[0];
    fcinfo.flinfo = hashFmgr;
    PGFunction func = hashFmgr->fn_addr;

    for (int j = 0; j < nval; j++) {
        val = cell->m_val[keyIdx];
        if (likely(NOT_NULL(val.flag))) {
            fcinfo.arg[0] = val.val;
            if (reHash) {
                /* rotate hashkey left 1 bit at each rehash step */
                hashV = hashRes[j];
                hashV = (hashV << 1) | ((hashV & 0x80000000) ? 1 : 0);
                hashV ^= func(&fcinfo);
                hashRes[j] = hashV;
            } else
                hashRes[j] = func(&fcinfo);
        } else {
            if (!reHash)
                hashRes[j] = 0;  // give the init value;
        }

        cell = (hashCell*)((char*)cell + m_cellSize);
    }
}

// simple hash function without string type in the key
template <bool reHash>
inline void hashBasedOperator::hashColT(ScalarVector* val, FmgrInfo* hashFmgr, int nval, ScalarValue* hashRes)
{
    ScalarValue* value = val->m_vals;
    uint8* flag = val->m_flag;
    FunctionCallInfoData fcinfo;
    Datum args[2];
    fcinfo.arg = &args[0];
    fcinfo.flinfo = hashFmgr;
    PGFunction func = hashFmgr->fn_addr;
    ScalarValue hashV;

    for (int j = 0; j < nval; j++) {
        if (likely(NOT_NULL(flag[j]))) {
            fcinfo.arg[0] = value[j];
            if (reHash) {
                /* rotate hashkey left 1 bit at each rehash step */
                hashV = hashRes[j];
                hashV = (hashV << 1) | ((hashV & 0x80000000) ? 1 : 0);
                hashV ^= func(&fcinfo);
                hashRes[j] = hashV;
            } else
                hashRes[j] = func(&fcinfo);
        } else {
            if (!reHash)
                hashRes[j] = 0;  // give the init value;
        }
    }
}

inline void hashBasedOperator::hashBatch(
    VectorBatch* batch, int* keyIdx, ScalarValue* hashRes, FmgrInfo* hashFmgr, bool needSpill)
{
    int i;
    int nrows = batch->m_rows;
    ScalarVector* pVector = batch->m_arr;
    AutoContextSwitch memGuard(m_tmpContext);

    hashColT<false>(&pVector[keyIdx[0]], hashFmgr, nrows, hashRes);

    for (i = 1; i < m_key; i++)
        hashColT<true>(&pVector[keyIdx[i]], (hashFmgr + i), nrows, hashRes);

    /* Rehash the hash value for avoiding the key and distribute key using the same hash function. */
    for (i = 0; i < nrows; i++) {
        if (needSpill)
            hashRes[i] = hash_new_uint32(DatumGetUInt32(hashRes[i]));
        else
            hashRes[i] = hash_uint32(DatumGetUInt32(hashRes[i]));
    }

    MemoryContextReset(m_tmpContext);
}

inline void hashBasedOperator::hashCellArray(
    hashCell* cell, int nrows, int* keyIdx, ScalarValue* hashRes, FmgrInfo* hashFmgr, bool needSpill)
{
    int i;
    AutoContextSwitch memGuard(m_tmpContext);

    hashCellT<false>(cell, keyIdx[0], hashFmgr, nrows, hashRes);

    for (i = 1; i < m_key; i++)
        hashCellT<true>(cell, keyIdx[i], (hashFmgr + i), nrows, hashRes);

    /* Rehash the hash value for avoiding the key and distribute key using the same hash function. */
    for (i = 0; i < nrows; i++) {
        if (needSpill)
            hashRes[i] = hash_new_uint32(DatumGetUInt32(hashRes[i]));
        else
            hashRes[i] = hash_uint32(DatumGetUInt32(hashRes[i]));
    }

    MemoryContextReset(m_tmpContext);
}

extern ScalarValue addVariable(MemoryContext context, ScalarValue val);
extern ScalarValue replaceVariable(MemoryContext context, ScalarValue oldVal, ScalarValue val);
extern ScalarValue addToVarBuffer(VarBuf* buf, ScalarValue value);
extern void* TempFileCreate();
extern ScalarValue DatumToScalarInContext(MemoryContext context, Datum datumVal, Oid datumType);

#endif /* VECHASHTABLE_H_ */
