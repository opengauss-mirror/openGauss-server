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
 * cstore_insert.h
 *        routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_INSERT_H
#define CSTORE_INSERT_H

#include "access/cstore_am.h"
#include "access/cstore_delta.h"
#include "access/cstore_psort.h"
#include "access/cstore_vector.h"
#include "access/cstore_minmax_func.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/spin.h"

struct InsertArg {
    /* map to CStoreInsert::m_tmpBatchRows.
     *
     * because it's a temp batchrows, so
     * 1. it's used by each batch inserter when PCK exists, or
     * 2. it's shared by each partition when inserting into partitioned table,
     */
    bulkload_rows *tmpBatchRows;

    /* map to CStoreInsert::m_idxBatchRow
     *
     * because it's a temp batchrows, so
     * 1. it's shared by each index inserter when inserting into ordinary table, or
     * 2. it's shared by each index inserter of each partition when inserting into
     *    partitioned table.
     */
    bulkload_rows *idxBatchRow;

    ResultRelInfo *es_result_relations;

    /* sort type: tuple sorting or batch sorting */
    int sortType;

    /* which function will be called,
     * 1. false => void CStoreInsert::BatchInsert(bulkload_rows *batchRowPtr, int options)
     * 2. true  => void CStoreInsert::BatchInsert(VectorBatch *pBatch,        int options)
     * this value must be defined explicitly.
     */
    bool using_vectorbatch;

    InsertArg()
    {
        tmpBatchRows = NULL;
        idxBatchRow = NULL;
        es_result_relations = NULL;
        sortType = TUPLE_SORT;    /* default psort type */
        using_vectorbatch = true; /* caller should specify the right way */
    }
};

/*
 * This class provides some API for batch insert in ColStore.
 * In future, we can add more API.
 */
class CStoreInsert : public BaseObject {
public:
    CStoreInsert(_in_ Relation relation, _in_ const InsertArg &args, _in_ bool is_update_cu, _in_ Plan *plan,
                 _in_ MemInfoArg *ArgmemInfo);

    virtual ~CStoreInsert();
    virtual void Destroy();

    void BeginBatchInsert(const InsertArg &args);
    void EndBatchInsert();

    /*
     * Batch insert interface
     */
    void BatchInsert(bulkload_rows *batchRowPtr, int options);
    void BatchInsert(VectorBatch *pVec, int options);
    void BatchInsertCommon(bulkload_rows *batchRowPtr, int options);
    void FlashData(int options);

    void CUInsert(_in_ BatchCUData *CUData, _in_ int options);

    void SetEndFlag();

    inline bool IsEnd()
    {
        return m_insert_end_flag;
    }

    static void InitInsertArg(Relation rel, ResultRelInfo *resultRelInfo, bool using_vectorbatch, InsertArg &args);
    static void DeInitInsertArg(InsertArg &args);
    static void InitIndexInsertArg(Relation heap_rel, const int *keys_map, int nkeys, InsertArg &args);
    void InitInsertMemArg(Plan *plan, MemInfoArg *ArgmemInfo);

    inline CStorePSort* GetSorter()
    {
        return m_sorter;
    }

    inline MemoryContext GetTmpMemCnxt()
    {
        return m_tmpMemCnxt;
    }

    inline bulkload_rows* GetBufferedBatchRows()
    {
        return m_bufferedBatchRows;
    }

    void SortAndInsert(int options);

    Relation m_relation;
    CU ***m_aio_cu_PPtr;
    AioDispatchCUDesc_t ***m_aio_dispath_cudesc;
    int *m_aio_dispath_idx;

    /* memory info for memory adjustment */
    MemInfoArg *m_cstorInsertMem;

private:
    void FreeMemAllocateByAdio();
    // Whether need partial clustering when load
    // 
    inline bool NeedPartialSort(void) const;

    // Write CU data and CUDesc
    // 
    void SaveAll(int options, const char *delBitmap = NULL);

    void CUListFlushAll(int attno);
    void CUListWriteCompeleteIO(int col, int count);

    void CUWrite(int attno, int col);
    void CUListWrite();

    // Compress batch rows into CU
    // Get min/max of CU
    // 
    CU *FormCU(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr);
    Size FormCUTInitMem(CU *cuPtr, bulkload_rows *batchRowPtr, int col, bool hasNull);
    void FormCUTCopyMem(CU *cuPtr, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, Size dtSize, int col, bool hasNull);
    template <bool hasNull>
    void FormCUT(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr);
    template <bool hasNull>
    void FormCUTNumeric(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr);
    template <bool hasNull>
    void FormCUTNumString(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr);

    template <bool bpcharType, bool hasNull, bool has_MinMax_func>
    bool FormNumberStringCU(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr);

    template <bool hasNull>
    bool TryFormNumberStringCU(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr, uint32 atttypid);

    void InitIndexColId(int which_index);

    void InitIndexInfo();

    void InitDeltaInfo();

    // Insert delta table
    void InsertDeltaTable(bulkload_rows *batchRowPtr, int options);

    // Insert index table
    // 
    void InsertIdxTableIfNeed(bulkload_rows *batchRowPtr, uint32 cuId);

    void InsertNotPsortIdx(int indice);

    void FlushIndexDataIfNeed();

    void InitFuncPtr();

    void InitColSpaceAlloc();

    bool TryEncodeNumeric(int col, bulkload_rows *batchRowPtr, CUDesc *cuDescPtr, CU *cuPtr, bool hasNull);
    void DoBatchInsert(int options);
    typedef void (CStoreInsert::*m_formCUFunc)(int, bulkload_rows *, CUDesc *, CU *);
    struct FormCUFuncArray {
        m_formCUFunc colFormCU[2];
    };
    inline void SetFormCUFuncArray(Form_pg_attribute attr, int col);

    bool m_isUpdate;
    /* Function Pointer Array Area */
    FuncSetMinMax *m_setMinMaxFuncs;    /* min/max value function */
    FormCUFuncArray *m_formCUFuncArray; /* Form CU function pointer */

    /* If relation has cluster key, it will work for partial sort */
    CStorePSort *m_sorter;

    CUDesc **m_cuDescPPtr;                 /* The cudesc of all columns of m_relation */
    CU **m_cuPPtr;                         /* The CU of all columns of m_relation; */
    CUStorage **m_cuStorage;               /* CU storage */
    compression_options *m_cuCmprsOptions; /* compression filter */
    cu_tmp_compress_info m_cuTempInfo;     /* temp info for CU compression */

    /* buffered batchrows for many VectorBatch values */
    bulkload_rows *m_bufferedBatchRows;

    /* temp batchrows for fetching values from sort processor */
    bulkload_rows *m_tmpBatchRows;

    /* memory context for avoiding memory leaks during bulk-insert */
    MemoryContext m_tmpMemCnxt;
    MemoryContext m_batchInsertCnxt;

    /* work for delta insert */
    Relation m_delta_relation;
    TupleDesc m_delta_desc;
    bool m_append_only; /* don't insert into delta when true */

    /* work for Index insert */
    ResultRelInfo *m_resultRelInfo; /* contain index meta info */

    Relation *m_idxRelation;        /* index relations */
    Relation *m_deltaIdxRelation;   /* delta index relations */
    InsertArg *m_idxInsertArgs;     /* index inserting arguments */
    CStoreInsert **m_idxInsert;     /* index inserter */
    int **m_idxKeyAttr;             /* index keys */
    int *m_idxKeyNum;               /* index keys' number */
    bulkload_rows *m_idxBatchRow;   /* shallow copy for index insert */
    EState *m_estate;               /* estate information */
    ExprContext *m_econtext;        /* curr context for expr eval */
    Datum *m_fake_values;           /* value array to form fake heap tuple */
    bool *m_fake_isnull;            /* null array to from fake heap tuple */

    /* ADIO info */
    MemoryContext m_aio_memcnxt;
    int32 *m_aio_cache_write_threshold;
    File **m_vfdList;

    /* the max number of values within one CU */
    int m_fullCUSize;

    /* the number delta threshold */
    int m_delta_rows_threshold;

    /* compression options. */
    int16 m_compress_modes;

    /* indicate the end of insert or not */
    bool m_insert_end_flag;
};

enum PartitionCacheStrategy {
    CACHE_EACH_PARTITION_AS_POSSIBLE = 0,  // cache every partition as much as possible,  default strategy
    FLASH_WHEN_SWICH_PARTITION             // flash cached data when switch partition
};

class PartitionValueCache : public BaseObject {
public:
    PartitionValueCache(Relation rel);

    virtual ~PartitionValueCache();
    virtual void Destroy();

    // Serialize row value to partition value cache
    Size WriteRow(Datum *values, const bool *nulls);

    // Deserialize values from partition value cache
    int ReadBatchRow(bulkload_rows *batch, Datum *values, bool *nulls);

    void Reset();

    void EndWrite()
    {
        FlushData();
    }

    int m_rows;

private:
    int ReadRow(_out_ Datum *values, _out_ bool *nulls);

    int InternalRead(char *buf, int len);

    int InternalWrite(const char *buf, int len);

    FORCE_INLINE int InternalWriteInt(int data)
    {
        return InternalWrite((char *)&data, sizeof(data));
    }

    int FillBuffer();

    void FlushData();

    Relation m_rel;
    char *m_buffer;
    uint64 m_writeOffset;
    File m_fd;
    int m_bufCursor;
    int m_dataLen;
    uint64 m_readOffset;

    const static int MAX_BUFFER_SIZE = 32768;
};

// Work for loading partition table
// Do real batch insert using CStoreInsert objection for each partition
// Note that we should control memory size for each partition
// 
class CStorePartitionInsert : public CStore {
public:
    CStorePartitionInsert(_in_ Relation relation, _in_ ResultRelInfo *es_result_relations, _in_ int type,
                          _in_ bool is_update_cu, _in_ Plan *plan, _in_ MemInfoArg *ArgmemInfo);
    virtual ~CStorePartitionInsert();
    virtual void Destroy();
    void BatchInsert(_in_ Datum *values, _in_ const bool *nulls, _in_ int options);
    void BatchInsert(VectorBatch *batch, int hi_options);

    void EndBatchInsert();
    void SetEndFlag()
    {
    }

    void SetPartitionCacheStrategy(int partition_cache_strategy);
    bool hasEnoughMem(int partitionidx, int64 memsize_used);
    int findBiggestPartition() const;
    void InitInsertPartMemArg(Plan *plan, MemInfoArg *ArgmemInfo);

    /* record memory auto spread info. bulkload memory */
    MemInfoArg *m_memInfo;

private:
    void InitValueCache();

    void DeInitValueCache();

    bool CacheValues(Datum *values, const bool *nulls, int partitionidx);

    void SaveCacheValues(int partitionidx, bool doFlush, int options);

    void MoveBatchRowToPartitionValueCache(int partitionidx);

    // Try to get a batchrow from free list
    // 
    bulkload_rows *GetBatchRow(int partitionIdx);

    // Return the batchrow held by the partition to the free list
    // 
    void ReleaseBatchRow(int partitionIdx);

    /*
     * @Description: is insert value switch partition
     * @IN partitionIdx: current partition number
     * @Return: true switch partiton, false not switch
     */
    inline bool IsSwitchPartition(int partitionIdx)
    {
        return (m_last_insert_partition != -1) && (m_last_insert_partition != partitionIdx);
    };

    inline Relation GetPartFakeRelation(int partitionIdx)
    {
        AutoContextSwitch contextSwitcher(m_cstorePartMemContext);

        Relation partitionRelation = m_storePartFakeRelation[partitionIdx];
        if (NULL == partitionRelation) {
            Assert(m_relation->partMap->type == PART_TYPE_RANGE);

            PartitionIdentifier partitionIdentifier;
            partitionIdentifier.partSeq = partitionIdx;
            partitionIdentifier.partArea = PART_AREA_RANGE;
            Oid partoid = partIDGetPartOid(m_relation, &partitionIdentifier);
            Partition partition = partitionOpen(m_relation, partoid, RowExclusiveLock);
            partitionRelation = partitionGetRelation(m_relation, partition);
            m_storePartFakeRelation[partitionIdx] = partitionRelation;
            partitionClose(m_relation, partition, NoLock);
        }

        return partitionRelation;
    };

    // CStoreInsert object for each partition
    // 
    CStoreInsert **m_insert;

    Relation *m_storePartFakeRelation;
    int m_partitionNum;
    InsertArg m_insertArgs;

    bool m_isUpdate;
    /* disk cache objects for each partition */
    PartitionValueCache **m_partValCache;
    /* flag to use disk cache */
    bool *m_partUseCahce;
    List *m_batchFreeList;

    /* batchrows for each partition */
    bulkload_rows **m_partRelBatchRows;
    /* temp batchrows for disk cache */
    bulkload_rows *m_partTmpBatch;

    /* temp memory context to avoid space leaks during disk cache inserting */
    MemoryContext m_tmpMemCnxt;
    /* memory context for all objects alive during partitioned table inserting */
    MemoryContext m_cstorePartMemContext;

    /* partition cache strategy */
    int m_cache_strategy;

    /* last insert partition number */
    int m_last_insert_partition;

    /* all partition relation keep the same value to the one of their parent
     * relation. so we can share this var between all the partition relations.
     */
    int m_fullCUSize;
    Datum *m_val;
    bool *m_null;
    Datum *m_tmp_val;
    bool *m_tmp_null;

    Size *m_diskFileSize;
};



#endif
