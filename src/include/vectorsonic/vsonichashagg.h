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
 * vsonichashagg.h
 *     sonic hash agg class and class member declare
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonichashagg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VSONICHASHAGG
#define VSONICHASHAGG

#include "nodes/plannodes.h"
#include "vecexecutor/vecagg.h"
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonicpartition.h"

class SonicHashAgg : public SonicHash {
public:
    SonicHashAgg(VecAggState* node, int arrSize);
    ~SonicHashAgg(){};

    /* Main sonic HashAgg function */
    VectorBatch* Run();

    /* Main process to build hash table */
    void Build();

    /* Main process to produce result */
    VectorBatch* Probe();

    /* Get the hash source */
    SonicHashSource* GetHashSource();

    /* Reset current status */
    bool ResetNecessary(VecAggState* node);

    /* Show number of partition files */
    int getFileNum()
    {
        if (m_partFileSource != NULL)
            return m_partNum * m_buildOp.cols;
        else
            return 0;
    }

private:
    /*
     * Fowllowing functions are called for preparing all information needed in sonic hashagg node.
     */
    void initAggInfo();

    void initMemoryControl();

    void initBatch();

    void initDataArray();

    void initHashTable();

    void initMatchFunc(TupleDesc desc, uint16* keyIdx, uint16 keyNum);

    void BindingFp();

    /*
     * Build hash table and process match procedure.
     */
    /* match function. */
    template <bool simpleType>
    bool matchValue(ScalarVector* pVector, uint16 keyIdx, int16 pVectorIdx, uint32 cmpIdx);

    template <bool simpleType>
    void matchArray(ScalarVector* pVector, uint16 keyIdx, uint16 cmpRows);

    /* build sonic hash table function */
    template <bool useSegHashTable, bool unique_check>
    void buildAggTblBatch(VectorBatch* batch);

    int64 insertHashTbl(VectorBatch* batch, int idx, uint32 hashval, uint32 hashLoc);

    void calcHashContextSize(MemoryContext ctx, int64* memorySize, int64* freeSize);

    /* judge current used memory context */
    void judgeMemoryOverflow(char* opname, int planId, int dop, Instrumentation* instrument, int64 size_needed);

    bool judgeMemoryAllowExpand();

    /* calculate hash table size */
    template <bool expand, bool logit>
    int64 calcHashTableSize(int64 oldSize);

    /* Following function are used for alloc new hash table or expand it. */
    void AllocHashTbl(VectorBatch* batch, int idx, uint32 hashval, int hashLoc);

    void tryExpandHashTable();

    void expandHashTable();

    /* following functions are about partiton function */
    int64 calcLeftRows(int64 rows_in_mem);

    uint16 calcPartitionNum(long numGroups);

    void resetVariableMemberIfNecessary(int partNum);

    void initPartition(SonicHashPartition** partSource);

    SonicHashPartition** createPartition(uint16 num_partitions);

    void releaseAllFileHandlerBuffer();

    /*
     * Following functions are used for calculating agg function result and build the final result.
     */
    void AggregationOnScalar(VecAggInfo* aggInfo, ScalarVector* pVector, int idx);

    void BatchAggregation(VectorBatch* batch);

    void Profile(char* stats, bool* can_wlm_warning_statistics);

    void BuildScanBatchSimple(int idx);

    void BuildScanBatchFinal(int idx);

    VectorBatch* ProducerBatch();

    void (SonicHashAgg::*m_buildScanBatch)(int idx);

    typedef void (SonicHashAgg::*pKeyMatchArrayFunc)(ScalarVector* pVector, uint16 keyIdx, uint16 cmpRows);
    typedef bool (SonicHashAgg::*pKeyMatchValueFunc)(
        ScalarVector* pVector, uint16 keyIdx, int16 pVectorIdx, uint32 cmpIdx);

private:
    /* runtime state */
    VecAggState* m_runtime;

    /* runtime stage (source, build, probe) */
    int m_runState;

    /* memory or disk. */
    int m_strategy;

    /* record avg width of encoded columns in memory */
    int64 m_tupleCount;
    int64 m_colWidth;
    int m_arrayElementSize;

    /* record memory need for each expand */
    int64 m_arrayExpandSize;

    /* row numbers in memory of first spill */
    int m_fill_table_rows;

    /* expression-evaluation context */
    ExprContext* m_econtext;

    /* Sonic hash source */
    SonicHashSource* m_sonicHashSource;

    /* partitions for hash source(file source) */
    SonicHashPartition** m_partFileSource;

    /* partitions for respill hash source */
    SonicHashPartition** m_overflowFileSource;

    /* number of partitions */
    uint16 m_partNum;

    /* number of overflow partitions */
    uint16 m_overflowNum;

    /* current partition index */
    int m_currPartIdx;

    /*
     * Create a list of the tuple columns that actually need to be stored in
     * hashtable entries.  The incoming tuples from the outer plan node will
     * contain grouping columns, other columns referenced in targetlist and
     * qual, columns used to compute the aggregate functions, and perhaps just
     * junk columns we don't use at all.  Only columns of the first two types
     * need to be stored in the hashtable, and getting rid of the others can
     * make the table entries significantly smaller.
     */

    /* the actual structure for the table is bucket, hash, next, hash_needed cols, agg cols
     * number of columns needed in hash table
     */
    uint16 m_hashNeed;
    uint16* m_hashInBatchIdx;
    uint16* m_keyIdxInSonic;

    /* the information to describe the aggregation */
    /* number of aggregation function */
    uint16 m_aggNum;

    /* number of aggregation function need to transform */
    uint16 m_finalAggNum;

    /* check if current agg function is count(*)/count(any) or not. */
    bool* m_aggCount;

    /* agg index in the tupe, as some agg function may split into two function */
    uint16* m_aggIdx;

    /* whether the key is simple or not */
    bool m_keySimple;

    /* the index which need to do some final calculation */
    finalAggInfo* m_finalAggInfo;

    /* equal function used to match values. */
    FmgrInfo* m_equalFuncs;

    /* batch infomation used during aggregation */
    /* batch for scaning the hash table */
    VectorBatch* m_scanBatch;

    /* projection batch. */
    VectorBatch* m_proBatch;

    /* same structure as outer node return batch */
    VectorBatch* m_outBatch;

    /* batch used to store data from file */
    VectorBatch* m_sourceBatch;

    /* flag used to mark */
    bool m_useSegHashTbl;

    /* number of segment hash table */
    int m_segNum;

    /* segment index hash table */
    SonicDatumArray* m_segBucket;

    /* where we put the next data */
    SonicDatumArray* m_next;

    /* mark weather hash table can grow up */
    bool m_enableExpansion;

    /* build function pointer */
    void (SonicHashAgg::*m_buildFun)(VectorBatch* batch);

    /* hash build time */
    double m_hashbuild_time;

    /* hash agg-calc time */
    double m_calcagg_time;

    /* match function array. */
    pKeyMatchArrayFunc* m_arrayKeyMatch;
    pKeyMatchValueFunc* m_valueKeyMatch;

    /* record the miss match idx. */
    uint16 m_missIdx[BatchMaxSize];
    uint16 m_missNum;

    /* record the suspect idx which need to be match. */
    uint16 m_suspectIdx[BatchMaxSize];
    uint16 m_suspectNum;

    /* record the bucket location. */
    uint32 m_bucketLoc[BatchMaxSize];

    /* handle duplicate, record the orginial the location. */
    uint32 m_orgLoc[BatchMaxSize];
};
#endif
