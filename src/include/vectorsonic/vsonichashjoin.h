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
 * vsonichashjoin.h
 *     Routines to handle vector sonic hashjoin nodes.
 *     Sonic Hash Join nodes are based on the column-based hash table.
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonichashjoin.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICHASHJOIN_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICHASHJOIN_H_

#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonicpartition.h"

/*
 * Max partition number when doing partition or repartition.
 * Total partition number can be larger than this value.
 */
#define SONIC_PART_MAX_NUM 1024

typedef enum { reportTypeBuild = 1, reportTypeProbe, reportTypeRepartition } ReportType;

struct BatchPos {
    uint32 partIdx;
    int rowIdx;
};

class SonicHashJoin : public SonicHash {
public:
    SonicHashJoin(int size, VecHashJoinState* node);

    ~SonicHashJoin(){};

    void Build();

    VectorBatch* Probe();

    void closeAllFiles();

    void ResetNecessary();

    void freeMemoryContext();

private:
    /* init functions */
    void setHashIndex(uint16* keyIndx, uint16* oKeyIndx, List* hashKeys);

    void initMemoryControl();

    void initHashTable(uint8 byteSize, uint32 curPartIdx);

    void initHashFmgr();

    /* binding function pointer. */
    template <bool complicateJoinKey>
    void bindingFp();

    /* build side functions to put data */
    template <bool complicateJoinKey>
    void saveToMemory(VectorBatch* batch);

    template <bool complicateJoinKey, bool optspill>
    void saveToDisk(VectorBatch* batch);

    template <bool complicateJoinKey>
    void flushToDisk();

    template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
    void buildHashTable(uint32 curPartIdx);

    uint64 calcHashSize(int64 nrows);

    void prepareProbe();

    /* output functions */
    VectorBatch* buildRes(VectorBatch* inBatch, VectorBatch* outBatch);

    /* Memory functions */
    bool hasEnoughMem();

    void judgeMemoryOverflow(uint64 hash_head_size);

    void calcHashContextSize(MemoryContext ctx, uint64* allocateSize, uint64* freeSize);

    void calcDatumArrayExpandSize();

    /* probe functions */
    VectorBatch* probeMemory();

    VectorBatch* probeGrace();

    template <typename T, bool complicateJoinKey, bool isSegHashTable>
    VectorBatch* probeMemoryTable(SonicHashSource* probeP);

    template <typename T, bool complicateJoinKey, bool isSegHashTable>
    VectorBatch* probePartition(SonicHashSource* probeP = NULL);

    /* join functions */
    template <typename bucketType, bool complicateJoinKey, bool isSegHashTable, bool isPartStatus>
    VectorBatch* innerJoin(VectorBatch* batch);

    /* profiling functions */
    void profile(bool writeLog, uint32 partIdx, uint8 bucketTypeSize);

    template <typename BucketType, bool isSegHashTable>
    void profileFunc(char* stats, uint32 partIdx);

    /* Match Functions */
    void initMatchFunc(TupleDesc desc, uint16 keyNum);

    void DispatchKeyInnerFunction(int KeyIndx);

    template <typename innerType>
    void DispatchKeyOuterFunction(int KeyIndx);

    /* complicate join key */
    void CalcComplicateHashVal(VectorBatch* batch, List* hashKeys, bool inner);

    /* Match Functions for complicate join key */
    template <bool matchPartComplicateKey>
    void matchComplicateKey(VectorBatch* batch, SonicHashMemPartition* inPartition = NULL);

    /* partitions functions */
    uint32 calcPartitionNum();

    void calcPartIdx(uint32* hashVal, uint32 partNum, int nrows);

    void calcRePartIdx(uint32* hashVal, uint32 partNum, uint32 rbit, int nrows);

    bool preparePartition();

    void initProbePartitions();

    template <bool complicateJoinKey>
    void rePartition(uint32 rePartIdx);

    void recordPartitionInfo(bool buildside, int32 partIdx, uint32 istart, uint32 iend);

    void analyzePartition(bool buildside, uint32 startPartIdx, uint32 endPartIdx, int64* totalFileNum,
        long* totalFileSize, long* partSizeMin, long* partSizeMax, int* spillPartNum);

    void reportSorthashinfo(ReportType reportType, uint32 totalPartNum, int32 rePartIdx = -1);

    template <bool isInner>
    void initPartition(SonicHashPartition* partition);

    void loadInnerPartitions(uint64 memorySize);

    void loadInnerPartition(uint32 partIdx);

    void loadMultiInnerPartition(uint32 partIdx, SonicHashMemPartition* memPartition);

    void quickSort(uint32* elements, const int start, const int end);

    void sortPartitionSize(uint64 memorySize);

    template <bool complicateJoinKey, bool optspill>
    void saveProbePartition();

    /* bloom filter functions */
    void pushDownFilterIfNeed();

    /* clean functions */
    void finishJoinPartition(uint32 partIdx);

    void closePartFiles(uint32 partIdx);

    void releaseAllFileHandlerBuffer(bool isInner);

    void resetMemoryControl();
    uint64 get_hash_head_size(int64 rows);

public:
    double m_build_time;
    double m_probe_time;

private:
    VectorBatch* (SonicHashJoin::*m_probeFun[2])();

    typedef VectorBatch* (SonicHashJoin::*probeTypeFun)(SonicHashSource* probeP);

    /* save probe partition function */
    void (SonicHashJoin::*m_saveProbePartition)();

    /* the build function */
    void (SonicHashJoin::*m_funBuild[2])(VectorBatch* batch);

    /* static information */
    probeTypeFun m_probeTypeFun;

    bool m_complicatekey;

    /* describe outer operator */
    SonicHashInputOpAttr m_probeOp;

    /* describe whether hash key is integer. */
    bool* m_integertype;

    /* runtime state */
    VecHashJoinState* m_runtime;

    /* where we put data */
    char* m_next;

    /* join strategy */
    uint8 m_strategy;

    /* runtime attribute */
    VectorBatch* m_outRawBatch;

    /* record matched position. */
    uint32 m_innerMatchLoc[2 * BatchMaxSize];
    uint32 m_innerPartMatchLoc[2 * BatchMaxSize];
    uint16 m_outerMatchLoc[2 * BatchMaxSize];
    uint16 m_matchLocIndx;

    /* rocord partition indexes of data in atom or batch */
    uint32 m_partIdx[INIT_DATUM_ARRAY_SIZE];
    uint32 m_probeIdx;

    /* memory need for each expand */
    size_t m_arrayExpandSize;

    /* partitions for inner source */
    SonicHashPartition** m_innerPartitions;

    /* partitions for outer source */
    SonicHashPartition** m_outerPartitions;

    /*
     * Get build side data from lower operator.
     * Used for join without spilling.
     */
    SonicHashOpSource* m_hashOpPartition;

    /* status for probePartition() */
    uint8 m_probePartStatus;

    /*
     * number of partitions.
     * inner and outer should have the same number of partitions.
     */
    uint32 m_partNum;

    /* partition index ordered by size */
    uint32* m_partSizeOrderedIdx;

    /* the offset in m_partSizeOrderedIdx that have been loaded */
    int32 m_partLoadedOffset;

    /* hash level for repartition */
    uint8* m_pLevel;

    /* maximum partition level need to be paid attention */
    uint8 m_maxPLevel;

    /* partition is from a valid repartition process or not: for repartition process */
    bool* m_isValid;

    /*
     * the cjVector is only allocated and
     * used when m_complicateJoinKey is true
     */
    ScalarVector* m_cjVector;
    /* complicate inner batch */
    VectorBatch* m_complicate_innerBatch;
    /* complicate outer batch */
    VectorBatch* m_complicate_outerBatch;

    bool m_nulleqmatch[BatchMaxSize];

    /*
     * flag to show whether the build side partition is in memory.
     * SONIC_PART_MAX_NUM is enough, because this array is not
     * used after probePartition.
     */
    bool m_memPartFlag[SONIC_PART_MAX_NUM];

    /* record file partition info. */
    BatchPos m_diskPartIdx[BatchMaxSize];

    /* number of data in m_diskPartIdx[] */
    uint32 m_diskPartNum;
};

extern bool isSonicHashJoinEnable(HashJoin* hj);

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICHASHJOIN_H_ */