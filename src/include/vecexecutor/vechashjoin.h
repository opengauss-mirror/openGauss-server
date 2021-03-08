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
 * vechashjoin.h
 *     Prototypes for vectorized hash join
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vechashjoin.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECHASHJOIN_H_
#define VECHASHJOIN_H_

#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecnodes.h"
#include "workload/workload.h"
// hash join runtime state
#define HASH_BUILD 0
#define HASH_PROBE 1
#define HASH_END 2

typedef enum {
    HASH_JOIN_INNER = 0,
    HASH_JOIN_LEFT,
    HASH_JOIN_RIGHT,
    HASH_JOIN_SEMI,
    HASH_JOIN_ANTI,
    HASH_JOIN_RIGHT_SEMI,
    HASH_JOIN_RIGHT_ANTI,
    HASH_JOIN_LEFT_ANTI_FULL,
    HASH_JOIN_RIGHT_ANTI_FULL,
    HASH_JOIN_TYPE_NUM
} hashJoinType;

// hash strategy
#define MEMORY_HASH 0
#define GRACE_HASH 1

// probe status
#define PROBE_FETCH 0
#define PROBE_PARTITION_FILE 1
#define PROBE_DATA 2
#define PROBE_FINAL 3
#define PROBE_PREPARE_PAIR 4

extern VecHashJoinState* ExecInitVecHashJoin(VecHashJoin* node, EState* estate, int eflags);
extern VectorBatch* ExecVecHashJoin(VecHashJoinState* node);
extern void ExecEndVecHashJoin(VecHashJoinState* node);
extern void ExecReScanVecHashJoin(VecHashJoinState* node);
extern long ExecGetMemCostVecHash(VecHashJoin*);
extern void ExecEarlyFreeVecHashJoin(VecHashJoinState* node);

// Save current probing place for next join iteration
//
struct JoinStateLog {
    int lastBuildIdx;
    hashCell* lastCell;
    bool restore;
};

// Record hash key matching passed cells
//
struct ReCheckCellLoc {
    hashCell* cell;
    int resultIdx;
    int oriIdx;
};

// Vectorized hash join implementation class
//
class HashJoinTbl : public hashBasedOperator {
public:
    HashJoinTbl(VecHashJoinState* runtimeContext);

    void Build();

    VectorBatch* Probe();

    void ResetNecessary();

public:
    // number of columns in outer child
    //
    int m_outCols;

    // list of join keys in outer child
    //
    int* m_outKeyIdx;

    // list of original value of varattno on join keys in outer child
    int* m_outOKeyIdx;

    // list of OID of collation on join keys in outer child
    Oid* m_outKeyCollation;

    // a list for cache the data.
    List* m_cache;

    // the status flag to indicate the probe.
    int m_probeStatus;

    bool m_complicateJoinKey;

    // the cjVector is only alloced and used when m_complicateJoinKey is true
    //
    ScalarVector* m_cjVector;

    // out batch is simple
    bool m_outSimple;

    // inner batch is simple
    bool m_innerSimple;

    // whether check the key match
    bool m_doProbeData;

    // inner batch
    VectorBatch* m_innerBatch;

    // outer batch
    VectorBatch* m_outerBatch;

    // complicate inner batch
    VectorBatch* m_complicate_innerBatch;

    // complicate outer batch
    VectorBatch* m_complicate_outerBatch;

    // inner qual batch
    VectorBatch* m_inQualBatch;

    // outer qual batch
    VectorBatch* m_outQualBatch;

    // out raw batch
    VectorBatch* m_outRawBatch;

    VectorBatch* m_result;

    // runtime state
    VecHashJoinState* m_runtime;

    // hash Join type.
    hashJoinType m_joinType;

    // memory hash or grace hash
    int m_strategy;

    JoinStateLog m_joinStateLog;
    // prob source
    hashOpSource* m_probOpSource;  // prob source

    ReCheckCellLoc m_reCheckCell[BatchMaxSize];

    // flag the row match
    bool m_match[BatchMaxSize];

    int m_probeIdx;

    // for null-eq special case
    bool m_nulleqmatch[BatchMaxSize];

    // build file source
    hashFileSource* m_buildFileSource;

    // probe file source
    hashFileSource* m_probeFileSource;

    bool* m_simpletype;

    Oid* m_outerkeyType;

    /* partition level of each file source: for repartition process */
    uint8* m_pLevel;

    /* maximum partition level need to be paid attention */
    uint8 m_maxPLevel;

    /* partition is from a valid repartition process or not: for repartition process */
    bool* m_isValid;

    /* Point to cell in semiJoin,  in order to return value of righttree.*/
    hashCell** cellPoint;

    /* Print warning message after partitioned three times */
    bool m_isWarning;

    double m_build_time;
    double m_probe_time;

private:
    void SetJoinType();
    void PrepareProbe();

    template <bool complicateJoinKey, bool NeedCopy>
    void buildHashTable(hashSource* source, int64 rownum);

    template <bool complicateJoinKey>
    void bindingFp();

    // prepare for disk hash.
    void initFile(bool buildSide, VectorBatch* templateBatch, int fileNum);

    // probe the in memory hash table.
    VectorBatch* probeMemory();

    // probe the hash table in a grace way.
    VectorBatch* probeGrace();

    // probe in memory hash table
    VectorBatch* probeHashTable(hashSource* probSource);

    // probe the partition.
    template <bool complicateJoinKey>
    void probePartition();

    /* repartition file source of a specific file */
    template <bool complicateJoinKey, bool buildside>
    void RePartitionFileSource(hashFileSource* hashSource, int fileIdx);

    /* record partition info into log file */
    void recordPartitionInfo(bool buildside, int fileIdx, int istart, int iend);

    // prepare the partition join.
    void preparePartition();

    // init memory control parameter
    void initMemoryControl();

    // calc the spilling file.
    int calcSpillFile();

    // end hash join
    VectorBatch* endJoin();

    // build result batch.
    VectorBatch* buildResult(VectorBatch* inBatch, VectorBatch* outBatch, bool checkqual);

    bool* checkQual(VectorBatch* inBatch, VectorBatch* outBatch);

    // match key

    template <typename innerType, typename outerType, bool simpleType, bool nulleqnull>
    void matchKey(ScalarVector* key, int nrows, int hashValKeyIdx, int key_num);

    bool simpletype(Oid type);

    void matchComplicateKey(VectorBatch* batch);

    void DispatchKeyInnerFunction(int KeyIdx);

    template <typename innerType>
    void DispatchKeyOuterFunction(int KeyIdx);

    // different join function
    // full join
    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* innerJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* leftJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* leftJoinWithQualT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightJoinWithQualT(VectorBatch* batch);

    // semi join
    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* semiJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* semiJoinWithQualT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* antiJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* antiJoinWithQualT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightSemiJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightSemiJoinWithQualT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightAntiJoinT(VectorBatch* batch);

    template <bool complicateJoinKey, bool simpleKey>
    VectorBatch* rightAntiJoinWithQualT(VectorBatch* batch);

    void PushDownFilterIfNeed();

private:
    // build function array.
    void (HashJoinTbl::*m_funBuild[2])(VectorBatch* batch);  // the build function;

    void CalcComplicateHashVal(VectorBatch* batch, List* hashKeys, bool inner);

    bool HasEnoughMem(int nrows);

    template <bool complicateJoinKey, bool simple>
    void SaveToMemory(VectorBatch* batch);

    template <bool complicateJoinKey>
    void flushToDisk();

    template <bool complicateJoinKey, bool buildSide>
    void SaveToDisk(VectorBatch* batch);

    VectorBatch* (HashJoinTbl::*m_probeFun[2])();  // the probe function;

    VectorBatch* (HashJoinTbl::*m_joinFun)(VectorBatch* batch);  // join function

    VectorBatch* (HashJoinTbl::*m_joinFunArray[36])(VectorBatch* batch);

    typedef void (HashJoinTbl::*pMatchKeyFunc)(ScalarVector* key, int nrows, int hashValKeyIdx, int key_num);

    pMatchKeyFunc* m_matchKeyFunction;
};

#endif /* VECHASHJOIN_H_ */
