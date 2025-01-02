
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
 * vecasofjoin.h
 *     Prototypes for vectorized asof join
 *
 * IDENTIFICATION
 *        src/include/vecexecutor/vecasofjoin.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECASOFJOIN_H_
#define VECASOFJOIN_H_

#include "vecexecutor/vecnodes.h"
#include "workload/workload.h"
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonichashjoin.h"
#include "vectorsonic/vsonicpartition.h"

// asof join runtime state
#define ASOF_PART 0
#define ASOF_MERGE 1
#define ASOF_END 2

extern VecAsofJoinState *ExecInitVecAsofJoin(VecAsofJoin *node, EState *estate, int eflags);
extern VectorBatch *ExecVecAsofJoin(VecAsofJoinState *node);
extern void ExecEndVecAsofJoin(VecAsofJoinState *node);
extern void ExecReScanVecAsofJoin(VecAsofJoinState *node);
extern void ExecEarlyFreeVecAsofJoin(VecAsofJoinState *node);

class AsofHashJoin : public SonicHash {
public:
    AsofHashJoin(int size, VecAsofJoinState *node);

    ~AsofHashJoin() {};

    void BuildInner();
    void BuildOuter();

    VectorBatch *Probe();

    void ResetNecessary();

    void freeMemoryContext();

private:
    /* init functions */
    void setHashIndex(uint16 *keyIndx, uint16 *oKeyIndx, List *hashKeys);

    /* init functions */
    void setSortIndex(AttrNumber *keyIndx, Oid *keyCollations, bool *nullsFirstFlags, Oid *opIndx, List *hashKeys,
                      List *sortKeys, int sortStrategy);

    void initMemoryControl();
    void initPartitions();

    void initHashFmgr();

    int comparisonValue(char *cmpName);
    bool CompareSortColumn(SonicSortPartition *build, SonicSortPartition *probe, bool check_part);
    void CopyRow(SonicSortPartition *build, SonicSortPartition *probe);

    void bindingFp();

    /* build side functions to put data */
    void partSort(VectorBatch *batch, bool isBuildOp);

    /* output functions */
    VectorBatch *buildRes(VectorBatch *inBatch, VectorBatch *outBatch);

    void calcHashContextSize(MemoryContext ctx, uint64 *allocateSize, uint64 *freeSize);

    /* Match Functions */
    void initMatchFunc(TupleDesc desc, uint16 keyNum);

    void DispatchKeyInnerFunction(int KeyIndx);

    template <typename innerType>
    void DispatchKeyOuterFunction(int KeyIndx);

    /* partitions functions */
    uint32 calcPartitionNum();

    void calcPartIdx(uint32 *hashVal, uint32 partNum, int nrows);

    /* clean functions */
    void finishJoinPartition(uint32 partIdx);

    void resetMemoryControl();

public:
    double m_build_time;
    double m_probe_time;

private:
    typedef struct SortInputOpAttr {
        int numsortkeys;
        AttrNumber *sortColIdx;
        Oid *sortOperators;
        Oid *collations;
        bool *nullsFirst;
        TupleDesc tupleDesc;

        SortInputOpAttr()
        {
            numsortkeys = 0;
            sortColIdx = NULL;
            sortOperators = NULL;
            collations = NULL;
            nullsFirst = NULL;
            tupleDesc = NULL;
        }
    } SortInputOpAttr;

    bool m_complicatekey;
    /*  last build batch */
    VectorBatch *m_lastBatch;

    /* describe outer operator */
    SonicHashInputOpAttr m_probeOp;

    /* describe inner sort operator */
    SortInputOpAttr m_buildSortOp;
    /* describe outer sort operator */
    SortInputOpAttr m_probeSortOp;
    /* describe compare flag */
    int m_compareValue;
    /* describe spill disk count */
    uint64 m_spillCount;
    /* describe spill disk size */
    long m_spillSize;
    /* describe whether hash key is integer. */
    bool *m_integertype;
    /* runtime state */
    VecAsofJoinState *m_runtime;
    /* record partition indexes of data in atom or batch */
    uint32 m_partIdx[2 * BatchMaxSize];
    /* partitions for inner source */
    SonicHashPartition **m_innerPartitions;
    /* partitions for outer source */
    SonicHashPartition **m_outerPartitions;

    /*
     * number of partitions.
     * inner and outer should have the same number of partitions.
     */
    uint32 m_partNum;

    /*
     * save the match part index
     */
    uint32 m_matchPartIdx;

    /*
     * the probe row  index
     */
    uint64 m_probeIdx;

    /*
     * the cjVector is only allocated and
     * used when m_complicateJoinKey is true
     */
    ScalarVector *m_cjVector;
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICHASHJOIN_H_ */