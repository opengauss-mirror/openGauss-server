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
 * vecsetop.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecsetop.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECSETOP_H_
#define VECSETOP_H_

#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecnodes.h"

extern VecSetOpState* ExecInitVecSetOp(VecSetOp* node, EState* estate, int eflags);
extern VectorBatch* ExecVecSetOp(VecSetOpState* node);
extern void ExecEndVecSetOp(VecSetOpState* node);
extern void ExecReScanVecSetOp(VecSetOpState* node);
extern void ExecEarlyFreeVecHashedSetop(VecSetOpState* node);

#define SETOP_PREPARE 0
#define SETOP_BUILD 1
#define SETOP_FETCH 2

typedef struct VecSetOpStatePerGroupData {
    long numLeft;  /* number of left-input dups in group */
    long numRight; /* number of right-input dups in group */
} VecSetOpStatePerGroupData;

struct SetOpHashCell {
    SetOpHashCell* m_next;
    VecSetOpStatePerGroupData perGroup;
    hashVal m_val[FLEXIBLE_ARRAY_MEMBER];
};

struct SetOpStateLog {
    bool restore;            /* indicate that the output scanbatch is full, and need to restore one */
    int lastIdx;             /* store the last idx in m_setOpHashData */
    int numOutput;           /* store the remaining number of ouput for the current cell */
    SetOpHashCell* lastCell; /* store the last SetOpHashCell for the next time to go on processing */
};

class setOpTbl : public hashBasedOperator {
public:
    setOpTbl(VecSetOpState* runtime);
    ~setOpTbl(){};

    // reset the environment if necessary when rescan
    //
    void ResetNecessary(VecSetOpState* node);

    /* the entrance to start build the hashtable */
    void Build();

    /* probe the established hash table to get the result batch */
    VectorBatch* Probe();

    /* the function point pointing to execute the mainly task */
    VectorBatch* (setOpTbl::*Operation)();

private:
    /* bind the function point to the coresponding function */
    void BindingFp();

    /* hash based Set Operation */
    VectorBatch* RunHash();

    /* sort based Set Operation */
    template <bool simple>
    VectorBatch* RunSort();

    /*
     * Dump the cell to the m_scanBatch(not the parameter batch) for outputNum times,
     * batch is the data source and need to be stored in m_lastBatch because it is not scaned over,
     * idx indicate the index of next processed row.
     */
    bool DumpOutput(SetOpHashCell* cell, VectorBatch* batch, int& outputNum, int idx);

    /* get the hash source */
    hashSource* GetHashSource();

    /* function point to build the hash table */
    void (setOpTbl::*m_BuildFun)(VectorBatch* batch);

    /* function point to build the scan batch for the output result */
    void (setOpTbl::*m_BuildScanBatch)(SetOpHashCell* cell);

    /* the implemental function to build hash table */
    template <bool simple>
    void BuildSetOpTbl(VectorBatch* batch);

    /* the function to initialize the hashCell, which is also used to initialize the firstCell in sort strategy */
    template <bool simple>
    void InitCell(SetOpHashCell* cell, VectorBatch* batch, int row, int flag);

    /* the function to compare row in batch and cell */
    template <bool simple>
    bool MatchKey(VectorBatch* batch, int batchIdx, SetOpHashCell* cell);

    /* the implemental function to build scan batch */
    void BuildScanBatch(SetOpHashCell* cell);
    template <bool expand>
    int computeHashTableSize(int oldsize);
    void HashTableGrowUp();

    FORCE_INLINE uint32 get_bucket(uint32 hashvalue)
    {
        return hashvalue & (uint32)(m_size - 1);
    }
    template <bool simple>
    void AllocHashSlot(VectorBatch* batch, int i, int flag, bool foundMatch, SetOpHashCell* cell);
    ScalarValue getHashValue(SetOpHashCell* hashentry);

private:
    VecSetOpState* m_runtime;

    /* the number of output columns, include additional column*/
    int m_outerColNum;

    int m_runState;

    /* flag to identify which side of the input */
    int m_firstFlag;

    /* indicate the flag column added by system */
    AttrNumber m_junkCol;

    hashSource* m_hashSource;

    /* include four types which use different strategy to compute the numOutput */
    int m_cmd;

    /* some status log */
    SetOpStateLog m_statusLog;

    /* the batch for scan the result */
    VectorBatch* m_scanBatch;

    /* this is used to store all heads of SetOpHashCell linked lists like m_data */
    SetOpHashCell** m_setOpHashData;

    /* set op hash table size*/
    int m_size;

    VectorBatch* m_lastBatch;

    int m_max_hashsize;     /* memory allowed max hashtable size */
    bool m_can_grow;        /* mark weather can grow up */
    int64 m_grow_threshold; /* the threshold to grow up */
};

#define DO_SETOPERATION(tbl) (((setOpTbl*)tbl)->*(((setOpTbl*)tbl)->Operation))()

#endif /* VECSETOP_H_ */
