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
 * vechashagg.h
 *     hash agg class and class member declare.
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vechashagg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECHASHAGG_H_
#define VECHASHAGG_H_

#include "vecexecutor/vecagg.h"

struct AggStateLog {
    bool restore;
    hashCell* lastCell;
    int lastIdx;
    int lastSeg;
};

class HashAggRunner : public BaseAggRunner {
public:
    HashAggRunner(VecAggState* runtime);
    ~HashAggRunner(){};

    bool ResetNecessary(VecAggState* node);

    /* Wrap allocate new hash cell and initialization */
    template <bool simple, bool sgltbl>
    void AllocHashSlot(VectorBatch* batch, int i);
    void HashTableGrowUp();

private:
    /* Different build function, if we exceed the threshold in memory row number,we need to flush to the disk.*/
    template <bool simple, bool unique_check>
    void buildAggTbl(VectorBatch* batch);

    void Build();

    VectorBatch* Probe();

    /* Hash based aggregation.*/
    VectorBatch* Run();

    /* Get the hash source.*/
    hashSource* GetHashSource();

    void BindingFp();

    template <bool expand, bool logit>
    int64 computeHashTableSize(int64 newsize);
    ScalarValue getHashValue(hashCell* hashentry);

    FORCE_INLINE uint32 get_bucket(uint32 hashvalue)
    {
        return hashvalue & (uint32)(m_hashSize - 1);
    }

    template <bool simple>
    void dyHashSlotTmp(VectorBatch* batch, int i, int hash_idx, int start_idx);

    void GetPosbyLoc(uint64 idx, int* nseg, int64* pos);

    template <bool expand, bool logit>
    void BuildHashTable(int64 oldsize);

    void Profile(char* stats, bool* can_wlm_warning_statistics);

private:
    /* Some status log.*/
    AggStateLog m_statusLog;

    /* Hash source.*/
    hashSource* m_hashSource;

    /* Hash value  to store.*/
    ScalarValue m_hashVal[BatchMaxSize];
    int m_fileIdx;
    bool m_can_grow;        /* mark weather can grow up */
    int64 m_max_hashsize;   /* memory allowed max hashtable size */
    int64 m_grow_threshold; /* the threshold to grow up */
    double m_hashbuild_time;
    double m_hashagg_time;
    MemoryContext m_hashcell_context; /* stack context for hashcell */
    HashSegTbl* m_hashData;           /*hashagg table */
    int m_segnum;                     /* segment number */
    int m_hashseg_max;                /* max hashsize for one segment */
    int64 m_hashSize;                 /* total hash size */
    void (HashAggRunner::*m_buildFun)(VectorBatch* batch);
    int m_spill_times; /* spill time */
};

#endif
