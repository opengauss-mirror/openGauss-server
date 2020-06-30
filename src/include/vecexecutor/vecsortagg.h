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
 * vecsortagg.h
 *     sort agg class and class member declare.
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecsortagg.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef VECSORTAGG_H_
#define VECSORTAGG_H_

#include "vecexecutor/vecagg.h"

#define GET_MATCH_KEY 0
#define SORT_MATCH 1

#define GET_SOURCE 0
#define RUN_SORT 1
#define SWITCH_PARSE 2

typedef struct GroupintAtomContainer {
    hashCell* sortGrp[2 * BatchMaxSize];

    /* we need double space for extreme case.*/
    VarBuf* sortBckBuf;

    VarBuf* sortCurrentBuf;

    /* the structure to support the sort group aggregation.*/
    int sortGrpIdx;

} GroupintAtomContainer;

class SortAggRunner : public BaseAggRunner {
public:
    SortAggRunner(VecAggState* runtime);
    ~SortAggRunner(){};

    void endSortAgg();

    void Build()
    {}

    VectorBatch* Probe()
    {
        return NULL;
    }

    bool ResetNecessary(VecAggState* node);

private:
    hashSource* GetSortSource();

    template <bool simple, bool hasDistinct>
    void buildSortAgg(VectorBatch* batch);

    void init_phase();

    void BindingFp();

    void switch_phase();

    void init_indexForApFun();

    template <bool lastCall>
    void FreeSortGrpMem(int num);

    VectorBatch* ReturnData();

    VectorBatch* ReturnLastData();

    void BuildNullScanBatch();

    bool set_group_num(int current_grp);

    VectorBatch* ReturnNullData();

    void set_key();

    template <bool simple, bool hashDistinct>
    void BatchMatchAndAgg(int current_grp, VectorBatch* batch);

    VectorBatch* RunSort();

    /* sort aggregation*/
    VectorBatch* Run();

private:
    bool m_FreeMem;

    bool m_ApFun;

    bool m_noData;

    int m_prepareState;

    /* group state*/
    int m_groupState;

    GroupintAtomContainer* m_sortGrps;

    /* add for AP function*/
    Batchsortstate* m_batchSortIn;

    /* add for AP function*/
    Batchsortstate* m_batchSortOut;

    /* storage result of sort for cube or grouping set*/
    VectorBatch* m_SortBatch;

    hashSource* m_sortSource;

    void (SortAggRunner::*m_buildSortFun)(VectorBatch* batch);
};

#endif
