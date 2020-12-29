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
 * vecwindowagg.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecwindowagg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NODEVECWINDOWAGG_H
#define NODEVECWINDOWAGG_H

#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecagg.h"
#include "windowapi.h"

extern VecWindowAggState* ExecInitVecWindowAgg(VecWindowAgg* node, EState* estate, int eflags);
extern const VectorBatch* ExecVecWindowAgg(VecWindowAggState* node);
extern void ExecEndVecWindowAgg(VecWindowAggState* node);
extern void ExecReScanVecWindowAgg(VecWindowAggState* node);
#define VA_FETCHBATCH 0
#define VA_EVALFUNCTION 1
#define VA_END 2

#define WINDOW_RANK 0
#define WINDOW_ROWNUMBER 1

/*  Save current probing place for next probe */
typedef struct WindowStoreLog {
    int lastFetchIdx;  /* frame idx */
    int lastFrameRows; /* rows idx in a frame */
    bool restore;
    void set(bool flag, int new_idx, int new_rows);
} WindowStoreLog;

/* window aggregation info */
typedef struct WindowAggIdxInfo {
    int aggIdx;    /* the idx of agg */
    bool is_final; /* if has final function */
} WindowAggIdxInfo;

class VecWinAggRuntime : public BaseAggRunner {
public:
    // constructor/
    VecWinAggRuntime(VecWindowAggState* runtime);
    ~VecWinAggRuntime(){};

    // return the result batch.
    const VectorBatch* getBatch();

    /* virtual function in BaseAggRunner */
    void Build()
    {}

    VectorBatch* Probe()
    {
        return NULL;
    }

    bool ResetNecessary(VecAggState* node)
    {
        return true;
    }

    VectorBatch* Run()
    {
        return NULL;
    }

    void BindingFp()
    {}

    /* return agg number */
    int get_aggNum()
    {
        return m_aggNum;
    }

    bool MatchPeerByOrder(VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2);

    bool MatchPeerByPartition(VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2);

    void MatchSequenceByOrder(VectorBatch* batch, int start, int end);

    void MatchSequenceByPartition(VectorBatch* batch, int start, int end);
    void ReplaceEqfunc();
    void ResetNecessary();

private:
    void FetchBatch();

    template <bool has_part>
    const VectorBatch* EvalWindow();
    VectorBatch* EvalAllBatch();
    const VectorBatch* EvalPerBatch();
    void DispatchAssembleFunc();

    void InitAggIdxInfo(VecAggInfo* aggInfo);

    void EvalWindowFunction(WindowStatePerFunc perfuncstate, int idx);

    void DispatchWindowFunction(WindowStatePerFunc perfuncstate, int i);
    void DispatchAggFunction(WindowStatePerAgg peraggState, WindowStatePerFunc perfuncstate, VecAggInfo* aggInfo);
    void EvalWindowAggNopartition(VectorBatch* batch);
    bool CheckAggEncoded(Oid type_id);

public:
    // executor state
    VecWindowAggState* m_winruntime;

    VectorBatch* m_currentBatch;

    VectorBatch* m_outBatch;

    /* to store last batch value */
    VectorBatch* m_lastBatch;

    /* store batch value */
    BatchStore* m_batchstorestate;

    // runtime status
    int m_status;

    // down side has pop all data?
    bool m_noInput;

    FunctionCallInfoData* m_windowFunc;

    int m_winFuns;

    // is the key column simple
    bool m_simplePartKey;

    int m_partitionkey;  // partition by

    int* m_partitionkeyIdx;

    ScalarDesc* m_pkeyDesc;

    bool m_simpleSortKey;

    int m_sortKey;  // sort by

    int* m_sortKeyIdx;

    ScalarDesc* m_skeyDesc;

    uint8 m_winSequence[BatchMaxSize];
    /* array for order equal result */
    uint8 m_winSequence_dup[BatchMaxSize];
    FmgrInfo* m_parteqfunctions;
    FmgrInfo* m_ordeqfunctions;

    /* the number of frames */
    int m_windowIdx;

    /* array for window func index */
    int* m_funcIdx;

    /* we need one more BatchMaxSize  space for last batch */
    hashCell* m_windowGrp[BatchMaxSize + 1];

    /* the numbers in each frame */
    int m_framrows[BatchMaxSize + 1];

    int m_result_rows; /* the rows for agg */

    /* the store state for each function to probe data */
    WindowStoreLog* m_window_store;

    ScalarVector* m_vector; /* the vector for expression */

    /* the same window with last batch for no agg case */
    bool m_same_frame;

    bool* m_cellvar_encoded;               /* trans-value is encoded */
    VarBuf* m_windowCurrentBuf;            /* window agg buffer */
    WindowAggIdxInfo* m_windowagg_idxinfo; /* window agg info */

private:
    template <bool simple, bool is_partition, bool is_ord>
    bool AssembleAggWindow(VectorBatch* batch);

    template <bool simple, bool is_ord>
    bool MatchPeer(VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2, int nkeys, const int* keyIdx);

    template <bool simple, bool is_ord>
    void MatchSequence(VectorBatch* batch, int start, int end, int nkeys, const int* keyIdx);
    template <bool simple, bool has_partition_key>
    void buildWindowAgg(VectorBatch* batch);

    template <bool simple, bool has_partition_key>
    void buildWindowNoAgg(VectorBatch* batch);

    bool (VecWinAggRuntime::*m_assembeFun)(VectorBatch* batch);

    const VectorBatch* (VecWinAggRuntime::*m_EvalFunc)();

    void (VecWinAggRuntime::*m_buildScanBatch)(hashCell* cell, ScalarVector* result_vector, int agg_idx, int final_idx);

    bool (VecWinAggRuntime::*m_MatchPeerOrd)(
        VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2, int nkeys, const int* keyIdx);

    bool (VecWinAggRuntime::*m_MatchPeerPart)(
        VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2, int nkeys, const int* keyIdx);

    void (VecWinAggRuntime::*m_MatchSeqOrd)(VectorBatch* batch, int start, int end, int nkeys, const int* keyIdx);

    void (VecWinAggRuntime::*m_MatchSeqPart)(VectorBatch* batch, int start, int end, int nkeys, const int* keyIdx);
    template <bool simple>
    bool AssemblePerBatch(VectorBatch* batch);
    void RefreshLastbatch(VectorBatch* batch);

    template <bool init_by_cell>
    void initCellValue(hashCell* cell, hashCell* last_cell, int m_frame_rows);

    void BatchAggregation(VectorBatch* batch);
    void BuildScanBatchSimple(hashCell* cell, ScalarVector* result_vector, int agg_idx, int final_idx);
    void BuildScanBatchFinal(hashCell* cell, ScalarVector* result_vector, int agg_idx, int final_idx);

    template <bool has_part>
    void EvalWindowAgg();

    bool CheckStoreValid();

    template <bool simple, bool has_partition_key>
    void buildWindowAggWithSort(VectorBatch* batch);

    void EvalWindowFuncRank(int whichFn, int idx);

    bool IsFinal(int idx);
};

/*
 *@Description: set WindowStoreLog
 */
inline void WindowStoreLog::set(bool flag, int new_idx, int new_rows)
{
    lastFetchIdx = new_idx;
    lastFrameRows = new_rows;
    restore = flag;
}

extern ScalarVector* vwindow_row_number(PG_FUNCTION_ARGS);
extern ScalarVector* vwindow_rank(PG_FUNCTION_ARGS);
extern ScalarVector* vwindow_denserank(PG_FUNCTION_ARGS);

template <int which_func>
extern ScalarVector* vwindowfunc_withsort(PG_FUNCTION_ARGS);

/*
 * @Description: Evaluate window_rank and winow_rownumber with sort
 * @template  which_func - which func,  window_rank is 0 and window_number is 1
 */
template <int which_func>
ScalarVector* vwindowfunc_withsort(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    int whichFn = PG_GETARG_INT32(0);
    VecWindowAggState* state;
    VecWinAggRuntime* winRuntime;
    ScalarVector* pResCol;
    VectorBatch* curWinBatch;
    VectorBatch* preWinBatch;

    int j;
    int batch_rows = 0;
    int start_idx = 0;
    int start_rows = 0; /*  the position for frame start  */
    int idx = 0;        /* vector index */
    int frame_number = 0;
    int fun_idx;

    state = (VecWindowAggState*)winobj->winstate;
    winRuntime = (VecWinAggRuntime*)state->VecWinAggRuntime;

    /* the number of window frame */
    frame_number = winRuntime->m_windowIdx;
    preWinBatch = winRuntime->m_lastBatch;
    curWinBatch = winRuntime->m_currentBatch;

    /* which func, rank or row_number */
    fun_idx = winRuntime->m_funcIdx[whichFn];

    /* result vector */
    pResCol = state->perfunc[whichFn].wfuncstate->m_resultVector;
    pResCol->m_rows = 0;
    batch_rows = winRuntime->m_currentBatch->m_rows;

    /* means that there are remain data in last frame */
    if (winRuntime->m_window_store[whichFn].restore == true) {
        start_idx = winRuntime->m_window_store[whichFn].lastFetchIdx;
        start_rows = winRuntime->m_window_store[whichFn].lastFrameRows;
        winRuntime->m_window_store[whichFn].set(false, 0, 0);
    }

    /* the last frame and there is no data in outer plan */
    if (winRuntime->m_noInput == true) {
        /* rows of current frame */
        int rows = winRuntime->m_framrows[frame_number];
        bool first = true;

        for (int m = start_rows; m < rows; m++) {
            if (first || which_func == WINDOW_RANK) {
                pResCol->m_vals[idx] = winRuntime->m_windowGrp[frame_number]->m_val[fun_idx].val;
                first = false;
            } else
                pResCol->m_vals[idx] = pResCol->m_vals[idx - 1] + 1;

            pResCol->m_rows++;
            idx++;

            if (idx == curWinBatch->m_rows && m < rows - 1) {
                winRuntime->m_window_store[whichFn].set(true, frame_number, m + 1);
                break;
            }
        }
    } else {
        for (j = start_idx; j < frame_number; j++) {
            /* rows of current frame */
            int n_rows = winRuntime->m_framrows[j];
            int m = 0;
            /* the number of rows to be fetched from current frame */
            int frame_calcu_rows = rtl::min(batch_rows - idx, n_rows - start_rows);
            for (m = start_rows; m < frame_calcu_rows + start_rows; m++) {
                pResCol->m_vals[idx] = ((m == start_rows) || (which_func == WINDOW_RANK)) ?
                    winRuntime->m_windowGrp[j]->m_val[fun_idx].val : (pResCol->m_vals[idx - 1] + 1);

                pResCol->m_rows++;
                idx++;
            }

            /* current batch is full */
            if (idx == batch_rows) {
                if (which_func == WINDOW_RANK)
                    state->currentpos = n_rows;
                if (m < n_rows) /* current frame is not end */
                {
                    winRuntime->m_window_store[whichFn].set(true, j, m);
                } else {
                    if (j < frame_number - 1) /* still has frame to be fetch */
                    {
                        winRuntime->m_window_store[whichFn].set(true, j + 1, 0);
                    } else {
                        winRuntime->m_window_store[whichFn].set(false, 0, 0);
                    }
                }
                break;
            }
            /* for a new frame, set start_rows to be 0 */
            start_rows = 0;
        }
    }

    Assert(pResCol->m_rows <= BatchMaxSize);
    Assert(pResCol->m_rows == batch_rows);

    return NULL;
}

#endif /* NODEVECAPPEND_H */
