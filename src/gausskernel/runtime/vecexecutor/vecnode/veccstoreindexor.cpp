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
 * -------------------------------------------------------------------------
 *
 * veccstoreindexor.cpp
 *  routines to handle IndexOr nodes for column store.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstoreindexor.cpp
 *
 * -------------------------------------------------------------------------
 */


/*
* INTERFACE ROUTINES
* 	ExecInitCStoreIndexOr		creates and initializes a cstoreindexctidscan node.
*	ExecCStoreIndexOr			scans a column store with 'OR' indexed.
   ExecEndCStoreIndexOr		release any storage allocated
*/
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/instrument.h"
#include "executor/node/nodeBitmapOr.h"
#include "vecexecutor/vecnodecstoreindexor.h"
#include "cstore.h"
#include "vecexecutor/vecexecutor.h"

static int cmpctid(const void* p1, const void* p2)
{
    return ItemPointerCompare((ItemPointer)p1, (ItemPointer)p2);
}

static void deleteStringInfo(StringInfo str)
{
    pfree_ext(str->data);
    pfree_ext(str);
}

/* ----------------------------------------------------------------
 *		ExecInitCstoreIndexOring
 *
 *		Begin all of the subscans of the CStoreIndexOr node.
 * ----------------------------------------------------------------
 */
CStoreIndexOrState* ExecInitCstoreIndexOr(CStoreIndexOr* node, EState* estate, int eflags)
{
    CStoreIndexOrState* indexOringState = makeNode(CStoreIndexOrState);
    PlanState** childplanstates = NULL;
    int nplans = 0;
    int i = 0;
    ListCell* cell = NULL;
    Plan* initNode = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * Set up empty vector of subplan states
     */
    nplans = list_length(node->bitmapplans);

    childplanstates = (PlanState**)palloc0(nplans * sizeof(PlanState*));

    /*
     * create new CStoreIndexOrState for our CStoreIndexOr node
     */
    indexOringState->ps.plan = (Plan*)node;
    indexOringState->ps.state = estate;
    indexOringState->bitmapplans = childplanstates;
    indexOringState->nplans = nplans;
    indexOringState->resultTids = NULL;
    indexOringState->fetchCount = 0;
    indexOringState->m_resultBatch = NULL;
    indexOringState->ps.vectorized = true;

    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "bitmapplanstates".
     */
    i = 0;
    foreach (cell, node->bitmapplans) {
        initNode = (Plan*)lfirst(cell);
        childplanstates[i] = ExecInitNode(initNode, estate, eflags);
        i++;
    }

    return indexOringState;
}

/* ----------------------------------------------------------------
 *	                    ExecCstoreIndexOring
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCstoreIndexOr(CStoreIndexOrState* node)
{
    PlanState** childplans = NULL;
    int nplans = 0;
    int plan_num = 0;
    int var_size = 0;
    errno_t rc;
    StringInfo subresult = makeStringInfo();
    StringInfo tempStringInfo = makeStringInfo();

    var_size = (int)sizeof(Datum);

    if (node->resultTids != NULL) {
        goto fetchDirect;
    }

    /*
     * get information from the node
     */
    childplans = node->bitmapplans;
    nplans = node->nplans;

    /*
     * Scan all the subplans and OR their result bitmaps
     */
    node->resultTids = makeStringInfo();
    for (plan_num = 0; plan_num < nplans; plan_num++) {
        PlanState* subnode = childplans[plan_num];

        /* fetch all the tids from one index scan. */
        VectorBatch* tids = NULL;
        for (;;) {
            tids = VectorEngine(subnode);
            if (unlikely(BatchIsNull(tids)))
                break;

            appendBinaryStringInfo(subresult, (char*)(tids->m_arr[tids->m_cols - 1].m_vals), tids->m_rows * var_size);
        }

        /* If get NULL(not empty batch) from CstoreIndexCtidScan, just return NULL here. */
        if (tids == NULL) {
            /* Delete temp StringInfoData */
            deleteStringInfo(subresult);
            deleteStringInfo(tempStringInfo);

            return NULL;
        }

        qsort(subresult->data, subresult->len / var_size, var_size, cmpctid);

        /* do the merge join between the tids from different index. */
        if (0 == plan_num) {
            copyStringInfo(node->resultTids, subresult);
            node->m_resultBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tids);
        } else {
            copyStringInfo(tempStringInfo, node->resultTids);
            int size1 = tempStringInfo->len / var_size;
            int size2 = subresult->len / var_size;
            int i = 0;
            int j = 0;

            resetStringInfo(node->resultTids);

            ItemPointer* v1array = (ItemPointer*)tempStringInfo->data;
            ItemPointer* v2array = (ItemPointer*)subresult->data;

            while (i < size1 && j < size2) {
                int cmpResult = cmpctid(&(v1array[i]), &(v2array[j]));
                if (cmpResult == 0) {
                    appendBinaryStringInfo(node->resultTids, (char*)(&(v1array[i])), var_size);
                    i++;
                    j++;
                } else if (cmpResult < 0) {
                    appendBinaryStringInfo(node->resultTids, (char*)(&(v1array[i])), var_size);
                    i++;
                } else {
                    appendBinaryStringInfo(node->resultTids, (char*)(&(v2array[j])), var_size);
                    j++;
                }
            }

            while (i < size1) {
                appendBinaryStringInfo(node->resultTids, (char*)(&(v1array[i])), var_size);
                i++;
            }

            while (j < size2) {
                appendBinaryStringInfo(node->resultTids, (char*)(&(v2array[j])), var_size);
                j++;
            }
        }
        resetStringInfo(subresult);
    }

    /* Delete temp StringInfoData */
    deleteStringInfo(subresult);
    deleteStringInfo(tempStringInfo);

fetchDirect:
    node->m_resultBatch->Reset();
    uint64 toFetchCount = Min(BatchMaxSize, node->resultTids->len / var_size - node->fetchCount);
    if (toFetchCount != 0) {
        int colNum = node->m_resultBatch->m_cols;
        rc = memcpy_s(node->m_resultBatch->m_arr[colNum - 1].m_vals,
            BatchMaxSize * var_size,
            node->resultTids->data + node->fetchCount * var_size,
            toFetchCount * var_size);
        securec_check(rc, "", "");
        node->m_resultBatch->m_rows = toFetchCount;
        node->m_resultBatch->FixRowCount();
        node->fetchCount += toFetchCount;
    }

    return node->m_resultBatch;
}

/* ----------------------------------------------------------------
 *		ExecEndCstoreIndexOring
 *
 *		Shuts down the subscans of the BitmapOr node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndCstoreIndexOr(CStoreIndexOrState* node)
{
    PlanState** bitmapplans = NULL;
    int nplans = 0;

    /*
     * get information from the node
     */
    bitmapplans = node->bitmapplans;
    nplans = node->nplans;

    /*
     * shut down each of the subscans (that we've initialized)
     */
    for (int i = 0; i < nplans; i++) {
        if (bitmapplans[i])
            ExecEndNode(bitmapplans[i]);
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanCstoreIndexOr
 * ----------------------------------------------------------------
 */
void ExecReScanCstoreIndexOr(CStoreIndexOrState* node)
{
    int i = 0;

    if (PointerIsValid(node->resultTids)) {
        deleteStringInfo(node->resultTids);
        node->resultTids = NULL;
    }

    node->fetchCount = 0;

    for (i = 0; i < node->nplans; i++) {
        PlanState* subnode = node->bitmapplans[i];

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if (node->ps.chgParam != NULL)
            UpdateChangedParamSet(subnode, node->ps.chgParam);

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (subnode->chgParam == NULL)
            VecExecReScan(subnode);
    }
}
