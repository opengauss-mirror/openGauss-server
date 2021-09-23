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
 * vecwindowagg.cpp
 *	  routines to handle vectorized append nodes.
 *
 *
 * IDENTIFICATION
 *	  Code/src/gausskernel/runtime/vecexecutor/vecnode/vecwindowagg.cpp
 *
 * -------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitVecWindowAgg	- initialize the append node
 *		ExecVecWindowAgg		- retrieve the next batch from the node
 *		ExecEndVecWindowAgg	- shut down the append node
 *		ExecReScanVecWindowAgg - rescan the append node
 *
 *	 NOTES
 *		This implementation follows the same logic as row based append and
 *      we can even reuse some code. See notes in nodeAppend.cpp.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeAppend.h"
#include "vecexecutor/vecwindowagg.h"
#include "vecexecutor/vecexecutor.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeWindowAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "windowapi.h"
#include "vecexecutor/vecfunc.h"
#include "utils/hsearch.h"
#include "utils/batchstore.h"
#include "vecexecutor/vecexpression.h"
#include "utils/int8.h"
#include "vecexecutor/vechashtable.h"
/* ----------------------------------------------------------------
 *		ExecInitVecWindowAgg
 *
 *		Begin all of the subscans of the append node.
 * ----------------------------------------------------------------
 */
extern WindowStatePerAggData* initialize_peragg(
    WindowAggState* winstate, WindowFunc* wfunc, WindowStatePerAgg peraggstate);

VecWindowAggState* ExecInitVecWindowAgg(VecWindowAgg* node, EState* estate, int eflags)
{
    VecWindowAggState* winstate = NULL;
    Plan* outerplan = NULL;
    ExprContext* econtext = NULL;
    ExprContext* tmpcontext = NULL;
    WindowStatePerFunc perfunc;
    WindowStatePerAgg peragg;
    int numfuncs, wfuncno, numaggs, aggno;
    ListCell* l = NULL;
    int i;

    /* Check for support eflags */
    DBG_ASSERT(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     *create state structure
     */
    winstate = makeNode(VecWindowAggState);
    winstate->ss.ps.plan = (Plan*)node;
    winstate->ss.ps.state = estate;
    winstate->ss.ps.vectorized = true;

    /*
     * Create expression contexts.  We need two, one for per-input-tuple
     * processing and one for per-output-tuple processing.  We cheat a little
     * by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, &winstate->ss.ps);
    tmpcontext = winstate->ss.ps.ps_ExprContext;
    winstate->tmpcontext = tmpcontext;
    ExecAssignVectorForExprEval(winstate->tmpcontext);
    ExecAssignExprContext(estate, &winstate->ss.ps);

    /* Create long-lived context for storage of partition-local memory etc */
    winstate->partcontext = AllocSetContextCreate(CurrentMemoryContext,
        "VecWindowAgg_Partition",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Create mid-lived context for aggregate trans values etc */
    winstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext,
        "VecWindowAgg_Aggregates",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * tuple table initialization
     */
    ExecInitScanTupleSlot(estate, &winstate->ss);
    ExecInitResultTupleSlot(estate, &winstate->ss.ps);

    winstate->ss.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)winstate);

    /*
     * WindowAgg nodes never have quals, since they can only occur at the
     * logical top level of a query (ie, after any WHERE or HAVING filters)
     */
    Assert(node->plan.qual == NIL);
    winstate->ss.ps.qual = NIL;

    /*
     * initialize child nodes
     */
    outerplan = outerPlan(node);
    outerPlanState(winstate) = ExecInitNode(outerplan, estate, eflags);

    /*
     * initialize source tuple type (which is also the tuple type that we'll
     * store in the tuplestore and use in all our working slots).
     */
    ExecAssignScanTypeFromOuterPlan(&winstate->ss);

    /*
     * Initialize result tuple type and projection info.
     * result Tuple Table Slot contains virtual tuple, default tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&winstate->ss.ps, TAM_HEAP);

    {
        PlanState* planstate = &winstate->ss.ps;

        planstate->ps_ProjInfo = ExecBuildVecProjectionInfo(
            planstate->targetlist, node->plan.qual, planstate->ps_ExprContext, planstate->ps_ResultTupleSlot, NULL);

        ExecAssignVectorForExprEval(planstate->ps_ProjInfo->pi_exprContext);
    }

    winstate->ss.ps.ps_TupFromTlist = false;

    /* Set up data for comparing tuples */
    if (node->partNumCols > 0)
        winstate->partEqfunctions = execTuplesMatchPrepare(node->partNumCols, node->partOperators);
    if (node->ordNumCols > 0)
        winstate->ordEqfunctions = execTuplesMatchPrepare(node->ordNumCols, node->ordOperators);

    /*
     * WindowAgg nodes use aggvalues and aggnulls as well as Agg nodes.
     */
    numfuncs = winstate->numfuncs;
    numaggs = winstate->numaggs;
    econtext = winstate->ss.ps.ps_ExprContext;

    /*
     * allocate per-wfunc/per-agg state information.
     */
    perfunc = (WindowStatePerFunc)palloc0(sizeof(WindowStatePerFuncData) * numfuncs);
    peragg = (WindowStatePerAgg)palloc0(sizeof(WindowStatePerAggData) * numaggs);
    winstate->perfunc = perfunc;
    winstate->peragg = peragg;
    winstate->windowAggInfo = (VecAggInfo*)palloc0(sizeof(VecAggInfo) * numaggs);

    wfuncno = -1;
    aggno = -1;
    foreach (l, winstate->funcs) {
        WindowFuncExprState* wfuncstate = (WindowFuncExprState*)lfirst(l);
        WindowFunc* wfunc = (WindowFunc*)wfuncstate->xprstate.expr;
        WindowStatePerFunc perfuncstate;
        AclResult aclresult;

        if (wfunc->winref != node->winref) /* planner screwed up? */
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("WindowFunc with winref %u assigned to WindowAgg with winref %u",
                        wfunc->winref,
                        node->winref)));

        /* Look for a previous duplicate window function */
        for (i = 0; i <= wfuncno; i++) {
            if (equal(wfunc, perfunc[i].wfunc) && !contain_volatile_functions((Node*)wfunc))
                break;
        }
        if (i <= wfuncno) {
            /*
             * Found a match to an existing result vector, so just mark it.
             * And we do not need to recalculate this duplicate window function
             * by pointing to the first matched one.
             */
            wfuncstate->wfuncno = i;
            wfuncstate->m_resultVector = perfunc[i].wfuncstate->m_resultVector;
            continue;
        }

        /* Nope, so assign a new PerAgg record */
        perfuncstate = &perfunc[++wfuncno];

        /* Mark WindowFunc state node with assigned index in the result array */
        wfuncstate->wfuncno = wfuncno;

        /* Check permission to call window function */
        aclresult = pg_proc_aclcheck(wfunc->winfnoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(wfunc->winfnoid));

        /*
         * Allocate work memory space for the result of this window function.
         */
        ScalarDesc scalar_desc;
        wfuncstate->m_resultVector = New(CurrentMemoryContext) ScalarVector();
        wfuncstate->m_resultVector->init(CurrentMemoryContext, scalar_desc);

        /* Fill in the perfuncstate data */
        perfuncstate->wfuncstate = wfuncstate;
        perfuncstate->wfunc = wfunc;
        perfuncstate->numArguments = list_length(wfuncstate->args);

        fmgr_info_cxt(wfunc->winfnoid, &perfuncstate->flinfo, econtext->ecxt_per_query_memory);
        fmgr_info_set_expr((Node*)wfunc, &perfuncstate->flinfo);

        perfuncstate->winCollation = wfunc->inputcollid;

        get_typlenbyval(wfunc->wintype, &perfuncstate->resulttypeLen, &perfuncstate->resulttypeByVal);

        /*
         * If it's really just a plain aggregate function, we'll emulate the
         * Agg environment for it.
         */
        perfuncstate->plain_agg = wfunc->winagg;
        if (wfunc->winagg) {
            WindowStatePerAgg peraggstate;
            perfuncstate->aggno = ++aggno;
            peraggstate = &winstate->peragg[aggno];
            initialize_peragg(winstate, wfunc, peraggstate);
            peraggstate->wfuncno = wfuncno;
        } else {
            WindowObject winobj = makeNode(WindowObjectData);

            winobj->winstate = winstate;
            winobj->argstates = wfuncstate->args;
            winobj->localmem = NULL;
            perfuncstate->winobj = winobj;
        }
    }

    /* Update numfuncs, numaggs to match number of unique functions found */
    winstate->numfuncs = wfuncno + 1;
    winstate->numaggs = aggno + 1;

    /* Set up WindowObject for aggregates, if needed */
    if (winstate->numaggs > 0) {
        WindowObject agg_winobj = makeNode(WindowObjectData);

        agg_winobj->winstate = winstate;
        agg_winobj->argstates = NIL;
        agg_winobj->localmem = NULL;
        /* make sure markptr = -1 to invalidate. It may not get used */
        agg_winobj->markptr = -1;
        agg_winobj->readptr = -1;
        winstate->agg_winobj = agg_winobj;
    }

    /* copy frame options to state node for easy access */
    winstate->frameOptions = node->frameOptions;

    /* initialize frame bound offset expressions */
    winstate->startOffset = ExecInitVecExpr((Expr*)node->startOffset, (PlanState*)winstate);
    winstate->endOffset = ExecInitVecExpr((Expr*)node->endOffset, (PlanState*)winstate);

    winstate->all_first = true;
    winstate->partition_spooled = false;
    winstate->more_partitions = false;

    /* Vectorized specific */
    winstate->VecWinAggRuntime = NULL;
    return winstate;
}

/* ----------------------------------------------------------------
 *	   ExecVecWindowAgg
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
const VectorBatch* ExecVecWindowAgg(VecWindowAggState* node)
{
    if (node->VecWinAggRuntime == NULL)
        node->VecWinAggRuntime = New(CurrentMemoryContext) VecWinAggRuntime(node);

    return ((VecWinAggRuntime*)node->VecWinAggRuntime)->getBatch();
}

/* ----------------------------------------------------------------
 *		ExecEndVecWindowAgg
 *
 *		Shuts down the subscans of the append node.
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndVecWindowAgg(VecWindowAggState* node)
{
    // Nothing special to handle, so reuse append code
    PlanState* outer_plan = NULL;

    VecWinAggRuntime* vecwindowrun = (VecWinAggRuntime*)node->VecWinAggRuntime;
    /*
     * clear batchstore tuple.
     */
    if (vecwindowrun != NULL && vecwindowrun->get_aggNum() > 0) {
        Assert(vecwindowrun->m_batchstorestate != NULL);
        batchstore_end(vecwindowrun->m_batchstorestate);
    }

    pfree_ext(node->perfunc);
    pfree_ext(node->peragg);

    outer_plan = outerPlanState(node);
    ExecEndNode(outer_plan);
}

/*
 * @Description: reset variable in VecWinAggRuntime.
 */
void VecWinAggRuntime::ResetNecessary()
{
    int i = 0;
    errno_t rc = EOK;
    m_status = VA_FETCHBATCH;
    m_noInput = false;
    m_windowIdx = 0;
    m_same_frame = false;
    m_result_rows = 0;
    rc = memset_s(m_windowGrp, (BatchMaxSize + 1) * sizeof(hashCell*), 0, (BatchMaxSize + 1) * sizeof(hashCell*));
    securec_check(rc, "\0", "\0");

    rc = memset_s(m_framrows, (BatchMaxSize + 1) * sizeof(int), 0, (BatchMaxSize + 1) * sizeof(int));
    securec_check(rc, "\0", "\0");

    rc = memset_s(&m_winSequence_dup[0], sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
    securec_check(rc, "\0", "\0");

    /* if m_batchstorestate is used, should rebuild */
    if (m_batchstorestate != NULL) {
        WindowAgg* node = (WindowAgg*)m_winruntime->ss.ps.plan;
        TupleDesc out_desc = outerPlanState(m_winruntime)->ps_ResultTupleSlot->tts_tupleDescriptor;
        int64 operator_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
        int64 max_mem = (node->plan.operatorMaxMem > 0) ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) : 0;
        batchstore_end(m_batchstorestate);

        m_batchstorestate = batchstore_begin_heap(
            out_desc, true, false, operator_mem, max_mem, node->plan.plan_node_id, SET_DOP(node->plan.dop));
        m_batchstorestate->m_eflags = 0;
        m_batchstorestate->m_windowagg_use = true;

        if (!m_batchstorestate->m_colInfo) {
            m_batchstorestate->InitColInfo(m_outBatch);
        }
    }

    m_windowCurrentBuf->Reset();

    for (i = 0; i < m_winFuns; i++) {
        m_window_store[i].lastFetchIdx = 0;
        m_window_store[i].lastFrameRows = 0;
        m_window_store[i].restore = false;
    }

    for (i = 0; i < m_winFuns; i++) {
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[i]);

        /* Release any partition-local state of this window function */
        if (perfuncstate->winobj)
            perfuncstate->winobj->localmem = NULL;
    }

    /*
     * Release all partition-local memory (in particular, any partition-local
     * state that we might have trashed our pointers to in the above loop, and
     * any aggregate temp data).  We don't rely on retail pfree because some
     * aggregates might have allocated data we don't have direct pointers to.
     */
    MemoryContextResetAndDeleteChildren(m_winruntime->partcontext);
    MemoryContextResetAndDeleteChildren(m_winruntime->aggcontext);

    m_lastBatch->Reset(true);
    m_outBatch->Reset(true);
    MemoryContextResetAndDeleteChildren(m_hashContext);
}

/*
 * @Description: rescan function for  windowagg.
 */
void ExecReScanVecWindowAgg(VecWindowAggState* node)
{
    VecWinAggRuntime* tb = (VecWinAggRuntime*)node->VecWinAggRuntime;

    if (tb != NULL) {
        tb->ResetNecessary();
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ss.ps.lefttree->chgParam == NULL) {
        VecExecReScan(node->ss.ps.lefttree);
    }
}

/*
 * @Description: constructed function for init window agg information.
 */
VecWinAggRuntime::VecWinAggRuntime(VecWindowAggState* runtime) : BaseAggRunner(), m_winruntime(runtime)
{
    WindowAgg* node = (WindowAgg*)runtime->ss.ps.plan;
    TupleDesc out_desc = outerPlanState(m_winruntime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    int rc = EOK;

    m_outBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, out_desc);
    m_currentBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, out_desc);
    m_lastBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, out_desc);
    m_status = VA_FETCHBATCH;
    m_noInput = false;
    m_winFuns = m_winruntime->numfuncs;
    m_partitionkey = node->partNumCols;
    m_partitionkeyIdx = NULL;
    m_cellvar_encoded = NULL;
    m_simplePartKey = true;
    m_parteqfunctions = runtime->partEqfunctions;
    m_ordeqfunctions = runtime->ordEqfunctions;
    m_sortKeyIdx = NULL;
    m_skeyDesc = NULL;
    m_batchstorestate = NULL;
    m_aggIdx = NULL;
    m_aggCount = NULL;
    m_finalAggInfo = NULL;
    m_aggNum = runtime->numaggs;
    m_cols = 0;
    m_windowIdx = 0;
    m_same_frame = false;
    m_finalAggNum = 0;
    m_result_rows = 0;
    m_windowagg_idxinfo = NULL;
    m_MatchSeqPart = NULL;
    m_MatchPeerPart = NULL;

    m_cellVarLen = m_winFuns - m_aggNum;

    Assert(m_cellVarLen >= 0);

    m_window_store = (WindowStoreLog*)palloc0(sizeof(WindowStoreLog) * m_winFuns);

    rc = memset_s(m_windowGrp, (BatchMaxSize + 1) * sizeof(hashCell*), 0, (BatchMaxSize + 1) * sizeof(hashCell*));
    securec_check(rc, "\0", "\0");

    rc = memset_s(m_framrows, (BatchMaxSize + 1) * sizeof(int), 0, (BatchMaxSize + 1) * sizeof(int));
    securec_check(rc, "\0", "\0");

    int64 operator_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
    int64 max_mem = (node->plan.operatorMaxMem > 0) ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) : 0;

    if (m_partitionkey > 0) {
        m_partitionkeyIdx = (int*)palloc0(sizeof(int) * m_partitionkey);
        m_pkeyDesc = (ScalarDesc*)palloc0(sizeof(ScalarDesc) * m_partitionkey);
        for (int i = 0; i < m_partitionkey; i++) {
            m_partitionkeyIdx[i] = node->partColIdx[i] - 1;
            m_pkeyDesc[i] = m_outBatch->m_arr[m_partitionkeyIdx[i]].m_desc;
            if (m_pkeyDesc[i].encoded) {
                m_simplePartKey = false;
            }
        }
    }

    ScalarDesc unknown_desc;
    m_vector = New(CurrentMemoryContext) ScalarVector();
    m_vector->init(CurrentMemoryContext, unknown_desc);

    m_sortKey = node->ordNumCols;
    m_simpleSortKey = true;

    DispatchAssembleFunc();

    if (m_sortKey > 0) {
        m_sortKeyIdx = (int*)palloc0(sizeof(int) * m_sortKey);
        m_skeyDesc = (ScalarDesc*)palloc0(sizeof(ScalarDesc) * m_sortKey);
        for (int i = 0; i < m_sortKey; i++) {
            m_sortKeyIdx[i] = node->ordColIdx[i] - 1;
            m_skeyDesc[i] = m_outBatch->m_arr[m_sortKeyIdx[i]].m_desc;
            if (m_skeyDesc[i].encoded) {
                m_simpleSortKey = false;
            }
        }

        m_cellvar_encoded = (bool*)palloc0(sizeof(bool) * m_aggNum);
    }

    /* Set up function for comparing ScalarVector */
    if (m_simpleSortKey) {
        m_MatchPeerOrd = &VecWinAggRuntime::MatchPeer<true, true>;
        m_MatchSeqOrd = &VecWinAggRuntime::MatchSequence<true, true>;
    } else {
        m_MatchPeerOrd = &VecWinAggRuntime::MatchPeer<false, true>;
        m_MatchSeqOrd = &VecWinAggRuntime::MatchSequence<false, true>;
    }

    /* Initialize window function information according to m_winFuncs and WindowStatePerFunc */
    m_windowFunc = (FunctionCallInfoData*)palloc0(sizeof(FunctionCallInfoData) * m_winFuns);

    m_funcIdx = (int*)palloc0(sizeof(int) * m_winFuns);

    int j = 0;
    for (int i = 0; i < m_winFuns; i++) {
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[i]);
        if (perfuncstate->plain_agg == false) {
            DispatchWindowFunction(perfuncstate, i);
            m_funcIdx[i] = j;
            j++;
        }
    }

    for (int i = 0; i < m_aggNum; i++) {
        WindowStatePerAgg peraggstate = &(m_winruntime->peragg[i]);
        int winfuncno = peraggstate->wfuncno;
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[winfuncno]);
        DispatchAggFunction(peraggstate, perfuncstate, &runtime->windowAggInfo[i]);

        if (m_sortKey > 0)
            m_cellvar_encoded[i] = CheckAggEncoded(peraggstate->transfn.fn_rettype);
    }

    /* agg start from here */
    if (m_aggNum > 0) {
        m_batchstorestate = batchstore_begin_heap(
            out_desc, true, false, operator_mem, max_mem, node->plan.plan_node_id, SET_DOP(node->plan.dop));
        m_batchstorestate->m_eflags = 0;
        m_batchstorestate->m_windowagg_use = true;

        if (!m_batchstorestate->m_colInfo) {
            m_batchstorestate->InitColInfo(m_outBatch);
        }

        init_aggInfo(m_aggNum, runtime->windowAggInfo);

        m_windowagg_idxinfo = (WindowAggIdxInfo*)palloc0(sizeof(WindowAggIdxInfo) * m_aggNum);
        InitAggIdxInfo(runtime->windowAggInfo);
    }

    m_cellSize = offsetof(hashCell, m_val) + m_cols * sizeof(hashVal);

    if (m_finalAggNum > 0)
        m_buildScanBatch = &VecWinAggRuntime::BuildScanBatchFinal;
    else
        m_buildScanBatch = &VecWinAggRuntime::BuildScanBatchSimple;

    ReplaceEqfunc();

    m_econtext = runtime->ss.ps.ps_ExprContext;

    m_windowCurrentBuf = New(CurrentMemoryContext) VarBuf(CurrentMemoryContext);
}

/*
 * @Description: get batch value from left tree
 */
const VectorBatch* VecWinAggRuntime::getBatch()
{
    while (true) {
        switch (m_status) {
            case VA_FETCHBATCH: {
                FetchBatch();

                if (m_aggNum == 0 && true == m_noInput)
                    return NULL;
                if (m_aggNum > 0 && m_partitionkey == 0 && m_framrows[0] == 0)
                    return NULL;
                if (m_partitionkey != 0 && m_noInput == true && m_framrows[m_windowIdx] == 0) /* no input */
                    return NULL;

                m_status = VA_EVALFUNCTION;
            } break;

            case VA_EVALFUNCTION: {
                const VectorBatch* res_batch = NULL;
                return res_batch = InvokeFp(m_EvalFunc)();
            }

            case VA_END: {
                return NULL;
            }
            default:
                break;
        }
    }
}

void VecWinAggRuntime::FetchBatch()
{
    PlanState* outer_plan = outerPlanState(m_winruntime);
    VectorBatch* outer_batch = NULL;

    for (;;) {
        outer_batch = VectorEngine(outer_plan);
        if (BatchIsNull(outer_batch)) {
            m_noInput = true;
            m_result_rows += m_framrows[m_windowIdx];
            break;
        }

        if (InvokeFp(m_assembeFun)(outer_batch))
            break;
    }
}

void VecWinAggRuntime::DispatchWindowFunction(WindowStatePerFunc perfuncstate, int i)
{
    VecFuncCacheEntry* entry = NULL;
    Oid funcoid = perfuncstate->flinfo.fn_oid;
    bool found = false;

    entry = (VecFuncCacheEntry*)hash_search(g_instance.vec_func_hash, &funcoid, HASH_FIND, &found);

    if (found) {
        InitFunctionCallInfoData(m_windowFunc[i], &perfuncstate->flinfo, 2, perfuncstate->winCollation, NULL, NULL);

        if (m_aggNum == 0 || m_sortKey == 0)
            m_windowFunc[i].flinfo->vec_fn_addr = entry->vec_fn_cache[0];
        else
            m_windowFunc[i].flinfo->vec_fn_addr = entry->vec_fn_cache[1];
    } else {
        const FmgrBuiltin* fbp = NULL;
        fbp = fmgr_isbuiltin(funcoid);
        if (fbp != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported window function %s in vector engine", fbp->funcName)));
        else
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Unsupported window function %u in vector engine", funcoid)));
    }
}

/*
 * @Description: initialize vector agg function.
 * @in peraggState: per-aggregate working state for the Agg Function
 * @in perfuncstate: per-funcstate working state for Window Function(Include Agg Function)
 * @in aggInfo: the agg information to be intialized
 * @return - void
 */
void VecWinAggRuntime::DispatchAggFunction(
    WindowStatePerAgg peraggState, WindowStatePerFunc perfuncstate, VecAggInfo* aggInfo)
{
    VecFuncCacheEntry* entry = NULL;
    Oid transfn_oid = perfuncstate->flinfo.fn_oid;
    bool found = false;

    entry = (VecFuncCacheEntry*)hash_search(g_instance.vec_func_hash, &transfn_oid, HASH_FIND, &found);

    if (found) {
        InitFunctionCallInfoData(
            aggInfo->vec_agg_function, &peraggState->transfn, 2, perfuncstate->winCollation, NULL, NULL);

        aggInfo->vec_agg_cache = &entry->vec_agg_cache[0];
        aggInfo->vec_agg_final = &entry->vec_transform_function[0];

        aggInfo->vec_agg_function.flinfo->vec_fn_addr = aggInfo->vec_agg_cache[0];

        if (OidIsValid(peraggState->finalfn_oid)) {
            InitFunctionCallInfoData(
                aggInfo->vec_final_function, &peraggState->finalfn, 2, perfuncstate->winCollation, NULL, NULL);
            aggInfo->vec_final_function.flinfo->fn_addr = aggInfo->vec_agg_final[0];
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_AGG), errmsg("UnSupported vector window aggregation function %u", transfn_oid)));
    }
}

/*
 * @Description: check aggfunc trans value is encode
 * @in type_id - type oid
 * @return bool - return true if encoded
 */
bool VecWinAggRuntime::CheckAggEncoded(Oid type_id)
{
    if (INT8ARRAYOID == type_id || FLOAT8ARRAYOID == type_id)
        return false;

    return COL_IS_ENCODE(type_id);
}

template <bool simple, bool is_ord>
bool VecWinAggRuntime::MatchPeer(
    VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2, int nkeys, const int* keyIdx)
{
    bool match = true;
    int i;

    ScalarValue val1;
    ScalarValue val2;
    uint8 flag1;
    uint8 flag2;
    ScalarVector* vector1 = batch1->m_arr;
    ScalarVector* vector2 = batch2->m_arr;
    FunctionCallInfoData fcinfo;
    PGFunction eqfunc;
    Datum args[2];
    fcinfo.arg = &args[0];

    for (i = 0; i < nkeys; i++) {
        val1 = vector1[keyIdx[i]].m_vals[idx1];
        val2 = vector2[keyIdx[i]].m_vals[idx2];
        flag1 = vector1[keyIdx[i]].m_flag[idx1];
        flag2 = vector2[keyIdx[i]].m_flag[idx2];
        if (BOTH_NOT_NULL(flag1, flag2)) {
            if (simple || vector1[keyIdx[i]].m_desc.encoded == false) {
                if (val1 == val2)
                    continue;
                else
                    return false;
            } else {
                fcinfo.arg[0] = ScalarVector::Decode(val1);
                fcinfo.arg[1] = ScalarVector::Decode(val2);
                if (is_ord) {
                    fcinfo.flinfo = (m_ordeqfunctions + i);
                    eqfunc = m_ordeqfunctions[i].fn_addr;
                    match = eqfunc(&fcinfo);
                } else {
                    fcinfo.flinfo = (m_parteqfunctions + i);
                    eqfunc = m_parteqfunctions[i].fn_addr;
                    match = eqfunc(&fcinfo);
                }

                if (match == false)
                    return false;
            }
        } else if (IS_NULL(flag1) && IS_NULL(flag2))  // both null is equal
            continue;
        else if (IS_NULL(flag1) || IS_NULL(flag2))
            return false;
    }

    return true;
}

/*
 * @Description: devide the batch into partitions and evaluate  aggregates
 * @in batch - current batch
 * @return bool - return true if there are more than one frames
 */
template <bool simple, bool is_partition, bool is_ord>
bool VecWinAggRuntime::AssembleAggWindow(VectorBatch* batch)
{
    batchstore_putbatch(m_batchstorestate, batch);

    if (true == is_ord)
        buildWindowAggWithSort<simple, is_partition>(batch);
    else
        buildWindowAgg<simple, is_partition>(batch);

    RefreshLastbatch(batch);

    if (m_windowIdx > 0) {
        m_result_rows = 0;

        for (int i = 0; i < m_windowIdx; i++) {
            m_result_rows += m_framrows[i];
        }

        if (m_noInput == true)
            m_result_rows += m_framrows[m_windowIdx];
        return true;
    } else
        return false;
}

/*
 * @Description: devide the batch into partitions
 * @in batch - current batch
 * @return bool -  return true if there are more than one frames
 */
template <bool simple>
bool VecWinAggRuntime::AssemblePerBatch(VectorBatch* batch)
{
    for (int i = 0; i < m_windowIdx; i++) {
        m_framrows[i] = 0;
    }

    m_windowIdx = 0;

    if (m_partitionkey > 0)
        buildWindowNoAgg<simple, true>(batch);
    else
        buildWindowNoAgg<simple, false>(batch);
    m_currentBatch = batch;
    m_windowIdx++;
    return true;
}

/*
 * @Description: to refresh m_lastBatch, only copy one row for next matching
 * @in batch - the batch to be refreshed
 * @return - void
 */
void VecWinAggRuntime::RefreshLastbatch(VectorBatch* batch)
{
    m_lastBatch->Reset(true);
    /* -1 means the last row */
    m_lastBatch->Copy<true, false>(batch, batch->m_rows - 1, -1);
}

/*
 * @Description: Check whether the current agg has a final function
 * @in idx - agg index
 * @return  bool - return true is there are avg function
 */
bool VecWinAggRuntime::IsFinal(int idx)
{
    for (int i = 0; i < m_finalAggNum; i++) {
        if (m_finalAggInfo[i].idx == idx)
            return true;
    }

    return false;
}

/*
 * @Description: to initialize the  cell, if init_by_cell is true, initialize the cell value by last_cell
 * @in cell - the point to be initialized
 * @return - void
 */
template <bool init_by_cell>
void VecWinAggRuntime::initCellValue(hashCell* cell, hashCell* last_cell, int frame_rows)
{
    int k;

    /* set the init value. */
    if (false == init_by_cell) {
        /* init window func info */
        for (k = 0; k < m_cellVarLen; k++) {
            cell->m_val[k].val = 1;
            SET_NOTNULL(cell->m_val[k].flag);
        }

        /* init window agg info */
        for (k = 0; k < m_aggNum; k++) {
            if (m_aggCount[k]) {
                cell->m_val[m_aggIdx[k]].val = 0;
                SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
            } else
                SET_NULL(cell->m_val[m_aggIdx[k]].flag);
        }
    } else {
        /* init window func info with last_cell */
        for (k = 0; k < m_cellVarLen; k++) {
            cell->m_val[k].val = frame_rows + last_cell->m_val[k].val;
            SET_NOTNULL(cell->m_val[k].flag);
        }

        /* init window agg info with last_cell */
        for (k = 0; k < m_aggNum; k++) {
            /* no avg */
            if (m_finalAggNum == 0) {
                if (NOT_NULL(last_cell->m_val[m_aggIdx[k]].flag)) {
                    if (m_cellvar_encoded[k] == 0)
                        cell->m_val[m_aggIdx[k]].val = last_cell->m_val[m_aggIdx[k]].val;
                    else {
                        cell->m_val[m_aggIdx[k]].val = addVariable(m_hashContext, last_cell->m_val[m_aggIdx[k]].val);
                    }
                    SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
                } else
                    SET_NULL(cell->m_val[m_aggIdx[k]].flag);
            } else {
                int agg_idx = m_aggIdx[k];
                bool is_final = IsFinal(agg_idx);

                if (NOT_NULL(last_cell->m_val[m_aggIdx[k]].flag)) {
                    if (m_cellvar_encoded[k] == 0)
                        cell->m_val[m_aggIdx[k]].val = last_cell->m_val[m_aggIdx[k]].val;
                    else {
                        cell->m_val[m_aggIdx[k]].val = addVariable(m_hashContext, last_cell->m_val[m_aggIdx[k]].val);
                    }

                    SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
                    if (is_final) {
                        if (NOT_NULL(last_cell->m_val[m_aggIdx[k] + 1].flag)) {
                            cell->m_val[m_aggIdx[k] + 1].val = last_cell->m_val[m_aggIdx[k] + 1].val;
                            SET_NOTNULL(cell->m_val[m_aggIdx[k] + 1].flag);
                        } else
                            SET_NULL(cell->m_val[m_aggIdx[k] + 1].flag);

                        /* For stddev_samp, we need copy sum(x2) */
                        if (NOT_NULL(last_cell->m_val[m_aggIdx[k] + 2].flag)) {
                            if (m_cellvar_encoded[k])
                                cell->m_val[m_aggIdx[k] + 2].val =
                                    addVariable(m_hashContext, last_cell->m_val[m_aggIdx[k] + 2].val);
                            else
                                cell->m_val[m_aggIdx[k] + 2].val = last_cell->m_val[m_aggIdx[k] + 2].val;

                            SET_NOTNULL(cell->m_val[m_aggIdx[k] + 2].flag);
                        } else
                            SET_NULL(cell->m_val[m_aggIdx[k] + 2].flag);
                    }
                } else {
                    SET_NULL(cell->m_val[m_aggIdx[k]].flag);
                }
            }
        }
    }
}

/*
 * @Description: Binding funtion pointer for executing.
 */
void VecWinAggRuntime::DispatchAssembleFunc()
{
    if (m_aggNum > 0) {
        if (m_partitionkey == 0 && m_sortKey == 0) {
            if (m_simplePartKey)
                m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<true, false, false>;
            else
                m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<false, false, false>;
            m_EvalFunc = &VecWinAggRuntime::EvalWindow<false>;
        } else {
            if (m_sortKey > 0) {
                if (m_partitionkey > 0) {
                    if (m_simplePartKey) {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<true, true, true>;
                    } else {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<false, true, true>;
                    }
                } else {
                    if (m_simplePartKey) {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<true, false, true>;
                    } else {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<false, false, true>;
                    }
                }
                m_EvalFunc = &VecWinAggRuntime::EvalWindow<true>;
            } else {
                if (m_partitionkey > 0) {
                    if (m_simplePartKey) {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<true, true, false>;
                    } else {
                        m_assembeFun = &VecWinAggRuntime::AssembleAggWindow<false, true, false>;
                    }
                    m_EvalFunc = &VecWinAggRuntime::EvalWindow<true>;
                }
            }
        }

        if (m_sortKey > 0) {
            if (m_simplePartKey) {
                m_MatchPeerPart = &VecWinAggRuntime::MatchPeer<true, true>;
                m_MatchSeqPart = &VecWinAggRuntime::MatchSequence<true, true>;
            } else {
                m_MatchPeerPart = &VecWinAggRuntime::MatchPeer<false, false>;
                m_MatchSeqPart = &VecWinAggRuntime::MatchSequence<false, false>;
            }
        }
    } else {
        if (m_simplePartKey)
            m_assembeFun = &VecWinAggRuntime::AssemblePerBatch<true>;
        else
            m_assembeFun = &VecWinAggRuntime::AssemblePerBatch<false>;
        m_EvalFunc = &VecWinAggRuntime::EvalPerBatch;
    }
}

/*
 * @Description: Project and compute Aggregation.
 * @in batch - current batch.
 * @return - void
 */
void VecWinAggRuntime::BatchAggregation(VectorBatch* batch)
{
    int wfuncno;
    WindowStatePerFunc perfuncstate;

    for (int i = 0; i < m_aggNum; i++) {
        ScalarVector* pVector = NULL;
        WindowStatePerAgg peraggstate = &m_winruntime->peragg[i];
        ExprContext* econtext = NULL;
        ListCell* arg = NULL;
        wfuncno = peraggstate->wfuncno;
        perfuncstate = &m_winruntime->perfunc[wfuncno];
        WindowFuncExprState* wfuncstate = perfuncstate->wfuncstate;

        if (wfuncstate->args != NULL) {
            econtext = m_winruntime->tmpcontext;
            econtext->ecxt_outerbatch = batch;
            econtext->align_rows = batch->m_rows;

            foreach (arg, wfuncstate->args) {
                ExprState* argstate = (ExprState*)lfirst(arg);
                pVector = VectorExprEngine(argstate, econtext, econtext->ecxt_outerbatch->m_sel, m_vector, NULL);
                if (pVector != m_vector)
                    m_vector->copy(pVector);
            }
        } else
            m_vector->copy(&batch->m_arr[0]);
        AggregationOnScalar(&m_winruntime->windowAggInfo[i], m_vector, m_aggIdx[i], &m_Loc[0]);

        if (econtext != NULL)
            ResetExprContext(econtext);
    }
}

/*
 * @Description: eval aggregation with no partition.
 * @in batch - current batch.
 * @return - void
 */
void VecWinAggRuntime::EvalWindowAggNopartition(VectorBatch* batch)
{
    bool finished = false;
    bool first_batch = true;
    int i;
    int nrows = batch->m_rows;
    hashCell* cell = NULL;
    int start_idx = 0;

    if (BatchIsNull(m_lastBatch) == false) {
        first_batch = false;
        if (MatchPeerByOrder(m_lastBatch, m_lastBatch->m_rows - 1, batch, 0) == false) {
            m_windowIdx++; /* a new frame */
        }
    }

    cell = m_windowGrp[m_windowIdx];

    if (cell == NULL) {
        cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);

        if (true == first_batch) {
            initCellValue<false>(cell, NULL, 0);
        } else {
            /* init cell value by last order cell value */
            if (m_windowIdx == 0) {
                initCellValue<true>(cell, m_windowGrp[m_windowIdx], m_framrows[m_windowIdx]);
            } else {
                initCellValue<true>(cell, m_windowGrp[m_windowIdx - 1], m_framrows[m_windowIdx - 1]);
            }
        }

        m_windowGrp[m_windowIdx] = cell;
    }

    Assert(cell != NULL);
    m_Loc[0] = cell;
    m_outBatch->Copy<false, true>(batch, 0, 1); /* copy first row data */
    m_framrows[m_windowIdx]++;

    MatchSequenceByOrder(batch, 0, nrows - 1);

    for (i = 1; i < nrows; i++) {
        if (m_winSequence[i] == 1) {
            /* a new order , eval aggregation */
            BatchAggregation(m_outBatch);
            m_outBatch->Reset();
            m_windowIdx++;
            start_idx = i;
        }

        cell = m_windowGrp[m_windowIdx];

        if (cell == NULL) {
            cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);

            initCellValue<true>(cell, m_windowGrp[m_windowIdx - 1], m_framrows[m_windowIdx - 1]);

            m_windowGrp[m_windowIdx] = cell;
        }

        Assert(cell != NULL);
        m_Loc[i - start_idx] = cell;
        m_outBatch->Copy<false, true>(batch, i, i + 1); /* copy one row from batch */
        m_framrows[m_windowIdx]++;
        /* the end of current batch */
        if (i == nrows - 1) {
            BatchAggregation(m_outBatch);
            m_outBatch->Reset();
            finished = true;
        }
    }

    /* current batch is end, but no new order */
    if (false == finished) {
        BatchAggregation(m_outBatch);
        m_outBatch->Reset();
    }
}

/*
 * @Description: init cell value and  compute Aggregation.
 * @in batch - current batch.
 * @return - void
 */
template <bool simple, bool has_partition_key>
void VecWinAggRuntime::buildWindowAggWithSort(VectorBatch* batch)
{
    int i;
    int nrows = batch->m_rows;
    hashCell* cell = NULL;
    VectorBatch* last_batch = NULL;
    int last_batch_idx;
    int start_idx = 0;
    errno_t rc = 0;

    /* no partition */
    if (has_partition_key == false) {
        EvalWindowAggNopartition(batch);
        return;
    } else {
        bool first_batch = true;
        bool match_peer = true;
        bool match_order = true;
        bool finished = false;

        MatchSequence<simple, false>(batch, 0, batch->m_rows - 1, m_partitionkey, m_partitionkeyIdx);
        /* use m_winSequence_dup to store the partition result */
        rc = memcpy_s(&m_winSequence_dup[0], BatchMaxSize, &m_winSequence[0], batch->m_rows);
        securec_check(rc, "\0", "\0");

        MatchSequenceByOrder(batch, 0, nrows - 1);
        last_batch = m_lastBatch;
        last_batch_idx = last_batch->m_rows - 1;

        /* not the first time */
        if (BatchIsNull(m_lastBatch) == false) {
            first_batch = false;
            match_peer =
                MatchPeer<simple, false>(m_lastBatch, last_batch_idx, batch, 0, m_partitionkey, m_partitionkeyIdx);
            match_order = MatchPeerByOrder(m_lastBatch, m_lastBatch->m_rows - 1, batch, 0);
            if (match_peer == false || match_order == false) {
                m_windowIdx++; /* a new frame */
            }
        }

        cell = m_windowGrp[m_windowIdx];

        if (cell == NULL) {
            cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);
            if (true == first_batch || match_peer == false) {
                initCellValue<false>(cell, NULL, 0); /* a new frame */
            } else {
                /* init cell value by last order cell value */
                if (m_windowIdx == 0) {
                    initCellValue<true>(cell, m_windowGrp[m_windowIdx], m_framrows[m_windowIdx]);
                } else {
                    initCellValue<true>(cell, m_windowGrp[m_windowIdx - 1], m_framrows[m_windowIdx - 1]);
                }
            }
            m_windowGrp[m_windowIdx] = cell;
        }

        Assert(cell != NULL);
        m_Loc[0] = cell;
        m_outBatch->Copy<false, true>(batch, 0, 1);
        m_framrows[m_windowIdx]++;

        for (i = 1; i < nrows; i++) {
            if (m_winSequence_dup[i] == 1 || m_winSequence[i] == 1) {
                m_windowIdx++;
                BatchAggregation(m_outBatch);
                m_outBatch->Reset();
                start_idx = i;
            }
            cell = m_windowGrp[m_windowIdx];

            if (cell == NULL) {
                cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);
                if (m_winSequence_dup[i] == 1) {
                    initCellValue<false>(cell, NULL, 0); /* a new frame */
                } else {
                    initCellValue<true>(cell, m_windowGrp[m_windowIdx - 1], m_framrows[m_windowIdx - 1]);
                }

                m_windowGrp[m_windowIdx] = cell;
            }

            Assert(cell != NULL);
            m_Loc[i - start_idx] = cell;
            m_outBatch->Copy<false, true>(batch, i, i + 1); /* copy one row from batch */
            m_framrows[m_windowIdx]++;
            /* the end of current batch */
            if (i == nrows - 1) {
                BatchAggregation(m_outBatch);
                m_outBatch->Reset();
                finished = true;
            }
        }

        /* current batch is end, but no new order */
        if (false == finished) {
            BatchAggregation(m_outBatch);
            m_outBatch->Reset();
        }
    }
}

/*
 * @Description: init cell value and  compute Aggregation.
 * @in batch - current batch.
 * @return - void
 */
template <bool simple, bool has_partition_key>
void VecWinAggRuntime::buildWindowAgg(VectorBatch* batch)
{
    int i;
    int nrows = batch->m_rows;
    hashCell* cell = NULL;
    VectorBatch* last_batch = NULL;
    int last_batch_idx;

    /* no partition */
    if (has_partition_key == false) {
        for (i = 0; i < nrows; i++) {
            /* only one result */
            cell = m_windowGrp[0];
            if (cell == NULL) {
                cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);
                initCellValue<false>(cell, NULL, 0);
                m_windowGrp[0] = cell;
            }

            Assert(cell != NULL);
            m_Loc[i] = cell;
        }

        m_framrows[0] += nrows;
        BatchAggregation(batch);
        return;
    }

    MatchSequence<simple, false>(batch, 0, batch->m_rows - 1, m_partitionkey, m_partitionkeyIdx);

    last_batch = m_lastBatch;
    last_batch_idx = last_batch->m_rows - 1;

    /* not the first time */
    if (BatchIsNull(m_lastBatch) == false) {
        if (MatchPeer<simple, false>(m_lastBatch, last_batch_idx, batch, 0, m_partitionkey, m_partitionkeyIdx) == false) {
            m_windowIdx++; /* a new frame */
        }
    }

    cell = m_windowGrp[m_windowIdx];

    if (cell == NULL) {
        cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);
        initCellValue<false>(cell, NULL, 0);
        m_windowGrp[m_windowIdx] = cell;
    }

    Assert(cell != NULL);
    m_Loc[0] = cell;
    m_framrows[m_windowIdx]++;

    for (i = 1; i < nrows; i++) {
        if (m_winSequence[i] == 1) {
            /* a new frame */
            m_windowIdx++;
        }

        cell = m_windowGrp[m_windowIdx];

        if (cell == NULL) {
            cell = (hashCell*)m_windowCurrentBuf->Allocate(m_cellSize);
            initCellValue<false>(cell, NULL, 0);
            m_windowGrp[m_windowIdx] = cell;
        }

        Assert(cell != NULL);
        m_Loc[i] = cell;
        m_framrows[m_windowIdx]++;
    }
    /* do aggregation */
    BatchAggregation(batch);
}

/*
 * @Description: devide data into partitions.
 * @in batch - current batch.
 * @return - void
 */
template <bool simple, bool has_partition_key>
void VecWinAggRuntime::buildWindowNoAgg(VectorBatch* batch)
{
    int i;
    int nrows = batch->m_rows;
    VectorBatch* last_batch = NULL;
    int last_batch_idx;

    if (has_partition_key == false) {
        /* all is one frame */
        m_framrows[0] = nrows;
        m_same_frame = true;
        return;
    }

    MatchSequence<simple, false>(batch, 0, batch->m_rows - 1, m_partitionkey, m_partitionkeyIdx);

    last_batch = m_lastBatch;
    last_batch_idx = last_batch->m_rows - 1;

    if (BatchIsNull(m_lastBatch) == false) {
        if (MatchPeer<simple, false>(m_lastBatch, last_batch_idx, batch, 0, m_partitionkey, m_partitionkeyIdx) == true) {
            m_same_frame = true;
        } else {
            m_same_frame = false;
        }
    }

    for (i = 0; i < nrows; i++) {
        /* a new frame */
        if (m_winSequence[i] == 1) {
            m_windowIdx++;
        }
        m_framrows[m_windowIdx]++;
    }
}

template <bool simple, bool is_ord>
void VecWinAggRuntime::MatchSequence(VectorBatch* batch, int start, int end, int nkeys, const int* keyIdx)
{
    ScalarVector* vector = NULL;
    Datum key1, key2;
    uint8 flag1, flag2;
    int nrows = batch->m_rows;
    FunctionCallInfoData fcinfo;
    Datum args[2];
    fcinfo.arg = &args[0];
    int idx = 0;
    PGFunction eqfunc;
    errno_t rc;
    Assert(end <= nrows);

    rc = memset_s(&m_winSequence[0], sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * nrows);
    securec_check(rc, "\0", "\0");
    // no order keys, just return back
    if (nkeys == 0)
        return;

    for (int i = 0; i < nkeys; i++) {
        vector = &batch->m_arr[keyIdx[i]];
        key1 = vector->m_vals[start];
        flag1 = vector->m_flag[start];
        idx = 1;
        for (int j = start + 1; j <= end; j++) {
            key2 = vector->m_vals[j];
            flag2 = vector->m_flag[j];

            // m_winSequence[j] == 1, means already a new group, no need to compare.
            if (m_winSequence[idx] == 0) {
                if (BOTH_NOT_NULL(flag1, flag2)) {
                    if (simple || vector->m_desc.encoded == false) {
                        m_winSequence[idx] = (key1 == key2) ? 0 : 1;
                    } else {
                        fcinfo.arg[0] = ScalarVector::Decode(key1);
                        fcinfo.arg[1] = ScalarVector::Decode(key2);

                        if (is_ord) {
                            fcinfo.flinfo = (m_ordeqfunctions + i);
                            eqfunc = m_ordeqfunctions[i].fn_addr;
                            m_winSequence[idx] = eqfunc(&fcinfo) ? 0 : 1;
                        } else {
                            fcinfo.flinfo = (m_parteqfunctions + i);
                            eqfunc = m_parteqfunctions[i].fn_addr;
                            m_winSequence[idx] = eqfunc(&fcinfo) ? 0 : 1;
                        }
                    }
                } else if (IS_NULL(flag1) && IS_NULL(flag2)) {
                    // not the same
                } else if (IS_NULL(flag1) || IS_NULL(flag2)) {
                    m_winSequence[idx] = 1;
                }
            }

            idx++;
            key1 = key2;
            flag1 = flag2;
        }
    }
}

/*
 * @Description: fetch agg values for result vector
 */
template <bool has_partition>
void VecWinAggRuntime::EvalWindowAgg()
{
    int naggs = m_aggNum;
    int start_idx = 0;
    int start_rows = 0;
    int k = 0;
    int n = 0;
    int agg_rows = 0; /* rows index */
    int batch_rows = m_currentBatch->m_rows;
    int rc = EOK;

    for (int i = 0; i < naggs; i++) {
        int agg_idx = m_aggIdx[i];
        WindowStatePerAgg peraggstate = &(m_winruntime->peragg[i]);
        int winfuncno = peraggstate->wfuncno;
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[winfuncno]);
        agg_rows = 0;

        /* means that there are remain data in last frame */
        bool isRemainData = (has_partition && m_window_store[winfuncno].restore);
        if (isRemainData) {
            start_idx = m_window_store[winfuncno].lastFetchIdx;
            start_rows = m_window_store[winfuncno].lastFrameRows;
            m_window_store[winfuncno].set(false, 0, 0);
        }

        perfuncstate->wfuncstate->m_resultVector->m_rows = 0;
        if (perfuncstate->wfuncstate->m_resultVector->m_buf != NULL) {
            perfuncstate->wfuncstate->m_resultVector->m_buf->Reset();
        }

        rc = memset_s(perfuncstate->wfuncstate->m_resultVector->m_flag,
            sizeof(uint8) * BatchMaxSize,
            0,
            sizeof(uint8) * BatchMaxSize);
        securec_check(rc, "\0", "\0");

        if (m_finalAggNum > 0 && agg_idx == m_finalAggInfo[k].idx) {
            n = k;
            k++;
        }

        /* there is one result if no partition */
        if (false == has_partition) {
            for (int m = 0; m < m_currentBatch->m_rows; m++)
                InvokeFp(m_buildScanBatch)(m_windowGrp[0], perfuncstate->wfuncstate->m_resultVector, agg_idx, n);
            return;
        }

        /*  there is no data in lefttree */
        if (m_noInput == true) {
            int rows = m_framrows[m_windowIdx];
            for (int m = start_rows; m < rows; m++) {
                agg_rows++;
                InvokeFp(m_buildScanBatch)(
                    m_windowGrp[m_windowIdx], perfuncstate->wfuncstate->m_resultVector, agg_idx, n);

                if (agg_rows == m_currentBatch->m_rows && m < rows - 1) {
                    m_window_store[winfuncno].set(true, m_windowIdx, m + 1);
                    break;
                }
            }
        } else {
            for (int j = start_idx; j < m_windowIdx; j++) {
                int n_rows = m_framrows[j];
                int m = 0;
                /* the number of rows to be fetched from current frame */
                int frame_calcu_rows = rtl::min(batch_rows - agg_rows, n_rows - start_rows);
                for (m = start_rows; m < frame_calcu_rows + start_rows; m++) {
                    agg_rows++;
                    InvokeFp(m_buildScanBatch)(m_windowGrp[j], perfuncstate->wfuncstate->m_resultVector, agg_idx, n);
                }

                /* current batch is full */
                if (agg_rows == batch_rows) {
                    /* current frame is not end */
                    if (m < n_rows) {
                        m_window_store[winfuncno].set(true, j, m);
                    } else {
                    /* still has frame to be fetch */
                        if (j < m_windowIdx - 1) {
                            m_window_store[winfuncno].set(true, j + 1, 0);
                        } else {
                            m_window_store[winfuncno].set(false, 0, 0);
                        }
                    }
                    break;
                }
                /* for a new frame, set start_rows to be 0 */
                start_rows = 0;
            }
        }
        Assert(agg_rows == batch_rows);
    }
}

/*
 * @Description: For more than one window funcs, to check window store log is consistent
 */
bool VecWinAggRuntime::CheckStoreValid()
{
    for (int i = 1; i < m_winFuns; i++) {
        if (m_window_store[0].lastFetchIdx != m_window_store[i].lastFetchIdx)
            return false;
        if (m_window_store[0].lastFrameRows != m_window_store[i].lastFrameRows)
            return false;
        if (m_window_store[0].restore != m_window_store[i].restore)
            return false;
    }
    return true;
}

/*
 * @Description: execute project batch for  agg + partition case
 */
template <bool has_partition>
const VectorBatch* VecWinAggRuntime::EvalWindow()
{
    ExprContext* econtext = NULL;
    int nfuns = m_winruntime->numfuncs;
    int i;
    VectorBatch* result_batch = NULL;
    int batch_rows;

    m_currentBatch->Reset(true);

    /* batch rows to be fetched */
    if (m_result_rows > BatchMaxSize) {
        batch_rows = BatchMaxSize;
        m_result_rows = m_result_rows - BatchMaxSize;
    } else {
        batch_rows = m_result_rows;
        m_result_rows = 0;
    }

    /* get batch from memory or temp file */
    m_batchstorestate->GetBatch(true, m_currentBatch, batch_rows);

    if (BatchIsNull(m_currentBatch)) {
        return NULL;
    }

    for (i = 0; i < nfuns; i++) {
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[i]);

        if (perfuncstate->plain_agg) {
            continue;
        }

        EvalWindowFunction(perfuncstate, i);
    }

    econtext = m_winruntime->ss.ps.ps_ExprContext;
    ResetExprContext(econtext);

    econtext->ecxt_outerbatch = m_currentBatch;

    /* eval window agg */
    EvalWindowAgg<true>();

    result_batch = ExecVecProject(m_winruntime->ss.ps.ps_ProjInfo);

    if (has_partition) {
        Assert(CheckStoreValid());
        /* no data in frame */
        if (m_window_store[0].lastFetchIdx == 0) {
            if (m_noInput == true && m_window_store[0].restore == false) {
                m_status = VA_END; /* the end */
            } else if (m_result_rows == 0) {
                int rc;
                m_windowGrp[0] = m_windowGrp[m_windowIdx];
                rc = memset_s(&m_windowGrp[1], BatchMaxSize * sizeof(hashCell*), 0, BatchMaxSize * sizeof(hashCell*));
                securec_check(rc, "\0", "\0");

                /* trim data until one partition read finish */
                batchstore_trim(m_batchstorestate, true);
                int n_rows = m_framrows[m_windowIdx];
                rc = memset_s(m_framrows, (BatchMaxSize + 1) * sizeof(int), 0, (BatchMaxSize + 1) * sizeof(int));
                securec_check(rc, "\0", "\0");
                m_windowIdx = 0;
                m_framrows[0] = n_rows;
                m_status = VA_FETCHBATCH;
            }
            /* fetch some data */
        } else {
            m_status = VA_EVALFUNCTION;
        }
    }
    return result_batch;
}

/*
 * @Description: get agg value for result vector(no avg).
 * @in hashCell: the address for instore agg value
 * @out result_vector: the vector to put result value
 * @in agg_idx: the index for agg number
 * @in final_idx: the index for agg number that include avg
 * @return - void
 */
void VecWinAggRuntime::BuildScanBatchSimple(hashCell* cell, ScalarVector* result_vector, int agg_idx, int final_idx)
{
    int nrows = result_vector->m_rows;
    ScalarVector* vector = result_vector;

    vector->m_vals[nrows] = cell->m_val[agg_idx].val;
    vector->m_flag[nrows] = cell->m_val[agg_idx].flag;
    vector->m_rows++;
}

/*
 * @Description: get agg value for result vector(include avg).
 * @in hashCell: the address for instore agg value
 * @out result_vector: the vector to put result value
 * @in agg_idx: the index for agg number
 * @in final_idx: the index for agg number that include avg
 * @return - void
 */
void VecWinAggRuntime::BuildScanBatchFinal(hashCell* cell, ScalarVector* result_vector, int agg_idx, int final_idx)
{
    int nrows = result_vector->m_rows;
    ScalarVector* vector = NULL;

    ExprContext* econtext = m_winruntime->ss.ps.ps_ExprContext;
    AutoContextSwitch mem_guard(econtext->ecxt_per_tuple_memory);
    vector = result_vector;

    if (agg_idx == m_finalAggInfo[final_idx].idx) {
        /* to invoke function*/
        FunctionCallInfo fcinfo = &m_finalAggInfo[final_idx].info->vec_final_function;
        fcinfo->arg[0] = (Datum)cell;
        fcinfo->arg[1] = (Datum)agg_idx;
        fcinfo->arg[2] = (Datum)(&vector->m_vals[nrows]);
        fcinfo->arg[3] = (Datum)(&vector->m_flag[nrows]);

        /*
         * for var final function , we must make sure the return val
         * has added pointer val header.
         */
        FunctionCallInvoke(fcinfo);
        vector->m_rows++;
    } else {
        vector->m_vals[nrows] = cell->m_val[agg_idx].val;
        vector->m_flag[nrows] = cell->m_val[agg_idx].flag;
        vector->m_rows++;
    }
}

/*
 * @Description: execute project batch for no agg
 */
const VectorBatch* VecWinAggRuntime::EvalPerBatch()
{
    ExprContext* econtext = NULL;
    int nfuns = m_winruntime->numfuncs;
    int i;
    VectorBatch* result_batch = NULL;

    /* no agg */
    Assert(m_winruntime->numaggs == 0);

    if (m_noInput == true)
        return NULL;

    for (i = 0; i < nfuns; i++) {
        WindowStatePerFunc perfuncstate = &(m_winruntime->perfunc[i]);

        if (perfuncstate->plain_agg)
            continue;

        /* eval window func */
        EvalWindowFunction(perfuncstate, i);
    }
    Assert(CheckStoreValid());

    econtext = m_winruntime->ss.ps.ps_ExprContext;
    ResetExprContext(econtext);
    econtext->ecxt_outerbatch = m_currentBatch;
    result_batch = ExecVecProject(m_winruntime->ss.ps.ps_ProjInfo);

    m_lastBatch->Reset(true);
    RefreshLastbatch(m_currentBatch);
    m_status = VA_FETCHBATCH;

    return result_batch;
}

void VecWinAggRuntime::EvalWindowFunction(WindowStatePerFunc perfuncstate, int idx)
{
    m_windowFunc[idx].arg[0] = (Datum)idx;
    m_windowFunc[idx].context = (Node*)perfuncstate->winobj;

    VecFunctionCallInvoke(&m_windowFunc[idx]);
}

bool VecWinAggRuntime::MatchPeerByOrder(VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2)
{
    return InvokeFp(m_MatchPeerOrd)(batch1, idx1, batch2, idx2, m_sortKey, m_sortKeyIdx);
}

bool VecWinAggRuntime::MatchPeerByPartition(VectorBatch* batch1, int idx1, VectorBatch* batch2, int idx2)
{
    return InvokeFp(m_MatchPeerPart)(batch1, idx1, batch2, idx2, m_partitionkey, m_partitionkeyIdx);
}

void VecWinAggRuntime::MatchSequenceByOrder(VectorBatch* batch, int start, int end)
{
    InvokeFp(m_MatchSeqOrd)(batch, start, end, m_sortKey, m_sortKeyIdx);
}

void VecWinAggRuntime::MatchSequenceByPartition(VectorBatch* batch, int start, int end)
{
    InvokeFp(m_MatchSeqPart)(batch, start, end, m_partitionkey, m_partitionkeyIdx);
}

/*
 * @Description: init aggregation info
 * @in aggInfo: the agg info
 * @return - void
 */
void VecWinAggRuntime::InitAggIdxInfo(VecAggInfo* aggInfo)
{
    int agg_idx = 0;
    for (int i = 0; i < m_aggNum; i++) {
        m_windowagg_idxinfo[i].aggIdx = agg_idx;

        if (aggInfo[i].vec_final_function.flinfo != NULL) {
            agg_idx += 2;
            m_windowagg_idxinfo[i].is_final = true;
        } else {
            agg_idx++;
            m_windowagg_idxinfo[i].is_final = false;
        }
    }
}
void VecWinAggRuntime::ReplaceEqfunc()
{
    for (int i = 0; i < m_partitionkey; i++) {
        switch (m_pkeyDesc[i].typeId) {
            case TIMETZOID:
                m_parteqfunctions[i].fn_addr = timetz_eq_withhead;
                break;
            case TINTERVALOID:
                m_parteqfunctions[i].fn_addr = tintervaleq_withhead;
                break;
            case INTERVALOID:
                m_parteqfunctions[i].fn_addr = interval_eq_withhead;
                break;
            case NAMEOID:
                m_parteqfunctions[i].fn_addr = nameeq_withhead;
                break;
            default:
                break;
        }
    }

    for (int i = 0; i < m_sortKey; i++) {
        switch (m_skeyDesc[i].typeId) {
            case TIMETZOID:
                m_ordeqfunctions[i].fn_addr = timetz_eq_withhead;
                break;
            case TINTERVALOID:
                m_ordeqfunctions[i].fn_addr = tintervaleq_withhead;
                break;
            case INTERVALOID:
                m_ordeqfunctions[i].fn_addr = interval_eq_withhead;
                break;
            case NAMEOID:
                m_ordeqfunctions[i].fn_addr = nameeq_withhead;
                break;
            default:
                break;
        }
    }
}
/*
 * row_number
 * just increment up from 1 until current partition finishes.
 */
ScalarVector* vwindow_row_number(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    int which_fn = PG_GETARG_INT32(0);
    VecWindowAggState* state = NULL;
    VecWinAggRuntime* win_runtime = NULL;
    ScalarVector* res_col = NULL;
    rownumber_context* context = NULL;
    int nvalue = 0;
    int frame_start_number = 0;
    int i, m;
    int batch_rows; /* current batch rows */
    int n_rows;     /* how many numbers in each windows */
    int group_window_num = 0;
    int start_idx = 0;
    int start_rows = 0;
    int frame_calcu_rows = 0;
    VectorBatch* pre_win_batch = NULL;

    state = (VecWindowAggState*)winobj->winstate;
    win_runtime = (VecWinAggRuntime*)state->VecWinAggRuntime;

    batch_rows = win_runtime->m_currentBatch->m_rows;
    group_window_num = win_runtime->m_windowIdx;
    pre_win_batch = win_runtime->m_lastBatch;

    res_col = state->perfunc[which_fn].wfuncstate->m_resultVector;
    context = (rownumber_context*)WinGetPartitionLocalMemory(winobj, sizeof(rownumber_context));

    /* means that there are remain data in last frame */
    if (win_runtime->m_window_store[which_fn].restore == true) {
        start_idx = win_runtime->m_window_store[which_fn].lastFetchIdx;
        start_rows = win_runtime->m_window_store[which_fn].lastFrameRows;
        win_runtime->m_window_store[which_fn].set(false, 0, 0);
    }

    /* not the first time */
    if (BatchIsNull(pre_win_batch) == false) {
        /* no agg */
        if (win_runtime->get_aggNum() == 0) {
            if (win_runtime->m_same_frame == false)
                context->rownumber = 0;
        } else if (start_rows == 0) {
            context->rownumber = 0;
        }
    }

    frame_start_number = context->rownumber;

    /*  there is no data in lefttree */
    if (win_runtime->m_noInput == true) {
        /* rows in curernt frame */
        n_rows = win_runtime->m_framrows[win_runtime->m_windowIdx];
        for (m = start_rows; m < n_rows; m++) {
            frame_start_number++;
            res_col->m_vals[nvalue] = frame_start_number;
            SET_NOTNULL(res_col->m_flag[nvalue]);
            nvalue++;

            /* current batch is full */
            if (nvalue == batch_rows && m != n_rows - 1) {
                win_runtime->m_window_store[which_fn].set(true, win_runtime->m_windowIdx, m + 1);
                context->rownumber = frame_start_number;
                break;
            }
        }
    } else {
        for (i = start_idx; i < group_window_num; i++) {
            n_rows = win_runtime->m_framrows[i];
            /* the number of rows to be fetched from current frame */
            frame_calcu_rows = rtl::min(batch_rows - nvalue, n_rows - start_rows);
            for (m = start_rows; m < frame_calcu_rows + start_rows; m++) {
                frame_start_number++;
                res_col->m_vals[nvalue] = frame_start_number;
                SET_NOTNULL(res_col->m_flag[nvalue]);
                nvalue++;
            }

            if (frame_calcu_rows + start_rows == n_rows && nvalue < batch_rows) {
                frame_start_number = 0;
                start_rows = 0;
            }

            /* current batch is full */
            if (nvalue == batch_rows) {
                /* current frame is not end */
                if (m < n_rows) {
                    win_runtime->m_window_store[which_fn].set(true, i, m);
                } else {
                    /* still has frame to be fetch */
                    if (i < group_window_num - 1) {
                        win_runtime->m_window_store[which_fn].set(true, i + 1, 0);
                    } else {
                        win_runtime->m_window_store[which_fn].set(false, 0, 0);
                    }
                }

                context->rownumber = frame_start_number;
                break;
            }
            /* for a new frame, set start_rows to be 0 */
            start_rows = 0;
        }
    }

    win_runtime->m_same_frame = false;

    Assert(nvalue <= BatchMaxSize);
    res_col->m_rows = nvalue;
    Assert(nvalue == batch_rows);

    return NULL;
}

/*
 * @Description: Rank changes when key columns change.
 *   The new rank number is the current row number.
 */
ScalarVector* vwindow_rank(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    int which_fn = PG_GETARG_INT32(0);
    VecWindowAggState* state = NULL;
    VecWinAggRuntime* win_runtime = NULL;
    ScalarVector* res_col = NULL;
    rank_context* context = NULL;
    int nvalue = 0;
    int frame_start_rank = 0;
    int frame_start_pos = 0;
    VectorBatch* cur_win_batch = NULL;
    VectorBatch* pre_win_batch = NULL;

    int i, j, m;
    int batch_rows = 0; /* current batch rows */
    /* the numbers of each frames */
    int n_rows = 0;
    int start_idx = 0;  /* the index of the frame */
    int start_rows = 0; /* the index of the rows */
    /* the rows will be fetched from current window */
    int frame_calcu_rows = 0;
    int group_window_num = 0; /* the number of frames */

    state = (VecWindowAggState*)winobj->winstate;
    win_runtime = (VecWinAggRuntime*)state->VecWinAggRuntime;

    group_window_num = win_runtime->m_windowIdx;
    pre_win_batch = win_runtime->m_lastBatch;
    cur_win_batch = win_runtime->m_currentBatch;

    res_col = state->perfunc[which_fn].wfuncstate->m_resultVector;
    batch_rows = win_runtime->m_currentBatch->m_rows;

    context = (rank_context*)WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

    /* means that there are remain data in last frame */
    if (win_runtime->m_window_store[which_fn].restore == true) {
        start_idx = win_runtime->m_window_store[which_fn].lastFetchIdx;
        start_rows = win_runtime->m_window_store[which_fn].lastFrameRows;
        win_runtime->m_window_store[which_fn].set(false, 0, 0);
    }

    /* not the first time */
    if (BatchIsNull(pre_win_batch) == false) {
        /* no aggregation */
        if (win_runtime->get_aggNum() == 0) {
            /* a new frame */
            if (win_runtime->m_same_frame == false) {
                context->rank = 1;
                state->currentpos = 1;
                frame_start_rank = 1;
                frame_start_pos = 1;
            } else {
                /* match the first row of current batch with the last row of last batch */
                if (win_runtime->MatchPeerByOrder(pre_win_batch, pre_win_batch->m_rows - 1, cur_win_batch, 0)) {
                    frame_start_rank = context->rank;
                    frame_start_pos = state->currentpos;
                } else {
                    frame_start_rank = state->currentpos;
                    frame_start_pos = state->currentpos;
                }
            }
        } else {
            /* a new frame */
            if (start_rows == 0) {
                frame_start_rank = 1;
                frame_start_pos = 1;
            } else if (win_runtime->MatchPeerByOrder(pre_win_batch, pre_win_batch->m_rows - 1, cur_win_batch, 0)) {
                frame_start_rank = context->rank;
                frame_start_pos = state->currentpos;
            } else {
                frame_start_rank = state->currentpos;
                frame_start_pos = state->currentpos;
            }
        }
    } else {
        /* the first case */
        frame_start_rank = 1;
        frame_start_pos = 1;
    }

    /*  there is no data in lefttree */
    if (win_runtime->m_noInput == true) {
        n_rows = win_runtime->m_framrows[win_runtime->m_windowIdx];
        /* the number of rows to be fetched from current frame */
        frame_calcu_rows = rtl::min(batch_rows - nvalue, n_rows - start_rows);
        /* match result if there is order */
        win_runtime->MatchSequenceByOrder(cur_win_batch, nvalue, frame_calcu_rows + nvalue - 1);
        /* the first rows */
        res_col->m_vals[nvalue] = frame_start_rank;
        SET_NOTNULL(res_col->m_flag[nvalue]);
        nvalue++;
        /* idx for array win_runtime->m_winSequence */
        int match_sequence_idx = 1;

        for (m = start_rows + 1; m < frame_calcu_rows + start_rows; m++) {
            /* a new order */
            if (win_runtime->m_winSequence[match_sequence_idx] == 1) {
                if (win_runtime->m_winSequence[match_sequence_idx - 1] == 0) {
                    /* belong to last frame */
                    frame_start_rank = frame_start_pos + 1;
                    res_col->m_vals[nvalue] = frame_start_rank;
                } else {
                    Assert(win_runtime->m_winSequence[match_sequence_idx - 1] == 1);
                    res_col->m_vals[nvalue] = ++frame_start_rank;
                }
            } else {
                Assert(win_runtime->m_winSequence[match_sequence_idx] == 0);
                res_col->m_vals[nvalue] = frame_start_rank;
            }
            SET_NOTNULL(res_col->m_flag[nvalue]);
            nvalue++;
            frame_start_pos++;
            match_sequence_idx++;
        }

        /* current batch is full */
        if (nvalue == batch_rows) {
            /* current frame is not end */
            if (m < n_rows) {
                win_runtime->m_window_store[which_fn].set(true, win_runtime->m_windowIdx, m);
                state->currentpos = frame_start_pos;
                context->rank = frame_start_rank;
            } else { /* the end */
                win_runtime->m_window_store[which_fn].set(false, 0, 0);
                context->rank = frame_start_rank;
                state->currentpos = ++frame_start_pos;
            }
        }
    }

    for (j = start_idx; j < group_window_num; j++) {
        n_rows = win_runtime->m_framrows[j];
        /* the number of rows to be fetched from current frame */
        frame_calcu_rows = rtl::min(batch_rows - nvalue, n_rows - start_rows);

        /* match result if there is order */
        win_runtime->MatchSequenceByOrder(cur_win_batch, nvalue, frame_calcu_rows + nvalue - 1);

        /* the first rows */
        res_col->m_vals[nvalue] = frame_start_rank;
        SET_NOTNULL(res_col->m_flag[nvalue]);
        nvalue++;

        /* idx for array win_runtime->m_winSequence */
        int match_sequence_idx = 1;
        for (i = start_rows + 1; i < frame_calcu_rows + start_rows; i++) {
            if (win_runtime->m_winSequence[match_sequence_idx] == 1) { /* a new order */
                if (win_runtime->m_winSequence[match_sequence_idx - 1] == 0) {
                    /* belong to last frame */
                    frame_start_rank = frame_start_pos + 1;
                    res_col->m_vals[nvalue] = frame_start_rank;
                } else {
                    Assert(win_runtime->m_winSequence[match_sequence_idx - 1] == 1);
                    res_col->m_vals[nvalue] = ++frame_start_rank;
                }
            } else {
                Assert(win_runtime->m_winSequence[match_sequence_idx] == 0);
                res_col->m_vals[nvalue] = frame_start_rank;
            }

            SET_NOTNULL(res_col->m_flag[nvalue]);
            nvalue++;
            frame_start_pos++;
            match_sequence_idx++;
        }

        /* we will exchange to a new partition */
        if (frame_calcu_rows + start_rows == n_rows && nvalue < batch_rows) {
            frame_start_rank = 1;
            frame_start_pos = 1;
        }

        /* current batch is full */
        if (nvalue == batch_rows) {
            if (i < n_rows) { /* current frame is not end */
                win_runtime->m_window_store[which_fn].set(true, j, i);
                state->currentpos = ++frame_start_pos;
                context->rank = frame_start_rank;
            } else {
                if (j < group_window_num - 1) { /* still has frame to be fetch */
                    win_runtime->m_window_store[which_fn].set(true, j + 1, 0);
                } else {
                    win_runtime->m_window_store[which_fn].set(false, 0, 0);
                }
                context->rank = frame_start_rank;
                state->currentpos = ++frame_start_pos;
            }
            break;
        }
        /* for a new frame, set start_rows to be 0 */
        start_rows = 0;
    }

    Assert(nvalue <= BatchMaxSize);
    Assert(nvalue == batch_rows);
    res_col->m_rows = nvalue;

    return NULL;
}

/*
 * @Description: Rank changes when key columns change.
 *   The new rank number is the current row number.
 */
ScalarVector* vwindow_denserank(PG_FUNCTION_ARGS)
{
    WindowObject winobj = PG_WINDOW_OBJECT();
    int which_fn = PG_GETARG_INT32(0);
    VecWindowAggState* state = NULL;
    VecWinAggRuntime* win_runtime = NULL;
    ScalarVector* res_col = NULL;
    rank_context* context = NULL;
    int nvalue = 0;
    int frame_start_rank = 0;
    VectorBatch* cur_win_batch = NULL;
    VectorBatch* pre_win_batch = NULL;

    int i, j;
    int batch_rows = 0; /* current batch rows */
    /* the numbers of each frames */
    int n_rows = 0;
    /* the rows will be fetched from current window */
    int frame_calcu_rows = 0;
    int group_window_num = 0; /* the number of frames */

    state = (VecWindowAggState*)winobj->winstate;
    win_runtime = (VecWinAggRuntime*)state->VecWinAggRuntime;

    group_window_num = win_runtime->m_windowIdx;
    pre_win_batch = win_runtime->m_lastBatch;
    cur_win_batch = win_runtime->m_currentBatch;

    res_col = state->perfunc[which_fn].wfuncstate->m_resultVector;
    batch_rows = win_runtime->m_currentBatch->m_rows;

    context = (rank_context*)WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

    /* not the first time */
    if (BatchIsNull(pre_win_batch) == false) {
        /* no aggregation */
        Assert(win_runtime->get_aggNum() == 0);

        if (win_runtime->m_same_frame == false) { /* a new frame */
            context->rank = 1;
            state->currentpos = 1;
            frame_start_rank = 1;
        } else {
            /* match the first row of current batch with the last row of last batch */
            if (win_runtime->MatchPeerByOrder(pre_win_batch, pre_win_batch->m_rows - 1, cur_win_batch, 0)) {
                frame_start_rank = context->rank;
            } else {
                frame_start_rank = state->currentpos;
            }
        }
    } else {
        /* the first case */
        frame_start_rank = 1;
    }

    for (j = 0; j < group_window_num; j++) {
        n_rows = win_runtime->m_framrows[j];
        /* the number of rows to be fetched from current frame */
        frame_calcu_rows = rtl::min(batch_rows - nvalue, n_rows);

        /* match result if there is order */
        win_runtime->MatchSequenceByOrder(cur_win_batch, nvalue, frame_calcu_rows + nvalue - 1);

        /* the first rows */
        res_col->m_vals[nvalue] = frame_start_rank;
        SET_NOTNULL(res_col->m_flag[nvalue]);
        nvalue++;

        /* idx for array win_runtime->m_winSequence */
        int match_sequence_idx = 1;
        for (i = 1; i < frame_calcu_rows; i++) {
            if (win_runtime->m_winSequence[match_sequence_idx] == 1) { /* a new order */
                res_col->m_vals[nvalue] = ++frame_start_rank;
            } else {
                res_col->m_vals[nvalue] = frame_start_rank;
            }

            SET_NOTNULL(res_col->m_flag[nvalue]);
            nvalue++;
            match_sequence_idx++;
        }

        /* we will exchange to a new partition */
        if (frame_calcu_rows == n_rows && nvalue < batch_rows) {
            frame_start_rank = 1;
        }
    }

    context->rank = frame_start_rank;
    state->currentpos = ++frame_start_rank;

    Assert(nvalue <= BatchMaxSize);
    Assert(nvalue == batch_rows);
    res_col->m_rows = nvalue;

    return NULL;
}
