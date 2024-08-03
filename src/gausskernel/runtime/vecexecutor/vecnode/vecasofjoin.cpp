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
 * vecasofjoin.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecasofjoin.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "codegen/vechashjoincodegen.h"

#include "catalog/pg_type.h"
#include "nodes/plannodes.h"
#include "access/nbtree.h"
#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vecasofjoin.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.inl"
#include "vectorsonic/vsonichash.h"
#include "postgres.h"
#include "parser/parse_oper.h"
#include "utils/rel_gs.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "commands/explain.h"
#include "utils/anls_opt.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "vecexecutor/vecexpression.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/memprot.h"
#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "pgxc/pgxc.h"
#endif

#define INSTR (m_runtime->js.ps.instrument)

#define BTLess ((char *)"<")
#define BTLessEqual ((char *)"<=")
#define BTGreaterEqual ((char *)">=")
#define BTGreater ((char *)">")
const uint8 CONSTNUM2 = 2;
const uint8 PARTMAXNUM = 32;

VecAsofJoinState *ExecInitVecAsofJoin(VecAsofJoin *node, EState *estate, int eflags)
{
    VecAsofJoinState *asof_state = NULL;
    Plan *outer_node = NULL;
    Plan *inner_node = NULL;
    List *lclauses = NIL;
    List *rclauses = NIL;
    List *lsclauses = NIL;
    List *rsclauses = NIL;
    List *hoperators = NIL;
    ListCell *l = NULL;
    ListCell *ls = NULL;
    FmgrInfo *eqfunctions = NULL;
    ListCell *ho = NULL;
    int i;
    int key_num;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    asof_state = makeNode(VecAsofJoinState);
    asof_state->js.ps.plan = (Plan *)node;
    asof_state->js.ps.state = estate;
    asof_state->js.ps.vectorized = true;
    asof_state->hashTbl = NULL;
    asof_state->cmpName = NULL;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &asof_state->js.ps);

    /*
     * initialize child expressions
     */
    asof_state->js.ps.targetlist = (List *)ExecInitVecExpr((Expr *)node->join.plan.targetlist, (PlanState *)asof_state);
    asof_state->js.ps.qual = (List *)ExecInitVecExpr((Expr *)node->join.plan.qual, (PlanState *)asof_state);
    asof_state->js.jointype = node->join.jointype;
    asof_state->js.joinqual = (List *)ExecInitVecExpr((Expr *)node->join.joinqual, (PlanState *)asof_state);
    asof_state->js.nulleqqual = (List *)ExecInitVecExpr((Expr *)node->join.nulleqqual, (PlanState *)asof_state);
    asof_state->hashclauses = (List *)ExecInitVecExpr((Expr *)node->hashclauses, (PlanState *)asof_state);

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    outer_node = outerPlan(node);
    inner_node = innerPlan(node);

    outerPlanState(asof_state) = ExecInitNode(outer_node, estate, eflags);
    innerPlanState(asof_state) = ExecInitNode(inner_node, estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &asof_state->js.ps);

    /*
     * initialize tuple type and projection info
     * result tupleSlot only contains virtual tuple, so the default
     * tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&asof_state->js.ps);

    if (asof_state->js.ps.targetlist) {
        asof_state->js.ps.ps_ProjInfo =
            ExecBuildVecProjectionInfo(asof_state->js.ps.targetlist, node->join.plan.qual,
                                       asof_state->js.ps.ps_ExprContext, asof_state->js.ps.ps_ResultTupleSlot, NULL);

        ExecAssignVectorForExprEval(asof_state->js.ps.ps_ProjInfo->pi_exprContext);
    } else {
        ExecAssignVectorForExprEval(asof_state->js.ps.ps_ExprContext);

        asof_state->js.ps.ps_ProjInfo = NULL;
    }

    lclauses = NIL;
    rclauses = NIL;
    hoperators = NIL;
    foreach (l, asof_state->hashclauses) {
        FuncExprState *fstate = (FuncExprState *)lfirst(l);
        OpExpr *hclause = NULL;

        Assert(IsA(fstate, FuncExprState));
        hclause = (OpExpr *)fstate->xprstate.expr;
        Assert(IsA(hclause, OpExpr));
        lclauses = lappend(lclauses, linitial(fstate->args));
        rclauses = lappend(rclauses, lsecond(fstate->args));
        hoperators = lappend_oid(hoperators, hclause->opno);
    }

    lsclauses = NIL;
    rsclauses = NIL;
    foreach (ls, asof_state->js.joinqual) {
        FuncExprState *fstate = (FuncExprState *)lfirst(ls);
        OpExpr *hclause = NULL;

        Assert(IsA(fstate, FuncExprState));
        hclause = (OpExpr *)fstate->xprstate.expr;
        Assert(IsA(hclause, OpExpr));
        asof_state->cmpName = get_opname(hclause->opno);
        lsclauses = lappend(lsclauses, linitial(fstate->args));
        rsclauses = lappend(rsclauses, lsecond(fstate->args));
        break;
    }

    key_num = list_length(rclauses);
    eqfunctions = (FmgrInfo *)palloc(key_num * sizeof(FmgrInfo));
    i = 0;
    foreach (ho, hoperators) {
        Oid hashop = lfirst_oid(ho);
        Oid eq_function;

        eq_function = get_opcode(hashop);
        fmgr_info(eq_function, &eqfunctions[i]);
        i++;
    }

    asof_state->hj_OuterHashKeys = lclauses;
    asof_state->hj_InnerHashKeys = rclauses;
    asof_state->hj_HashOperators = hoperators;
    asof_state->eqfunctions = eqfunctions;
    asof_state->hj_OuterSortKeys = lsclauses;
    asof_state->hj_InnerSortKeys = rsclauses;

    return asof_state;
}

VectorBatch *ExecVecAsofJoin(VecAsofJoinState *node)
{
    int64 rows = 0;

    for (;;) {
        switch (node->joinState) {
            case ASOF_PART: {
                if (node->hashTbl == NULL)
                    node->hashTbl = New(CurrentMemoryContext) AsofHashJoin(INIT_DATUM_ARRAY_SIZE, node);

                ((AsofHashJoin *)(node->hashTbl))->BuildInner();
                rows = ((AsofHashJoin *)(node->hashTbl))->getRows();

                /* Early free right tree after hash table built */
                ExecEarlyFree(innerPlanState(node));

                EARLY_FREE_LOG(elog(LOG,
                                    "Early Free: Hash Table for %sHashJoin"
                                    " is built at node %d, memory used %d MB.",
                                    JOIN_NAME, (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));

                if (0 == rows) {
                    // When hash table size is zero, no need to fetch left tree any more and
                    // should deinit the consumer in left tree earlier.
                    //
                    ExecEarlyDeinitConsumer((PlanState *)node);
                    return NULL;
                }

                ((AsofHashJoin *)(node->hashTbl))->BuildOuter();

                /* Early free left tree after hash table built */
                ExecEarlyFree(outerPlanState(node));

                EARLY_FREE_LOG(elog(LOG,
                                    "Early Free: Hash Table for %sHashJoin"
                                    " is built at node %d, memory used %d MB.",
                                    JOIN_NAME, (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));

                node->joinState = ASOF_MERGE;
            } break;

            case ASOF_MERGE: {
                instr_time start_time;
                (void)INSTR_TIME_SET_CURRENT(start_time);
                VectorBatch *result = NULL;

                result = ((AsofHashJoin *)(node->hashTbl))->Probe();
                ((AsofHashJoin *)(node->hashTbl))->m_probe_time += elapsed_time(&start_time);

                return result;
            }
            default:
                break;
        }
    }
}

void ExecEndVecAsofJoin(VecAsofJoinState *node)
{
    void *tbl = node->hashTbl;

    if (tbl != NULL) {
        AsofHashJoin *hj_tbl = (AsofHashJoin *)tbl;
        hj_tbl->freeMemoryContext();
    }

    ExecFreeExprContext(&node->js.ps);
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

/*
 * @Description: radix hash join constructor.
 * 	In hash join constructor, hashContext manages hash table and partition.
 * 	Other variables are under hash join node context.
 */
AsofHashJoin::AsofHashJoin(int size, VecAsofJoinState *node)
    : SonicHash(size),
      m_complicatekey(false),
      m_runtime(node),
      m_matchPartIdx(0),
      m_spillCount(0),
      m_spillSize(0),
      m_partNum(0),
      m_probeIdx(0),
      m_build_time(0.0),
      m_probe_time(0.0)
{
    AddControlMemoryContext(m_runtime->js.ps.instrument, m_memControl.hashContext);
    m_compareValue = comparisonValue(node->cmpName);
    m_buildOp.tupleDesc = outerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    m_probeOp.tupleDesc = innerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    m_buildSortOp.tupleDesc = m_buildOp.tupleDesc;
    m_probeSortOp.tupleDesc = m_probeOp.tupleDesc;
    m_buildOp.cols = m_buildOp.tupleDesc->natts;
    m_probeOp.cols = m_probeOp.tupleDesc->natts;

    m_buildOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
    m_probeOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
    m_lastBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
    m_eqfunctions = m_runtime->eqfunctions;

    /* init hash key index */
    m_buildOp.keyNum = list_length(m_runtime->hj_OuterHashKeys);
    m_probeOp.keyNum = m_buildOp.keyNum;
    m_buildOp.keyIndx = (uint16 *)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.keyIndx = (uint16 *)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_buildOp.oKeyIndx = (uint16 *)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.oKeyIndx = (uint16 *)palloc0(sizeof(uint16) * m_buildOp.keyNum);

    m_integertype = (bool *)palloc(sizeof(bool) * m_buildOp.keyNum);
    for (int i = 0; i < m_buildOp.keyNum; i++) {
        m_integertype[i] = true;
    }

    setHashIndex(m_buildOp.keyIndx, m_buildOp.oKeyIndx, m_runtime->hj_OuterHashKeys);
    setHashIndex(m_probeOp.keyIndx, m_probeOp.oKeyIndx, m_runtime->hj_InnerHashKeys);
    /* init sort key index */
    m_buildSortOp.numsortkeys = list_length(m_runtime->hj_OuterSortKeys) + m_buildOp.keyNum;
    m_probeSortOp.numsortkeys = m_buildSortOp.numsortkeys;
    m_buildSortOp.sortColIdx = (AttrNumber *)palloc0(sizeof(AttrNumber) * m_buildSortOp.numsortkeys);
    m_probeSortOp.sortColIdx = (AttrNumber *)palloc0(sizeof(AttrNumber) * m_buildSortOp.numsortkeys);
    m_buildSortOp.sortOperators = (Oid *)palloc0(sizeof(Oid) * m_buildSortOp.numsortkeys);
    m_probeSortOp.sortOperators = (Oid *)palloc0(sizeof(Oid) * m_buildSortOp.numsortkeys);
    m_buildSortOp.collations = (Oid *)palloc0(sizeof(Oid) * m_buildSortOp.numsortkeys);
    m_probeSortOp.collations = (Oid *)palloc0(sizeof(Oid) * m_buildSortOp.numsortkeys);
    m_buildSortOp.nullsFirst = (bool *)palloc0(sizeof(bool) * m_buildSortOp.numsortkeys);
    m_probeSortOp.nullsFirst = (bool *)palloc0(sizeof(bool) * m_buildSortOp.numsortkeys);

    setSortIndex(m_buildSortOp.sortColIdx, m_buildSortOp.collations, m_buildSortOp.nullsFirst,
                 m_buildSortOp.sortOperators, m_runtime->hj_OuterHashKeys, m_runtime->hj_OuterSortKeys,
                 BTLessStrategyNumber);

    setSortIndex(m_probeSortOp.sortColIdx, m_probeSortOp.collations, m_probeSortOp.nullsFirst,
                 m_probeSortOp.sortOperators, m_runtime->hj_InnerHashKeys, m_runtime->hj_InnerSortKeys,
                 BTLessStrategyNumber);

    initMemoryControl();

    m_memControl.hashContext = AllocSetContextCreate(CurrentMemoryContext, "AsofHashJoinContext",
                                                     ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                     ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT, m_memControl.totalMem);

    /* init hash functions */
    m_buildOp.hashFunc = (hashValFun *)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_buildOp.hashAtomFunc = (hashValFun *)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashFunc = (hashValFun *)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashAtomFunc = NULL;

    bindingFp();

    initHashFmgr();

    initPartitions();

    replaceEqFunc();

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t ret = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(ret, "\0", "\0");
    }
}

/*
 * @Description: Binding some execution functions.
 */
void AsofHashJoin::bindingFp()
{
    initHashFunc(m_buildOp.tupleDesc, (void *)m_buildOp.hashFunc, m_buildOp.keyIndx, false);
    initHashFunc(m_buildOp.tupleDesc, (void *)m_buildOp.hashAtomFunc, m_buildOp.keyIndx, true);
    initHashFunc(m_probeOp.tupleDesc, (void *)m_probeOp.hashFunc, m_probeOp.keyIndx, false);
}

/*
 * @Description: Compute hash key index and check whether the hashkey is simple type.
 * @out keyIndx - Record build side hash key attr number.
 * @out oKeyIndx - Record build side hash key origin attr number.
 * @in hashkeys - keyIndx, oKeyIndx should be allocated by caller.
 */
void AsofHashJoin::setHashIndex(uint16 *keyIndx, uint16 *oKeyIndx, List *hashKeys)
{
    int i = 0;
    ListCell *lc = NULL;
    ExprState *expr_state = NULL;
    Var *variable = NULL;

    foreach (lc, hashKeys) {
        expr_state = (ExprState *)lfirst(lc);
        if (IsA(expr_state->expr, Var)) {
            variable = (Var *)expr_state->expr;
        } else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType *rel_type = (RelabelType *)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var *)rel_type->arg)->varattno > 0) {
                variable = (Var *)((RelabelType *)expr_state->expr)->arg;
            } else {
                m_complicatekey = true;
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not support complicate key")));
                break;
            }
        } else {
            m_complicatekey = true;
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not support complicate key")));
            break;
        }

        keyIndx[i] = variable->varattno - 1;
        oKeyIndx[i] = variable->varoattno - 1;
        m_integertype[i] = (unsigned int)(m_integertype[i]) & (unsigned int)integerType(variable->vartype);
        i++;
    }
}

/*
 * @Description: Compute sort key index .
 * @out keyIndx - Record build side sort key attr number.
 * @out keyCollations - Record build side  sort key collation .
 * @out nullsFirstFlags - Record build side sort null first.
 * @out opIndx - Record build side sort key variable.
 * @in sortkeys - keyIndx, varIndx should be allocated by caller.
 * @in sortStrategy  sort direction (ASC or DESC)
 */
void AsofHashJoin::setSortIndex(AttrNumber *keyIndx, Oid *keyCollations, bool *nullsFirstFlags, Oid *opIndx,
                                List *hashKeys, List *sortKeys, int sortStrategy)
{
    int i = 0;
    ListCell *lc = NULL;
    ExprState *expr_state = NULL;
    Var *variable = NULL;
    Var *ovariable = NULL;
    Oid sortop;

    foreach (lc, hashKeys) {
        expr_state = (ExprState *)lfirst(lc);
        if (IsA(expr_state->expr, Var)) {
            variable = (Var *)expr_state->expr;
        } else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType *rel_type = (RelabelType *)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var *)rel_type->arg)->varattno > 0) {
                variable = (Var *)((RelabelType *)expr_state->expr)->arg;
            }
        } else {
            Assert(false);
        }

        if (sortStrategy == BTLessStrategyNumber) {
            get_sort_group_operators(variable->vartype, true, false, false, &sortop, NULL, NULL, NULL);
        } else {
            get_sort_group_operators(variable->vartype, false, false, true, NULL, NULL, &sortop, NULL);
        }

        keyIndx[i] = variable->varattno;
        keyCollations[i] = variable->varcollid;
        nullsFirstFlags[i] = false;
        opIndx[i] = sortop;

        i++;
    }

    lc = NULL;
    foreach (lc, sortKeys) {
        expr_state = (ExprState *)lfirst(lc);
        if (IsA(expr_state->expr, Var)) {
            variable = (Var *)expr_state->expr;
        } else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType *rel_type = (RelabelType *)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var *)rel_type->arg)->varattno > 0) {
                variable = (Var *)((RelabelType *)expr_state->expr)->arg;
            }
        } else {
            Assert(false);
        }
        if (sortStrategy == BTLessStrategyNumber) {
            get_sort_group_operators(variable->vartype, true, false, false, &sortop, NULL, NULL, NULL);
        } else {
            get_sort_group_operators(variable->vartype, false, false, true, NULL, NULL, &sortop, NULL);
        }

        keyIndx[i] = variable->varattno;
        keyCollations[i] = variable->varcollid;
        nullsFirstFlags[i] = false;
        opIndx[i] = sortop;

        i++;
    }
}

/*
 * @Description: Initial memory control information.
 */
void AsofHashJoin::initMemoryControl()
{
    VecHashJoin *node = (VecHashJoin *)m_runtime->js.ps.plan;

    m_memControl.totalMem = SET_NODEMEM(((Plan *)node)->operatorMemKB[0], ((Plan *)node)->dop) * 1024L;
    if (((Plan *)node)->operatorMaxMem > 0) {
        m_memControl.maxMem = SET_NODEMEM(((Plan *)node)->operatorMaxMem, ((Plan *)node)->dop) * 1024L;
    }

    elog(DEBUG2, "AsofHashJoinTbl[%d]: operator memory uses %dKB", ((Plan *)node)->plan_node_id,
         (int)(m_memControl.totalMem / 1024L));
}

/*
 * @Description: Initial hash functions info from m_runtime.
 * 	Should be under hashjoin hashContext.
 */
void AsofHashJoin::initHashFmgr()
{
    ListCell *lc = NULL;
    int i = 0;
    m_buildOp.hashFmgr = (FmgrInfo *)palloc(sizeof(FmgrInfo) * m_buildOp.keyNum);
    m_probeOp.hashFmgr = (FmgrInfo *)palloc(sizeof(FmgrInfo) * m_probeOp.keyNum);
    foreach (lc, m_runtime->hj_HashOperators) {
        Oid hashop = lfirst_oid(lc);
        Oid left_hashfn;
        Oid right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmodule(MOD_VEC_EXECUTOR),
                            errmsg("could not find hash function for hash operator %u", hashop)));
        }
        fmgr_info(left_hashfn, &m_probeOp.hashFmgr[i]);
        fmgr_info(right_hashfn, &m_buildOp.hashFmgr[i]);
        i++;
    }

    if (u_sess->attr.attr_sql.enable_fast_numeric) {
        replace_numeric_hash_to_bi(i, m_probeOp.hashFmgr);
        replace_numeric_hash_to_bi(i, m_buildOp.hashFmgr);
    }
}

/*
 * @Description: Build side main function.
 */
void AsofHashJoin::BuildInner()
{
    PlanState *node = innerPlanState(m_runtime);
    VectorBatch *batch = NULL;
    instr_time start_time;

    for (;;) {
        batch = VectorEngine(node);
        if (unlikely(BatchIsNull(batch))) {

            break;
        }
        (void)INSTR_TIME_SET_CURRENT(start_time);
        partSort(batch, false);
        m_rows += batch->m_rows;
        m_build_time += elapsed_time(&start_time);
    }
}

/*
 * @Description: Build side main function.
 */
void AsofHashJoin::BuildOuter()
{
    PlanState *node = outerPlanState(m_runtime);
    VectorBatch *batch = NULL;
    instr_time start_time;

    for (;;) {
        batch = VectorEngine(node);
        if (unlikely(BatchIsNull(batch))) {
            break;
        }

        (void)INSTR_TIME_SET_CURRENT(start_time);

        partSort(batch, true);

        m_rows += batch->m_rows;
        m_build_time += elapsed_time(&start_time);
    }

    if (HAS_INSTR(&m_runtime->js, true)) {
        calcHashContextSize(m_memControl.hashContext, &m_memControl.allocatedMem, &m_memControl.availMem);
        INSTR->sysBusy = m_memControl.sysBusy;
        INSTR->spreadNum = m_memControl.spreadNum;
        INSTR->sorthashinfo.hashbuild_time = m_build_time;
        INSTR->sorthashinfo.spaceUsed = m_memControl.allocatedMem - m_memControl.availMem;
    }
}

/*
 * @Description: save the data to memory.
 * @in batch - Put the data in batch to momory.
 */
void AsofHashJoin::partSort(VectorBatch *batch, bool isBuildOp)
{
    int rows = batch->m_rows;
    uint32 *part_idx = NULL;
    uint32 *key_idx = NULL;
    SonicHashInputOpAttr hashOp = isBuildOp ? m_buildOp : m_probeOp;
    SortInputOpAttr sortOp = isBuildOp ? m_buildSortOp : m_probeSortOp;
    List *hashKeys = isBuildOp ? m_runtime->hj_OuterHashKeys : m_runtime->hj_InnerHashKeys;

    /* First, calclute the hashVal of each row */

    hashBatchArray(batch, (void *)hashOp.hashFunc, hashOp.hashFmgr, hashOp.keyIndx, m_hashVal);

    /* Get partition number for hash value. */
    calcPartIdx(m_hashVal, m_partNum, rows);

    part_idx = m_partIdx;
    SonicSortPartition *part = NULL;
    VecHashJoin *node = (VecHashJoin *)m_runtime->js.ps.plan;
    int row_idx = 0;
    for (row_idx = 0; row_idx < rows; row_idx++, part_idx++) {
        part = isBuildOp ? (SonicSortPartition *)m_innerPartitions[*part_idx]
                         : (SonicSortPartition *)m_outerPartitions[*part_idx];
        Assert(part != NULL);
        if (!part->m_sortState) {
            MemoryContext old_cxt = MemoryContextSwitchTo(part->m_context);
            part->m_sortState = batchsort_begin_heap(
                sortOp.tupleDesc, sortOp.numsortkeys, sortOp.sortColIdx, sortOp.sortOperators, sortOp.collations,
                sortOp.nullsFirst, m_memControl.totalMem / (m_partNum * 1024L), false,
                m_memControl.maxMem / (m_partNum * 1024L), ((Plan *)node)->dop, SET_DOP(((Plan *)node)->dop));

            part->m_sortState->jitted_CompareMultiColumn = NULL;
            part->m_sortState->jitted_CompareMultiColumn_TOPN = NULL;

            (void)MemoryContextSwitchTo(old_cxt);
        }
        bool is_null = false;
        int i = 0;
        for (i = 0; i < hashOp.keyNum; i++) {
            if (IS_NULL(batch->m_arr[hashOp.keyIndx[i]].m_flag[row_idx])) {
                is_null = true;
            }
        }
        if (!is_null) {
            part->m_sortState->sort_putbatch(part->m_sortState, batch, row_idx, row_idx + 1);
            part->m_rows++;
            if (!isBuildOp) {
                m_probeIdx++;
            }
            /* sql active feature */
            if (part->m_sortState->m_tapeset) {
                long currentFileBlocks = LogicalTapeSetBlocks(part->m_sortState->m_tapeset);
                int64 spill_size = (int64)(currentFileBlocks - part->m_sortState->m_lastFileBlocks);
                m_spillSize += spill_size * (BLCKSZ);
                m_spillCount += 1;
                pgstat_increase_session_spill_size(spill_size * (BLCKSZ));
                part->m_sortState->m_lastFileBlocks = currentFileBlocks;
                if (HAS_INSTR(&m_runtime->js, true)) {
                    INSTR->sorthashinfo.spill_size += m_spillSize;
                    if (isBuildOp) {
                        INSTR->sorthashinfo.spill_innerSize += m_spillSize;
                        if (!part->m_isSpill) {
                            INSTR->sorthashinfo.spill_innerPartNum += 1;
                            part->m_isSpill = true;
                        }
                    } else {
                        INSTR->sorthashinfo.spill_outerSize += m_spillSize;
                        if (!part->m_isSpill) {
                            INSTR->sorthashinfo.spill_outerPartNum += 1;
                            part->m_isSpill = true;
                        }
                    }
                }
            }
        }
    }
}

/*
 * @Description: Fine one file partition pair can join.
 * @return - false when cannot get any pair because all partitions are finished searching.
 */
VectorBatch *AsofHashJoin::Probe()
{
    EState *estate = NULL;
    uint32 idx = m_matchPartIdx; /* offset of the partition to load */
    uint32 matchIdx = 0;

    /*
     * Search for a valid partition that can be fit in the memory.
     * If an inner partition is too large for the current memory,
     * repartition the inner partition and the coresponding outer partition,
     * then search for the next, until we found a vaild one.
     */
    for (; idx < m_partNum; ++idx) {
        SonicSortPartition *buildPart = (SonicSortPartition *)m_outerPartitions[idx];
        SonicSortPartition *probePart = (SonicSortPartition *)m_innerPartitions[idx];
        Assert(buildPart != NULL);
        Assert(probePart != NULL);
        if (probePart->m_rows == 0 || buildPart->m_rows == 0) {
            finishJoinPartition(idx);
            continue;
        }

        if (probePart->m_data == NULL && probePart->m_sortState != NULL) {
            MemoryContext old_cxt = MemoryContextSwitchTo(probePart->m_context);
            probePart->m_data = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
            (void)MemoryContextSwitchTo(old_cxt);

            batchsort_performsort(probePart->m_sortState);
            if (probePart->m_sortState->m_tapeset) {
                long currentFileBlocks = LogicalTapeSetBlocks(probePart->m_sortState->m_tapeset);
                int64 spill_size = (int64)(currentFileBlocks - probePart->m_sortState->m_lastFileBlocks);
                m_spillSize += spill_size * (BLCKSZ);
                m_spillCount += 1;
                pgstat_increase_session_spill_size(spill_size * (BLCKSZ));
                probePart->m_sortState->m_lastFileBlocks = currentFileBlocks;
                if (HAS_INSTR(&m_runtime->js, true)) {
                    INSTR->sorthashinfo.spill_size += m_spillSize;
                    INSTR->sorthashinfo.spill_outerSize += m_spillSize;
                }
            }

            batchsort_getbatch(probePart->m_sortState, true, probePart->m_data);
            probePart->m_fetchCount += probePart->m_data->m_rows;
        }

        if (buildPart->m_data == NULL && buildPart->m_sortState != NULL) {
            MemoryContext old_cxt = MemoryContextSwitchTo(buildPart->m_context);
            buildPart->m_data = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
            (void)MemoryContextSwitchTo(old_cxt);

            batchsort_performsort(buildPart->m_sortState);
            if (buildPart->m_sortState->m_tapeset) {
                long currentFileBlocks = LogicalTapeSetBlocks(buildPart->m_sortState->m_tapeset);
                int64 spill_size = (int64)(currentFileBlocks - buildPart->m_sortState->m_lastFileBlocks);
                m_spillSize += spill_size * (BLCKSZ);
                m_spillCount += 1;
                pgstat_increase_session_spill_size(spill_size * (BLCKSZ));
                buildPart->m_sortState->m_lastFileBlocks = currentFileBlocks;
                if (HAS_INSTR(&m_runtime->js, true)) {
                    INSTR->sorthashinfo.spill_size += m_spillSize;
                    INSTR->sorthashinfo.spill_innerSize += m_spillSize;
                }
            }

            batchsort_getbatch(buildPart->m_sortState, true, buildPart->m_data);
            buildPart->m_fetchCount += buildPart->m_data->m_rows;
        }

        for (; probePart->m_cmpIdx < probePart->m_rows; ++probePart->m_cmpIdx) {
            if (buildPart->m_cmpIdx < buildPart->m_rows) {
                //	If right > left, then there is no match
                if (!CompareSortColumn(buildPart, probePart, false)) {
                    continue;
                }
            }

            // Exponential search forward for a non-matching value

            int64 bound = 1;
            int64 begin = buildPart->m_cmpIdx;
            buildPart->m_cmpIdx = begin + bound;
            while (buildPart->m_cmpIdx < buildPart->m_rows) {
                if (CompareSortColumn(buildPart, probePart, false)) {
                    //	If right <= left, jump ahead
                    bound *= CONSTNUM2;
                    buildPart->m_cmpIdx = begin + bound;
                } else {
                    break;
                }
            }

            //	Binary search for the first non-matching value
            int64 first = begin + bound / 2;
            int64 last = Min(begin + bound, buildPart->m_rows);
            while (first < last) {
                const int64 mid = first + (last - first) / 2;
                buildPart->m_cmpIdx = mid;
                if (CompareSortColumn(buildPart, probePart, false)) {
                    first = mid + 1;
                } else {
                    last = mid;
                }
            }
            buildPart->m_cmpIdx = --first;
            // check is the part key equal ?
            if (!CompareSortColumn(buildPart, probePart, true)) {
                continue;
            }
            // copy row to output
            CopyRow(buildPart, probePart);
            matchIdx++;
            if (matchIdx == BatchMaxSize) {
                m_buildOp.batch->FixRowCount(BatchMaxSize);
                m_probeOp.batch->FixRowCount(BatchMaxSize);
                ++probePart->m_cmpIdx;
                m_matchPartIdx = idx;
                return buildRes(m_probeOp.batch, m_buildOp.batch);
            }
        }

        finishJoinPartition(idx);
    }
    m_buildOp.batch->FixRowCount(matchIdx);
    m_probeOp.batch->FixRowCount(matchIdx);
    m_partNum = 0;
    if (m_buildOp.batch->m_rows != 0) {
        return buildRes(m_probeOp.batch, m_buildOp.batch);
    } else {
        return NULL;
    }
}

/*
 * Inline-able copy of FunctionCall2Coll() to save some cycles in sorting.
 */
static inline Datum myFunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, CONSTNUM2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));

    return result;
}

/*
 * Apply a sort function (by now converted to fmgr lookup form)
 * and return a 3-way comparison result.  This takes care of handling
 * reverse-sort and NULLs-ordering properly.  We assume that DESC and
 * NULLS_FIRST options are encoded in sk_flags the same way btree does it.
 */
static inline int32 inlineApplySortFunction(FmgrInfo *sortFunction, uint32 sk_flags, Oid collation, Datum datum1,
                                            bool isNull1, Datum datum2, bool isNull2)
{
    int32 compare = 0;

    if (!isNull1 && !isNull2) {
        if (sortFunction->fn_addr == NULL) {
            if (datum1 < datum2)
                compare = -1;
            else if (datum1 == datum2)
                compare = 0;
            else if (datum1 > datum2)
                compare = 1;
        } else
            compare = DatumGetInt32(myFunctionCall2Coll(sortFunction, collation, datum1, datum2));

        if (sk_flags & SK_BT_DESC)
            compare = -compare;
    } else if (isNull1) {
        if (isNull2)
            compare = 0; /* NULL "=" NULL */
        else if (sk_flags & SK_BT_NULLS_FIRST)
            compare = -1; /* NULL "<" NOT_NULL */
        else
            compare = 1; /* NULL ">" NOT_NULL */
    } else if (isNull2) {
        if (sk_flags & SK_BT_NULLS_FIRST)
            compare = 1; /* NOT_NULL ">" NULL */
        else
            compare = -1; /* NOT_NULL "<" NULL */
    }
    return compare;
}

/*
 * @Description: compare two part by sort key
 * @in build - build part.
 * @in probe - probe part.
 * @in check_part - check  part key.
 */
bool AsofHashJoin::CompareSortColumn(SonicSortPartition *build, SonicSortPartition *probe, bool check_part)
{
    Batchsortstate *buildState = build->m_sortState;
    Batchsortstate *probeState = probe->m_sortState;

    Assert(buildState->m_nKeys == probeState->m_nKeys);

    VectorBatch *buildData = build->m_data;
    VectorBatch *probeData = probe->m_data;

    ScanKey buildScanKey = buildState->m_scanKeys;
    ScanKey probeScanKey = probeState->m_scanKeys;

    int64 buildIdx = 0;
    int64 probeIdx = 0;

    if (build->m_cmpIdx >= build->m_fetchCount) {
        m_lastBatch->Reset();
        m_lastBatch->Copy<true, true>(buildData);
        batchsort_getbatch(buildState, true, buildData);
        build->m_fetchCount += buildData->m_rows;
    }

    buildIdx = buildData->m_rows - (build->m_fetchCount - build->m_cmpIdx);
    if (buildIdx < 0) {
        buildData = m_lastBatch;
        buildIdx = m_lastBatch->m_rows + buildIdx;
    }

    if (probe->m_cmpIdx >= probe->m_fetchCount) {
        batchsort_getbatch(probeState, true, probeData);
        probe->m_fetchCount += probeData->m_rows;
    }
    probeIdx = probeData->m_rows - (probe->m_fetchCount - probe->m_cmpIdx);

    Assert(probeIdx >= 0);
    Assert(buildIdx >= 0);
    int nkey;
    int32 compare = 0;
    ScalarValue datum1, datum2;
    ScalarValue datum1Tmp, datum2Tmp;
    bool isnull1 = false;
    bool isnull2 = false;

    CHECK_FOR_INTERRUPTS();

    int buildColIdx;
    int probeColIdx;

    for (nkey = 0; nkey < buildState->m_nKeys; ++nkey, ++buildScanKey, ++probeScanKey) {
        buildColIdx = buildScanKey->sk_attno - 1;
        probeColIdx = probeScanKey->sk_attno - 1;

        Assert(buildColIdx >= 0);
        Assert(probeColIdx >= 0);

        Form_pg_attribute buildAttr = &buildState->tupDesc->attrs[buildColIdx];
        Form_pg_attribute probeAttr = &probeState->tupDesc->attrs[probeColIdx];

        Assert(buildAttr->atttypid == probeAttr->atttypid);

        Oid typeOid = buildAttr->atttypid;

        datum1Tmp = datum1 = buildData->m_arr[buildColIdx].m_vals[buildIdx];
        datum2Tmp = datum2 = probeData->m_arr[probeColIdx].m_vals[probeIdx];

        isnull1 = IS_NULL(buildData->m_arr[buildColIdx].m_flag[buildIdx]);
        isnull2 = IS_NULL(probeData->m_arr[probeColIdx].m_flag[probeIdx]);

        switch (typeOid) {
            // extract header
            case TIMETZOID:
            case INTERVALOID:
            case TINTERVALOID:
            case NAMEOID:
            case MACADDROID:
                datum1 = isnull1 == false ? PointerGetDatum((char *)datum1Tmp + VARHDRSZ_SHORT) : 0;
                datum2 = isnull2 == false ? PointerGetDatum((char *)datum2Tmp + VARHDRSZ_SHORT) : 0;
                break;
            case TIDOID:
                datum1 = isnull1 == false ? PointerGetDatum(&datum1Tmp) : 0;
                datum2 = isnull2 == false ? PointerGetDatum(&datum2Tmp) : 0;
                break;
            default:
                break;
        }

        compare = inlineApplySortFunction(&buildScanKey->sk_func, buildScanKey->sk_flags, buildScanKey->sk_collation,
                                          datum1, isnull1, datum2, isnull2);
        if (check_part) {
            return compare == 0;
        }
        if (compare != 0)
            return compare <= m_compareValue;
    }

    return compare <= m_compareValue;
}

/*
 * @Description: compare two part by sort key
 * @in build - build part.
 * @in probe - probe part.
 */
void AsofHashJoin::CopyRow(SonicSortPartition *build, SonicSortPartition *probe)
{
    VectorBatch *buildData = build->m_data;
    VectorBatch *probeData = probe->m_data;

    int64 buildIdx = 0;
    int64 probeIdx = 0;

    buildIdx = buildData->m_rows - (build->m_fetchCount - build->m_cmpIdx);
    if (buildIdx < 0) {
        buildData = m_lastBatch;
        buildIdx = m_lastBatch->m_rows + buildIdx;
    }

    probeIdx = probeData->m_rows - (probe->m_fetchCount - probe->m_cmpIdx);

    Assert(probeIdx >= 0);
    Assert(buildIdx >= 0);

    m_probeOp.batch->Copy<true, true>(buildData, buildIdx, buildIdx + 1);
    m_buildOp.batch->Copy<true, true>(probeData, probeIdx, probeIdx + 1);
}

/*
 * Description: Delete memory context.
 */
void AsofHashJoin::freeMemoryContext()
{
    if (m_memControl.hashContext != NULL) {
        for (int idx = 0; idx < m_partNum; ++idx) {
            finishJoinPartition(idx);
        }
        /* Delete child context for hashContext */
        MemoryContextDelete(m_memControl.hashContext);
        m_memControl.hashContext = NULL;
        m_innerPartitions = NULL;
        m_outerPartitions = NULL;
    }
    if (m_memControl.tmpContext != NULL) {
        /* Delete child context for tmpContext */
        MemoryContextDelete(m_memControl.tmpContext);
        m_memControl.tmpContext = NULL;
    }
}

/*
 * @Description: Release build and probe partition.
 * @in partIdx - Partition index.
 */
void AsofHashJoin::finishJoinPartition(uint32 partIdx)
{
    if (m_innerPartitions && m_innerPartitions[partIdx] != NULL) {
        m_innerPartitions[partIdx]->freeResources();
        m_innerPartitions[partIdx] = NULL;
    }

    if (m_outerPartitions && m_outerPartitions[partIdx] != NULL) {
        m_outerPartitions[partIdx]->freeResources();
        m_outerPartitions[partIdx] = NULL;
    }
}

/*
 * @Description: Build output with inBatch and outBatch.
 * @in inBatch - Build side batch.
 * @in outBatch - Probe side batch.
 */
VectorBatch *AsofHashJoin::buildRes(VectorBatch *inBatch, VectorBatch *outBatch)
{
    ExprContext *econtext = NULL;
    VectorBatch *res_batch = NULL;
    List *filter_qual = NIL;
    ;
    ScalarVector *pvector = NULL;
    bool has_qual = false;

    DBG_ASSERT(inBatch->m_rows == outBatch->m_rows);

    ResetExprContext(m_runtime->js.ps.ps_ExprContext);

    /* Testing whether ps_ProjInfo is NULL */
    Assert(m_runtime->js.ps.ps_ProjInfo);
    econtext = m_runtime->js.ps.ps_ProjInfo->pi_exprContext;
    initEcontextBatch(NULL, outBatch, inBatch, NULL);

    if (m_runtime->js.joinqual != NULL && list_length(m_runtime->js.joinqual) >= CONSTNUM2) {
        filter_qual = list_delete_first(m_runtime->js.joinqual);
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;
        pvector = ExecVecQual(filter_qual, econtext, false);
        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (m_runtime->js.ps.qual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;
        pvector = ExecVecQual(m_runtime->js.ps.qual, econtext, false);

        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (has_qual) {
        outBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
        inBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
    }

    initEcontextBatch(NULL, outBatch, inBatch, NULL);
    res_batch = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);
    if (res_batch->m_rows != inBatch->m_rows) {
        res_batch->FixRowCount(inBatch->m_rows);
    }

    inBatch->Reset();
    outBatch->Reset();
    return res_batch;
}

/*
 * @Description: Calculate memory context size
 * @in ctx: memory context to calculate
 * @in allocateSize: pointer to put total allocated size including child context
 * @in freeSize: pointer to put total free size including child context
 */
void AsofHashJoin::calcHashContextSize(MemoryContext ctx, uint64 *allocateSize, uint64 *freeSize)
{
    AllocSetContext *aset = (AllocSetContext *)ctx;
    MemoryContext child;
    if (NULL == ctx) {
        return;
    }
    /* calculate MemoryContext Stats */
    *allocateSize += (aset->totalSpace);
    *freeSize += (aset->freeSpace);

    /* recursive MemoryContext's child */
    for (child = ctx->firstchild; child != NULL; child = child->nextchild) {
        calcHashContextSize(child, allocateSize, freeSize);
    }
}

/*
 * @Description: Calculate partition number with planner parameters.
 */
uint32 AsofHashJoin::calcPartitionNum()
{
    VecAsofJoin *node = NULL;
    Plan *inner_plan = NULL;
    int64 nrows;
    int width;
    uint32 part_num;

    node = (VecAsofJoin *)m_runtime->js.ps.plan;
    inner_plan = innerPlan(node);
    nrows = (int64)inner_plan->plan_rows;
    width = inner_plan->plan_width;
    elog(DEBUG2, "VecAsofJoin: total size: %ldByte, operator size: %ldByte.", (nrows * width), m_memControl.totalMem);

    part_num = getPower2NextNum((nrows * width) / m_memControl.totalMem);
    part_num = Max(PARTMAXNUM, part_num);
    part_num = Min(part_num, SONIC_PART_MAX_NUM);

    return part_num;
}

/*
 * @Description: Calculate partition index and record them in m_partIdx.
 * @in hashVal - hash value to be used. The space should be allocated by caller.
 * @in partNum - partition number.
 * @in nrows - hash value numbers.
 */
inline void AsofHashJoin::calcPartIdx(uint32 *hashVal, uint32 partNum, int nrows)
{
    Assert(partNum != 0);
    for (int i = 0; i < nrows; i++) {
        m_partIdx[i] = *hashVal % partNum;
        hashVal++;
    }
}

/*
 * @Description: Initial partitions.
 */
void AsofHashJoin::initPartitions()
{
    SonicSortPartition *part = NULL;
    if (!m_partNum) {
        m_partNum = calcPartitionNum();
    }
    AutoContextSwitch memSwitch(m_memControl.hashContext);
    SonicHashPartition **buildP = (SonicHashPartition **)palloc0(sizeof(SonicHashPartition *) * m_partNum);
    SonicHashPartition **probeP = (SonicHashPartition **)palloc0(sizeof(SonicHashPartition *) * m_partNum);
    for (uint32 partIdx = 0; partIdx < m_partNum; partIdx++) {
        part = New(CurrentMemoryContext) SonicSortPartition(
                (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem);
        part->m_data = NULL;
        part->m_sortState = NULL;
        buildP[partIdx] = part;
        part = New(CurrentMemoryContext) SonicSortPartition(
                (char*)"outerPartitionContext", m_complicatekey, m_probeOp.tupleDesc, m_memControl.totalMem);
        part->m_data = NULL;
        part->m_sortState = NULL;
        probeP[partIdx] = part;
    }
    m_outerPartitions = probeP;
    m_innerPartitions = buildP;
}

/*
 * @Description: calculate comparison Value
 * @in cmpName - cmp operater name
 */
int AsofHashJoin::comparisonValue(char *cmpName)
{
    if (pg_strcasecmp(cmpName, BTGreaterEqual) || pg_strcasecmp(cmpName, BTLessEqual)) {
        return 0;
    } else if (pg_strcasecmp(cmpName, BTGreater) || pg_strcasecmp(cmpName, BTLess)) {
        return -1;
    }
    Assert(false);
    elog(FATAL, "sortStrategy  %s NOT IMPLEMENTED", cmpName);
}

/*
 * @Description: Reset resources.
 */
void AsofHashJoin::ResetNecessary()
{
    /* Reset hashContext */
    MemoryContextResetAndDeleteChildren(m_memControl.hashContext);
    initPartitions();
    m_runtime->joinState = ASOF_PART;
    resetMemoryControl();
    m_rows = 0;
    m_matchPartIdx = 0;
    m_spillCount = 0;
    m_spillSize = 0;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned
     * by first VectorEngine.
     */
    if (m_runtime->js.ps.righttree->chgParam == NULL) {
        VecExecReScan(m_runtime->js.ps.righttree);
    }
}

/*
 * @Description: reset memory control information.
 */
void AsofHashJoin::resetMemoryControl()
{
    /* do not reset the total memory, since it might be successfully spread in the last partition scan */
    m_memControl.sysBusy = false;
    m_memControl.spillToDisk = false;
    m_memControl.spillNum = 0;
    m_memControl.spreadNum = 0;
    m_memControl.availMem = 0;
    m_memControl.allocatedMem = 0;

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t rc = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(rc, "\0", "\0");
    }
}

void ExecReScanVecAsofJoin(VecAsofJoinState *node)
{
    AsofHashJoin *hj_tbl = (AsofHashJoin *)node->hashTbl;
    if (hj_tbl != NULL)
        hj_tbl->ResetNecessary();

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first VectorEngine.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        VecExecReScan(node->js.ps.lefttree);
}

/*
 * @Description: Early free the memory for VecHashJoin.
 *
 * @param[IN] node:  vector executor state for HashJoin
 * @return: void
 */
void ExecEarlyFreeVecAsofJoin(VecAsofJoinState *node)
{
    PlanState *plan_state = &node->js.ps;
    if (plan_state->earlyFreed) {
        return;
    }
    void *tbl = node->hashTbl;
    if (tbl != NULL) {
        AsofHashJoin *hj_tbl = (AsofHashJoin *)tbl;
        hj_tbl->freeMemoryContext();
    }
    EARLY_FREE_LOG(elog(LOG,
                        "Early Free: After early freeing %sHashJoin "
                        "at node %d, memory used %d MB.",
                        JOIN_NAME, plan_state->plan->plan_node_id, getSessionMemoryUsageMB()));
    plan_state->earlyFreed = true;
    ExecEarlyFree(innerPlanState(node));
    ExecEarlyFree(outerPlanState(node));
}
