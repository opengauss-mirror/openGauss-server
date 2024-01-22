/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * vecmergejoin.cpp
 *
 * IDENTIFICATION
 *    Code/src/gausskernel/runtime/vecexecutor/vecnode/vecmergejoin.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *      ExecVecMergeJoin            mergejoin outer and inner relations.
 *      ExecInitVecMergeJoin        creates and initializes run time states
 *      ExecEndVecMergeJoin         cleans up the node.
 *
 * NOTES
 *
 *      Vectorized Merge-join is a vectorized implementation of regular merge.
 *      join. So we can reuse state machine it has.
 *
 *
 *      NOTES:
 *      ------
 *      1.Get/advance values is done by accessing vector batch, which we use row
 *        offset as representation of a row.
 *      2.To guarantee we can always access the marked rows, we have two choices,
 *        one is to use mark/restr mechanism, the other is vMJ node itself cache
 *        all batches since marked and release them when a new marker is set.
 *      3.Try use vExpression evaluation whenver is possible. The only exception
 *        is the key test (not qual test), which we have to do row by row due to
 *        its jumpy behavior. After key test, we call these tuples are candidates.
 *      4.Accumulate candidates tuples and invoke vExpression to do joinqual and
 *        otherqual check: whenver we want to do a qual check, we will simply
 *        remember it and delay to the batch is full. Current expression does not
 *        support index based selection vector, and we can avoid data copy when
 *        that feature is ready.
 *      5.For non-inner joins, we need to clean up the output batch somehow to
 *        hournour different join semantics. This is possible as we always copy
 *        more rows out before applying quals. Details in ProduceResultBatch().
 *
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "executor/exec/execdebug.h"
#include "executor/exec/execStream.h"
#include "vecexecutor/vecmergejoin.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "access/nbtree.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecexpression.h"

/* Define CodeGen Object */
extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

/* Define a special value representing a null tuple*/
const MJBatchOffset NullBatchTuple = {0xFFFF, true, false};

#define BatchTupleIsNull(t) (t).m_fEmpty

#define MJMaxSize BatchMaxSize

/*
 * Runtime data for each mergejoin clause
 */
typedef struct VecMergeJoinClauseData {
    /* Executable expression trees */
    ExprState* lexpr; /* left-hand (outer) input expression */
    ExprState* rexpr; /* right-hand (inner) input expression */

    /*
     * If we have a current left or right input tuple, the values of the
     * expressions are loaded into these fields:
     */
    Datum ldatum; /* current left-hand value */
    Datum rdatum; /* current right-hand value */
    bool lisnull; /* and their isnull flags */
    bool risnull;

    /*
     * The comparison strategy in use, and the lookup info to let us call the
     * btree comparison support function, and the collation to use.
     */
    bool reverse;     /* if true, negate the cmpfn's output */
    bool nulls_first; /* if true, nulls sort low */
    FmgrInfo cmpfinfo;
    Oid collation;

    /* Vectorize specific
     * -------------------------

     * inner and outer vectors
     */
    ScalarVector* m_lvector;
    ScalarVector* m_rvector;

    /* marked tuple dataum/isnull*/
    Datum m_markedrdatum;
    char* mark_buf;
    int mark_buf_len;
    bool m_markedrisnull;
    SortSupportData ssup;
} VecMergeJoinClauseData;

/* Result type for MJEvalOuterValues and MJEvalInnerValues */
typedef enum {
    MJEVAL_MATCHABLE,    /* normal, potentially matchable tuple */
    MJEVAL_NONMATCHABLE, /* tuple cannot join because it has a null */
    MJEVAL_ENDOFJOIN     /* end of input (physical or effective) */
} MJEvalResult;

/* Slot type for merge join */
typedef enum {
    MJ_INNER_VAR,       /* slot of inner table */
    MJ_OUTER_VAR,       /* slot of outer table */
    MJ_INNER_MARKED_VAR /* marked slot of inner table */
} MJSlotVarType;

static VecMergeJoinClause MJVecExamineQuals(List* mergeclauses, Oid* mergefamilies, Oid* mergecollations,
    const int* mergestrategies, const bool* mergenullsfirst, PlanState* parent)
{
    VecMergeJoinClause clauses;
    int nClauses = list_length(mergeclauses);
    int iClause;
    ListCell* cl = NULL;

    clauses = (VecMergeJoinClause)palloc0(nClauses * sizeof(VecMergeJoinClauseData));

    iClause = 0;
    foreach (cl, mergeclauses) {
        OpExpr* qual = (OpExpr*)lfirst(cl);
        VecMergeJoinClause clause = &clauses[iClause];
        Oid opfamily = mergefamilies[iClause];
        Oid collation = mergecollations[iClause];
        StrategyNumber opstrategy = mergestrategies[iClause];
        bool nulls_first = mergenullsfirst[iClause];
        int op_strategy;
        Oid op_lefttype;
        Oid op_righttype;
        Oid sortfunc;

        if (!IsA(qual, OpExpr))
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_CHECK_VIOLATION),
                    errmsg("mergejoin clause is not an OpExpr")));

        /*
         * Prepare the input expressions for execution.
         */
        clause->lexpr = ExecInitVecExpr((Expr*)linitial(qual->args), parent);
        clause->rexpr = ExecInitVecExpr((Expr*)lsecond(qual->args), parent);

        /* Set up sort support data */
        clause->ssup.ssup_cxt = CurrentMemoryContext;
        clause->ssup.ssup_collation = collation;
        if (opstrategy == BTLessStrategyNumber)
            clause->ssup.ssup_reverse = false;
        else if (opstrategy == BTGreaterStrategyNumber)
            clause->ssup.ssup_reverse = true;
        else /* planner screwed up */
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("unsupported mergejoin strategy %d", opstrategy)));
        clause->ssup.ssup_nulls_first = nulls_first;
        clause->nulls_first = nulls_first;

        /* Extract the operator's declared left/right datatypes */
        get_op_opfamily_properties(qual->opno, opfamily, false, &op_strategy, &op_lefttype, &op_righttype);
        if (op_strategy != BTEqualStrategyNumber) /* should not happen */
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("cannot merge using non-equality operator %u", qual->opno)));

        /* And get the matching support or comparison function */
        sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTSORTSUPPORT_PROC);
        if (OidIsValid(sortfunc)) {
            /* The sort support function should provide a comparator */
            OidFunctionCall1(sortfunc, PointerGetDatum(&clause->ssup));
            Assert(clause->ssup.comparator != NULL);
        } else {
            /* opfamily doesn't provide sort support, get comparison func */
            sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC);
            if (!OidIsValid(sortfunc)) /* should not happen */
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("missing support function %d(%u,%u) in opfamily %u",
                            BTORDER_PROC,
                            op_lefttype,
                            op_righttype,
                            opfamily)));
            /* We'll use a shim to call the old-style btree comparator */
            PrepareSortSupportComparisonShim(sortfunc, &clause->ssup);
        }

        iClause++;
    }

    return clauses;
}

/* EvaluateBatch
 * Evaluate expressions for the batch
 */
template <int child>
void EvaluateBatch(VecMergeJoinState* state, VectorBatch* batch)
{
    ExprContext* econtext = ((child == INNER_VAR) ? state->mj_InnerEContext : state->mj_OuterEContext);
    MemoryContext oldContext;

    ResetExprContext(econtext);
    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    /* Evaluate values required by join expression */
    econtext->align_rows = batch->m_rows;

    for (int i = 0; i < state->mj_NumClauses; i++) {
        ExprState* expr = NULL;
        ScalarVector* input_result = NULL;
        ScalarVector* return_result = NULL;
        VecMergeJoinClause clause = &state->mj_Clauses[i];

        if (child == INNER_VAR) {
            initEcontextBatch(NULL, NULL, batch, NULL);
            expr = clause->rexpr;
            input_result = clause->m_rvector;
        } else {
            initEcontextBatch(NULL, batch, NULL, NULL);
            expr = clause->lexpr;
            input_result = clause->m_lvector;
        }
        return_result = VectorExprEngine(expr, econtext, batch->m_sel, input_result, NULL);
        if (return_result != input_result) {
            input_result->copy(return_result);
        }
    }

    (void)MemoryContextSwitchTo(oldContext);
}

/* GetNextBatchTuple
 * Get next batch tuple from the scan
 */
template <int child>
MJBatchOffset GetNextBatchTuple(VecMergeJoinState* state)
{
    BatchAccessor* pba = NULL;
    MJBatchOffset batchOffset;

    Assert(child == INNER_VAR || child == OUTER_VAR);
    pba = &state->m_inputs[child - INNER_VAR];

    /* We differentiate several cases here:
     * 1. curOffset equals -1: this means this is the first time of calling
     * 2. curOffset equals maxOffset: we finish all rows within current
     *    batch, so move to next batch. Note that curOffset can rewind as
     *    we can use markPos/Restr to change it.
     */
    if (pba->m_curOffset == -1 || pba->m_curOffset == pba->m_maxOffset) {
        VectorBatch* batch = NULL;

        /* If we have a marked tuple, we may access it later, so we need to
         * save all context around it and the tuple itself. Some data are
         * needed for non-integer style clauses comparisons only. After
         * comparison is passed, we restore position to get the needed batch.
         */
        if (child == INNER_VAR && !BatchTupleIsNull(state->mj_MarkedOffset) &&
            state->mj_MarkedOffset.m_batchSeq == pba->m_batchSeq) {
            int offset = state->mj_MarkedOffset.m_offset;

            for (int i = 0; i < state->mj_NumClauses; i++) {
                VecMergeJoinClause clause = &state->mj_Clauses[i];

                if (clause->m_rvector->m_desc.encoded) {
                    clause->m_markedrisnull = clause->m_rvector->IsNull(offset);
                    if (clause->m_markedrisnull == false) {
                        Datum value = ScalarVector::Decode(clause->m_rvector->m_vals[offset]);
                        int len = VARSIZE_ANY(value);
                        if (clause->mark_buf == NULL) {
                            clause->mark_buf = (char*)palloc(len);
                            clause->mark_buf_len = len;
                        } else if (len > clause->mark_buf_len) {
                            clause->mark_buf = (char*)repalloc(clause->mark_buf, len);
                            clause->mark_buf_len = len;
                        }
                        errno_t errorno = memcpy_s(clause->mark_buf, len, (char*)value, len);
                        securec_check(errorno, "\0", "\0");
                        clause->m_markedrdatum = PointerGetDatum(clause->mark_buf);
                    }
                } else {
                    clause->m_markedrdatum = clause->m_rvector->m_vals[offset];
                    clause->m_markedrisnull = clause->m_rvector->IsNull(offset);
                }
            }
        }

        /* Now we can get next batch */
        batch = VectorEngine(pba->m_plan);
        if (!BatchIsNull(batch)) {
            /* Evaluate expressions for the batch */
            EvaluateBatch<child>(state, batch);

            /* Reset scan position to the new batch */
            pba->m_curOffset = 0;
            pba->m_curBatch = batch;
            pba->m_batchSeq++;
            pba->m_maxOffset = batch->m_rows;
        } else {
            /*
             * No more rows from children nodes and pba->m_curBatch can not be used.
             */
            pba->m_curBatch = NULL;
            return NullBatchTuple;
        }
    }

    /* Move to next tuple in the batch */
    batchOffset.m_fEmpty = false;
    batchOffset.m_fMarked = false;
    batchOffset.m_offset = pba->m_curOffset++;
    batchOffset.m_batchSeq = pba->m_batchSeq;

    return batchOffset;
}

/* BatchMarkPos
 * Mark the current batch scan position on inner side.
 */
void BatchMarkPos(VecMergeJoinState* state)
{
    BatchAccessor* pba = NULL;
    PlanState* innerPlan = NULL;

    /* Record offset and batch sequence. We delay the batch copy to later
     * when we are leaving current batch.
     */
    pba = &state->m_inputs[0];
    innerPlan = pba->m_plan;
    state->mj_MarkedOffset = state->mj_InnerOffset;
    state->mj_MarkedOffset.m_fMarked = true;
    state->mj_MarkedOffset.m_batchSeq = pba->m_batchSeq;

    ExecVecMarkPos(innerPlan);
}

/*
 * @Description: Restore scan position such that the first VectorEngine
 *   following the restore operation will yield the same tuple as the first
 *   one following the mark operation.
 *
 * @param[IN] state: state of VecMergeJoin
 * @return bool: true if restore position successfully
 */
static bool BatchRestrPos(VecMergeJoinState* state)
{
    BatchAccessor* pba = NULL;
    VectorBatch* batch = NULL;
    PlanState* innerPlan = NULL;
    int markedSeq;

    pba = &state->m_inputs[0];
    innerPlan = pba->m_plan;
    markedSeq = state->mj_MarkedOffset.m_batchSeq;

    /* Skip the restore if the position is within the same batch or mj_InnerOffset is not null*/
    if (markedSeq != pba->m_batchSeq || BatchTupleIsNull(state->mj_InnerOffset)) {
        ExecVecRestrPos(innerPlan);
        batch = VectorEngine(innerPlan);

        /* batch is NULL only if query has been stopped or canceled. */
        if (BatchIsNull(batch)) {
            return false;
        }

        EvaluateBatch<INNER_VAR>(state, batch);

        /* Reset current batch related */
        pba->m_curBatch = batch;
        pba->m_maxOffset = batch->m_rows;
        pba->m_batchSeq = markedSeq;
    }

    /* restore position to next row */
    DBG_ASSERT(state->mj_MarkedOffset.m_offset >= 0);
    DBG_ASSERT(state->mj_MarkedOffset.m_offset + 1 <= pba->m_maxOffset);
    pba->m_curOffset = state->mj_MarkedOffset.m_offset + 1;

    return true;
}

/* MJGetValues
 * Get the values of the mergejoined expressions for the current
 * inner or outer tuple.  We also detect whether it's impossible for
 * the current tuple to match anything.
 */
template <int child>
FORCE_INLINE MJEvalResult MJGetValues(VecMergeJoinState* mergestate, MJBatchOffset slot)
{
    MJEvalResult result = MJEVAL_MATCHABLE;
    bool fFill = false;
    Assert(child == MJ_INNER_VAR || child == MJ_OUTER_VAR || child == MJ_INNER_MARKED_VAR);
    /* Check for end of input subplan */
    if (BatchTupleIsNull(slot))
        return MJEVAL_ENDOFJOIN;

    for (int i = 0; i < mergestate->mj_NumClauses; i++) {
        ScalarValue* pValues = NULL;
        VecMergeJoinClause clause = &mergestate->mj_Clauses[i];
        /* Get tuples from cached batch*/
        if (child == MJ_INNER_VAR) {
            DBG_ASSERT(slot.m_offset < clause->m_rvector->m_rows);
            pValues = clause->m_rvector->m_vals;
            clause->rdatum = pValues[slot.m_offset];
            clause->risnull = clause->m_rvector->IsNull(slot.m_offset);
            fFill = mergestate->mj_FillInner;
        } else if (child == MJ_OUTER_VAR) {
            DBG_ASSERT(slot.m_offset < clause->m_lvector->m_rows);
            pValues = clause->m_lvector->m_vals;
            clause->ldatum = pValues[slot.m_offset];
            clause->lisnull = clause->m_lvector->IsNull(slot.m_offset);
            fFill = mergestate->mj_FillOuter;
        } else if (child == MJ_INNER_MARKED_VAR) {
            /* marked tuple is always from inner side*/
            clause->rdatum = clause->m_markedrdatum;
            clause->risnull = clause->m_markedrisnull;
            fFill = mergestate->mj_FillInner;
        }

        /* If we allow null = null, don't end join */
        if (clause->lisnull && mergestate->js.nulleqqual == NIL) {
            /* match is impossible; can we end the join early? */
            if (i == 0 && !clause->nulls_first && !fFill)
                result = MJEVAL_ENDOFJOIN;
            else if (result == MJEVAL_MATCHABLE)
                result = MJEVAL_NONMATCHABLE;
        }
    }

    return result;
}

/* MJGetInnerValues
 * Same as above, for the outer tuple.
 */
static FORCE_INLINE MJEvalResult MJGetOuterValues(VecMergeJoinState* mergestate, MJBatchOffset outerslot)
{
    return MJGetValues<MJ_OUTER_VAR>(mergestate, outerslot);
}

/* MJGetInnerValues
 * Same as above, for the inner tuple.
 */
static FORCE_INLINE MJEvalResult MJGetInnerValues(VecMergeJoinState* mergestate, MJBatchOffset innerslot)
{
    return MJGetValues<MJ_INNER_VAR>(mergestate, innerslot);
}

/* MJGetMarkedValues
 * Same as above, for the marked tuple.
 */
static FORCE_INLINE MJEvalResult MJGetMarkedValues(VecMergeJoinState* mergestate, MJBatchOffset markedslot)
{
    DBG_ASSERT(!markedslot.m_fEmpty);
    /*
     * If the batchseq of the batch which contains the markedslot and the batchseq of the current batch
     * are not equal, we can't get the marked value from mergestate->mj_Clauses->m_rvector, because the
     * m_rvector values were calculated from the current inner batch.
     * Similarly, when the current inner batch, which calculated from the new batch is NULL, we can't get
     * the marked value from current mergestate->mj_Clauses->m_rvector, because the rvector values may
     * have been rewrited by the new batch.
     */
    if (markedslot.m_batchSeq != mergestate->m_inputs[0].m_batchSeq || BatchIsNull(mergestate->m_inputs[0].m_curBatch))
        return MJGetValues<MJ_INNER_MARKED_VAR>(mergestate, markedslot);

    return MJGetValues<MJ_INNER_VAR>(mergestate, markedslot);
}

/*
 * @Description: get datum value from vector datum
 * @in typeId - column type id
 * @in datum - vector datum
 * @out - row datum
 */
static FORCE_INLINE Datum MJGetDatumValue(Oid typeId, Datum datum)
{
    Datum result;

    if (COL_IS_ENCODE(typeId)) {
        switch (typeId) {
            case TIMETZOID:
            case INTERVALOID:
            case TINTERVALOID:
            case NAMEOID:
                result = PointerGetDatum((char*)datum + VARHDRSZ_SHORT);
                break;
            default:
                result = ScalarVector::Decode(datum);
                break;
        }

    } else {
        /* convert vector datum to row datum for tid */
        if (TIDOID == typeId)
            result = PointerGetDatum(&datum);
        else
            result = datum;
    }

    return result;
}
/* MJVecCompare
 * Compare the mergejoinable values of the current two input tuples
 * and return 0 if they are equal (ie, the mergejoin equalities all
 * succeed), +1 if outer > inner, -1 if outer < inner.
 */
static int32 MJVecCompare(VecMergeJoinState* mergestate)
{
    int32 result = 0;
    bool nulleqnull = false;
    Datum ldatum, rdatum;
    int i;

    for (i = 0; i < mergestate->mj_NumClauses; i++) {
        VecMergeJoinClause clause = &mergestate->mj_Clauses[i];
        /* Deal with null inputs */
        if (clause->lisnull) {
            if (clause->risnull) {
                nulleqnull = true;
                continue;
            }
            if (clause->nulls_first)
                result = -1;  // NULL < NOT NULL
            else
                result = 1;  // NULL > NOT NULL
        }
        if (clause->risnull) {
            if (clause->nulls_first)
                result = 1;  // NOT NULL > NULL
            else
                result = -1;  // NOT NULL < NULL
        }

        if (!clause->lisnull && !clause->risnull) {
            ldatum = MJGetDatumValue(clause->lexpr->resultType, clause->ldatum);
            rdatum = MJGetDatumValue(clause->rexpr->resultType, clause->rdatum);

            result = (*clause->ssup.comparator)(ldatum, rdatum, &clause->ssup);
            if (clause->ssup.ssup_reverse)
                result = -result;
        }

        if (result != 0)
            break;
    }
    /*
     * If we had any null comparison results or NULL-vs-NULL inputs, we do not
     * want to report that the tuples are equal.  Instead, if result is still
     * 0, change it to +1.  This will result in advancing the inner side of
     * the join.
     *
     * Likewise, if there was a constant-false joinqual, do not report
     * equality.  We have to check this as part of the mergequals, else the
     * rescan logic will do the wrong thing.
     */
    if (result == 0 && ((nulleqnull && mergestate->js.nulleqqual == NIL) || mergestate->mj_ConstFalseJoin))
        result = 1;

    return result;
}

/* DoBatchFill
 *      Actually generate rows with inner and outer tuple.
 */
template <bool fInnerNull, bool fOuterNull>
int DoBatchFill(VecMergeJoinState* node, MJBatchOffset innerTuple, MJBatchOffset outerTuple)
{
    int i;
    int n = node->m_pInnerMatch->m_rows;
    ScalarVector* dst = NULL;
    ScalarVector* src = NULL;
    uint16 in_offset = innerTuple.m_offset;
    uint16 out_offset = outerTuple.m_offset;
    uint8* dst_flag = NULL;
    uint8* src_flag = NULL;

    /* Caller has to handle the batch before fill again */
    DBG_ASSERT(n < MJMaxSize);

    /* Before we implement index based selection vector, we have to copy
     * the input batches before the qual check.
     */
    dst = node->m_pInnerMatch->m_arr;
    src = !fInnerNull ? node->m_inputs[0].m_curBatch->m_arr : NULL;
    for (i = 0; i < node->m_pInnerMatch->m_cols; i++) {
        dst_flag = dst[i].m_flag;
        if (!fInnerNull) {
            src_flag = src[i].m_flag;
            if (!IS_NULL(src_flag[in_offset])) {
                if (src[i].m_desc.encoded) {
                    Datum DatumVal = ScalarVector::Decode(src[i].m_vals[in_offset]);
                    switch (src[i].m_desc.typeId) {
                        case TIMETZOID:
                        case INTERVALOID:
                        case TINTERVALOID:
                        case NAMEOID:
                            dst[i].AddVar(PointerGetDatum((char*)DatumVal + VARHDRSZ_SHORT), n);
                            break;
                        default:
                            dst[i].AddVar(DatumVal, n);
                            break;
                    }
                    dst[i].m_rows = n + 1;
                    SET_NOTNULL(dst_flag[n]);
                } else {
                    dst[i].m_vals[n] = src[i].m_vals[in_offset];
                    dst[i].m_rows = n + 1;
                    SET_NOTNULL(dst_flag[n]);
                }
            } else {
                SET_NULL(dst_flag[n]);
            }
        } else {
            SET_NULL(dst_flag[n]);
        }
    }
    dst = node->m_pOuterMatch->m_arr;
    src = !fOuterNull ? node->m_inputs[1].m_curBatch->m_arr : NULL;
    for (i = 0; i < node->m_pOuterMatch->m_cols; i++) {
        dst_flag = dst[i].m_flag;

        if (!fOuterNull) {
            src_flag = src[i].m_flag;
            if (!IS_NULL(src_flag[out_offset])) {
                if (src[i].m_desc.encoded) {
                    Datum DatumVal = ScalarVector::Decode(src[i].m_vals[out_offset]);
                    switch (src[i].m_desc.typeId) {
                        case TIMETZOID:
                        case INTERVALOID:
                        case TINTERVALOID:
                        case NAMEOID:
                            dst[i].AddVar(PointerGetDatum((char*)DatumVal + VARHDRSZ_SHORT), n);
                            break;
                        default:
                            dst[i].AddVar(DatumVal, n);
                            break;
                    }
                    dst[i].m_rows = n + 1;
                    SET_NOTNULL(dst_flag[n]);
                } else {
                    dst[i].m_vals[n] = src[i].m_vals[out_offset];
                    dst[i].m_rows = n + 1;
                    SET_NOTNULL(dst_flag[n]);
                }
            } else {
                SET_NULL(dst_flag[n]);
            }
        } else {
            SET_NULL(dst_flag[n]);
        }
    }

    node->m_pInnerMatch->m_rows = n + 1;
    node->m_pInnerOffset[n] = innerTuple;
    node->m_pOuterMatch->m_rows = n + 1;
    node->m_pOuterOffset[n] = outerTuple;
    return n + 1;
}

/* BatchFillOuter
 * Generate a fake join tuple with nulls for the outer tuple,
 * Delay the qual checks till we get a full batch.
 */
static int BatchFillOuter(VecMergeJoinState* node)
{
    return DoBatchFill<true, false>(node, NullBatchTuple, node->mj_OuterOffset);
}

/* BatchFillInner
 * Generate a fake join tuple with nulls for the inner tuple,
 * Delay the qual checks till we get a full batch.
 */
static int BatchFillInner(VecMergeJoinState* node)
{
    return DoBatchFill<false, true>(node, node->mj_InnerOffset, NullBatchTuple);
}

/* BatchFillInnerAndOuter
 * Generate a join tuple with values from inner and outer.
 * Delay the qual checks till we get a full batch.
 */
int BatchFillInnerAndOuter(VecMergeJoinState* node)
{
    return DoBatchFill<false, false>(node, node->mj_InnerOffset, node->mj_OuterOffset);
}

/* ProduceResultBatch
 * Produce a result batch from inner batch and outer batch rows.
 * Templatize on jointype for better performance.

 * Conveient marcros for this function only
 */
#define InEmpty(i) inOffset[i].m_fEmpty
#define InBatch(i) inOffset[i].m_batchSeq
#define InOffset(i) inOffset[i].m_offset
#define OuBatch(i) ouOffset[i].m_batchSeq
#define OuEmpty(i) ouOffset[i].m_fEmpty
#define OuOffset(i) ouOffset[i].m_offset
#define SameInTuple(i, j) (InOffset(i) == InOffset(j) && InBatch(i) == InBatch(j))
#define SameOuTuple(i, j) (OuOffset(i) == OuOffset(j) && OuBatch(i) == OuBatch(j))

template <int joinType>
VectorBatch* ProduceResultBatchT(VecMergeJoinState* node, bool fCheckQual)
{
    int i, prev, next, last, cRows;
    ExprContext* econtext = NULL;
    VectorBatch* resBatch = NULL;
    MJBatchOffset* inOffset = NULL;
    MJBatchOffset* ouOffset = NULL;
    bool* pSelection = NULL;
    VectorBatch* inBatch = NULL;
    VectorBatch* ouBatch = NULL;
    bool fQualified = false;
    bool fFirstInEmpty = false;
    bool fFirstOuEmpty = false;
    bool hasqual = false;

    /* Expect both sides are equal*/
    inBatch = node->m_pInnerMatch;
    ouBatch = node->m_pOuterMatch;

    if (inBatch->m_rows == 0 && ouBatch->m_rows == 0)
        return NULL;

    DBG_ASSERT(inBatch->m_rows == ouBatch->m_rows);

    inOffset = node->m_pInnerOffset;
    ouOffset = node->m_pOuterOffset;
    cRows = inBatch->m_rows;

    econtext = node->js.ps.ps_ExprContext;
    initEcontextBatch(node->m_pCurrentBatch, ouBatch, inBatch, NULL);
    pSelection = econtext->ecxt_scanbatch->m_sel;

    /* Uuselect empty fill tuples to avoid qual checks. Inner join and
     * semi join does not generate empty filled tuples.
     */
    int colNum = inBatch->m_cols;
    for (i = 0; i < colNum; i++) {
        inBatch->m_arr[i].m_rows = cRows;
    }
    colNum = ouBatch->m_cols;
    for (i = 0; i < colNum; i++) {
        ouBatch->m_arr[i].m_rows = cRows;
    }

    /*
     * In QualCodeGen, we use 'econtext->ecxt_scanbatch->m_rows' as the parameter
     * to travesal the whole batch. So we should pre-set the number of rows as
     * we do in 'ExecVecMakeFunctionResult' to avoid the rows is zero.
     */
    econtext->ecxt_scanbatch->m_rows = cRows;
    econtext->ecxt_scanbatch->ResetSelection(true);
    for (i = 0; i < cRows; i++) {
        if (joinType != JOIN_INNER && joinType != JOIN_SEMI) {
            if (InEmpty(i) || OuEmpty(i)) {
                DBG_ASSERT(InEmpty(i) ^ OuEmpty(i));
                pSelection[i] = false;
            }
        }
    }

    /* Once the qual has been codegened, LLVM optimization is applied */
    if (node->jitted_joinqual) {
        if (HAS_INSTR(&node->js, false)) {
            node->js.ps.instrument->isLlvmOpt = true;
        }
    }

    /* First apply the qual check if any. This does not include
     * any empty fill tuples.
     */
    if (fCheckQual) {
        hasqual = true;
        /*
         * If mjstate->js.joinqual has beed codegened, use LLVM optimization first
         */
        if (node->jitted_joinqual) {
            (void)node->jitted_joinqual(econtext);
        } else if (node->js.joinqual != NULL) {
            ExecVecQual(node->js.joinqual, econtext, false, false);
        }
    }

    /* Now scan through the selection vector and clean up for non inner joins */
    last = cRows - 1;
    fFirstInEmpty = true;
    fFirstOuEmpty = true;
    for (i = 0; i < cRows; i++) {
        switch (joinType) {
                /* For outer joins, we decide if we need the empty fill tuple, which shall
                 * always show up as the last one in the join sequence. Going backward, if
                 * this tuple successfully joined with one, then we shall discard the empty
                 *fill tuple. Otherwise, keep it.

                 * In previous already output batches, we could have the same join row, and
                 * it could be 10 batches ago. Be careful with this case.
                 */
            case JOIN_FULL:
            case JOIN_LEFT: {
                if (InEmpty(i)) {
                    if (fFirstInEmpty && node->m_prevOuterQualified &&
                        OuOffset(i) == node->m_prevOuterOffset.m_offset &&
                        OuBatch(i) == node->m_prevOuterOffset.m_batchSeq)
                        pSelection[i] = false;
                    else {
                        pSelection[i] = true;
                        /* For full joins, we always fill an empty outer tuple then an inner
                         * tuple. So have to skip the empty outer tuple.
                         */
                        for (prev = (joinType == JOIN_LEFT) ? (i - 1) : (i - 2); (prev >= 0) && SameOuTuple(i, prev);
                             prev--) {
                            DBG_ASSERT(joinType == JOIN_FULL || !OuEmpty(prev));
                            if (pSelection[prev]) {
                                pSelection[i] = false;
                                break;
                            }
                        }
                    }
                    fFirstInEmpty = false;
                }
                if (joinType == JOIN_LEFT)
                    break;
            }
            /* fall through */
            case JOIN_RIGHT: {
                if (OuEmpty(i)) {
                    if (fFirstOuEmpty && node->m_prevInnerQualified &&
                        InOffset(i) == node->m_prevInnerOffset.m_offset &&
                        InBatch(i) == node->m_prevInnerOffset.m_batchSeq)
                        pSelection[i] = false;
                    else {
                        pSelection[i] = true;
                        for (prev = i - 1; prev >= 0 && SameInTuple(i, prev); prev--) {
                            DBG_ASSERT(joinType == JOIN_FULL || !InEmpty(prev));
                            if (pSelection[prev]) {
                                pSelection[i] = false;
                                break;
                            }
                        }
                    }
                    fFirstOuEmpty = false;
                }
                break;
            }
            case JOIN_SEMI: {
                /* For semi joins, if we see a successful joined tuple, we going forward to
                 * remove all other successful joins of the same tuple.
                 * For the first row (offset zero) in current batch, if previous batch already
                 * output the same row, we shall remember it and not output now.
                 */
                if (pSelection[i]) {
                    if (node->m_prevOuterQualified && OuOffset(i) == node->m_prevOuterOffset.m_offset &&
                        OuBatch(i) == node->m_prevOuterOffset.m_batchSeq)
                        pSelection[i] = false;
                    for (next = i + 1; next < cRows && SameOuTuple(i, next); next++) {
                        DBG_ASSERT(!OuEmpty(next));
                        pSelection[next] = false;
                    }

                    /* fast skip all removed rows */
                    i = next - 1;
                }
                break;
            }
            case JOIN_ANTI:
            case JOIN_LEFT_ANTI_FULL: {
                /* For anti joins, exclude rows having a successful qualification. As anti join
                 * follows same filling strategy as left join, we can do backward check.

                 * In previous already output batches contains same join row, it surely not yet
                 * output as it has to wait till see the end sequence of the same row. So just need
                 * to remember previous batch's qualification on current row.
                 */
                DBG_ASSERT(node->mj_FillOuter && !node->mj_FillInner);
                if (InEmpty(i) || i == last) {
                    fQualified = false;
                    if (fFirstInEmpty && node->m_prevOuterQualified &&
                        OuOffset(i) == node->m_prevOuterOffset.m_offset &&
                        OuBatch(i) == node->m_prevOuterOffset.m_batchSeq)
                        fQualified = true;
                    else {
                        int toStart = i;
                        if (InEmpty(i)) {
                            pSelection[i] = true;
                            toStart = i - 1;
                        }
                        for (prev = toStart; prev >= 0 && SameOuTuple(i, prev); prev--) {
                            DBG_ASSERT(!OuEmpty(prev));
                            if (pSelection[prev]) {
                                fQualified = true;
                                break;
                            }
                        }
                    }
                    if (fQualified) {
                        for (prev = i; prev >= 0 && SameOuTuple(i, prev); prev--) {
                            pSelection[prev] = false;
                        }
                    }
                    if (InEmpty(i))
                        fFirstInEmpty = false;
                }
                break;
            }
            default:
                DBG_ASSERT(joinType == JOIN_INNER);
                break;
        }
    }
    /* Re-examine the selection vector and set some status for next batch. We
     * remember the last tuple, which could even from many batches ago, status
     * for the clean up procedure above.

     * Anti join is different because when there is a qualified, it actually
     * de-select all join rows. So check now is too late. Instead, we use the
     * flag carried from clean up procedure.

     * For full joins, be careful with the real *last* tuple: other joins
     * can only have at most one empty matching tuple, so the even the last
     * tuple is the empty fill one is ok as the sequence is complete. For
     * full joins, as we have two empty matching tuples, skip one.
     */
    if (joinType == JOIN_FULL && OuEmpty(last))
        last--;
    /* Check if the current batch continues from previous. If we are in a new
     * key sequence, or we are continuing and there is no match, then we shall
     * check current batch.
     */
    if (last >= 0) {
        if (node->m_prevInnerOffset.m_offset != InOffset(last) || node->m_prevInnerOffset.m_batchSeq != InBatch(last)) {
            node->m_prevInnerQualified = false;
            node->m_prevInnerOffset = inOffset[last];
        }
        if (node->m_prevOuterOffset.m_offset != OuOffset(last) || node->m_prevOuterOffset.m_batchSeq != OuBatch(last)) {
            node->m_prevOuterQualified = false;
            node->m_prevOuterOffset = ouOffset[last];
        }
    }

    switch (joinType) {
        case JOIN_FULL:
        case JOIN_LEFT:
        case JOIN_SEMI: {
            if (!node->m_prevOuterQualified) {
                for (prev = last; prev >= 0 && SameOuTuple(last, prev); prev--) {
                    DBG_ASSERT(joinType == JOIN_FULL || !OuEmpty(prev));
                    if (pSelection[prev]) {
                        node->m_prevOuterQualified = true;
                        break;
                    }
                }
            }
            if (joinType != JOIN_FULL)
                break;
        }
        /* fall through */
        case JOIN_RIGHT: {
            if (!node->m_prevInnerQualified) {
                for (prev = last; prev >= 0 && SameInTuple(last, prev); prev--) {
                    DBG_ASSERT(joinType == JOIN_FULL || !InEmpty(prev));
                    if (pSelection[prev]) {
                        node->m_prevInnerQualified = true;
                        break;
                    }
                }
            }
            break;
        }
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL: {
            node->m_prevOuterQualified = fQualified;
            break;
        }
        default:
            DBG_ASSERT(joinType == JOIN_INNER);
            break;
    }

    if (node->js.ps.qual != NULL) {
        hasqual = true;
        ExecVecQual(node->js.ps.qual, econtext, false, false);
    }

    /* Do the final projection and pack the output if required.*/
    if (node->js.ps.ps_ProjInfo == NULL) {
        inBatch->Pack(econtext->ecxt_scanbatch->m_sel);
        Assert(inBatch->IsValid() && ouBatch->IsValid());
        node->m_pReturnBatch->m_rows = inBatch->m_rows;
        node->m_pReturnBatch->m_arr[0].m_rows = inBatch->m_rows;
        resBatch = node->m_pReturnBatch;
    } else {
        if (hasqual) {
            inBatch->Pack(econtext->ecxt_scanbatch->m_sel);
            ouBatch->Pack(econtext->ecxt_scanbatch->m_sel);
        }

        initEcontextBatch(NULL, ouBatch, inBatch, NULL);

        if (inBatch->m_rows == 0)
            return NULL;
        resBatch = ExecVecProject(node->js.ps.ps_ProjInfo);
        if (resBatch->m_rows != inBatch->m_rows) {
            resBatch->m_rows = inBatch->m_rows;
            resBatch->FixRowCount();
        }
    }

    return resBatch;
}

/* ExecVecMergeJoin
 * Execute vectorized merge join
 */
template <int joinType>
VectorBatch* ExecVecMergeJoinT(VecMergeJoinState* node)
{
    int32 compareResult;
    MJBatchOffset innerTupleSlot;
    MJBatchOffset outerTupleSlot;
    ExprContext* econtext = NULL;
    bool doFillOuter = false;
    bool doFillInner = false;
    VectorBatch* resultBatch = NULL;

    /* prevent excessive calls */
    DBG_ASSERT(!node->js.ps.ps_vec_TupFromTlist);

    econtext = node->js.ps.ps_ExprContext;
    doFillOuter = node->mj_FillOuter;
    doFillInner = node->mj_FillInner;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a join tuple.
     */
restart:
    if (node->m_fDone) {
        ExecEarlyFree(innerPlanState(node));
        ExecEarlyFree(outerPlanState(node));
        EARLY_FREE_LOG(elog(LOG,
            "Early Free: MergeJoin is done "
            "at node %d, memory used %d MB.",
            (node->js.ps.plan)->plan_node_id,
            getSessionMemoryUsageMB()));

        return NULL;
    }

    node->m_pInnerMatch->Reset();
    node->m_pOuterMatch->Reset();
    ResetExprContext(econtext);
    for (;;) {
        /* get the current state of the join and do things accordingly.*/
        switch (node->mj_JoinState) {
            /*
             * EXEC_MJ_INITIALIZE_OUTER means that this is the first time
             * ExecMergeJoin() has been called and so we have to fetch the
             * first matchable tuple for both outer and inner subplans. We
             * do the outer side in INITIALIZE_OUTER state, then advance
             * to INITIALIZE_INNER state for the inner subplan.
             */
            case EXEC_MJ_INITIALIZE_OUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_INITIALIZE_OUTER\n");
                outerTupleSlot = GetNextBatchTuple<OUTER_VAR>(node);
                node->mj_OuterOffset = outerTupleSlot;
                switch (MJGetOuterValues(node, outerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /* OK to go get the first inner tuple */
                        node->mj_JoinState = EXEC_MJ_INITIALIZE_INNER;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Stay in same state to fetch next outer tuple */
                        if (doFillOuter) {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * inner tuple, and return it if it passes the
                             * non-join quals.
                             */
                            if (BatchFillOuter(node) == MJMaxSize)
                                goto done;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: nothing in outer subplan\n");
                        if (doFillInner) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples. We set MatchedInner = true to
                             * force the ENDOUTER state to advance inner.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            node->mj_MatchedInner = true;
                            break;
                        }
                        /* Otherwise we're done. */
                        node->m_fDone = true;

                        /*
                         * If one side of MergeJoin returns 0 tuple and no need to
                         * generate a fake join tuple with nulls, we should deinit
                         * the consumer under MergeJoin earlier.
                         */
                        ExecEarlyDeinitConsumer((PlanState*)node);
                        goto done;
                    default:
                        break;
                }
                break;
            case EXEC_MJ_INITIALIZE_INNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_INITIALIZE_INNER\n");
                innerTupleSlot = GetNextBatchTuple<INNER_VAR>(node);
                node->mj_InnerOffset = innerTupleSlot;
                /* Compute join values and check for unmatchability */
                switch (MJGetInnerValues(node, innerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /*
                         * OK, we have the initial tuples.  Begin by skipping
                         * non-matching tuples.
                         */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Stay in same state to fetch next inner tuple */
                        if (doFillInner) {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * outer tuple, and return it if it passes the
                             * non-join quals.
                             */
                            if (BatchFillInner(node) == MJMaxSize)
                                goto done;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more inner tuples */
                        MJ_printf("ExecMergeJoin: nothing in inner subplan\n");
                        if (doFillOuter) {
                            /*
                             * Need to emit left-join tuples for all outer
                             * tuples, including the one we just fetched.  We
                             * set MatchedOuter = false to force the ENDINNER
                             * state to emit first tuple before advancing
                             * outer.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            node->mj_MatchedOuter = false;
                            break;
                        }
                        node->m_fDone = true;

                        /*
                         * If one side of MergeJoin returns 0 tuple and no need to
                         * generate a fake join tuple with nulls, we should deinit
                         * the consumer under MergeJoin earlier.
                         */
                        ExecEarlyDeinitConsumer((PlanState*)node);
                        goto done;
                    default:
                        break;
                }
                break;
            /*
             * EXEC_MJ_JOINTUPLES means we have two tuples which satisfied
             * the merge clause so we join them and then proceed to get
             * the next inner tuple (EXEC_MJ_NEXTINNER).
             */
            case EXEC_MJ_JOINTUPLES:
                MJ_printf("ExecMergeJoin: EXEC_MJ_JOINTUPLES\n");
                /* For outer joins, if qual check passed, we shall set the following
                 * values to true. But the qual check is delayed, thus we intentionally
                 * leave them to false to copy the tuples but remove them after we get
                 * qual check result later. For other non-inner joins, we rely on the
                 * same strategy.
                 */
                DBG_ASSERT(!node->mj_MatchedOuter);
                DBG_ASSERT(!node->mj_MatchedInner);

                /* Set the next state-machine state. */
                node->mj_JoinState = EXEC_MJ_NEXTINNER;
                /* We find the join candidates, now copy them out for later
                 * qualification checks. Need to be careful with SEMI or ANTI
                 * joins to set right joinstate.
                 */
                if (BatchFillInnerAndOuter(node) == MJMaxSize)
                    goto done;
                break;
            /*
             * EXEC_MJ_NEXTINNER means advance the inner scan to the next
             * tuple. If the tuple is not nil, we then proceed to test it
             * against the join qualification.
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this inner tuple.
             */
            case EXEC_MJ_NEXTINNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_NEXTINNER\n");
                if (doFillInner && !node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true;
                    if (BatchFillInner(node) == MJMaxSize)
                        goto done;
                }
                /*
                 * now we get the next inner tuple, if any.  If there's none,
                 * advance to next outer tuple (which may be able to join to
                 * previously marked tuples).
                 *
                 * NB: must NOT do "extraMarks" here, since we may need to
                 * return to previously marked tuples.
                 */
                innerTupleSlot = GetNextBatchTuple<INNER_VAR>(node);
                node->mj_InnerOffset = innerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(innerTupleSlot);
                node->mj_MatchedInner = false;

                /* Compute join values and check for unmatchability */
                switch (MJGetInnerValues(node, innerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /*
                         * Test the new inner tuple to see if it matches
                         * outer.
                         *
                         * If they do match, then we join them and move on to
                         * the next inner tuple (EXEC_MJ_JOINTUPLES).
                         *
                         * If they do not match then advance to next outer
                         * tuple.
                         */
                        compareResult = MJVecCompare(node);
                        MJ_DEBUG_COMPARE(compareResult);
                        if (compareResult == 0)
                            node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                        else {
                            Assert(compareResult < 0);
                            node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        }
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /*
                         * It contains a NULL and hence can't match any outer
                         * tuple, so we can skip the comparison and assume the
                         * new tuple is greater than current outer.
                         */
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /*
                         * No more inner tuples.  However, this might be only
                         * effective and not physical end of inner plan, so
                         * force mj_InnerTupleSlot to null to make sure we
                         * don't fetch more inner tuples.  (We need this hack
                         * because we are not transiting to a state where the
                         * inner plan is assumed to be exhausted.)
                         */
                        node->mj_InnerOffset = NullBatchTuple;
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    default:
                        break;
                }
                break;
                /* -------------------------------------------
                 * EXEC_MJ_NEXTOUTER means
                 *
                 *              outer inner
                 * outer tuple -  5     5  - marked tuple
                 *                5     5
                 *                6     6  - inner tuple
                 *                7     7
                 *
                 * we know we just bumped into the
                 * first inner tuple > current outer tuple (or possibly
                 * the end of the inner stream)
                 * so get a new outer tuple and then
                 * proceed to test it against the marked tuple
                 * (EXEC_MJ_TESTOUTER)
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this outer tuple.
                 * ------------------------------------------------
                 */
            case EXEC_MJ_NEXTOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_NEXTOUTER\n");
                if (doFillOuter && !node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true;
                    if (BatchFillOuter(node) == MJMaxSize)
                        goto done;
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = GetNextBatchTuple<OUTER_VAR>(node);
                node->mj_OuterOffset = outerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(outerTupleSlot);
                node->mj_MatchedOuter = false;
                /* Compute join values and check for unmatchability */
                switch (MJGetOuterValues(node, outerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /* Go test the new tuple against the marked tuple */
                        node->mj_JoinState = EXEC_MJ_TESTOUTER;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: end of outer subplan\n");
                        innerTupleSlot = node->mj_InnerOffset;
                        if (doFillInner && !BatchTupleIsNull(innerTupleSlot)) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            break;
                        }
                        node->m_fDone = true;
                        goto done;
                    case MJEVAL_NONMATCHABLE:
                        break;
                    default:
                        break;
                }
                break;
            /* --------------------------------------------------------
             * EXEC_MJ_TESTOUTER If the new outer tuple and the marked
             * tuple satisfy the merge clause then we know we have
             * duplicates in the outer scan so we have to restore the
             * inner scan to the marked tuple and proceed to join the
             * new outer tuple with the inner tuples.
             *
             * This is the case when
             *                        outer inner
             *                          4     5  - marked tuple
             *           outer tuple -  5     5
             *       new outer tuple -  5     5
             *                          6     8  - inner tuple
             *                          7    12
             *
             *              new outer tuple == marked tuple
             *
             * If the outer tuple fails the test, then we are done
             * with the marked tuples, and we have to look for a
             * match to the current inner tuple.  So we will
             * proceed to skip outer tuples until outer >= inner
             * (EXEC_MJ_SKIP_TEST).
             *
             *      This is the case when
             *
             *                        outer inner
             *                          5     5  - marked tuple
             *           outer tuple -  5     5
             *       new outer tuple -  6     8  - inner tuple
             *                          7    12
             *
             *              new outer tuple > marked tuple
             *
             * ---------------------------------------------------------
             */
            case EXEC_MJ_TESTOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_TESTOUTER\n");
                /*
                 * Here we must compare the outer tuple with the marked inner
                 * tuple.  (We can ignore the result of MJEvalInnerValues,
                 * since the marked inner tuple is certainly matchable.)
                 */
                innerTupleSlot = node->mj_MarkedOffset;
                (void)MJGetMarkedValues(node, innerTupleSlot);

                compareResult = MJVecCompare(node);
                MJ_DEBUG_COMPARE(compareResult);
                if (compareResult == 0) {
                    /*
                     * the merge clause matched so now we restore the inner
                     * scan position to the first mark, and go join that tuple
                     * (and any following ones) to the new outer.
                     *
                     * NOTE: we do not need to worry about the MatchedInner
                     * state for the rescanned inner tuples.  We know all of
                     * them will match this new outer tuple and therefore
                     * won't be emitted as fill tuples.  This works *only*
                     * because we require the extra joinquals to be constant
                     * when doing a right or full join --- otherwise some of
                     * the rescanned tuples might fail the extra joinquals.
                     * This obviously won't happen for a constant-true extra
                     * joinqual, while the constant-false case is handled by
                     * forcing the merge clause to never match, so we never
                     * get here.
                     */
                    if (!BatchRestrPos(node)) {
                        /* query done if fail to restore position */
                        node->m_fDone = true;
                        goto restart;
                    }

                    /*
                     *ExecRestrPos probably should give us b-ack a new Slot,
                     * but since it doesn't, use the marked slot.  (The
                     * previously returned mj_InnerTupleSlot cannot be assumed
                     * to hold the required tuple.)
                     */
                    node->mj_InnerOffset = innerTupleSlot;
                    /* we need not do MJEvalInnerValues again */
                    node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else {
                    /* ----------------
                     *  if the new outer tuple didn't match the marked inner
                     *  tuple then we have a case like:
                     *           outer inner
                     *             4     4  - marked tuple
                     * new outer - 5     4
                     *             6     5  - inner tuple
                     *             7
                     *  which means that all subsequent outer tuples will be
                     *  larger than our marked inner tuples.  So we need not
                     *  revisit any of the marked tuples but can proceed to
                     *  look for a match to the current inner.  If there's
                     *  no more inners, no more matches are possible.
                     * ----------------
                     */
                    Assert(compareResult > 0);
                    innerTupleSlot = node->mj_InnerOffset;

                    /* reload comparison data for current inner */
                    switch (MJGetInnerValues(node, innerTupleSlot)) {
                        case MJEVAL_MATCHABLE:
                            /* proceed to compare it to the current outer */
                            node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                            break;
                        case MJEVAL_NONMATCHABLE:
                            /*
                             * current inner can't possibly match any outer;
                             * better to advance the inner scan than the
                             * outer.
                             */
                            node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                            break;
                        case MJEVAL_ENDOFJOIN:
                            /* No more inner tuples */
                            if (doFillOuter) {
                                /*
                                 * Need to emit left-join tuples for remaining
                                 * outer tuples.
                                 */
                                node->mj_JoinState = EXEC_MJ_ENDINNER;
                                break;
                            }
                            node->m_fDone = true;
                            goto done;
                        default:
                            break;
                    }
                }
                break;
                /* ----------------------------------------------------------
                 * EXEC_MJ_SKIP means compare tuples and if they do not
                 * match, skip whichever is lesser.
                 *
                 * For example:
                 *
                 *              outer inner
                 *                5     5
                 *                5     5
                 * outer tuple -  6     8  - inner tuple
                 *                7    12
                 *                8    14
                 *
                 * we have to advance the outer scan
                 * until we find the outer 8.
                 *
                 * On the other hand:
                 *
                 *              outer inner
                 *                5     5
                 *                5     5
                 * outer tuple - 12     8  - inner tuple
                 *               14    10
                 *               17    12
                 *
                 * we have to advance the inner scan
                 * until we find the inner 12.
                 * ----------------------------------------------------------
                 */
            case EXEC_MJ_SKIP_TEST:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIP_TEST\n");
                /*
                 * before we advance, make sure the current tuples do not
                 * satisfy the mergeclauses.  If they do, then we update the
                 * marked tuple position and go join them.
                 */
                compareResult = MJVecCompare(node);
                MJ_DEBUG_COMPARE(compareResult);
                if (compareResult == 0) {
                    BatchMarkPos(node);
                    node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if (compareResult < 0)
                    node->mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                else
                    node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                break;
            /*
             * SKIPOUTER_ADVANCE: advance over an outer tuple that is
             * known not to join to any inner tuple.
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this outer tuple.
             */
            case EXEC_MJ_SKIPOUTER_ADVANCE:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIPOUTER_ADVANCE\n");
                if (doFillOuter && !node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true;
                    if (BatchFillOuter(node) == MJMaxSize)
                        goto done;
                }
                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = GetNextBatchTuple<OUTER_VAR>(node);
                node->mj_OuterOffset = outerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(outerTupleSlot);
                node->mj_MatchedOuter = false;

                /* Compute join values and check for unmatchability */
                switch (MJGetOuterValues(node, outerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /* Go test the new tuple against the current inner */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Can't match, so fetch next outer tuple */
                        node->mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: end of outer subplan\n");
                        innerTupleSlot = node->mj_InnerOffset;
                        if (doFillInner && !BatchTupleIsNull(innerTupleSlot)) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            break;
                        }
                        node->m_fDone = true;
                        goto done;
                    default:
                        break;
                }
                break;
                /*
                 * SKIPINNER_ADVANCE: advance over an inner tuple that is
                 * known not to join to any outer tuple.
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this inner tuple.
                 */
            case EXEC_MJ_SKIPINNER_ADVANCE:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIPINNER_ADVANCE\n");
                if (doFillInner && !node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true; /* do it only once */
                    if (BatchFillInner(node) == MJMaxSize)
                        goto done;
                }
                /*
                 * now we get the next inner tuple, if any
                 */
                innerTupleSlot = GetNextBatchTuple<INNER_VAR>(node);
                node->mj_InnerOffset = innerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(innerTupleSlot);
                node->mj_MatchedInner = false;
                /* Compute join values and check for unmatchability */
                switch (MJGetInnerValues(node, innerTupleSlot)) {
                    case MJEVAL_MATCHABLE:
                        /* proceed to compare it to the current outer */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /*
                         * current inner can't possibly match any outer;
                         * better to advance the inner scan than the outer.
                         */
                        node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more inner tuples */
                        MJ_printf("ExecMergeJoin: end of inner subplan\n");
                        outerTupleSlot = node->mj_OuterOffset;
                        if (doFillOuter && !BatchTupleIsNull(outerTupleSlot)) {
                            /*
                             * Need to emit left-join tuples for remaining
                             * outer tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            break;
                        }
                        /* Otherwise we're done. */
                        node->m_fDone = true;
                        goto done;
                    default:
                        break;
                }
                break;
            /*
             * EXEC_MJ_ENDOUTER means we have run out of outer tuples, but
             * are doing a right/full join and therefore must null-fill
             * any remaing unmatched inner tuples.
             */
            case EXEC_MJ_ENDOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_ENDOUTER\n");
                Assert(doFillInner);
                if (!node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true;
                    if (BatchFillInner(node) == MJMaxSize)
                        goto done;
                }
                /*
                 * now we get the next inner tuple, if any
                 */
                innerTupleSlot = GetNextBatchTuple<INNER_VAR>(node);
                node->mj_InnerOffset = innerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(innerTupleSlot);
                node->mj_MatchedInner = false;
                if (BatchTupleIsNull(innerTupleSlot)) {
                    MJ_printf("ExecMergeJoin: end of inner subplan\n");
                    node->m_fDone = true;
                    goto done;
                }
                /* Else remain in ENDOUTER state and process next tuple. */
                break;
            /*
             * EXEC_MJ_ENDINNER means we have run out of inner tuples, but
             * are doing a left/full join and therefore must null- fill
             * any remaing unmatched outer tuples.
             */
            case EXEC_MJ_ENDINNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_ENDINNER\n");
                Assert(doFillOuter);
                if (!node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true;
                    if (BatchFillOuter(node) == MJMaxSize)
                        goto done;
                }
                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = GetNextBatchTuple<OUTER_VAR>(node);
                node->mj_OuterOffset = outerTupleSlot;
                VEC_MJ_DEBUG_PROC_NODE(outerTupleSlot);
                node->mj_MatchedOuter = false;
                if (BatchTupleIsNull(outerTupleSlot)) {
                    MJ_printf("ExecMergeJoin: end of outer subplan\n");
                    node->m_fDone = true;
                    goto done;
                }

                /* Else remain in ENDINNER state and process next tuple. */
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("unrecognized mergejoin state: %d", (int)node->mj_JoinState)));
        }
    }
done:
    /* Enough rows for current batch, do the qual check and projection. If
     * qual checks remove all rows, then we shall restart the loop.
     */
    resultBatch = ProduceResultBatchT<joinType>(node, true);
    if (BatchIsNull(resultBatch) == false)
        return resultBatch;
    goto restart;
}

VectorBatch* ExecVecMergeJoin(VecMergeJoinState* node)
{
    VectorBatch* batch = NULL;
    switch (node->js.jointype) {
        case JOIN_INNER:
            batch = ExecVecMergeJoinT<JOIN_INNER>(node);
            break;
        case JOIN_SEMI:
            batch = ExecVecMergeJoinT<JOIN_SEMI>(node);
            break;
        case JOIN_ANTI:
            batch = ExecVecMergeJoinT<JOIN_ANTI>(node);
            break;
        case JOIN_LEFT:
            batch = ExecVecMergeJoinT<JOIN_LEFT>(node);
            break;
        case JOIN_RIGHT:
            batch = ExecVecMergeJoinT<JOIN_RIGHT>(node);
            break;
        case JOIN_FULL:
            batch = ExecVecMergeJoinT<JOIN_FULL>(node);
            break;
        case JOIN_LEFT_ANTI_FULL:
            batch = ExecVecMergeJoinT<JOIN_LEFT_ANTI_FULL>(node);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type: %d", (int)node->js.jointype)));
    }
    return batch;
}

/* ExecInitVecMergeJoin
 * Init vectorized merge join execution
 */
VecMergeJoinState* ExecInitVecMergeJoin(VecMergeJoin* node, EState* estate, int eflags)
{
    int i;
    VecMergeJoinState* mergestate = NULL;
    BatchAccessor* pbaccessor = NULL;
    MemoryContext context;

    /* We rely on optimizer to optimize operation of it */
    Assert(sizeof(int64) == sizeof(MJBatchOffset));

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    MJ1_printf("ExecInitMergeJoin: %s\n", "initializing node");

    /*
     * create state structure
     */
    mergestate = makeNode(VecMergeJoinState);
    mergestate->js.ps.plan = (Plan*)node;
    mergestate->js.ps.state = estate;
    mergestate->js.ps.vectorized = true;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mergestate->js.ps);

    /*
     * we need two additional econtexts in which we can compute the join
     * expressions from the left and right input tuples.  The node's regular
     * econtext won't do because it gets reset too often.
     */
    mergestate->mj_OuterEContext = CreateExprContext(estate);
    mergestate->mj_InnerEContext = CreateExprContext(estate);

    /* initialize child expressions */
    mergestate->js.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->join.plan.targetlist, (PlanState*)mergestate);
    mergestate->js.ps.qual = (List*)ExecInitVecExpr((Expr*)node->join.plan.qual, (PlanState*)mergestate);
    mergestate->js.jointype = node->join.jointype;
    mergestate->js.joinqual = (List*)ExecInitVecExpr((Expr*)node->join.joinqual, (PlanState*)mergestate);
    mergestate->js.nulleqqual = (List*)ExecInitVecExpr((Expr*)node->join.nulleqqual, (PlanState*)mergestate);
    mergestate->mj_ConstFalseJoin = false;
    mergestate->mj_ExtraMarks = false;
    /* mergeclauses are handled below */
    /*
     * initialize child nodes
     *
     * inner child must support MARK/RESTORE.
     */
    outerPlanState(mergestate) = ExecInitNode(outerPlan(node), estate, eflags);
    innerPlanState(mergestate) = ExecInitNode(innerPlan(node), estate, eflags | EXEC_FLAG_MARK);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &mergestate->js.ps);
    mergestate->mj_MarkedOffset = NullBatchTuple;

    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
            mergestate->mj_FillOuter = false;
            mergestate->mj_FillInner = false;
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            mergestate->mj_FillOuter = true;
            mergestate->mj_FillInner = false;
            break;
        case JOIN_RIGHT:
            /* JOIN_RIGHT_ANTI_FULL can not create mergejoin plan, please refer to
             * select_mergejoin_clauses
             * *mergejoin_allowed = false;
             */
            mergestate->mj_FillOuter = false;
            mergestate->mj_FillInner = true;

            /*
             * Can't handle right or full join with non-constant extra
             * joinclauses.  This should have been caught by planner.
             */
            if (!check_constant_qual(node->join.joinqual, &mergestate->mj_ConstFalseJoin))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("RIGHT JOIN is only supported with merge-joinable join conditions")));
            break;
        case JOIN_FULL:
            mergestate->mj_FillOuter = true;
            mergestate->mj_FillInner = true;

            /*
             * Can't handle right or full join with non-constant extra
             * joinclauses.  This should have been caught by planner.
             */
            if (!check_constant_qual(node->join.joinqual, &mergestate->mj_ConstFalseJoin))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("FULL JOIN is only supported with merge-joinable join conditions")));
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type: %d", (int)node->join.jointype)));
    }

    /*
     * initialize tuple type and projection info
     * result table tuple slot for merge join contains virtual tuple, so the
     * default tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&mergestate->js.ps);

    PlanState* planstate = &mergestate->js.ps;

    planstate->ps_ProjInfo = ExecBuildVecProjectionInfo(
        planstate->targetlist, node->join.plan.qual, planstate->ps_ExprContext, planstate->ps_ResultTupleSlot, NULL);
    ScalarDesc desc;
    if (planstate->ps_ProjInfo == NULL) {
        mergestate->m_pReturnBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, &desc, 1);
    } else {
        mergestate->m_pReturnBatch = NULL;
    }

    /* preprocess the merge clauses
     * We mixed use VecMergeJoinClause and MergeJoinClause here as MJExamineQuals() does not
     * access their different memebers.
     */
    context = CurrentMemoryContext;
    mergestate->mj_NumClauses = list_length(node->mergeclauses);
    mergestate->mj_Clauses = (VecMergeJoinClause)MJVecExamineQuals(node->mergeclauses,
        node->mergeFamilies,
        node->mergeCollations,
        node->mergeStrategies,
        node->mergeNullsFirst,
        (PlanState*)mergestate);

#ifdef ENABLE_LLVM_COMPILE
    /*
     * After all of the expressions have been decided, check if the following
     * exprs can be codegened or not.
     */
    llvm::Function* mj_joinqual = NULL;
    llvm::Function* jitted_vectarget = NULL;
    dorado::GsCodeGen* llvmCodeGen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen =
        CodeGenThreadObjectReady() &&
        CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
    if (consider_codegen) {
        mj_joinqual = dorado::VecExprCodeGen::QualCodeGen(mergestate->js.joinqual, (PlanState*)mergestate, false);
        if (mj_joinqual != NULL)
            llvmCodeGen->addFunctionToMCJit(mj_joinqual, reinterpret_cast<void**>(&(mergestate->jitted_joinqual)));

        if (mergestate->js.ps.ps_ProjInfo->pi_targetlist)
            jitted_vectarget = dorado::VecExprCodeGen::TargetListCodeGen(
                mergestate->js.ps.ps_ProjInfo->pi_targetlist, (PlanState*)mergestate);
        if (jitted_vectarget != NULL) {
            llvmCodeGen->addFunctionToMCJit(
                jitted_vectarget, reinterpret_cast<void**>(&(mergestate->js.ps.ps_ProjInfo->jitted_vectarget)));
        }
    }
#endif

    for (i = 0; i < mergestate->mj_NumClauses; i++) {
        VecMergeJoinClause clause = &mergestate->mj_Clauses[i];

        clause->m_lvector = New(context) ScalarVector();
        clause->m_rvector = New(context) ScalarVector();
        clause->m_lvector->init(context, desc);
        clause->m_rvector->init(context, desc);
        clause->mark_buf = NULL;
        clause->mark_buf_len = 0;
    }

    /*
     * initialize join state
     */
    mergestate->m_fDone = false;
    mergestate->mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    mergestate->js.ps.ps_vec_TupFromTlist = false;
    mergestate->mj_MatchedOuter = false;
    mergestate->mj_MatchedInner = false;
    mergestate->mj_OuterOffset = NullBatchTuple;
    mergestate->mj_InnerOffset = NullBatchTuple;

    mergestate->m_prevInnerOffset = NullBatchTuple;
    mergestate->m_prevInnerQualified = false;
    mergestate->m_prevOuterOffset = NullBatchTuple;
    mergestate->m_prevOuterQualified = false;

    /* Initialize batch accessors */
    pbaccessor = &mergestate->m_inputs[0];
    pbaccessor->m_curBatch = NULL;
    pbaccessor->m_batchSeq = 0;
    pbaccessor->m_curOffset = -1;
    pbaccessor->m_plan = innerPlanState(mergestate);
    pbaccessor = &mergestate->m_inputs[1];
    pbaccessor->m_curBatch = NULL;
    pbaccessor->m_batchSeq = 0;
    pbaccessor->m_curOffset = -1;
    pbaccessor->m_plan = outerPlanState(mergestate);

    TupleDesc innerDesc = innerPlanState(mergestate)->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc outerDesc = outerPlanState(mergestate)->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc resDesc = mergestate->js.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

    mergestate->m_pInnerMatch = New(context) VectorBatch(context, innerDesc);
    mergestate->m_pInnerOffset = (MJBatchOffset*)palloc(sizeof(MJBatchOffset) * MJMaxSize);
    mergestate->m_pOuterMatch = New(context) VectorBatch(context, outerDesc);
    mergestate->m_pOuterOffset = (MJBatchOffset*)palloc(sizeof(MJBatchOffset) * MJMaxSize);
    mergestate->m_pCurrentBatch = New(context) VectorBatch(context, resDesc);
    mergestate->mj_MarkedBatch = New(context) VectorBatch(context, innerDesc);

    /* Allocate vector for qualification results */
    ExecAssignVectorForExprEval(mergestate->js.ps.ps_ExprContext);

    return mergestate;
}

/* ExecEndVecMergeJoin
 * End the vectorized merge join
 */
void ExecEndVecMergeJoin(VecMergeJoinState* node)
{
    MJ1_printf("ExecEndMergeJoin: %s\n", "ending node processing");

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

    /*
     * shut down the subplans
     */
    ExecEndNode(innerPlanState(node));
    ExecEndNode(outerPlanState(node));

    MJ1_printf("ExecEndMergeJoin: %s\n", "node processing ended");
}

/* ExecReScanVecMergeJoin
 * ReScan the vectorized merge join
 */
void ExecReScanVecMergeJoin(VecMergeJoinState* node)
{
    BatchAccessor* pbaccessor = NULL;
    node->mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    node->js.ps.ps_vec_TupFromTlist = false;
    node->mj_MatchedOuter = false;
    node->mj_MatchedInner = false;
    node->mj_OuterOffset = NullBatchTuple;
    node->mj_InnerOffset = NullBatchTuple;
    node->m_fDone = false;

    node->m_prevInnerOffset = NullBatchTuple;
    node->m_prevInnerQualified = false;
    node->m_prevOuterOffset = NullBatchTuple;
    node->m_prevOuterQualified = false;

    /* Reset batch accessors */
    pbaccessor = &node->m_inputs[0];
    pbaccessor->m_curBatch = NULL;
    pbaccessor->m_batchSeq = 0;
    pbaccessor->m_curOffset = -1;
    pbaccessor = &node->m_inputs[1];
    pbaccessor->m_curBatch = NULL;
    pbaccessor->m_batchSeq = 0;
    pbaccessor->m_curOffset = -1;

    node->mj_MarkedOffset = NullBatchTuple;

    /*
     * if chgParam of subnodes is not null then plans will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        VecExecReScan(node->js.ps.lefttree);
    if (node->js.ps.righttree->chgParam == NULL)
        VecExecReScan(node->js.ps.righttree);
}
